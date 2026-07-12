#![allow(dead_code)]

use crate::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_OPEN, FIREWALL_RANDOM, FIREWALL_UNKNOWN};
use crate::messages::Ipv4Peer;
use crate::rpc::DhtHandle;
use libudx::{Datagram, UdxSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

pub(crate) const NAT_MIN_SAMPLES: u32 = 4;

#[derive(Debug, Clone)]
struct Sample {
    host: String,
    port: u16,
    hits: u32,
}

#[derive(Debug)]
pub struct Nat {
    samples_host: Vec<Sample>,
    samples_full: Vec<Sample>,
    visited: std::collections::HashMap<String, u8>,
    frozen: bool,
    firewalled: bool,

    pub sampled: u32,
    pub firewall: u64,
    pub addresses: Option<Vec<Ipv4Peer>>,

    analyzing_notify: Arc<Notify>,
    analyzing_settled: Arc<AtomicBool>,
}

impl Nat {
    pub fn new(firewalled: bool) -> Self {
        Self {
            samples_host: Vec::new(),
            samples_full: Vec::new(),
            visited: std::collections::HashMap::new(),
            frozen: false,
            firewalled,
            sampled: 0,
            firewall: if firewalled {
                FIREWALL_UNKNOWN
            } else {
                FIREWALL_OPEN
            },
            addresses: None,
            analyzing_notify: Arc::new(Notify::new()),
            analyzing_settled: Arc::new(AtomicBool::new(!firewalled)),
        }
    }

    pub fn destroy(&mut self) {
        self.frozen = true;
        // Force-settle on destroy. Mirrors Node `nat.js` `destroy()` calling
        // `_resolve()` unconditionally so any pending `analyzing` await
        // wakes immediately rather than hanging on a NAT that will never
        // accumulate more samples.
        if !self.analyzing_settled.swap(true, Ordering::AcqRel) {
            self.analyzing_notify.notify_waiters();
        }
    }

    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    pub fn unfreeze(&mut self) {
        self.frozen = false;
        self.update_firewall();
        self.update_addresses();
    }

    pub fn update(&mut self) {
        if self.firewalled && self.firewall == FIREWALL_OPEN {
            self.firewall = FIREWALL_UNKNOWN;
        }
        self.update_firewall();
        self.update_addresses();
    }

    pub fn add(&mut self, addr: &Ipv4Peer, from: &Ipv4Peer) {
        let from_ref = format!("{}:{}", from.host, from.port);

        if self.visited.get(&from_ref) == Some(&2) {
            return;
        }
        self.visited.insert(from_ref, 2);

        add_sample(&mut self.samples_host, &addr.host, 0);
        add_sample(&mut self.samples_full, &addr.host, addr.port);

        self.sampled += 1;

        if (self.sampled >= 3 || !self.firewalled) && !self.frozen {
            self.update();
        }

        self.try_settle();
    }

    pub fn is_settled(&self) -> bool {
        self.firewall == FIREWALL_CONSISTENT || self.firewall == FIREWALL_OPEN
    }

    pub fn analyzing(&self) -> impl std::future::Future<Output = ()> + 'static {
        let notify = Arc::clone(&self.analyzing_notify);
        let settled = Arc::clone(&self.analyzing_settled);
        async move {
            if settled.load(Ordering::Acquire) {
                return;
            }
            let waiter = notify.notified();
            tokio::pin!(waiter);
            waiter.as_mut().enable();
            if settled.load(Ordering::Acquire) {
                return;
            }
            waiter.await;
        }
    }

    fn try_settle(&mut self) {
        let should = matches!(self.firewall, FIREWALL_OPEN | FIREWALL_CONSISTENT)
            || self.sampled >= NAT_MIN_SAMPLES;
        if should && !self.analyzing_settled.swap(true, Ordering::AcqRel) {
            self.analyzing_notify.notify_waiters();
        }
    }

    pub fn mark_visited(&mut self, host: &str, port: u16) -> bool {
        let key = format!("{host}:{port}");
        if self.visited.contains_key(&key) {
            return false;
        }
        self.visited.insert(key, 1);
        true
    }

    /// Seed the puncher's NAT classifier with reflexive samples by sending
    /// DHT pings out the supplied puncher socket. Mirrors Node's
    /// `lib/nat.js:25-79 autoSample()`.
    ///
    /// Walks `dht.recent_nodes(max_samples + 5)`, fires up to 4 concurrent
    /// `dht.ping_via_socket` calls, and feeds each pong's `(to, from)`
    /// into [`Self::add`]. Returns the number of NEW samples added.
    ///
    /// Short-circuits with `0` if the NAT is already settled. Each ping
    /// is bound by a ~2s wall-clock timeout (Node-default); on failure
    /// (timeout, decode error, channel closed) the sample is skipped and
    /// the function continues.
    pub(crate) async fn auto_sample(
        &mut self,
        dht: &DhtHandle,
        socket: UdxSocket,
        dht_reply_rx: mpsc::UnboundedReceiver<Datagram>,
        max_samples: usize,
    ) -> usize {
        if self.firewall != FIREWALL_UNKNOWN {
            drop(dht_reply_rx);
            return 0;
        }

        let candidates = dht.recent_nodes(max_samples + 5);
        if candidates.is_empty() {
            drop(dht_reply_rx);
            return 0;
        }

        let forwarder = spawn_dht_reply_forwarder(dht.clone(), dht_reply_rx);

        let mut joinset: tokio::task::JoinSet<Result<crate::rpc::PingResponse, crate::rpc::DhtError>> =
            tokio::task::JoinSet::new();
        for node in candidates.into_iter().take(max_samples) {
            let dht_clone = dht.clone();
            let socket_clone = socket.clone();
            joinset.spawn(async move {
                tokio::time::timeout(
                    Duration::from_millis(2000),
                    dht_clone.ping_via_socket(node, socket_clone),
                )
                .await
                .map_err(|_| crate::rpc::DhtError::ChannelClosed)?
            });
        }

        let mut added = 0usize;
        let before = self.sampled;
        while let Some(joined) = joinset.join_next().await {
            match joined {
                Ok(Ok(resp)) => {
                    let Some(to) = resp.to.as_ref() else {
                        tracing::warn!(from = %format!("{}:{}", resp.from.host, resp.from.port), "auto_sample: ping reply missing reflexive `to`; skipping");
                        continue;
                    };
                    self.add(to, &resp.from);
                    if self.sampled > before + added as u32 {
                        added = (self.sampled - before) as usize;
                    }
                    if self.firewall != FIREWALL_UNKNOWN {
                        break;
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(err = ?e, "auto_sample: ping_via_socket failed");
                }
                Err(e) => {
                    tracing::warn!(err = %e, "auto_sample: join error");
                }
            }
        }

        joinset.abort_all();
        forwarder.abort();
        added
    }

    fn update_firewall(&mut self) {
        if !self.firewalled {
            self.firewall = FIREWALL_OPEN;
            return;
        }

        if self.sampled < 3 {
            return;
        }

        let max = match self.samples_full.first() {
            Some(s) => s.hits,
            None => return,
        };

        if max >= 3 {
            self.firewall = FIREWALL_CONSISTENT;
            return;
        }

        if max == 1 {
            self.firewall = FIREWALL_RANDOM;
            return;
        }

        // max === 2
        // 1 host, >= 4 total samples ie, 2 bad ones -> random
        if self.samples_host.len() == 1 && self.sampled > 3 {
            self.firewall = FIREWALL_RANDOM;
            return;
        }

        // double hit on two different ips -> assume consistent
        if self.samples_host.len() > 1
            && self.samples_full.len() > 1
            && self.samples_full[1].hits > 1
        {
            self.firewall = FIREWALL_CONSISTENT;
            return;
        }

        // (4 just means all the samples we expect) - no decision - assume random
        if self.sampled > 4 {
            self.firewall = FIREWALL_RANDOM;
        }
    }

    fn update_addresses(&mut self) {
        if self.firewall == FIREWALL_UNKNOWN {
            self.addresses = None;
            return;
        }

        if self.firewall == FIREWALL_RANDOM {
            if let Some(s) = self.samples_host.first() {
                self.addresses = Some(vec![Ipv4Peer {
                    host: s.host.clone(),
                    port: s.port,
                }]);
            }
            return;
        }

        if self.firewall == FIREWALL_CONSISTENT {
            let mut addrs = Vec::new();
            for s in &self.samples_full {
                if s.hits >= 2 || addrs.len() < 2 {
                    addrs.push(Ipv4Peer {
                        host: s.host.clone(),
                        port: s.port,
                    });
                }
            }
            self.addresses = Some(addrs);
        }
    }
}

/// Bridge raw `dht_reply_rx` datagrams from the puncher socket into the
/// `DhtNode` actor via [`DhtHandle::forward_inbound_reply_bytes`]. Spawned
/// for the duration of one [`Nat::auto_sample`] run; aborted on return.
fn spawn_dht_reply_forwarder(
    dht: DhtHandle,
    mut rx: mpsc::UnboundedReceiver<Datagram>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(dgram) = rx.recv().await {
            dht.forward_inbound_reply_bytes(dgram.addr, dgram.data);
        }
    })
}

fn add_sample(samples: &mut Vec<Sample>, host: &str, port: u16) {
    for i in 0..samples.len() {
        if samples[i].port != port || samples[i].host != host {
            continue;
        }

        samples[i].hits += 1;

        // Bubble up to maintain descending sort by hits
        let mut j = i;
        while j > 0 && samples[j - 1].hits < samples[j].hits {
            samples.swap(j - 1, j);
            j -= 1;
        }
        return;
    }

    samples.push(Sample {
        host: host.to_string(),
        port,
        hits: 1,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(host: &str, port: u16) -> Ipv4Peer {
        Ipv4Peer {
            host: host.into(),
            port,
        }
    }

    #[test]
    fn new_firewalled_starts_unknown() {
        let nat = Nat::new(true);
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);
        assert!(nat.addresses.is_none());
        assert_eq!(nat.sampled, 0);
    }

    #[test]
    fn new_not_firewalled_starts_open() {
        let nat = Nat::new(false);
        assert_eq!(nat.firewall, FIREWALL_OPEN);
    }

    #[test]
    fn add_sample_sorting() {
        let mut samples = Vec::new();
        add_sample(&mut samples, "1.2.3.4", 1000);
        add_sample(&mut samples, "5.6.7.8", 2000);
        add_sample(&mut samples, "1.2.3.4", 1000);
        assert_eq!(samples[0].host, "1.2.3.4");
        assert_eq!(samples[0].hits, 2);
        assert_eq!(samples[1].host, "5.6.7.8");
        assert_eq!(samples[1].hits, 1);
    }

    #[test]
    fn add_sample_triple_hit_stays_sorted() {
        let mut samples = Vec::new();
        add_sample(&mut samples, "a", 1);
        add_sample(&mut samples, "b", 2);
        add_sample(&mut samples, "c", 3);
        add_sample(&mut samples, "b", 2);
        add_sample(&mut samples, "b", 2);
        assert_eq!(samples[0].host, "b");
        assert_eq!(samples[0].hits, 3);
    }

    #[test]
    fn consistent_nat_three_same_port() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);
        assert!(nat.addresses.is_some());
        let addrs = nat.addresses.as_ref().unwrap();
        assert_eq!(addrs[0].host, "1.2.3.4");
        assert_eq!(addrs[0].port, 5000);
    }

    #[test]
    fn random_nat_all_different_ports() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5001), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5002), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_RANDOM);
        let addrs = nat.addresses.as_ref().unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].host, "1.2.3.4");
    }

    #[test]
    fn duplicate_from_ignored() {
        let mut nat = Nat::new(true);
        let from = peer("10.0.0.1", 1);
        nat.add(&peer("1.2.3.4", 5000), &from);
        nat.add(&peer("1.2.3.4", 5000), &from);         assert_eq!(nat.sampled, 1);
    }

    #[test]
    fn freeze_prevents_update() {
        let mut nat = Nat::new(true);
        nat.freeze();
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);
        assert!(nat.addresses.is_none());

        nat.unfreeze();
        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);
        assert!(nat.addresses.is_some());
    }

    #[test]
    fn not_firewalled_always_open() {
        let mut nat = Nat::new(false);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        assert_eq!(nat.firewall, FIREWALL_OPEN);
    }

    #[test]
    fn mark_visited_dedup() {
        let mut nat = Nat::new(true);
        assert!(nat.mark_visited("1.2.3.4", 1000));
        assert!(!nat.mark_visited("1.2.3.4", 1000));
    }

    #[test]
    fn two_hits_multi_host_consistent() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("5.6.7.8", 5000), &peer("10.0.0.3", 3));
        // max=2, hosts>1, full[1].hits=1 → not enough evidence yet
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);

        nat.add(&peer("5.6.7.8", 5000), &peer("10.0.0.4", 4));
        // full[1].hits=2 with hosts>1 → consistent
        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);
    }

    #[test]
    fn two_hits_single_host_over_three_random() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5001), &peer("10.0.0.3", 3));
        // max=2, 1 host, sampled=3 → not enough evidence
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);

        nat.add(&peer("1.2.3.4", 5002), &peer("10.0.0.4", 4));
        // max=2, 1 host, sampled>3 → random
        assert_eq!(nat.firewall, FIREWALL_RANDOM);
    }

    #[test]
    fn over_four_samples_no_decision_random() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("5.6.7.8", 6000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);

        nat.add(&peer("9.9.9.9", 7000), &peer("10.0.0.4", 4));
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);

        nat.add(&peer("8.8.8.8", 8000), &peer("10.0.0.5", 5));
        // sampled>4, no strong signal → random
        assert_eq!(nat.firewall, FIREWALL_RANDOM);
    }

    #[test]
    fn update_resets_open_if_firewalled() {
        let mut nat = Nat::new(false);
        assert_eq!(nat.firewall, FIREWALL_OPEN);

        nat.firewalled = true;
        nat.update();
        assert_eq!(nat.firewall, FIREWALL_UNKNOWN);
    }

    #[test]
    fn consistent_addresses_include_high_hit_entries() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);

        let addrs = nat.addresses.as_ref().unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].port, 5000);

        nat.add(&peer("1.2.3.4", 6000), &peer("10.0.0.4", 4));
        let addrs = nat.addresses.as_ref().unwrap();
        assert!(addrs.len() >= 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn analyzing_resolves_on_consistent() {
        let mut nat = Nat::new(true);
        let analyzing = nat.analyzing();

        nat.add(&peer("1.2.3.4", 5000), &peer("100.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("100.0.0.2", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("100.0.0.3", 1));

        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);

        tokio::time::timeout(std::time::Duration::from_millis(100), analyzing)
            .await
            .expect("analyzing did not resolve after CONSISTENT classification");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn analyzing_resolves_on_min_samples_exhaustion() {
        let mut nat = Nat::new(true);
        let analyzing = nat.analyzing();

        nat.add(&peer("1.2.3.4", 1), &peer("100.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 2), &peer("100.0.0.2", 1));
        nat.add(&peer("1.2.3.4", 3), &peer("100.0.0.3", 1));
        nat.add(&peer("1.2.3.4", 4), &peer("100.0.0.4", 1));

        assert!(nat.sampled >= NAT_MIN_SAMPLES);

        tokio::time::timeout(std::time::Duration::from_millis(100), analyzing)
            .await
            .expect("analyzing did not resolve after MIN_SAMPLES exhaustion");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn analyzing_resolves_on_destroy() {
        let mut nat = Nat::new(true);
        let analyzing = nat.analyzing();

        nat.destroy();

        tokio::time::timeout(std::time::Duration::from_millis(100), analyzing)
            .await
            .expect("analyzing did not resolve after destroy()");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn settled_state_after_three_consistent_samples() {
        let mut nat = Nat::new(true);
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.1", 1));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.2", 2));
        nat.add(&peer("1.2.3.4", 5000), &peer("10.0.0.3", 3));
        assert_eq!(nat.firewall, FIREWALL_CONSISTENT);
        assert!(
            nat.firewall != FIREWALL_UNKNOWN,
            "settled NAT must not be UNKNOWN — auto_sample short-circuits in this state"
        );
    }

    /// Drives `Nat::auto_sample` end-to-end against real loopback DHT nodes
    /// (no internet required). A primary node's routing table is seeded
    /// with `NAT_MIN_SAMPLES` local peer nodes via
    /// `DhtHandle::insert_node_for_test` (a test-only helper), each backed
    /// by a genuine `rpc::spawn`-ed DHT actor bound to `127.0.0.1` that can
    /// answer the internal `ping_via_socket` FIND_NODE probe. This exercises
    /// the concurrent-ping, reflexive-`to`/`from` sample-collection, and
    /// puncher-socket reply-forwarding paths without any mock trait — the
    /// "mock" is simply a same-host, no-bootstrap DHT peer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auto_sample_collects_target_samples() {
        use crate::routing_table::Node;
        use crate::socket_pool::SocketPool;

        let runtime = libudx::UdxRuntime::new().expect("runtime");

        // Primary node under test: firewalled, no bootstrap, ephemeral.
        let primary_cfg = crate::rpc::DhtConfig {
            bootstrap: vec![],
            host: "127.0.0.1".to_string(),
            ephemeral: Some(true),
            firewalled: true,
            ..crate::rpc::DhtConfig::default()
        };
        let (_primary_task, primary) = crate::rpc::spawn(&runtime, primary_cfg)
            .await
            .expect("spawn primary");

        // Ping targets: independent loopback DHT nodes that can answer a
        // FIND_NODE probe sent via a puncher socket.
        let target_count = NAT_MIN_SAMPLES as usize;
        let mut target_tasks = Vec::new();
        let mut target_handles = Vec::new();
        for _ in 0..target_count {
            let cfg = crate::rpc::DhtConfig {
                bootstrap: vec![],
                host: "127.0.0.1".to_string(),
                ephemeral: Some(true),
                firewalled: false,
                ..crate::rpc::DhtConfig::default()
            };
            let (task, handle) = crate::rpc::spawn(&runtime, cfg).await.expect("spawn target");
            target_tasks.push(task);
            target_handles.push(handle);
        }

        // Seed the primary's routing table directly (bypassing discovery)
        // so `recent_nodes()` returns our loopback targets deterministically.
        for handle in &target_handles {
            let port = handle.local_port().await.expect("target local_port");
            let node = Node {
                id: rand::random(),
                host: "127.0.0.1".to_string(),
                port,
                token: None,
                added_tick: 0,
                seen_tick: 0,
                pinged_tick: 0,
                down_hints: 0,
            };
            primary.insert_node_for_test(node);
        }

        // Acquire a puncher socket exactly as production code does, wiring
        // its DHT-reply lane through to `auto_sample`.
        let pool = SocketPool::new("127.0.0.1".to_string());
        let mut socket_ref = pool.acquire(&runtime).await.expect("acquire puncher socket");
        let socket = socket_ref.socket.clone();
        let dht_reply_rx = socket_ref
            .take_dht_reply_rx()
            .expect("dht_reply_rx available");

        let mut nat = Nat::new(true);
        let added = tokio::time::timeout(
            Duration::from_secs(10),
            nat.auto_sample(&primary, socket, dht_reply_rx, target_count),
        )
        .await
        .expect("auto_sample timed out");

        // All targets share one reflexive loopback address, so the NAT
        // classifier can settle on FIREWALL_CONSISTENT after 3 matching
        // samples and `auto_sample` breaks out early (mirrors Node's
        // early-exit once `self.firewall != FIREWALL_UNKNOWN`) — it need
        // not visit every candidate to prove samples are being collected.
        assert!(
            added >= 3,
            "expected auto_sample to collect at least 3 samples from reachable loopback targets, got {added}"
        );
        assert!(
            nat.sampled >= 3,
            "Nat::add should have been driven by successful ping_via_socket replies"
        );
        assert_eq!(
            nat.firewall, FIREWALL_CONSISTENT,
            "identical reflexive address across all targets should settle as CONSISTENT"
        );

        let _ = primary.destroy().await;
        for handle in &target_handles {
            let _ = handle.destroy().await;
        }
    }
}
