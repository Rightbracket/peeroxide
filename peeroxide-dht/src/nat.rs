#![allow(dead_code)]

use crate::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_OPEN, FIREWALL_RANDOM, FIREWALL_UNKNOWN};
use crate::messages::Ipv4Peer;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

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
}
