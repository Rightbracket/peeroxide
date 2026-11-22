use std::fmt;
use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;

use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{HyperDhtHandle, KeyPair};
use peeroxide_dht::messages::Ipv4Peer;

fn hex_short(bytes: &[u8]) -> String {
    bytes.iter().take(4).fold(String::new(), |mut s, b| {
        fmt::Write::write_fmt(&mut s, format_args!("{b:02x}")).ok();
        s
    })
}

/// 10-minute refresh interval, matching Node.js `REFRESH_INTERVAL`.
const REFRESH_INTERVAL: Duration = Duration::from_secs(600);

/// Up to 2-minute random jitter added to refresh interval.
const REFRESH_JITTER_MS: u64 = 120_000;

/// The wait before a failed refresh is tried again. It doubles with each
/// failure in a row, up to [`RETRY_MAX`].
const RETRY_MIN: Duration = Duration::from_secs(1);

/// The longest wait between tries of a failing refresh.
const RETRY_MAX: Duration = Duration::from_secs(60);

/// How often, while waiting to try a failed refresh again, the routing table
/// is checked: a table that was empty and has gained a node ends the wait.
const TABLE_POLL: Duration = Duration::from_secs(1);

/// The waits before the lookups that follow one that worked after failing,
/// or that started with no DHT node known.
/// Peers that could not reach the DHT either, such as a server started at
/// the same time, may announce only a little later, up to [`RETRY_MAX`]:
/// these lookups find them, rather than the one after the full interval.
const RECOVERY_LOOKUPS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(45),
];

pub(crate) enum DiscoveryEvent {
    PeerFound {
        public_key: [u8; 32],
        relay_addresses: Vec<Ipv4Peer>,
        topic: [u8; 32],
        /// Which join of the topic found it.
        join: u64,
    },
    RefreshComplete {
        topic: [u8; 32],
        join: u64,
    },
    /// A peer's wait after a failed dial is over: it may be dialled again.
    RetryDue {
        public_key: [u8; 32],
        /// Which retry of the peer's this is.
        token: u64,
    },
}

pub(crate) struct PeerDiscoveryConfig {
    pub topic: [u8; 32],
    /// Tells this join of the topic from a later one.
    pub join: u64,
    pub is_server: bool,
    pub is_client: bool,
}

pub(crate) async fn run_discovery(
    config: PeerDiscoveryConfig,
    dht: HyperDhtHandle,
    key_pair: KeyPair,
    relay_addresses: Vec<Ipv4Peer>,
    event_tx: mpsc::UnboundedSender<DiscoveryEvent>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) {
    // Failed refreshes in a row. Node refreshes again only after the full
    // interval, or when the network comes back online; a refresh that failed
    // or reached no node is tried again here after a short wait instead, so
    // that a swarm that joined before any DHT node was reachable is announced
    // and finds its peers soon after one is.
    let mut failures: u32 = 0;
    // Recovery lookups left to make, see [`RECOVERY_LOOKUPS`].
    let mut recovery_lookups = 0;

    loop {
        let empty_before = matches!(dht.table_size().await, Ok(0));
        // A leave cancels the refresh in progress too, so that it makes no
        // announce after the topic is left.
        let problems = tokio::select! {
            biased;
            _ = &mut cancel_rx => break,
            problems = do_refresh(&config, &dht, &key_pair, &relay_addresses, &event_tx) => problems,
        };

        let (delay, table_empty) = if problems.is_empty() {
            if failures > 0 {
                tracing::info!(failures, "discovery refresh succeeded after failing");
            }
            // A refresh that started with no DHT node known worked: the DHT
            // has only just become reachable, as after failures.
            if config.is_client && (failures > 0 || empty_before) {
                recovery_lookups = RECOVERY_LOOKUPS.len();
            }
            failures = 0;
            if recovery_lookups > 0 {
                let next = RECOVERY_LOOKUPS[RECOVERY_LOOKUPS.len() - recovery_lookups];
                recovery_lookups -= 1;
                (next, false)
            } else {
                let jitter_ms = rand::rng().random_range(0..REFRESH_JITTER_MS);
                (REFRESH_INTERVAL + Duration::from_millis(jitter_ms), false)
            }
        } else {
            failures = failures.saturating_add(1);
            let delay = retry_delay(failures);
            for problem in &problems {
                // Warn once per run of failures; the retries that follow
                // within seconds would otherwise flood the log.
                if failures == 1 {
                    tracing::warn!(retry_in = ?delay, "{problem}");
                } else {
                    tracing::debug!(failures, retry_in = ?delay, "{problem}");
                }
            }
            recovery_lookups = 0;
            let empty_now = matches!(dht.table_size().await, Ok(0));
            if empty_before && !empty_now {
                // The table gained a node during the refresh, too late for
                // it: try again with it now.
                (Duration::ZERO, false)
            } else {
                (delay, empty_now)
            }
        };

        tokio::select! {
            biased;
            _ = &mut cancel_rx => break,
            _ = wait_before_refresh(&dht, delay, table_empty) => {}
        }
    }
}

/// The wait before trying again a refresh that has failed `failures` times
/// in a row.
fn retry_delay(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(31);
    RETRY_MIN
        .saturating_mul(1u32 << doublings)
        .min(RETRY_MAX)
}

/// Wait `delay` before the next refresh, or less if the routing table was
/// empty and gains a node meanwhile.
async fn wait_before_refresh(dht: &HyperDhtHandle, delay: Duration, table_empty: bool) {
    let deadline = tokio::time::Instant::now() + delay;
    if !table_empty {
        tokio::time::sleep_until(deadline).await;
        return;
    }
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        tokio::time::sleep(TABLE_POLL.min(deadline - now)).await;
        if matches!(dht.table_size().await, Ok(n) if n > 0) {
            return;
        }
    }
}

/// Announce and/or look up the topic once. Returns what went wrong: an
/// announce or lookup that failed, or that reached no node. Empty if the
/// refresh worked.
async fn do_refresh(
    config: &PeerDiscoveryConfig,
    dht: &HyperDhtHandle,
    key_pair: &KeyPair,
    relay_addresses: &[Ipv4Peer],
    event_tx: &mpsc::UnboundedSender<DiscoveryEvent>,
) -> Vec<String> {
    let mut problems = Vec::new();

    if config.is_server {
        // Run topic-announce and self-announce on hash(pk) in parallel.
        // - Self-announce stores ForwardEntries on hash(pk)-close nodes,
        //   making them PEER_HANDSHAKE FE-holders for receivers' Phase 2
        //   (query_find_peer in connect_with_nodes).
        // - Topic-announce stores peer records on topic-close nodes, which
        //   are returned by `lookup(topic)` to discover this peer.
        // Both are independent queries and parallelizing halves refresh
        // latency on first start (which matters for short test windows).
        //
        // On first refresh `current_relay_addresses` is empty — the topic
        // record then carries no relay hints; receivers fall through Phase 1
        // immediately into Phase 2 which queries FE-holders directly via
        // FIND_PEER on hash(pk). On subsequent refreshes the prior cycle's
        // self-announce will have populated `current_relay_addresses` from
        // the hash(pk) acker set, which the topic-announce then propagates.
        // Mirrors Node's `Announcer.relayAddresses` semantics.
        let pk_target = hash(&key_pair.public_key);
        let topic_relays: Vec<Ipv4Peer> = if relay_addresses.is_empty() {
            dht.current_relay_addresses()
        } else {
            relay_addresses.to_vec()
        };

        let topic_announce = dht.announce(config.topic, key_pair, &topic_relays);
        let self_announce = dht.announce(pk_target, key_pair, relay_addresses);
        let (topic_res, self_res) = tokio::join!(topic_announce, self_announce);

        // An announce that no node confirmed taking is not known stored: it
        // failed, as Node's does ("Too few nodes responded").
        match topic_res {
            Ok(r) if r.acknowledged == 0 => {
                problems.push(format!(
                    "announce confirmed by no DHT node ({} reached)",
                    r.closest_nodes.len()
                ));
            }
            Ok(r) => {
                tracing::debug!(
                    closest = r.closest_nodes.len(),
                    advertised_relays = topic_relays.len(),
                    "announce complete"
                );
            }
            Err(e) => problems.push(format!("announce failed: {e}")),
        }
        match self_res {
            Ok(r) if r.acknowledged == 0 => {
                problems.push(format!(
                    "self-announce (hash(pk)) confirmed by no DHT node ({} reached)",
                    r.closest_nodes.len()
                ));
            }
            Ok(r) => {
                tracing::debug!(
                    closest = r.closest_nodes.len(),
                    "self-announce (hash(pk)) complete"
                );
            }
            Err(e) => problems.push(format!("self-announce (hash(pk)) failed: {e}")),
        }
    }

    if config.is_client {
        match dht.lookup_with_replies(config.topic).await {
            Ok(lookup) if lookup.replies == 0 => {
                problems.push("lookup reached no DHT node".to_string());
            }
            Ok(lookup) => {
                for result in lookup.results {
                    tracing::debug!(
                        from = %format!("{}:{}", result.from.host, result.from.port),
                        peer_count = result.peers.len(),
                        "lookup result"
                    );
                    for peer in result.peers {
                        tracing::debug!(
                            pk = %hex_short(&peer.public_key),
                            relay_count = peer.relay_addresses.len(),
                            relays = ?peer.relay_addresses.iter().map(|a| format!("{}:{}", a.host, a.port)).collect::<Vec<_>>(),
                            "discovered peer"
                        );
                        // Forward the peer's own advertised relays only. Do NOT
                        // fall back to `result.from` (the lookup-responder),
                        // because that node holds a ForwardEntry keyed on the
                        // topic, not on hash(peer.public_key). PEER_HANDSHAKE
                        // targets the latter, so using a topic-relay as a
                        // PEER_HANDSHAKE forwarder is guaranteed to fail with
                        // CLOSER_NODES → empty-reply. An empty relay list lets
                        // connect_with_nodes drop straight into its Phase 2
                        // FIND_NODE walk on hash(peer.public_key), which finds
                        // the nodes the sender actually self-announced to.
                        let _ = event_tx.send(DiscoveryEvent::PeerFound {
                            public_key: peer.public_key,
                            relay_addresses: peer.relay_addresses,
                            topic: config.topic,
                            join: config.join,
                        });
                    }
                }
            }
            Err(e) => problems.push(format!("lookup failed: {e}")),
        }
    }

    let _ = event_tx.send(DiscoveryEvent::RefreshComplete {
        topic: config.topic,
        join: config.join,
    });

    problems
}
