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

pub(crate) enum DiscoveryEvent {
    PeerFound {
        public_key: [u8; 32],
        relay_addresses: Vec<Ipv4Peer>,
        topic: [u8; 32],
    },
    RefreshComplete {
        topic: [u8; 32],
    },
}

pub(crate) struct PeerDiscoveryConfig {
    pub topic: [u8; 32],
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
    do_refresh(&config, &dht, &key_pair, &relay_addresses, &event_tx).await;

    loop {
        let jitter_ms = rand::rng().random_range(0..REFRESH_JITTER_MS);
        let delay = REFRESH_INTERVAL + Duration::from_millis(jitter_ms);

        tokio::select! {
            _ = tokio::time::sleep(delay) => {
                do_refresh(&config, &dht, &key_pair, &relay_addresses, &event_tx).await;
            }
            _ = &mut cancel_rx => break,
        }
    }
}

async fn do_refresh(
    config: &PeerDiscoveryConfig,
    dht: &HyperDhtHandle,
    key_pair: &KeyPair,
    relay_addresses: &[Ipv4Peer],
    event_tx: &mpsc::UnboundedSender<DiscoveryEvent>,
) {
    if config.is_server {
        match dht.announce(config.topic, key_pair, relay_addresses).await {
            Ok(r) => {
                tracing::debug!(
                    closest = r.closest_nodes.len(),
                    "announce complete"
                );
            }
            Err(e) => {
                tracing::warn!(err = %e, "announce failed");
            }
        }

        // Self-announce: announce hash(publicKey) so that nodes closest to our
        // public key store a ForwardEntry.  This is how PEER_HANDSHAKE requests
        // get routed — Node.js does this in persistent.js announce().
        let pk_target = hash(&key_pair.public_key);
        match dht.announce(pk_target, key_pair, relay_addresses).await {
            Ok(r) => {
                tracing::debug!(
                    closest = r.closest_nodes.len(),
                    "self-announce (hash(pk)) complete"
                );
            }
            Err(e) => {
                tracing::warn!(err = %e, "self-announce (hash(pk)) failed");
            }
        }
    }

    if config.is_client {
        match dht.lookup(config.topic).await {
            Ok(results) => {
                for result in results {
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
                        });
                    }
                }
            }
            Err(e) => {
                tracing::warn!(err = %e, "lookup failed");
            }
        }
    }

    let _ = event_tx.send(DiscoveryEvent::RefreshComplete {
        topic: config.topic,
    });
}
