#![deny(clippy::all)]

//! Issue #55: the first persistent node of a DHT, bound to a wildcard host
//! with nothing to bootstrap from. Other nodes only accept a node id that
//! matches the address they see the node on, and such a node has no one to
//! ask for its address.

use libudx::UdxRuntime;
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, HyperDhtHandle, KeyPair};
use peeroxide_dht::peer::peer_id;
use peeroxide_dht::rpc::{self, DhtConfig, DhtError, DhtHandle};

fn local(host: &str, bootstrap: Vec<String>, ephemeral: Option<bool>) -> DhtConfig {
    let mut cfg = DhtConfig::default();
    cfg.host = host.to_string();
    cfg.bootstrap = bootstrap;
    cfg.ephemeral = ephemeral;
    cfg.firewalled = false;
    cfg
}

async fn spawn_rpc(runtime: &UdxRuntime, cfg: DhtConfig) -> (DhtHandle, u16) {
    let (_task, handle) = rpc::spawn(runtime, cfg).await.expect("spawn");
    handle.bootstrapped().await.expect("bootstrapped");
    let port = handle.local_port().await.expect("port");
    (handle, port)
}

async fn spawn_hyper(runtime: &UdxRuntime, dht: DhtConfig) -> (HyperDhtHandle, u16) {
    let mut cfg = HyperDhtConfig::default();
    cfg.dht = dht;
    let (_task, handle, _rx) = hyperdht::spawn(runtime, cfg).await.expect("spawn");
    handle.bootstrapped().await.expect("bootstrapped");
    let port = handle.local_port().await.expect("port");
    (handle, port)
}

/// Announces a topic from one ephemeral client and looks it up from another,
/// both bootstrapped only from `first`. Returns how many nodes stored the
/// announcement and how many peers the lookup found.
async fn announce_and_lookup(runtime: &UdxRuntime, first_port: u16) -> (usize, usize) {
    let bootstrap = vec![format!("127.0.0.1:{first_port}")];
    let (announcer, _) = spawn_hyper(runtime, local("127.0.0.1", bootstrap.clone(), None)).await;
    let (looker, _) = spawn_hyper(runtime, local("127.0.0.1", bootstrap, None)).await;

    let key_pair = KeyPair::generate();
    let topic = hash(b"peeroxide issue 55");
    let stored = announcer
        .announce(topic, &key_pair, &[])
        .await
        .expect("announce")
        .closest_nodes
        .len();
    let found = looker
        .lookup(topic)
        .await
        .expect("lookup")
        .iter()
        .map(|r| r.peers.len())
        .sum();

    let _ = announcer.destroy().await;
    let _ = looker.destroy().await;
    (stored, found)
}

/// Without a public host, the first node can't learn its address: other
/// nodes don't keep it, and the DHT built on it can't store anything.
/// Requests from ephemeral clients don't tell it its address either.
#[tokio::test]
async fn wildcard_first_node_without_public_host_is_not_used() {
    let runtime = UdxRuntime::new().expect("runtime");
    let (first, first_port) = spawn_hyper(&runtime, local("0.0.0.0", vec![], None)).await;
    let id_before = first.dht().table_id().await.expect("id");
    assert_ne!(id_before, Some(peer_id("127.0.0.1", first_port)));

    let (stored, found) = announce_and_lookup(&runtime, first_port).await;
    assert_eq!(stored, 0, "no node should accept the announcement");
    assert_eq!(found, 0, "lookup should find nothing");

    assert_eq!(
        first.dht().table_id().await.expect("id"),
        id_before,
        "requests from ephemeral clients must not change the id"
    );
    let _ = first.destroy().await;
}

/// `DhtConfig::bootstrapper` gives a wildcard-bound first node its id up
/// front, so the DHT built on it works.
#[tokio::test]
async fn bootstrapper_makes_wildcard_first_node_usable() {
    let runtime = UdxRuntime::new().expect("runtime");
    let cfg = DhtConfig::bootstrapper(0, "127.0.0.1");
    assert_eq!(cfg.host, "0.0.0.0");
    let (first, first_port) = spawn_hyper(&runtime, cfg).await;
    assert_eq!(
        first.dht().table_id().await.expect("id"),
        Some(peer_id("127.0.0.1", first_port))
    );
    let remote = first.dht().remote_address().await.expect("remote address");
    assert_eq!(remote.map(|r| (r.host, r.port)), Some(("127.0.0.1".to_string(), first_port)));

    let bootstrap = vec![format!("127.0.0.1:{first_port}")];
    let (second, _) = spawn_rpc(&runtime, local("127.0.0.1", bootstrap, Some(false))).await;
    assert_eq!(
        second.table_size().await.expect("table size"),
        1,
        "the second node should keep the first in its routing table"
    );

    let (stored, found) = announce_and_lookup(&runtime, first_port).await;
    assert!(stored >= 1, "the first node should accept the announcement");
    assert_eq!(found, 1, "lookup should find the announced peer");

    let _ = second.destroy().await;
    let _ = first.destroy().await;
}

/// A wildcard-bound first node without a public host learns its id from the
/// first reply to one of its own requests, after which other nodes keep it.
#[tokio::test]
async fn wildcard_first_node_learns_id_from_later_replies() {
    let runtime = UdxRuntime::new().expect("runtime");
    let (first, first_port) = spawn_rpc(&runtime, local("0.0.0.0", vec![], None)).await;

    let bootstrap = vec![format!("127.0.0.1:{first_port}")];
    let (second, second_port) =
        spawn_rpc(&runtime, local("127.0.0.1", bootstrap, Some(false))).await;
    assert_eq!(
        second.table_size().await.expect("table size"),
        0,
        "a node without a valid id must not be kept"
    );
    assert_ne!(
        first.table_id().await.expect("id"),
        Some(peer_id("127.0.0.1", first_port))
    );

    first.ping("127.0.0.1", second_port).await.expect("ping second");
    assert_eq!(
        first.table_id().await.expect("id"),
        Some(peer_id("127.0.0.1", first_port)),
        "the reply should tell the first node its address"
    );
    let remote = first.remote_address().await.expect("remote address");
    assert_eq!(remote.map(|r| (r.host, r.port)), Some(("127.0.0.1".to_string(), first_port)));

    second.ping("127.0.0.1", first_port).await.expect("ping first");
    assert_eq!(second.table_size().await.expect("table size"), 1);

    let _ = second.destroy().await;
    let _ = first.destroy().await;
}

/// A wildcard-bound node that learns its id while bootstrapping queries its
/// bootstrap nodes again with it, so they keep it although they first saw it
/// without one.
#[tokio::test]
async fn wildcard_node_requeries_bootstrap_nodes_with_learned_id() {
    let runtime = UdxRuntime::new().expect("runtime");
    let (first, first_port) = spawn_rpc(&runtime, local("0.0.0.0", vec![], Some(false))).await;

    let bootstrap = vec![format!("127.0.0.1:{first_port}")];
    let (second, second_port) =
        spawn_rpc(&runtime, local("0.0.0.0", bootstrap, Some(false))).await;
    assert_eq!(
        second.table_id().await.expect("id"),
        Some(peer_id("127.0.0.1", second_port))
    );
    assert_eq!(
        first.table_size().await.expect("table size"),
        1,
        "the first node should keep the second once it has an id"
    );

    let _ = second.destroy().await;
    let _ = first.destroy().await;
}

/// A firewalled node sends its requests from a client socket, not the one it
/// is reached on, so the replies can't tell it the address its id derives
/// from.
#[tokio::test]
async fn firewalled_wildcard_node_does_not_take_client_socket_address() {
    let runtime = UdxRuntime::new().expect("runtime");
    let mut cfg = local("0.0.0.0", vec![], Some(false));
    cfg.firewalled = true;
    let (first, first_port) = spawn_rpc(&runtime, cfg).await;
    let id_before = first.table_id().await.expect("id");

    let bootstrap = vec![format!("127.0.0.1:{first_port}")];
    let (second, second_port) =
        spawn_rpc(&runtime, local("127.0.0.1", bootstrap, Some(false))).await;

    first.ping("127.0.0.1", second_port).await.expect("ping second");
    assert_eq!(first.table_id().await.expect("id"), id_before);

    second.ping("127.0.0.1", first_port).await.expect("ping first");
    assert_eq!(second.table_size().await.expect("table size"), 0);

    let _ = second.destroy().await;
    let _ = first.destroy().await;
}

#[tokio::test]
async fn invalid_public_host_is_rejected() {
    let runtime = UdxRuntime::new().expect("runtime");
    for host in ["0.0.0.0", "::1", "example.com", ""] {
        let err = rpc::spawn(&runtime, DhtConfig::bootstrapper(0, host))
            .await
            .err()
            .unwrap_or_else(|| panic!("public_host {host:?} should be rejected"));
        assert!(matches!(err, DhtError::InvalidConfig(_)), "{host:?}: {err}");
    }
}
