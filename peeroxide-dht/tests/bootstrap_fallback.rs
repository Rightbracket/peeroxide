//! A node that found no bootstrap node reachable at start fills its routing
//! table once one is: a query on a table with fewer than k nodes also asks
//! the bootstrap nodes, as Node's `Query._open` does (#56).

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig, KeyPair};
use peeroxide_dht::rpc::DhtConfig;

fn local_dht(bootstrap: Vec<String>, port: u16) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = port;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = false;
    dht.ephemeral = Some(false);
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

#[tokio::test]
async fn a_query_on_an_empty_routing_table_asks_the_bootstrap_nodes() {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The bootstrap node's port is held by a socket that answers nothing.
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let bs_port = silent.local_addr()?.port();
    let bootstrap = vec![format!("127.0.0.1:{bs_port}")];

    let rt = UdxRuntime::new()?;
    let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), 0)).await?;
    node.bootstrapped().await?;
    assert_eq!(node.table_size().await?, 0, "no node answered the bootstrap");

    // The bootstrap node comes up.
    drop(silent);
    let (_, bs, _) = spawn(&rt, local_dht(vec![], bs_port)).await?;

    // The next query reaches it, though the routing table was empty.
    let keys = KeyPair::generate();
    let result = node.announce([0x56; 32], &keys, &[]).await?;
    assert!(!result.closest_nodes.is_empty(), "the announce reached no node");
    assert!(result.acknowledged > 0, "no node acknowledged the announce");
    assert!(node.table_size().await? > 0, "the routing table is still empty");
    let lookup = node.lookup_with_replies([0x56; 32]).await?;
    assert!(lookup.replies > 0, "no node answered the lookup");
    assert!(
        lookup.results.iter().any(|r| r.peers.iter().any(|p| p.public_key == keys.public_key)),
        "the lookup did not find the announce"
    );

    node.destroy().await?;
    bs.destroy().await?;
    Ok(())
}
