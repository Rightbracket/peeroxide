//! A swarm never holds more connections than `max_peers`, however many
//! peers it dials at once, so that a `memory_budget` shared out over
//! `max_peers` holds.

use std::time::Duration;

use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig};
use peeroxide_dht::rpc::DhtConfig;

fn local_dht(bootstrap: Vec<String>, firewalled: bool) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = firewalled;
    if !firewalled {
        dht.ephemeral = Some(false);
    }
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

#[tokio::test]
async fn a_swarm_that_dials_several_peers_at_once_keeps_to_max_peers() {
    let _ = tracing_subscriber::fmt::try_init();
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    // Reachable DHT nodes, which store announces and relay handshakes.
    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], false)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = Vec::new();
    for _ in 0..3 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), false)).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }
    let topic = [0x6d; 32];

    // Three servers on the topic.
    let mut servers = Vec::new();
    for _ in 0..3 {
        let mut config = SwarmConfig::default();
        config.dht = local_dht(bootstrap.clone(), false);
        let (task, server, conn_rx) = spawn_swarm(config).await?;
        let mut opts = JoinOpts::default();
        opts.client = false;
        server.join(topic, opts).await?;
        server.flush().await?;
        servers.push((task, server, conn_rx));
    }

    // A client with room for one peer, which dials up to three at once.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap, false);
    config.max_peers = 1;
    assert!(config.max_parallel >= 3);
    let (_task, client, mut conn_rx) = spawn_swarm(config).await?;
    let mut opts = JoinOpts::default();
    opts.server = false;
    client.join(topic, opts).await?;
    client.flush().await?;

    // The first connection, and then none for as long as it is held.
    let mut connections = Vec::new();
    let first = tokio::time::timeout(Duration::from_secs(30), conn_rx.recv())
        .await
        .map_err(|_| "no connection")?
        .ok_or("connection channel closed")?;
    connections.push(first);
    let quiet = tokio::time::Instant::now() + Duration::from_secs(15);
    while let Ok(Some(conn)) = tokio::time::timeout_at(quiet, conn_rx.recv()).await {
        connections.push(conn);
    }
    assert_eq!(connections.len(), 1, "a swarm with max_peers = 1 held {} connections", connections.len());

    client.destroy().await?;
    for (_, server, _) in &servers {
        server.destroy().await?;
    }
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}
