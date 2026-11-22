//! A swarm that joins a topic while no DHT node is reachable is announced,
//! and found, within seconds of one becoming reachable: its failed announce
//! is tried again soon, not at the next 10-minute refresh (#56).

use std::time::{Duration, Instant};

use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig};
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
async fn a_swarm_that_joined_offline_is_found_once_the_dht_is_reachable() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The bootstrap node's port is held by a socket that answers nothing, so
    // the server's swarm starts with no DHT node reachable.
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let bs_port = silent.local_addr()?.port();
    let bootstrap = vec![format!("127.0.0.1:{bs_port}")];

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), 0);
    let (_, server, mut accepted) = spawn_swarm(config).await?;
    let topic = [0x56; 32];
    let mut server_only = JoinOpts::default();
    server_only.client = false;
    server.join(topic, server_only).await?;
    // The first announce has been tried, and reached no node.
    tokio::time::timeout(Duration::from_secs(60), server.flush())
        .await
        .map_err(|_| "the first refresh did not complete")??;

    // The DHT becomes reachable.
    drop(silent);
    let rt = libudx::UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![], bs_port)).await?;
    let online = Instant::now();
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), 0)).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }

    // The server is announced within seconds, not at its next refresh, 10
    // minutes on.
    let server_key = server.key_pair().public_key;
    loop {
        let found = nodes[0]
            .lookup(topic)
            .await?
            .iter()
            .any(|r| r.peers.iter().any(|p| p.public_key == server_key));
        if found {
            break;
        }
        if online.elapsed() > Duration::from_secs(15) {
            return Err("the server was not announced once the DHT was reachable".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // And a client on the topic finds it.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), 0);
    let (_, client, mut conns) = spawn_swarm(config).await?;
    let mut client_only = JoinOpts::default();
    client_only.server = false;
    client.join(topic, client_only).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let conn = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let conn = tokio::time::timeout(remaining, conns.recv())
            .await
            .map_err(|_| "the client did not find the server")?
            .ok_or("connection channel closed")?;
        if conn.remote_public_key() == &server_key {
            break conn;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .map_err(|_| "the server delivered no connection")?
        .ok_or("connection channel closed")?;
    drop(conn);

    client.destroy().await?;
    server.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}

/// A client that joined while no DHT node was reachable finds a server that
/// announces only after the client's first lookup that worked: that lookup
/// is followed by a few more within a minute, not by one 10 minutes on.
#[tokio::test]
async fn a_client_that_joined_offline_finds_a_server_that_announces_soon_after() {
    tokio::time::timeout(Duration::from_secs(120), run_client_first())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run_client_first() -> Result<(), Box<dyn std::error::Error>> {
    let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let bs_port = silent.local_addr()?.port();
    let bootstrap = vec![format!("127.0.0.1:{bs_port}")];
    let topic = [0x57; 32];

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), 0);
    let (_, client, mut conns) = spawn_swarm(config).await?;
    let mut client_only = JoinOpts::default();
    client_only.server = false;
    client.join(topic, client_only).await?;
    tokio::time::timeout(Duration::from_secs(60), client.flush())
        .await
        .map_err(|_| "the first refresh did not complete")??;

    // The DHT becomes reachable, and the client's next lookup, about a
    // second on, finds no one.
    drop(silent);
    let rt = libudx::UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![], bs_port)).await?;
    let online = Instant::now();
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), 0)).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Only then does the server announce.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), 0);
    let (_, server, _accepted) = spawn_swarm(config).await?;
    let server_key = server.key_pair().public_key;
    let mut server_only = JoinOpts::default();
    server_only.client = false;
    server.join(topic, server_only).await?;
    server.flush().await?;

    let deadline = online + Duration::from_secs(45);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let conn = tokio::time::timeout(remaining, conns.recv())
            .await
            .map_err(|_| "the client did not find the server")?
            .ok_or("connection channel closed")?;
        if conn.remote_public_key() == &server_key {
            break;
        }
    }

    client.destroy().await?;
    server.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}
