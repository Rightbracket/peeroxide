//! A peer joined by its key is connected to without a topic, and connected
//! to again when the connection closes.

use std::time::Duration;

use peeroxide::{spawn as spawn_swarm, JoinOpts, KeyPair, SwarmConfig};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig};
use peeroxide_dht::rpc::DhtConfig;

fn local_dht(bootstrap: Vec<String>) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = false;
    dht.ephemeral = Some(false);
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

#[tokio::test]
async fn a_peer_joined_by_its_key_is_connected_to_and_kept() {
    tokio::time::timeout(Duration::from_secs(150), run())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![])).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = Vec::new();
    for _ in 0..2 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone())).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }

    // The server is reachable by its key: it announces on a topic the client
    // never joins.
    let server_keys = KeyPair::generate();
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    config.key_pair = Some(server_keys.clone());
    let (_, server, mut accepted) = spawn_swarm(config).await?;
    let mut server_only = JoinOpts::default();
    server_only.client = false;
    server.join([0x91; 32], server_only).await?;
    server.flush().await?;

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    let (_, client, mut conns) = spawn_swarm(config).await?;
    client.join_peer(server_keys.public_key).await?;
    let conn = tokio::time::timeout(Duration::from_secs(30), conns.recv())
        .await
        .map_err(|_| "the peer joined by its key was not connected to")?
        .ok_or("connection channel closed")?;
    assert_eq!(conn.remote_public_key(), &server_keys.public_key);
    let first = tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .map_err(|_| "the server delivered no connection")?
        .ok_or("connection channel closed")?;

    // The connection closes: the client connects again.
    drop(conn);
    drop(first);
    let again = tokio::time::timeout(Duration::from_secs(60), conns.recv())
        .await
        .map_err(|_| "the peer joined by its key was not connected to again")?
        .ok_or("connection channel closed")?;
    assert_eq!(again.remote_public_key(), &server_keys.public_key);
    let second = tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .map_err(|_| "the server delivered no second connection")?
        .ok_or("connection channel closed")?;

    // Once left, a closed connection is not made again. The connection is
    // held long enough to count as proven first, so that, were the peer still
    // joined, it would be connected to again within about a second.
    tokio::time::sleep(Duration::from_secs(16)).await;
    client.leave_peer(server_keys.public_key).await?;
    drop(again);
    drop(second);
    assert!(
        tokio::time::timeout(Duration::from_secs(8), conns.recv()).await.is_err(),
        "a peer left was connected to again"
    );

    client.destroy().await?;
    server.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}
