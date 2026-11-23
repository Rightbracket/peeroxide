//! Only the holder of a key may take, or drop, the DHT's handshake routing
//! and peer record for that key's target (`hash(public_key)`). Another key
//! announcing or unannouncing under that target must not make the server
//! unreachable (#69).

use std::time::Duration;

use peeroxide::{discovery_key, spawn as spawn_swarm, JoinOpts, KeyPair, SwarmConfig};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig, HyperDhtHandle};
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

fn server_only() -> JoinOpts {
    let mut opts = JoinOpts::default();
    opts.server = true;
    opts.client = false;
    opts
}

async fn local_network(
    rt: &libudx::UdxRuntime,
) -> Result<(Vec<String>, Vec<HyperDhtHandle>), Box<dyn std::error::Error>> {
    let (_, bs, _) = spawn(rt, local_dht(vec![])).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = vec![bs];
    for _ in 0..2 {
        let (_, node, _) = spawn(rt, local_dht(bootstrap.clone())).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }
    Ok((bootstrap, nodes))
}

#[tokio::test]
async fn an_impostor_announcing_a_servers_target_does_not_take_its_handshakes() {
    tokio::time::timeout(Duration::from_secs(90), impostor_announce())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn impostor_announce() -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (bootstrap, nodes) = local_network(&rt).await?;

    let server_keys = KeyPair::generate();
    let target = discovery_key(&server_keys.public_key);
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    config.key_pair = Some(server_keys.clone());
    let (_, server, mut accepted) = spawn_swarm(config).await?;
    server.join(target, server_only()).await?;
    server.flush().await?;

    // Another key announces under the server's own target, after the server.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    let (_, impostor, _impostor_conns) = spawn_swarm(config).await?;
    impostor.join(target, server_only()).await?;
    impostor.flush().await?;

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone());
    let (_, client, mut conns) = spawn_swarm(config).await?;
    client.join_peer(server_keys.public_key).await?;

    let conn = tokio::time::timeout(Duration::from_secs(20), conns.recv())
        .await
        .map_err(|_| "the client never connected: its handshake went to the impostor")?
        .ok_or("connection channel closed")?;
    assert_eq!(conn.remote_public_key(), &server_keys.public_key);
    tokio::time::timeout(Duration::from_secs(10), accepted.recv())
        .await
        .map_err(|_| "the server delivered no connection")?
        .ok_or("connection channel closed")?;

    client.destroy().await?;
    impostor.destroy().await?;
    server.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    Ok(())
}

#[tokio::test]
async fn an_impostor_unannouncing_a_servers_target_does_not_remove_its_record() {
    tokio::time::timeout(Duration::from_secs(60), impostor_unannounce())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn impostor_unannounce() -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (bootstrap, nodes) = local_network(&rt).await?;
    let (_, peer, _) = spawn(&rt, local_dht(bootstrap.clone())).await?;
    peer.bootstrapped().await?;

    let server_keys = KeyPair::generate();
    let target = discovery_key(&server_keys.public_key);
    let announced = peer.announce(target, &server_keys, &[]).await?;
    assert!(announced.acknowledged > 0, "no node took the self-announce");
    assert!(
        peer.find_peer(target).await?.is_some(),
        "the self-announce was not found"
    );

    // Another key, with a valid signature of its own, unannounces the target.
    peer.unannounce(target, &KeyPair::generate()).await?;

    let found = peer
        .find_peer(target)
        .await?
        .ok_or("the server's record was removed by another key's unannounce")?;
    assert_eq!(found.public_key, server_keys.public_key);

    peer.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    Ok(())
}
