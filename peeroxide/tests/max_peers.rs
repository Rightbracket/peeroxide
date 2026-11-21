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

#[tokio::test]
async fn a_dropped_connection_frees_its_slot_for_the_same_peer() {
    let _ = tracing_subscriber::fmt::try_init();
    tokio::time::timeout(Duration::from_secs(120), reconnect())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let rt = libudx::UdxRuntime::new()?;
    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], false)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = Vec::new();
    for _ in 0..3 {
        let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), false)).await?;
        node.bootstrapped().await?;
        nodes.push(node);
    }
    let topic = [0x72; 32];

    // A server with room for one peer.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), false);
    config.max_peers = 1;
    let (_server_task, server, mut server_rx) = spawn_swarm(config).await?;
    let mut opts = JoinOpts::default();
    opts.client = false;
    server.join(topic, opts).await?;
    server.flush().await?;

    let key_pair = peeroxide::KeyPair::generate();
    for round in 0..2 {
        let mut config = SwarmConfig::default();
        config.dht = local_dht(bootstrap.clone(), false);
        config.key_pair = Some(key_pair.clone());
        let (_task, client, mut client_rx) = spawn_swarm(config).await?;
        let mut opts = JoinOpts::default();
        opts.server = false;
        client.join(topic, opts).await?;

        let accepted = tokio::time::timeout(Duration::from_secs(30), server_rx.recv())
            .await
            .map_err(|_| format!("round {round}: the server accepted no connection"))?
            .ok_or("connection channel closed")?;
        assert_eq!(accepted.remote_public_key(), &key_pair.public_key);
        let dialed = tokio::time::timeout(Duration::from_secs(30), client_rx.recv())
            .await
            .map_err(|_| format!("round {round}: the client made no connection"))?
            .ok_or("connection channel closed")?;

        // Close the connection at both ends; the server's slot is free again.
        drop(accepted);
        drop(dialed);
        client.destroy().await?;
    }

    server.destroy().await?;
    for node in &nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}

#[tokio::test]
async fn a_handshake_whose_stream_never_comes_up_gives_its_slot_back() {
    let _ = tracing_subscriber::fmt::try_init();
    tokio::time::timeout(Duration::from_secs(90), stalled_handshake())
        .await
        .expect("timed out")
        .expect("failed");
}

/// A client completes a handshake with a server that has room for one peer,
/// then never sends the stream its first packet. The handshake says it came
/// through a relay, so the server listens for that packet rather than dialling
/// back, and its stream, sending nothing, never gives up on its own. The
/// server must give the slot back within its setup deadline (10 s).
async fn stalled_handshake() -> Result<(), Box<dyn std::error::Error>> {
    use peeroxide_dht::crypto::hash;
    use peeroxide_dht::hyperdht::KeyPair;
    use peeroxide_dht::hyperdht_messages::{NoisePayload, UdxInfo, FIREWALL_UNKNOWN, PEER_HANDSHAKE};
    use peeroxide_dht::messages::Ipv4Peer;
    use peeroxide_dht::noise::Keypair as NoiseKeypair;
    use peeroxide_dht::noise_wrap::NoiseWrap;
    use peeroxide_dht::rpc::UserRequestParams;
    use peeroxide_dht::Router;

    let rt = libudx::UdxRuntime::new()?;
    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], false)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), false);
    config.max_peers = 1;
    let (_server_task, server, mut server_rx) = spawn_swarm(config).await?;
    let mut opts = JoinOpts::default();
    opts.client = false;
    server.join([0x73; 32], opts).await?;
    let server_pk = server.key_pair().public_key;
    let server_port = server.dht().local_port().await?;

    let (_client_join, client, _client_rx) = spawn(&rt, local_dht(bootstrap, false)).await?;
    client.bootstrapped().await?;

    // The stalled client: a handshake straight to the server, which takes the
    // slot and waits for a stream that is never opened.
    let stalled = KeyPair::generate();
    let mut nw = NoiseWrap::new_initiator(
        NoiseKeypair {
            public_key: stalled.public_key,
            secret_key: stalled.secret_key,
        },
        server_pk,
    );
    let noise = nw.send(&NoisePayload {
        version: 1,
        error: 0,
        firewall: FIREWALL_UNKNOWN,
        holepunch: None,
        addresses4: vec![],
        addresses6: vec![],
        udx: Some(UdxInfo {
            version: 1,
            reusable_socket: false,
            id: 1,
            seq: 0,
        }),
        secret_stream: None,
        relay_through: None,
        relay_addresses: None,
    })?;
    let mut params = UserRequestParams::default();
    params.command = PEER_HANDSHAKE;
    params.target = Some(hash(&server_pk));
    let client_address = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: client.local_port().await?,
    };
    params.value = Some(Router::encode_client_handshake(noise, Some(client_address), None)?);
    let resp = client.dht().request(params, "127.0.0.1", server_port).await?;
    assert_eq!(resp.error, 0);
    assert!(resp.value.is_some(), "the server did not answer the handshake");
    let stalled_at = tokio::time::Instant::now();

    // A real peer, retrying until the slot is free.
    let server_addr = format!("127.0.0.1:{server_port}").parse()?;
    let peer = KeyPair::generate();
    let _conn = loop {
        let attempt = tokio::time::timeout(
            Duration::from_secs(5),
            client.connect_to(&peer, server_pk, server_addr, &rt),
        );
        if let Ok(Ok(conn)) = attempt.await {
            break conn;
        }
        if stalled_at.elapsed() > Duration::from_secs(30) {
            return Err("the stalled handshake still holds the server's only slot".into());
        }
    };
    let accepted = tokio::time::timeout(Duration::from_secs(10), server_rx.recv())
        .await
        .map_err(|_| "the server delivered no connection")?
        .ok_or("connection channel closed")?;
    assert_eq!(accepted.remote_public_key(), &peer.public_key);

    server.destroy().await?;
    client.destroy().await?;
    bs.destroy().await?;
    Ok(())
}
