//! A swarm server answers every copy of a PEER_HANDSHAKE with the same reply.
//!
//! Node hyperdht clients send one Noise handshake message through up to two
//! relays in parallel and act on the first reply that arrives (`lib/connect.js`),
//! so a server must answer each copy identically, as Node's
//! `Server._onpeerhandshake` does.
//!
//! - `duplicate_peer_handshake_gets_the_same_reply`: a Rust client sends the
//!   same message straight to the server twice and checks that both replies
//!   are byte-identical and usable.
//! - `duplicate_through_two_relays_keeps_the_staged_holepunch`: the same
//!   message reaches a firewalled server through two of its relays, so the
//!   first copy stages a passive holepunch. The second copy must not replace
//!   that session: a PEER_HOLEPUNCH for the first reply's id must still be
//!   answered.

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig, SwarmHandle};
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig, KeyPair};
use peeroxide_dht::hyperdht_messages::{
    decode_handshake_from_bytes, decode_holepunch_msg_from_bytes, HolepunchPayload, NoisePayload,
    RelayInfo, UdxInfo, ERROR_NONE, FIREWALL_CONSISTENT, FIREWALL_RANDOM, FIREWALL_UNKNOWN,
    PEER_HANDSHAKE, PEER_HOLEPUNCH,
};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::NoiseWrap;
use peeroxide_dht::rpc::{DhtConfig, UserRequestParams};
use peeroxide_dht::secure_payload::SecurePayload;
use peeroxide_dht::Router;

fn local_dht(bootstrap: Vec<String>, firewalled: bool) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = firewalled;
    if !firewalled {
        // A reachable node joins the routing table at once, as the Node
        // testnet nodes (`ephemeral: false`) do.
        dht.ephemeral = Some(false);
    }
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

#[tokio::test]
async fn duplicate_peer_handshake_gets_the_same_reply() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(30), run()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("duplicate handshake test failed: {e}"),
        Err(_) => panic!("duplicate handshake test timed out after 30s"),
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = UdxRuntime::new()?;

    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], true)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];

    // The server: a swarm announcing a topic.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), true);
    let (_task, server, _conn_rx) = spawn_swarm(config).await?;
    server
        .join([0x5a; 32], {
            let mut opts = JoinOpts::default();
            opts.client = false;
            opts
        })
        .await?;
    let server_pk = server.key_pair().public_key;
    let server_addr = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: server.dht().local_port().await?,
    };

    // The client: one Noise message, sent twice.
    let (_cli_join, client, _cli_rx) = spawn(&rt, local_dht(bootstrap, true)).await?;
    client.bootstrapped().await?;

    let client_kp = KeyPair::generate();
    let mut nw = NoiseWrap::new_initiator(
        NoiseKeypair {
            public_key: client_kp.public_key,
            secret_key: client_kp.secret_key,
        },
        server_pk,
    );
    let noise = nw.send(&client_noise_payload())?;
    let request = Router::encode_client_handshake(noise, None, Some(server_addr.clone()))?;

    let mut replies = Vec::new();
    for _ in 0..2 {
        let mut params = UserRequestParams::default();
        params.command = PEER_HANDSHAKE;
        params.target = Some(hash(&server_pk));
        params.value = Some(request.clone());
        let resp = client
            .dht()
            .request(params, &server_addr.host, server_addr.port)
            .await?;
        assert_eq!(resp.error, 0, "server returned error code {}", resp.error);
        replies.push(resp.value.ok_or("handshake reply without a value")?);
    }

    let first = decode_handshake_from_bytes(&replies[0])?;
    let second = decode_handshake_from_bytes(&replies[1])?;
    assert_eq!(
        first.noise, second.noise,
        "both copies of one handshake must get the same Noise reply"
    );

    // And that reply completes the client's handshake with the server's key.
    let payload = nw.recv(&first.noise)?;
    assert_eq!(payload.error, 0);
    assert_eq!(nw.finalize()?.remote_public_key, server_pk);

    server.destroy().await?;
    client.destroy().await?;
    bs.destroy().await?;
    Ok(())
}

fn client_noise_payload() -> NoisePayload {
    NoisePayload {
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
    }
}

async fn wait_for<T>(
    what: &str,
    mut probe: impl AsyncFnMut() -> Option<T>,
) -> Result<T, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(v) = probe().await {
            return Ok(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn relays_of(server: &SwarmHandle) -> Option<Vec<RelayInfo>> {
    let relays = server.dht().current_relays();
    (relays.len() >= 2).then_some(relays)
}

#[tokio::test]
async fn duplicate_through_two_relays_keeps_the_staged_holepunch() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(60), run_relayed()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("relayed duplicate handshake test failed: {e}"),
        Err(_) => panic!("relayed duplicate handshake test timed out after 60s"),
    }
}

async fn run_relayed() -> Result<(), Box<dyn std::error::Error>> {
    let rt = UdxRuntime::new()?;

    // Reachable DHT nodes: they store the server's announce, so they relay
    // handshakes to it, and they give the firewalled server the NAT samples
    // it needs to classify itself.
    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], false)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
    let mut nodes = Vec::new();
    for _ in 0..3 {
        let (join, node, rx) = spawn(&rt, local_dht(bootstrap.clone(), false)).await?;
        node.bootstrapped().await?;
        nodes.push((join, node, rx));
    }

    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), true);
    let (_task, server, _conn_rx) = spawn_swarm(config).await?;
    server
        .join([0x5b; 32], {
            let mut opts = JoinOpts::default();
            opts.client = false;
            opts
        })
        .await?;
    server.flush().await?;
    let server_pk = server.key_pair().public_key;
    let target = hash(&server_pk);

    wait_for("the server to classify itself as firewalled", async || {
        let state = server.dht().firewalled().await.ok()?;
        (state == FIREWALL_CONSISTENT || state == FIREWALL_RANDOM).then_some(())
    })
    .await?;
    let relays = wait_for("two announce relays", async || relays_of(&server).await).await?;

    let (_cli_join, client, _cli_rx) = spawn(&rt, local_dht(bootstrap, true)).await?;
    client.bootstrapped().await?;

    let client_kp = KeyPair::generate();
    let mut nw = NoiseWrap::new_initiator(
        NoiseKeypair {
            public_key: client_kp.public_key,
            secret_key: client_kp.secret_key,
        },
        server_pk,
    );
    let request = Router::encode_client_handshake(nw.send(&client_noise_payload())?, None, None)?;

    // One copy through each relay, the second after the first has been
    // answered and its passive holepunch staged.
    let mut replies = Vec::new();
    for relay in &relays[..2] {
        let mut params = UserRequestParams::default();
        params.command = PEER_HANDSHAKE;
        params.target = Some(target);
        params.value = Some(request.clone());
        let addr = &relay.relay_address;
        let resp = client.dht().request(params, &addr.host, addr.port).await?;
        assert_eq!(resp.error, 0, "relay {addr:?} returned error code {}", resp.error);
        replies.push(decode_handshake_from_bytes(
            &resp.value.ok_or("handshake reply without a value")?,
        )?);
    }
    assert_eq!(
        replies[0].noise, replies[1].noise,
        "both relayed copies of one handshake must get the same Noise reply"
    );

    let payload = nw.recv(&replies[0].noise)?;
    assert_eq!(payload.error, 0);
    let holepunch = payload
        .holepunch
        .ok_or("a firewalled server should offer a holepunch to a firewalled client")?;
    assert!(!holepunch.relays.is_empty());
    let noise = nw.finalize()?;

    // The session staged for the first copy must still exist.
    let secure = SecurePayload::new(noise.holepunch_secret);
    let probe = HolepunchPayload {
        error: ERROR_NONE,
        firewall: FIREWALL_UNKNOWN,
        round: 0,
        connected: false,
        punching: false,
        addresses: Some(vec![]),
        remote_address: None,
        token: Some([7; 32]),
        remote_token: None,
    };
    let value = Router::encode_client_holepunch(holepunch.id, secure.encrypt(&probe)?, None)?;
    let mut params = UserRequestParams::default();
    params.command = PEER_HOLEPUNCH;
    params.target = Some(target);
    params.value = Some(value);
    let addr: &Ipv4Peer = &relays[0].relay_address;
    let resp = client.dht().request(params, &addr.host, addr.port).await?;
    assert_eq!(resp.error, 0, "holepunch returned error code {}", resp.error);
    let reply = decode_holepunch_msg_from_bytes(
        &resp
            .value
            .ok_or("no holepunch reply: the first handshake's session was replaced")?,
    )?;
    assert_eq!(reply.id, holepunch.id);
    let reply_payload = secure.decrypt(&reply.payload)?;
    assert_eq!(reply_payload.error, ERROR_NONE);
    assert_eq!(reply_payload.remote_token, Some([7; 32]));

    server.destroy().await?;
    client.destroy().await?;
    for (_, node, _) in nodes {
        node.destroy().await?;
    }
    bs.destroy().await?;
    Ok(())
}
