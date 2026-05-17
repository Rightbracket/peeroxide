//! M6.8d: Two Rust HyperDHT nodes complete a Noise IK handshake via PEER_HANDSHAKE.

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{
    run_server, HyperDhtConfig, KeyPair, ServerConfig, spawn,
};
use peeroxide_dht::hyperdht_messages::{
    NoisePayload, FIREWALL_UNKNOWN, PEER_HANDSHAKE,
};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::NoiseWrap;
use peeroxide_dht::Router;
use peeroxide_dht::rpc::{DhtConfig, UserRequestParams};

fn to_noise_kp(kp: &KeyPair) -> NoiseKeypair {
    NoiseKeypair {
        public_key: kp.public_key,
        secret_key: kp.secret_key,
    }
}

#[tokio::test]
async fn two_rust_nodes_noise_ik_handshake() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(30), run_handshake_test()).await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("handshake integration test failed: {e}"),
        Err(_) => panic!("handshake integration test timed out after 30s"),
    }
}

async fn run_handshake_test() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Bootstrap node ─────────────────────────────────────────────────
    let rt = UdxRuntime::new()?;

    let mut bs_dht = DhtConfig::default();
    bs_dht.bootstrap = vec![];
    bs_dht.port = 0;
    bs_dht.host = "127.0.0.1".to_string();
    bs_dht.firewalled = true;

    let mut bs_config = HyperDhtConfig::default();
    bs_config.dht = bs_dht;
    let (_bs_join, bs_handle, _bs_rx) = spawn(&rt, bs_config).await?;
    let bs_port = bs_handle.dht().local_port().await?;
    tracing::info!(bs_port, "bootstrap node ready");

    let bootstrap = vec![format!("127.0.0.1:{bs_port}")];

    // ── 2. Server node ────────────────────────────────────────────────────
    let mut srv_dht = DhtConfig::default();
    srv_dht.bootstrap = bootstrap.clone();
    srv_dht.port = 0;
    srv_dht.host = "127.0.0.1".to_string();
    srv_dht.firewalled = true;

    let mut srv_config = HyperDhtConfig::default();
    srv_config.dht = srv_dht;
    let (_srv_join, srv_handle, srv_rx) = spawn(&rt, srv_config).await?;
    let srv_port = srv_handle.dht().local_port().await?;
    tracing::info!(srv_port, "server node ready");

    let server_kp = KeyPair::generate();
    let target = hash(&server_kp.public_key);

    srv_handle.register_server(&target);

    let server_config = ServerConfig::new(server_kp.clone(), 0);
    let server_rt = UdxRuntime::new()?;
    let server_task = tokio::spawn(run_server(srv_rx, server_config, server_rt));

    // ── 3. Client node ────────────────────────────────────────────────────
    let mut cli_dht = DhtConfig::default();
    cli_dht.bootstrap = bootstrap.clone();
    cli_dht.port = 0;
    cli_dht.host = "127.0.0.1".to_string();
    cli_dht.firewalled = true;

    let mut cli_config = HyperDhtConfig::default();
    cli_config.dht = cli_dht;
    let (_cli_join, cli_handle, _cli_rx) = spawn(&rt, cli_config).await?;

    srv_handle.bootstrapped().await?;
    cli_handle.bootstrapped().await?;
    tracing::info!("both nodes bootstrapped");

    // ── 4. Client-side Noise IK handshake ─────────────────────────────────
    let client_kp = KeyPair::generate();
    let mut nw = NoiseWrap::new_initiator(to_noise_kp(&client_kp), server_kp.public_key);

    let payload = NoisePayload {
        version: 1,
        error: 0,
        firewall: FIREWALL_UNKNOWN,
        holepunch: None,
        addresses4: vec![],
        addresses6: vec![],
        udx: None,
        secret_stream: None,
        relay_through: None,
        relay_addresses: None,
    };
    let noise_bytes = nw.send(&payload)?;

    let srv_peer = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: srv_port,
    };
    let hs_value = Router::encode_client_handshake(noise_bytes, None, Some(srv_peer.clone()))?;

    tracing::info!("sending PEER_HANDSHAKE to server at 127.0.0.1:{srv_port}");
    let mut params = UserRequestParams::default();
    params.command = PEER_HANDSHAKE;
    params.target = Some(target);
    params.value = Some(hs_value);
    let resp = cli_handle
        .dht()
        .request(
            params,
            "127.0.0.1",
            srv_port,
        )
        .await?;

    assert_eq!(resp.error, 0, "server returned error code {}", resp.error);
    let reply_value = resp
        .value
        .expect("server reply should contain handshake value");

    // ── 5. Validate the handshake reply ───────────────────────────────────
    let hs_result = {
        let router = cli_handle
            .router()
            .lock()
            .map_err(|_| "router lock poisoned")?;
        router.validate_handshake_reply(&reply_value, &srv_peer, &resp.from)?
    };

    let remote_payload = nw.recv(&hs_result.noise)?;
    let nw_result = nw.finalize()?;

    tracing::info!("handshake completed successfully");

    // ── 6. Assertions ─────────────────────────────────────────────────────

    assert_eq!(
        nw_result.remote_public_key, server_kp.public_key,
        "remote public key should match server's key"
    );

    assert_eq!(remote_payload.error, 0, "server payload should have no error");

    assert_ne!(
        nw_result.holepunch_secret,
        [0u8; 32],
        "holepunch secret should be non-zero"
    );

    assert_ne!(nw_result.tx, [0u8; 32], "tx key should be non-zero");
    assert_ne!(nw_result.rx, [0u8; 32], "rx key should be non-zero");
    assert_ne!(nw_result.tx, nw_result.rx, "tx and rx keys should differ");

    assert!(nw_result.is_initiator, "client should be initiator");

    tracing::info!(
        remote_pk = hex::encode(nw_result.remote_public_key),
        holepunch_secret = hex::encode(nw_result.holepunch_secret),
        "Noise IK handshake verified"
    );

    // ── Cleanup ───────────────────────────────────────────────────────────
    cli_handle.destroy().await?;
    srv_handle.destroy().await?;
    bs_handle.destroy().await?;

    server_task.abort();
    let _ = server_task.await;

    Ok(())
}
