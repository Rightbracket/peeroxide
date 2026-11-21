//! A connect to a peer that answers the handshake, then never sends its half
//! of the stream's setup, fails within the setup deadline (20 s) rather than
//! waiting for good. The peer's stream is real: it acknowledges what it is
//! sent, so the connecting stream never gives up on it by itself.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use libudx::UdxRuntime;
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{alloc_stream_id, spawn, HyperDhtConfig, KeyPair, ServerEvent};
use peeroxide_dht::hyperdht_messages::{
    encode_handshake_to_bytes, HandshakeMessage, NoisePayload, SecretStreamInfo, UdxInfo,
    FIREWALL_OPEN, MODE_REPLY,
};
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::NoiseWrap;
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
async fn a_connect_to_a_peer_that_never_sets_up_its_stream_fails_in_time() {
    let _ = tracing_subscriber::fmt::try_init();
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("timed out")
        .expect("failed");
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = UdxRuntime::new()?;
    let (_, bs, _) = spawn(&rt, local_dht(vec![])).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];

    let (_, server, mut server_events) = spawn(&rt, local_dht(bootstrap.clone())).await?;
    let server_kp = KeyPair::generate();
    server.register_server(&hash(&server_kp.public_key));
    let server_addr: SocketAddr = format!("127.0.0.1:{}", server.local_port().await?).parse()?;

    // The stalled peer: a Noise reply, then a stream that connects and says
    // nothing.
    let socket = server.server_socket().await?.ok_or("no server socket")?;
    let stream_rt = UdxRuntime::new()?;
    let keys = server_kp.clone();
    // Told once the client's half of the setup has reached the stalled peer:
    // the connect got that far.
    let (setup_seen_tx, setup_seen) = tokio::sync::oneshot::channel();
    let mut setup_seen_tx = Some(setup_seen_tx);
    let stalled = tokio::spawn(async move {
        let mut streams = Vec::new();
        while let Some(event) = server_events.recv().await {
            let ServerEvent::PeerHandshake { msg, from, reply_tx, .. } = event else {
                continue;
            };
            let mut nw = NoiseWrap::new_responder(NoiseKeypair {
                public_key: keys.public_key,
                secret_key: keys.secret_key,
            });
            let remote = nw.recv(&msg.noise).expect("the client's Noise message");
            let local_id = alloc_stream_id();
            let noise = nw
                .send(&NoisePayload {
                    version: 1,
                    error: 0,
                    firewall: FIREWALL_OPEN,
                    holepunch: None,
                    addresses4: vec![],
                    addresses6: vec![],
                    udx: Some(UdxInfo { version: 1, reusable_socket: true, id: u64::from(local_id), seq: 0 }),
                    secret_stream: Some(SecretStreamInfo { version: 1 }),
                    relay_through: None,
                    relay_addresses: None,
                })
                .expect("noise reply");
            nw.finalize().expect("finalize");
            let reply = HandshakeMessage { mode: MODE_REPLY, noise, peer_address: None, relay_address: None };
            let _ = reply_tx.send(encode_handshake_to_bytes(&reply).ok());

            let remote_id = remote.udx.expect("client udx info").id as u32;
            let client: SocketAddr = format!("{}:{}", from.host, from.port).parse().expect("addr");
            let mut stream = stream_rt.create_stream(local_id).await.expect("stream");
            stream.connect(&socket, remote_id, client).await.expect("connect");
            if let Ok(Some(_)) = stream.read().await {
                if let Some(tx) = setup_seen_tx.take() {
                    let _ = tx.send(());
                }
            }
            streams.push(stream);
        }
    });

    let (_, client, _) = spawn(&rt, local_dht(bootstrap)).await?;
    client.bootstrapped().await?;
    let started = Instant::now();
    let result = client.connect_to(&KeyPair::generate(), server_kp.public_key, server_addr, &rt).await;
    let took = started.elapsed();
    let err = match result {
        Ok(_) => panic!("a stream with no setup from its peer was taken as connected"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("stream setup"), "the connect failed otherwise: {err}");
    assert!(setup_seen.await.is_ok(), "the client's setup never reached the stalled peer");
    assert!(took < Duration::from_secs(30), "the connect took {took:?}");

    stalled.abort();
    client.destroy().await?;
    server.destroy().await?;
    bs.destroy().await?;
    Ok(())
}
