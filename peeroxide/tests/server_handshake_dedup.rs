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
//! - `rejected_relayed_handshake_is_not_cached_or_staged`: a server with no
//!   free connection slot neither caches its replies nor keeps the passive
//!   holepunches it staged for them.
//! - `rust_client_connects_directly_to_firewalled_server`: a Rust client sends
//!   its handshake straight to a firewalled swarm server (no relay). The
//!   server now dials such a client, as Node does; this guards that Rust to
//!   Rust direct connections still work with that dial (data flows both ways).

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle};
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

/// A firewalled swarm server on a small local testnet, ready to be reached
/// through two of its announce relays.
struct RelayedTestnet {
    _rt: UdxRuntime,
    bs: peeroxide_dht::hyperdht::HyperDhtHandle,
    nodes: Vec<peeroxide_dht::hyperdht::HyperDhtHandle>,
    server: SwarmHandle,
    server_conns: tokio::sync::mpsc::Receiver<SwarmConnection>,
    client: peeroxide_dht::hyperdht::HyperDhtHandle,
    relays: Vec<RelayInfo>,
    server_pk: [u8; 32],
}

impl RelayedTestnet {
    async fn start(max_peers: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = UdxRuntime::new()?;

        // Reachable DHT nodes: they store the server's announce, so they
        // relay handshakes to it, and they give the firewalled server the
        // NAT samples it needs to classify itself.
        let (_, bs, _) = spawn(&rt, local_dht(vec![], false)).await?;
        let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
        let mut nodes = Vec::new();
        for _ in 0..3 {
            let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), false)).await?;
            node.bootstrapped().await?;
            nodes.push(node);
        }

        let mut config = SwarmConfig::default();
        config.dht = local_dht(bootstrap.clone(), true);
        config.max_peers = max_peers;
        let (_, server, server_conns) = spawn_swarm(config).await?;
        server
            .join([0x5b; 32], {
                let mut opts = JoinOpts::default();
                opts.client = false;
                opts
            })
            .await?;
        server.flush().await?;
        let server_pk = server.key_pair().public_key;

        wait_for("the server to classify itself as firewalled", async || {
            let state = server.dht().firewalled().await.ok()?;
            (state == FIREWALL_CONSISTENT || state == FIREWALL_RANDOM).then_some(())
        })
        .await?;
        let relays = wait_for("two announce relays", async || relays_of(&server).await).await?;

        let (_, client, _) = spawn(&rt, local_dht(bootstrap, true)).await?;
        client.bootstrapped().await?;

        Ok(Self {
            _rt: rt,
            bs,
            nodes,
            server,
            server_conns,
            client,
            relays,
            server_pk,
        })
    }

    fn noise_initiator(&self) -> NoiseWrap {
        let client_kp = KeyPair::generate();
        NoiseWrap::new_initiator(
            NoiseKeypair {
                public_key: client_kp.public_key,
                secret_key: client_kp.secret_key,
            },
            self.server_pk,
        )
    }

    async fn request(
        &self,
        command: u64,
        value: Vec<u8>,
        relay: usize,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut params = UserRequestParams::default();
        params.command = command;
        params.target = Some(hash(&self.server_pk));
        params.value = Some(value);
        let addr: &Ipv4Peer = &self.relays[relay].relay_address;
        let resp = self.client.dht().request(params, &addr.host, addr.port).await?;
        assert_eq!(resp.error, 0, "relay {addr:?} returned error code {}", resp.error);
        Ok(resp.value)
    }

    /// Send one handshake through each of two relays, the second after the
    /// first has been answered (and, if accepted, its holepunch staged).
    async fn handshake_twice(
        &self,
        request: &[u8],
    ) -> Result<[Vec<u8>; 2], Box<dyn std::error::Error>> {
        let mut replies = Vec::new();
        for relay in 0..2 {
            let value = self
                .request(PEER_HANDSHAKE, request.to_vec(), relay)
                .await?
                .ok_or("handshake reply without a value")?;
            replies.push(decode_handshake_from_bytes(&value)?.noise);
        }
        Ok([replies.remove(0), replies.remove(0)])
    }

    /// Send one PEER_HOLEPUNCH probe for session `id` and return the
    /// decrypted reply, or `None` if the server did not answer.
    async fn probe_holepunch(
        &self,
        id: u64,
        secure: &SecurePayload,
    ) -> Result<Option<HolepunchPayload>, Box<dyn std::error::Error>> {
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
        let value = Router::encode_client_holepunch(id, secure.encrypt(&probe)?, None)?;
        let reply = match self.request(PEER_HOLEPUNCH, value, 0).await {
            Ok(Some(v)) => v,
            Ok(None) | Err(_) => return Ok(None),
        };
        let reply = decode_holepunch_msg_from_bytes(&reply)?;
        assert_eq!(reply.id, id);
        Ok(Some(secure.decrypt(&reply.payload)?))
    }

    async fn destroy(self) -> Result<(), Box<dyn std::error::Error>> {
        self.server.destroy().await?;
        self.client.destroy().await?;
        for node in self.nodes {
            node.destroy().await?;
        }
        self.bs.destroy().await?;
        Ok(())
    }
}

async fn with_timeout(name: &str, fut: impl std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>) {
    let _ = tracing_subscriber::fmt::try_init();
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{name} failed: {e}"),
        Err(_) => panic!("{name} timed out after 60s"),
    }
}

#[tokio::test]
async fn duplicate_through_two_relays_keeps_the_staged_holepunch() {
    with_timeout("relayed duplicate handshake test", run_relayed()).await;
}

async fn run_relayed() -> Result<(), Box<dyn std::error::Error>> {
    let net = RelayedTestnet::start(64).await?;
    let mut nw = net.noise_initiator();
    let request = Router::encode_client_handshake(nw.send(&client_noise_payload())?, None, None)?;

    let replies = net.handshake_twice(&request).await?;
    assert_eq!(
        replies[0], replies[1],
        "both relayed copies of one handshake must get the same Noise reply"
    );

    let payload = nw.recv(&replies[0])?;
    assert_eq!(payload.error, 0);
    let holepunch = payload
        .holepunch
        .ok_or("a firewalled server should offer a holepunch to a firewalled client")?;
    assert!(!holepunch.relays.is_empty());
    let noise = nw.finalize()?;

    // The session staged for the first copy must still exist.
    let secure = SecurePayload::new(noise.holepunch_secret);
    let reply = net
        .probe_holepunch(holepunch.id, &secure)
        .await?
        .ok_or("no holepunch reply: the first handshake's session was replaced")?;
    assert_eq!(reply.error, ERROR_NONE);
    assert_eq!(reply.remote_token, Some([7; 32]));

    net.destroy().await
}

#[tokio::test]
async fn rejected_relayed_handshake_is_not_cached_or_staged() {
    with_timeout("rejected relayed handshake test", run_rejected()).await;
}

/// A server with no free connection slot (`max_peers = 0`) rejects every
/// handshake. Its replies must not be cached, and it must not keep the
/// passive holepunch it staged for them.
async fn run_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let net = RelayedTestnet::start(0).await?;
    let mut nw = net.noise_initiator();
    let request = Router::encode_client_handshake(nw.send(&client_noise_payload())?, None, None)?;

    let replies = net.handshake_twice(&request).await?;
    assert_ne!(
        replies[0], replies[1],
        "a rejected handshake's reply must not be cached: each copy gets a fresh responder"
    );

    // A single, separate handshake: nothing replaces its session, so only
    // the rejection itself can have released it.
    let mut nw = net.noise_initiator();
    let request = Router::encode_client_handshake(nw.send(&client_noise_payload())?, None, None)?;
    let reply = net
        .request(PEER_HANDSHAKE, request, 0)
        .await?
        .ok_or("handshake reply without a value")?;
    let payload = nw.recv(&decode_handshake_from_bytes(&reply)?.noise)?;
    let holepunch = payload.holepunch.ok_or("the reply should still carry a holepunch")?;
    let noise = nw.finalize()?;

    let secure = SecurePayload::new(noise.holepunch_secret);
    assert!(
        net.probe_holepunch(holepunch.id, &secure).await?.is_none(),
        "a rejected handshake must not keep a staged holepunch session"
    );

    net.destroy().await
}

#[tokio::test]
async fn rust_client_connects_directly_to_firewalled_server() {
    with_timeout("direct Rust client test", run_direct_rust_client()).await;
}

async fn run_direct_rust_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut net = RelayedTestnet::start(64).await?;
    let server_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", net.server.dht().local_port().await?).parse()?;

    let mut client_conn = net
        .client
        .connect_to(&KeyPair::generate(), net.server_pk, server_addr, &net._rt)
        .await?;
    let mut server_conn = tokio::time::timeout(Duration::from_secs(10), net.server_conns.recv())
        .await
        .map_err(|_| "timed out waiting for the server's connection")?
        .ok_or("server connection channel closed")?;
    assert!(!server_conn.is_initiator);

    client_conn.stream.write(b"ping").await?;
    let got = tokio::time::timeout(Duration::from_secs(10), server_conn.peer.stream.read())
        .await
        .map_err(|_| "timed out reading on the server")??
        .ok_or("server stream closed")?;
    assert_eq!(got, b"ping");

    server_conn.peer.stream.write(b"pong").await?;
    let got = tokio::time::timeout(Duration::from_secs(10), client_conn.stream.read())
        .await
        .map_err(|_| "timed out reading on the client")??
        .ok_or("client stream closed")?;
    assert_eq!(got, b"pong");

    net.destroy().await
}
