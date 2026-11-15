//! A node sets the read buffer limit of its own config
//! (`HyperDhtConfig::read_buffer_limit`) on the streams of its connections,
//! however two peers are joined: the DHT on the connections it makes, and a
//! swarm on those it accepts. Each test gives the server, the client and,
//! where there is one, the relay a limit of its own, and checks each end.
//!
//! - `a_direct_connection_takes_each_side_s_limit`: the client sends its
//!   handshake straight to the server. A second client, with no limit, gets
//!   none.
//! - `a_holepunched_connection_takes_each_side_s_limit`: both are
//!   firewalled, the client finds the server through the DHT and takes no
//!   shortcut to a server on its own host, so the two punch through to each
//!   other.
//! - `a_relayed_connection_takes_each_side_s_limit`: the server has its
//!   connections go through a blind relay (`SwarmConfig::relay_through`).
//! - `the_limit_is_off_by_default`.

use std::net::SocketAddr;
use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle};
use peeroxide_dht::hyperdht::{
    spawn, ConnectOpts, HyperDhtConfig, HyperDhtHandle, KeyPair, PeerConnection,
};
use peeroxide_dht::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_RANDOM};
use peeroxide_dht::relay_service::{run_relay_server, RelayServiceConfig};
use peeroxide_dht::rpc::DhtConfig;
use tokio::sync::mpsc::Receiver;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SERVER_LIMIT: u32 = 65_536;
const CLIENT_LIMIT: u32 = 98_304;
const RELAY_LIMIT: u32 = 131_072;
const TOPIC: [u8; 32] = [0x6c; 32];

fn local_dht(bootstrap: Vec<String>, firewalled: bool, limit: Option<u32>) -> HyperDhtConfig {
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
    config.read_buffer_limit = limit;
    config
}

async fn wait_for<T>(what: &str, mut probe: impl AsyncFnMut() -> Option<T>) -> TestResult<T> {
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

async fn with_timeout(name: &str, fut: impl std::future::Future<Output = TestResult>) {
    let _ = tracing_subscriber::fmt::try_init();
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{name} failed: {e}"),
        Err(_) => panic!("{name} timed out after 60s"),
    }
}

/// A small local testnet: a bootstrap node and three more reachable nodes.
/// They store a server's announce and relay handshakes to it, and give
/// firewalled nodes the samples they classify their NAT by.
struct Testnet {
    rt: UdxRuntime,
    bootstrap: Vec<String>,
    nodes: Vec<HyperDhtHandle>,
}

impl Testnet {
    async fn start() -> TestResult<Self> {
        let rt = UdxRuntime::new()?;
        let (_, bs, _) = spawn(&rt, local_dht(vec![], false, None)).await?;
        let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
        let mut nodes = vec![bs];
        for _ in 0..3 {
            let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), false, None)).await?;
            node.bootstrapped().await?;
            nodes.push(node);
        }
        Ok(Self { rt, bootstrap, nodes })
    }

    /// A firewalled swarm server with a limit of `SERVER_LIMIT`, announced on
    /// the topic and classified, with two relays to reach it through.
    async fn server(
        &self,
        configure: impl FnOnce(&mut SwarmConfig),
    ) -> TestResult<(SwarmHandle, Receiver<SwarmConnection>)> {
        let mut config = SwarmConfig::default();
        config.dht = local_dht(self.bootstrap.clone(), true, Some(SERVER_LIMIT));
        configure(&mut config);
        let (_, server, conns) = spawn_swarm(config).await?;
        server
            .join(TOPIC, {
                let mut opts = JoinOpts::default();
                opts.client = false;
                opts
            })
            .await?;
        server.flush().await?;
        wait_for("the server to classify itself as firewalled", async || {
            let state = server.dht().firewalled().await.ok()?;
            (state == FIREWALL_CONSISTENT || state == FIREWALL_RANDOM).then_some(())
        })
        .await?;
        wait_for("two announce relays", async || {
            (server.dht().current_relays().len() >= 2).then_some(())
        })
        .await?;
        Ok((server, conns))
    }

    /// A firewalled DHT node that connects to the server.
    async fn client(&self, limit: Option<u32>) -> TestResult<HyperDhtHandle> {
        let (_, client, _) = spawn(&self.rt, local_dht(self.bootstrap.clone(), true, limit)).await?;
        client.bootstrapped().await?;
        Ok(client)
    }

    async fn destroy(self) {
        for node in self.nodes {
            let _ = node.destroy().await;
        }
    }
}

async fn address_of(server: &SwarmHandle) -> TestResult<SocketAddr> {
    Ok(format!("127.0.0.1:{}", server.dht().local_port().await?).parse()?)
}

/// The client's connection and the server's end of it carry each its own
/// node's limit, and messages both ways.
async fn check_ends(
    client: &mut PeerConnection,
    server_conns: &mut Receiver<SwarmConnection>,
    client_limit: Option<u32>,
) -> TestResult {
    let mut server = tokio::time::timeout(Duration::from_secs(10), server_conns.recv())
        .await
        .map_err(|_| "timed out waiting for the server's connection")?
        .ok_or("server connection channel closed")?;
    assert_eq!(
        client.stream.get_ref().read_buffer_limit(),
        client_limit,
        "the client's stream takes the client's limit"
    );
    assert_eq!(
        server.peer.stream.get_ref().read_buffer_limit(),
        Some(SERVER_LIMIT),
        "the server's stream takes the server's limit"
    );

    client.stream.write(b"ping").await?;
    let got = tokio::time::timeout(Duration::from_secs(10), server.peer.stream.read())
        .await
        .map_err(|_| "timed out reading on the server")??
        .ok_or("server stream closed")?;
    assert_eq!(got, b"ping");
    server.peer.stream.write(b"pong").await?;
    let got = tokio::time::timeout(Duration::from_secs(10), client.stream.read())
        .await
        .map_err(|_| "timed out reading on the client")??
        .ok_or("client stream closed")?;
    assert_eq!(got, b"pong");
    Ok(())
}

#[tokio::test]
async fn a_direct_connection_takes_each_side_s_limit() {
    with_timeout("direct connection test", run_direct()).await;
}

async fn run_direct() -> TestResult {
    let net = Testnet::start().await?;
    let (server, mut server_conns) = net.server(|_| {}).await?;
    let server_pk = server.key_pair().public_key;
    let server_addr = address_of(&server).await?;

    let client = net.client(Some(CLIENT_LIMIT)).await?;
    let mut conn = client
        .connect_to(&KeyPair::generate(), server_pk, server_addr, &net.rt)
        .await?;
    check_ends(&mut conn, &mut server_conns, Some(CLIENT_LIMIT)).await?;

    let unlimited = net.client(None).await?;
    let mut conn = unlimited
        .connect_to(&KeyPair::generate(), server_pk, server_addr, &net.rt)
        .await?;
    check_ends(&mut conn, &mut server_conns, None).await?;

    let _ = server.destroy().await;
    let _ = client.destroy().await;
    let _ = unlimited.destroy().await;
    net.destroy().await;
    Ok(())
}

#[tokio::test]
async fn a_holepunched_connection_takes_each_side_s_limit() {
    with_timeout("holepunched connection test", run_holepunched()).await;
}

async fn run_holepunched() -> TestResult {
    let net = Testnet::start().await?;
    let (server, mut server_conns) = net.server(|_| {}).await?;

    let client = net.client(Some(CLIENT_LIMIT)).await?;
    let mut opts = ConnectOpts::default();
    opts.local_connection = false;
    let mut conn = client
        .connect_with_options(
            &KeyPair::generate(),
            server.key_pair().public_key,
            &[],
            &net.rt,
            opts,
        )
        .await?;
    check_ends(&mut conn, &mut server_conns, Some(CLIENT_LIMIT)).await?;

    let _ = server.destroy().await;
    let _ = client.destroy().await;
    net.destroy().await;
    Ok(())
}

#[tokio::test]
async fn a_relayed_connection_takes_each_side_s_limit() {
    with_timeout("relayed connection test", run_relayed()).await;
}

async fn run_relayed() -> TestResult {
    let net = Testnet::start().await?;

    // The relay: a reachable node that runs a blind-relay server, with a
    // limit of its own. A peer on its host dials the first private address
    // it is told of: the relay's own address as the network sees it
    // (`remote_address`), before those of this machine's interfaces, which
    // nodes bound to 127.0.0.1 do not reach.
    let (_, relay, relay_events) =
        spawn(&net.rt, local_dht(net.bootstrap.clone(), false, Some(RELAY_LIMIT))).await?;
    relay.bootstrapped().await?;
    let relay_seen_at = wait_for("the relay to learn its own address", async || {
        relay.remote_address().await.ok().flatten()
    })
    .await?;
    let relay_seen_at: SocketAddr =
        format!("{}:{}", relay_seen_at.host, relay_seen_at.port).parse()?;
    let relay_key_pair = KeyPair::generate();
    let _relay_service = run_relay_server(
        net.rt.handle(),
        relay.clone(),
        relay_key_pair.clone(),
        relay_events,
        RelayServiceConfig::default(),
    );

    let relay_addr: SocketAddr = format!("127.0.0.1:{}", relay.local_port().await?).parse()?;
    let (server, mut server_conns) = net
        .server(|config| {
            config.relay_through = Some(relay_key_pair.public_key);
            config.relay_address = Some(relay_addr);
        })
        .await?;
    let server_addr = address_of(&server).await?;

    let client = net.client(Some(CLIENT_LIMIT)).await?;
    let mut conn = client
        .connect_to(&KeyPair::generate(), server.key_pair().public_key, server_addr, &net.rt)
        .await?;
    assert_eq!(
        conn.remote_addr,
        Some(relay_seen_at),
        "the connection should go through the relay"
    );
    check_ends(&mut conn, &mut server_conns, Some(CLIENT_LIMIT)).await?;

    let _ = server.destroy().await;
    let _ = client.destroy().await;
    let _ = relay.destroy().await;
    net.destroy().await;
    Ok(())
}

#[test]
fn the_limit_is_off_by_default() {
    assert_eq!(HyperDhtConfig::default().read_buffer_limit, None);
    assert_eq!(HyperDhtConfig::with_public_bootstrap().read_buffer_limit, None);
    assert_eq!(SwarmConfig::default().dht.read_buffer_limit, None);
    assert_eq!(SwarmConfig::with_public_bootstrap().dht.read_buffer_limit, None);
}
