//! A peer that the network can reach takes the shortcuts the Node.js version
//! takes with one.
//!
//! - `an_open_server_puts_its_own_address_first_in_its_topic_record`: a swarm
//!   server that isn't firewalled lists the address the network sees it at
//!   first among the addresses its topic record gives for a handshake, as the
//!   Node.js version's announcer does, so a peer can send its handshake
//!   straight to it. Its first record has it, though the nodes that accept its
//!   announce are only known after that record is stored.
//! - `an_open_client_waits_for_a_firewalled_server_to_dial_it`: a client that
//!   isn't firewalled tells a firewalled swarm server so, and waits for the
//!   server to dial it instead of holepunching; data then flows both ways. The
//!   server dials an open client and stages no holepunch, and a client that
//!   doesn't say it is open isn't dialled, so the connection is made only if
//!   both sides do their part. The interop tests cover a Node.js server, whose
//!   counters also show who dialled.

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig, SwarmHandle};
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig, HyperDhtHandle, KeyPair};
use peeroxide_dht::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_RANDOM};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::rpc::DhtConfig;

/// A DHT node on loopback.
fn local_dht(bootstrap: Vec<String>, firewalled: bool, ephemeral: bool) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = firewalled;
    dht.ephemeral = Some(ephemeral);
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

fn server_opts() -> JoinOpts {
    let mut opts = JoinOpts::default();
    opts.client = false;
    opts
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

async fn with_timeout(name: &str, fut: impl std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>) {
    let _ = tracing_subscriber::fmt::try_init();
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{name} failed: {e}"),
        Err(_) => panic!("{name} timed out after 60s"),
    }
}

/// A bootstrap node and three more, all reachable and persistent, as the
/// Node testnet nodes (`ephemeral: false`) are: they store announces, relay
/// handshakes, and tell the nodes that join where they see them.
struct Testnet {
    rt: UdxRuntime,
    nodes: Vec<HyperDhtHandle>,
    bootstrap: Vec<String>,
}

impl Testnet {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let rt = UdxRuntime::new()?;
        let (_, bs, _) = spawn(&rt, local_dht(vec![], false, false)).await?;
        let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];
        let mut nodes = vec![bs];
        for _ in 0..3 {
            let (_, node, _) = spawn(&rt, local_dht(bootstrap.clone(), false, false)).await?;
            node.bootstrapped().await?;
            nodes.push(node);
        }
        Ok(Self { rt, nodes, bootstrap })
    }

    async fn destroy(self) -> Result<(), Box<dyn std::error::Error>> {
        for node in self.nodes {
            node.destroy().await?;
        }
        Ok(())
    }
}

/// The address a node on the testnet is seen at, once the nodes it hears
/// from agree on it.
async fn own_address(dht: &HyperDhtHandle) -> Result<Ipv4Peer, Box<dyn std::error::Error>> {
    let own = wait_for("the node to learn its own address", async || {
        dht.remote_address().await.ok().flatten()
    })
    .await?;
    let expected = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: dht.local_port().await?,
    };
    assert_eq!(own, expected, "a node on loopback is seen where it listens");
    Ok(own)
}

#[tokio::test]
async fn an_open_server_puts_its_own_address_first_in_its_topic_record() {
    with_timeout("own address test", run_own_address_first()).await;
}

async fn run_own_address_first() -> Result<(), Box<dyn std::error::Error>> {
    let net = Testnet::start().await?;

    let mut config = SwarmConfig::default();
    config.dht = local_dht(net.bootstrap.clone(), false, false);
    let (_, server, _conns) = spawn_swarm(config).await?;
    let own = own_address(server.dht()).await?;

    let topic = [0x6c; 32];
    server.join(topic, server_opts()).await?;
    server.flush().await?;

    let server_pk = server.key_pair().public_key;
    let lookup = &net.nodes[1];
    let record = wait_for("the server's record on its topic", async || {
        let results = lookup.lookup(topic).await.ok()?;
        results
            .into_iter()
            .flat_map(|result| result.peers)
            .find(|peer| peer.public_key == server_pk)
    })
    .await?;
    assert_eq!(
        record.relay_addresses.first(),
        Some(&own),
        "the record should give the server's own address first: {:?}",
        record.relay_addresses
    );

    server.destroy().await?;
    net.destroy().await
}

/// A firewalled swarm server that can be holepunched through its relays.
async fn firewalled_server(
    net: &Testnet,
) -> Result<(SwarmHandle, tokio::sync::mpsc::Receiver<peeroxide::SwarmConnection>), Box<dyn std::error::Error>> {
    let mut config = SwarmConfig::default();
    config.dht = local_dht(net.bootstrap.clone(), true, true);
    let (_, server, conns) = spawn_swarm(config).await?;
    server.join([0x5c; 32], server_opts()).await?;
    server.flush().await?;
    wait_for("the server to classify itself as firewalled", async || {
        let state = server.dht().firewalled().await.ok()?;
        (state == FIREWALL_CONSISTENT || state == FIREWALL_RANDOM).then_some(())
    })
    .await?;
    wait_for("the server's holepunch relays", async || {
        (!server.dht().current_relays().is_empty()).then_some(())
    })
    .await?;
    Ok((server, conns))
}

#[tokio::test]
async fn an_open_client_waits_for_a_firewalled_server_to_dial_it() {
    with_timeout("open client test", run_open_client()).await;
}

async fn run_open_client() -> Result<(), Box<dyn std::error::Error>> {
    let net = Testnet::start().await?;
    let (server, mut server_conns) = firewalled_server(&net).await?;

    // Reachable, and ephemeral: it doesn't store the server's announce, and
    // reaches the server through the nodes that do.
    let (_, client, _) = spawn(&net.rt, local_dht(net.bootstrap.clone(), false, true)).await?;
    client.bootstrapped().await?;
    own_address(&client).await?;

    let mut client_conn = client
        .connect(&KeyPair::generate(), server.key_pair().public_key, &net.rt)
        .await?;
    let mut server_conn = tokio::time::timeout(Duration::from_secs(10), server_conns.recv())
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

    client.destroy().await?;
    server.destroy().await?;
    net.destroy().await
}
