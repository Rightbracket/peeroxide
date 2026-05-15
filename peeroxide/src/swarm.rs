use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use std::sync::Arc;

use libudx::{RuntimeHandle, UdxRuntime};
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{
    self, HyperDhtConfig, HyperDhtHandle, KeyPair, PeerConnection, ServerEvent,
};
use peeroxide_dht::hyperdht_messages::{
    encode_handshake_to_bytes, HandshakeMessage, NoisePayload, RelayThroughInfo, SecretStreamInfo,
    UdxInfo, MODE_REPLY,
};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::NoiseWrap;
use peeroxide_dht::secret_stream::SecretStream;

use crate::connection_set::{ConnectionInfo, ConnectionSet};
use crate::error::SwarmError;
use crate::peer_discovery::{run_discovery, DiscoveryEvent, PeerDiscoveryConfig};
use crate::peer_info::{PeerInfo, Priority};

static NEXT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

fn next_stream_id() -> u32 {
    NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed)
}

const DEFAULT_MAX_PEERS: usize = 64;
const DEFAULT_MAX_PARALLEL: usize = 3;

// ── Retry backoff tiers (matching Node.js lib/retry-timer.js) ────────────────
// Each tier: [base_ms, jitter1, jitter2, jitter3]
// Delay = base + rand(0..j1) + rand(0..j2) + rand(0..j3)
const BACKOFF_S: [u64; 4] = [1000, 250, 100, 50];
const BACKOFF_M: [u64; 4] = [5000, 1000, 500, 250];
const BACKOFF_L: [u64; 4] = [15000, 5000, 2500, 1000];
const BACKOFF_X: [u64; 4] = [600_000, 60_000, 30_000, 15_000];

fn retry_delay(info: &PeerInfo) -> Duration {
    let idx = if info.proven {
        (info.attempts as usize).min(3)
    } else {
        ((info.attempts + 1) as usize).min(3)
    };
    let tier = match idx {
        0 => &BACKOFF_S,
        1 => &BACKOFF_M,
        2 => &BACKOFF_L,
        _ => &BACKOFF_X,
    };
    let mut rng = rand::rng();
    let jitter = rng.random_range(0..tier[1])
        + rng.random_range(0..tier[2])
        + rng.random_range(0..tier[3]);
    Duration::from_millis(tier[0] + jitter)
}

fn short_hex(bytes: &[u8]) -> String {
    bytes.iter().take(4).fold(String::new(), |mut s, b| {
        use fmt::Write;
        write!(s, "{b:02x}").ok();
        s
    })
}

// ── Public types ─────────────────────────────────────────────────────────────

/// Configuration for a [`Hyperswarm`](SwarmHandle) instance.
#[non_exhaustive]
pub struct SwarmConfig {
    /// Ed25519 key pair. Auto-generated if `None`.
    pub key_pair: Option<KeyPair>,
    /// Underlying HyperDHT configuration.
    pub dht: HyperDhtConfig,
    /// Maximum total peer connections (default 64).
    pub max_peers: usize,
    /// Maximum concurrent outgoing connection attempts (default 3).
    pub max_parallel: usize,
    /// Firewall value sent in handshakes (default 0).
    pub firewall: u64,
    /// Public key of a relay node to force all server connections through.
    /// When set, server handshake replies include `relay_through` info directing
    /// clients to connect via the specified relay using the blind-relay protocol.
    pub relay_through: Option<[u8; 32]>,
    /// Socket address of the relay node. When provided alongside `relay_through`,
    /// the server connects to the relay directly instead of discovering it via DHT.
    pub relay_address: Option<std::net::SocketAddr>,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            key_pair: None,
            dht: HyperDhtConfig::default(),
            max_peers: DEFAULT_MAX_PEERS,
            max_parallel: DEFAULT_MAX_PARALLEL,
            firewall: 0,
            relay_through: None,
            relay_address: None,
        }
    }
}

impl SwarmConfig {
    /// Create a config pre-populated with the public HyperDHT bootstrap nodes.
    pub fn with_public_bootstrap() -> Self {
        Self {
            dht: HyperDhtConfig::with_public_bootstrap(),
            ..Self::default()
        }
    }
}

/// Options for joining a topic.
#[non_exhaustive]
pub struct JoinOpts {
    /// Announce on this topic (server mode).
    pub server: bool,
    /// Look up peers on this topic (client mode).
    pub client: bool,
}

impl Default for JoinOpts {
    fn default() -> Self {
        Self {
            server: true,
            client: true,
        }
    }
}

/// An established swarm connection.
#[non_exhaustive]
pub struct SwarmConnection {
    /// The underlying encrypted peer connection.
    pub peer: PeerConnection,
    /// `true` if we initiated this connection.
    pub is_initiator: bool,
    /// Topic(s) associated with this connection.
    pub topics: Vec<[u8; 32]>,
    _runtime: UdxRuntime,
}

impl SwarmConnection {
    /// Returns the remote peer's static public key.
    pub fn remote_public_key(&self) -> &[u8; 32] {
        &self.peer.remote_public_key
    }
}

impl fmt::Debug for SwarmConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwarmConnection")
            .field("remote_public_key", &short_hex(&self.peer.remote_public_key))
            .field("is_initiator", &self.is_initiator)
            .field("topics", &self.topics.len())
            .finish()
    }
}

/// Clone-able handle for controlling a running Hyperswarm.
#[derive(Clone)]
pub struct SwarmHandle {
    cmd_tx: mpsc::Sender<SwarmCommand>,
    dht: HyperDhtHandle,
    key_pair: KeyPair,
}

impl SwarmHandle {
    /// Access the underlying [`HyperDhtHandle`] for low-level DHT operations.
    ///
    /// This exposes mutable/immutable storage, manual peer lookup, and other
    /// DHT primitives not covered by the high-level swarm API.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use peeroxide::{spawn, discovery_key, JoinOpts, SwarmConfig, KeyPair};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = SwarmConfig::with_public_bootstrap();
    /// let (_task, handle, _conn_rx) = spawn(config).await?;
    ///
    /// // Publish a mutable record under the swarm's own keypair
    /// let kp = handle.key_pair();
    /// handle.dht().mutable_put(kp, b"hello", 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Caveats
    ///
    /// - **Do not call `destroy()`** on the returned handle. The swarm owns
    ///   the DHT lifecycle; destroying it here will break discovery and
    ///   connection establishment.
    /// - **`connect` methods require a `UdxRuntime`** that is not accessible
    ///   from the public API. Use swarm-level topic joins for connection
    ///   establishment instead.
    pub fn dht(&self) -> &HyperDhtHandle {
        &self.dht
    }

    /// The Ed25519 key pair identifying this swarm node.
    ///
    /// This is the same key pair used for topic announcements and Noise
    /// handshakes. It can also be used with [`HyperDhtHandle::mutable_put`]
    /// to publish data that other peers can discover and verify.
    pub fn key_pair(&self) -> &KeyPair {
        &self.key_pair
    }

    /// Join a topic for peer discovery.
    ///
    /// When `opts.server` is true, the swarm announces so other peers can
    /// connect to us. When `opts.client` is true, the swarm looks up peers
    /// and initiates connections.
    pub async fn join(&self, topic: [u8; 32], opts: JoinOpts) -> Result<(), SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::Join {
                topic,
                server: opts.server,
                client: opts.client,
                reply_tx,
            })
            .await
            .map_err(|_| SwarmError::Destroyed)?;
        reply_rx.await.map_err(|_| SwarmError::ChannelClosed)?
    }

    /// Leave a topic, stopping discovery and unannouncing.
    pub async fn leave(&self, topic: [u8; 32]) -> Result<(), SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::Leave { topic, reply_tx })
            .await
            .map_err(|_| SwarmError::Destroyed)?;
        reply_rx.await.map_err(|_| SwarmError::ChannelClosed)?
    }

    /// Wait until all joined topics have completed their initial discovery.
    pub async fn flush(&self) -> Result<(), SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(SwarmCommand::Flush { reply_tx })
            .await
            .map_err(|_| SwarmError::Destroyed)?;
        reply_rx.await.map_err(|_| SwarmError::ChannelClosed)?
    }

    /// Destroy the swarm, cancelling all discovery and closing connections.
    pub async fn destroy(&self) -> Result<(), SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.cmd_tx.send(SwarmCommand::Destroy { reply_tx }).await;
        let _ = reply_rx.await;
        Ok(())
    }
}

// ── Internal types ───────────────────────────────────────────────────────────

enum SwarmCommand {
    Join {
        topic: [u8; 32],
        server: bool,
        client: bool,
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    Leave {
        topic: [u8; 32],
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    Flush {
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
    Destroy {
        reply_tx: oneshot::Sender<Result<(), SwarmError>>,
    },
}

#[allow(dead_code)] // Fields read during leave/unannounce (future)
struct TopicState {
    is_server: bool,
    is_client: bool,
    cancel_tx: Option<oneshot::Sender<()>>,
    refreshed: bool,
}

struct ActorConfig {
    max_peers: usize,
    max_parallel: usize,
    firewall: u64,
    relay_through: Option<[u8; 32]>,
    relay_address: Option<std::net::SocketAddr>,
}

struct SwarmActor {
    key_pair: KeyPair,
    dht: HyperDhtHandle,
    config: ActorConfig,
    runtime_handle: Arc<RuntimeHandle>,

    topics: HashMap<[u8; 32], TopicState>,
    discovery_event_tx: mpsc::UnboundedSender<DiscoveryEvent>,

    peers: HashMap<[u8; 32], PeerInfo>,
    connections: ConnectionSet,
    queue: Vec<[u8; 32]>,

    conn_tx: mpsc::Sender<SwarmConnection>,

    server_registered: bool,
    relay_address: Option<Ipv4Peer>,

    active_connects: usize,
    flush_waiters: Vec<oneshot::Sender<Result<(), SwarmError>>>,
    server_failure_tx: Option<mpsc::UnboundedSender<[u8; 32]>>,
}

struct ConnectAttemptResult {
    public_key: [u8; 32],
    result: Result<(PeerConnection, UdxRuntime), SwarmError>,
}

// ── Spawn ────────────────────────────────────────────────────────────────────

/// Create and start a Hyperswarm instance.
///
/// Returns a background task handle, a control handle, and a receiver
/// that yields each new [`SwarmConnection`].
pub async fn spawn(
    config: SwarmConfig,
) -> Result<(JoinHandle<()>, SwarmHandle, mpsc::Receiver<SwarmConnection>), SwarmError> {
    let key_pair = config.key_pair.unwrap_or_else(KeyPair::generate);
    let runtime = UdxRuntime::new()?;

    let (dht_join, dht, server_rx) = hyperdht::spawn(&runtime, config.dht).await?;
    dht.bootstrapped().await?;

    let local_port = dht.dht().local_port().await?;
    let relay_address = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: local_port,
    };

    tracing::info!(port = local_port, "swarm started");

    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (conn_tx, conn_rx) = mpsc::channel(64);
    let (discovery_event_tx, discovery_event_rx) = mpsc::unbounded_channel();

    let handle_dht = dht.clone();
    let handle_key_pair = key_pair.clone();

    let actor = SwarmActor {
        key_pair,
        dht,
        config: ActorConfig {
            max_peers: config.max_peers,
            max_parallel: config.max_parallel,
            firewall: config.firewall,
            relay_through: config.relay_through,
            relay_address: config.relay_address,
        },
        runtime_handle: runtime.handle(),
        topics: HashMap::new(),
        discovery_event_tx,
        peers: HashMap::new(),
        connections: ConnectionSet::new(),
        queue: Vec::new(),
        conn_tx,
        server_registered: false,
        relay_address: Some(relay_address),
        active_connects: 0,
        flush_waiters: Vec::new(),
        server_failure_tx: None,
    };

    // Keep the DHT runtime alive for the swarm's lifetime.
    // We must await dht_join AFTER actor.run() (which calls dht.destroy()),
    // so the DhtNode finishes closing its IO sockets before we drop the runtime.
    let join = tokio::spawn(async move {
        actor.run(cmd_rx, discovery_event_rx, server_rx).await;
        let _ = dht_join.await;
        drop(runtime);
    });

    let handle = SwarmHandle {
        cmd_tx,
        dht: handle_dht,
        key_pair: handle_key_pair,
    };
    Ok((join, handle, conn_rx))
}

// ── Actor ────────────────────────────────────────────────────────────────────

impl SwarmActor {
    async fn run(
        mut self,
        mut cmd_rx: mpsc::Receiver<SwarmCommand>,
        mut discovery_rx: mpsc::UnboundedReceiver<DiscoveryEvent>,
        mut server_rx: mpsc::UnboundedReceiver<ServerEvent>,
    ) {
        let (connect_result_tx, mut connect_result_rx) =
            mpsc::unbounded_channel::<ConnectAttemptResult>();
        // Reverse channel: spawned create_server_connection tasks signal back
        // on failure so the actor can release the pre-emptively reserved
        // `connections` slot. Without this, a single failed handshake stream
        // would lock that peer's slot forever, blocking any retry from a
        // different relay path.
        let (server_failure_tx, mut server_failure_rx) =
            mpsc::unbounded_channel::<[u8; 32]>();
        self.server_failure_tx = Some(server_failure_tx);

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    if self.handle_command(cmd) {
                        break;
                    }
                }
                event = discovery_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_discovery_event(event, &connect_result_tx);
                    }
                }
                event = server_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_server_event(event);
                    }
                }
                result = connect_result_rx.recv() => {
                    if let Some(result) = result {
                        self.handle_connect_result(result, &connect_result_tx);
                    }
                }
                pk = server_failure_rx.recv() => {
                    if let Some(pk) = pk {
                        if self.connections.remove(&pk) {
                            tracing::debug!(pk = %short_hex(&pk), "server: stream failed, released connection slot");
                        }
                    }
                }
            }
        }

        for (_, state) in self.topics.drain() {
            if let Some(cancel) = state.cancel_tx {
                let _ = cancel.send(());
            }
        }

        if self.server_registered {
            let target = hash(&self.key_pair.public_key);
            self.dht.unregister_server(&target);
            self.server_registered = false;
        }

        let _ = self.dht.destroy().await;
    }

    /// Returns `true` when the actor should shut down.
    fn handle_command(&mut self, cmd: SwarmCommand) -> bool {
        match cmd {
            SwarmCommand::Join {
                topic,
                server,
                client,
                reply_tx,
            } => {
                let result = self.do_join(topic, server, client);
                let _ = reply_tx.send(result);
                false
            }
            SwarmCommand::Leave { topic, reply_tx } => {
                let result = self.do_leave(topic);
                let _ = reply_tx.send(result);
                false
            }
            SwarmCommand::Flush { reply_tx } => {
                if self.all_topics_refreshed() {
                    let _ = reply_tx.send(Ok(()));
                } else {
                    self.flush_waiters.push(reply_tx);
                }
                false
            }
            SwarmCommand::Destroy { reply_tx } => {
                if self.server_registered {
                    let target = hash(&self.key_pair.public_key);
                    self.dht.unregister_server(&target);
                    self.server_registered = false;
                }
                let _ = reply_tx.send(Ok(()));
                true
            }
        }
    }

    fn do_join(&mut self, topic: [u8; 32], server: bool, client: bool) -> Result<(), SwarmError> {
        if self.topics.contains_key(&topic) {
            return Ok(());
        }

        if server && !self.server_registered {
            let target = hash(&self.key_pair.public_key);
            self.dht.register_server(&target);
            self.server_registered = true;
            tracing::debug!(pk = %short_hex(&self.key_pair.public_key), "server registered");
        }

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let relay_addresses = self
            .relay_address
            .as_ref()
            .map_or_else(Vec::new, |a| vec![a.clone()]);

        tokio::spawn(run_discovery(
            PeerDiscoveryConfig {
                topic,
                is_server: server,
                is_client: client,
            },
            self.dht.clone(),
            self.key_pair.clone(),
            relay_addresses,
            self.discovery_event_tx.clone(),
            cancel_rx,
        ));

        self.topics.insert(
            topic,
            TopicState {
                is_server: server,
                is_client: client,
                cancel_tx: Some(cancel_tx),
                refreshed: false,
            },
        );
        Ok(())
    }

    fn do_leave(&mut self, topic: [u8; 32]) -> Result<(), SwarmError> {
        if let Some(state) = self.topics.remove(&topic) {
            if let Some(cancel) = state.cancel_tx {
                let _ = cancel.send(());
            }
            for peer in self.peers.values_mut() {
                peer.topics.retain(|t| *t != topic);
            }

            if state.is_server && self.server_registered {
                let has_remaining_server_topics =
                    self.topics.values().any(|t| t.is_server);
                if !has_remaining_server_topics {
                    let target = hash(&self.key_pair.public_key);
                    self.dht.unregister_server(&target);
                    self.server_registered = false;
                    tracing::debug!(
                        pk = %short_hex(&self.key_pair.public_key),
                        "server unregistered (no remaining server topics)"
                    );
                }
            }
        }
        Ok(())
    }

    fn handle_discovery_event(
        &mut self,
        event: DiscoveryEvent,
        connect_result_tx: &mpsc::UnboundedSender<ConnectAttemptResult>,
    ) {
        match event {
            DiscoveryEvent::PeerFound {
                public_key,
                relay_addresses,
                topic,
            } => {
                if public_key == self.key_pair.public_key {
                    return;
                }
                if self.connections.has(&public_key) {
                    return;
                }
                if self.connections.len() >= self.config.max_peers {
                    return;
                }

                let info = self
                    .peers
                    .entry(public_key)
                    .or_insert_with(|| PeerInfo::new(public_key, relay_addresses.clone()));

                if !relay_addresses.is_empty() {
                    info.relay_addresses = relay_addresses;
                }
                if !info.topics.contains(&topic) {
                    info.topics.push(topic);
                }

                if !info.queued && !info.connecting && !info.banned && !info.is_waiting() {
                    info.queued = true;
                    info.priority = info.get_priority();
                    self.queue.push(public_key);
                    self.attempt_connections(connect_result_tx);
                }
            }
            DiscoveryEvent::RefreshComplete { topic } => {
                if let Some(state) = self.topics.get_mut(&topic) {
                    state.refreshed = true;
                }
                self.check_flush_waiters();
            }
        }
    }

    fn attempt_connections(
        &mut self,
        connect_result_tx: &mpsc::UnboundedSender<ConnectAttemptResult>,
    ) {
        while self.active_connects < self.config.max_parallel && !self.queue.is_empty() {
            // Sort by priority descending
            self.queue.sort_by(|a, b| {
                let pa = self
                    .peers
                    .get(a)
                    .map_or(Priority::VeryLow, |i| i.priority);
                let pb = self
                    .peers
                    .get(b)
                    .map_or(Priority::VeryLow, |i| i.priority);
                pb.cmp(&pa)
            });

            let pk = self.queue.remove(0);
            let relay_addrs = if let Some(info) = self.peers.get_mut(&pk) {
                info.queued = false;
                info.connecting = true;
                info.attempts += 1;
                info.relay_addresses.clone()
            } else {
                vec![]
            };

            self.active_connects += 1;
            let dht = self.dht.clone();
            let key_pair = self.key_pair.clone();
            let result_tx = connect_result_tx.clone();
            let rh = self.runtime_handle.clone();

            tokio::spawn(async move {
                let conn_runtime = UdxRuntime::shared(rh);
                tracing::debug!(pk = %short_hex(&pk), "connecting to peer");
                match dht
                    .connect_with_nodes(&key_pair, pk, &relay_addrs, &conn_runtime)
                    .await
                {
                    Ok(conn) => {
                        tracing::debug!(pk = %short_hex(&pk), "peer connected");
                        let _ = result_tx.send(ConnectAttemptResult {
                            public_key: pk,
                            result: Ok((conn, conn_runtime)),
                        });
                    }
                    Err(e) => {
                        tracing::debug!(pk = %short_hex(&pk), err = %e, "peer connect failed");
                        let _ = result_tx.send(ConnectAttemptResult {
                            public_key: pk,
                            result: Err(SwarmError::Dht(e)),
                        });
                    }
                }
            });
        }
    }

    fn handle_connect_result(
        &mut self,
        result: ConnectAttemptResult,
        connect_result_tx: &mpsc::UnboundedSender<ConnectAttemptResult>,
    ) {
        self.active_connects = self.active_connects.saturating_sub(1);

        if let Some(info) = self.peers.get_mut(&result.public_key) {
            info.connecting = false;
        }

        match result.result {
            Ok((conn, runtime)) => {
                let pk = result.public_key;

                // Dedup: compare public keys to decide tie-break
                if self.connections.has(&pk) {
                    let we_are_dominant = self.key_pair.public_key > pk;
                    if let Some(existing) = self.connections.get(&pk) {
                        if existing.is_initiator == we_are_dominant {
                            tracing::debug!(pk = %short_hex(&pk), "dedup: keeping existing");
                            return;
                        }
                    }
                    self.connections.remove(&pk);
                }

                self.connections
                    .add(pk, ConnectionInfo { is_initiator: true });

                let topics = if let Some(info) = self.peers.get_mut(&pk) {
                    info.connected();
                    info.topics.clone()
                } else {
                    vec![]
                };

                let swarm_conn = SwarmConnection {
                    peer: conn,
                    is_initiator: true,
                    topics,
                    _runtime: runtime,
                };
                if self.conn_tx.try_send(swarm_conn).is_err() {
                    tracing::warn!("connection channel full, dropping connection");
                }
            }
            Err(e) => {
                tracing::debug!(pk = %short_hex(&result.public_key), err = %e, "connect failed");
                self.schedule_retry(result.public_key);
            }
        }

        self.attempt_connections(connect_result_tx);
    }

    fn schedule_retry(&mut self, pk: [u8; 32]) {
        let Some(info) = self.peers.get_mut(&pk) else {
            return;
        };
        if info.banned || info.topics.is_empty() {
            return;
        }

        let delay = retry_delay(info);
        info.set_waiting(true);

        let relay_addresses = info.relay_addresses.clone();
        let topic = info.topics[0];
        let event_tx = self.discovery_event_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = event_tx.send(DiscoveryEvent::PeerFound {
                public_key: pk,
                relay_addresses,
                topic,
            });
        });
    }

    fn handle_server_event(&mut self, event: ServerEvent) {
        match event {
            ServerEvent::PeerHandshake {
                msg,
                from,
                peer_address,
                target: _,
                reply_tx,
            } => {
                self.handle_server_handshake(msg, from, peer_address, reply_tx);
            }
            ServerEvent::PeerHolepunch { reply_tx, .. } => {
                // Holepunch handling requires libudx incoming-stream support.
                // Acknowledge without creating a connection.
                let _ = reply_tx.send(None);
            }
            _ => {}
        }
    }

    fn handle_server_handshake(
        &mut self,
        msg: HandshakeMessage,
        from: Ipv4Peer,
        peer_address: Option<Ipv4Peer>,
        reply_tx: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        let client_address = peer_address.clone().unwrap_or_else(|| from.clone());
        let is_forwarded = peer_address.is_some();

        if is_forwarded {
            tracing::debug!(
                relay = %format!("{}:{}", from.host, from.port),
                peer = %format!("{}:{}", client_address.host, client_address.port),
                "server handshake: forwarded — dialing peer_address"
            );
        }

        let noise_kp = NoiseKeypair {
            public_key: self.key_pair.public_key,
            secret_key: self.key_pair.secret_key,
        };

        let mut nw = NoiseWrap::new_responder(noise_kp);

        let remote_payload = match nw.recv(&msg.noise) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(err = %e, "server handshake: noise recv failed");
                let _ = reply_tx.send(None);
                return;
            }
        };

        if remote_payload.error != 0 {
            let _ = reply_tx.send(None);
            return;
        }

        let local_stream_id = next_stream_id();

        let (relay_token, relay_through_info) = if let Some(relay_pk) = self.config.relay_through {
            let token: [u8; 32] = rand::random();
            let info = RelayThroughInfo {
                version: 1,
                public_key: relay_pk,
                token,
            };
            (Some(token), Some(info))
        } else {
            (None, None)
        };

        let reply_payload = NoisePayload {
            version: 1,
            error: 0,
            firewall: self.config.firewall,
            holepunch: None,
            addresses4: vec![],
            addresses6: vec![],
            udx: Some(UdxInfo {
                version: 1,
                reusable_socket: true,
                id: u64::from(local_stream_id),
                seq: 0,
            }),
            secret_stream: Some(SecretStreamInfo { version: 1 }),
            relay_through: relay_through_info,
            relay_addresses: self.config.relay_address.map(|addr| {
                vec![peeroxide_dht::messages::Ipv4Peer {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                }]
            }),
        };

        let noise_reply = match nw.send(&reply_payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(err = %e, "server handshake: noise send failed");
                let _ = reply_tx.send(None);
                return;
            }
        };

        let nw_result = match nw.finalize() {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(err = %e, "server handshake: noise finalize failed");
                let _ = reply_tx.send(None);
                return;
            }
        };

        let reply_msg = HandshakeMessage {
            mode: MODE_REPLY,
            noise: noise_reply,
            peer_address: None,
            relay_address: None,
        };
        let _ = reply_tx.send(encode_handshake_to_bytes(&reply_msg).ok());

        let remote_pk = nw_result.remote_public_key;

        if self.connections.has(&remote_pk) {
            tracing::debug!(pk = %short_hex(&remote_pk), "server: already connected");
            return;
        }
        if self.connections.len() >= self.config.max_peers {
            tracing::debug!("server: at max connections");
            return;
        }

        let remote_udx = match remote_payload.udx {
            Some(u) => u,
            None => {
                tracing::debug!("server: no UDX info in handshake");
                return;
            }
        };

        self.connections
            .add(remote_pk, ConnectionInfo { is_initiator: false });

        let conn_tx = self.conn_tx.clone();

        if let (Some(relay_pk), Some(token)) = (self.config.relay_through, relay_token) {
            let dht = self.dht.clone();
            let key_pair = self.key_pair.clone();
            let relay_addr = self.config.relay_address;
            let rh = self.runtime_handle.clone();
            let failure_tx = self.server_failure_tx.clone();
            tokio::spawn(async move {
                match create_server_relay_connection(
                    rh,
                    dht,
                    key_pair,
                    relay_pk,
                    relay_addr,
                    token,
                    local_stream_id,
                    nw_result,
                )
                .await
                {
                    Ok((conn, runtime)) => {
                        let swarm_conn = SwarmConnection {
                            peer: conn,
                            is_initiator: false,
                            topics: vec![],
                            _runtime: runtime,
                        };
                        if conn_tx.send(swarm_conn).await.is_err() {
                            tracing::warn!("connection channel closed");
                        }
                    }
                    Err(e) => {
                        tracing::debug!(err = %e, "server: relay connection failed");
                        if let Some(tx) = failure_tx {
                            let _ = tx.send(remote_pk);
                        }
                    }
                }
            });
        } else {
            let rh = self.runtime_handle.clone();
            let dht = self.dht.clone();
            let client_addr_for_task = client_address.clone();
            let failure_tx = self.server_failure_tx.clone();
            tokio::spawn(async move {
                match create_server_connection(rh, dht, local_stream_id, &remote_udx, &client_addr_for_task, &nw_result)
                    .await
                {
                    Ok((conn, runtime)) => {
                        let swarm_conn = SwarmConnection {
                            peer: conn,
                            is_initiator: false,
                            topics: vec![],
                            _runtime: runtime,
                        };
                        if conn_tx.send(swarm_conn).await.is_err() {
                            tracing::warn!("connection channel closed");
                        }
                    }
                    Err(e) => {
                        tracing::debug!(err = %e, "server: stream establishment failed");
                        if let Some(tx) = failure_tx {
                            let _ = tx.send(remote_pk);
                        }
                    }
                }
            });
        }
    }

    fn all_topics_refreshed(&self) -> bool {
        !self.topics.is_empty() && self.topics.values().all(|t| t.refreshed)
    }

    fn check_flush_waiters(&mut self) {
        if self.all_topics_refreshed() {
            for waiter in self.flush_waiters.drain(..) {
                let _ = waiter.send(Ok(()));
            }
        }
    }
}

async fn create_server_connection(
    runtime_handle: Arc<RuntimeHandle>,
    dht: HyperDhtHandle,
    local_stream_id: u32,
    remote_udx: &UdxInfo,
    client_address: &Ipv4Peer,
    noise_result: &peeroxide_dht::noise_wrap::NoiseWrapResult,
) -> Result<(PeerConnection, UdxRuntime), SwarmError> {
    let runtime = UdxRuntime::shared(runtime_handle);

    let remote_id = u32::try_from(remote_udx.id).map_err(|_| {
        SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
            "remote UDX id out of u32 range".into(),
        ))
    })?;

    let addr: std::net::SocketAddr = std::net::SocketAddr::new(
        client_address.host.parse().map_err(|_| {
            SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
                "invalid client address".into(),
            ))
        })?,
        client_address.port,
    );

    let socket = dht
        .listen_socket()
        .await
        .map_err(SwarmError::Dht)?
        .ok_or_else(|| {
            SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
                "DHT listen socket not available".into(),
            ))
        })?;

    let stream = runtime.create_stream(local_stream_id).await?;
    stream.connect(&socket, remote_id, addr).await?;

    let async_stream = stream.into_async_stream();
    let ss = SecretStream::from_session(
        false,
        async_stream,
        noise_result.tx,
        noise_result.rx,
        noise_result.handshake_hash,
        noise_result.remote_public_key,
    )
    .await
    .map_err(|e| SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::SecretStream(e)))?;

    let conn = PeerConnection::with_remote_addr(ss, noise_result.remote_public_key, addr, socket, None);
    Ok((conn, runtime))
}

#[allow(clippy::too_many_arguments)]
async fn create_server_relay_connection(
    runtime_handle: Arc<RuntimeHandle>,
    dht: HyperDhtHandle,
    key_pair: KeyPair,
    relay_pk: [u8; 32],
    relay_addr: Option<std::net::SocketAddr>,
    token: [u8; 32],
    local_stream_id: u32,
    noise_result: peeroxide_dht::noise_wrap::NoiseWrapResult,
) -> Result<(PeerConnection, UdxRuntime), SwarmError> {
    use peeroxide_dht::blind_relay::BlindRelayClient;
    use peeroxide_dht::protomux::Mux;

    let runtime = UdxRuntime::shared(runtime_handle);

    // 1. HyperDHT connection to the relay node (control channel).
    // Use direct address when available, fall back to DHT routing.
    let connect_fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<PeerConnection, peeroxide_dht::hyperdht::HyperDhtError>> + Send>> =
        if let Some(addr) = relay_addr {
            tracing::debug!(?addr, "server: connecting to relay at known address");
            Box::pin(dht.connect_to(&key_pair, relay_pk, addr, &runtime))
        } else {
            Box::pin(dht.connect(&key_pair, relay_pk, &runtime))
        };
    let relay_conn = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        connect_fut,
    )
    .await
    .map_err(|_| {
        SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::HandshakeFailed(
            "relay connect timeout".into(),
        ))
    })?
    .map_err(SwarmError::Dht)?;

    let relay_addr = relay_conn.remote_addr.ok_or_else(|| {
        SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
            "relay connection has no remote_addr".into(),
        ))
    })?;

    // 2. Protomux over the control channel.
    let (mux, mux_run) = Mux::new(relay_conn.stream);
    let mux_task = tokio::spawn(mux_run);

    // 3. Open blind-relay client + pair as initiator (server initiates pairing).
    // Channel id = our public key (must match relay server's `id: socket.remotePublicKey`).
    let mut relay_client = BlindRelayClient::open(&mux, Some(key_pair.public_key.to_vec()))
        .await
        .map_err(|e| SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::Relay(e)))?;
    relay_client
        .wait_opened()
        .await
        .map_err(|e| SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::Relay(e)))?;

    let pair_response = relay_client
        .pair(true, &token, u64::from(local_stream_id))
        .await
        .map_err(|e| SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::Relay(e)))?;

    let remote_id = u32::try_from(pair_response.remote_id).map_err(|_| {
        SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
            "relay remote_id out of u32 range".into(),
        ))
    })?;

    // 4. Connect data UDX stream through the relay, reusing the control
    //    channel's socket so the relay sees traffic from the same source address.
    let data_stream = runtime.create_stream(local_stream_id).await?;
    data_stream
        .connect(&relay_conn.socket, remote_id, relay_addr)
        .await?;

    // 5. Wrap with SecretStream using the original Noise keys.
    // Server is responder (is_initiator=false) in the Noise handshake.
    let async_stream = data_stream.into_async_stream();
    let ss = SecretStream::from_session(
        false,
        async_stream,
        noise_result.tx,
        noise_result.rx,
        noise_result.handshake_hash,
        noise_result.remote_public_key,
    )
    .await
    .map_err(|e| SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::SecretStream(e)))?;

    let conn = PeerConnection::with_remote_addr(
        ss,
        noise_result.remote_public_key,
        relay_addr,
        relay_conn.socket,
        Some(mux_task),
    );
    Ok((conn, runtime))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_first_attempt_unproven() {
        let mut info = PeerInfo::new([0u8; 32], vec![]);
        info.attempts = 0;
        let d = retry_delay(&info);
        // Tier M: 5000..6750 ms (unproven, idx = min(0+1, 3) = 1)
        assert!(d.as_millis() >= 5000);
        assert!(d.as_millis() < 7000);
    }

    #[test]
    fn retry_delay_first_attempt_proven() {
        let mut info = PeerInfo::new([0u8; 32], vec![]);
        info.attempts = 0;
        info.proven = true;
        let d = retry_delay(&info);
        // Tier S: 1000..1400 ms (proven, idx = min(0, 3) = 0)
        assert!(d.as_millis() >= 1000);
        assert!(d.as_millis() < 1500);
    }

    #[test]
    fn retry_delay_many_attempts() {
        let mut info = PeerInfo::new([0u8; 32], vec![]);
        info.attempts = 10;
        let d = retry_delay(&info);
        // Tier X: 600_000..705_000 ms (idx capped at 3)
        assert!(d.as_millis() >= 600_000);
        assert!(d.as_millis() < 710_000);
    }

    #[test]
    fn short_hex_format() {
        let bytes = [0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33];
        assert_eq!(short_hex(&bytes), "deadbeef");
    }

    #[test]
    fn default_config() {
        let c = SwarmConfig::default();
        assert!(c.key_pair.is_none());
        assert_eq!(c.max_peers, 64);
        assert_eq!(c.max_parallel, 3);
        assert_eq!(c.firewall, 0);
    }

    #[test]
    fn default_join_opts() {
        let j = JoinOpts::default();
        assert!(j.server);
        assert!(j.client);
    }

    #[tokio::test]
    async fn server_unregistered_on_leave_last_topic() {
        let config = SwarmConfig::default();

        let (_task, handle, _conn_rx) = crate::spawn(config).await.unwrap();
        let target = hash(&handle.key_pair().public_key);
        let topic = [0xAA; 32];

        handle.join(topic, JoinOpts { server: true, client: false }).await.unwrap();

        {
            let router = handle.dht().router().lock().unwrap();
            assert!(
                router.get(&target).is_some(),
                "server must be registered after join"
            );
        }

        handle.leave(topic).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        {
            let router = handle.dht().router().lock().unwrap();
            assert!(
                router.get(&target).is_none(),
                "server must be unregistered after leaving last server topic"
            );
        }

        handle.destroy().await.unwrap();
    }

    #[tokio::test]
    async fn server_unregistered_on_destroy() {
        let config = SwarmConfig::default();

        let (_task, handle, _conn_rx) = crate::spawn(config).await.unwrap();
        let target = hash(&handle.key_pair().public_key);
        let topic = [0xBB; 32];

        handle.join(topic, JoinOpts { server: true, client: false }).await.unwrap();

        {
            let router = handle.dht().router().lock().unwrap();
            assert!(
                router.get(&target).is_some(),
                "server must be registered after join"
            );
        }

        handle.destroy().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        {
            let router = handle.dht().router().lock().unwrap();
            assert!(
                router.get(&target).is_none(),
                "server must be unregistered after destroy"
            );
        }
    }

    #[tokio::test]
    async fn server_unregistered_on_handle_drop() {
        let config = SwarmConfig::default();

        let (task, handle, _conn_rx) = crate::spawn(config).await.unwrap();
        let dht_handle = handle.dht().clone();
        let target = hash(&handle.key_pair().public_key);
        let topic = [0xCC; 32];

        handle.join(topic, JoinOpts { server: true, client: false }).await.unwrap();

        {
            let router = dht_handle.router().lock().unwrap();
            assert!(
                router.get(&target).is_some(),
                "server must be registered after join"
            );
        }

        drop(handle);
        drop(_conn_rx);
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;

        {
            let router = dht_handle.router().lock().unwrap();
            assert!(
                router.get(&target).is_none(),
                "server must be unregistered after implicit shutdown (handle drop)"
            );
        }
    }
}
