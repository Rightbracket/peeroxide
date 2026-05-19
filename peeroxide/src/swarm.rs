use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use std::sync::Arc;

use libudx::{RuntimeHandle, UdxRuntime, UdxSocket, UdxStream};
use peeroxide_dht::crypto::hash;
use peeroxide_dht::holepuncher::{HolepunchEvent, Holepuncher};
use peeroxide_dht::hyperdht::{
    self, HyperDhtConfig, HyperDhtHandle, KeyPair, PeerConnection, ServerEvent,
};
use peeroxide_dht::hyperdht_messages::{
    encode_handshake_to_bytes, encode_holepunch_msg_to_bytes, HandshakeMessage, HolepunchInfo,
    HolepunchMessage, HolepunchPayload, NoisePayload, RelayInfo, RelayThroughInfo,
    SecretStreamInfo, UdxInfo, ERROR_NONE, FIREWALL_CONSISTENT, FIREWALL_RANDOM, FIREWALL_UNKNOWN,
    MODE_FROM_RELAY, MODE_FROM_SECOND_RELAY, MODE_FROM_SERVER, MODE_REPLY,
};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::{NoiseWrap, NoiseWrapResult};
use peeroxide_dht::secret_stream::SecretStream;
use peeroxide_dht::secure_payload::SecurePayload;
use peeroxide_dht::socket_pool::SocketPool;

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
    /// Enable the same-NAT LAN-shortcut (default `true`). When `false`,
    /// receivers ignore the loopback/private addresses advertised in the
    /// server's `addresses4` and always dial the public IP (= FE-holder's
    /// peer_address tag). Mirrors Node hyperdht `opts.localConnection`.
    ///
    /// Tests that want to exercise the real network path (without leaning
    /// on same-host loopback) should set this to `false`.
    pub local_connection: bool,
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
            local_connection: true,
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
    local_connection: bool,
}

struct SwarmActor {
    key_pair: KeyPair,
    dht: HyperDhtHandle,
    config: ActorConfig,
    runtime_handle: Arc<RuntimeHandle>,
    local_port: u16,

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

    // Passive-holepunch state. When the server is firewalled, each accepted
    // handshake stages an InFlightHolepunch entry instead of immediately
    // creating a UDX connection. The entry persists across PEER_HOLEPUNCH
    // rounds (preserving SecurePayload tokens + the Holepuncher's NAT
    // analysis) and is resolved into a real SwarmConnection only when the
    // firewall hook on the puncher's primary socket commits the 4-tuple.
    connects: HashMap<u64, InFlightHolepunch>,
    pending_handshakes: HashMap<[u8; 32], u64>,
    next_holepunch_id: u64,
    passive_hp_event_tx: mpsc::UnboundedSender<PassiveHolepunchEvent>,
}

/// State carried between the initial server handshake and the moment the
/// passive Holepuncher's primary socket sees its first probe. Owned
/// directly by `SwarmActor` (single-threaded mutation, no `Arc<Mutex>`).
struct InFlightHolepunch {
    remote_pk: [u8; 32],
    /// Persists across rounds so `token()` derivation is stable per Node
    /// `lib/holepuncher.js`.
    payload: SecurePayload,
    /// `None` once `punch()` has been spawned (the task takes ownership).
    /// Round handling between spawn and punch-land only updates state that
    /// can be reconstructed from the incoming round.
    puncher: Option<Holepuncher>,
    /// Listening-mode UDX stream whose `set_firewall_hook` will commit the
    /// 4-tuple when the first probe lands.
    udx_stream: UdxStream,
    /// Clone of the puncher's primary `UdxSocket`. Held independently so
    /// the live UDP socket survives even after `puncher` is moved into a
    /// spawned `punch()` task, and so we have something to hand to
    /// `PeerConnection::new` when finalizing.
    udx_socket: UdxSocket,
    /// Kept verbatim from the original handshake; consumed by `SecretStream::from_session`
    /// when the punch lands.
    noise_result: NoiseWrapResult,
    /// Highest round number we have accepted so far. Used purely to detect
    /// reordered PEER_HOLEPUNCH duplicates; replies always echo
    /// `remote_hp.round` to stay in lockstep with the client loop.
    round: u64,
    /// 10s deadline. Cancelled when the firewall hook fires or when a new
    /// handshake from the same remote_pk preempts this entry.
    abort_task: Option<JoinHandle<()>>,
    /// Pre-computed addresses advertised in PEER_HOLEPUNCH reply
    /// `addresses`. MUST point at OUR puncher socket, not echo back the
    /// initiator's reflexive address (Phase 3 MVP: loopback only).
    local_punch_addrs: Vec<Ipv4Peer>,
}

/// Internal actor-loop events fired by the passive-holepunch path.
/// Decoupled from `SwarmCommand` because the firewall-hook closure must be
/// `FnOnce + Send + 'static` and cannot await the public command channel.
enum PassiveHolepunchEvent {
    /// Firewall hook fired — the puncher's primary socket has bound a
    /// remote 4-tuple at `addr`. Time to finalize the SecretStream.
    Punched { id: u64, addr: SocketAddr },
    /// 10s deadline elapsed without the hook firing.
    Abort { id: u64 },
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
    // Fetch the actual port of the socket the server-side firewall hook
    // listens on (matches `dht.server_socket()` = primary_socket port). This
    // is what we must advertise in `addresses4` so the receiver dials the
    // same UDP socket where the sender's UDX demux is registered. For
    // firewalled nodes (default) this is the client_socket port, NOT the
    // dht `local_port` (which is the server_socket port).
    let primary_socket_port = match dht.server_socket().await? {
        Some(s) => match s.local_addr().await {
            Ok(addr) => addr.port(),
            Err(_) => local_port,
        },
        None => local_port,
    };
    tracing::info!(
        target: "peeroxide::_events::swarm::started",
        port = local_port,
        primary_port = primary_socket_port,
        "swarm started"
    );

    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let (conn_tx, conn_rx) = mpsc::channel(64);
    let (discovery_event_tx, discovery_event_rx) = mpsc::unbounded_channel();
    let (passive_hp_event_tx, passive_hp_event_rx) = mpsc::unbounded_channel();

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
            local_connection: config.local_connection,
        },
        runtime_handle: runtime.handle(),
        local_port: primary_socket_port,
        topics: HashMap::new(),
        discovery_event_tx,
        peers: HashMap::new(),
        connections: ConnectionSet::new(),
        queue: Vec::new(),
        conn_tx,
        server_registered: false,
        relay_address: config.relay_address.map(|addr| Ipv4Peer {
            host: addr.ip().to_string(),
            port: addr.port(),
        }),
        active_connects: 0,
        flush_waiters: Vec::new(),
        server_failure_tx: None,
        connects: HashMap::new(),
        pending_handshakes: HashMap::new(),
        next_holepunch_id: 0,
        passive_hp_event_tx,
    };

    // Keep the DHT runtime alive for the swarm's lifetime.
    // We must await dht_join AFTER actor.run() (which calls dht.destroy()),
    // so the DhtNode finishes closing its IO sockets before we drop the runtime.
    let join = tokio::spawn(async move {
        actor.run(cmd_rx, discovery_event_rx, server_rx, passive_hp_event_rx).await;
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
        mut passive_hp_event_rx: mpsc::UnboundedReceiver<PassiveHolepunchEvent>,
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
                        self.handle_server_event(event).await;
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
                event = passive_hp_event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_passive_holepunch_event(event).await;
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
                    // Defer the dial attempt until RefreshComplete to avoid a
                    // queue-time relay-snapshot race. A single lookup typically
                    // yields multiple PeerFound events per peer (one per
                    // responding FE-holder) and relay_addresses accumulates
                    // across them. Spawning the dial on the first event would
                    // snapshot a partial list; the spawned task can't pick up
                    // later relay additions. RefreshComplete fires once all
                    // PeerFound events for a refresh have been processed, so
                    // info.relay_addresses is at its widest by then.
                }
            }
            DiscoveryEvent::RefreshComplete { topic } => {
                if let Some(state) = self.topics.get_mut(&topic) {
                    state.refreshed = true;
                }
                self.attempt_connections(connect_result_tx);
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
            let mut connect_opts = peeroxide_dht::hyperdht::ConnectOpts::default();
            connect_opts.local_connection = self.config.local_connection;

            tokio::spawn(async move {
                let conn_runtime = UdxRuntime::shared(rh);
                tracing::debug!(pk = %short_hex(&pk), "connecting to peer");
                match dht
                    .connect_with_options(&key_pair, pk, &relay_addrs, &conn_runtime, connect_opts)
                    .await
                {
                    Ok(conn) => {
                        tracing::info!(
                            target: "peeroxide::_events::peer::connected",
                            pk = %short_hex(&pk),
                            "peer connected"
                        );
                        let _ = result_tx.send(ConnectAttemptResult {
                            public_key: pk,
                            result: Ok((conn, conn_runtime)),
                        });
                    }
                    Err(e) => {
                        tracing::info!(
                            target: "peeroxide::_events::peer::connect_failed",
                            pk = %short_hex(&pk),
                            err = %e,
                            "peer connect failed"
                        );
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

    async fn handle_server_event(&mut self, event: ServerEvent) {
        match event {
            ServerEvent::PeerHandshake {
                msg,
                from,
                peer_address,
                target: _,
                reply_tx,
            } => {
                self.handle_server_handshake(msg, from, peer_address, reply_tx).await;
            }
            ServerEvent::PeerHolepunch {
                msg,
                from: _,
                peer_address,
                target: _,
                reply_tx,
            } => {
                self.handle_peer_holepunch(msg, peer_address, reply_tx).await;
            }
            _ => {}
        }
    }

    async fn handle_server_handshake(
        &mut self,
        msg: HandshakeMessage,
        from: Ipv4Peer,
        peer_address: Option<Ipv4Peer>,
        reply_tx: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        let initial_client_address = peer_address.clone().unwrap_or_else(|| from.clone());
        let is_forwarded = peer_address.is_some();

        if is_forwarded {
            tracing::debug!(
                relay = %format!("{}:{}", from.host, from.port),
                peer = %format!("{}:{}", initial_client_address.host, initial_client_address.port),
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

        let client_address = initial_client_address;

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

        // Decide whether this reply will stage a passive holepunch:
        //   - the DHT has settled on a firewalled classification
        //     (CONSISTENT or RANDOM — not OPEN, not UNKNOWN); UNKNOWN
        //     means NAT analysis hasn't gathered enough samples to be
        //     sure, in which case the existing direct path is the safer
        //     choice (Node hyperdht has the same gate), AND
        //   - we are not forcing all server connections through a relay
        //     (relay_through bypasses NAT traversal entirely).
        let firewall_state = self.dht.firewalled().await.unwrap_or(FIREWALL_UNKNOWN);
        let attempt_holepunch = matches!(firewall_state, FIREWALL_CONSISTENT | FIREWALL_RANDOM)
            && relay_through_info.is_none();

        // Try to stage the passive Holepuncher up front so the reply we
        // send in this handshake can advertise the puncher's bound port
        // and the holepunch session id. If staging fails, fall back to a
        // reply with `holepunch: None` (the existing direct path); a
        // genuinely firewalled receiver will then time out and retry via
        // discovery rather than getting stuck on a broken half-state.
        let holepunch_setup = if attempt_holepunch {
            match self
                .try_setup_passive_holepunch(
                    remote_payload.firewall,
                    local_stream_id,
                    remote_payload.udx.as_ref(),
                )
                .await
            {
                Ok(setup) => Some(setup),
                Err(e) => {
                    tracing::debug!(err = %e, "passive holepunch setup failed; sending direct-path reply");
                    None
                }
            }
        } else {
            None
        };

        // addresses4 advertised in our noise reply: real LAN interfaces
        // (Node parity, `hyperdht/lib/server.js:277-284`). The port we
        // pair with each address is the puncher socket's bound port when
        // a passive holepunch is staged — that's where the firewall hook
        // is registered, so same-host receivers dialling it land on a
        // real punch. Otherwise advertise the DHT primary socket port.
        let advertise_port = holepunch_setup
            .as_ref()
            .map_or(self.local_port, |s| s.puncher_port);
        let addresses4 = self.dht.noise_addresses4(advertise_port).await;

        let holepunch_info = holepunch_setup.as_ref().map(|setup| {
            let relays: Vec<RelayInfo> = self
                .dht
                .current_relay_addresses()
                .into_iter()
                .map(|relay_addr| RelayInfo {
                    relay_address: relay_addr,
                    peer_address: client_address.clone(),
                })
                .collect();
            HolepunchInfo {
                id: setup.id,
                relays,
            }
        });

        let reply_payload = NoisePayload {
            version: 1,
            error: 0,
            firewall: self.config.firewall,
            holepunch: holepunch_info,
            addresses4,
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
                vec![Ipv4Peer {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                }]
            }),
        };

        let noise_reply = match nw.send(&reply_payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(err = %e, "server handshake: noise send failed");
                if let Some(setup) = holepunch_setup {
                    setup.abort_task.abort();
                }
                let _ = reply_tx.send(None);
                return;
            }
        };

        let nw_result = match nw.finalize() {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(err = %e, "server handshake: noise finalize failed");
                if let Some(setup) = holepunch_setup {
                    setup.abort_task.abort();
                }
                let _ = reply_tx.send(None);
                return;
            }
        };

        // Encode reply with mode + peer_address derived from inbound mode.
        // Per Node `lib/server.js _addHandshake`:
        //   - inbound FROM_CLIENT → respond MODE_REPLY (direct reply path).
        //   - inbound FROM_RELAY / FROM_SECOND_RELAY → respond MODE_FROM_SERVER
        //     with peer_address pointing to the originating client (carried in
        //     the inbound msg.peer_address). The dht layer will dispatch this
        //     via dht.relay_with_tid back to the FE-holder, which then routes
        //     a tid-preserved REPLY to the receiver.
        let (reply_mode, reply_peer_address) = match msg.mode {
            MODE_FROM_RELAY | MODE_FROM_SECOND_RELAY => {
                (MODE_FROM_SERVER, peer_address.clone())
            }
            _ => (MODE_REPLY, None),
        };

        let reply_msg = HandshakeMessage {
            mode: reply_mode,
            noise: noise_reply,
            peer_address: reply_peer_address,
            relay_address: None,
        };
        let _ = reply_tx.send(encode_handshake_to_bytes(&reply_msg).ok());

        let remote_pk = nw_result.remote_public_key;

        // If we staged a passive holepunch above, hand ownership of the
        // staged state into the actor's `connects` map and return. Slot
        // accounting (self.connections.add) is deferred until the firewall
        // hook fires — see `PassiveHolepunchEvent::Punched` handling.
        if let Some(setup) = holepunch_setup {
            self.commit_passive_holepunch(setup, remote_pk, nw_result);
            return;
        }

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

    /// Stage a passive Holepuncher, create its listening UdxStream, wire
    /// the firewall hook, and arm the 10s abort timer — all the work that
    /// must happen *before* the handshake reply goes out (the reply has
    /// to advertise the puncher's bound port and the session id).
    async fn try_setup_passive_holepunch(
        &mut self,
        remote_firewall: u64,
        local_stream_id: u32,
        remote_udx: Option<&UdxInfo>,
    ) -> Result<HolepunchSetup, SwarmError> {
        use peeroxide_dht::hyperdht::HyperDhtError;

        let remote_udx = remote_udx.ok_or_else(|| {
            SwarmError::Dht(HyperDhtError::HandshakeFailed(
                "passive holepunch: client did not advertise UDX info".into(),
            ))
        })?;

        let remote_id = u32::try_from(remote_udx.id).map_err(|_| {
            SwarmError::Dht(HyperDhtError::StreamEstablishment(
                "remote UDX id out of u32 range".into(),
            ))
        })?;

        let pool = SocketPool::new("0.0.0.0".into());
        let runtime = UdxRuntime::shared(self.runtime_handle.clone());

        // Per the Holepuncher contract this is_initiator=false, firewalled=true
        // construction is the passive side of the punch. The HolepunchEvent
        // channel is intentionally discarded (`_event_rx` is dropped): the
        // passive puncher never emits `Connected` itself — the libudx
        // firewall hook on its primary socket is the authoritative
        // punch-landed signal for the server side.
        let (event_tx, _event_rx) = mpsc::unbounded_channel::<HolepunchEvent>();
        let mut puncher = Holepuncher::new(&pool, &runtime, true, false, remote_firewall, event_tx)
            .await
            .map_err(|e| {
                SwarmError::Dht(HyperDhtError::HandshakeFailed(format!(
                    "passive holepunch: socket pool acquire failed: {e}"
                )))
            })?;

        // Seed the passive puncher's NAT BEFORE punch() can run. auto_sample
        // takes ownership of the puncher socket's dht_reply_rx and must be
        // the only consumer — running it first guarantees that. Without this
        // the passive NAT stays UNKNOWN and the coerce_firewall revert
        // (removing UNKNOWN→CONSISTENT) would break the passive punch path.
        // Mirrors Node lib/holepuncher.js:13-20.
        let _added = puncher.auto_sample(self.dht.dht()).await;

        let socket_ref = puncher.primary_socket().ok_or_else(|| {
            SwarmError::Dht(HyperDhtError::StreamEstablishment(
                "puncher returned no primary socket".into(),
            ))
        })?;
        let socket = socket_ref.socket.clone();
        let socket_addr = socket.local_addr().await.map_err(|e| {
            SwarmError::Dht(HyperDhtError::StreamEstablishment(format!(
                "puncher socket local_addr failed: {e}"
            )))
        })?;
        let puncher_port = socket_addr.port();

        let udx_stream = runtime.create_stream(local_stream_id).await?;

        let holepunch_id = self.next_holepunch_id;
        self.next_holepunch_id = self.next_holepunch_id.wrapping_add(1);

        // Single-fire hook: the FIRST inbound packet whose 4-tuple matches
        // (local_id == this stream, source is currently unknown) commits
        // the remote address and unblocks the connection. The closure must
        // be FnOnce + Send + 'static, so we send the result to the actor
        // through an mpsc channel rather than awaiting anything inline.
        let event_tx = self.passive_hp_event_tx.clone();
        udx_stream.set_firewall_hook(&socket, remote_id, move |_sock, port, ip| {
            let addr = SocketAddr::new(ip, port);
            let _ = event_tx.send(PassiveHolepunchEvent::Punched {
                id: holepunch_id,
                addr,
            });
            true
        })?;

        let abort_tx = self.passive_hp_event_tx.clone();
        let abort_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = abort_tx.send(PassiveHolepunchEvent::Abort { id: holepunch_id });
        });

        Ok(HolepunchSetup {
            id: holepunch_id,
            puncher_port,
            puncher,
            udx_stream,
            udx_socket: socket,
            abort_task,
        })
    }

    /// Move a successfully-staged setup into the `connects` map. Performs
    /// remote_pk dedup: if a previous handshake from this same peer is
    /// still in-flight, its puncher/stream/timer are dropped here so the
    /// new entry replaces them cleanly.
    fn commit_passive_holepunch(
        &mut self,
        setup: HolepunchSetup,
        remote_pk: [u8; 32],
        noise_result: NoiseWrapResult,
    ) {
        if let Some(old_id) = self.pending_handshakes.insert(remote_pk, setup.id) {
            if let Some(old_entry) = self.connects.remove(&old_id) {
                if let Some(task) = old_entry.abort_task {
                    task.abort();
                }
                tracing::debug!(
                    pk = %short_hex(&remote_pk),
                    old_id,
                    new_id = setup.id,
                    "passive holepunch: replacing stale in-flight entry"
                );
            }
        }

        let payload = SecurePayload::new(noise_result.holepunch_secret);

        // Holepunch payload `addresses` list (Algorithm B): prefer
        // autoSample-derived reflexive samples on the puncher; fall
        // back to local LAN interfaces when autoSample produced none.
        // Mirrors Node `hyperdht/lib/holepuncher.js:221-227`.
        let local_punch_addrs = setup.puncher.punch_addresses(setup.puncher_port);

        self.connects.insert(
            setup.id,
            InFlightHolepunch {
                remote_pk,
                payload,
                puncher: Some(setup.puncher),
                udx_stream: setup.udx_stream,
                udx_socket: setup.udx_socket,
                noise_result,
                round: 0,
                abort_task: Some(setup.abort_task),
                local_punch_addrs,
            },
        );
    }

    async fn handle_peer_holepunch(
        &mut self,
        msg: HolepunchMessage,
        peer_address: Ipv4Peer,
        reply_tx: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        let Some(entry) = self.connects.get_mut(&msg.id) else {
            tracing::debug!(id = msg.id, "peer holepunch: unknown id");
            let _ = reply_tx.send(None);
            return;
        };

        let remote_hp = match entry.payload.decrypt(&msg.payload) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(id = msg.id, err = %e, "peer holepunch: decrypt failed");
                let _ = reply_tx.send(None);
                return;
            }
        };

        // Update the puncher's view of the remote side. Once `punch()` has
        // been moved into its spawned task, the puncher slot is `None` and
        // further updates are no-ops — by that point the probe loop is
        // already running with the verified address it captured.
        if let Some(puncher) = entry.puncher.as_mut() {
            if let Some(addrs) = &remote_hp.addresses {
                puncher.update_remote(
                    remote_hp.punching,
                    remote_hp.firewall,
                    addrs,
                    Some(peer_address.host.as_str()),
                );
            } else {
                puncher.update_remote(
                    remote_hp.punching,
                    remote_hp.firewall,
                    &[],
                    Some(peer_address.host.as_str()),
                );
            }
            // The passive puncher's NAT sampler is never fed (only the
            // active/initiator side's `run_holepunch_rounds` calls
            // `puncher.nat.add(...)` from PEER_HOLEPUNCH replies), so
            // `puncher.analyze(false).await` would deadlock forever
            // here, blocking the entire SwarmActor event loop. The
            // passive side's success signal is the firewall hook on
            // the puncher's primary UDX stream firing when the punch
            // probe lands — not a NAT classification gate.
        }

        if remote_hp.round > entry.round {
            entry.round = remote_hp.round;
        }

        // Spawn the active probe task only on the first round where the
        // initiator asks us to punch. `puncher.punch()` takes &mut self and
        // runs a long-lived probe loop, so the puncher is moved out and
        // owned by the task — subsequent rounds just keep building replies.
        if remote_hp.punching && entry.puncher.is_some() {
            if let Some(mut puncher) = entry.puncher.take() {
                tokio::spawn(async move {
                    let pool = SocketPool::new("0.0.0.0".into());
                    if let Ok(rt) = UdxRuntime::new() {
                        puncher.punch(&pool, &rt).await;
                    }
                });
            }
        }

        let server_firewall = self.dht.firewalled().await.unwrap_or(FIREWALL_UNKNOWN);
        let reply_hp = HolepunchPayload {
            error: ERROR_NONE,
            firewall: server_firewall,
            round: remote_hp.round,
            connected: false,
            punching: remote_hp.punching,
            addresses: Some(entry.local_punch_addrs.clone()),
            remote_address: Some(peer_address.clone()),
            token: Some(entry.payload.token(&peer_address.host)),
            remote_token: remote_hp.token,
        };

        let encrypted = match entry.payload.encrypt(&reply_hp) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!(id = msg.id, err = %e, "peer holepunch: encrypt failed");
                let _ = reply_tx.send(None);
                return;
            }
        };

        // Mirror item-8 handshake reply-mode selection: relayed inbound
        // (FROM_RELAY / FROM_SECOND_RELAY) MUST be answered with mode
        // FROM_SERVER so the FE-holder's router dispatches a tid-preserved
        // ReplyTo back to the original client. A direct FROM_CLIENT inbound
        // (currently unused for holepunch, but kept for symmetry) is
        // answered with MODE_REPLY.
        let reply_mode = match msg.mode {
            MODE_FROM_RELAY | MODE_FROM_SECOND_RELAY => MODE_FROM_SERVER,
            _ => MODE_REPLY,
        };

        let reply_msg = HolepunchMessage {
            mode: reply_mode,
            id: msg.id,
            payload: encrypted,
            peer_address: Some(peer_address),
        };

        let _ = reply_tx.send(encode_holepunch_msg_to_bytes(&reply_msg).ok());
    }

    async fn handle_passive_holepunch_event(&mut self, event: PassiveHolepunchEvent) {
        match event {
            PassiveHolepunchEvent::Punched { id, addr } => {
                let Some(mut entry) = self.connects.remove(&id) else {
                    return;
                };
                if let Some(task) = entry.abort_task.take() {
                    task.abort();
                }
                self.pending_handshakes.remove(&entry.remote_pk);

                if self.connections.has(&entry.remote_pk) {
                    tracing::debug!(
                        pk = %short_hex(&entry.remote_pk),
                        "passive holepunch landed but already connected; dropping"
                    );
                    return;
                }
                if self.connections.len() >= self.config.max_peers {
                    tracing::debug!("passive holepunch landed but at max connections");
                    return;
                }

                tracing::debug!(
                    id,
                    pk = %short_hex(&entry.remote_pk),
                    ?addr,
                    "passive holepunch landed; finalizing SecretStream"
                );

                self.connections
                    .add(entry.remote_pk, ConnectionInfo { is_initiator: false });

                let runtime = UdxRuntime::shared(self.runtime_handle.clone());
                let conn_tx = self.conn_tx.clone();
                let failure_tx = self.server_failure_tx.clone();
                let remote_pk = entry.remote_pk;

                tokio::spawn(async move {
                    let async_stream = entry.udx_stream.into_async_stream();
                    let ss = match SecretStream::from_session(
                        false,
                        async_stream,
                        entry.noise_result.tx,
                        entry.noise_result.rx,
                        entry.noise_result.handshake_hash,
                        entry.noise_result.remote_public_key,
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::debug!(err = %e, "passive holepunch: SecretStream failed");
                            if let Some(tx) = failure_tx {
                                let _ = tx.send(remote_pk);
                            }
                            return;
                        }
                    };

                    let conn = PeerConnection::new(ss, remote_pk, entry.udx_socket, None);
                    let swarm_conn = SwarmConnection {
                        peer: conn,
                        is_initiator: false,
                        topics: vec![],
                        _runtime: runtime,
                    };
                    if conn_tx.send(swarm_conn).await.is_err() {
                        tracing::warn!("connection channel closed");
                    }
                });
            }
            PassiveHolepunchEvent::Abort { id } => {
                let Some(mut entry) = self.connects.remove(&id) else {
                    return;
                };
                if let Some(task) = entry.abort_task.take() {
                    task.abort();
                }
                self.pending_handshakes.remove(&entry.remote_pk);
                if let Some(mut puncher) = entry.puncher.take() {
                    puncher.destroy();
                }
                tracing::warn!(
                    id,
                    pk = %short_hex(&entry.remote_pk),
                    "passive holepunch timed out"
                );
            }
        }
    }
}

/// Output of [`SwarmActor::try_setup_passive_holepunch`]. Holds everything
/// the actor needs to (a) build the handshake reply and (b) install the
/// in-flight entry once Noise finalisation has produced a `remote_pk` +
/// holepunch secret.
struct HolepunchSetup {
    id: u64,
    puncher_port: u16,
    puncher: Holepuncher,
    udx_stream: UdxStream,
    udx_socket: UdxSocket,
    abort_task: JoinHandle<()>,
}

async fn create_server_connection(
    runtime_handle: Arc<RuntimeHandle>,
    dht: HyperDhtHandle,
    local_stream_id: u32,
    remote_udx: &UdxInfo,
    _client_address: &Ipv4Peer,
    noise_result: &peeroxide_dht::noise_wrap::NoiseWrapResult,
) -> Result<(PeerConnection, UdxRuntime), SwarmError> {
    let runtime = UdxRuntime::shared(runtime_handle);

    let remote_id = u32::try_from(remote_udx.id).map_err(|_| {
        SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
            "remote UDX id out of u32 range".into(),
        ))
    })?;

    let socket = dht
        .server_socket()
        .await
        .map_err(SwarmError::Dht)?
        .ok_or_else(|| {
            SwarmError::Dht(peeroxide_dht::hyperdht::HyperDhtError::StreamEstablishment(
                "DHT server socket not available".into(),
            ))
        })?;

    let stream = runtime.create_stream(local_stream_id).await?;
    stream.set_firewall_hook(&socket, remote_id, |_, _, _| true)?;

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

    let conn = PeerConnection::new(ss, noise_result.remote_public_key, socket, None);
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
