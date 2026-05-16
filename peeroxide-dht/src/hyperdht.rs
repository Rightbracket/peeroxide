#![deny(clippy::all)]

use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use ed25519_dalek::SigningKey;
use rand::random;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use libudx::{UdxAsyncStream, UdxRuntime, UdxSocket};

use crate::blind_relay::{BlindRelayClient, RelayError};
use crate::crypto::{
    ann_signable, hash, mutable_signable, sign_detached, verify_detached, NS_ANNOUNCE,
    NS_MUTABLE_PUT, NS_UNANNOUNCE,
};
use crate::holepuncher::{Holepuncher, HolepunchEvent};
use crate::hyperdht_messages::{
    decode_hyper_peer_from_bytes,
    decode_lookup_raw_reply_from_bytes, decode_mutable_get_response_from_bytes,
    encode_announce_to_bytes, encode_hyper_peer_to_bytes,
    encode_mutable_put_request_to_bytes, AnnounceMessage, HandshakeMessage, HolepunchMessage,
    HolepunchPayload, HyperPeer, MutablePutRequest, NoisePayload, RelayThroughInfo,
    SecretStreamInfo, UdxInfo, ANNOUNCE, ERROR_ABORTED, ERROR_NONE, ERROR_TRY_LATER, FIND_PEER,
    FIREWALL_OPEN, FIREWALL_UNKNOWN, IMMUTABLE_GET, IMMUTABLE_PUT, LOOKUP, MUTABLE_GET,
    MUTABLE_PUT, PEER_HANDSHAKE, PEER_HOLEPUNCH, UNANNOUNCE,
};
use crate::messages::Ipv4Peer;
use crate::noise::Keypair as NoiseKeypair;
use crate::noise_wrap::{NoiseWrap, NoiseWrapResult};
use crate::peer::NodeId;
use crate::persistent::{
    HandlerReply, IncomingHyperRequest, Persistent, PersistentConfig, PersistentStats,
};
use crate::protomux::Mux;
use crate::router::{ForwardEntry, HandshakeAction, HolepunchAction, Router};
use crate::query::QueryReply;
use crate::rpc::{DhtConfig, DhtError, DhtHandle, UserQueryParams, UserRequestParams};
use crate::secret_stream::{SecretStream, SecretStreamError};
use crate::secure_payload::SecurePayload;
use crate::socket_pool::SocketPool;

// ── Errors ────────────────────────────────────────────────────────────────────

static NEXT_STREAM_ID: AtomicU32 = AtomicU32::new(1);

fn next_stream_id() -> u32 {
    NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed)
}

/// Matches Node.js `isBogon` from the `bogon` package — returns true for
/// loopback, link-local, private RFC-1918, and other reserved ranges.
fn is_addr_private(host: &str) -> bool {
    let Ok(ip) = host.parse::<std::net::Ipv4Addr>() else {
        return true;
    };
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_documentation()
        || ip.octets()[0] == 100 && ip.octets()[1] >= 64 && ip.octets()[1] <= 127 // CGN
}

/// Determines whether a direct connection should be attempted instead of holepunching.
///
/// Returns `true` (direct connect) when ANY of:
/// - The handshake was NOT relayed (client reached server directly)
/// - Server reports FIREWALL_OPEN
/// - Server has no holepunch relays (can't holepunch even if we wanted to)
/// - Both peers share the same host address
///
/// Matches Node.js connect.js decision logic.
pub fn should_direct_connect(
    relayed: bool,
    firewall: u64,
    remote_holepunchable: bool,
    same_host: bool,
) -> bool {
    !relayed || firewall == FIREWALL_OPEN || !remote_holepunchable || same_host
}

#[derive(Debug, Error)]
/// Errors returned by HyperDHT operations.
#[non_exhaustive]
pub enum HyperDhtError {
    /// Error propagated from the underlying DHT client.
    #[error("DHT error: {0}")]
    Dht(#[from] DhtError),
    /// Error while encoding or decoding protocol data.
    #[error("encoding error: {0}")]
    Encoding(#[from] crate::compact_encoding::EncodingError),
    /// Error from Noise handshake or session setup.
    #[error("noise error: {0}")]
    Noise(#[from] crate::noise::NoiseError),
    /// Error from the Noise wrapper layer.
    #[error("noise wrap error: {0}")]
    NoiseWrap(#[from] crate::noise_wrap::NoiseWrapError),
    /// Error from the router state machine.
    #[error("router error: {0}")]
    Router(#[from] crate::router::RouterError),
    /// Error while wrapping or unwrapping secure payloads.
    #[error("secure payload error: {0}")]
    SecurePayload(#[from] crate::secure_payload::SecurePayloadError),
    /// This DHT instance has been destroyed.
    #[error("node destroyed")]
    Destroyed,
    /// A signature did not verify.
    #[error("invalid signature")]
    InvalidSignature,
    /// A content hash did not match.
    #[error("invalid hash")]
    InvalidHash,
    /// The internal channel was closed.
    #[error("channel closed")]
    ChannelClosed,
    /// No peer was found for the requested target.
    #[error("peer not found")]
    PeerNotFound,
    /// No relay nodes were available for the operation.
    #[error("no relay nodes available")]
    NoRelayNodes,
    /// The handshake failed with the given message.
    #[error("handshake failed: {0}")]
    HandshakeFailed(String),
    /// Hole punching did not succeed.
    #[error("holepunch failed")]
    HolepunchFailed,
    /// Hole punching was aborted by the remote side.
    #[error("holepunch aborted")]
    HolepunchAborted,
    /// The remote firewall rejected the connection.
    #[error("firewall rejected")]
    FirewallRejected,
    /// Error from the UDX transport layer.
    #[error("UDX error: {0}")]
    Udx(#[from] libudx::UdxError),
    /// Error from the secret stream layer.
    #[error("secret stream error: {0}")]
    SecretStream(#[from] SecretStreamError),
    /// Failed to establish a UDX stream.
    #[error("stream establishment failed: {0}")]
    StreamEstablishment(String),
    /// Error from the relay subsystem.
    #[error("relay error: {0}")]
    Relay(#[from] RelayError),
}

// ── Server events (forwarded to listen() subscribers) ────────────────────────

#[derive(Debug)]
/// Events forwarded to server-side listeners.
#[non_exhaustive]
pub enum ServerEvent {
    /// A peer handshake request that may need local server handling.
    PeerHandshake {
        /// The decoded handshake message.
        msg: HandshakeMessage,
        /// Address of the peer that sent the request to us.
        ///
        /// For a direct (non-forwarded) handshake, this is the remote peer.
        /// For a relayed handshake, this is the forwarding DHT node (the
        /// previous hop), NOT the originating peer; use `peer_address` for
        /// the originating peer's address in that case.
        from: Ipv4Peer,
        /// Address of the originating peer for a forwarded handshake, or
        /// `None` for a direct handshake.
        ///
        /// Populated from the relay-layer's `peer_address` payload field.
        /// Mirrors Node Hyperswarm's "peerAddress" semantics in
        /// `lib/router.js`. Servers should prefer this over `from` when
        /// choosing a remote address to dial.
        peer_address: Option<Ipv4Peer>,
        /// Optional DHT target associated with the request.
        target: Option<NodeId>,
        /// Reply channel for the generated response.
        reply_tx: oneshot::Sender<Option<Vec<u8>>>,
    },
    /// A peer holepunch request that may need local server handling.
    PeerHolepunch {
        /// The decoded holepunch message.
        msg: HolepunchMessage,
        /// Address of the peer that sent the request.
        from: Ipv4Peer,
        /// Address of the peer we should punch toward.
        peer_address: Ipv4Peer,
        /// Optional DHT target associated with the request.
        target: Option<NodeId>,
        /// Reply channel for the generated response.
        reply_tx: oneshot::Sender<Option<Vec<u8>>>,
    },
}

// ── KeyPair ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
/// An Ed25519 key pair (libsodium layout: seed‖public_key).
pub struct KeyPair {
    /// The 32-byte public key.
    pub public_key: [u8; 32],
    /// The 64-byte secret key in libsodium layout.
    pub secret_key: [u8; 64],
}

impl KeyPair {
    /// Generate a new random key pair.
    pub fn generate() -> Self {
        let seed: [u8; 32] = random();
        Self::from_seed(seed)
    }

    /// Derive a deterministic key pair from a 32-byte seed.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(&seed);
        let pk: [u8; 32] = signing_key.verifying_key().to_bytes();
        let mut sk = [0u8; 64];
        sk[..32].copy_from_slice(&seed);
        sk[32..].copy_from_slice(&pk);
        Self {
            public_key: pk,
            secret_key: sk,
        }
    }
}

impl fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPair")
            .field("public_key", &to_hex(self.public_key))
            .finish_non_exhaustive()
    }
}

impl KeyPair {
    fn to_noise_keypair(&self) -> NoiseKeypair {
        NoiseKeypair {
            public_key: self.public_key,
            secret_key: self.secret_key,
        }
    }
}

// ── Result types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
/// Result from a LOOKUP query.
#[non_exhaustive]
pub struct LookupResult {
    /// Node that returned the lookup result.
    pub from: Ipv4Peer,
    /// Optional intermediate hop used to reach the node.
    pub to: Option<Ipv4Peer>,
    /// Peers advertised by the node.
    pub peers: Vec<HyperPeer>,
}

#[derive(Debug, Clone)]
/// Result from an ANNOUNCE operation.
#[non_exhaustive]
pub struct AnnounceResult {
    /// Closest nodes contacted during the announce.
    pub closest_nodes: Vec<Ipv4Peer>,
}

#[derive(Debug, Clone)]
/// Result from an immutable put operation.
#[non_exhaustive]
pub struct ImmutablePutResult {
    /// Content hash used as the target key.
    pub hash: [u8; 32],
    /// Closest nodes contacted during the write.
    pub closest_nodes: Vec<Ipv4Peer>,
}

#[derive(Debug, Clone)]
/// Result from a mutable put operation.
#[non_exhaustive]
pub struct MutablePutResult {
    /// Public key used as the mutable record key.
    pub public_key: [u8; 32],
    /// Closest nodes contacted during the write.
    pub closest_nodes: Vec<Ipv4Peer>,
    /// Record sequence number that was written.
    pub seq: u64,
    /// Signature over the stored value.
    pub signature: [u8; 64],
    /// Number of commit-phase requests that timed out.
    pub commit_timeouts: u32,
}

#[derive(Debug, Clone)]
/// Result from a mutable get operation.
#[non_exhaustive]
pub struct MutableGetResult {
    /// Retrieved value bytes.
    pub value: Vec<u8>,
    /// Sequence number attached to the value.
    pub seq: u64,
    /// Signature verifying the value.
    pub signature: [u8; 64],
    /// Node that returned the value.
    pub from: Ipv4Peer,
}

#[derive(Clone)]
/// Metadata needed to establish a peer connection.
#[non_exhaustive]
pub struct ConnectResult {
    /// Remote peer's public key.
    pub remote_public_key: [u8; 32],
    /// Address used to reach the server during handshake.
    pub server_address: Ipv4Peer,
    /// Address of the client-side peer endpoint.
    pub client_address: Ipv4Peer,
    /// Whether the connection was relayed through a third party.
    pub is_relayed: bool,
    /// Final Noise state and negotiated keys.
    pub noise: NoiseWrapResult,
    /// Local UDX stream id to use for the connection.
    pub local_stream_id: u32,
    /// Remote UDX metadata advertised by the peer.
    pub remote_udx: Option<UdxInfo>,
    /// Clone of the puncher's primary UDX socket when holepunching lands.
    ///
    /// When set by `run_holepunch_rounds`' `Connected` arm in a follow-up
    /// commit, this is the socket whose punched 4-tuple is established.
    /// Consumers should prefer it over the DHT server socket when present.
    pub udx_socket: Option<UdxSocket>,
}

impl fmt::Debug for ConnectResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectResult")
            .field("remote_public_key", &self.remote_public_key)
            .field("server_address", &self.server_address)
            .field("client_address", &self.client_address)
            .field("is_relayed", &self.is_relayed)
            .field("noise", &self.noise)
            .field("local_stream_id", &self.local_stream_id)
            .field("remote_udx", &self.remote_udx)
            .field("udx_socket", &self.udx_socket.as_ref().map(|_| "Some(UdxSocket)"))
            .finish()
    }
}

/// Established encrypted connection to a peer.
///
/// Wraps a [`SecretStream`] over a UDX transport, keeping the underlying
/// socket alive for the connection's lifetime.
#[non_exhaustive]
pub struct PeerConnection {
    /// Encrypted bidirectional stream to the peer.
    pub stream: SecretStream<UdxAsyncStream>,
    /// Remote peer's public key.
    pub remote_public_key: [u8; 32],
    /// Remote peer's network address (used by server-side relay to connect data streams).
    pub remote_addr: Option<std::net::SocketAddr>,
    /// The UDX socket underlying this connection. Public so relay flows
    /// in downstream crates can reuse the control channel's socket for
    /// data streams (matching Node.js behaviour).
    pub socket: UdxSocket,
    _relay_task: Option<JoinHandle<()>>,
}

impl PeerConnection {
    /// Create a new peer connection from its components.
    pub fn new(
        stream: SecretStream<UdxAsyncStream>,
        remote_public_key: [u8; 32],
        socket: UdxSocket,
        relay_task: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            stream,
            remote_public_key,
            remote_addr: None,
            socket,
            _relay_task: relay_task,
        }
    }

    /// Create a new peer connection with a known remote address.
    pub fn with_remote_addr(
        stream: SecretStream<UdxAsyncStream>,
        remote_public_key: [u8; 32],
        remote_addr: std::net::SocketAddr,
        socket: UdxSocket,
        relay_task: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            stream,
            remote_public_key,
            remote_addr: Some(remote_addr),
            socket,
            _relay_task: relay_task,
        }
    }
}

impl fmt::Debug for PeerConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerConnection")
            .field("remote_public_key", &&self.remote_public_key[..8])
            .field("remote_addr", &self.remote_addr)
            .field("relayed", &self._relay_task.is_some())
            .finish_non_exhaustive()
    }
}

/// Configuration used by the server-side handshake and holepunch handler.
#[non_exhaustive]
pub struct ServerConfig {
    /// Server identity key pair.
    pub key_pair: KeyPair,
    /// Firewall mode advertised to connecting peers.
    pub firewall: u64,
}

impl ServerConfig {
    /// Create a new server configuration.
    pub fn new(key_pair: KeyPair, firewall: u64) -> Self {
        Self { key_pair, firewall }
    }
}

// ── Bootstrap defaults ────────────────────────────────────────────────────────

/// The three public HyperDHT bootstrap nodes (from `hyperdht/lib/constants.js`).
///
/// Format: `suggestedIP@hostname:port`.  `parse_bootstrap_str`
/// extracts the IP before `@`, so these work without DNS resolution.
pub const DEFAULT_BOOTSTRAP: [&str; 3] = [
    "88.99.3.86@node1.hyperdht.org:49737",
    "142.93.90.113@node2.hyperdht.org:49737",
    "138.68.147.8@node3.hyperdht.org:49737",
];

// ── Config ────────────────────────────────────────────────────────────────────

/// Per-connect options for [`HyperDhtHandle::connect_with_options`].
///
/// Default: same-NAT LAN-shortcut enabled (matches Node hyperdht's
/// `opts.localConnection` default of `true`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectOpts {
    /// Allow the same-NAT LAN-shortcut. When `false`, the receiver
    /// ignores any loopback/private addresses the server advertised in
    /// its `addresses4` and dials only the public address. Set this to
    /// `false` to force the real-network code path under test.
    pub local_connection: bool,
}

impl Default for ConnectOpts {
    fn default() -> Self {
        Self {
            local_connection: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
/// Configuration for a HyperDHT instance.
#[non_exhaustive]
pub struct HyperDhtConfig {
    /// DHT transport and bootstrap settings.
    pub dht: DhtConfig,
    /// Persistent storage settings for stored records.
    pub persistent: PersistentConfig,
}

impl HyperDhtConfig {
    /// Create a config pre-populated with the public HyperDHT bootstrap nodes.
    ///
    /// This is the typical starting point for connecting to the live network.
    /// `DhtConfig::default()` intentionally keeps `bootstrap` empty so that
    /// unit tests can run without network access.
    pub fn with_public_bootstrap() -> Self {
        Self {
            dht: DhtConfig {
                bootstrap: DEFAULT_BOOTSTRAP.iter().map(|s| (*s).to_string()).collect(),
                ..DhtConfig::default()
            },
            persistent: PersistentConfig::default(),
        }
    }
}

// ── AdminRequest ──────────────────────────────────────────────────────────────

enum AdminRequest {
    PersistentStats { reply: oneshot::Sender<PersistentStats> },
}

// ── HyperDhtHandle ────────────────────────────────────────────────────────────

#[derive(Clone)]
/// Main public HyperDHT API handle.
pub struct HyperDhtHandle {
    dht: DhtHandle,
    router: Arc<Mutex<Router>>,
    server_tx: mpsc::UnboundedSender<ServerEvent>,
    admin_tx: mpsc::UnboundedSender<AdminRequest>,
    current_relay_addresses: Arc<Mutex<Vec<Ipv4Peer>>>,
}

impl HyperDhtHandle {
    // ── WIRE STATS ────────────────────────────────────────────────────────────

    /// Snapshot of cumulative wire bytes (sent, received) since this DHT
    /// node started. Counts every UDP datagram exchanged at the IO layer
    /// — queries, requests, replies, retries, relays, and any user-issued
    /// puts/gets — regardless of which higher-level operation produced them.
    ///
    /// Useful for distinguishing "useful payload throughput" (what consumers
    /// see) from "raw network throughput" (what the OS sees). The ratio
    /// between them is the DHT's protocol amplification factor.
    pub fn wire_stats(&self) -> (u64, u64) {
        self.dht.wire_stats()
    }

    /// Borrow the shared wire-counter handle for long-lived sampling. The
    /// returned counters are `Arc<AtomicU64>` internally; cloning is cheap.
    pub fn wire_counters(&self) -> crate::io::WireCounters {
        self.dht.wire_counters()
    }

    // ── LOOKUP ────────────────────────────────────────────────────────────────

    /// Query the DHT for peers advertising the target.
    pub async fn lookup(&self, target: [u8; 32]) -> Result<Vec<LookupResult>, HyperDhtError> {
        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: LOOKUP,
                value: None,
                commit: false,
                concurrency: None,
            })
            .await?;

        let mut results = Vec::new();
        for reply in replies {
            if let Some(value) = &reply.value {
                if let Ok(raw) = decode_lookup_raw_reply_from_bytes(value) {
                    if !raw.peers.is_empty() {
                        results.push(LookupResult {
                            from: reply.from.clone(),
                            to: None,
                            peers: raw.peers,
                        });
                    }
                }
            }
        }
        Ok(results)
    }

    // ── ANNOUNCE ─────────────────────────────────────────────────────────────

    /// Announce this peer under the given target.
    pub async fn announce(
        &self,
        target: [u8; 32],
        key_pair: &KeyPair,
        relay_addresses: &[Ipv4Peer],
    ) -> Result<AnnounceResult, HyperDhtError> {
        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: LOOKUP,
                value: None,
                commit: true,
                concurrency: None,
            })
            .await?;

        let mut closest_nodes = Vec::new();
        let mut accepted_relays: Vec<Ipv4Peer> = Vec::new();

        for reply in &replies {
            closest_nodes.push(reply.from.clone());

            let token = match &reply.token {
                Some(t) => *t,
                None => continue,
            };
            let node_id = match &reply.from_id {
                Some(id) => *id,
                None => continue,
            };

            // Stored record uses caller-provided relays verbatim, matching
            // Node `_requestAnnounce`: `ann.peer.relayAddresses = relayAddresses || []`.
            // Closest-ack accumulation lives only in handle-state
            // (`current_relay_addresses` below) for *subsequent* announces
            // to consume — it is NEVER written into a stored peer record.
            // Do not "fix" this by re-adding per-record accumulation; the
            // TOPIC-close nodes don't hold FE for `hash(pk)`, so advertising
            // them as PEER_HANDSHAKE relays yields "empty reply".
            let record_relays: Vec<Ipv4Peer> = relay_addresses.iter().take(3).cloned().collect();

            let peer = HyperPeer {
                public_key: key_pair.public_key,
                relay_addresses: record_relays,
            };

            let peer_encoded = encode_hyper_peer_to_bytes(&peer)?;
            let signable =
                ann_signable(&target, &token, &node_id, &peer_encoded, &[], &NS_ANNOUNCE);
            let signature = sign_detached(&signable, &key_pair.secret_key);

            let ann = AnnounceMessage {
                peer: Some(peer),
                refresh: None,
                signature: Some(signature),
                bump: 0,
            };
            let ann_bytes = encode_announce_to_bytes(&ann)?;

            let _ = self
                .dht
                .request(
                    UserRequestParams {
                        token: Some(token),
                        command: ANNOUNCE,
                        target: Some(target),
                        value: Some(ann_bytes),
                        timeout_ms: None,
                        retries: None,
                    },
                    &reply.from.host,
                    reply.from.port,
                )
                .await;

            // Accumulate this acker into handle-state relay list (Node
            // `Announcer._commit`: `if (relayAddresses.length < 3)
            // relayAddresses.push({ host: msg.from.host, port: msg.from.port })`).
            // We push only once per accepted reply and cap at 3. Dedup is
            // implicit — each `reply.from` is a distinct DHT node.
            if accepted_relays.len() < 3 {
                accepted_relays.push(reply.from.clone());
            }
        }

        // Only update handle-state `current_relay_addresses` when announcing
        // on `hash(public_key)` (self-announce). Topic-announces also produce
        // a `closest_nodes` set, but those are topic-close nodes (NOT FE-holders
        // for PEER_HANDSHAKE forwarding). Mixing them into `current_relay_addresses`
        // — when parallel announces race on the shared Mutex — would corrupt
        // the relay list the topic-announce reads on a subsequent refresh.
        // See peer_discovery::do_refresh for the parallel-join order.
        if target == hash(&key_pair.public_key) {
            if let Ok(mut guard) = self.current_relay_addresses.lock() {
                *guard = accepted_relays;
            }
        }

        Ok(AnnounceResult { closest_nodes })
    }

    // ── FIND_PEER ─────────────────────────────────────────────────────────────

    /// Return the first peer record found for the target.
    pub async fn find_peer(
        &self,
        target: [u8; 32],
    ) -> Result<Option<HyperPeer>, HyperDhtError> {
        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: FIND_PEER,
                value: None,
                commit: false,
                concurrency: None,
            })
            .await?;

        for reply in replies {
            if let Some(value) = reply.value {
                if let Ok(peer) = decode_hyper_peer_from_bytes(&value) {
                    return Ok(Some(peer));
                }
            }
        }
        Ok(None)
    }

    /// Run a FIND_PEER query and return all raw replies.
    ///
    /// Unlike [`find_peer`](Self::find_peer), this returns every reply from
    /// the iterative query so callers can try connecting through each
    /// responding node's address.
    pub async fn query_find_peer(
        &self,
        target: [u8; 32],
    ) -> Result<Vec<QueryReply>, HyperDhtError> {
        Ok(self
            .dht
            .query(UserQueryParams {
                target,
                command: FIND_PEER,
                value: None,
                commit: false,
                concurrency: None,
            })
            .await?)
    }

    // ── UNANNOUNCE ────────────────────────────────────────────────────────────

    /// Remove a previously announced peer record.
    pub async fn unannounce(
        &self,
        target: [u8; 32],
        key_pair: &KeyPair,
    ) -> Result<(), HyperDhtError> {
        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: LOOKUP,
                value: None,
                commit: false,
                concurrency: None,
            })
            .await?;

        for reply in &replies {
            let token = match &reply.token {
                Some(t) => *t,
                None => continue,
            };
            let node_id = match &reply.from_id {
                Some(id) => *id,
                None => continue,
            };

            let peer = HyperPeer {
                public_key: key_pair.public_key,
                relay_addresses: vec![],
            };
            let peer_encoded = encode_hyper_peer_to_bytes(&peer)?;
            let signable = ann_signable(
                &target,
                &token,
                &node_id,
                &peer_encoded,
                &[],
                &NS_UNANNOUNCE,
            );
            let signature = sign_detached(&signable, &key_pair.secret_key);

            let ann = AnnounceMessage {
                peer: Some(peer),
                refresh: None,
                signature: Some(signature),
                bump: 0,
            };
            let ann_bytes = encode_announce_to_bytes(&ann)?;

            let _ = self
                .dht
                .request(
                    UserRequestParams {
                        token: Some(token),
                        command: UNANNOUNCE,
                        target: Some(target),
                        value: Some(ann_bytes),
                        timeout_ms: None,
                        retries: None,
                    },
                    &reply.from.host,
                    reply.from.port,
                )
                .await;
        }

        Ok(())
    }

    // ── IMMUTABLE_PUT ────────────────────────────────────────────────────────

    /// Store immutable content under its content hash.
    pub async fn immutable_put(
        &self,
        value: &[u8],
    ) -> Result<ImmutablePutResult, HyperDhtError> {
        let target = hash(value);

        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: IMMUTABLE_GET,
                value: None,
                commit: true,
                concurrency: None,
            })
            .await?;

        let mut closest_nodes = Vec::new();

        for reply in &replies {
            closest_nodes.push(reply.from.clone());

            let token = match &reply.token {
                Some(t) => *t,
                None => continue,
            };

            let _ = self
                .dht
                .request(
                    UserRequestParams {
                        token: Some(token),
                        command: IMMUTABLE_PUT,
                        target: Some(target),
                        value: Some(value.to_vec()),
                        timeout_ms: None,
                        retries: None,
                    },
                    &reply.from.host,
                    reply.from.port,
                )
                .await;
        }

        Ok(ImmutablePutResult {
            hash: target,
            closest_nodes,
        })
    }

    // ── IMMUTABLE_GET ────────────────────────────────────────────────────────

    /// Fetch immutable content by content hash.
    pub async fn immutable_get(
        &self,
        target: [u8; 32],
    ) -> Result<Option<Vec<u8>>, HyperDhtError> {
        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: IMMUTABLE_GET,
                value: None,
                commit: false,
                concurrency: None,
            })
            .await?;

        for reply in replies {
            if let Some(value) = reply.value {
                if hash(&value) == target {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    // ── MUTABLE_PUT ───────────────────────────────────────────────────────────

    /// Store a signed mutable record for the given key pair.
    pub async fn mutable_put(
        &self,
        key_pair: &KeyPair,
        value: &[u8],
        seq: u64,
    ) -> Result<MutablePutResult, HyperDhtError> {
        let target = hash(&key_pair.public_key);
        let signable = mutable_signable(&NS_MUTABLE_PUT, seq, value);
        let signature = sign_detached(&signable, &key_pair.secret_key);

        let put = MutablePutRequest {
            public_key: key_pair.public_key,
            seq,
            value: value.to_vec(),
            signature,
        };
        let put_bytes = encode_mutable_put_request_to_bytes(&put)?;

        let seq_bytes = encode_compact_uint(seq);

        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: MUTABLE_GET,
                value: Some(seq_bytes),
                commit: true,
                concurrency: None,
            })
            .await?;

        let mut closest_nodes = Vec::new();
        let mut commit_timeouts: u32 = 0;

        for reply in &replies {
            closest_nodes.push(reply.from.clone());

            let token = match &reply.token {
                Some(t) => *t,
                None => continue,
            };

            if let Err(DhtError::RequestFailed(_)) = self
                .dht
                .request(
                    UserRequestParams {
                        token: Some(token),
                        command: MUTABLE_PUT,
                        target: Some(target),
                        value: Some(put_bytes.clone()),
                        timeout_ms: None,
                        retries: None,
                    },
                    &reply.from.host,
                    reply.from.port,
                )
                .await
            {
                commit_timeouts += 1;
            }
        }

        Ok(MutablePutResult {
            public_key: key_pair.public_key,
            closest_nodes,
            seq,
            signature,
            commit_timeouts,
        })
    }

    // ── MUTABLE_GET ───────────────────────────────────────────────────────────

    /// Fetch and verify a mutable record for the given public key.
    pub async fn mutable_get(
        &self,
        public_key: &[u8; 32],
        seq: u64,
    ) -> Result<Option<MutableGetResult>, HyperDhtError> {
        let target = hash(public_key);
        let seq_bytes = encode_compact_uint(seq);

        let replies = self
            .dht
            .query(UserQueryParams {
                target,
                command: MUTABLE_GET,
                value: Some(seq_bytes),
                commit: false,
                concurrency: None,
            })
            .await?;

        for reply in replies {
            if let Some(value) = &reply.value {
                if let Ok(resp) = decode_mutable_get_response_from_bytes(value) {
                    if resp.seq >= seq {
                        let signable =
                            mutable_signable(&NS_MUTABLE_PUT, resp.seq, &resp.value);
                        if verify_detached(&resp.signature, &signable, public_key) {
                            return Ok(Some(MutableGetResult {
                                value: resp.value,
                                seq: resp.seq,
                                signature: resp.signature,
                                from: reply.from,
                            }));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Wait until the DHT is bootstrapped.
    pub async fn bootstrapped(&self) -> Result<(), HyperDhtError> {
        self.dht.bootstrapped().await.map_err(HyperDhtError::Dht)
    }

    /// Destroy the underlying DHT instance.
    pub async fn destroy(&self) -> Result<(), HyperDhtError> {
        self.dht.destroy().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the number of nodes in the routing table.
    pub async fn table_size(&self) -> Result<usize, HyperDhtError> {
        self.dht.table_size().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the local port the DHT server socket is bound to.
    pub async fn local_port(&self) -> Result<u16, HyperDhtError> {
        self.dht.local_port().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the DHT server socket for multiplexing UDX streams.
    pub async fn server_socket(&self) -> Result<Option<UdxSocket>, HyperDhtError> {
        self.dht.server_socket().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the actual listen socket (bound to the advertised server port).
    pub async fn listen_socket(&self) -> Result<Option<UdxSocket>, HyperDhtError> {
        self.dht.listen_socket().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the externally-observed IPv4 address of this node, as inferred
    /// from the `to` field of inbound DHT responses (NAT consensus).
    ///
    /// Returns `None` until the NAT analysis has settled. Mirrors Node
    /// Hyperswarm's `dht.remoteAddress()`.
    pub async fn remote_address(&self) -> Result<Option<crate::messages::Ipv4Peer>, HyperDhtError> {
        self.dht.remote_address().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the current firewall classification (one of the
    /// `FIREWALL_OPEN` / `FIREWALL_UNKNOWN` / `FIREWALL_CONSISTENT` /
    /// `FIREWALL_RANDOM` constants from [`crate::hyperdht_messages`]).
    ///
    /// Mirrors Node Hyperswarm's `dht.firewalled`.
    pub async fn firewalled(&self) -> Result<u64, HyperDhtError> {
        self.dht.firewalled().await.map_err(HyperDhtError::Dht)
    }

    /// Returns the set of DHT-relay endpoints this node is currently
    /// announcing through (capped at 3, refreshed on each `announce()`).
    ///
    /// These are the addresses of DHT nodes that successfully accepted this
    /// node's last announce — meaning they hold a `ForwardEntry` and will
    /// relay `PEER_HANDSHAKE` on this node's behalf. Empty until the first
    /// `announce()` completes.
    ///
    /// Mirrors Node Hyperswarm's `Server.relayAddresses` / `Announcer.relayAddresses`.
    pub fn current_relay_addresses(&self) -> Vec<crate::messages::Ipv4Peer> {
        self.current_relay_addresses
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// Access the shared router state.
    pub fn router(&self) -> &Arc<Mutex<Router>> {
        &self.router
    }

    /// Access the underlying DHT handle.
    pub fn dht(&self) -> &DhtHandle {
        &self.dht
    }

    /// Returns persistent storage statistics collected from the request handler.
    pub async fn persistent_stats(&self) -> Result<PersistentStats, HyperDhtError> {
        let (tx, rx) = oneshot::channel();
        self.admin_tx
            .send(AdminRequest::PersistentStats { reply: tx })
            .map_err(|_| HyperDhtError::Destroyed)?;
        rx.await.map_err(|_| HyperDhtError::Destroyed)
    }

    /// Mark a target as having a local server available.
    pub fn register_server(&self, target: &[u8; 32]) {
        if let Ok(mut router) = self.router.lock() {
            router.set(
                target,
                ForwardEntry {
                    relay: None,
                    has_server: true,
                    inserted: std::time::Instant::now(),
                },
            );
        }
    }

    /// Remove the local-server marker for a target.
    pub fn unregister_server(&self, target: &[u8; 32]) {
        if let Ok(mut router) = self.router.lock() {
            router.delete(target);
        }
    }

    /// Access the server event sender.
    pub fn server_sender(&self) -> &mpsc::UnboundedSender<ServerEvent> {
        &self.server_tx
    }

    // ── CONNECT (client-side holepunch orchestration) ─────────────────────

    /// Connect to a remote peer using the DHT and relay fallback.
    pub async fn connect(
        &self,
        key_pair: &KeyPair,
        remote_public_key: [u8; 32],
        runtime: &UdxRuntime,
    ) -> Result<PeerConnection, HyperDhtError> {
        self.connect_with_nodes(key_pair, remote_public_key, &[], runtime)
            .await
    }

    /// Connect to a remote peer, optionally using known relay addresses first.
    ///
    /// Connection strategy (matches Node.js `findAndConnect` in
    /// `hyperdht/lib/connect.js`):
    /// 1. Try provided `relay_addresses` first (optimistic pre-connect).
    /// 2. Run FIND_PEER on hash(remote_pk); each responder is an FE-holder
    ///    (it returned a stored peer record), making it a known-good
    ///    PEER_HANDSHAKE forwarder. Try each `reply.from`, and additionally
    ///    each `peer.relay_addresses` entry from the stored records.
    /// 3. Fallback to FIND_NODE walk on hash(remote_pk) for nodes the DHT
    ///    routing table knows about but didn't store an FE.
    pub async fn connect_with_nodes(
        &self,
        key_pair: &KeyPair,
        remote_public_key: [u8; 32],
        relay_addresses: &[Ipv4Peer],
        runtime: &UdxRuntime,
    ) -> Result<PeerConnection, HyperDhtError> {
        self.connect_with_options(
            key_pair,
            remote_public_key,
            relay_addresses,
            runtime,
            ConnectOpts::default(),
        )
        .await
    }

    /// Like [`connect_with_nodes`] but accepts a [`ConnectOpts`] options bag.
    ///
    /// Use this when you need to disable the same-NAT LAN-shortcut
    /// (e.g. to force the real-network code path under test).
    pub async fn connect_with_options(
        &self,
        key_pair: &KeyPair,
        remote_public_key: [u8; 32],
        relay_addresses: &[Ipv4Peer],
        runtime: &UdxRuntime,
        opts: ConnectOpts,
    ) -> Result<PeerConnection, HyperDhtError> {
        let mut last_err = HyperDhtError::NoRelayNodes;
        let mut tried: Vec<(String, u16)> = Vec::new();

        // Phase 1: Optimistic pre-connect through provided relay addresses.
        for relay in relay_addresses {
            tried.push((relay.host.clone(), relay.port));
            match self.connect_through_node(key_pair, &remote_public_key, relay, runtime, &opts)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::debug!(relay = %format!("{}:{}", relay.host, relay.port), err = %e, "pre-connect relay attempt failed");
                    last_err = e;
                }
            }
        }

        let target = hash(&remote_public_key);

        // Phase 2: FIND_PEER on hash(pk). Responders are FE-holders (they
        // returned a stored peer record under this target). Their `reply.from`
        // address is a verified PEER_HANDSHAKE forwarder. This mirrors Node's
        // `findAndConnect` which does `dht.findPeer(c.target)` and tries each
        // `data.from`.
        let peer_replies = self
            .query_find_peer(target)
            .await
            .unwrap_or_else(|e| {
                tracing::debug!(err = %e, "connect_with_nodes: FIND_PEER query failed");
                Vec::new()
            });
        tracing::debug!(
            reply_count = peer_replies.len(),
            "connect_with_nodes: FIND_PEER completed"
        );

        for reply in &peer_replies {
            if reply.value.is_none() {
                continue;
            }
            if tried
                .iter()
                .any(|(h, p)| h == &reply.from.host && *p == reply.from.port)
            {
                continue;
            }
            tried.push((reply.from.host.clone(), reply.from.port));
            tracing::debug!(
                relay = %format!("{}:{}", reply.from.host, reply.from.port),
                "connect_with_nodes: trying FE-holder"
            );
            match self.connect_through_node(key_pair, &remote_public_key, &reply.from, runtime, &opts)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::debug!(
                        relay = %format!("{}:{}", reply.from.host, reply.from.port),
                        err = %e,
                        "FE-holder relay attempt failed"
                    );
                    last_err = e;
                }
            }
        }

        for reply in &peer_replies {
            if let Some(value) = &reply.value {
                if let Ok(peer) = decode_hyper_peer_from_bytes(value) {
                    for relay in &peer.relay_addresses {
                        if tried
                            .iter()
                            .any(|(h, p)| h == &relay.host && *p == relay.port)
                        {
                            continue;
                        }
                        tried.push((relay.host.clone(), relay.port));
                        match self.connect_through_node(key_pair, &remote_public_key, relay, runtime, &opts)
                            .await
                        {
                            Ok(result) => return Ok(result),
                            Err(e) => {
                                tracing::debug!(
                                    relay = %format!("{}:{}", relay.host, relay.port),
                                    err = %e,
                                    "peer-record relay attempt failed"
                                );
                                last_err = e;
                            }
                        }
                    }
                }
            }
        }

        // Phase 3: Fallback to FIND_NODE walk for closer nodes the DHT routing
        // table reaches but that didn't return an FE record (e.g. mid-refresh).
        let table_size = self.dht.table_size().await.unwrap_or(0);
        tracing::debug!(
            table_size,
            "connect_with_nodes: routing table size before FIND_NODE"
        );
        let node_replies = self.dht.find_node(target).await.map_err(HyperDhtError::Dht)?;
        tracing::debug!(
            reply_count = node_replies.len(),
            "connect_with_nodes: FIND_NODE completed"
        );

        if relay_addresses.is_empty() && peer_replies.is_empty() && node_replies.is_empty() {
            return Err(HyperDhtError::PeerNotFound);
        }

        let mut candidates: Vec<Ipv4Peer> = Vec::new();
        for reply in &node_replies {
            candidates.push(reply.from.clone());
            for cn in &reply.closer_nodes {
                if !candidates
                    .iter()
                    .any(|c| c.host == cn.host && c.port == cn.port)
                {
                    candidates.push(cn.clone());
                }
            }
        }
        tracing::debug!(
            candidate_count = candidates.len(),
            "connect_with_nodes: total FIND_NODE candidates (replies + closer_nodes)"
        );

        for (i, candidate) in candidates.iter().enumerate() {
            let skip = tried
                .iter()
                .any(|(h, p)| h == &candidate.host && *p == candidate.port);
            tracing::debug!(
                i,
                candidate = %format!("{}:{}", candidate.host, candidate.port),
                skip,
                "connect_with_nodes: candidate check"
            );
            if skip {
                continue;
            }
            tried.push((candidate.host.clone(), candidate.port));
            tracing::debug!(
                candidate = %format!("{}:{}", candidate.host, candidate.port),
                "connect_with_nodes: trying node candidate"
            );
            match self.connect_through_node(key_pair, &remote_public_key, candidate, runtime, &opts)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    tracing::debug!(
                        relay = %format!("{}:{}", candidate.host, candidate.port),
                        err = %e,
                        "find_node candidate attempt failed"
                    );
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }

    /// Connect directly to a peer at a known address, bypassing DHT routing.
    ///
    /// Sends a PEER_HANDSHAKE directly to `target_addr` for `remote_public_key`.
    /// Useful when the target's address is already known (e.g. from prior
    /// configuration or out-of-band exchange), avoiding the FIND_NODE phase
    /// that requires the target to be well-propagated in the DHT.
    pub async fn connect_to(
        &self,
        key_pair: &KeyPair,
        remote_public_key: [u8; 32],
        target_addr: std::net::SocketAddr,
        runtime: &UdxRuntime,
    ) -> Result<PeerConnection, HyperDhtError> {
        let relay = Ipv4Peer {
            host: target_addr.ip().to_string(),
            port: target_addr.port(),
        };
        self.connect_through_node(key_pair, &remote_public_key, &relay, runtime, &ConnectOpts::default())
            .await
    }

    async fn connect_through_node(
        &self,
        key_pair: &KeyPair,
        remote_public_key: &[u8; 32],
        relay: &Ipv4Peer,
        runtime: &UdxRuntime,
        opts: &ConnectOpts,
    ) -> Result<PeerConnection, HyperDhtError> {
        let target = hash(remote_public_key);

        // Phase 1: Noise IK handshake via PEER_HANDSHAKE relay
        let mut nw = NoiseWrap::new_initiator(key_pair.to_noise_keypair(), *remote_public_key);

        let local_stream_id = next_stream_id();

        // Advertise our own loopback address so a same-host server can pick
        // a local dial target. Mirrors Node `lib/connect.js` client noise
        // payload (`addresses4: addresses4`, populated from local IFs).
        let client_addresses4 = match self.dht.local_port().await {
            Ok(p) => vec![Ipv4Peer {
                host: "127.0.0.1".to_string(),
                port: p,
            }],
            Err(_) => vec![],
        };

        let local_payload = NoisePayload {
            version: 1,
            error: 0,
            firewall: FIREWALL_UNKNOWN,
            holepunch: None,
            addresses4: client_addresses4,
            addresses6: vec![],
            udx: Some(UdxInfo {
                version: 1,
                reusable_socket: true,
                id: u64::from(local_stream_id),
                seq: 0,
            }),
            secret_stream: Some(SecretStreamInfo { version: 1 }),
            relay_through: None,
            relay_addresses: None,
        };

        let noise_bytes = nw.send(&local_payload)?;
        let handshake_value =
            Router::encode_client_handshake(noise_bytes, None, None)?;

        let resp = self
            .dht
            .request(
                UserRequestParams {
                    token: None,
                    command: PEER_HANDSHAKE,
                    target: Some(target),
                    value: Some(handshake_value),
                    timeout_ms: Some(10000),
                    retries: Some(0),
                },
                &relay.host,
                relay.port,
            )
            .await?;

        if resp.error != 0 {
            return Err(HyperDhtError::HandshakeFailed(format!(
                "error code {}",
                resp.error
            )));
        }

        let reply_value = resp
            .value
            .ok_or_else(|| HyperDhtError::HandshakeFailed("empty reply".into()))?;

        let hs_result = {
            let router = self.router.lock().map_err(|_| HyperDhtError::ChannelClosed)?;
            router.validate_handshake_reply(&reply_value, relay, &resp.from)?
        };

        let remote_payload = nw.recv(&hs_result.noise)?;
        let nw_result = nw.finalize()?;

        if remote_payload.error != 0 {
            return Err(HyperDhtError::FirewallRejected);
        }

        // Check if the remote peer wants us to relay through a third node.
        if let Some(ref relay_through) = remote_payload.relay_through {
            let relay_addrs = remote_payload.relay_addresses.clone().unwrap_or_default();
            tracing::debug!(
                relay_pk = ?&relay_through.public_key[..8],
                relay_addr_hints = relay_addrs.len(),
                "remote requested relay_through"
            );
            return Box::pin(
                self.relay_connection(
                    key_pair,
                    relay_through,
                    &relay_addrs,
                    &nw_result,
                    false,
                    true,
                    runtime,
                ),
            )
            .await;
        }

        // Skip holepunching when the remote peer is directly reachable.
        // Node.js (connect.js) checks:
        //   payload.firewall === FIREWALL.OPEN  -- server says it's open
        //   (relayed && !remoteHolepunchable)   -- relayed but server has no HP relays
        //   same NAT (own remote addr matches server's reflexive)  -- LAN shortcut
        // In any of those cases, connect directly using the server address from the handshake.
        let remote_holepunchable = remote_payload
            .holepunch
            .as_ref()
            .is_some_and(|hp| !hp.relays.is_empty());

        // NAT-derived same-host check. `hs_result.client_address.host` is the
        // FE-holder's source IP (not our own public address), so comparing it
        // to `server_address.host` is always false on relayed handshakes and
        // cannot be the same-host signal. Use our own NAT-sampled public
        // address instead. Disabled when the caller opts out via
        // `local_connection=false`.
        let same_host = if !opts.local_connection {
            false
        } else {
            match self.dht.remote_address().await {
                Ok(Some(my_remote)) => my_remote.host == hs_result.server_address.host,
                _ => false,
            }
        };

        tracing::debug!(
            relayed = hs_result.relayed,
            firewall = remote_payload.firewall,
            remote_holepunchable,
            same_host,
            server_address = %format!("{}:{}", hs_result.server_address.host, hs_result.server_address.port),
            "handshake complete, deciding connection path"
        );

        if should_direct_connect(
            hs_result.relayed,
            remote_payload.firewall,
            remote_holepunchable,
            same_host,
        ) {
            // Pick the dial target. When same-host is true (both peers
            // share a NAT), prefer a private/loopback address from the
            // remote's advertised list, since the public IP often can't be
            // reached from inside the same NAT (no NAT hairpin). Otherwise
            // prefer a non-private public address, falling back to the
            // FE-holder's-tagged server address. Mirrors Node
            // `lib/connect.js::holepunch` LAN-shortcut.
            let connect_addr = if same_host {
                remote_payload
                    .addresses4
                    .iter()
                    .find(|a| is_addr_private(&a.host))
                    .cloned()
                    .or_else(|| {
                        remote_payload
                            .addresses4
                            .iter()
                            .find(|a| !is_addr_private(&a.host))
                            .cloned()
                    })
                    .unwrap_or_else(|| hs_result.server_address.clone())
            } else {
                remote_payload
                    .addresses4
                    .iter()
                    .find(|a| !is_addr_private(&a.host))
                    .cloned()
                    .unwrap_or_else(|| hs_result.server_address.clone())
            };
            tracing::debug!(
                same_host,
                connect_addr = %format!("{}:{}", connect_addr.host, connect_addr.port),
                server_address = %format!("{}:{}", hs_result.server_address.host, hs_result.server_address.port),
                advertised = ?remote_payload.addresses4.iter().map(|a| format!("{}:{}", a.host, a.port)).collect::<Vec<_>>(),
                "direct-connect dial target chosen"
            );

            let direct = ConnectResult {
                remote_public_key: nw_result.remote_public_key,
                server_address: connect_addr,
                client_address: hs_result.client_address,
                is_relayed: false,
            noise: nw_result,
            local_stream_id,
            remote_udx: remote_payload.udx.clone(),
            udx_socket: None,
        };
            let shared = self.server_socket().await?;
            return establish_stream_with_socket(&direct, runtime, shared).await;
        }

        // Phase 2: Holepunch rounds via PEER_HOLEPUNCH relay
        let server_address = hs_result.server_address.clone();
        let hp_result = self
            .run_holepunch_rounds(
                &nw_result,
                &remote_payload,
                relay,
                &target,
                &server_address,
                runtime,
                local_stream_id,
            )
            .await?;
        let shared = match hp_result.udx_socket.clone() {
            Some(s) => Some(s),
            None => self.server_socket().await?,
        };
        establish_stream_with_socket(&hp_result, runtime, shared).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_holepunch_rounds(
        &self,
        nw_result: &NoiseWrapResult,
        remote_payload: &NoisePayload,
        relay: &Ipv4Peer,
        target: &[u8; 32],
        _server_address: &Ipv4Peer,
        runtime: &UdxRuntime,
        local_stream_id: u32,
    ) -> Result<ConnectResult, HyperDhtError> {
        // Node-shaped multi-round client holepunch loop.
        //
        // The previous (legacy) implementation sent exactly two PEER_HOLEPUNCH
        // messages — a probe (`punching=false`) and an immediate punch
        // (`punching=true`) — then waited for the local Holepuncher to emit a
        // Connected event. That does not match Node's `lib/connect.js`
        // semantics and fails against passive Node servers (and against our
        // own swarm-side passive holepunch server).
        //
        // The flow below mirrors Node:
        //   1. Up to `HOLEPUNCH_MAX_PROBE_ROUNDS` probe rounds, each with
        //      `punching=false`. After every reply we feed the new info into
        //      the Holepuncher via `update_remote(...)` and ask `analyze(false)`
        //      whether the local NAT state is stable enough to punch.
        //   2. A reply is only treated as "verified" when its `remote_token`
        //      echoes the token we sent on the same round — only the
        //      legitimate server could echo our random token through the
        //      encrypted FE-holder relay.
        //   3. Once we have at least one verified remote address AND
        //      `analyze(false)` is true we send a single final
        //      `punching=true` round, then race the local `puncher.punch()`
        //      against incoming `HolepunchEvent::Connected`/`Aborted` events,
        //      bounded by `HOLEPUNCH_TOTAL_DEADLINE`.
        //   4. `reply.error` is decoded each round: `ERROR_NONE` continues,
        //      `ERROR_TRY_LATER` is retried in the next probe round,
        //      `ERROR_ABORTED` returns `HolepunchAborted`, anything else
        //      returns `HolepunchFailed`.

        /// Maximum wall-clock time spent in the full holepunch flow (probes +
        /// final punch + waiting for Connected).
        const HOLEPUNCH_TOTAL_DEADLINE: std::time::Duration =
            std::time::Duration::from_secs(30);
        /// Maximum number of `punching=false` probe rounds before we either
        /// transition to the final punch round or give up.
        const HOLEPUNCH_MAX_PROBE_ROUNDS: u32 = 3;

        let sp = SecurePayload::new(nw_result.holepunch_secret);
        let pool = SocketPool::new("0.0.0.0".into());
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        let hp_id = remote_payload
            .holepunch
            .as_ref()
            .map_or(0, |hp| hp.id);

        let mut puncher = Holepuncher::new(
            &pool,
            runtime,
            true,
            true,
            remote_payload.firewall,
            event_tx,
        )
        .await
        .map_err(|_| HyperDhtError::HolepunchFailed)?;

        let deadline = tokio::time::Instant::now() + HOLEPUNCH_TOTAL_DEADLINE;

        // State carried between probe rounds.
        let mut cached_reply_token: Option<[u8; 32]> = None;
        let mut verified_remote_addr: Option<Ipv4Peer> = None;
        let mut stable = false;
        let mut last_round: u64 = 0;

        for round in 0u64..=u64::from(HOLEPUNCH_MAX_PROBE_ROUNDS) {
            last_round = round;

            // Fresh outbound token per round; the reply must echo this back
            // in `remote_token` for us to consider the host verified.
            let our_outbound_token: [u8; 32] = rand::random();

            let probe_payload = HolepunchPayload {
                error: ERROR_NONE,
                firewall: puncher.nat.firewall,
                round,
                connected: false,
                punching: false,
                addresses: Some(puncher.nat.addresses.clone().unwrap_or_default()),
                remote_address: None,
                token: Some(our_outbound_token),
                remote_token: cached_reply_token,
            };

            let encrypted_probe = sp.encrypt(&probe_payload)?;
            let hp_value = Router::encode_client_holepunch(hp_id, encrypted_probe, None)?;

            tracing::info!(round, hp_id, "probe sent");

            let hp_resp = self
                .dht
                .request(
                    UserRequestParams {
                        token: None,
                        command: PEER_HOLEPUNCH,
                        target: Some(*target),
                        value: Some(hp_value),
                        timeout_ms: None,
                        retries: None,
                    },
                    &relay.host,
                    relay.port,
                )
                .await?;

            if hp_resp.error != 0 {
                puncher.destroy();
                return Err(HyperDhtError::HolepunchFailed);
            }

            let Some(reply_value) = &hp_resp.value else {
                puncher.destroy();
                return Err(HyperDhtError::HolepunchFailed);
            };

            let hp_result = {
                let router = self
                    .router
                    .lock()
                    .map_err(|_| HyperDhtError::ChannelClosed)?;
                router.validate_holepunch_reply(reply_value, relay, &hp_resp.from, relay)?
            };

            let reply_hp = sp.decrypt(&hp_result.payload)?;

            match reply_hp.error {
                ERROR_NONE => {}
                ERROR_TRY_LATER => {
                    tracing::info!(round, "reply ERROR_TRY_LATER, continuing");
                    cached_reply_token = reply_hp.token;
                    continue;
                }
                ERROR_ABORTED => {
                    puncher.destroy();
                    return Err(HyperDhtError::HolepunchAborted);
                }
                other => {
                    tracing::warn!(round, error = other, "holepunch reply server error");
                    puncher.destroy();
                    return Err(HyperDhtError::HolepunchFailed);
                }
            }

            // Token-echo verification: only consider the host verified if
            // the reply echoes back our outbound token in `remote_token`.
            let token_matches = reply_hp.remote_token == Some(our_outbound_token);
            let verified_host = if token_matches {
                Some(hp_result.peer_address.host.as_str())
            } else {
                None
            };

            tracing::info!(
                round,
                punching = reply_hp.punching,
                firewall = reply_hp.firewall,
                addresses = reply_hp.addresses.as_ref().map_or(0, |a| a.len()),
                verified = token_matches,
                "probe reply"
            );

            if let Some(addrs) = &reply_hp.addresses {
                puncher.update_remote(
                    reply_hp.punching,
                    reply_hp.firewall,
                    addrs,
                    verified_host,
                );
            }

            if token_matches {
                verified_remote_addr = Some(hp_result.peer_address.clone());
            }
            cached_reply_token = reply_hp.token;

            if verified_remote_addr.is_some() && puncher.analyze(false).await {
                stable = true;
                tracing::info!(
                    round,
                    "analyze stable=true, transitioning to final punch round"
                );
                break;
            }
        }

        // Probe budget exhausted without stabilizing — give analyze one last
        // chance with the `stabilize=true` flag before failing.
        if !stable && !puncher.analyze(true).await {
            tracing::warn!("holepunch probe rounds exhausted, NAT state unstable");
            puncher.destroy();
            return Err(HyperDhtError::HolepunchFailed);
        }

        let Some(verified_addr) = verified_remote_addr else {
            tracing::warn!("no verified remote address after probe rounds");
            puncher.destroy();
            return Err(HyperDhtError::HolepunchFailed);
        };

        // ── Final punch round (single round with punching=true) ─────────────
        let final_round = last_round.saturating_add(1);
        let final_outbound_token: [u8; 32] = rand::random();
        let punch_payload = HolepunchPayload {
            error: ERROR_NONE,
            firewall: puncher.nat.firewall,
            round: final_round,
            connected: false,
            punching: true,
            addresses: Some(puncher.nat.addresses.clone().unwrap_or_default()),
            remote_address: Some(verified_addr.clone()),
            token: Some(final_outbound_token),
            remote_token: cached_reply_token,
        };

        let encrypted_punch = sp.encrypt(&punch_payload)?;
        let hp_punch_value = Router::encode_client_holepunch(hp_id, encrypted_punch, None)?;

        tracing::info!(
            round = final_round,
            verified_addr = %format!("{}:{}", verified_addr.host, verified_addr.port),
            "final punch sent, waiting for Connected event"
        );

        let punch_resp = self
            .dht
            .request(
                UserRequestParams {
                    token: None,
                    command: PEER_HOLEPUNCH,
                    target: Some(*target),
                    value: Some(hp_punch_value),
                    timeout_ms: None,
                    retries: None,
                },
                &relay.host,
                relay.port,
            )
            .await?;

        if let Some(reply_value) = &punch_resp.value {
            let hp_result = {
                let router = self
                    .router
                    .lock()
                    .map_err(|_| HyperDhtError::ChannelClosed)?;
                router.validate_holepunch_reply(reply_value, relay, &punch_resp.from, relay)?
            };

            if let Ok(reply_hp) = sp.decrypt(&hp_result.payload) {
                let verified_host = if reply_hp.remote_token == Some(final_outbound_token) {
                    Some(hp_result.peer_address.host.as_str())
                } else {
                    None
                };
                if let Some(addrs) = &reply_hp.addresses {
                    puncher.update_remote(
                        reply_hp.punching,
                        reply_hp.firewall,
                        addrs,
                        verified_host,
                    );
                }
            }
        }

        // Spawn the local punch loop and race it against Connected / Aborted
        // events and the global deadline. The pinned `punch_fut` mutably
        // borrows `puncher` for the duration of the inner scope; on timeout
        // we exit the scope so the borrow is released before we call
        // `puncher.destroy()`.

        // Clone the puncher's primary UDX socket BEFORE punch_fut takes &mut
        // puncher, so it is available inside the Connected arm below.
        //
        // Per D8 (Node connect.js): the initiator's rawStream attaches to the
        // SAME socket the puncher used — that is where the NAT mapping was
        // opened by the probe exchange. Using the DHT server socket would send
        // the UDX SYN through an unrelated NAT path and never reach the peer.
        //
        // The Arc-clone keeps the socket alive after the Holepuncher is
        // dropped on the early-return; SocketPool::acquire's route_messages
        // task continues running as long as one clone exists.
        let puncher_udx: Option<UdxSocket> = puncher
            .primary_socket()
            .map(|sr| sr.socket.clone());
        debug_assert!(
            puncher_udx.is_some(),
            "primary socket missing before punch loop — logic error"
        );

        let timed_out = {
            let punch_fut = puncher.punch(&pool, runtime);
            tokio::pin!(punch_fut);
            let mut punch_done = false;
            let deadline_sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(deadline_sleep);

            loop {
                tokio::select! {
                    ev = event_rx.recv() => {
                        match ev {
                            Some(HolepunchEvent::Connected { addr }) => {
                                tracing::info!(from = %addr, "punch successful");
                                let connected_addr = Ipv4Peer {
                                    host: addr.ip().to_string(),
                                    port: addr.port(),
                                };
                                return Ok(ConnectResult {
                                    remote_public_key: nw_result.remote_public_key,
                                    server_address: connected_addr.clone(),
                                    client_address: connected_addr,
                                    is_relayed: true,
                                    noise: nw_result.clone(),
                                    local_stream_id,
                                    remote_udx: remote_payload.udx.clone(),
                                    udx_socket: puncher_udx,
                                });
                            }
                            Some(HolepunchEvent::Aborted) | None => {
                                tracing::info!("punch aborted");
                                return Err(HyperDhtError::HolepunchAborted);
                            }
                        }
                    }
                    _ = punch_fut.as_mut(), if !punch_done => {
                        punch_done = true;
                        tracing::info!("local punch loop completed, awaiting Connected event");
                    }
                    _ = deadline_sleep.as_mut() => {
                        break true;
                    }
                }
            }
        };

        if timed_out {
            tracing::info!("punch deadline elapsed");
            puncher.destroy();
            return Err(HyperDhtError::HolepunchFailed);
        }

        unreachable!("holepunch select loop exited without return or timeout");
    }

    /// Establish an encrypted connection to a peer via a relay node.
    ///
    /// The relay node forwards raw UDX packets between the two peers using
    /// the blind-relay protocol. The returned [`PeerConnection`] is encrypted
    /// end-to-end with the original peer's keys (the relay cannot read the data).
    ///
    /// `relay_addr_hints` are optional addresses where the relay may be reachable
    /// directly (e.g. from the server's NoisePayload `relay_addresses`). They are
    /// tried first before falling back to full DHT routing.
    #[allow(clippy::too_many_arguments)]
    async fn relay_connection(
        &self,
        key_pair: &KeyPair,
        relay_through: &RelayThroughInfo,
        relay_addr_hints: &[Ipv4Peer],
        noise_result: &NoiseWrapResult,
        relay_is_initiator: bool,
        noise_is_initiator: bool,
        runtime: &UdxRuntime,
    ) -> Result<PeerConnection, HyperDhtError> {
        // 1. HyperDHT connection to the relay node.
        // Try known addresses first (pre-connect), then fall back to DHT routing.
        // Node.js does `dht.connect(publicKey)` — we enhance with address hints.
        let relay_conn = self
            .connect_with_nodes(key_pair, relay_through.public_key, relay_addr_hints, runtime)
            .await?;

        let relay_addr = relay_conn.remote_addr.ok_or_else(|| {
            HyperDhtError::StreamEstablishment("relay connection has no remote_addr".into())
        })?;

        // 2. Protomux over the control channel.
        let (mux, mux_run) = Mux::new(relay_conn.stream);
        let mux_task = tokio::spawn(mux_run);

        // 3. Open blind-relay client with our public key as channel id.
        // The relay server uses `id = socket.remotePublicKey` (our key).
        let mut relay_client =
            BlindRelayClient::open(&mux, Some(key_pair.public_key.to_vec())).await?;
        relay_client.wait_opened().await?;

        let data_stream_id = next_stream_id();

        let pair_response = relay_client
            .pair(
                relay_is_initiator,
                &relay_through.token,
                u64::from(data_stream_id),
            )
            .await?;

        let remote_id = u32::try_from(pair_response.remote_id).map_err(|_| {
            HyperDhtError::StreamEstablishment("relay remote_id out of u32 range".into())
        })?;

        // 4. Connect data UDX stream through the relay, reusing the control
        //    channel's socket so the relay sees traffic from the same source address.
        let data_stream = runtime.create_stream(data_stream_id).await?;
        data_stream
            .connect(&relay_conn.socket, remote_id, relay_addr)
            .await?;

        // 5. Wrap with SecretStream::from_session using the original peer's
        //    Noise keys (end-to-end encryption through the relay).
        let async_stream = data_stream.into_async_stream();
        let ss = SecretStream::from_session(
            noise_is_initiator,
            async_stream,
            noise_result.tx,
            noise_result.rx,
            noise_result.handshake_hash,
            noise_result.remote_public_key,
        )
        .await?;

        Ok(PeerConnection {
            stream: ss,
            remote_public_key: noise_result.remote_public_key,
            remote_addr: Some(relay_addr),
            socket: relay_conn.socket,
            _relay_task: Some(mux_task),
        })
    }
}

/// Create a UDX stream, connect it to the remote peer, and wrap with
/// [`SecretStream::from_session`] using the Noise handshake keys.
///
/// Call after [`HyperDhtHandle::connect`] to upgrade a [`ConnectResult`]
/// into an encrypted bidirectional stream.
///
/// A fresh UDX socket bound to an ephemeral port is created for the stream.
/// To reuse an existing socket (Node.js-style single-socket multiplexing),
/// use [`establish_stream_with_socket`] instead.
pub async fn establish_stream(
    result: &ConnectResult,
    runtime: &UdxRuntime,
) -> Result<PeerConnection, HyperDhtError> {
    establish_stream_with_socket(result, runtime, None).await
}

/// Call after [`HyperDhtHandle::connect`] to upgrade a [`ConnectResult`]
/// into an encrypted bidirectional stream, optionally reusing an existing socket.
///
/// When `shared_socket` is `Some`, the stream reuses that socket (matching
/// the Node.js single-socket multiplexing model). When `None`, a fresh socket
/// bound to an ephemeral port is created.
pub async fn establish_stream_with_socket(
    result: &ConnectResult,
    runtime: &UdxRuntime,
    shared_socket: Option<UdxSocket>,
) -> Result<PeerConnection, HyperDhtError> {
    let remote_udx = result
        .remote_udx
        .as_ref()
        .ok_or_else(|| HyperDhtError::StreamEstablishment("no remote UDX info".into()))?;

    let remote_id = u32::try_from(remote_udx.id)
        .map_err(|_| HyperDhtError::StreamEstablishment("remote UDX id out of u32 range".into()))?;

    let addr: SocketAddr = SocketAddr::new(
        result
            .server_address
            .host
            .parse()
            .map_err(|e| HyperDhtError::StreamEstablishment(format!("invalid address: {e}")))?,
        result.server_address.port,
    );

    tracing::debug!(local_id = result.local_stream_id, remote_id, %addr, "establishing UDX stream");
    let socket = if let Some(s) = shared_socket {
        s
    } else {
        let s = runtime.create_socket().await?;
        s.bind("0.0.0.0:0".parse().expect("valid addr")).await?;
        s
    };
    let stream = runtime.create_stream(result.local_stream_id).await?;
    stream.connect(&socket, remote_id, addr).await?;

    let async_stream = stream.into_async_stream();
    let ss = SecretStream::from_session(
        result.noise.is_initiator,
        async_stream,
        result.noise.tx,
        result.noise.rx,
        result.noise.handshake_hash,
        result.noise.remote_public_key,
    )
    .await?;
    tracing::debug!("SecretStream established");

    Ok(PeerConnection {
        remote_public_key: result.remote_public_key,
        stream: ss,
        remote_addr: Some(addr),
        socket,
        _relay_task: None,
    })
}

// ── Server-side event handler ─────────────────────────────────────────────────

/// Per-server state for pending handshake and holepunch exchanges.
pub struct ServerSession {
    /// Cached holepunch secrets indexed by remote public key.
    holepunch_secrets: std::collections::HashMap<[u8; 32], ServerPeerState>,
}

#[allow(dead_code)]
struct ServerPeerState {
    holepunch_secret: [u8; 32],
    remote_public_key: [u8; 32],
    client_address: Ipv4Peer,
    local_stream_id: u32,
    remote_udx: Option<UdxInfo>,
}

/// Run the server-side request loop for peer handshakes and holepunches.
pub async fn run_server(
    mut event_rx: mpsc::UnboundedReceiver<ServerEvent>,
    config: ServerConfig,
    runtime: UdxRuntime,
) {
    let mut session = ServerSession {
        holepunch_secrets: std::collections::HashMap::new(),
    };
    let pool = SocketPool::new("0.0.0.0".into());

    while let Some(event) = event_rx.recv().await {
        match event {
            ServerEvent::PeerHandshake {
                msg,
                from,
                peer_address: _,
                target,
                reply_tx,
            } => {
                let reply = handle_server_handshake(
                    &config,
                    &mut session,
                    msg,
                    &from,
                    target.as_ref(),
                );
                let _ = reply_tx.send(reply);
            }
            ServerEvent::PeerHolepunch {
                msg,
                from: _,
                peer_address,
                target: _,
                reply_tx,
            } => {
                let holepunch_secret =
                    session.holepunch_secrets.values().find_map(|state| {
                        let sp = SecurePayload::new(state.holepunch_secret);
                        if sp.decrypt(&msg.payload).is_ok() {
                            Some(state.holepunch_secret)
                        } else {
                            None
                        }
                    });
                let reply: Option<Vec<u8>> = if let Some(secret) = holepunch_secret {
                    let (event_tx, _event_rx) = mpsc::unbounded_channel();
                    match build_passive_holepunch_reply(
                        config.firewall,
                        &pool,
                        &runtime,
                        &peer_address,
                        secret,
                        &msg.payload,
                        msg.id,
                        event_tx,
                    )
                    .await
                    {
                        Ok(result) => {
                            if result.remote_hp.punching {
                                let pool_clone = SocketPool::new("0.0.0.0".into());
                                let mut puncher = result.puncher;
                                tokio::spawn(async move {
                                    if let Ok(rt) = UdxRuntime::new() {
                                        puncher.punch(&pool_clone, &rt).await;
                                    }
                                });
                            }
                            Some(result.reply_bytes)
                        }
                        Err(_) => None,
                    }
                } else {
                    None
                };
                let _ = reply_tx.send(reply);
            }
        }
    }
}

fn handle_server_handshake(
    config: &ServerConfig,
    session: &mut ServerSession,
    msg: HandshakeMessage,
    from: &Ipv4Peer,
    _target: Option<&NodeId>,
) -> Option<Vec<u8>> {
    let mut nw = NoiseWrap::new_responder(config.key_pair.to_noise_keypair());

    let remote_payload = match nw.recv(&msg.noise) {
        Ok(p) => p,
        Err(_) => return None,
    };

    if remote_payload.error != 0 {
        return None;
    }

    let local_stream_id = next_stream_id();

    let reply_payload = NoisePayload {
        version: 1,
        error: 0,
        firewall: config.firewall,
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
        relay_through: None,
        relay_addresses: None,
    };

    let noise_reply = match nw.send(&reply_payload) {
        Ok(b) => b,
        Err(_) => return None,
    };

    let nw_result = match nw.finalize() {
        Ok(r) => r,
        Err(_) => return None,
    };

    session.holepunch_secrets.insert(
        nw_result.remote_public_key,
        ServerPeerState {
            holepunch_secret: nw_result.holepunch_secret,
            remote_public_key: nw_result.remote_public_key,
            client_address: from.clone(),
            local_stream_id,
            remote_udx: remote_payload.udx.clone(),
        },
    );

    let reply_msg = HandshakeMessage {
        mode: crate::hyperdht_messages::MODE_REPLY,
        noise: noise_reply,
        peer_address: Some(from.clone()),
        relay_address: None,
    };

    crate::hyperdht_messages::encode_handshake_to_bytes(&reply_msg).ok()
}

/// Result returned by [`build_passive_holepunch_reply`].
#[non_exhaustive]
pub struct PassiveHolepunchReply {
    /// Fully-encoded [`HolepunchMessage`] bytes ready to send on the wire.
    pub reply_bytes: Vec<u8>,
    /// Decrypted incoming holepunch payload from the remote peer.
    pub remote_hp: HolepunchPayload,
    /// The [`SecurePayload`] instance used for this exchange; reuse it across rounds for token continuity.
    pub payload: SecurePayload,
    /// Constructed [`Holepuncher`]; caller decides whether to spawn `punch()` or retain it.
    pub puncher: Holepuncher,
}

/// Build a passive (server-side) holepunch reply from raw encrypted bytes.
#[allow(clippy::too_many_arguments)]
pub async fn build_passive_holepunch_reply(
    config_firewall: u64,
    pool: &SocketPool,
    runtime: &UdxRuntime,
    peer_address: &Ipv4Peer,
    holepunch_secret: [u8; 32],
    incoming_payload: &[u8],
    msg_id: u64,
    event_tx: mpsc::UnboundedSender<HolepunchEvent>,
) -> Result<PassiveHolepunchReply, HyperDhtError> {
    let sp = SecurePayload::new(holepunch_secret);
    let remote_hp = sp.decrypt(incoming_payload)?;

    let reply_hp = HolepunchPayload {
        error: 0,
        firewall: config_firewall,
        round: remote_hp.round,
        connected: false,
        punching: remote_hp.punching,
        addresses: Some(vec![peer_address.clone()]),
        remote_address: Some(peer_address.clone()),
        token: Some(sp.token(&peer_address.host)),
        remote_token: remote_hp.token,
    };

    let encrypted_reply = sp.encrypt(&reply_hp)?;

    let mut puncher = Holepuncher::new(pool, runtime, true, false, remote_hp.firewall, event_tx)
        .await
        .map_err(|_| HyperDhtError::HolepunchFailed)?;

    if let Some(addrs) = &remote_hp.addresses {
        puncher.update_remote(true, remote_hp.firewall, addrs, Some(peer_address.host.as_str()));
    }

    let reply_msg = HolepunchMessage {
        mode: crate::hyperdht_messages::MODE_REPLY,
        id: msg_id,
        payload: encrypted_reply,
        peer_address: Some(peer_address.clone()),
    };

    let reply_bytes = crate::hyperdht_messages::encode_holepunch_msg_to_bytes(&reply_msg)?;

    Ok(PassiveHolepunchReply {
        reply_bytes,
        remote_hp,
        payload: sp,
        puncher,
    })
}

// ── Spawn ─────────────────────────────────────────────────────────────────────

/// Create a HyperDHT instance and start its background tasks.
pub async fn spawn(
    runtime: &UdxRuntime,
    config: HyperDhtConfig,
) -> Result<
    (
        JoinHandle<Result<(), HyperDhtError>>,
        HyperDhtHandle,
        mpsc::UnboundedReceiver<ServerEvent>,
    ),
    HyperDhtError,
> {
    let (dht_join, dht_handle) = crate::rpc::spawn(runtime, config.dht).await?;
    let persistent_config = config.persistent;

    let request_rx = dht_handle
        .subscribe_requests()
        .await
        .ok_or(HyperDhtError::ChannelClosed)?;

    let router = Arc::new(Mutex::new(Router::new()));
    let (server_tx, server_rx) = mpsc::unbounded_channel();
    let (admin_tx, admin_rx) = mpsc::unbounded_channel::<AdminRequest>();

    let request_task = tokio::spawn(run_request_handler(
        request_rx,
        persistent_config,
        dht_handle.clone(),
        Arc::clone(&router),
        server_tx.clone(),
        admin_rx,
    ));

    let join = tokio::spawn(async move {
        tokio::select! {
            res = dht_join => {
                match res {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(HyperDhtError::Dht(e)),
                    Err(_) => Err(HyperDhtError::ChannelClosed),
                }
            }
            res = request_task => {
                match res {
                    Ok(()) => Ok(()),
                    Err(_) => Err(HyperDhtError::ChannelClosed),
                }
            }
        }
    });

    let handle = HyperDhtHandle {
        dht: dht_handle,
        router,
        server_tx,
        admin_tx,
        current_relay_addresses: Arc::new(Mutex::new(Vec::new())),
    };
    Ok((join, handle, server_rx))
}

async fn run_request_handler(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<crate::rpc::UserRequest>,
    config: PersistentConfig,
    dht: DhtHandle,
    router: Arc<Mutex<Router>>,
    server_tx: mpsc::UnboundedSender<ServerEvent>,
    mut admin_rx: mpsc::UnboundedReceiver<AdminRequest>,
) {
    let mut storage = Persistent::new(config);

    loop {
        let mut req = tokio::select! {
            biased;
            Some(admin_req) = admin_rx.recv() => {
                match admin_req {
                    AdminRequest::PersistentStats { reply } => {
                        let _ = reply.send(storage.stats());
                    }
                }
                continue;
            }
            req = rx.recv() => match req {
                Some(r) => r,
                None => break,
            },
        };
        match req.command {
            PEER_HANDSHAKE => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: PEER_HANDSHAKE");
                handle_peer_handshake(req, &dht, &router, &server_tx);
                continue;
            }
            PEER_HOLEPUNCH => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: PEER_HOLEPUNCH");
                handle_peer_holepunch(req, &dht, &router, &server_tx);
                continue;
            }
            _ => {}
        }

        let node_id = req.id;

        let incoming = IncomingHyperRequest {
            command: req.command,
            target: req.target,
            token: req.token,
            value: req.value.clone(),
            from: req.from.clone(),
            id: node_id,
        };

        let reply = match req.command {
            FIND_PEER => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: FIND_PEER");
                storage.on_find_peer(&incoming)
            }
            LOOKUP => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: LOOKUP");
                storage.on_lookup(&incoming)
            }
            ANNOUNCE => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: ANNOUNCE");
                let own_id = dht.table_id().await.ok().flatten();
                if let Some(nid) = own_id {
                    let result = storage.on_announce(&incoming, &nid);
                    if !matches!(result, HandlerReply::Silent) {
                        if let Some(target) = incoming.target {
                            if let Ok(mut r) = router.lock() {
                                let already_server = r.get(&target).is_some_and(|e| e.has_server);
                                if !already_server {
                                    r.set(
                                        &target,
                                        ForwardEntry {
                                            relay: Some(incoming.from.clone()),
                                            has_server: false,
                                            inserted: std::time::Instant::now(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                    result
                } else {
                    HandlerReply::Silent
                }
            }
            UNANNOUNCE => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: UNANNOUNCE");
                let own_id = dht.table_id().await.ok().flatten();
                if let Some(nid) = own_id {
                    storage.on_unannounce(&incoming, &nid)
                } else {
                    HandlerReply::Silent
                }
            }
            MUTABLE_PUT => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: MUTABLE_PUT");
                storage.on_mutable_put(&incoming)
            }
            MUTABLE_GET => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: MUTABLE_GET");
                storage.on_mutable_get(&incoming)
            }
            IMMUTABLE_PUT => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: IMMUTABLE_PUT");
                storage.on_immutable_put(&incoming)
            }
            IMMUTABLE_GET => {
                tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "request: IMMUTABLE_GET");
                storage.on_immutable_get(&incoming)
            }
            _ => {
                tracing::debug!(cmd = req.command, from = %format!("{}:{}", req.from.host, req.from.port), "request: unknown command");
                drop(req);
                continue;
            }
        };

        match reply {
            HandlerReply::Value(v) | HandlerReply::ValueNoToken(v) => {
                req.reply(v);
            }
            HandlerReply::Error(code) => {
                req.error(code);
            }
            HandlerReply::Silent => {
                drop(req);
            }
        }
    }
}

fn handle_peer_handshake(
    mut req: crate::rpc::UserRequest,
    dht: &DhtHandle,
    router: &Arc<Mutex<Router>>,
    server_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let Some(value) = &req.value else {
        req.error(1);
        return;
    };

    let action = {
        let router = match router.lock() {
            Ok(r) => r,
            Err(_) => {
                req.error(1);
                return;
            }
        };
        match router.route_handshake(req.target.as_ref(), &req.from, value) {
            Ok(a) => a,
            Err(_) => {
                req.error(1);
                return;
            }
        }
    };

    match action {
        HandshakeAction::Reply(value) => {
            tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "handshake REPLY");
            req.reply(Some(value));
        }
        HandshakeAction::ForwardRequest { value, to } => {
            // FE-holder relay: forward the inbound REQUEST as a new REQUEST
            // to `to`, preserving the original requester's tid end-to-end.
            // Fire-and-forget — the eventual REPLY will reach the original
            // client directly via the tid-preserved chain (server replies
            // with `dht.relay_with_tid` back to this FE-holder, which then
            // dispatches HandshakeAction::ReplyTo to send the REPLY packet
            // straight to the client at peer_address). Mirrors Node
            // `dht-rpc::Request.relay(value, to)`.
            tracing::info!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "handshake FORWARD_REQUEST — tid-preserved relay"
            );
            if let Err(e) = dht.relay_with_tid(
                PEER_HANDSHAKE,
                req.target,
                Some(value),
                &to,
                req.tid,
            ) {
                tracing::debug!(err = %e, "ForwardRequest: relay_with_tid send failed");
            }
            // No reply to the inbound REQUEST — the original client awaits
            // its REPLY via the preserved-tid chain, not via this hop.
            req.release();
        }
        HandshakeAction::ReplyTo { value, to } => {
            // FE-holder finalises the relay chain: send a REPLY packet
            // (Response, not Request) with the inbound tid (= original
            // client's tid) directly to the client at `to`. The REPLY
            // matches the client's outstanding inflight entry. Mirrors
            // Node `dht-rpc::Request.reply(value, { to })` as invoked from
            // `lib/server.js _addHandshake case FROM_SERVER`.
            tracing::info!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "handshake REPLY_TO — finalise tid-preserved chain"
            );
            if let Err(e) = dht.send_reply_to(req.tid, req.target, &to, Some(value)) {
                tracing::debug!(err = %e, "ReplyTo: send_reply_to failed");
            }
            // The inbound REQUEST came from the server via its relay_with_tid;
            // the server is not awaiting a reply, so we do not call
            // req.reply / req.error here.
            req.release();
        }
        HandshakeAction::Relay { value, to } => {
            // Legacy variant — the router no longer emits this for handshakes.
            // Retained for forward-compatibility (non_exhaustive enum) so any
            // external producer continues to function. Treat as fire-and-forget
            // tid-preserved relay (equivalent to ForwardRequest).
            tracing::warn!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "handshake legacy Relay action — treating as ForwardRequest"
            );
            if let Err(e) = dht.relay_with_tid(
                PEER_HANDSHAKE,
                req.target,
                Some(value),
                &to,
                req.tid,
            ) {
                tracing::debug!(err = %e, "legacy Relay: relay_with_tid send failed");
            }
            req.release();
        }
        HandshakeAction::HandleLocally(msg) => {
            tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), inbound_mode = msg.mode, "handshake HANDLE_LOCALLY");
            let (reply_tx, reply_rx) = oneshot::channel();
            let from = req.from.clone();
            let target = req.target;
            let peer_address = msg.peer_address.clone();
            // FROM_CLIENT direct handshakes are answered via the inbound
            // request's REPLY channel (`req.reply`). FROM_RELAY /
            // FROM_SECOND_RELAY handshakes arrive as REQUESTs whose tid was
            // preserved by the FE-holder via `req.relay`-style forwarding;
            // we mirror Node `lib/server.js _addHandshake case FROM_RELAY`
            // by calling `dht.relay_with_tid(...)` to push a tid-preserved
            // FROM_SERVER REQUEST back to the FE-holder. The FE-holder then
            // finalises the chain by emitting the REPLY directly to the
            // original client (see HandshakeAction::ReplyTo above).
            let inbound_mode = msg.mode;
            let inbound_tid = req.tid;

            let sent = server_tx
                .send(ServerEvent::PeerHandshake {
                    msg,
                    from,
                    peer_address,
                    target,
                    reply_tx,
                })
                .is_ok();

            if sent {
                let dht = dht.clone();
                tokio::spawn(async move {
                    match reply_rx.await {
                        Ok(Some(value)) => match inbound_mode {
                            crate::hyperdht_messages::MODE_FROM_CLIENT => req.reply(Some(value)),
                            crate::hyperdht_messages::MODE_FROM_RELAY
                            | crate::hyperdht_messages::MODE_FROM_SECOND_RELAY => {
                                if let Err(e) = dht.relay_with_tid(
                                    PEER_HANDSHAKE,
                                    target,
                                    Some(value),
                                    &req.from,
                                    inbound_tid,
                                ) {
                                    tracing::debug!(err = %e, "relay_with_tid send failed");
                                }
                                // The FE-holder used `req.relay`-style
                                // forwarding to reach us; it is not awaiting
                                // a REPLY for this REQUEST. Release the
                                // inbound req so the deferred-reply pipeline
                                // does not emit an auto-error REPLY racing
                                // against the genuine REPLY arriving via the
                                // chain's tail.
                                req.release();
                            }
                            _ => req.reply(Some(value)),
                        },
                        Ok(None) => req.reply(None),
                        Err(_) => req.error(1),
                    }
                });
            } else {
                req.reply(None);
            }
        }
        HandshakeAction::CloserNodes => {
            tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "handshake CLOSER_NODES");
            req.reply(None);
        }
        HandshakeAction::Drop => {
            tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "handshake DROP");
            drop(req);
        }
    }
}

fn handle_peer_holepunch(
    mut req: crate::rpc::UserRequest,
    dht: &DhtHandle,
    router: &Arc<Mutex<Router>>,
    server_tx: &mpsc::UnboundedSender<ServerEvent>,
) {
    let Some(value) = &req.value else {
        req.error(1);
        return;
    };

    let action = {
        let router = match router.lock() {
            Ok(r) => r,
            Err(_) => {
                req.error(1);
                return;
            }
        };
        match router.route_holepunch(req.target.as_ref(), &req.from, value) {
            Ok(a) => a,
            Err(_) => {
                req.error(1);
                return;
            }
        }
    };

    match action {
        HolepunchAction::ForwardRequest { value, to } => {
            // FE-holder relay: forward the inbound REQUEST as a new REQUEST
            // to `to`, preserving the original requester's tid end-to-end.
            // Fire-and-forget — the eventual REPLY reaches the original
            // client directly via the tid-preserved chain (server replies
            // with `dht.relay_with_tid` back to this FE-holder, which then
            // dispatches HolepunchAction::ReplyTo to send the REPLY packet
            // straight to the client at peer_address). Mirrors Node
            // `dht-rpc::Request.relay(value, to)` and item-8 for handshake.
            tracing::info!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "holepunch FORWARD_REQUEST — tid-preserved relay"
            );
            if let Err(e) = dht.relay_with_tid(
                PEER_HOLEPUNCH,
                req.target,
                Some(value),
                &to,
                req.tid,
            ) {
                tracing::debug!(err = %e, "ForwardRequest: relay_with_tid send failed");
            }
            // No reply to the inbound REQUEST — the original client awaits
            // its REPLY via the preserved-tid chain, not via this hop.
            req.release();
        }
        HolepunchAction::ReplyTo { value, to } => {
            // FE-holder finalises the relay chain: send a REPLY packet
            // (Response, not Request) with the inbound tid (= original
            // client's tid) directly to the client at `to`. The REPLY
            // matches the client's outstanding inflight entry. Mirrors
            // Node `dht-rpc::Request.reply(value, { to })` as invoked from
            // `lib/server.js _addHolepunch case FROM_SERVER`.
            tracing::info!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "holepunch REPLY_TO — finalise tid-preserved chain"
            );
            if let Err(e) = dht.send_reply_to(req.tid, req.target, &to, Some(value)) {
                tracing::debug!(err = %e, "ReplyTo: send_reply_to failed");
            }
            // The inbound REQUEST came from the server via its relay_with_tid;
            // the server is not awaiting a reply, so we do not call
            // req.reply / req.error here.
            req.release();
        }
        HolepunchAction::Relay { value, to } => {
            // Legacy variant — the router no longer emits this for holepunch.
            // Retained for forward-compatibility (non_exhaustive enum) so any
            // external producer continues to function. Treat as fire-and-forget
            // tid-preserved relay (equivalent to ForwardRequest).
            tracing::warn!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "holepunch legacy Relay action — treating as ForwardRequest"
            );
            if let Err(e) = dht.relay_with_tid(
                PEER_HOLEPUNCH,
                req.target,
                Some(value),
                &to,
                req.tid,
            ) {
                tracing::debug!(err = %e, "legacy Relay: relay_with_tid send failed");
            }
            req.release();
        }
        HolepunchAction::Reply { value, to } => {
            // Legacy variant — the router no longer emits this for holepunch.
            // Retained for forward-compatibility (non_exhaustive enum). Treat
            // as REPLY-to-arbitrary-address (equivalent to ReplyTo).
            tracing::warn!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                to = %format!("{}:{}", to.host, to.port),
                "holepunch legacy Reply action — treating as ReplyTo"
            );
            if let Err(e) = dht.send_reply_to(req.tid, req.target, &to, Some(value)) {
                tracing::debug!(err = %e, "legacy Reply: send_reply_to failed");
            }
            req.release();
        }
        HolepunchAction::HandleLocally { msg, peer_address } => {
            tracing::debug!(
                from = %format!("{}:{}", req.from.host, req.from.port),
                peer = %format!("{:?}", peer_address),
                inbound_mode = msg.mode,
                "holepunch HANDLE_LOCALLY"
            );
            let (reply_tx, reply_rx) = oneshot::channel();
            let from = req.from.clone();
            let target = req.target;
            // FROM_CLIENT direct holepunch (if ever wired) is answered via
            // the inbound request's REPLY channel (`req.reply`). FROM_RELAY /
            // FROM_SECOND_RELAY holepunches arrive as REQUESTs whose tid was
            // preserved by the FE-holder via `req.relay`-style forwarding;
            // we mirror Node `lib/server.js _addHolepunch case FROM_RELAY`
            // by calling `dht.relay_with_tid(...)` to push a tid-preserved
            // FROM_SERVER REQUEST back to the FE-holder. The FE-holder then
            // finalises the chain by emitting the REPLY directly to the
            // original client (see HolepunchAction::ReplyTo above).
            let inbound_mode = msg.mode;
            let inbound_tid = req.tid;

            let sent = server_tx
                .send(ServerEvent::PeerHolepunch {
                    msg,
                    from,
                    peer_address,
                    target,
                    reply_tx,
                })
                .is_ok();

            if sent {
                let dht = dht.clone();
                tokio::spawn(async move {
                    match reply_rx.await {
                        Ok(Some(value)) => match inbound_mode {
                            crate::hyperdht_messages::MODE_FROM_CLIENT => req.reply(Some(value)),
                            crate::hyperdht_messages::MODE_FROM_RELAY
                            | crate::hyperdht_messages::MODE_FROM_SECOND_RELAY => {
                                if let Err(e) = dht.relay_with_tid(
                                    PEER_HOLEPUNCH,
                                    target,
                                    Some(value),
                                    &req.from,
                                    inbound_tid,
                                ) {
                                    tracing::debug!(err = %e, "relay_with_tid send failed");
                                }
                                // The FE-holder used `req.relay`-style
                                // forwarding to reach us; it is not awaiting
                                // a REPLY for this REQUEST. Release the
                                // inbound req so the deferred-reply pipeline
                                // does not emit an auto-error REPLY racing
                                // against the genuine REPLY arriving via the
                                // chain's tail.
                                req.release();
                            }
                            _ => req.reply(Some(value)),
                        },
                        Ok(None) => req.reply(None),
                        Err(_) => req.error(1),
                    }
                });
            } else {
                req.reply(None);
            }
        }
        HolepunchAction::Drop => {
            tracing::debug!(from = %format!("{}:{}", req.from.host, req.from.port), "holepunch DROP");
            drop(req);
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn encode_compact_uint(v: u64) -> Vec<u8> {
    let mut state = crate::compact_encoding::State::new();
    crate::compact_encoding::preencode_uint(&mut state, v);
    state.alloc();
    crate::compact_encoding::encode_uint(&mut state, v);
    state.buffer
}

fn to_hex(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").ok();
        s
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperdht_messages::{FIREWALL_CONSISTENT, FIREWALL_RANDOM};

    #[test]
    fn hyperdht_config_defaults() {
        let cfg = HyperDhtConfig::default();
        assert_eq!(cfg.dht.port, 0);
        assert_eq!(cfg.dht.host, "0.0.0.0");
        assert_eq!(cfg.dht.concurrency, 10);
        assert!(cfg.dht.bootstrap.is_empty());
        assert_eq!(
            cfg.persistent.max_records,
            PersistentConfig::default().max_records
        );
        assert_eq!(
            cfg.persistent.max_per_key,
            PersistentConfig::default().max_per_key
        );
    }

    #[test]
    fn keypair_generate_produces_unique_keys() {
        let kp1 = KeyPair::generate();
        let kp2 = KeyPair::generate();
        assert_ne!(kp1.public_key, kp2.public_key);
    }

    #[test]
    fn keypair_from_seed_deterministic() {
        let seed = [0x42u8; 32];
        let kp1 = KeyPair::from_seed(seed);
        let kp2 = KeyPair::from_seed(seed);
        assert_eq!(kp1.public_key, kp2.public_key);
        assert_eq!(kp1.secret_key, kp2.secret_key);
    }

    #[test]
    fn keypair_public_key_matches_secret() {
        let kp = KeyPair::from_seed([0x11u8; 32]);
        assert_eq!(&kp.secret_key[32..], &kp.public_key);
    }

    #[test]
    fn keypair_sign_verify_roundtrip() {
        let kp = KeyPair::generate();
        let msg = b"test message";
        let sig = sign_detached(msg, &kp.secret_key);
        assert!(verify_detached(&sig, msg, &kp.public_key));
    }

    #[test]
    fn encode_compact_uint_round_trips() {
        use crate::compact_encoding::{decode_uint, State};
        for val in [0u64, 1, 127, 128, 255, 65535, u64::MAX / 2] {
            let bytes = encode_compact_uint(val);
            let mut s = State::from_buffer(&bytes);
            let decoded = decode_uint(&mut s).unwrap();
            assert_eq!(decoded, val, "compact uint round-trip failed for {val}");
        }
    }

    #[test]
    fn hyperdht_error_display() {
        let e = HyperDhtError::Destroyed;
        assert!(e.to_string().contains("destroyed"));
        let e2 = HyperDhtError::InvalidSignature;
        assert!(e2.to_string().contains("signature"));
    }

    #[test]
    fn keypair_debug_hides_secret() {
        let kp = KeyPair::from_seed([0x42u8; 32]);
        let dbg = format!("{kp:?}");
        assert!(dbg.contains("KeyPair"));
        assert!(!dbg.contains("secret_key"));
    }

    #[tokio::test]
    async fn spawn_and_destroy() {
        let runtime = libudx::UdxRuntime::new().expect("runtime");
        let config = HyperDhtConfig {
            dht: DhtConfig {
                bootstrap: vec![],
                port: 0,
                ..DhtConfig::default()
            },
            persistent: PersistentConfig::default(),
        };
        let (join, handle, _server_rx) = spawn(&runtime, config).await.expect("spawn");
        handle.destroy().await.expect("destroy");
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            join,
        )
        .await;
    }

    #[tokio::test]
    async fn wire_stats_starts_at_zero_and_is_addressable() {
        let runtime = libudx::UdxRuntime::new().expect("runtime");
        let config = HyperDhtConfig {
            dht: DhtConfig {
                bootstrap: vec![],
                port: 0,
                ..DhtConfig::default()
            },
            persistent: PersistentConfig::default(),
        };
        let (join, handle, _rx) = spawn(&runtime, config).await.expect("spawn");
        let (sent, received) = handle.wire_stats();
        assert_eq!(sent, 0, "no traffic yet");
        assert_eq!(received, 0);
        // Counters are shared via Arc — incrementing through `wire_counters()`
        // must be visible via `wire_stats()`.
        let counters = handle.wire_counters();
        counters
            .bytes_sent
            .fetch_add(123, std::sync::atomic::Ordering::Relaxed);
        counters
            .bytes_received
            .fetch_add(456, std::sync::atomic::Ordering::Relaxed);
        let (sent, received) = handle.wire_stats();
        assert_eq!(sent, 123);
        assert_eq!(received, 456);
        handle.destroy().await.expect("destroy");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), join).await;
    }

    #[test]
    fn next_stream_id_is_unique() {
        let a = next_stream_id();
        let b = next_stream_id();
        let c = next_stream_id();
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[tokio::test]
    async fn establish_stream_missing_udx_info() {
        let runtime = libudx::UdxRuntime::new().expect("runtime");
        let nw_result = NoiseWrapResult {
            remote_public_key: [0xAA; 32],
            tx: [1; 32],
            rx: [2; 32],
            handshake_hash: [3; 64],
            holepunch_secret: [4; 32],
            is_initiator: true,
        };
        let result = ConnectResult {
            remote_public_key: [0xAA; 32],
            server_address: Ipv4Peer { host: "127.0.0.1".into(), port: 9999 },
            client_address: Ipv4Peer { host: "127.0.0.1".into(), port: 8888 },
            is_relayed: false,
            noise: nw_result,
            local_stream_id: 1,
            remote_udx: None,
            udx_socket: None,
        };
        let err = establish_stream(&result, &runtime).await.unwrap_err();
        assert!(matches!(err, HyperDhtError::StreamEstablishment(_)));
    }

    #[tokio::test]
    async fn establish_stream_bad_address() {
        let runtime = libudx::UdxRuntime::new().expect("runtime");
        let nw_result = NoiseWrapResult {
            remote_public_key: [0xBB; 32],
            tx: [1; 32],
            rx: [2; 32],
            handshake_hash: [3; 64],
            holepunch_secret: [4; 32],
            is_initiator: true,
        };
        let result = ConnectResult {
            remote_public_key: [0xBB; 32],
            server_address: Ipv4Peer { host: "not-an-ip".into(), port: 9999 },
            client_address: Ipv4Peer { host: "127.0.0.1".into(), port: 8888 },
            is_relayed: false,
            noise: nw_result,
            local_stream_id: next_stream_id(),
            remote_udx: Some(UdxInfo { version: 1, reusable_socket: true, id: 42, seq: 0 }),
            udx_socket: None,
        };
        let err = establish_stream(&result, &runtime).await.unwrap_err();
        assert!(matches!(err, HyperDhtError::StreamEstablishment(_)));
    }

    #[tokio::test]
    async fn establish_stream_remote_id_overflow() {
        let runtime = libudx::UdxRuntime::new().expect("runtime");
        let nw_result = NoiseWrapResult {
            remote_public_key: [0xCC; 32],
            tx: [1; 32],
            rx: [2; 32],
            handshake_hash: [3; 64],
            holepunch_secret: [4; 32],
            is_initiator: true,
        };
        let result = ConnectResult {
            remote_public_key: [0xCC; 32],
            server_address: Ipv4Peer { host: "127.0.0.1".into(), port: 9999 },
            client_address: Ipv4Peer { host: "127.0.0.1".into(), port: 8888 },
            is_relayed: false,
            noise: nw_result,
            local_stream_id: next_stream_id(),
            remote_udx: Some(UdxInfo {
                version: 1,
                reusable_socket: true,
                id: u64::from(u32::MAX) + 1,
                seq: 0,
            }),
            udx_socket: None,
        };
        let err = establish_stream(&result, &runtime).await.unwrap_err();
        assert!(matches!(err, HyperDhtError::StreamEstablishment(_)));
    }

    #[test]
    fn default_bootstrap_has_three_nodes() {
        assert_eq!(DEFAULT_BOOTSTRAP.len(), 3);
        for entry in &DEFAULT_BOOTSTRAP {
            assert!(entry.contains('@'), "missing @ in {entry}");
            assert!(entry.ends_with(":49737"), "wrong port in {entry}");
        }
    }

    #[test]
    fn with_public_bootstrap_populates_nodes() {
        let cfg = HyperDhtConfig::with_public_bootstrap();
        assert_eq!(cfg.dht.bootstrap.len(), 3);
        assert_eq!(cfg.dht.bootstrap[0], DEFAULT_BOOTSTRAP[0]);
        assert_eq!(cfg.dht.bootstrap[1], DEFAULT_BOOTSTRAP[1]);
        assert_eq!(cfg.dht.bootstrap[2], DEFAULT_BOOTSTRAP[2]);
        assert_eq!(cfg.dht.port, 0);
        assert!(cfg.dht.firewalled);
    }

    #[test]
    fn direct_connect_when_not_relayed() {
        assert!(should_direct_connect(false, FIREWALL_RANDOM, true, false));
    }

    #[test]
    fn direct_connect_when_firewall_open() {
        assert!(should_direct_connect(true, FIREWALL_OPEN, true, false));
    }

    #[test]
    fn direct_connect_when_not_holepunchable() {
        assert!(should_direct_connect(true, FIREWALL_RANDOM, false, false));
    }

    #[test]
    fn direct_connect_when_same_host() {
        assert!(should_direct_connect(true, FIREWALL_RANDOM, true, true));
    }

    #[test]
    fn holepunch_when_relayed_firewalled_holepunchable_different_host() {
        assert!(!should_direct_connect(true, FIREWALL_RANDOM, true, false));
        assert!(!should_direct_connect(true, FIREWALL_CONSISTENT, true, false));
        assert!(!should_direct_connect(true, FIREWALL_UNKNOWN, true, false));
    }

    #[test]
    fn direct_connect_all_conditions_false_except_one() {
        assert!(should_direct_connect(false, FIREWALL_RANDOM, true, false));
        assert!(should_direct_connect(true, FIREWALL_OPEN, true, false));
        assert!(should_direct_connect(true, FIREWALL_RANDOM, false, false));
        assert!(should_direct_connect(true, FIREWALL_RANDOM, true, true));
    }

    // ── Scenario-matrix topology tests ───────────────────────────────────
    //
    // These map to the user-defined topology matrix:
    //   T1: both open
    //   T2: sender firewalled, receiver open
    //   T3: sender open, receiver firewalled
    //   T4: both firewalled, same network
    //   T5: both firewalled, different networks
    //   T6: one behind CGNAT
    //
    // For each topology we test the connection decision from the
    // *connector's* perspective (the one calling should_direct_connect).

    #[test]
    fn topology_both_open() {
        // T1: Neither side relayed, both FIREWALL_OPEN → direct connect
        assert!(should_direct_connect(false, FIREWALL_OPEN, true, false));
    }

    #[test]
    fn topology_sender_firewalled_receiver_open() {
        // T2: Sender is firewalled, discovered receiver via relay (relayed=true).
        // Receiver is FIREWALL_OPEN → direct connect (firewall==OPEN branch).
        assert!(should_direct_connect(true, FIREWALL_OPEN, true, false));
    }

    #[test]
    fn topology_sender_open_receiver_firewalled() {
        // T3: Sender is open. Found receiver via relay (relayed=true).
        // Receiver is firewalled (CONSISTENT), holepunchable → holepunch.
        assert!(!should_direct_connect(true, FIREWALL_CONSISTENT, true, false));
        // If receiver is NOT holepunchable → direct connect (fallback).
        assert!(should_direct_connect(true, FIREWALL_CONSISTENT, false, false));
    }

    #[test]
    fn topology_both_firewalled_same_network() {
        // T4: Both firewalled, same network (same_host=true).
        // same_host → direct connect regardless of firewall state.
        assert!(should_direct_connect(true, FIREWALL_RANDOM, true, true));
        assert!(should_direct_connect(true, FIREWALL_CONSISTENT, true, true));
    }

    #[test]
    fn topology_both_firewalled_different_networks() {
        // T5: Both firewalled, different networks.
        // When holepunchable → holepunch attempt.
        assert!(!should_direct_connect(true, FIREWALL_RANDOM, true, false));
        assert!(!should_direct_connect(true, FIREWALL_CONSISTENT, true, false));
        // When NOT holepunchable → direct connect fallback (no HP relay).
        assert!(should_direct_connect(true, FIREWALL_RANDOM, false, false));
        assert!(should_direct_connect(true, FIREWALL_CONSISTENT, false, false));
    }

    #[test]
    fn topology_one_behind_cgnat() {
        // T6: CGNAT peer is FIREWALL_RANDOM and holepunchable.
        // Non-CGNAT peer connects via relay → holepunch.
        assert!(!should_direct_connect(true, FIREWALL_RANDOM, true, false));
        // CGNAT peer with no holepunch support → direct connect fallback.
        assert!(should_direct_connect(true, FIREWALL_RANDOM, false, false));
    }

    #[tokio::test]
    async fn current_relay_addresses_starts_empty() {
        use libudx::UdxRuntime;

        let runtime = UdxRuntime::new().expect("runtime");
        let cfg = HyperDhtConfig {
            dht: crate::rpc::DhtConfig {
                bootstrap: vec![],
                ephemeral: Some(true),
                ..crate::rpc::DhtConfig::default()
            },
            ..HyperDhtConfig::default()
        };
        let (_task, handle, _rx) = spawn(&runtime, cfg).await.expect("spawn");

        assert!(
            handle.current_relay_addresses().is_empty(),
            "current_relay_addresses should be empty before any announce()"
        );

        let _ = handle.destroy().await;
    }

    #[tokio::test]
    async fn current_relay_addresses_is_shared_across_clones() {
        use libudx::UdxRuntime;

        let runtime = UdxRuntime::new().expect("runtime");
        let cfg = HyperDhtConfig {
            dht: crate::rpc::DhtConfig {
                bootstrap: vec![],
                ephemeral: Some(true),
                ..crate::rpc::DhtConfig::default()
            },
            ..HyperDhtConfig::default()
        };
        let (_task, handle, _rx) = spawn(&runtime, cfg).await.expect("spawn");

        let clone = handle.clone();

        {
            let mut guard = handle
                .current_relay_addresses
                .lock()
                .expect("lock current_relay_addresses");
            *guard = vec![Ipv4Peer {
                host: "1.2.3.4".to_string(),
                port: 49737,
            }];
        }

        let via_clone = clone.current_relay_addresses();
        assert_eq!(via_clone.len(), 1);
        assert_eq!(via_clone[0].host, "1.2.3.4");
        assert_eq!(via_clone[0].port, 49737);

        let _ = handle.destroy().await;
    }
}

#[cfg(test)]
mod passive_holepunch_helper_tests {
    use super::*;
    use crate::hyperdht_messages::{
        decode_holepunch_msg_from_bytes, FIREWALL_CONSISTENT, FIREWALL_OPEN,
    };
    use crate::messages::Ipv4Peer;
    use crate::secure_payload::SecurePayload;
    use libudx::UdxRuntime;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn passive_holepunch_helper_roundtrip() {
        let holepunch_secret: [u8; 32] = [7u8; 32];

        let incoming_hp = HolepunchPayload {
            error: 0,
            firewall: FIREWALL_CONSISTENT,
            round: 0,
            connected: false,
            punching: true,
            addresses: Some(vec![Ipv4Peer {
                host: "10.0.0.1".to_string(),
                port: 5000,
            }]),
            remote_address: None,
            token: Some([0x42u8; 32]),
            remote_token: None,
        };

        let sp_sender = SecurePayload::new(holepunch_secret);
        let incoming_payload = sp_sender.encrypt(&incoming_hp).expect("encrypt incoming");

        let pool = SocketPool::new("0.0.0.0".to_string());
        let runtime = UdxRuntime::new().expect("UdxRuntime");
        let peer_addr = Ipv4Peer {
            host: "1.2.3.4".to_string(),
            port: 9876,
        };

        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let result = build_passive_holepunch_reply(
            FIREWALL_OPEN,
            &pool,
            &runtime,
            &peer_addr,
            holepunch_secret,
            &incoming_payload,
            0,
            event_tx,
        )
        .await
        .expect("build_passive_holepunch_reply should succeed");

        assert!(result.remote_hp.punching, "remote_hp.punching should be true");
        assert_eq!(
            result.remote_hp.token,
            Some([0x42u8; 32]),
            "remote_hp.token should be present"
        );

        let decoded_msg =
            decode_holepunch_msg_from_bytes(&result.reply_bytes).expect("decode reply_bytes");

        let decoded_hp = result
            .payload
            .decrypt(&decoded_msg.payload)
            .expect("decrypt reply payload");

        assert!(!decoded_hp.connected, "decoded_hp.connected should be false");
        assert!(decoded_hp.punching, "decoded_hp.punching should be true");
        assert_eq!(
            decoded_hp.remote_token,
            Some([0x42u8; 32]),
            "remote_token should be echoed in reply"
        );

        assert!(
            result.puncher.primary_socket().is_some(),
            "puncher should have a primary socket"
        );
    }
}
