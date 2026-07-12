//! blind-relay protocol messages and the shared pairing engine —
//! wire-compatible with Node.js `blind-relay@1.4.0`.
//!
//! The blind-relay protocol uses Protomux with protocol name `"blind-relay"`.
//! A [`crate::blind_relay::BlindRelayServer`] owns the shared pairing tables
//! and limits for a relay instance, while each accepted control connection gets
//! its own [`crate::blind_relay::BlindRelaySession`]. Sessions send `pair` and
//! `unpair` messages keyed by a 32-byte token; once opposite sides of the same
//! token arrive, the server yields a [`crate::blind_relay::MatchedPairing`] to
//! the caller. The caller then creates the two raw data-plane streams and
//! bridges them blindly. This module never decrypts or inspects the relayed
//! application payloads; it only matches tokens and coordinates lifecycle.
//!
//! Peeroxide adds one hardening behavior that Node's reference `blind-relay`
//! does not have: [`crate::blind_relay::BlindRelayServer::sweep_idle_sessions`]
//! closes sessions that have seen no `pair`/`unpair` activity for
//! [`crate::blind_relay::BlindRelayServerConfig::idle_session_timeout`]. This
//! is in addition to [`crate::blind_relay::BlindRelayServer::sweep_expired_pairings`],
//! which drops unmatched pending tokens after `pairing_timeout`.
//!
//! Stream teardown follows Node's lifecycle closely once a pairing is active:
//! unpairing an active token tears its bridged data-plane streams down, and
//! closing either session does the same for every active pairing owned by that
//! session. In peeroxide this happens via
//! [`crate::blind_relay::BlindRelayServer::unpair`] and
//! [`crate::blind_relay::BlindRelayServer::release_session`], which signal the
//! transport layer to drop the bridged streams.

#![allow(dead_code)]

use crate::compact_encoding::{self as c, State};
use crate::protomux::{self, Channel, ChannelEvent, Mux};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, trace};

/// Pair message — requests relay pairing with a 32-byte token.
#[derive(Debug, Clone, PartialEq)]
pub struct PairMessage {
    /// Indicates whether this peer initiated the pair request.
    pub is_initiator: bool,
    /// Relay token used to match the pair and unpair messages.
    pub token: [u8; 32],
    /// Stream identifier assigned by the local peer.
    pub id: u64,
    /// Sequence number for the pair request.
    pub seq: u64,
}

/// Unpair message — cancels a relay pairing.
#[derive(Debug, Clone, PartialEq)]
pub struct UnpairMessage {
    /// Relay token used to cancel a pending pair request.
    pub token: [u8; 32],
}

/// Protocol name used over Protomux.
pub const PROTOCOL_NAME: &str = "blind-relay";

/// Protomux message type index for pair.
pub const MSG_TYPE_PAIR: u32 = 0;

/// Protomux message type index for unpair.
pub const MSG_TYPE_UNPAIR: u32 = 1;

/// Pre-encodes a [`PairMessage`], advancing the state cursor.
pub fn preencode_pair(state: &mut State, msg: &PairMessage) {
    state.end += 1; // bitfield(7) = 1 byte
    state.end += 32; // fixed32 token
    c::preencode_uint(state, msg.id);
    c::preencode_uint(state, msg.seq);
}

/// Encodes a [`PairMessage`] into the state buffer.
pub(crate) fn encode_pair(state: &mut State, msg: &PairMessage) {
    let flags: u8 = if msg.is_initiator { 1 } else { 0 };
    c::encode_uint8(state, flags);
    c::encode_fixed32(state, &msg.token);
    c::encode_uint(state, msg.id);
    c::encode_uint(state, msg.seq);
}

/// Decodes a [`PairMessage`] from the state buffer.
pub(crate) fn decode_pair(state: &mut State) -> c::Result<PairMessage> {
    let flags = c::decode_uint8(state)?;
    let is_initiator = flags & 1 != 0;
    let token = c::decode_fixed32(state)?;
    let id = c::decode_uint(state)?;
    let seq = c::decode_uint(state)?;
    Ok(PairMessage {
        is_initiator,
        token,
        id,
        seq,
    })
}

/// Pre-encodes a [`UnpairMessage`], advancing the state cursor.
pub fn preencode_unpair(state: &mut State, _msg: &UnpairMessage) {
    state.end += 1; // bitfield(7) = 1 byte
    state.end += 32; // fixed32 token
}

/// Encodes a [`UnpairMessage`] into the state buffer.
pub(crate) fn encode_unpair(state: &mut State, msg: &UnpairMessage) {
    c::encode_uint8(state, 0); // flags = 0
    c::encode_fixed32(state, &msg.token);
}

/// Decodes a [`UnpairMessage`] from the state buffer.
pub(crate) fn decode_unpair(state: &mut State) -> c::Result<UnpairMessage> {
    let _flags = c::decode_uint8(state)?;
    let token = c::decode_fixed32(state)?;
    Ok(UnpairMessage { token })
}

/// Encode a pair message to bytes (preencode + allocate + encode).
pub(crate) fn encode_pair_to_vec(msg: &PairMessage) -> Vec<u8> {
    let mut state = State::new();
    preencode_pair(&mut state, msg);
    state.alloc();
    encode_pair(&mut state, msg);
    state.buffer
}

/// Encode an unpair message to bytes.
pub(crate) fn encode_unpair_to_vec(msg: &UnpairMessage) -> Vec<u8> {
    let mut state = State::new();
    preencode_unpair(&mut state, msg);
    state.alloc();
    encode_unpair(&mut state, msg);
    state.buffer
}

/// Decode a pair message from bytes.
pub(crate) fn decode_pair_from_slice(data: &[u8]) -> c::Result<PairMessage> {
    let mut state = State::from_buffer(data);
    decode_pair(&mut state)
}

/// Decode an unpair message from bytes.
pub(crate) fn decode_unpair_from_slice(data: &[u8]) -> c::Result<UnpairMessage> {
    let mut state = State::from_buffer(data);
    decode_unpair(&mut state)
}

// ── Client ───────────────────────────────────────────────────────────────────

/// Errors that can occur while using the blind relay client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RelayError {
    /// A Protomux operation failed while opening, sending, or receiving.
    #[error("protomux error: {0}")]
    Protomux(#[from] protomux::ProtomuxError),

    /// Encoding or decoding of relay messages failed.
    #[error("encoding error: {0}")]
    Encoding(#[from] c::EncodingError),

    /// The channel closed before a matching pair response arrived.
    #[error("channel closed before pair response")]
    ChannelClosed,

    /// The client was destroyed before the operation could complete.
    #[error("relay client destroyed")]
    Destroyed,

    /// A pair request with this token is already in flight.
    #[error("already pairing with this token")]
    AlreadyPairing,
}

/// Response from a successful relay pairing.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PairResponse {
    /// Relay-assigned remote stream identifier.
    pub remote_id: u64,
}

/// Client-side blind-relay over an existing Protomux connection.
///
/// Wraps a Protomux channel with protocol `"blind-relay"`. Sends pair/unpair
/// messages and waits for the relay server to match the token.
pub struct BlindRelayClient {
    channel: Channel,
}

impl BlindRelayClient {
    /// Open a blind-relay channel on the given Mux.
    ///
    /// `id` should be the local public key used when connecting to the relay.
    /// Both the relay server and the connecting peer must use the same `id`
    /// (the connecting peer's public key) so that Protomux can pair the
    /// channels correctly.
    ///
    /// Sends the Open frame immediately. Call [`Self::wait_opened`] before
    /// sending pair/unpair messages.
    pub async fn open(mux: &Mux, id: Option<Vec<u8>>) -> Result<Self, RelayError> {
        let channel = mux.create_channel(PROTOCOL_NAME, id, None).await?;
        Ok(Self { channel })
    }

    /// Wait for the remote side to open the channel.
    pub async fn wait_opened(&mut self) -> Result<(), RelayError> {
        self.channel.wait_opened().await?;
        Ok(())
    }

    /// Send a pair request and wait for the relay server's response.
    ///
    /// Returns the relay-assigned `remote_id` (UDX stream ID on the relay side).
    /// Blocks until the server sends a matching pair message back.
    pub async fn pair(
        &mut self,
        is_initiator: bool,
        token: &[u8; 32],
        stream_id: u64,
    ) -> Result<PairResponse, RelayError> {
        let msg = PairMessage {
            is_initiator,
            token: *token,
            id: stream_id,
            seq: 0,
        };
        self.channel
            .send(MSG_TYPE_PAIR, &encode_pair_to_vec(&msg))?;

        debug!(
            is_initiator,
            token = %format_args!("{:02x?}", &token[..4]),
            stream_id,
            "sent pair request"
        );

        loop {
            match self.channel.recv().await {
                Some(ChannelEvent::Message { message_type, data }) => {
                    if message_type == MSG_TYPE_PAIR {
                        let response = decode_pair_from_slice(&data)?;
                        if response.token == *token && response.is_initiator == is_initiator {
                            debug!(
                                remote_id = response.id,
                                "pair response received"
                            );
                            return Ok(PairResponse {
                                remote_id: response.id,
                            });
                        }
                    }
                }
                Some(ChannelEvent::Closed { .. }) | None => {
                    return Err(RelayError::ChannelClosed);
                }
                Some(ChannelEvent::Opened { .. }) => {}
            }
        }
    }

    /// Cancel a pending pair request.
    pub fn unpair(&self, token: &[u8; 32]) -> Result<(), RelayError> {
        let msg = UnpairMessage { token: *token };
        self.channel
            .send(MSG_TYPE_UNPAIR, &encode_unpair_to_vec(&msg))?;
        Ok(())
    }

    /// Close the blind-relay channel.
    pub fn close(&mut self) {
        self.channel.close();
    }
}

// ── Server ───────────────────────────────────────────────────────────────────
//
// Protocol-only implementation of the relay-matching engine (Node's
// `blind-relay` `Server`/`BlindRelaySession`/`BlindRelayPair` from
// `index.js`). This module has no dependency on libudx or UdxRuntime: it
// tracks the shared pairing table, per-session bookkeeping, and configurable
// limits, and hands a [`MatchedPairing`] to the caller once both sides of a
// token have registered. The caller (see `peeroxide-dht::relay_service`) is
// responsible for creating the two raw UDX streams and bridging them with
// `UdxStream::relay_to` — this module never touches transport.
//
// Node has no equivalent of `max_sessions`/`max_pairings_per_session`/
// `pairing_timeout`/`idle_session_timeout` — its `blind-relay` package is
// effectively unbounded and timeout-free. Peeroxide adds these as hardening
// measures with deliberately generous defaults, so normal deployments should
// not hit them in practice; see the field docs below for the exact behavior.

/// Configuration limits for a [`BlindRelayServer`].
///
/// Node's `blind-relay`/`blind-relay-service` has none of these — see the
/// module-level notes above. Peeroxide's defaults are intentionally generous,
/// but they are still real limits/timeouts rather than a literally unbounded
/// replica of Node's behavior.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BlindRelayServerConfig {
    /// Maximum number of concurrently accepted sessions. New connections
    /// past this cap are rejected by the caller (see `relay_service`).
    pub max_sessions: usize,
    /// Maximum number of concurrent pending+active pairings a single
    /// session may hold. Guards against one peer exhausting the relay's
    /// pairing table.
    pub max_pairings_per_session: usize,
    /// How long an unmatched (pending) pairing may wait for its other side
    /// before being dropped. Node has no such timeout; a pending pairing
    /// there lives forever until `unpair`/session close.
    pub pairing_timeout: Duration,
    /// How long a session may go without any activity (pair/unpair
    /// messages) before it is closed. **No Node precedent** — confirmed
    /// against `blind-relay`'s source, a session and its streams there
    /// live until the channel actually closes or `unpair`/`destroy` is
    /// called explicitly; this is a deliberate peeroxide-only hardening
    /// addition, not a protocol requirement.
    pub idle_session_timeout: Duration,
}

impl Default for BlindRelayServerConfig {
    fn default() -> Self {
        Self {
            max_sessions: 10_000,
            max_pairings_per_session: 256,
            pairing_timeout: Duration::from_secs(300),
            idle_session_timeout: Duration::from_secs(600),
        }
    }
}

/// Relay statistics, named to mirror `blind-relay-service`'s `relay.stats`
/// Prometheus fields 1:1 for easy cross-reference against the Node
/// reference implementation.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct RelayStats {
    /// Total sessions ever accepted.
    pub sessions_accepted: AtomicU64,
    /// Currently active (open) sessions.
    pub sessions_active: AtomicI64,
    /// Total pairing requests ever received.
    pub pairings_requested: AtomicU64,
    /// Total pairings that successfully matched both sides.
    pub pairings_matched: AtomicU64,
    /// Total pairings cancelled (via `unpair`, timeout, or session close
    /// before a match).
    pub pairings_cancelled: AtomicU64,
    /// Currently pending (registered, unmatched) pairings.
    pub pairings_pending: AtomicI64,
    /// Currently active (matched, bridging) pairings.
    pub pairings_active: AtomicI64,
    /// Total data-plane streams ever opened (2 per matched pairing).
    pub streams_opened: AtomicU64,
    /// Total data-plane streams ever closed.
    pub streams_closed: AtomicU64,
    /// Total data-plane stream errors.
    pub streams_errors: AtomicU64,
}

/// Point-in-time snapshot of [`RelayStats`], for logging/inspection.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct RelayStatsSnapshot {
    /// See [`RelayStats::sessions_accepted`].
    pub sessions_accepted: u64,
    /// See [`RelayStats::sessions_active`].
    pub sessions_active: i64,
    /// See [`RelayStats::pairings_requested`].
    pub pairings_requested: u64,
    /// See [`RelayStats::pairings_matched`].
    pub pairings_matched: u64,
    /// See [`RelayStats::pairings_cancelled`].
    pub pairings_cancelled: u64,
    /// See [`RelayStats::pairings_pending`].
    pub pairings_pending: i64,
    /// See [`RelayStats::pairings_active`].
    pub pairings_active: i64,
    /// See [`RelayStats::streams_opened`].
    pub streams_opened: u64,
    /// See [`RelayStats::streams_closed`].
    pub streams_closed: u64,
    /// See [`RelayStats::streams_errors`].
    pub streams_errors: u64,
}

impl RelayStats {
    /// Take a point-in-time snapshot of all counters.
    pub fn snapshot(&self) -> RelayStatsSnapshot {
        RelayStatsSnapshot {
            sessions_accepted: self.sessions_accepted.load(Ordering::Relaxed),
            sessions_active: self.sessions_active.load(Ordering::Relaxed),
            pairings_requested: self.pairings_requested.load(Ordering::Relaxed),
            pairings_matched: self.pairings_matched.load(Ordering::Relaxed),
            pairings_cancelled: self.pairings_cancelled.load(Ordering::Relaxed),
            pairings_pending: self.pairings_pending.load(Ordering::Relaxed),
            pairings_active: self.pairings_active.load(Ordering::Relaxed),
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_closed: self.streams_closed.load(Ordering::Relaxed),
            streams_errors: self.streams_errors.load(Ordering::Relaxed),
        }
    }
}

/// One side of a matched pairing, handed to the caller so it can create the
/// data-plane raw stream for that side and reply on the wire.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MatchedSide {
    /// The session that registered this side of the pairing.
    pub session_id: u64,
    /// Whether this side identified itself as the initiator.
    pub is_initiator: bool,
    /// The `id` field from this side's `pair` message — the *client's own*
    /// local stream id, used by the caller as `remote_id` when connecting
    /// the relay's raw stream for this side (mirrors Node's
    /// `BlindRelayLink.remoteId`).
    pub client_stream_id: u64,
    /// Channel back to this side's session-driver task; send a
    /// [`SessionOutbound::PairMatched`] once the caller has created and
    /// bridged the raw streams, so the session can reply on the wire.
    pub outbound_tx: mpsc::UnboundedSender<SessionOutbound>,
}

/// Both sides of a pairing that just matched — the caller must create two
/// raw data-plane streams (one per side) and bridge them (e.g. via
/// `UdxStream::relay_to`, both directions), then notify each side via its
/// `outbound_tx`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MatchedPairing {
    /// The relay token that was matched.
    pub token: [u8; 32],
    /// The side that registered first.
    pub first: MatchedSide,
    /// The side that registered second (completed the match).
    pub second: MatchedSide,
}

/// Instructions from the server engine to a session's driver task.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SessionOutbound {
    /// Send a `pair` reply on the wire with this relay-assigned local
    /// stream id (mirrors Node's `session._pair.send({ isInitiator, token,
    /// id: stream.id, seq: 0 })`).
    PairMatched {
        /// The token that matched.
        token: [u8; 32],
        /// This side's `is_initiator` flag, echoed back.
        is_initiator: bool,
        /// The relay's own newly-created local stream id for this side.
        local_stream_id: u64,
    },
    /// Close this session (e.g. it was swept for being idle past
    /// `idle_session_timeout` — a peeroxide-only addition with no Node
    /// precedent; see [`BlindRelayServerConfig::idle_session_timeout`]).
    /// [`BlindRelaySession::run`] treats this the same as a natural
    /// remote close.
    Close,
}

/// Outcome of registering a `pair` request with [`BlindRelayServer::try_pair`].
#[derive(Debug)]
#[non_exhaustive]
pub enum PairOutcome {
    /// Registered; waiting for the other side of the token.
    Pending,
    /// This session already has a pairing registered for this token
    /// (mirrors Node's silent no-op `if (pair.links[+isInitiator]) return`).
    AlreadyPairing,
    /// Both sides are now present — caller must wire the data plane.
    Matched(MatchedPairing),
    /// `max_pairings_per_session` or `max_sessions` limits were exceeded.
    LimitExceeded,
}

/// Outcome of [`BlindRelayServer::unpair`].
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnpairOutcome {
    /// A pending (unmatched) pairing was cancelled.
    Cancelled,
    /// An already-matched (active) pairing was destroyed. Mirrors Node's
    /// `_onunpair`, which — when the token isn't found in the pending
    /// table — looks it up in `this._streams` and calls
    /// `.destroy(errors.PAIRING_CANCELLED())` on it.
    Destroyed,
    /// No pairing (pending or active) was found for this token.
    NotFound,
}

struct PendingLink {
    session_id: u64,
    is_initiator: bool,
    client_stream_id: u64,
    outbound_tx: mpsc::UnboundedSender<SessionOutbound>,
    created_at: Instant,
}

struct PendingPairing {
    links: [Option<PendingLink>; 2],
}

impl PendingPairing {
    fn empty() -> Self {
        Self { links: [None, None] }
    }

    fn slot(&self, is_initiator: bool) -> &Option<PendingLink> {
        &self.links[usize::from(is_initiator)]
    }
}

/// An already-matched pairing whose data-plane streams the caller has
/// wired up. Held so [`BlindRelayServer::unpair`] (on an active token) and
/// [`BlindRelayServer::release_session`] can signal the caller to tear
/// the streams down — mirrors Node's `session._streams` map.
struct ActivePairing {
    session_ids: [u64; 2],
    teardown_tx: mpsc::UnboundedSender<()>,
}

struct ServerState {
    config: BlindRelayServerConfig,
    pairing: Mutex<HashMap<[u8; 32], PendingPairing>>,
    active_pairings: Mutex<HashMap<[u8; 32], ActivePairing>>,
    session_count: AtomicU64,
    next_session_id: AtomicU64,
    /// Last-activity timestamp per session, for [`BlindRelayServer::sweep_idle_sessions`].
    /// Peeroxide-only — see [`BlindRelayServerConfig::idle_session_timeout`].
    session_activity: Mutex<HashMap<u64, Instant>>,
    /// Outbound channel per session, so the idle-sweep task can reach a
    /// specific session to send [`SessionOutbound::Close`].
    session_outbound: Mutex<HashMap<u64, mpsc::UnboundedSender<SessionOutbound>>>,
    stats: RelayStats,
}

/// Shared pairing/session-limit engine for a blind-relay server.
///
/// Protocol-only — see the module-level docs above. Cloning shares the same
/// underlying state (cheap `Arc` clone), so every accepted
/// [`BlindRelaySession`] holds its own clone.
#[derive(Clone)]
pub struct BlindRelayServer {
    inner: Arc<ServerState>,
}

impl BlindRelayServer {
    /// Create a new relay server with the given configuration.
    pub fn new(config: BlindRelayServerConfig) -> Self {
        Self {
            inner: Arc::new(ServerState {
                config,
                pairing: Mutex::new(HashMap::new()),
                active_pairings: Mutex::new(HashMap::new()),
                session_count: AtomicU64::new(0),
                next_session_id: AtomicU64::new(1),
                session_activity: Mutex::new(HashMap::new()),
                session_outbound: Mutex::new(HashMap::new()),
                stats: RelayStats::default(),
            }),
        }
    }

    /// Current statistics snapshot.
    pub fn stats(&self) -> RelayStatsSnapshot {
        self.inner.stats.snapshot()
    }

    /// The configured limits.
    pub fn config(&self) -> &BlindRelayServerConfig {
        &self.inner.config
    }

    /// Reserve a new session slot, returning its id, or `None` if
    /// `max_sessions` is already at capacity.
    pub fn try_accept_session(&self) -> Option<u64> {
        loop {
            let current = self.inner.session_count.load(Ordering::Acquire);
            if current as usize >= self.inner.config.max_sessions {
                return None;
            }
            if self
                .inner
                .session_count
                .compare_exchange(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let id = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
                self.inner.stats.sessions_accepted.fetch_add(1, Ordering::Relaxed);
                self.inner.stats.sessions_active.fetch_add(1, Ordering::Relaxed);
                return Some(id);
            }
        }
    }

    /// Register a newly-accepted session's outbound channel and seed its
    /// activity baseline, so a session with zero `pair`/`unpair` traffic
    /// doesn't immediately look infinitely idle to
    /// [`Self::sweep_idle_sessions`]. Call once, right after
    /// [`Self::try_accept_session`] succeeds.
    pub fn register_session(&self, session_id: u64, outbound_tx: mpsc::UnboundedSender<SessionOutbound>) {
        self.inner
            .session_outbound
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id, outbound_tx);
        self.touch_session(session_id);
    }

    /// Record activity for `session_id` (called on every `pair`/`unpair`
    /// message processed), resetting its idle clock.
    pub fn touch_session(&self, session_id: u64) {
        self.inner
            .session_activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(session_id, Instant::now());
    }

    /// Release a session slot (call when a session's connection closes).
    /// Also cancels any pending pairings still held by that session, and
    /// tears down any *active* (already-matched) pairings it was part of
    /// (mirrors Node's `_onclose`, which destroys every stream in
    /// `this._streams` when a session's channel closes).
    pub fn release_session(&self, session_id: u64) {
        self.inner.session_count.fetch_sub(1, Ordering::AcqRel);
        self.inner.stats.sessions_active.fetch_sub(1, Ordering::Relaxed);
        self.inner
            .session_activity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&session_id);
        self.inner
            .session_outbound
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&session_id);

        let mut pairing = self.inner.pairing.lock().unwrap_or_else(|e| e.into_inner());
        pairing.retain(|_token, pair| {
            let mut touched = false;
            for slot in &mut pair.links {
                if slot.as_ref().is_some_and(|l| l.session_id == session_id) {
                    *slot = None;
                    touched = true;
                }
            }
            if touched && pair.links.iter().all(Option::is_none) {
                self.inner.stats.pairings_cancelled.fetch_add(1, Ordering::Relaxed);
                self.inner.stats.pairings_pending.fetch_sub(1, Ordering::Relaxed);
                false // remove entry
            } else {
                true
            }
        });
        drop(pairing);

        // Mirrors Node's `_onclose`: destroy every active stream this
        // session was part of.
        let mut active = self.inner.active_pairings.lock().unwrap_or_else(|e| e.into_inner());
        active.retain(|_token, pair| {
            if pair.session_ids.contains(&session_id) {
                let _ = pair.teardown_tx.send(());
                self.inner.stats.pairings_active.fetch_sub(1, Ordering::Relaxed);
                false // remove entry
            } else {
                true
            }
        });
    }

    /// Register a `pair` request from a session. See [`PairOutcome`].
    #[allow(clippy::too_many_arguments)]
    pub fn try_pair(
        &self,
        session_id: u64,
        is_initiator: bool,
        token: [u8; 32],
        client_stream_id: u64,
        session_pairing_count: usize,
        outbound_tx: mpsc::UnboundedSender<SessionOutbound>,
    ) -> PairOutcome {
        self.touch_session(session_id);

        if session_pairing_count >= self.inner.config.max_pairings_per_session {
            return PairOutcome::LimitExceeded;
        }

        self.inner.stats.pairings_requested.fetch_add(1, Ordering::Relaxed);

        let mut pairing = self.inner.pairing.lock().unwrap_or_else(|e| e.into_inner());
        let pair = pairing.entry(token).or_insert_with(PendingPairing::empty);

        if pair.slot(is_initiator).is_some() {
            // Mirrors Node: a duplicate pair message for a slot already
            // occupied by this session is a silent no-op.
            return PairOutcome::AlreadyPairing;
        }

        let link = PendingLink {
            session_id,
            is_initiator,
            client_stream_id,
            outbound_tx,
            created_at: Instant::now(),
        };
        pair.links[usize::from(is_initiator)] = Some(link);

        let other = &pair.links[usize::from(!is_initiator)];
        let Some(other_link) = other else {
            self.inner.stats.pairings_pending.fetch_add(1, Ordering::Relaxed);
            return PairOutcome::Pending;
        };

        // Both sides present: matched. Take ownership of both links and
        // remove the pairing table entry.
        let first = MatchedSide {
            session_id: other_link.session_id,
            is_initiator: other_link.is_initiator,
            client_stream_id: other_link.client_stream_id,
            outbound_tx: other_link.outbound_tx.clone(),
        };
        let second = MatchedSide {
            session_id,
            is_initiator,
            client_stream_id,
            outbound_tx: pair.links[usize::from(is_initiator)]
                .as_ref()
                .expect("just inserted")
                .outbound_tx
                .clone(),
        };
        pairing.remove(&token);

        // Only the first side ever incremented `pairings_pending`.
        self.inner.stats.pairings_pending.fetch_sub(1, Ordering::Relaxed);
        self.inner.stats.pairings_matched.fetch_add(1, Ordering::Relaxed);
        self.inner.stats.pairings_active.fetch_add(1, Ordering::Relaxed);

        debug!(
            token = %format_args!("{:02x?}", &token[..4]),
            "blind-relay pairing matched"
        );

        PairOutcome::Matched(MatchedPairing { token, first, second })
    }

    /// Register an already-matched pairing's data-plane as active, so a
    /// later [`Self::unpair`] or [`Self::release_session`] can signal
    /// `teardown_tx` to tear the bridged streams down. Call once the
    /// caller (see `relay_service::bridge_one_pairing`) has created and
    /// wired the two raw streams for a [`MatchedPairing`] returned from
    /// [`Self::try_pair`].
    pub fn mark_active(
        &self,
        token: [u8; 32],
        session_ids: [u64; 2],
        teardown_tx: mpsc::UnboundedSender<()>,
    ) {
        self.inner
            .active_pairings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                token,
                ActivePairing {
                    session_ids,
                    teardown_tx,
                },
            );
    }

    /// Cancel a pairing for `session_id`/`token`. See [`UnpairOutcome`].
    ///
    /// Checks the *pending* (unmatched) table first; if not found there,
    /// checks *active* (already-matched) pairings and signals their
    /// `teardown_tx` — mirrors Node's `_onunpair`, which does the same
    /// two-step lookup and calls `.destroy(errors.PAIRING_CANCELLED())` on
    /// an active stream it finds in `this._streams`.
    pub fn unpair(&self, session_id: u64, token: [u8; 32]) -> UnpairOutcome {
        self.touch_session(session_id);

        let mut pairing = self.inner.pairing.lock().unwrap_or_else(|e| e.into_inner());
        let Some(pair) = pairing.get_mut(&token) else {
            drop(pairing);
            return self.unpair_active(token);
        };

        let mut found = false;
        for slot in &mut pair.links {
            if slot.as_ref().is_some_and(|l| l.session_id == session_id) {
                *slot = None;
                found = true;
            }
        }

        if !found {
            return UnpairOutcome::NotFound;
        }

        if pair.links.iter().all(Option::is_none) {
            pairing.remove(&token);
        }

        self.inner.stats.pairings_cancelled.fetch_add(1, Ordering::Relaxed);
        self.inner.stats.pairings_pending.fetch_sub(1, Ordering::Relaxed);

        UnpairOutcome::Cancelled
    }

    /// Look up `token` in the active-pairings table and, if found, signal
    /// its `teardown_tx` and remove the entry. Shared tail of
    /// [`Self::unpair`] for the "already matched" case.
    fn unpair_active(&self, token: [u8; 32]) -> UnpairOutcome {
        let mut active = self.inner.active_pairings.lock().unwrap_or_else(|e| e.into_inner());
        let Some(pair) = active.remove(&token) else {
            return UnpairOutcome::NotFound;
        };
        let _ = pair.teardown_tx.send(());
        self.inner.stats.pairings_active.fetch_sub(1, Ordering::Relaxed);
        UnpairOutcome::Destroyed
    }

    /// Sweep sessions idle (no `pair`/`unpair` activity) longer than
    /// `idle_session_timeout`, returning their ids so the caller can send
    /// each a [`SessionOutbound::Close`]. Peeroxide-only — see
    /// [`BlindRelayServerConfig::idle_session_timeout`]. Callers should
    /// invoke this periodically (see `relay_service`), alongside
    /// [`Self::sweep_expired_pairings`].
    pub fn sweep_idle_sessions(&self) -> Vec<u64> {
        let timeout = self.inner.config.idle_session_timeout;
        let activity = self.inner.session_activity.lock().unwrap_or_else(|e| e.into_inner());
        let idle: Vec<u64> = activity
            .iter()
            .filter(|(_, last)| last.elapsed() > timeout)
            .map(|(id, _)| *id)
            .collect();
        drop(activity);

        if idle.is_empty() {
            return idle;
        }

        let outbound = self.inner.session_outbound.lock().unwrap_or_else(|e| e.into_inner());
        let closed: Vec<u64> = idle
            .into_iter()
            .filter(|id| {
                if let Some(tx) = outbound.get(id) {
                    let _ = tx.send(SessionOutbound::Close);
                    true
                } else {
                    false
                }
            })
            .collect();
        if !closed.is_empty() {
            trace!(count = closed.len(), "swept idle blind-relay sessions");
        }
        closed
    }

    /// Sweep pending pairings older than `pairing_timeout`, cancelling them.
    /// Callers should invoke this periodically (see `relay_service`).
    /// Returns the number of pairings dropped.
    pub fn sweep_expired_pairings(&self) -> usize {
        let timeout = self.inner.config.pairing_timeout;
        let mut pairing = self.inner.pairing.lock().unwrap_or_else(|e| e.into_inner());
        let before = pairing.len();
        pairing.retain(|_token, pair| {
            let oldest = pair
                .links
                .iter()
                .filter_map(|l| l.as_ref().map(|l| l.created_at))
                .min();
            !matches!(oldest, Some(created_at) if created_at.elapsed() > timeout)
        });
        let dropped = before - pairing.len();
        if dropped > 0 {
            self.inner
                .stats
                .pairings_cancelled
                .fetch_add(dropped as u64, Ordering::Relaxed);
            self.inner
                .stats
                .pairings_pending
                .fetch_sub(dropped as i64, Ordering::Relaxed);
            trace!(dropped, "swept expired blind-relay pairings");
        }
        dropped
    }
}

/// Drives one accepted connection's `pair`/`unpair` traffic against a shared
/// [`BlindRelayServer`]. Reactive counterpart to [`BlindRelayClient`]:
/// instead of sending requests and awaiting replies, it listens for
/// inbound `pair`/`unpair` messages and registers them with the server.
///
/// The caller is responsible for opening the underlying [`Channel`] (same
/// `protocol: "blind-relay"`, `id: <connecting peer's public key>`
/// convention the client uses — Protomux pairs the two ends by matching
/// `(protocol, id)`), driving [`Self::run`] to completion, and consuming the
/// `matched_tx` channel to wire up the actual data-plane streams.
pub struct BlindRelaySession {
    session_id: u64,
    server: BlindRelayServer,
    channel: Channel,
    pairing_count: usize,
    outbound_rx: mpsc::UnboundedReceiver<SessionOutbound>,
    outbound_tx: mpsc::UnboundedSender<SessionOutbound>,
}

impl BlindRelaySession {
    /// Wrap an already-open (or opening) `"blind-relay"` channel for a
    /// newly-accepted connection. Returns `None` if the server is at
    /// `max_sessions` capacity.
    pub fn new(server: BlindRelayServer, channel: Channel) -> Option<Self> {
        let session_id = server.try_accept_session()?;
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        server.register_session(session_id, outbound_tx.clone());
        Some(Self {
            session_id,
            server,
            channel,
            pairing_count: 0,
            outbound_rx,
            outbound_tx,
        })
    }

    /// This session's id, for logging/correlation.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Wait for the remote side to open the channel.
    pub async fn wait_opened(&mut self) -> Result<(), RelayError> {
        self.channel.wait_opened().await?;
        Ok(())
    }

    /// Drive this session until the channel closes, forwarding matched
    /// pairings to `matched_tx` for the caller to wire up the data plane.
    ///
    /// Returns when the underlying channel closes (remote close, error, or
    /// server shutdown). Always releases the session slot on return.
    pub async fn run(&mut self, matched_tx: &mpsc::UnboundedSender<MatchedPairing>) {
        loop {
            tokio::select! {
                biased;

                outbound = self.outbound_rx.recv() => {
                    match outbound {
                        Some(SessionOutbound::PairMatched { token, is_initiator, local_stream_id }) => {
                            let msg = PairMessage {
                                is_initiator,
                                token,
                                id: local_stream_id,
                                seq: 0,
                            };
                            if self.channel.send(MSG_TYPE_PAIR, &encode_pair_to_vec(&msg)).is_err() {
                                break;
                            }
                        }
                        Some(SessionOutbound::Close) => break,
                        None => break,
                    }
                }

                event = self.channel.recv() => {
                    match event {
                        Some(ChannelEvent::Message { message_type, data }) => {
                            self.on_message(message_type, &data, matched_tx);
                        }
                        Some(ChannelEvent::Opened { .. }) => {}
                        Some(ChannelEvent::Closed { .. }) | None => break,
                    }
                }
            }
        }

        self.server.release_session(self.session_id);
    }

    fn on_message(
        &mut self,
        message_type: u32,
        data: &[u8],
        matched_tx: &mpsc::UnboundedSender<MatchedPairing>,
    ) {
        match message_type {
            MSG_TYPE_PAIR => {
                let Ok(msg) = decode_pair_from_slice(data) else {
                    trace!("blind-relay: dropping malformed pair message");
                    return;
                };
                let outcome = self.server.try_pair(
                    self.session_id,
                    msg.is_initiator,
                    msg.token,
                    msg.id,
                    self.pairing_count,
                    self.outbound_tx.clone(),
                );
                match outcome {
                    PairOutcome::Pending => self.pairing_count += 1,
                    PairOutcome::Matched(matched) => {
                        let _ = matched_tx.send(matched);
                    }
                    PairOutcome::AlreadyPairing | PairOutcome::LimitExceeded => {}
                }
            }
            MSG_TYPE_UNPAIR => {
                let Ok(msg) = decode_unpair_from_slice(data) else {
                    trace!("blind-relay: dropping malformed unpair message");
                    return;
                };
                if self.server.unpair(self.session_id, msg.token) == UnpairOutcome::Cancelled {
                    self.pairing_count = self.pairing_count.saturating_sub(1);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod golden_interop {
    use super::{
        decode_pair_from_slice, decode_unpair_from_slice, encode_pair_to_vec, encode_unpair_to_vec,
        PairMessage, UnpairMessage,
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct GoldenFile {
        #[allow(dead_code)]
        generated_by: String,
        #[allow(dead_code)]
        blind_relay_version: String,
        fixtures: Vec<Fixture>,
    }

    #[derive(Deserialize)]
    struct Fixture {
        label: String,
        #[serde(rename = "type")]
        typ: String,
        hex: String,
        decoded: serde_json::Value,
    }

    fn load_fixtures() -> Vec<Fixture> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/interop/blind-relay-fixtures.json"
        );
        let data = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!("Failed to read blind-relay fixtures at {path}: {e}. Run `node generate-blind-relay-golden.js` in tests/node/ first.")
        });
        let file: GoldenFile = serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("Failed to parse blind-relay fixtures: {e}"));
        file.fixtures
    }

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap_or_else(|e| panic!("Invalid hex '{s}': {e}"))
    }

    fn token_from_hex(s: &str) -> [u8; 32] {
        let v = from_hex(s);
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&v);
        arr
    }

    #[test]
    fn golden_blind_relay_decode_pair() {
        let fixtures = load_fixtures();
        let pairs: Vec<_> = fixtures.iter().filter(|f| f.typ == "pair").collect();
        assert!(!pairs.is_empty(), "no pair fixtures found");

        for fix in pairs {
            let raw = from_hex(&fix.hex);
            let decoded = decode_pair_from_slice(&raw)
                .unwrap_or_else(|e| panic!("[{}] decode failed: {e}", fix.label));

            let d = &fix.decoded;
            let expected = PairMessage {
                is_initiator: d["is_initiator"].as_bool().unwrap(),
                token: token_from_hex(d["token"].as_str().unwrap()),
                id: d["id"].as_u64().unwrap(),
                seq: d["seq"].as_u64().unwrap(),
            };

            assert_eq!(decoded, expected, "[{}] decode mismatch", fix.label);
        }
    }

    #[test]
    fn golden_blind_relay_decode_unpair() {
        let fixtures = load_fixtures();
        let unpairs: Vec<_> = fixtures.iter().filter(|f| f.typ == "unpair").collect();
        assert!(!unpairs.is_empty(), "no unpair fixtures found");

        for fix in unpairs {
            let raw = from_hex(&fix.hex);
            let decoded = decode_unpair_from_slice(&raw)
                .unwrap_or_else(|e| panic!("[{}] decode failed: {e}", fix.label));

            let expected = UnpairMessage {
                token: token_from_hex(fix.decoded["token"].as_str().unwrap()),
            };

            assert_eq!(decoded, expected, "[{}] decode mismatch", fix.label);
        }
    }

    #[test]
    fn golden_blind_relay_encode_roundtrip() {
        let fixtures = load_fixtures();

        for fix in &fixtures {
            let expected_bytes = from_hex(&fix.hex);
            let d = &fix.decoded;

            let encoded = match fix.typ.as_str() {
                "pair" => {
                    let msg = PairMessage {
                        is_initiator: d["is_initiator"].as_bool().unwrap(),
                        token: token_from_hex(d["token"].as_str().unwrap()),
                        id: d["id"].as_u64().unwrap(),
                        seq: d["seq"].as_u64().unwrap(),
                    };
                    encode_pair_to_vec(&msg)
                }
                "unpair" => {
                    let msg = UnpairMessage {
                        token: token_from_hex(d["token"].as_str().unwrap()),
                    };
                    encode_unpair_to_vec(&msg)
                }
                other => panic!("[{}] unknown fixture type: {other}", fix.label),
            };

            assert_eq!(
                encoded, expected_bytes,
                "[{}] encode roundtrip mismatch\n  encoded: {}\n  expected: {}",
                fix.label,
                hex::encode(&encoded),
                fix.hex,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protomux::{FramedStream, Mux};
    use tokio::sync::mpsc;

    struct MemStream {
        rx: mpsc::UnboundedReceiver<Vec<u8>>,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    }

    impl FramedStream for MemStream {
        async fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.rx.recv().await)
        }

        async fn write_frame(&mut self, data: &[u8]) -> std::io::Result<()> {
            self.tx
                .send(data.to_vec())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "closed"))
        }
    }

    fn mem_pair() -> (MemStream, MemStream) {
        let (tx_a, rx_b) = mpsc::unbounded_channel();
        let (tx_b, rx_a) = mpsc::unbounded_channel();
        (
            MemStream { rx: rx_a, tx: tx_a },
            MemStream { rx: rx_b, tx: tx_b },
        )
    }

    #[test]
    fn pair_roundtrip_initiator() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0xaa; 32],
            id: 42,
            seq: 7,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pair_roundtrip_responder() {
        let msg = PairMessage {
            is_initiator: false,
            token: [0xbb; 32],
            id: 0,
            seq: 0,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn unpair_roundtrip() {
        let msg = UnpairMessage {
            token: [0xcc; 32],
        };
        let encoded = encode_unpair_to_vec(&msg);
        let decoded = decode_unpair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pair_wire_format() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0x42; 32],
            id: 1,
            seq: 2,
        };
        let encoded = encode_pair_to_vec(&msg);

        assert_eq!(encoded[0], 0x01); // flags: bit0=1 (initiator)
        assert_eq!(&encoded[1..33], &[0x42; 32]); // token
        assert_eq!(encoded[33], 0x01); // id=1 (varint)
        assert_eq!(encoded[34], 0x02); // seq=2 (varint)
        assert_eq!(encoded.len(), 35);
    }

    #[test]
    fn pair_wire_format_responder() {
        let msg = PairMessage {
            is_initiator: false,
            token: [0x00; 32],
            id: 0,
            seq: 0,
        };
        let encoded = encode_pair_to_vec(&msg);

        assert_eq!(encoded[0], 0x00); // flags: bit0=0 (responder)
        assert_eq!(&encoded[1..33], &[0x00; 32]); // token
        assert_eq!(encoded[33], 0x00); // id=0
        assert_eq!(encoded[34], 0x00); // seq=0
    }

    #[test]
    fn unpair_wire_format() {
        let msg = UnpairMessage {
            token: [0xff; 32],
        };
        let encoded = encode_unpair_to_vec(&msg);

        assert_eq!(encoded[0], 0x00); // flags: all zero
        assert_eq!(&encoded[1..33], &[0xff; 32]); // token
        assert_eq!(encoded.len(), 33);
    }

    #[test]
    fn pair_large_ids() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0xde; 32],
            id: 100_000,
            seq: 65_536,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn protocol_name_constant() {
        assert_eq!(PROTOCOL_NAME, "blind-relay");
    }

    #[tokio::test]
    async fn client_pair_with_fake_relay() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let token = [0xaa; 32];

        let client_task = tokio::spawn(async move {
            let mut client = BlindRelayClient::open(&mux_a, None).await.unwrap();
            client.wait_opened().await.unwrap();
            let resp = client.pair(true, &token, 42).await.unwrap();
            client.close();
            resp
        });

        // Fake relay server: open matching channel, wait for pair, send response
        let mut server_ch = mux_b
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();
        server_ch.wait_opened().await.unwrap();

        let event = server_ch.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, MSG_TYPE_PAIR);
                let pair_msg = decode_pair_from_slice(&data).unwrap();
                assert!(pair_msg.is_initiator);
                assert_eq!(pair_msg.token, token);
                assert_eq!(pair_msg.id, 42);

                let reply = PairMessage {
                    is_initiator: true,
                    token,
                    id: 99,
                    seq: 0,
                };
                server_ch
                    .send(MSG_TYPE_PAIR, &encode_pair_to_vec(&reply))
                    .unwrap();
            }
            other => panic!("expected pair Message, got {other:?}"),
        }

        let resp = client_task.await.unwrap();
        assert_eq!(resp.remote_id, 99);
    }

    #[tokio::test]
    async fn client_unpair() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let token = [0xbb; 32];

        let mut client = BlindRelayClient::open(&mux_a, None).await.unwrap();
        let mut server_ch = mux_b
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();

        client.wait_opened().await.unwrap();
        server_ch.wait_opened().await.unwrap();

        client.unpair(&token).unwrap();

        let event = server_ch.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, MSG_TYPE_UNPAIR);
                let unpair_msg = decode_unpair_from_slice(&data).unwrap();
                assert_eq!(unpair_msg.token, token);
            }
            other => panic!("expected unpair Message, got {other:?}"),
        }

        client.close();
    }

    // ── BlindRelayServer engine (protocol-only) ──────────────────────────

    fn noop_outbound() -> mpsc::UnboundedSender<SessionOutbound> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    #[test]
    fn try_pair_matches_two_sides() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x11; 32];

        let first = server.try_pair(1, true, token, 100, 0, noop_outbound());
        assert!(matches!(first, PairOutcome::Pending));
        assert_eq!(server.stats().pairings_pending, 1);

        let second = server.try_pair(2, false, token, 200, 0, noop_outbound());
        match second {
            PairOutcome::Matched(m) => {
                assert_eq!(m.token, token);
                assert_eq!(m.first.session_id, 1);
                assert!(m.first.is_initiator);
                assert_eq!(m.first.client_stream_id, 100);
                assert_eq!(m.second.session_id, 2);
                assert!(!m.second.is_initiator);
                assert_eq!(m.second.client_stream_id, 200);
            }
            other => panic!("expected Matched, got {other:?}"),
        }

        let stats = server.stats();
        assert_eq!(stats.pairings_pending, 0);
        assert_eq!(stats.pairings_matched, 1);
        assert_eq!(stats.pairings_active, 1);
        assert_eq!(stats.pairings_requested, 2);
    }

    #[test]
    fn try_pair_duplicate_same_slot_is_noop() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x22; 32];

        assert!(matches!(
            server.try_pair(1, true, token, 1, 0, noop_outbound()),
            PairOutcome::Pending
        ));
        // Same session, same slot (initiator), sent again — Node treats
        // this as a silent no-op.
        assert!(matches!(
            server.try_pair(1, true, token, 1, 1, noop_outbound()),
            PairOutcome::AlreadyPairing
        ));
    }

    #[test]
    fn try_pair_respects_max_pairings_per_session() {
        let config = BlindRelayServerConfig {
            max_pairings_per_session: 2,
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);

        assert!(matches!(
            server.try_pair(1, true, [0x01; 32], 1, 0, noop_outbound()),
            PairOutcome::Pending
        ));
        assert!(matches!(
            server.try_pair(1, true, [0x02; 32], 2, 1, noop_outbound()),
            PairOutcome::Pending
        ));
        // Session's 3rd concurrent pairing attempt exceeds the per-session cap.
        assert!(matches!(
            server.try_pair(1, true, [0x03; 32], 3, 2, noop_outbound()),
            PairOutcome::LimitExceeded
        ));
    }

    #[test]
    fn try_accept_session_respects_max_sessions() {
        let config = BlindRelayServerConfig {
            max_sessions: 1,
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);

        let first = server.try_accept_session();
        assert!(first.is_some());
        assert!(server.try_accept_session().is_none());

        server.release_session(first.unwrap());
        assert!(server.try_accept_session().is_some());
    }

    #[test]
    fn unpair_cancels_pending_registration() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x33; 32];

        server.try_pair(1, true, token, 1, 0, noop_outbound());
        assert_eq!(server.stats().pairings_pending, 1);

        assert_eq!(server.unpair(1, token), UnpairOutcome::Cancelled);
        assert_eq!(server.stats().pairings_pending, 0);
        assert_eq!(server.stats().pairings_cancelled, 1);

        // Second unpair for the same (now-gone) token finds nothing.
        assert_eq!(server.unpair(1, token), UnpairOutcome::NotFound);
    }

    #[test]
    fn unpair_unknown_token_not_found() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        assert_eq!(server.unpair(1, [0x99; 32]), UnpairOutcome::NotFound);
    }

    #[test]
    fn release_session_cancels_its_pending_pairings() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x44; 32];

        let session_id = server.try_accept_session().unwrap();
        server.try_pair(session_id, true, token, 1, 0, noop_outbound());
        assert_eq!(server.stats().pairings_pending, 1);

        server.release_session(session_id);

        assert_eq!(server.stats().pairings_pending, 0);
        assert_eq!(server.stats().pairings_cancelled, 1);
        assert_eq!(server.stats().sessions_active, 0);
        // The token is fully free again for a fresh pairing attempt.
        assert!(matches!(
            server.try_pair(2, true, token, 1, 0, noop_outbound()),
            PairOutcome::Pending
        ));
    }

    #[test]
    fn sweep_expired_pairings_drops_stale_entries() {
        let config = BlindRelayServerConfig {
            pairing_timeout: Duration::from_millis(1),
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);
        let token = [0x55; 32];

        server.try_pair(1, true, token, 1, 0, noop_outbound());
        std::thread::sleep(Duration::from_millis(20));

        let dropped = server.sweep_expired_pairings();
        assert_eq!(dropped, 1);
        assert_eq!(server.stats().pairings_pending, 0);
        assert_eq!(server.stats().pairings_cancelled, 1);

        // Free to re-register after the sweep.
        assert!(matches!(
            server.try_pair(2, true, token, 1, 0, noop_outbound()),
            PairOutcome::Pending
        ));
    }

    #[test]
    fn sweep_expired_pairings_keeps_fresh_entries() {
        let config = BlindRelayServerConfig {
            pairing_timeout: Duration::from_secs(300),
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);
        server.try_pair(1, true, [0x66; 32], 1, 0, noop_outbound());

        assert_eq!(server.sweep_expired_pairings(), 0);
        assert_eq!(server.stats().pairings_pending, 1);
    }

    // ── BlindRelaySession end-to-end (two real BlindRelayClients vs two
    //    BlindRelaySessions sharing one BlindRelayServer) ─────────────────

    #[tokio::test]
    async fn session_end_to_end_pair_match() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let (matched_tx, mut matched_rx) = mpsc::unbounded_channel::<MatchedPairing>();

        // Client A <-> relay-side session A, over one in-memory pipe.
        let (client_a_stream, session_a_stream) = mem_pair();
        let (mux_client_a, run_client_a) = Mux::new(client_a_stream);
        let (mux_session_a, run_session_a) = Mux::new(session_a_stream);
        tokio::spawn(run_client_a);
        tokio::spawn(run_session_a);

        // Client B <-> relay-side session B, over a second in-memory pipe.
        let (client_b_stream, session_b_stream) = mem_pair();
        let (mux_client_b, run_client_b) = Mux::new(client_b_stream);
        let (mux_session_b, run_session_b) = Mux::new(session_b_stream);
        tokio::spawn(run_client_b);
        tokio::spawn(run_session_b);

        let channel_a = mux_session_a
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();
        let channel_b = mux_session_b
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();

        let mut session_a = BlindRelaySession::new(server.clone(), channel_a).unwrap();
        let mut session_b = BlindRelaySession::new(server.clone(), channel_b).unwrap();

        let matched_tx_a = matched_tx.clone();
        let session_a_task = tokio::spawn(async move {
            session_a.run(&matched_tx_a).await;
        });
        let session_b_task = tokio::spawn(async move {
            session_b.run(&matched_tx).await;
        });

        let token = [0x77; 32];

        let mut client_a = BlindRelayClient::open(&mux_client_a, None).await.unwrap();
        let mut client_b = BlindRelayClient::open(&mux_client_b, None).await.unwrap();
        client_a.wait_opened().await.unwrap();
        client_b.wait_opened().await.unwrap();

        // Stand in for `relay_service`: once the match arrives, create the
        // (fake, in this test) data-plane streams and reply to both sides
        // with the newly-assigned local stream ids.
        let wiring_task = tokio::spawn(async move {
            let matched = matched_rx.recv().await.unwrap();
            assert_eq!(matched.token, token);

            matched
                .first
                .outbound_tx
                .send(SessionOutbound::PairMatched {
                    token,
                    is_initiator: matched.first.is_initiator,
                    local_stream_id: 9001,
                })
                .unwrap();
            matched
                .second
                .outbound_tx
                .send(SessionOutbound::PairMatched {
                    token,
                    is_initiator: matched.second.is_initiator,
                    local_stream_id: 9002,
                })
                .unwrap();

            matched
        });

        let (resp_a, resp_b) = tokio::join!(
            client_a.pair(true, &token, 111),
            client_b.pair(false, &token, 222),
        );
        let resp_a = resp_a.unwrap();
        let resp_b = resp_b.unwrap();
        let matched = wiring_task.await.unwrap();

        // Each client's reported remote_id is the relay-assigned local
        // stream id sent for its own session (9001 for whichever session
        // was A, 9002 for whichever was B) — assert both distinct ids were
        // actually delivered to the two clients (order depends on which
        // pair message the relay processed first).
        let ids = [resp_a.remote_id, resp_b.remote_id];
        assert!(ids.contains(&9001) && ids.contains(&9002));
        let _ = matched;

        client_a.close();
        client_b.close();
        session_a_task.abort();
        session_b_task.abort();
    }

    #[test]
    fn session_new_respects_max_sessions() {
        let config = BlindRelayServerConfig {
            max_sessions: 0,
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);

        // We don't need a real channel to exercise the capacity check —
        // try_accept_session is what BlindRelaySession::new consults first.
        assert!(server.try_accept_session().is_none());
    }

    // ── Stream teardown on unpair-of-active / session-close ───────────────
    // (Node precedent: blind-relay's `_onunpair` destroys an already-
    // matched stream found in `this._streams`; `_onclose` destroys every
    // stream in that map when the session's channel closes.)

    #[test]
    fn unpair_on_active_pairing_signals_teardown() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x88; 32];
        let (teardown_tx, mut teardown_rx) = mpsc::unbounded_channel::<()>();

        // Simulate a matched pairing whose data-plane the caller has
        // already wired up (mirrors relay_service::bridge_one_pairing
        // calling mark_active after try_pair returned Matched).
        server.try_pair(1, true, token, 1, 0, noop_outbound());
        let PairOutcome::Matched(_) = server.try_pair(2, false, token, 2, 0, noop_outbound())
        else {
            panic!("expected Matched");
        };
        server.mark_active(token, [1, 2], teardown_tx);
        assert_eq!(server.stats().pairings_active, 1);

        // Either session can unpair an active pairing (mirrors Node: the
        // lookup is by token in the server-wide `_streams`-equivalent
        // table, not scoped to "the session that registered this slot").
        assert_eq!(server.unpair(1, token), UnpairOutcome::Destroyed);
        assert!(teardown_rx.try_recv().is_ok(), "teardown signal not sent");
        assert_eq!(server.stats().pairings_active, 0);

        // A second unpair against the same (now-removed) token finds nothing.
        assert_eq!(server.unpair(1, token), UnpairOutcome::NotFound);
    }

    #[test]
    fn release_session_tears_down_its_active_pairings() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token = [0x99; 32];
        let (teardown_tx, mut teardown_rx) = mpsc::unbounded_channel::<()>();

        let session_a = server.try_accept_session().unwrap();
        let session_b = server.try_accept_session().unwrap();
        server.try_pair(session_a, true, token, 1, 0, noop_outbound());
        server.try_pair(session_b, false, token, 2, 0, noop_outbound());
        server.mark_active(token, [session_a, session_b], teardown_tx);
        assert_eq!(server.stats().pairings_active, 1);

        // Releasing *either* session (not just the one that happened to
        // register second) tears down the shared pairing.
        server.release_session(session_a);

        assert!(teardown_rx.try_recv().is_ok(), "teardown signal not sent");
        assert_eq!(server.stats().pairings_active, 0);
    }

    #[test]
    fn release_session_only_tears_down_its_own_active_pairings() {
        let server = BlindRelayServer::new(BlindRelayServerConfig::default());
        let token_a = [0xaa; 32];
        let token_b = [0xbb; 32];
        let (teardown_a_tx, mut teardown_a_rx) = mpsc::unbounded_channel::<()>();
        let (teardown_b_tx, mut teardown_b_rx) = mpsc::unbounded_channel::<()>();

        let session_1 = server.try_accept_session().unwrap();
        let session_2 = server.try_accept_session().unwrap();
        let session_3 = server.try_accept_session().unwrap();

        server.try_pair(session_1, true, token_a, 1, 0, noop_outbound());
        server.try_pair(session_2, false, token_a, 2, 0, noop_outbound());
        server.mark_active(token_a, [session_1, session_2], teardown_a_tx);

        server.try_pair(session_2, true, token_b, 3, 1, noop_outbound());
        server.try_pair(session_3, false, token_b, 4, 0, noop_outbound());
        server.mark_active(token_b, [session_2, session_3], teardown_b_tx);

        assert_eq!(server.stats().pairings_active, 2);

        // Releasing session_1 only affects token_a's pairing (session_1
        // wasn't part of token_b's pairing).
        server.release_session(session_1);

        assert!(teardown_a_rx.try_recv().is_ok());
        assert!(teardown_b_rx.try_recv().is_err());
        assert_eq!(server.stats().pairings_active, 1);
    }

    // ── Idle-session-timeout (peeroxide-only, no Node precedent) ──────────

    #[test]
    fn sweep_idle_sessions_closes_sessions_past_timeout() {
        let config = BlindRelayServerConfig {
            idle_session_timeout: Duration::from_millis(1),
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);
        let session_id = server.try_accept_session().unwrap();
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        server.register_session(session_id, outbound_tx);

        std::thread::sleep(Duration::from_millis(20));

        let closed = server.sweep_idle_sessions();
        assert_eq!(closed, vec![session_id]);
        assert!(matches!(
            outbound_rx.try_recv(),
            Ok(SessionOutbound::Close)
        ));
    }

    #[test]
    fn sweep_idle_sessions_keeps_recently_active_sessions() {
        let config = BlindRelayServerConfig {
            idle_session_timeout: Duration::from_secs(300),
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);
        let session_id = server.try_accept_session().unwrap();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        server.register_session(session_id, outbound_tx);

        assert_eq!(server.sweep_idle_sessions(), Vec::<u64>::new());
    }

    #[test]
    fn pair_and_unpair_reset_idle_clock() {
        let config = BlindRelayServerConfig {
            idle_session_timeout: Duration::from_millis(50),
            ..BlindRelayServerConfig::default()
        };
        let server = BlindRelayServer::new(config);
        let session_id = server.try_accept_session().unwrap();
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        server.register_session(session_id, outbound_tx);

        std::thread::sleep(Duration::from_millis(30));
        // Activity within the timeout window resets the clock.
        server.try_pair(session_id, true, [0x77; 32], 1, 0, noop_outbound());
        std::thread::sleep(Duration::from_millis(30));

        assert_eq!(
            server.sweep_idle_sessions(),
            Vec::<u64>::new(),
            "recent pair() activity should have reset the idle clock"
        );
    }
}
