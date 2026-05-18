//! IO layer for the DHT-RPC protocol.
//!
//! Faithful Rust port of the Node.js dht-rpc IO layer.
//! The [`Io`] struct is driven by the caller from a `tokio::select!` loop.

#![allow(missing_docs)]
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blake2::digest::consts::U32;
use blake2::digest::Mac;
use blake2::Blake2bMac;
use tokio::time::Instant;

use libudx::{Datagram, UdxRuntime, UdxSocket};

use crate::messages::{self, Ipv4Peer, Response};
use crate::peer::{self, NodeId};
use crate::routing_table::{RoutingTable, K};

type Blake2bMac256 = Blake2bMac<U32>;

const ERROR_INVALID_TOKEN: u64 = 2;
const DEFAULT_TIMEOUT_MS: u64 = 1000;
const DEFAULT_RETRIES: u32 = 3;

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IoError {
    #[error("UDP error: {0}")]
    Udx(#[from] libudx::UdxError),
    #[error("address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("encoding error: {0}")]
    Encoding(#[from] crate::compact_encoding::EncodingError),
    #[error("routing table lock poisoned")]
    LockPoisoned,
}

pub type IoResult<T> = Result<T, IoError>;

// ── Public config / stats / types ─────────────────────────────────────────────

/// IO layer configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IoConfig {
    pub max_window: usize,
    pub port: u16,
    pub host: String,
    pub firewalled: bool,
    pub ephemeral: bool,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            max_window: 80,
            port: 0,
            host: "0.0.0.0".to_string(),
            firewalled: true,
            ephemeral: true,
        }
    }
}

/// IO layer statistics.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct IoStats {
    pub active: u64,
    pub total: u64,
    pub responses: u64,
    pub timeouts: u64,
    pub retries: u64,
}

/// Wire-byte counters shared between the IO layer and consumers (e.g. progress
/// reporters in `peeroxide-cli`). Increments are `Relaxed` — these are
/// observability metrics, not synchronization primitives.
///
/// The counters track every UDP datagram the IO layer hands to or receives
/// from the OS sockets, regardless of which protocol layer originated it
/// (queries, requests, replies, relays, retries — all counted).
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct WireCounters {
    pub bytes_sent: Arc<AtomicU64>,
    pub bytes_received: Arc<AtomicU64>,
}

impl WireCounters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a `WireCounters` snapshot from externally-shared atomic counters.
    ///
    /// Use when a downstream consumer (e.g. a UI progress reporter) already
    /// owns `Arc<AtomicU64>` byte counters and wants a `WireCounters` view
    /// over them. The returned value shares the atomics with the caller.
    pub fn from_counters(bytes_sent: Arc<AtomicU64>, bytes_received: Arc<AtomicU64>) -> Self {
        Self {
            bytes_sent,
            bytes_received,
        }
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.bytes_sent.load(Ordering::Relaxed),
            self.bytes_received.load(Ordering::Relaxed),
        )
    }
}

/// Which socket was used for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketKind {
    Client,
    Server,
}

/// Info about an inflight request that was resolved by a response.
#[derive(Debug, Clone)]
pub struct ResolvedRequest {
    pub tid: u16,
    pub to: Ipv4Peer,
    pub command: u64,
    pub internal: bool,
    pub target: Option<NodeId>,
}

/// Events emitted by the IO layer.
#[non_exhaustive]
pub enum IoEvent {
    IncomingRequest(IncomingRequest),
    Response {
        tid: u16,
        from: Ipv4Peer,
        to: Ipv4Peer,
        id: Option<NodeId>,
        token: Option<[u8; 32]>,
        closer_nodes: Vec<Ipv4Peer>,
        error: u64,
        value: Option<Vec<u8>>,
        rtt: Duration,
        request: ResolvedRequest,
    },
}

/// An incoming request (server-side).
pub struct IncomingRequest {
    pub tid: u16,
    pub from: Ipv4Peer,
    pub to: Ipv4Peer,
    pub id: Option<NodeId>,
    pub token: Option<[u8; 32]>,
    pub internal: bool,
    pub command: u64,
    pub target: Option<NodeId>,
    pub value: Option<Vec<u8>>,
    pub(crate) reply_ctx: ReplyContext,
}

/// Parameters for creating an outgoing request.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RequestParams {
    pub to: Ipv4Peer,
    pub token: Option<[u8; 32]>,
    pub internal: bool,
    pub command: u64,
    pub target: Option<NodeId>,
    pub value: Option<Vec<u8>>,
    pub timeout_ms: Option<u64>,
    pub retries: Option<u32>,
}

/// A timeout event — emitted when a request exceeds all retries.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TimeoutEvent {
    pub tid: u16,
    pub to: Ipv4Peer,
    pub command: u64,
    pub internal: bool,
    pub target: Option<NodeId>,
}

// ── Private types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplyContext {
    pub(crate) socket_kind: SocketKind,
}

/// Bundled parameters for `send_reply_internal` to stay under clippy's argument limit.
struct ReplyInternalParams {
    socket_kind: SocketKind,
    tid: u16,
    target: Option<NodeId>,
    error: u64,
    include_token: bool,
    value: Option<Vec<u8>>,
}

struct InflightEntry {
    tid: u16,
    to: Ipv4Peer,
    addr: SocketAddr,
    internal: bool,
    command: u64,
    target: Option<NodeId>,
    buffer: Vec<u8>,
    socket_kind: SocketKind,
    sent: u32,
    retries: u32,
    deadline: Instant,
    timestamp: Instant,
    timeout: Duration,
}

struct PendingSend {
    tid: u16,
}

// ── CongestionWindow ──────────────────────────────────────────────────────────

/// Congestion window — direct port of JS CongestionWindow class.
pub struct CongestionWindow {
    i: usize,
    total: i32,
    window: [i32; 4],
    max_window: i32,
}

impl CongestionWindow {
    pub fn new(max_window: usize) -> Self {
        Self {
            i: 0,
            total: 0,
            window: [0; 4],
            max_window: max_window as i32,
        }
    }

    /// Returns true if the window is full and no more sends should be attempted.
    pub fn is_full(&self) -> bool {
        self.total >= 2 * self.max_window || self.window[self.i] >= self.max_window
    }

    /// Decrement the current quarter (called on response received).
    pub fn recv(&mut self) {
        if self.window[self.i] > 0 {
            self.window[self.i] -= 1;
            self.total -= 1;
        }
    }

    /// Increment the current quarter (called on send).
    pub fn send(&mut self) {
        self.total += 1;
        self.window[self.i] += 1;
    }

    /// Advance to the next quarter, clearing the oldest.
    pub fn drain(&mut self) {
        self.i = (self.i + 1) & 3;
        self.total -= self.window[self.i];
        self.window[self.i] = 0;
    }

    /// Reset all counters.
    pub fn clear(&mut self) {
        self.i = 0;
        self.total = 0;
        self.window = [0; 4];
    }
}

// ── Io ────────────────────────────────────────────────────────────────────────

pub struct Io {
    client_socket: UdxSocket,
    server_socket: UdxSocket,
    client_rx: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
    server_rx: tokio::sync::mpsc::UnboundedReceiver<Datagram>,
    inflight: Vec<InflightEntry>,
    congestion: CongestionWindow,
    pending: VecDeque<PendingSend>,
    tid: u16,
    secrets: Option<[[u8; 32]; 2]>,
    rotate_countdown: u32,
    firewalled: bool,
    pub ephemeral: bool,
    pub stats: IoStats,
    pub wire: WireCounters,
    table: Arc<Mutex<RoutingTable>>,
    destroying: bool,
}

impl Io {
    /// Create and bind the IO layer (two sockets).
    pub async fn bind(
        runtime: &UdxRuntime,
        table: Arc<Mutex<RoutingTable>>,
        config: IoConfig,
    ) -> IoResult<Self> {
        let server_addr: SocketAddr = format!("{}:{}", config.host, config.port)
            .parse()
            .map_err(IoError::AddrParse)?;
        let client_addr: SocketAddr = format!("{}:0", config.host)
            .parse()
            .map_err(IoError::AddrParse)?;

        let server_socket = runtime.create_socket().await?;
        server_socket.bind(server_addr).await?;
        let server_rx = server_socket.recv_start()?;

        let client_socket = runtime.create_socket().await?;
        client_socket.bind(client_addr).await?;
        let client_rx = client_socket.recv_start()?;

        let tid: u16 = rand::random();

        Ok(Io {
            client_socket,
            server_socket,
            client_rx,
            server_rx,
            inflight: Vec::new(),
            congestion: CongestionWindow::new(config.max_window),
            pending: VecDeque::new(),
            tid,
            secrets: None,
            rotate_countdown: 10,
            firewalled: config.firewalled,
            ephemeral: config.ephemeral,
            stats: IoStats::default(),
            wire: WireCounters::default(),
            table,
            destroying: false,
        })
    }

    /// Get a clone of the wire-byte counters. Cheap (Arc clone).
    pub fn wire_counters(&self) -> WireCounters {
        self.wire.clone()
    }

    pub async fn server_local_addr(&self) -> IoResult<std::net::SocketAddr> {
        self.server_socket.local_addr().await.map_err(IoError::from)
    }

    pub fn server_socket(&self) -> UdxSocket {
        self.server_socket.clone()
    }

    pub fn primary_socket(&self) -> UdxSocket {
        if self.firewalled {
            self.client_socket.clone()
        } else {
            self.server_socket.clone()
        }
    }

    /// Receive and decode the next message from either socket.
    /// Returns None only if both channels are closed.
    pub async fn recv(&mut self) -> Option<IoEvent> {
        loop {
            let (datagram, socket_kind) = tokio::select! {
                biased;
                msg = self.client_rx.recv() => (msg?, SocketKind::Client),
                msg = self.server_rx.recv() => (msg?, SocketKind::Server),
            };
            self.wire
                .bytes_received
                .fetch_add(datagram.data.len() as u64, Ordering::Relaxed);
            tracing::debug!(
                from = %datagram.addr,
                len = datagram.data.len(),
                first_byte = datagram.data.first().copied().unwrap_or(0),
                ?socket_kind,
                "IO::recv raw datagram"
            );
            if let Some(event) = self.process_datagram(datagram, socket_kind) {
                return Some(event);
            }
        }
    }

    /// Drain congestion window and rotate secrets. Call every ~750 ms.
    pub fn drain(&mut self) {
        if let Some(secrets) = &mut self.secrets {
            self.rotate_countdown -= 1;
            if self.rotate_countdown == 0 {
                self.rotate_countdown = 10;
                // Rotate: swap[0] and [1], then re-hash old [0] (now at [1]).
                secrets.swap(0, 1);
                // Hash secrets[1] (the old secrets[0]) with itself.
                if let Ok(mut mac) = Blake2bMac256::new_from_slice(&secrets[1]) {
                    mac.update(&secrets[1]);
                    let hash = mac.finalize().into_bytes();
                    secrets[1].copy_from_slice(&hash);
                }
            }
        }

        self.congestion.drain();

        while !self.congestion.is_full() {
            match self.pending.pop_front() {
                Some(p) => {
                    if let Some(idx) = self.inflight.iter().position(|e| e.tid == p.tid) {
                        self.send_inflight_at(idx);
                    }
                }
                None => break,
            }
        }
    }

    /// Return the earliest deadline across all inflight requests.
    /// Returns a far-future instant if there are no inflight requests.
    pub fn next_timeout_deadline(&self) -> Instant {
        self.inflight
            .iter()
            .map(|e| e.deadline)
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600))
    }

    /// Check for expired timeouts. Returns events for timed-out requests.
    /// Retries are handled internally.
    pub fn check_timeouts(&mut self) -> Vec<TimeoutEvent> {
        let now = Instant::now();
        let mut timeout_indices: Vec<usize> = Vec::new();
        let mut retry_indices: Vec<usize> = Vec::new();

        for (i, entry) in self.inflight.iter().enumerate() {
            if entry.deadline <= now {
                if entry.sent > entry.retries {
                    timeout_indices.push(i);
                } else {
                    retry_indices.push(i);
                }
            }
        }

        // Process retries first (in-place, no index shifts).
        for &i in &retry_indices {
            self.stats.retries += 1;
            self.send_inflight_at(i);
        }

        // Remove timed-out entries from highest to lowest index.
        timeout_indices.sort_unstable_by(|a, b| b.cmp(a));
        let mut events = Vec::with_capacity(timeout_indices.len());
        for i in timeout_indices {
            let entry = self.inflight.swap_remove(i);
            self.congestion.recv();
            self.stats.active = self.stats.active.saturating_sub(1);
            self.stats.timeouts += 1;
            events.push(TimeoutEvent {
                tid: entry.tid,
                to: entry.to,
                command: entry.command,
                internal: entry.internal,
                target: entry.target,
            });
        }
        events
    }

    /// Create an outgoing request.
    /// Returns the assigned TID, or `None` if the IO is destroying or the
    /// destination address is invalid.
    pub fn create_request(&mut self, params: RequestParams) -> Option<u16> {
        if self.destroying {
            return None;
        }

        let addr_str = format!("{}:{}", params.to.host, params.to.port);
        let addr: SocketAddr = addr_str.parse().ok()?;

        let tid = self.tid;
        self.tid = self.tid.wrapping_add(1);

        let socket_kind = if self.firewalled {
            SocketKind::Client
        } else {
            SocketKind::Server
        };

        let include_id = !self.ephemeral && socket_kind == SocketKind::Server;
        let id = if include_id {
            self.table.lock().ok().map(|t| *t.id())
        } else {
            None
        };

        let request = messages::Request {
            tid,
            to: params.to.clone(),
            id,
            token: params.token,
            internal: params.internal,
            command: params.command,
            target: params.target,
            value: params.value,
        };

        let buffer = match messages::encode_request_to_bytes(&request) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(err = %e, "create_request: encode failed");
                return None;
            }
        };

        self.stats.active += 1;
        self.stats.total += 1;

        let now = Instant::now();
        let to_str = format!("{}:{}", params.to.host, params.to.port);

        let timeout = Duration::from_millis(params.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
        let entry = InflightEntry {
            tid,
            to: params.to,
            addr,
            internal: params.internal,
            command: params.command,
            target: params.target,
            buffer,
            socket_kind,
            sent: 0,
            retries: params.retries.unwrap_or(DEFAULT_RETRIES),
            deadline: now + timeout,
            timestamp: now,
            timeout,
        };

        self.inflight.push(entry);

        let cong_full = self.congestion.is_full();
        tracing::debug!(
            tid,
            command = params.command,
            to = %to_str,
            ?socket_kind,
            cong_full,
            inflight_count = self.inflight.len(),
            "create_request: new inflight"
        );

        if cong_full {
            self.pending.push_back(PendingSend { tid });
        } else {
            let idx = self.inflight.len() - 1;
            self.send_inflight_at(idx);
        }

        Some(tid)
    }

    /// Send a one-shot DHT request out an arbitrary UDP socket (typically a
    /// puncher socket). The TID is allocated from the shared registry so the
    /// reply still matches via the existing inflight machinery, but no
    /// automatic retry will fire — we don't own the override socket beyond
    /// this call. Replies must be forwarded back through
    /// [`Self::handle_inbound_reply_bytes`].
    pub fn send_request_via_socket(
        &mut self,
        params: RequestParams,
        socket: &UdxSocket,
    ) -> Option<u16> {
        if self.destroying {
            return None;
        }

        let addr_str = format!("{}:{}", params.to.host, params.to.port);
        let addr: SocketAddr = addr_str.parse().ok()?;

        let tid = self.tid;
        self.tid = self.tid.wrapping_add(1);

        let include_id = !self.ephemeral;
        let id = if include_id {
            self.table.lock().ok().map(|t| *t.id())
        } else {
            None
        };

        let request = messages::Request {
            tid,
            to: params.to.clone(),
            id,
            token: params.token,
            internal: params.internal,
            command: params.command,
            target: params.target,
            value: params.value,
        };

        let buffer = match messages::encode_request_to_bytes(&request) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(err = %e, "send_request_via_socket: encode failed");
                return None;
            }
        };

        let buffer_len = buffer.len() as u64;
        if let Err(e) = socket.send_to(&buffer, addr) {
            tracing::warn!(err = %e, "send_request_via_socket: send_to failed");
            return None;
        }
        self.wire.bytes_sent.fetch_add(buffer_len, Ordering::Relaxed);

        self.stats.active += 1;
        self.stats.total += 1;

        let now = Instant::now();
        let timeout = Duration::from_millis(params.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

        self.inflight.push(InflightEntry {
            tid,
            to: params.to,
            addr,
            internal: params.internal,
            command: params.command,
            target: params.target,
            buffer,
            socket_kind: SocketKind::Client,
            sent: 1,
            retries: 0,
            deadline: now + timeout,
            timestamp: now,
            timeout,
        });

        Some(tid)
    }

    /// Inject reply bytes received on a non-`Io`-owned socket (puncher
    /// socket) into the response-matching path. The actor calls this when
    /// a `DhtCommand::InboundReplyBytes` arrives from the puncher-socket
    /// demux task.
    pub fn handle_inbound_reply_bytes(
        &mut self,
        addr: SocketAddr,
        data: Vec<u8>,
    ) -> Option<IoEvent> {
        self.wire
            .bytes_received
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.process_datagram(Datagram { data, addr }, SocketKind::Client)
    }

    /// Send a reply to an incoming request.
    pub fn send_reply(&mut self, req: &IncomingRequest, error: u64, value: Option<&[u8]>) {
        let socket_kind = req.reply_ctx.socket_kind;
        let include_token = error == 0;
        self.send_reply_internal(
            &req.from,
            ReplyInternalParams {
                socket_kind,
                tid: req.tid,
                target: req.target,
                error,
                include_token,
                value: value.map(|v| v.to_vec()),
            },
        );
    }

    /// Send a reply using explicit parameters (for deferred/delayed replies).
    /// Unlike `send_reply`, this does not require an `IncomingRequest` reference.
    pub(crate) fn send_reply_deferred(
        &mut self,
        to: &Ipv4Peer,
        ctx: ReplyContext,
        tid: u16,
        target: Option<NodeId>,
        error: u64,
        value: Option<&[u8]>,
    ) {
        let include_token = error == 0;
        self.send_reply_internal(
            to,
            ReplyInternalParams {
                socket_kind: ctx.socket_kind,
                tid,
                target,
                error,
                include_token,
                value: value.map(|v| v.to_vec()),
            },
        );
    }

    /// Generate a token for the given host using `secret[secret_index]`.
    pub fn token(&mut self, host: &str, secret_index: usize) -> [u8; 32] {
        self.init_secrets_if_needed();
        let secrets = match self.secrets.as_ref() {
            Some(s) => *s,
            None => return [0u8; 32],
        };
        let key = &secrets[secret_index % 2];
        match Blake2bMac256::new_from_slice(key) {
            Ok(mut mac) => {
                mac.update(host.as_bytes());
                let hash = mac.finalize().into_bytes();
                let mut token = [0u8; 32];
                token.copy_from_slice(&hash);
                token
            }
            Err(_) => [0u8; 32],
        }
    }

    /// Validate an incoming token against both secrets.
    pub fn validate_token(&mut self, host: &str, token: &[u8; 32]) -> bool {
        let t0 = self.token(host, 0);
        let t1 = self.token(host, 1);
        &t0 == token || &t1 == token
    }

    /// Send a fire-and-forget relay request (no inflight tracking, no response).
    /// Used by the Router to forward PEER_HANDSHAKE/PEER_HOLEPUNCH messages
    /// to relay targets.
    ///
    /// When `preserve_tid` is `Some(t)`, the outgoing packet uses tid `t`
    /// instead of allocating a fresh one. This is required for relay-back
    /// semantics that mirror Node `dht-rpc::Request.relay`: the inbound
    /// request's tid is propagated end-to-end so the eventual REPLY matches
    /// the original requester's inflight entry. Without tid preservation,
    /// relayed responses are silently dropped at intermediate hops.
    pub fn relay(
        &mut self,
        command: u64,
        target: Option<NodeId>,
        value: Option<Vec<u8>>,
        to: &Ipv4Peer,
        preserve_tid: Option<u16>,
    ) -> bool {
        if self.destroying {
            return false;
        }

        let addr: SocketAddr = match format!("{}:{}", to.host, to.port).parse() {
            Ok(a) => a,
            Err(_) => return false,
        };

        let tid = match preserve_tid {
            Some(t) => t,
            None => {
                let t = self.tid;
                self.tid = self.tid.wrapping_add(1);
                t
            }
        };

        let socket_kind = if self.firewalled {
            SocketKind::Client
        } else {
            SocketKind::Server
        };

        let include_id = !self.ephemeral && socket_kind == SocketKind::Server;
        let id = if include_id {
            self.table.lock().ok().map(|t| *t.id())
        } else {
            None
        };

        let request = messages::Request {
            tid,
            to: to.clone(),
            id,
            token: None,
            internal: false,
            command,
            target,
            value,
        };

        let buffer = match messages::encode_request_to_bytes(&request) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(err = %e, "relay: encode failed");
                return false;
            }
        };

        let socket = match socket_kind {
            SocketKind::Client => &self.client_socket,
            SocketKind::Server => &self.server_socket,
        };

        let buffer_len = buffer.len() as u64;
        if let Err(e) = socket.send_to(&buffer, addr) {
            tracing::warn!(err = %e, "relay: send_to failed");
            return false;
        }
        self.wire.bytes_sent.fetch_add(buffer_len, Ordering::Relaxed);

        true
    }

    /// Send a fire-and-forget REPLY (Response packet) to a specific address,
    /// using the provided tid. Mirrors Node `dht-rpc::Request.reply(value,
    /// { to })`: the tid is taken from an inbound request whose tid was
    /// preserved end-to-end through a relay chain, so the eventual REPLY
    /// matches the original requester's inflight entry at the chain's
    /// destination.
    ///
    /// Used by the handshake router's FROM_SERVER case (FE-holder finalising
    /// the tid-preserved chain by replying directly to the original client).
    /// Unlike [`Self::send_reply`], the destination address is independent
    /// of any inbound request; unlike [`Self::send_reply_deferred`], the
    /// socket kind is derived from current firewall state rather than passed
    /// in via a `ReplyContext`.
    pub fn reply_to(
        &mut self,
        tid: u16,
        target: Option<NodeId>,
        to: &Ipv4Peer,
        value: Option<Vec<u8>>,
    ) -> bool {
        if self.destroying {
            return false;
        }

        let socket_kind = if self.firewalled {
            SocketKind::Client
        } else {
            SocketKind::Server
        };

        self.send_reply_internal(
            to,
            ReplyInternalParams {
                socket_kind,
                tid,
                target,
                error: 0,
                include_token: true,
                value,
            },
        );

        true
    }

    /// Destroy the IO layer, closing both sockets.
    pub async fn destroy(mut self) -> IoResult<()> {
        self.destroying = true;
        for entry in self.inflight.drain(..) {
            self.congestion.recv();
            self.stats.active = self.stats.active.saturating_sub(1);
            tracing::debug!(tid = entry.tid, "destroy: dropping inflight request");
        }
        self.client_socket.close().await?;
        self.server_socket.close().await?;
        Ok(())
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn init_secrets_if_needed(&mut self) {
        if self.secrets.is_none() {
            let s0: [u8; 32] = rand::random();
            let s1: [u8; 32] = rand::random();
            self.secrets = Some([s0, s1]);
        }
    }

    /// Encode and send a response message.
    fn send_reply_internal(
        &mut self,
        to: &Ipv4Peer,
        params: ReplyInternalParams,
    ) {
        let include_id = !self.ephemeral && params.socket_kind == SocketKind::Server;

        let (id, closer_nodes) = match self.table.lock() {
            Ok(table) => {
                let id = if include_id { Some(*table.id()) } else { None };
                let closer_nodes: Vec<Ipv4Peer> = if let Some(t) = &params.target {
                    table
                        .closest(t, K)
                        .into_iter()
                        .map(|n| Ipv4Peer {
                            host: n.host.clone(),
                            port: n.port,
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                (id, closer_nodes)
            }
            Err(_) => (None, Vec::new()),
        };

        let token = if params.include_token {
            Some(self.token(&to.host, 1))
        } else {
            None
        };

        let response = Response {
            tid: params.tid,
            to: to.clone(),
            id,
            token,
            closer_nodes,
            error: params.error,
            value: params.value,
        };

        let bytes = match messages::encode_response_to_bytes(&response) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(err = %e, "send_reply_internal: encode failed");
                return;
            }
        };

        let addr_str = format!("{}:{}", to.host, to.port);
        let addr: SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(err = %e, "send_reply_internal: invalid address");
                return;
            }
        };

        let socket = match params.socket_kind {
            SocketKind::Client => &self.client_socket,
            SocketKind::Server => &self.server_socket,
        };

        let bytes_len = bytes.len() as u64;
        if let Err(e) = socket.send_to(&bytes, addr) {
            tracing::warn!(err = %e, "send_reply_internal: send_to failed");
        } else {
            self.wire.bytes_sent.fetch_add(bytes_len, Ordering::Relaxed);
        }
    }

    /// Actually send the inflight entry at index `idx`.
    fn send_inflight_at(&mut self, idx: usize) {
        let (buffer, addr, socket_kind) = {
            let entry = &mut self.inflight[idx];
            entry.sent += 1;
            entry.deadline = Instant::now() + entry.timeout;
            (entry.buffer.clone(), entry.addr, entry.socket_kind)
        };

        self.congestion.send();

        let socket = match socket_kind {
            SocketKind::Client => &self.client_socket,
            SocketKind::Server => &self.server_socket,
        };

        let buffer_len = buffer.len() as u64;
        if let Err(e) = socket.send_to(&buffer, addr) {
            tracing::warn!(err = %e, "send_inflight_at: send_to failed");
        } else {
            self.wire.bytes_sent.fetch_add(buffer_len, Ordering::Relaxed);
        }
    }

    /// Decode and dispatch a datagram from either socket.
    fn process_datagram(&mut self, datagram: Datagram, socket_kind: SocketKind) -> Option<IoEvent> {
        if datagram.data.len() < 2 {
            return None;
        }

        let (host, port) = match datagram.addr {
            SocketAddr::V4(v4) => (v4.ip().to_string(), v4.port()),
            SocketAddr::V6(_) => return None,
        };

        if port == 0 {
            return None;
        }

        let from = Ipv4Peer { host, port };

        match messages::decode_message(&datagram.data) {
            Err(ref e) => {
                tracing::debug!(
                    from = %format!("{}:{}", from.host, from.port),
                    len = datagram.data.len(),
                    first_byte = datagram.data.first().copied().unwrap_or(0),
                    err = %e,
                    "process_datagram: decode failed"
                );
                None
            }
            Ok(messages::Message::Request(req)) => {
                // Validate token if present.
                if req.token.is_some() {
                    let token_val = req.token.as_ref().map(|t| self.validate_token(&from.host, t));
                    if token_val == Some(false) {
                        let tid = req.tid;
                        let target = req.target;
                        let from_clone = from.clone();
                        self.send_reply_internal(
                            &from_clone,
                            ReplyInternalParams {
                                socket_kind,
                                tid,
                                target,
                                error: ERROR_INVALID_TOKEN,
                                include_token: true,
                                value: None,
                            },
                        );
                        return None;
                    }
                }

                // Validate incoming ID.
                let validated_id = req.id.and_then(|id| {
                    let expected = peer::peer_id(&from.host, from.port);
                    if expected == id {
                        Some(expected)
                    } else {
                        None
                    }
                });

                Some(IoEvent::IncomingRequest(IncomingRequest {
                    tid: req.tid,
                    from: from.clone(),
                    to: req.to,
                    id: validated_id,
                    token: req.token,
                    internal: req.internal,
                    command: req.command,
                    target: req.target,
                    value: req.value,
                    reply_ctx: ReplyContext { socket_kind },
                }))
            }

            Ok(messages::Message::Response(res)) => {
                // Find the matching inflight entry by TID.
                let pos = match self.inflight.iter().position(|e| e.tid == res.tid) {
                    Some(p) => p,
                    None => {
                        tracing::debug!(
                            tid = res.tid,
                            from = %format!("{}:{}", from.host, from.port),
                            "response TID not found in inflight"
                        );
                        return None;
                    }
                };
                let entry = self.inflight.swap_remove(pos);

                let rtt = entry.timestamp.elapsed();

                self.congestion.recv();
                self.stats.active = self.stats.active.saturating_sub(1);
                self.stats.responses += 1;

                // Validate incoming ID.
                let validated_id = res.id.and_then(|id| {
                    let expected = peer::peer_id(&from.host, from.port);
                    if expected == id {
                        Some(expected)
                    } else {
                        None
                    }
                });

                let request = ResolvedRequest {
                    tid: entry.tid,
                    to: entry.to,
                    command: entry.command,
                    internal: entry.internal,
                    target: entry.target,
                };

                Some(IoEvent::Response {
                    tid: res.tid,
                    from,
                    to: res.to,
                    id: validated_id,
                    token: res.token,
                    closer_nodes: res.closer_nodes,
                    error: res.error,
                    value: res.value,
                    rtt,
                    request,
                })
            }

        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── CongestionWindow tests ────────────────────────────────────────────────

    #[test]
    fn congestion_window_basic() {
        let mut cw = CongestionWindow::new(10);
        assert!(!cw.is_full());

        for _ in 0..10 {
            cw.send();
        }
        // window[0] == 10 == max_window → full
        assert!(cw.is_full());

        cw.recv();
        assert!(!cw.is_full());
    }

    #[test]
    fn congestion_window_drain_clears_oldest() {
        let mut cw = CongestionWindow::new(80);
        // Send 5 in quarter 0.
        for _ in 0..5 {
            cw.send();
        }
        assert_eq!(cw.total, 5);

        // Drain advances to quarter 1, clears quarter 1 (which is 0).
        cw.drain();
        assert_eq!(cw.total, 5); // nothing cleared yet (quarter 1 was 0)

        // Advance through all 4 quarters; original quarter 0 becomes the "oldest".
        cw.drain(); // → quarter 2
        cw.drain(); // → quarter 3
        cw.drain(); // → quarter 0 again; clears window[0] = 5
        assert_eq!(cw.total, 0);
        assert_eq!(cw.window[0], 0);
    }

    #[test]
    fn congestion_window_full_condition_total() {
        // total >= 2 * max_window triggers full
        let mut cw = CongestionWindow::new(5);
        // Send across two quarters so no single quarter hits max.
        for _ in 0..5 {
            cw.send();
        }
        cw.drain(); // advance to quarter 1
        for _ in 0..5 {
            cw.send();
        }
        // total = 10 = 2 * 5 → full
        assert!(cw.is_full());
    }

    #[test]
    fn congestion_window_full_condition_single_quarter() {
        let mut cw = CongestionWindow::new(3);
        cw.send();
        cw.send();
        cw.send();
        // window[0] == 3 == max_window → full
        assert!(cw.is_full());
    }

    #[test]
    fn congestion_window_clear() {
        let mut cw = CongestionWindow::new(10);
        for _ in 0..5 {
            cw.send();
        }
        cw.clear();
        assert_eq!(cw.total, 0);
        assert!(!cw.is_full());
    }

    // ── TID wrap test ─────────────────────────────────────────────────────────

    #[test]
    fn tid_wraps() {
        // u16 wrapping: 65535 + 1 = 0
        let tid: u16 = 65535;
        let next = tid.wrapping_add(1);
        assert_eq!(next, 0);
    }

    // ── Token tests ───────────────────────────────────────────────────────────

    fn compute_token(host: &str, secret: &[u8; 32]) -> [u8; 32] {
        let mut mac = Blake2bMac256::new_from_slice(secret).unwrap();
        mac.update(host.as_bytes());
        let hash = mac.finalize().into_bytes();
        let mut token = [0u8; 32];
        token.copy_from_slice(&hash);
        token
    }

    #[test]
    fn token_generation_deterministic() {
        let secret = [0xABu8; 32];
        let host = "192.168.1.1";
        let t1 = compute_token(host, &secret);
        let t2 = compute_token(host, &secret);
        assert_eq!(t1, t2);
    }

    #[test]
    fn token_generation_different_host() {
        let secret = [0xABu8; 32];
        let t1 = compute_token("192.168.1.1", &secret);
        let t2 = compute_token("192.168.1.2", &secret);
        assert_ne!(t1, t2);
    }

    #[test]
    fn token_generation_different_secret() {
        let s1 = [0xABu8; 32];
        let s2 = [0xCDu8; 32];
        let host = "10.0.0.1";
        let t1 = compute_token(host, &s1);
        let t2 = compute_token(host, &s2);
        assert_ne!(t1, t2);
    }

    #[test]
    fn token_validation_both_secrets() {
        let secrets = [[0x11u8; 32], [0x22u8; 32]];
        let host = "10.0.0.1";

        let t0 = compute_token(host, &secrets[0]);
        let t1 = compute_token(host, &secrets[1]);

        // Simulate validate_token logic: token matches either secret[0] or secret[1].
        let validate = |token: &[u8; 32]| -> bool {
            let v0 = compute_token(host, &secrets[0]);
            let v1 = compute_token(host, &secrets[1]);
            &v0 == token || &v1 == token
        };

        assert!(validate(&t0));
        assert!(validate(&t1));
    }

    #[test]
    fn token_validation_wrong_host_fails() {
        let secrets = [[0x11u8; 32], [0x22u8; 32]];
        let host = "10.0.0.1";
        let wrong_host = "10.0.0.2";

        let token = compute_token(host, &secrets[0]);

        let validate_wrong = |token: &[u8; 32]| -> bool {
            let v0 = compute_token(wrong_host, &secrets[0]);
            let v1 = compute_token(wrong_host, &secrets[1]);
            &v0 == token || &v1 == token
        };

        assert!(!validate_wrong(&token));
    }

}
