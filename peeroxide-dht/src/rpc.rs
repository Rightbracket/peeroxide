#![deny(clippy::all)]

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Instant};

use libudx::{UdxRuntime, UdxSocket};

use crate::io::{Io, IoConfig, IoEvent, ReplyContext, RequestParams, TimeoutEvent};
use crate::messages::{Command, Ipv4Peer};
use crate::nat::Nat;
use crate::peer::{peer_id, NodeId};
use crate::query::{
    parse_bootstrap_str, resolve_bootstrap_node, IoResponseData, Query, QueryReply, QueryRequest,
    QueryResult,
};
use crate::routing_table::{Node, RoutingTable, TableEvent, K};

const TICK_INTERVAL_MS: u64 = 5_000;
const DRAIN_INTERVAL_MS: u64 = 750;
const SLEEPING_INTERVAL_MS: u64 = 3 * TICK_INTERVAL_MS;
const REFRESH_TICKS: u64 = 60;
/// The least time between two tries to resolve the bootstrap hostnames that
/// did not resolve.
const BOOTSTRAP_RESOLVE_INTERVAL: Duration = Duration::from_secs(10);
const RECENT_NODE: u64 = 12;
const OLD_NODE: u64 = 360;
const MAX_REPINGING: u32 = 3;
const DOWN_HINTS_RATE_LIMIT: u32 = 50;

const CMD_PING: u64 = Command::Ping as u64;
const CMD_PING_NAT: u64 = Command::PingNat as u64;
const CMD_FIND_NODE: u64 = Command::FindNode as u64;
const CMD_DOWN_HINT: u64 = Command::DownHint as u64;
const CMD_DELAYED_PING: u64 = Command::DelayedPing as u64;

const ERR_UNKNOWN_COMMAND: u64 = 1;

/// Internal sentinel sent on `UserRequest::reply_tx` by [`UserRequest::release`]
/// to instruct the deferred-reply pipeline to suppress the auto-emitted REPLY
/// packet (the relay-forwarding paths consume the inbound REQUEST without
/// responding; the original requester's REPLY arrives via the tid-preserved
/// chain instead).
const SUPPRESS_REPLY_SENTINEL: u64 = u64::MAX;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
/// Errors returned by [`DhtHandle`] and [`spawn`].
#[non_exhaustive]
pub enum DhtError {
    /// Underlying I/O failed.
    #[error("IO error: {0}")]
    Io(#[from] crate::io::IoError),
    /// The DHT node has been destroyed.
    #[error("node destroyed")]
    Destroyed,
    /// The internal command channel is closed.
    #[error("command channel closed")]
    ChannelClosed,
    /// Bootstrapping did not complete successfully.
    #[error("bootstrap failed")]
    BootstrapFailed,
    /// A request failed with the given message.
    #[error("request failed: {0}")]
    RequestFailed(String),
    /// The [`DhtConfig`] passed to [`spawn`] is invalid.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

// ── Public config / request / response types ──────────────────────────────────

/// Configuration for creating a DHT node.
///
/// # Node id of a persistent node
///
/// A persistent (non-ephemeral) node advertises a node id, and other nodes
/// only accept it if it equals [`peer_id`]`(host, port)` of the address they
/// see the node on. The id is chosen as follows:
///
/// - [`public_host`](Self::public_host) set: `peer_id(public_host, port)`.
/// - Otherwise, [`host`](Self::host) is a specific address:
///   `peer_id(host, port)`.
/// - Otherwise (`host` is `0.0.0.0`): the node learns its public address
///   from the address other nodes report in direct replies to its own
///   requests, first during bootstrap (the most reported one, after which it
///   queries the bootstrap nodes again with its new id) and, failing that,
///   later (the first one). Until it has, it runs as ephemeral
///   (advertises no id), so other nodes do not add it to their routing tables
///   or use it to store announcements. A [`firewalled`](Self::firewalled)
///   node sends its requests from a different socket than the one it is
///   reached on, so it can't learn its address this way.
///
/// The first node of a new or private DHT has no bootstrap nodes to ask, so
/// if it is bound to a wildcard host it cannot learn its address and logs a
/// warning. Give such a node its public address with
/// [`public_host`](Self::public_host) or [`DhtConfig::bootstrapper`], or bind
/// it to that address directly. Node.js `dht-rpc` has the same requirement
/// (`DHT.bootstrapper(port, host)`).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DhtConfig {
    /// Bootstrap node addresses.
    pub bootstrap: Vec<String>,
    /// Local port to bind.
    pub port: u16,
    /// Local host to bind.
    pub host: String,
    /// Whether to force ephemeral mode.
    ///
    /// `None` (the default) means persistent when `bootstrap` is empty and
    /// ephemeral otherwise.
    pub ephemeral: Option<bool>,
    /// Whether to advertise as firewalled.
    pub firewalled: bool,
    /// Query concurrency limit.
    pub concurrency: usize,
    /// Maximum query window size.
    pub max_window: usize,
    /// Public IPv4 address other nodes reach this node on.
    ///
    /// A persistent node derives its node id from this address and the bound
    /// port, instead of from [`host`](Self::host) or from the addresses
    /// other nodes report back. Use it when the node is bound to a wildcard or
    /// private address but reachable on a known public IPv4 address with the
    /// same port, such as the first node of a DHT or a host behind 1:1 NAT.
    /// An unfirewalled persistent node also reports it from
    /// [`DhtHandle::remote_address`] whenever the NAT analysis has none.
    /// Must be an IPv4 address and not `0.0.0.0`; otherwise [`spawn`] returns
    /// [`DhtError::InvalidConfig`]. Ignored for ephemeral nodes.
    pub public_host: Option<String>,
}

impl DhtConfig {
    /// Config for a bootstrap node: persistent, unfirewalled, with no
    /// bootstrap nodes of its own, bound to `0.0.0.0:port` and known to the
    /// network as `public_host:port`.
    ///
    /// Mirrors Node.js `DHT.bootstrapper(port, host)`. See
    /// [`public_host`](Self::public_host) for why the public address is
    /// needed.
    pub fn bootstrapper(port: u16, public_host: impl Into<String>) -> Self {
        Self {
            port,
            ephemeral: Some(false),
            firewalled: false,
            public_host: Some(public_host.into()),
            ..Self::default()
        }
    }
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            bootstrap: vec![],
            port: 0,
            host: "0.0.0.0".to_string(),
            ephemeral: None,
            firewalled: true,
            concurrency: 10,
            max_window: 80,
            public_host: None,
        }
    }
}

#[derive(Debug, Clone)]
/// Response to a ping request.
#[non_exhaustive]
pub struct PingResponse {
    /// Remote peer that replied.
    pub from: Ipv4Peer,
    /// Optional peer id reported by the remote node.
    pub id: Option<NodeId>,
    /// Round-trip time for the ping.
    pub rtt: Duration,
    /// Reflexive address: our address as seen by the remote node.
    pub to: Option<Ipv4Peer>,
    /// Nodes returned by the remote peer (closer nodes from its routing table).
    pub closer_nodes: Vec<Ipv4Peer>,
}

#[derive(Debug, Clone)]
/// Data returned from a DHT request.
#[non_exhaustive]
pub struct ResponseData {
    /// Remote peer that replied.
    pub from: Ipv4Peer,
    /// Optional peer id reported by the remote node.
    pub id: Option<NodeId>,
    /// Optional response token.
    pub token: Option<[u8; 32]>,
    /// Nodes returned by the remote peer.
    pub closer_nodes: Vec<Ipv4Peer>,
    /// Response error code.
    pub error: u64,
    /// Optional response value.
    pub value: Option<Vec<u8>>,
    /// Round-trip time for the request.
    pub rtt: Duration,
}

#[derive(Debug, Clone)]
/// Parameters for a user-driven DHT query.
#[non_exhaustive]
pub struct UserQueryParams {
    /// Query target node id.
    pub target: NodeId,
    /// RPC command to send.
    pub command: u64,
    /// Optional query payload.
    pub value: Option<Vec<u8>>,
    /// Whether the query is a commit.
    pub commit: bool,
    /// Optional per-query concurrency override.
    pub concurrency: Option<usize>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
/// Parameters for a user-driven DHT request.
pub struct UserRequestParams {
    /// Optional request token.
    pub token: Option<[u8; 32]>,
    /// RPC command to send.
    pub command: u64,
    /// Optional target node id.
    pub target: Option<NodeId>,
    /// Optional request payload.
    pub value: Option<Vec<u8>>,
    /// Optional per-request timeout override (ms). Defaults to the dht-rpc DEFAULT_TIMEOUT_MS.
    pub timeout_ms: Option<u64>,
    /// Optional per-request retries override. Defaults to the dht-rpc DEFAULT_RETRIES.
    pub retries: Option<u32>,
}

/// An incoming user-facing request forwarded from the DHT.
#[non_exhaustive]
pub struct UserRequest {
    /// Origin peer for the request.
    pub from: Ipv4Peer,
    /// Optional origin peer id.
    pub id: Option<NodeId>,
    /// Transaction id from the inbound request, preserved end-to-end through
    /// any relay hops so the eventual REPLY matches the originator's inflight
    /// entry. Mirrors Node `dht-rpc/lib/io.js` Request.tid semantics.
    pub tid: u16,
    /// Optional request token.
    pub token: Option<[u8; 32]>,
    /// RPC command received.
    pub command: u64,
    /// Optional target node id.
    pub target: Option<NodeId>,
    /// Optional request payload.
    pub value: Option<Vec<u8>>,
    reply_tx: Option<oneshot::Sender<(u64, Option<Vec<u8>>)>>,
}

impl UserRequest {
    /// Replies to the request with a value and success code.
    pub fn reply(&mut self, value: Option<Vec<u8>>) {
        if let Some(tx) = self.reply_tx.take() {
            let _ = tx.send((0, value));
        }
    }

    /// Replies to the request with an error code.
    pub fn error(&mut self, code: u64) {
        if let Some(tx) = self.reply_tx.take() {
            let _ = tx.send((code, None));
        }
    }

    /// Consume the request without emitting any reply. The deferred-reply
    /// pipeline that normally fires `ERR_UNKNOWN_COMMAND` on a dropped
    /// `reply_tx` is suppressed via an internal sentinel error code
    /// (`SUPPRESS_REPLY_SENTINEL`).
    ///
    /// Used by relay-forwarding paths that consume the inbound REQUEST
    /// without responding — the original requester's REPLY arrives via the
    /// tid-preserved relay chain, not via this hop. Mirrors Node behaviour:
    /// `dht-rpc::Request.relay(value, to)` does not also call
    /// `req.reply(...)`. Without this, the inbound `req` would auto-emit a
    /// REPLY using the preserved tid (= original client's tid), racing
    /// against the genuine REPLY arriving via the chain's tail and causing
    /// the client to match on the first arrival (typically an error).
    pub fn release(&mut self) {
        if let Some(tx) = self.reply_tx.take() {
            let _ = tx.send((SUPPRESS_REPLY_SENTINEL, None));
        }
    }
}

// ── Internal command channel ──────────────────────────────────────────────────

enum DhtCommand {
    Bootstrapped {
        reply_tx: oneshot::Sender<Result<(), DhtError>>,
    },
    Ping {
        host: String,
        port: u16,
        reply_tx: oneshot::Sender<Result<PingResponse, DhtError>>,
    },
    FindNode {
        target: NodeId,
        reply_tx: oneshot::Sender<Result<Vec<QueryReply>, DhtError>>,
    },
    Query {
        params: UserQueryParams,
        reply_tx: oneshot::Sender<Result<Vec<QueryReply>, DhtError>>,
    },
    Request {
        params: UserRequestParams,
        host: String,
        port: u16,
        reply_tx: oneshot::Sender<Result<ResponseData, DhtError>>,
    },
    Relay {
        command: u64,
        target: Option<NodeId>,
        value: Option<Vec<u8>>,
        to: Ipv4Peer,
        preserve_tid: Option<u16>,
    },
    SendReplyTo {
        tid: u16,
        target: Option<NodeId>,
        to: Ipv4Peer,
        value: Option<Vec<u8>>,
    },
    SubscribeRequests {
        reply_tx: oneshot::Sender<mpsc::UnboundedReceiver<UserRequest>>,
    },
    TableSize {
        reply_tx: oneshot::Sender<usize>,
    },
    Destroy {
        reply_tx: oneshot::Sender<Result<(), DhtError>>,
    },
    LocalPort {
        reply_tx: oneshot::Sender<Result<u16, DhtError>>,
    },
    TableId {
        reply_tx: oneshot::Sender<Option<NodeId>>,
    },
    ServerSocket {
        reply_tx: oneshot::Sender<Option<UdxSocket>>,
    },
    ListenSocket {
        reply_tx: oneshot::Sender<Option<UdxSocket>>,
    },
    RemoteAddress {
        reply_tx: oneshot::Sender<(Option<Ipv4Peer>, u64)>,
    },
    #[allow(dead_code)] // constructed by T6 forwarder task; variant + handler land in T4
    InboundReplyBytes {
        addr: SocketAddr,
        data: Vec<u8>,
    },
    PingViaSocket {
        target: Ipv4Peer,
        socket: UdxSocket,
        reply_tx: oneshot::Sender<Result<PingResponse, DhtError>>,
    },
}

// ── Standalone (non-query) inflight tracking ──────────────────────────────────

enum StandaloneRequest {
    Ping(oneshot::Sender<Result<PingResponse, DhtError>>),
    UserRequest(oneshot::Sender<Result<ResponseData, DhtError>>),
    Reping {
        new_node: Node,
        old_node_id: NodeId,
        last_seen_tick: u64,
    },
    Check {
        node_id: NodeId,
        last_seen_tick: u64,
    },
}

struct DeferredReply {
    from: Ipv4Peer,
    reply_ctx: ReplyContext,
    tid: u16,
    target: Option<NodeId>,
    error: u64,
    value: Option<Vec<u8>>,
}

// ── DhtHandle (user-facing, Send + Sync + Clone) ──────────────────────────────

/// Handle for interacting with a running DHT node.
#[derive(Clone)]
pub struct DhtHandle {
    cmd_tx: mpsc::UnboundedSender<DhtCommand>,
    wire: crate::io::WireCounters,
    table: Arc<Mutex<RoutingTable>>,
}

impl DhtHandle {
    /// Snapshot of cumulative wire bytes (sent, received) since the DHT
    /// started. Counts every UDP datagram exchanged by this node, including
    /// retries, queries, replies, and relays.
    pub fn wire_stats(&self) -> (u64, u64) {
        self.wire.snapshot()
    }

    /// Borrow the shared wire-counter handle. Useful when you want a long-
    /// lived reference (e.g. for periodic sampling from a UI thread) without
    /// going through `wire_stats()` repeatedly.
    pub fn wire_counters(&self) -> crate::io::WireCounters {
        self.wire.clone()
    }

    /// Return up to `limit` recently-responsive DHT nodes from the local
    /// routing table, ordered by `seen_tick` descending. Used by
    /// [`crate::nat::Nat::auto_sample`] to pick ping targets that are
    /// likely to respond. Applies Node's skip-5-if-cache≥8 rule so the
    /// freshest nodes (probably engaged in current traffic) are not
    /// re-used as NAT-sample targets.
    pub(crate) fn recent_nodes(&self, limit: usize) -> Vec<Ipv4Peer> {
        let table = match self.table.lock() {
            Ok(t) => t,
            Err(_) => return Vec::new(),
        };
        table
            .recent(limit)
            .into_iter()
            .map(|n| Ipv4Peer {
                host: n.host.clone(),
                port: n.port,
            })
            .collect()
    }

    /// Forward inbound reply bytes from a non-`Io` socket (puncher socket)
    /// to the actor for TID matching. Used by the auto_sample forwarder
    /// task. Fire-and-forget; the actor is the source of truth for TID
    /// resolution.
    pub(crate) fn forward_inbound_reply_bytes(&self, addr: SocketAddr, data: Vec<u8>) {
        let _ = self
            .cmd_tx
            .send(DhtCommand::InboundReplyBytes { addr, data });
    }

    /// Insert a node directly into the local routing table, bypassing the
    /// normal discovery/query path. Used only by unit tests that need
    /// deterministic `recent_nodes()` candidates (e.g. `Nat::auto_sample`
    /// tests) without a live bootstrap round-trip.
    #[cfg(test)]
    pub(crate) fn insert_node_for_test(&self, node: crate::routing_table::Node) {
        if let Ok(mut table) = self.table.lock() {
            table.add(node);
        }
    }
}

impl DhtHandle {
    /// Waits until the node has finished bootstrapping.
    pub async fn bootstrapped(&self) -> Result<(), DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::Bootstrapped { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Sends a ping to `host:port`.
    pub async fn ping(&self, host: &str, port: u16) -> Result<PingResponse, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::Ping {
                host: host.to_string(),
                port,
                reply_tx: tx,
            })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Send a one-shot ping out via the supplied `socket` (typically a
    /// puncher socket) and resolve with the reply. Used by
    /// [`crate::nat::Nat::auto_sample`] to seed reflexive NAT samples
    /// from the puncher socket's own UDP binding, since the reflexive
    /// observation must reflect that socket's NAT mapping — not the
    /// main DHT socket's. No automatic retry.
    #[allow(dead_code)] // wired by T6 (Nat::auto_sample)
    pub(crate) async fn ping_via_socket(
        &self,
        target: Ipv4Peer,
        socket: UdxSocket,
    ) -> Result<PingResponse, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::PingViaSocket {
                target,
                socket,
                reply_tx: tx,
            })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Runs a `find_node` query for `target`.
    pub async fn find_node(&self, target: NodeId) -> Result<Vec<QueryReply>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::FindNode {
                target,
                reply_tx: tx,
            })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Runs a custom DHT query.
    pub async fn query(&self, params: UserQueryParams) -> Result<Vec<QueryReply>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::Query {
                params,
                reply_tx: tx,
            })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Sends a request to a remote peer.
    pub async fn request(
        &self,
        params: UserRequestParams,
        host: &str,
        port: u16,
    ) -> Result<ResponseData, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::Request {
                params,
                host: host.to_string(),
                port,
                reply_tx: tx,
            })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Fire-and-forget relay send (no response tracking).
    /// Relays an RPC command to `to` without waiting for a reply.
    pub fn relay(
        &self,
        command: u64,
        target: Option<NodeId>,
        value: Option<Vec<u8>>,
        to: &Ipv4Peer,
    ) -> Result<(), DhtError> {
        self.cmd_tx
            .send(DhtCommand::Relay {
                command,
                target,
                value,
                to: to.clone(),
                preserve_tid: None,
            })
            .map_err(|_| DhtError::ChannelClosed)
    }

    /// Fire-and-forget relay send that PRESERVES the inbound request's tid.
    ///
    /// Used by handshake-routing logic when forwarding a response back through
    /// a relay chain (mode FROM_SERVER). Mirrors Node `dht-rpc::Request.relay`:
    /// the tid is kept end-to-end so the original requester's inflight entry
    /// matches when the eventual REPLY arrives. Without this, the response is
    /// silently dropped at the relay hop.
    pub fn relay_with_tid(
        &self,
        command: u64,
        target: Option<NodeId>,
        value: Option<Vec<u8>>,
        to: &Ipv4Peer,
        tid: u16,
    ) -> Result<(), DhtError> {
        self.cmd_tx
            .send(DhtCommand::Relay {
                command,
                target,
                value,
                to: to.clone(),
                preserve_tid: Some(tid),
            })
            .map_err(|_| DhtError::ChannelClosed)
    }

    /// Fire-and-forget REPLY send to an arbitrary address using the supplied
    /// tid. Mirrors Node `dht-rpc::Request.reply(value, { to })`.
    ///
    /// Used by handshake routing when an FE-holder finalises the tid-preserved
    /// relay chain by sending the REPLY directly to the original client
    /// (FROM_SERVER → REPLY transition). The `tid` must be the inbound
    /// request's tid (= the original client's inflight tid, propagated end-to-end
    /// via [`Self::relay_with_tid`] at the previous hop).
    pub fn send_reply_to(
        &self,
        tid: u16,
        target: Option<NodeId>,
        to: &Ipv4Peer,
        value: Option<Vec<u8>>,
    ) -> Result<(), DhtError> {
        self.cmd_tx
            .send(DhtCommand::SendReplyTo {
                tid,
                target,
                to: to.clone(),
                value,
            })
            .map_err(|_| DhtError::ChannelClosed)
    }

    /// Subscribes to forwarded user requests.
    pub async fn subscribe_requests(&self) -> Option<mpsc::UnboundedReceiver<UserRequest>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::SubscribeRequests { reply_tx: tx })
            .ok()?;
        rx.await.ok()
    }

    /// Returns the current routing table size.
    pub async fn table_size(&self) -> Result<usize, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::TableSize { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)
    }

    /// Destroys the running DHT node.
    pub async fn destroy(&self) -> Result<(), DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::Destroy { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Returns the local bound port.
    pub async fn local_port(&self) -> Result<u16, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::LocalPort { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)?
    }

    /// Returns the node's current routing table ID, or None if not yet assigned.
    pub async fn table_id(&self) -> Result<Option<NodeId>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::TableId { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)
    }

    /// Returns a shared reference to the DHT server socket for UDX stream multiplexing.
    pub async fn server_socket(&self) -> Result<Option<UdxSocket>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::ServerSocket { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)
    }

    /// Returns the actual listen socket (the socket bound to the advertised port).
    pub async fn listen_socket(&self) -> Result<Option<UdxSocket>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::ListenSocket { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        rx.await.map_err(|_| DhtError::ChannelClosed)
    }

    /// Returns the externally-observed IPv4 address of this node, as inferred
    /// from the `to` field of inbound DHT responses (NAT consensus).
    ///
    /// Returns `None` if the NAT analysis hasn't yet settled — typically until
    /// at least 3 responses have been processed when running firewalled, or
    /// 1 response when running unfirewalled. A persistent, unfirewalled node
    /// reports the address its node id derives from whenever the NAT analysis
    /// has none: [`DhtConfig::public_host`], or the address it learned (if
    /// other nodes see it on its bound port).
    /// Mirrors Node Hyperswarm's `dht.remoteAddress()`.
    pub async fn remote_address(&self) -> Result<Option<Ipv4Peer>, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::RemoteAddress { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        let (addr, _firewall) = rx.await.map_err(|_| DhtError::ChannelClosed)?;
        Ok(addr)
    }

    /// Returns the current firewall classification (one of `FIREWALL_OPEN`,
    /// `FIREWALL_UNKNOWN`, `FIREWALL_CONSISTENT`, `FIREWALL_RANDOM` constants
    /// from [`crate::hyperdht_messages`]).
    ///
    /// Mirrors Node Hyperswarm's `dht.firewalled`.
    pub async fn firewalled(&self) -> Result<u64, DhtError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(DhtCommand::RemoteAddress { reply_tx: tx })
            .map_err(|_| DhtError::ChannelClosed)?;
        let (_addr, firewall) = rx.await.map_err(|_| DhtError::ChannelClosed)?;
        Ok(firewall)
    }
}

// ── DhtNode (internal background actor) ──────────────────────────────────────

struct DhtNode {
    io: Io,
    table: Arc<Mutex<RoutingTable>>,
    config: DhtConfig,
    local_port: u16,
    tick: u64,
    refresh_ticks: u64,
    repinging: u32,
    bootstrapped: bool,
    bootstrap_waiters: Vec<oneshot::Sender<Result<(), DhtError>>>,

    active_queries: HashMap<u64, Query>,
    tid_to_query: HashMap<u16, (u64, bool)>,
    standalone_tids: HashMap<u16, StandaloneRequest>,
    next_query_id: u64,

    cmd_rx: mpsc::UnboundedReceiver<DhtCommand>,
    request_subscribers: Vec<mpsc::UnboundedSender<UserRequest>>,

    destroyed: bool,
    last_tick_time: Instant,
    down_hints_per_tick: u32,
    bootstrap_query_id: Option<u64>,
    deferred_reply_tx: mpsc::UnboundedSender<DeferredReply>,
    deferred_reply_rx: mpsc::UnboundedReceiver<DeferredReply>,

    needs_id_update: bool,
    addr_samples: Vec<Ipv4Peer>,
    nat: Nat,
    /// Address of a persistent, unfirewalled node, from `public_host` or the
    /// address its id was derived from: reported by `remote_address` when the
    /// NAT sampler has none, as for Node's `DHT.bootstrapper`.
    public_addr: Option<Ipv4Peer>,

    /// Configured bootstrap nodes whose hostname did not resolve, as when
    /// DNS was down at start. Node resolves them for every query; here they
    /// are resolved again, at most every [`BOOTSTRAP_RESOLVE_INTERVAL`],
    /// when a query needs the bootstrap nodes.
    unresolved_bootstrap: Vec<String>,
    last_bootstrap_resolve: Option<Instant>,
    /// Bootstrap hostnames being resolved.
    bootstrap_resolving: usize,
    /// Queries held from finishing until the bootstrap hostnames being
    /// resolved are, so that they can ask those too.
    held_queries: Vec<u64>,
    bootstrap_resolved_tx: mpsc::UnboundedSender<(String, Option<String>)>,
    bootstrap_resolved_rx: mpsc::UnboundedReceiver<(String, Option<String>)>,
}

impl DhtNode {
    async fn run(mut self) -> Result<(), DhtError> {
        self.start_self_query(true);

        let mut drain_interval = interval(Duration::from_millis(DRAIN_INTERVAL_MS));
        let mut tick_interval = interval(Duration::from_millis(TICK_INTERVAL_MS));

        loop {
            if self.destroyed {
                break;
            }

            let timeout_at = self.io.next_timeout_deadline();

            tokio::select! {
                biased;

                Some(event) = self.io.recv() => {
                    // Requests leave from the listening (server) socket
                    // unless the node is firewalled.
                    let on_listen_socket = !self.config.firewalled;
                    self.handle_io_event(event, on_listen_socket);
                },

                _ = drain_interval.tick() => {
                    self.io.drain();
                },

                _ = tokio::time::sleep_until(timeout_at) => {
                    let timeouts = self.io.check_timeouts();
                    for evt in timeouts {
                        self.handle_timeout(evt);
                    }
                },

                _ = tick_interval.tick() => {
                    self.handle_tick();
                },

                Some(reply) = self.deferred_reply_rx.recv() => {
                    self.handle_deferred_reply(reply);
                },

                Some((raw, addr)) = self.bootstrap_resolved_rx.recv() => {
                    self.on_bootstrap_resolved(raw, addr);
                },

                Some(cmd) = self.cmd_rx.recv() => {
                    if self.handle_command(cmd) {
                        break;
                    }
                },
            }
        }

        self.io.destroy().await?;
        Ok(())
    }

    fn bootstrap_nodes(&self) -> Vec<(String, u16)> {
        self.config
            .bootstrap
            .iter()
            .filter_map(|s| parse_bootstrap_str(s))
            .collect()
    }

    /// Seed a query from the routing table and, if that gives fewer than
    /// `k` nodes, from the bootstrap nodes too, as Node's `Query._open`.
    /// Without the bootstrap nodes, a node whose routing table is empty, as
    /// when no node was reachable at start, would never fill it again.
    ///
    /// A query that may `wait` for the bootstrap hostnames being resolved
    /// does, if it needs them, as Node's awaits them: it asks the nodes it
    /// has at once, the bootstrap nodes as each resolves, and finishes only
    /// once every hostname has resolved or failed. The node's own refreshes
    /// do not wait: each hostname that resolves starts one.
    fn launch_query(&mut self, query_id: u64, mut q: Query, wait: bool) {
        q.add_from_table();
        if q.has_few_pending() {
            q.add_nodes(&self.bootstrap_nodes());
            self.resolve_bootstrap_again();
            if wait && self.bootstrap_resolving > 0 {
                q.hold();
                self.held_queries.push(query_id);
            }
        }
        self.start_seeded_query(query_id, q);
    }

    /// Give the held queries `nodes` to ask and, if `release`, let them
    /// finish.
    fn resume_held_queries(&mut self, nodes: &[(String, u16)], release: bool) {
        let ids = if release {
            std::mem::take(&mut self.held_queries)
        } else {
            self.held_queries.clone()
        };
        for query_id in ids {
            let Some(q) = self.active_queries.get_mut(&query_id) else {
                continue;
            };
            let reqs = q.resume(nodes, release);
            self.dispatch_query_requests(query_id, reqs);
            if self
                .active_queries
                .get(&query_id)
                .is_some_and(|q| q.is_finished())
            {
                self.active_queries.remove(&query_id);
                self.on_query_removed(query_id);
            }
        }
    }

    fn start_seeded_query(&mut self, query_id: u64, mut q: Query) {
        let reqs = q.poll_requests();
        self.dispatch_query_requests(query_id, reqs);
        // A query with no node to ask is over at once; kept, it would never
        // be removed.
        if !q.is_finished() {
            self.active_queries.insert(query_id, q);
        }
    }

    /// Resolve again, in the background, the bootstrap hostnames that did
    /// not resolve, for the queries that follow.
    fn resolve_bootstrap_again(&mut self) {
        if self.unresolved_bootstrap.is_empty() || self.bootstrap_resolving > 0 {
            return;
        }
        let now = Instant::now();
        if self
            .last_bootstrap_resolve
            .is_some_and(|at| now.duration_since(at) < BOOTSTRAP_RESOLVE_INTERVAL)
        {
            return;
        }
        self.last_bootstrap_resolve = Some(now);
        // Each hostname apart, so that one slow to fail holds back no other.
        self.bootstrap_resolving = self.unresolved_bootstrap.len();
        for raw in self.unresolved_bootstrap.clone() {
            let tx = self.bootstrap_resolved_tx.clone();
            tokio::spawn(async move {
                let addr = resolve_bootstrap_node(&raw, false).await;
                let _ = tx.send((raw, addr));
            });
        }
    }

    fn on_bootstrap_resolved(&mut self, raw: String, addr: Option<String>) {
        self.bootstrap_resolving = self.bootstrap_resolving.saturating_sub(1);
        let mut added = None;
        if let Some(addr) = addr {
            self.unresolved_bootstrap.retain(|s| *s != raw);
            if !self.config.bootstrap.contains(&addr) {
                tracing::info!(%raw, %addr, "bootstrap node resolved");
                added = parse_bootstrap_str(&addr);
                self.config.bootstrap.push(addr);
            }
        }
        // The held queries ask a new node at once, and finish once every
        // hostname is resolved or failed, as Node's `Query._open` awaits
        // them all: the first found may be down. They are released before
        // the refresh below, which may start resolving again.
        let new_nodes: Vec<(String, u16)> = added.clone().into_iter().collect();
        let release = self.bootstrap_resolving == 0;
        if release {
            // The next batch waits from when this one ends, so that slow
            // ones don't run back to back.
            self.last_bootstrap_resolve = Some(Instant::now());
        }
        if release || !new_nodes.is_empty() {
            self.resume_held_queries(&new_nodes, release);
        }
        let added = added.is_some();
        // Ask a new one now, rather than at the next refresh, if the table
        // still needs it.
        let short = self.table.lock().map(|t| t.len() < K).unwrap_or(false);
        if added && self.bootstrapped && short {
            self.run_refresh();
        }
    }

    /// Starts a `FIND_NODE` query for this node's own id through the routing
    /// table and the bootstrap nodes. The bootstrap query marks the node
    /// bootstrapped when it ends; a later one tells those nodes a new id.
    fn start_self_query(&mut self, is_bootstrap: bool) {
        let Some(own_id) = self.table.lock().ok().map(|t| *t.id()) else {
            return;
        };
        let bootstrap_nodes = self.bootstrap_nodes();

        let (result_tx, _result_rx) = oneshot::channel::<QueryResult>();
        let query_id = self.next_query_id;
        self.next_query_id += 1;
        if is_bootstrap {
            self.bootstrap_query_id = Some(query_id);
        }

        let mut q = Query::new(
            query_id,
            own_id,
            true,
            CMD_FIND_NODE,
            None,
            self.config.concurrency,
            false,
            result_tx,
            Arc::clone(&self.table),
            own_id,
        );

        q.add_from_table();
        q.add_nodes(&bootstrap_nodes);

        let requests = q.poll_requests();
        self.dispatch_query_requests(query_id, requests);

        if q.is_finished() {
            self.on_query_removed(query_id);
        } else {
            self.active_queries.insert(query_id, q);
        }

        tracing::debug!(query_id, is_bootstrap, "self query started");
    }

    /// `on_listen_socket`: a response arrived on the socket other nodes reach
    /// this node on, so its `to` is the address this node's id derives from.
    fn handle_io_event(&mut self, event: IoEvent, on_listen_socket: bool) {
        match event {
            IoEvent::IncomingRequest(req) => {
                self.add_node_from_network(req.from.clone(), req.id);
                self.handle_incoming_request(req);
            }
            IoEvent::Response {
                tid,
                from,
                id,
                token,
                closer_nodes,
                error,
                value,
                rtt,
                request,
                to,
            } => {
                self.add_node_from_network(from.clone(), id);

                if to.port != 0 && !to.host.is_empty() {
                    if self.needs_id_update
                        && on_listen_socket
                        && from == request.to
                        && to.host.parse::<Ipv4Addr>().is_ok_and(|ip| !ip.is_unspecified())
                    {
                        self.addr_samples.push(to.clone());
                        // During bootstrap the samples are tallied when it
                        // finishes; after that, adopt the first one.
                        if self.bootstrapped && self.try_update_id_from_samples() {
                            self.start_self_query(false);
                        }
                    }
                    self.nat.add(&to, &from);
                }

                let data = IoResponseData {
                    from: from.clone(),
                    from_id: id,
                    token,
                    closer_nodes: closer_nodes.clone(),
                    error,
                    value: value.clone(),
                    rtt,
                    to: to.clone(),
                };

                if let Some((query_id, is_commit)) = self.tid_to_query.remove(&tid) {
                    if is_commit {
                        let finished = self
                            .active_queries
                            .get_mut(&query_id)
                            .map(|q| q.on_commit_done())
                            .unwrap_or(true);
                        if finished {
                            self.active_queries.remove(&query_id);
                            self.on_query_removed(query_id);
                        }
                    } else {
                        let reqs = self
                            .active_queries
                            .get_mut(&query_id)
                            .map(|q| q.on_response(data))
                            .unwrap_or_default();
                        self.dispatch_query_requests(query_id, reqs);
                        if self
                            .active_queries
                            .get(&query_id)
                            .map(|q| q.is_finished())
                            .unwrap_or(false)
                        {
                            self.active_queries.remove(&query_id);
                            self.on_query_removed(query_id);
                        }
                    }
                } else if let Some(standalone) = self.standalone_tids.remove(&tid) {
                    self.handle_standalone_response(standalone, ResponseData {
                        from,
                        id,
                        token,
                        closer_nodes,
                        error,
                        value,
                        rtt,
                    }, to);
                }
            }
        }
    }

    fn handle_incoming_request(&mut self, req: crate::io::IncomingRequest) {
        if req.internal {
            match req.command {
                CMD_PING => {
                    self.io.send_reply(&req, 0, None);
                }
                CMD_DELAYED_PING => {
                    self.handle_delayed_ping(req);
                }
                CMD_PING_NAT => {
                    if let Some(ref val) = req.value {
                        if val.len() >= 2 {
                            let port = u16::from_le_bytes([val[0], val[1]]);
                            if port != 0 {
                                self.io.send_reply(&req, 0, None);
                            }
                        }
                    }
                }
                CMD_FIND_NODE => {
                    if req.target.is_some() {
                        self.io.send_reply(&req, 0, None);
                    }
                }
                CMD_DOWN_HINT => {
                    self.handle_down_hint_request(&req);
                    self.io.send_reply(&req, 0, None);
                }
                _ => {
                    let has_target = req.target.is_some();
                    let _ = has_target;
                    self.io.send_reply(&req, ERR_UNKNOWN_COMMAND, None);
                }
            }
        } else {
            self.forward_user_request(req);
        }
    }

    fn handle_delayed_ping(&mut self, req: crate::io::IncomingRequest) {
        let delay_ms = match &req.value {
            Some(v) if v.len() >= 4 => {
                u32::from_le_bytes([v[0], v[1], v[2], v[3]])
            }
            _ => return,
        };

        if delay_ms > 10_000 {
            self.io.send_reply(&req, ERR_UNKNOWN_COMMAND, None);
            return;
        }

        let duration = Duration::from_millis(delay_ms as u64);
        let tx = self.deferred_reply_tx.clone();
        let reply = DeferredReply {
            from: req.from,
            reply_ctx: req.reply_ctx,
            tid: req.tid,
            target: req.target,
            error: 0,
            value: None,
        };
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            let _ = tx.send(reply);
        });
    }

    fn handle_down_hint_request(&mut self, req: &crate::io::IncomingRequest) {
        let val = match &req.value {
            Some(v) if v.len() >= 6 => v.clone(),
            _ => return,
        };

        let ip_bytes: [u8; 4] = [val[0], val[1], val[2], val[3]];
        let port = u16::from_le_bytes([val[4], val[5]]);
        let host = Ipv4Addr::from(ip_bytes).to_string();
        let node_id = peer_id(&host, port);

        let (found_id, found_host, found_port, seen_tick, pinged_tick) = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            if let Some(node) = table.get(&node_id) {
                (
                    node.id,
                    node.host.clone(),
                    node.port,
                    node.seen_tick,
                    node.pinged_tick,
                )
            } else {
                return;
            }
        };

        if pinged_tick < self.tick {
            if let Ok(mut table) = self.table.lock() {
                if let Some(node) = table.get_mut(&found_id) {
                    node.down_hints += 1;
                    node.pinged_tick = self.tick;
                }
            }

            let params = RequestParams {
                to: Ipv4Peer {
                    host: found_host,
                    port: found_port,
                },
                token: None,
                internal: true,
                command: CMD_PING,
                target: None,
                value: None,
                timeout_ms: None,
                retries: None,
            };

            if let Some(tid) = self.io.create_request(params) {
                self.standalone_tids.insert(
                    tid,
                    StandaloneRequest::Check {
                        node_id: found_id,
                        last_seen_tick: seen_tick,
                    },
                );
            }
        }
    }

    fn forward_user_request(&mut self, req: crate::io::IncomingRequest) {
        let (reply_tx, reply_rx) = oneshot::channel::<(u64, Option<Vec<u8>>)>();

        let user_req = UserRequest {
            from: req.from.clone(),
            id: req.id,
            tid: req.tid,
            token: req.token,
            command: req.command,
            target: req.target,
            value: req.value.clone(),
            reply_tx: Some(reply_tx),
        };

        let mut maybe_req = Some(user_req);
        self.request_subscribers.retain(|tx| {
            if let Some(ur) = maybe_req.take() {
                match tx.send(ur) {
                    Ok(()) => true,
                    Err(e) => {
                        maybe_req = Some(e.0);
                        false
                    }
                }
            } else {
                !tx.is_closed()
            }
        });

        if maybe_req.is_some() {
            self.io.send_reply(&req, ERR_UNKNOWN_COMMAND, None);
        } else {
            let deferred_tx = self.deferred_reply_tx.clone();
            let from = req.from.clone();
            let reply_ctx = req.reply_ctx;
            let tid = req.tid;
            let target = req.target;
            tokio::spawn(async move {
                let outcome = match reply_rx.await {
                    Ok((SUPPRESS_REPLY_SENTINEL, _)) => None,
                    Ok((error, value)) => Some((error, value)),
                    Err(_) => Some((ERR_UNKNOWN_COMMAND, None)),
                };
                if let Some((error, value)) = outcome {
                    let _ = deferred_tx.send(DeferredReply {
                        from,
                        reply_ctx,
                        tid,
                        target,
                        error,
                        value,
                    });
                }
            });
        }
    }

    fn handle_standalone_response(
        &mut self,
        standalone: StandaloneRequest,
        resp: ResponseData,
        reflexive_addr: Ipv4Peer,
    ) {
        match standalone {
            StandaloneRequest::Ping(reply_tx) => {
                let to = if reflexive_addr.port != 0 && !reflexive_addr.host.is_empty() {
                    Some(reflexive_addr)
                } else {
                    None
                };
                let _ = reply_tx.send(Ok(PingResponse {
                    from: resp.from,
                    id: resp.id,
                    rtt: resp.rtt,
                    to,
                    closer_nodes: resp.closer_nodes,
                }));
            }
            StandaloneRequest::UserRequest(reply_tx) => {
                let _ = reply_tx.send(Ok(resp));
            }
            StandaloneRequest::Reping {
                new_node,
                old_node_id,
                last_seen_tick,
            } => {
                self.repinging = self.repinging.saturating_sub(1);
                let stale = {
                    let table = self.table.lock().ok();
                    table
                        .and_then(|t| t.get(&old_node_id).map(|n| n.seen_tick <= last_seen_tick))
                        .unwrap_or(true)
                };
                if stale {
                    if let Ok(mut table) = self.table.lock() {
                        table.remove(&old_node_id);
                        table.add(new_node);
                    }
                }
            }
            StandaloneRequest::Check {
                node_id,
                last_seen_tick,
            } => {
                let stale = {
                    let table = self.table.lock().ok();
                    table
                        .and_then(|t| t.get(&node_id).map(|n| n.seen_tick <= last_seen_tick))
                        .unwrap_or(false)
                };
                if stale {
                    if let Ok(mut table) = self.table.lock() {
                        table.remove(&node_id);
                    }
                }
            }
        }
    }

    fn handle_timeout(&mut self, evt: TimeoutEvent) {
        if let Some((query_id, is_commit)) = self.tid_to_query.remove(&evt.tid) {
            if is_commit {
                let finished = self
                    .active_queries
                    .get_mut(&query_id)
                    .map(|q| q.on_commit_done())
                    .unwrap_or(true);
                if finished {
                    self.active_queries.remove(&query_id);
                    self.on_query_removed(query_id);
                }
            } else {
                let reqs = self
                    .active_queries
                    .get_mut(&query_id)
                    .map(|q| q.on_timeout(&evt.to))
                    .unwrap_or_default();
                self.dispatch_query_requests(query_id, reqs);
                if self
                    .active_queries
                    .get(&query_id)
                    .map(|q| q.is_finished())
                    .unwrap_or(false)
                {
                    self.active_queries.remove(&query_id);
                    self.on_query_removed(query_id);
                }
            }
        } else if let Some(standalone) = self.standalone_tids.remove(&evt.tid) {
            self.handle_standalone_timeout(standalone);
        }
    }

    fn handle_standalone_timeout(&mut self, standalone: StandaloneRequest) {
        match standalone {
            StandaloneRequest::Ping(reply_tx) => {
                let _ = reply_tx.send(Err(DhtError::RequestFailed("timeout".into())));
            }
            StandaloneRequest::UserRequest(reply_tx) => {
                let _ = reply_tx.send(Err(DhtError::RequestFailed("timeout".into())));
            }
            StandaloneRequest::Reping {
                new_node,
                old_node_id,
                ..
            } => {
                self.repinging = self.repinging.saturating_sub(1);
                if let Ok(mut table) = self.table.lock() {
                    table.remove(&old_node_id);
                    table.add(new_node);
                }
            }
            StandaloneRequest::Check { node_id, .. } => {
                if let Ok(mut table) = self.table.lock() {
                    table.remove(&node_id);
                }
            }
        }
    }

    fn handle_tick(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_tick_time).as_millis() as u64;

        if elapsed > SLEEPING_INTERVAL_MS {
            self.tick += 2 * OLD_NODE;
            self.tick += 8 - (self.tick & 7);
            self.refresh_ticks = 1;
        } else {
            self.tick += 1;
        }

        self.last_tick_time = now;
        self.down_hints_per_tick = 0;

        if !self.bootstrapped {
            return;
        }

        if (self.tick & 7) == 0 {
            self.ping_some();
        }

        self.refresh_ticks = self.refresh_ticks.saturating_sub(1);
        if self.refresh_ticks == 0 {
            self.refresh_ticks = REFRESH_TICKS;
            self.run_refresh();
        }
    }

    fn ping_some(&mut self) {
        let cnt = if !self.standalone_tids.is_empty() {
            3usize
        } else {
            5
        };

        let nodes: Vec<(NodeId, String, u16, u64)> = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            if table.is_empty() {
                drop(table);
                self.run_refresh();
                return;
            }
            let all = table.closest(table.id(), table.len().min(50));
            let mut v: Vec<_> = all
                .iter()
                .filter(|n| n.pinged_tick < self.tick)
                .map(|n| (n.id, n.host.clone(), n.port, n.seen_tick))
                .collect();
            v.sort_by_key(|(_, _, _, seen)| *seen);
            v.truncate(cnt);
            v
        };

        for (node_id, host, port, last_seen) in nodes {
            if let Ok(mut table) = self.table.lock() {
                if let Some(node) = table.get_mut(&node_id) {
                    node.pinged_tick = self.tick;
                }
            }
            let params = RequestParams {
                to: Ipv4Peer { host, port },
                token: None,
                internal: true,
                command: CMD_PING,
                target: None,
                value: None,
                timeout_ms: None,
                retries: None,
            };
            if let Some(tid) = self.io.create_request(params) {
                self.standalone_tids.insert(
                    tid,
                    StandaloneRequest::Check {
                        node_id,
                        last_seen_tick: last_seen,
                    },
                );
            }
        }
    }

    fn run_refresh(&mut self) {
        self.refresh_ticks = REFRESH_TICKS;

        let target = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            table.random().map(|n| n.id).unwrap_or_else(|| *table.id())
        };

        let own_id = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            *table.id()
        };

        let (result_tx, result_rx) = oneshot::channel::<QueryResult>();
        let query_id = self.next_query_id;
        self.next_query_id += 1;

        let concurrency = (self.config.concurrency / 8).max(2);

        let q = Query::new(
            query_id,
            target,
            true,
            CMD_FIND_NODE,
            None,
            concurrency,
            false,
            result_tx,
            Arc::clone(&self.table),
            own_id,
        );
        self.launch_query(query_id, q, false);

        tokio::spawn(async move {
            let _ = result_rx.await;
        });
    }

    fn handle_command(&mut self, cmd: DhtCommand) -> bool {
        match cmd {
            DhtCommand::Bootstrapped { reply_tx } => {
                if self.bootstrapped {
                    let _ = reply_tx.send(Ok(()));
                } else {
                    self.bootstrap_waiters.push(reply_tx);
                }
            }

            DhtCommand::Ping {
                host,
                port,
                reply_tx,
            } => {
                let target = self.table.lock().ok().map(|t| *t.id());
                let params = RequestParams {
                    to: Ipv4Peer { host, port },
                    token: None,
                    internal: true,
                    command: CMD_FIND_NODE,
                    target,
                    value: None,
                    timeout_ms: None,
                    retries: None,
                };
                if let Some(tid) = self.io.create_request(params) {
                    self.standalone_tids
                        .insert(tid, StandaloneRequest::Ping(reply_tx));
                } else {
                    let _ = reply_tx.send(Err(DhtError::Destroyed));
                }
            }

            DhtCommand::FindNode { target, reply_tx } => {
                self.start_query(target, true, CMD_FIND_NODE, None, false, None, reply_tx);
            }

            DhtCommand::Query { params, reply_tx } => {
                let concurrency = params.concurrency.unwrap_or(self.config.concurrency);
                self.start_query(
                    params.target,
                    false,
                    params.command,
                    params.value,
                    params.commit,
                    Some(concurrency),
                    reply_tx,
                );
            }

            DhtCommand::Request {
                params,
                host,
                port,
                reply_tx,
            } => {
                let rparams = RequestParams {
                    to: Ipv4Peer { host, port },
                    token: params.token,
                    internal: false,
                    command: params.command,
                    target: params.target,
                    value: params.value,
                    timeout_ms: params.timeout_ms,
                    retries: params.retries,
                };
                if let Some(tid) = self.io.create_request(rparams) {
                    self.standalone_tids
                        .insert(tid, StandaloneRequest::UserRequest(reply_tx));
                } else {
                    let _ = reply_tx.send(Err(DhtError::Destroyed));
                }
            }

            DhtCommand::Relay {
                command,
                target,
                value,
                to,
                preserve_tid,
            } => {
                self.io.relay(command, target, value, &to, preserve_tid);
            }

            DhtCommand::SendReplyTo {
                tid,
                target,
                to,
                value,
            } => {
                self.io.reply_to(tid, target, &to, value);
            }

            DhtCommand::SubscribeRequests { reply_tx } => {
                let (tx, rx) = mpsc::unbounded_channel();
                self.request_subscribers.push(tx);
                let _ = reply_tx.send(rx);
            }

            DhtCommand::TableSize { reply_tx } => {
                let size = self
                    .table
                    .lock()
                    .map(|t| t.len())
                    .unwrap_or(0);
                let _ = reply_tx.send(size);
            }

            DhtCommand::Destroy { reply_tx } => {
                self.destroyed = true;
                let _ = reply_tx.send(Ok(()));
                return true;
            }

            DhtCommand::LocalPort { reply_tx } => {
                let _ = reply_tx.send(Ok(self.local_port));
            }

            DhtCommand::TableId { reply_tx } => {
                let id = self.table.lock().ok().map(|t| *t.id());
                let _ = reply_tx.send(id);
            }

            DhtCommand::ServerSocket { reply_tx } => {
                let socket = Some(self.io.primary_socket());
                let _ = reply_tx.send(socket);
            }

            DhtCommand::ListenSocket { reply_tx } => {
                let socket = Some(self.io.server_socket());
                let _ = reply_tx.send(socket);
            }

            DhtCommand::RemoteAddress { reply_tx } => {
                let addr = self
                    .nat
                    .addresses
                    .as_ref()
                    .and_then(|addrs| addrs.first().cloned())
                    .or_else(|| self.public_addr.clone());
                let firewall = self.nat.firewall;
                let _ = reply_tx.send((addr, firewall));
            }
            DhtCommand::InboundReplyBytes { addr, data } => {
                if let Some(event) = self.io.handle_inbound_reply_bytes(addr, data) {
                    self.handle_io_event(event, false);
                }
            }
            DhtCommand::PingViaSocket {
                target,
                socket,
                reply_tx,
            } => {
                let id_target = self.table.lock().ok().map(|t| *t.id());
                let params = RequestParams {
                    to: target,
                    token: None,
                    internal: true,
                    command: CMD_FIND_NODE,
                    target: id_target,
                    value: None,
                    timeout_ms: None,
                    retries: None,
                };
                if let Some(tid) = self.io.send_request_via_socket(params, &socket) {
                    self.standalone_tids
                        .insert(tid, StandaloneRequest::Ping(reply_tx));
                } else {
                    let _ = reply_tx.send(Err(DhtError::Destroyed));
                }
            }
        }
        false
    }

    #[allow(clippy::too_many_arguments)]
    fn start_query(
        &mut self,
        target: NodeId,
        internal: bool,
        command: u64,
        value: Option<Vec<u8>>,
        commit: bool,
        concurrency: Option<usize>,
        reply_tx: oneshot::Sender<Result<Vec<QueryReply>, DhtError>>,
    ) {
        let own_id = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => {
                    let _ = reply_tx.send(Err(DhtError::ChannelClosed));
                    return;
                }
            };
            *table.id()
        };

        let concurrency = concurrency.unwrap_or(self.config.concurrency);
        let (result_tx, result_rx) = oneshot::channel::<QueryResult>();
        let query_id = self.next_query_id;
        self.next_query_id += 1;

        let q = Query::new(
            query_id,
            target,
            internal,
            command,
            value,
            concurrency,
            commit,
            result_tx,
            Arc::clone(&self.table),
            own_id,
        );

        self.launch_query(query_id, q, true);

        tokio::spawn(async move {
            match result_rx.await {
                Ok(result) => {
                    let _ = reply_tx.send(Ok(result.closest_replies));
                }
                Err(_) => {
                    let _ = reply_tx.send(Err(DhtError::ChannelClosed));
                }
            }
        });
    }

    fn dispatch_query_requests(&mut self, query_id: u64, requests: Vec<QueryRequest>) {
        for req in requests {
            match req {
                QueryRequest::Query(params) => {
                    if let Some(tid) = self.io.create_request(params) {
                        self.tid_to_query.insert(tid, (query_id, false));
                    }
                }
                QueryRequest::Commit(params) => {
                    if let Some(tid) = self.io.create_request(params) {
                        self.tid_to_query.insert(tid, (query_id, true));
                    }
                }
                QueryRequest::DownHint(params) => {
                    if self.down_hints_per_tick < DOWN_HINTS_RATE_LIMIT {
                        self.down_hints_per_tick += 1;
                        let _ = self.io.create_request(params);
                    }
                }
            }
        }
    }

    fn add_node_from_network(&mut self, from: Ipv4Peer, from_id: Option<NodeId>) {
        let id = match from_id {
            Some(id) => id,
            None => return,
        };

        let own_id = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            *table.id()
        };

        if id == own_id {
            return;
        }

        {
            let mut table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            if let Some(node) = table.get_mut(&id) {
                node.seen_tick = self.tick;
                node.pinged_tick = self.tick;
                return;
            }
        }

        let new_node = Node {
            id,
            host: from.host,
            port: from.port,
            token: None,
            added_tick: self.tick,
            seen_tick: self.tick,
            pinged_tick: self.tick,
            down_hints: 0,
        };

        let added = {
            let mut table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            table.add(new_node.clone())
        };

        if !added {
            self.handle_bucket_full(new_node);
        } else if !self.held_queries.is_empty() {
            // A query held for the bootstrap hostnames can ask a node that
            // turned up meanwhile, rather than wait for them.
            self.resume_held_queries(&[(new_node.host, new_node.port)], false);
        }
    }

    fn handle_bucket_full(&mut self, new_node: Node) {
        if !self.bootstrapped || self.repinging >= MAX_REPINGING {
            return;
        }

        let events: Vec<TableEvent> = {
            let mut table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            table.drain_events()
        };

        for evt in events {
            let TableEvent::BucketFull {
                new_node: evt_new_node,
                bucket_index: _,
            } = evt;
            {
                let oldest = {
                    let table = match self.table.lock() {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    let close = table.closest(&evt_new_node.id, 20);
                    close
                        .iter()
                        .filter(|n| n.pinged_tick < self.tick)
                        .min_by(|a, b| {
                            a.pinged_tick
                                .cmp(&b.pinged_tick)
                                .then(a.added_tick.cmp(&b.added_tick))
                        })
                        .map(|n| (n.id, n.host.clone(), n.port, n.seen_tick))
                };

                if let Some((old_id, old_host, old_port, last_seen)) = oldest {
                    if self.tick - last_seen < RECENT_NODE
                        && self.tick.saturating_sub(
                            self.table
                                .lock()
                                .ok()
                                .and_then(|t| t.get(&old_id).map(|n| n.added_tick))
                                .unwrap_or(0),
                        ) > OLD_NODE
                    {
                        return;
                    }

                    if let Ok(mut table) = self.table.lock() {
                        if let Some(node) = table.get_mut(&old_id) {
                            node.pinged_tick = self.tick;
                        }
                    }

                    let params = RequestParams {
                        to: Ipv4Peer {
                            host: old_host,
                            port: old_port,
                        },
                        token: None,
                        internal: true,
                        command: CMD_PING,
                        target: None,
                        value: None,
                        timeout_ms: None,
                        retries: None,
                    };
                    if let Some(tid) = self.io.create_request(params) {
                        self.repinging += 1;
                        self.standalone_tids.insert(
                            tid,
                            StandaloneRequest::Reping {
                                new_node: evt_new_node,
                                old_node_id: old_id,
                                last_seen_tick: last_seen,
                            },
                        );
                    }
                }
            }
        }

        let _ = new_node;
    }

    fn on_query_removed(&mut self, query_id: u64) {
        if self.bootstrap_query_id == Some(query_id) {
            self.bootstrap_query_id = None;
            self.mark_bootstrapped();
        }
    }

    fn handle_deferred_reply(&mut self, reply: DeferredReply) {
        self.io.send_reply_deferred(
            &reply.from,
            reply.reply_ctx,
            reply.tid,
            reply.target,
            reply.error,
            reply.value.as_deref(),
        );
    }

    fn mark_bootstrapped(&mut self) {
        if self.bootstrapped {
            return;
        }

        if self.needs_id_update {
            if self.try_update_id_from_samples() {
                // Nodes queried so far saw this node without an id and didn't
                // keep it: query again with the new one before finishing, as
                // Node's `_bootstrap` does after `_updateNetworkState`.
                self.start_self_query(true);
                return;
            }
            tracing::warn!(
                bootstrap_nodes = self.config.bootstrap.len(),
                firewalled = self.config.firewalled,
                "persistent DHT node bound to a wildcard host could not learn its public \
                 address from bootstrap replies; it runs as ephemeral (other nodes won't add \
                 it to their routing tables) until replies to its own requests report one, \
                 which never happens for a firewalled node. The first node of a DHT must be \
                 given its public IPv4 address: set DhtConfig::public_host or bind host to \
                 that address"
            );
        }

        self.bootstrapped = true;
        tracing::info!(
            target: "peeroxide::_events::dht::bootstrapped",
            "DHT node bootstrapped"
        );
        for tx in self.bootstrap_waiters.drain(..) {
            let _ = tx.send(Ok(()));
        }
    }

    /// Derives the node id from the address samples collected so far and
    /// switches the node to persistent. Returns `false` if there are none.
    fn try_update_id_from_samples(&mut self) -> bool {
        let Some(addr) = self.determine_address_from_samples() else {
            return false;
        };
        let new_id = peer_id(&addr.host, addr.port);
        if let Ok(mut table) = self.table.lock() {
            table.rebuild_with_id(new_id);
        }
        self.needs_id_update = false;
        self.addr_samples.clear();
        self.io.ephemeral = false;
        if !self.config.firewalled && addr.port == self.local_port && self.public_addr.is_none() {
            self.public_addr = Some(addr.clone());
        }
        tracing::debug!(host = %addr.host, port = addr.port, "updated node ID from NAT samples");
        true
    }

    fn determine_address_from_samples(&self) -> Option<Ipv4Peer> {
        if self.addr_samples.is_empty() {
            return None;
        }

        let mut counts: HashMap<String, usize> = HashMap::new();
        for sample in &self.addr_samples {
            let key = format!("{}:{}", sample.host, sample.port);
            *counts.entry(key).or_default() += 1;
        }

        let best = counts.into_iter().max_by_key(|(_, count)| *count)?;
        let (host_port, _) = best;
        let parts: Vec<&str> = host_port.rsplitn(2, ':').collect();
        if parts.len() == 2 {
            let port: u16 = parts[0].parse().ok()?;
            let host = parts[1].to_string();
            Some(Ipv4Peer { host, port })
        } else {
            None
        }
    }
}

// ── Spawn ─────────────────────────────────────────────────────────────────────

/// Spawns a DHT node and returns its join handle and public handle.
pub async fn spawn(
    runtime: &UdxRuntime,
    config: DhtConfig,
) -> Result<(tokio::task::JoinHandle<Result<(), DhtError>>, DhtHandle), DhtError> {
    let public_host = match config.public_host.as_deref() {
        None => None,
        Some(h) => match h.parse::<Ipv4Addr>() {
            Ok(ip) if !ip.is_unspecified() => Some(ip.to_string()),
            _ => {
                return Err(DhtError::InvalidConfig(format!(
                    "public_host must be an IPv4 address other than 0.0.0.0, got {h:?}"
                )))
            }
        },
    };

    let table_id: NodeId = rand::random();
    let table = Arc::new(Mutex::new(RoutingTable::new(table_id)));
    let table_for_handle = Arc::clone(&table);

    let ephemeral = config.ephemeral.unwrap_or(!config.bootstrap.is_empty());
    let is_wildcard = config.host == "0.0.0.0" || config.host == "::";
    let id_host = match (ephemeral, &public_host) {
        (true, _) => None,
        (false, Some(h)) => Some(h.clone()),
        (false, None) if !is_wildcard => Some(config.host.clone()),
        (false, None) => None,
    };
    // A persistent node that doesn't know its address yet stays ephemeral on
    // the wire: other nodes would reject its random id anyway.
    let needs_id_update = !ephemeral && id_host.is_none();

    let io_config = IoConfig {
        max_window: config.max_window,
        port: config.port,
        host: config.host.clone(),
        firewalled: config.firewalled,
        ephemeral: ephemeral || needs_id_update,
    };

    let io = Io::bind(runtime, Arc::clone(&table), io_config).await?;
    let local_port = io.server_local_addr().await
        .map(|a| a.port())
        .unwrap_or(config.port);

    if let Some(host) = &id_host {
        let deterministic_id = peer_id(host, local_port);
        if let Ok(mut t) = table.lock() {
            t.rebuild_with_id(deterministic_id);
        }
    }

    let mut config = config;
    let mut resolved = Vec::with_capacity(config.bootstrap.len());
    let mut unresolved_bootstrap = Vec::new();
    for raw in &config.bootstrap {
        match resolve_bootstrap_node(raw, true).await {
            Some(addr) => resolved.push(addr),
            None => unresolved_bootstrap.push(raw.clone()),
        }
    }
    config.bootstrap = resolved;

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (deferred_reply_tx, deferred_reply_rx) = mpsc::unbounded_channel();
    let (bootstrap_resolved_tx, bootstrap_resolved_rx) = mpsc::unbounded_channel();

    let firewalled = config.firewalled;
    let public_addr = match (ephemeral, firewalled, public_host) {
        (false, false, Some(host)) => Some(Ipv4Peer { host, port: local_port }),
        _ => None,
    };

    let node = DhtNode {
        io,
        table,
        config,
        local_port,
        tick: 0,
        refresh_ticks: REFRESH_TICKS,
        repinging: 0,
        bootstrapped: false,
        bootstrap_waiters: Vec::new(),
        active_queries: HashMap::new(),
        tid_to_query: HashMap::new(),
        standalone_tids: HashMap::new(),
        next_query_id: 0,
        cmd_rx,
        request_subscribers: Vec::new(),
        destroyed: false,
        last_tick_time: Instant::now(),
        down_hints_per_tick: 0,
        bootstrap_query_id: None,
        deferred_reply_tx,
        deferred_reply_rx,
        needs_id_update,
        addr_samples: Vec::new(),
        nat: Nat::new(firewalled),
        public_addr,
        unresolved_bootstrap,
        last_bootstrap_resolve: None,
        bootstrap_resolving: 0,
        held_queries: Vec::new(),
        bootstrap_resolved_tx,
        bootstrap_resolved_rx,
    };

    let wire = node.io.wire_counters();
    let handle = tokio::spawn(node.run());
    let dht_handle = DhtHandle {
        cmd_tx,
        wire,
        table: Arc::clone(&table_for_handle),
    };

    Ok((handle, dht_handle))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dht_config_defaults() {
        let cfg = DhtConfig::default();
        assert_eq!(cfg.port, 0);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.concurrency, 10);
        assert_eq!(cfg.max_window, 80);
        assert!(cfg.firewalled);
        assert!(cfg.bootstrap.is_empty());
        assert!(cfg.ephemeral.is_none());
    }

    #[test]
    fn test_bootstrap_parse_simple() {
        let (host, port) = parse_bootstrap_str("127.0.0.1:10001").expect("parse");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 10001);
    }

    #[test]
    fn test_bootstrap_parse_with_at() {
        let (host, port) =
            parse_bootstrap_str("10.0.0.1@bootstrap.example.com:24242").expect("should parse");
        assert_eq!(host, "10.0.0.1");
        assert_eq!(port, 24242);
    }

    #[test]
    fn test_bootstrap_parse_bad_no_colon() {
        assert!(parse_bootstrap_str("localhost").is_none());
    }

    #[test]
    fn test_bootstrap_parse_bad_port() {
        assert!(parse_bootstrap_str("localhost:notaport").is_none());
    }

    #[test]
    fn test_err_display() {
        let e = DhtError::Destroyed;
        assert!(e.to_string().contains("destroyed"));
    }

    #[test]
    fn test_down_hints_rate_limit_constant() {
        assert_eq!(DOWN_HINTS_RATE_LIMIT, 50);
    }

    #[test]
    fn test_tick_constants() {
        assert_eq!(REFRESH_TICKS, 60);
        assert_eq!(RECENT_NODE, 12);
        assert_eq!(OLD_NODE, 360);
    }

    #[tokio::test]
    async fn remote_address_starts_unsettled() {
        use libudx::UdxRuntime;

        let runtime = UdxRuntime::new().expect("runtime");
        let cfg = DhtConfig {
            bootstrap: vec![],
            ephemeral: Some(true),
            ..DhtConfig::default()
        };
        let (_task, handle) = spawn(&runtime, cfg).await.expect("spawn");

        let addr = handle.remote_address().await.expect("remote_address");
        assert!(
            addr.is_none(),
            "remote_address should be None before any DHT responses arrive"
        );

        let firewall = handle.firewalled().await.expect("firewalled");
        assert_eq!(
            firewall,
            crate::hyperdht_messages::FIREWALL_UNKNOWN,
            "firewalled flag should start as UNKNOWN for a firewalled node"
        );

        let _ = handle.destroy().await;
    }

    #[tokio::test]
    async fn remote_address_unfirewalled_starts_open() {
        use libudx::UdxRuntime;

        let runtime = UdxRuntime::new().expect("runtime");
        let cfg = DhtConfig {
            bootstrap: vec![],
            firewalled: false,
            ephemeral: Some(true),
            ..DhtConfig::default()
        };
        let (_task, handle) = spawn(&runtime, cfg).await.expect("spawn");

        let firewall = handle.firewalled().await.expect("firewalled");
        assert_eq!(
            firewall,
            crate::hyperdht_messages::FIREWALL_OPEN,
            "unfirewalled node should report FIREWALL_OPEN immediately"
        );

        let _ = handle.destroy().await;
    }
}
