//! Iterative DHT query engine.
//!
//! Faithful Rust port of `dht-rpc/lib/query.js`.
//! A `Query` is a state machine driven by the DHT node event loop.

#![deny(clippy::all)]

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::io::RequestParams;
use crate::messages::{Command, Ipv4Peer};
use crate::peer::{peer_id, NodeId};
use crate::routing_table::{RoutingTable, K};

// ── Constants ─────────────────────────────────────────────────────────────────

const DOWN_HINT_CMD: u64 = Command::DownHint as u64;

// ── Public types ──────────────────────────────────────────────────────────────

/// A node entry in the query's pending list.
#[derive(Debug, Clone)]
pub(crate) struct QueryNode {
    pub id: NodeId,
    pub host: String,
    pub port: u16,
}

/// A reply from a node that made it into the `closest_replies` set.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryReply {
    /// The peer that replied.
    pub from: Ipv4Peer,
    /// Validated node ID (non-`None` only if the peer proved its address).
    pub from_id: Option<NodeId>,
    /// Round-trip token the peer echoed back (for commit operations).
    pub token: Option<[u8; 32]>,
    /// Closer nodes suggested by the responder.
    pub closer_nodes: Vec<Ipv4Peer>,
    /// Error code (0 = success).
    pub error: u64,
    /// Response value payload.
    pub value: Option<Vec<u8>>,
    /// Measured round-trip time for this reply.
    pub rtt: Duration,
}

/// The final result of a completed query.
#[derive(Debug)]
#[non_exhaustive]
pub struct QueryResult {
    /// Up to K closest nodes found, sorted by XOR distance to target.
    pub closest_replies: Vec<QueryReply>,
    /// Number of successful replies received.
    pub successes: u32,
    /// Number of error / timeout replies.
    pub errors: u32,
}

/// Response data forwarded from the IO layer to the query engine.
#[derive(Debug)]
pub(crate) struct IoResponseData {
    pub from: Ipv4Peer,
    pub from_id: Option<NodeId>,
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Vec<Ipv4Peer>,
    pub error: u64,
    pub value: Option<Vec<u8>>,
    pub rtt: Duration,
}

/// A request produced by the query engine for the [`crate::rpc::DhtNode`] to send.
#[derive(Debug)]
pub(crate) enum QueryRequest {
    /// Phase-1 iterative query request — the DhtNode must map the resulting TID
    /// back to this query so responses are routed here.
    Query(RequestParams),
    /// Phase-2 commit request — route response/timeout completion back via
    /// [`Query::on_commit_done`].
    Commit(RequestParams),
    /// DOWN_HINT fire-and-forget — do **not** map the TID to this query.
    DownHint(RequestParams),
}

// ── Private types ─────────────────────────────────────────────────────────────

#[derive(Debug)]
enum SeenState {
    /// Node is in the pending list; the `Vec` holds referrers.
    Pending(Vec<Ipv4Peer>),
    /// Node responded successfully (or with an application error).
    Done,
    /// Node timed out — considered unreachable.
    Down,
}

// ── Query ─────────────────────────────────────────────────────────────────────

/// Iterative Kademlia query state machine.
///
/// Driven externally by [`crate::rpc::DhtNode`]:
/// - Call [`Query::poll_requests`] after creation (and after adding nodes) to
///   get the initial set of requests to send.
/// - Call [`Query::on_response`] / [`Query::on_timeout`] as IO events arrive.
/// - Call [`Query::on_commit_done`] when a commit-phase TID is resolved.
/// - Check [`Query::is_finished`] after each call; if true, the result has been
///   sent via the oneshot channel and the query may be dropped.
pub(crate) struct Query {
    /// Unique query ID — used by the DhtNode to route TIDs.
    #[allow(dead_code)]
    pub id: u64,
    /// Query target (32-byte hash).
    pub target: NodeId,
    internal: bool,
    command: u64,
    value: Option<Vec<u8>>,
    k: usize,
    /// Concurrency limit (mutable — callers may lower/raise it).
    pub concurrency: usize,
    commit: bool,

    inflight: usize,
    slowdown: bool,
    /// Successful replies received.
    pub successes: u32,
    /// Error / timeout replies received.
    pub errors: u32,
    from_table: bool,
    committing: bool,
    commit_inflight: usize,

    seen: HashMap<String, SeenState>,
    pending: Vec<QueryNode>,
    /// Closest replies sorted by XOR distance; at most K entries.
    pub closest_replies: Vec<QueryReply>,

    finished: bool,
    result_tx: Option<oneshot::Sender<QueryResult>>,
    table: Arc<Mutex<RoutingTable>>,
    own_id: NodeId,
}

impl Query {
    /// Create a new iterative query.
    ///
    /// Pass `retries = u32::MAX` to use the default (5 for queries, 3 for
    /// DOWN_HINT).  The routing table reference is used for the fallback
    /// `add_from_table` path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: u64,
        target: NodeId,
        internal: bool,
        command: u64,
        value: Option<Vec<u8>>,
        concurrency: usize,
        commit: bool,
        result_tx: oneshot::Sender<QueryResult>,
        table: Arc<Mutex<RoutingTable>>,
        own_id: NodeId,
    ) -> Self {
        Self {
            id,
            target,
            internal,
            command,
            value,
            k: K,
            concurrency,
            commit,
            inflight: 0,
            slowdown: false,
            successes: 0,
            errors: 0,
            from_table: false,
            committing: false,
            commit_inflight: 0,
            seen: HashMap::new(),
            pending: Vec::new(),
            closest_replies: Vec::new(),
            finished: false,
            result_tx: Some(result_tx),
            table,
            own_id,
        }
    }

    /// Populate the pending list from the routing table's closest nodes.
    pub(crate) fn add_from_table(&mut self) {
        if self.pending.len() >= self.k {
            return;
        }
        self.from_table = true;
        let need = self.k.saturating_sub(self.pending.len());
        let nodes = {
            let table = match self.table.lock() {
                Ok(t) => t,
                Err(_) => return,
            };
            table
                .closest(&self.target, need)
                .into_iter()
                .map(|n| QueryNode {
                    id: n.id,
                    host: n.host.clone(),
                    port: n.port,
                })
                .collect::<Vec<_>>()
        };
        for node in nodes {
            self.add_pending_impl(node, None);
        }
    }

    /// Add explicit nodes (e.g., bootstrap or user-supplied nodes) to pending.
    pub(crate) fn add_nodes(&mut self, nodes: &[(String, u16)]) {
        for (host, port) in nodes {
            let id = peer_id(host, *port);
            if id == self.own_id {
                continue;
            }
            self.add_pending_impl(QueryNode { id, host: host.clone(), port: *port }, None);
        }
    }

    /// Generate the initial set of requests to send.
    /// Call once after [`Query::add_from_table`] / [`Query::add_nodes`].
    pub(crate) fn poll_requests(&mut self) -> Vec<QueryRequest> {
        self.read_more()
    }

    /// Handle an IO response for a phase-1 TID belonging to this query.
    /// Returns additional requests to send (phase-1 or commit, plus DOWN_HINTs).
    pub(crate) fn on_response(&mut self, data: IoResponseData) -> Vec<QueryRequest> {
        if self.inflight > 0 {
            self.inflight -= 1;
        }

        let addr = format!("{}:{}", data.from.host, data.from.port);
        self.seen.insert(addr, SeenState::Done);

        if self.committing {
            return vec![];
        }

        if data.error == 0 {
            self.successes += 1;
        } else {
            self.errors += 1;
        }

        // Add to closest_replies if the peer proved its ID and it's closer.
        if data.error == 0 {
            if let Some(id) = data.from_id {
                if self.is_closer(&id) {
                    let reply = QueryReply {
                        from: data.from.clone(),
                        from_id: data.from_id,
                        token: data.token,
                        closer_nodes: data.closer_nodes.clone(),
                        error: data.error,
                        value: data.value,
                        rtt: data.rtt,
                    };
                    self.push_closest(reply);
                }
            }
        }

        // Add suggested closer nodes.  Stop when one is not closer (JS break).
        let mut extra: Vec<QueryRequest> = Vec::new();
        for node in &data.closer_nodes {
            let node_id = peer_id(&node.host, node.port);
            if node_id == self.own_id {
                continue;
            }
            let qn = QueryNode {
                id: node_id,
                host: node.host.clone(),
                port: node.port,
            };
            let (should_break, hint) = self.add_pending_returning(qn, Some(data.from.clone()));
            if let Some(h) = hint {
                extra.push(h);
            }
            if should_break {
                break;
            }
        }

        // End initial slowdown once we have enough results.
        if !self.from_table && self.successes + self.errors >= self.concurrency as u32 {
            self.slowdown = false;
        }

        extra.extend(self.read_more());
        extra
    }

    /// Handle a timeout for a phase-1 TID belonging to this query.
    /// Returns DOWN_HINT requests plus next phase-1 requests.
    pub(crate) fn on_timeout(&mut self, to: &Ipv4Peer) -> Vec<QueryRequest> {
        let addr = format!("{}:{}", to.host, to.port);

        // Collect referrers before overwriting the state.
        let refs: Vec<Ipv4Peer> = match self.seen.get(&addr) {
            Some(SeenState::Pending(refs)) => refs.clone(),
            _ => vec![],
        };

        self.seen.insert(addr, SeenState::Down);

        // Build DOWN_HINT requests to each referrer.
        let mut requests: Vec<QueryRequest> = refs
            .iter()
            .filter_map(|referrer| make_down_hint(referrer, to))
            .collect();

        if self.inflight > 0 {
            self.inflight -= 1;
        }
        self.errors += 1;

        if !self.committing {
            requests.extend(self.read_more());
        }

        requests
    }

    /// Called when a commit-phase request completes (response or timeout).
    /// Returns `true` if the query is now fully finished.
    pub(crate) fn on_commit_done(&mut self) -> bool {
        if self.commit_inflight > 0 {
            self.commit_inflight -= 1;
        }
        if self.commit_inflight == 0 {
            self.finish_internal();
            return true;
        }
        false
    }

    /// Returns `true` if this query has completed and the result was sent.
    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Core "read more" driver — mirrors JS `_readMore()`.
    fn read_more(&mut self) -> Vec<QueryRequest> {
        if self.finished || self.committing {
            return vec![];
        }

        // Effective concurrency: slow down to 3 when in slowdown mode.
        let effective_concurrency = if self.slowdown {
            3_usize.min(self.concurrency)
        } else {
            self.concurrency
        };

        let mut requests = Vec::new();

        while self.inflight < effective_concurrency && !self.pending.is_empty() {
            let next = match self.pending.pop() {
                Some(n) => n,
                None => break,
            };
            // Skip nodes that are no longer in the closer set.
            if !self.is_closer(&next.id) {
                continue;
            }
            self.inflight += 1;
            requests.push(QueryRequest::Query(RequestParams {
                to: Ipv4Peer {
                    host: next.host,
                    port: next.port,
                },
                token: None,
                internal: self.internal,
                command: self.command,
                target: Some(self.target),
                value: self.value.clone(),
                timeout_ms: None,
                retries: None,
            }));
        }

        // Start initial slowdown: wait for some results before going broad.
        if !self.from_table && self.successes == 0 && self.errors == 0 {
            self.slowdown = true;
        }

        if !self.pending.is_empty() {
            return requests;
        }

        // All pending drained — check finish condition.
        if self.inflight == 0 {
            // Fallback: if we got very few successes and haven't queried the
            // routing table yet, try populating from it.
            if !self.from_table && self.successes < self.k as u32 / 4 {
                self.add_from_table();
                if !self.pending.is_empty() {
                    requests.extend(self.read_more());
                    return requests;
                }
            }
            let flush_reqs = self.flush();
            requests.extend(flush_reqs);
        }

        requests
    }

    /// Transition to the flush / commit phase.  Mirrors JS `_flush()`.
    fn flush(&mut self) -> Vec<QueryRequest> {
        if self.committing {
            return vec![];
        }
        self.committing = true;

        if !self.commit {
            self.finish_internal();
            return vec![];
        }

        // Phase-2: re-request the closest nodes that have a valid token.
        let commit_params: Vec<RequestParams> = self
            .closest_replies
            .iter()
            .filter_map(|reply| {
                reply.token.map(|token| RequestParams {
                    to: reply.from.clone(),
                    token: Some(token),
                    internal: self.internal,
                    command: self.command,
                    target: Some(self.target),
                    value: self.value.clone(),
                    timeout_ms: None,
                    retries: None,
                })
            })
            .collect();

        self.commit_inflight = commit_params.len();

        if self.commit_inflight == 0 {
            // No closest nodes had tokens — finish immediately.
            self.finish_internal();
            return vec![];
        }

        commit_params
            .into_iter()
            .map(QueryRequest::Commit)
            .collect()
    }

    /// Mark the query as done and deliver the result via the oneshot channel.
    fn finish_internal(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(QueryResult {
                closest_replies: self.closest_replies.clone(),
                successes: self.successes,
                errors: self.errors,
            });
        }
    }

    /// Returns `true` if `id` is closer to `target` than the farthest entry in
    /// `closest_replies`, or if the set is not yet full.
    fn is_closer(&self, id: &NodeId) -> bool {
        if self.closest_replies.len() < self.k {
            return true;
        }
        let last = &self.closest_replies[self.closest_replies.len() - 1];
        match last.from_id {
            Some(last_id) => self.compare(id, &last_id) < 0,
            None => true,
        }
    }

    /// Signed XOR-distance comparison relative to `self.target`.
    ///
    /// Returns a negative number if `a` is closer, positive if `b` is closer,
    /// zero if equidistant.  Matches JS `_compare`.
    pub(crate) fn compare(&self, a: &NodeId, b: &NodeId) -> i32 {
        for i in 0..32 {
            if a[i] == b[i] {
                continue;
            }
            let t = self.target[i];
            return (t ^ a[i]) as i32 - (t ^ b[i]) as i32;
        }
        0
    }

    /// Insertion-sort `reply` into `closest_replies`, cap at K, dedup on
    /// distance==0.  Matches JS `_pushClosest`.
    fn push_closest(&mut self, reply: QueryReply) {
        let m_id = match reply.from_id {
            Some(id) => id,
            None => return,
        };
        self.closest_replies.push(reply);
        let len = self.closest_replies.len();
        let mut i = len as isize - 2;
        while i >= 0 {
            let prev_id = match self.closest_replies[i as usize].from_id {
                Some(id) => id,
                None => break,
            };
            let cmp = self.compare(&prev_id, &m_id);
            if cmp < 0 {
                // prev is closer → m is already in the right position.
                break;
            }
            if cmp == 0 {
                // Duplicate distance — remove the newly appended copy.
                self.closest_replies.remove(i as usize + 1);
                return;
            }
            // prev is farther — bubble m up one more position.
            self.closest_replies.swap(i as usize, i as usize + 1);
            i -= 1;
        }
        if self.closest_replies.len() > self.k {
            self.closest_replies.pop();
        }
    }

    /// Add `node` to `pending`.
    ///
    /// Returns `(should_break, Option<DOWN_HINT request>)`.
    /// `should_break = true` means the node was not closer and the caller
    /// should stop processing more nodes from this response's closer_nodes list.
    fn add_pending_returning(
        &mut self,
        node: QueryNode,
        referrer: Option<Ipv4Peer>,
    ) -> (bool, Option<QueryRequest>) {
        let addr = format!("{}:{}", node.host, node.port);
        let is_closer = self.is_closer(&node.id);

        match self.seen.get(&addr) {
            Some(SeenState::Done) => {
                return (!is_closer, None);
            }
            Some(SeenState::Down) => {
                let hint = if let Some(ref ref_node) = referrer {
                    make_down_hint(
                        ref_node,
                        &Ipv4Peer {
                            host: node.host,
                            port: node.port,
                        },
                    )
                } else {
                    None
                };
                return (!is_closer, hint);
            }
            Some(SeenState::Pending(_)) => {
                if let Some(ref_node) = referrer {
                    if let Some(SeenState::Pending(refs)) = self.seen.get_mut(&addr) {
                        refs.push(ref_node);
                    }
                }
                return (!is_closer, None);
            }
            None => {}
        }

        if !is_closer {
            return (true, None);
        }

        let refs = match referrer {
            Some(r) => vec![r],
            None => vec![],
        };
        self.seen.insert(addr, SeenState::Pending(refs));
        self.pending.push(node);
        (false, None)
    }

    /// Convenience wrapper that discards the `should_break` signal.
    fn add_pending_impl(&mut self, node: QueryNode, referrer: Option<Ipv4Peer>) {
        let _ = self.add_pending_returning(node, referrer);
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Encode an IPv4 peer as 6 bytes (4 IP + 2 port LE) for DOWN_HINT payloads.
fn encode_ipv4_peer_6(peer: &Ipv4Peer) -> Option<Vec<u8>> {
    let addr = Ipv4Addr::from_str(&peer.host).ok()?;
    let mut out = Vec::with_capacity(6);
    out.extend_from_slice(&addr.octets());
    out.extend_from_slice(&peer.port.to_le_bytes());
    Some(out)
}

/// Build a DOWN_HINT `RequestParams` directed at `to`, encoding `down` as value.
fn make_down_hint(to: &Ipv4Peer, down: &Ipv4Peer) -> Option<QueryRequest> {
    let value = encode_ipv4_peer_6(down)?;
    Some(QueryRequest::DownHint(RequestParams {
        to: to.clone(),
        token: None,
        internal: true,
        command: DOWN_HINT_CMD,
        target: None,
        value: Some(value),
        timeout_ms: None,
        retries: None,
    }))
}

pub(crate) fn parse_bootstrap_str(s: &str) -> Option<(String, u16)> {
    let last_colon = s.rfind(':')?;
    let host_part = &s[..last_colon];
    let port: u16 = s[last_colon + 1..].parse().ok()?;
    // Handle "suggestedIP@hostname" format — extract the IP before '@',
    // matching Node.js dht-rpc parseNode() behavior.
    let host = if let Some(at_pos) = host_part.find('@') {
        &host_part[..at_pos]
    } else {
        host_part
    };
    Some((host.to_string(), port))
}

/// Resolve bootstrap strings to `ip:port` form.
///
/// For each entry, tries the suggested IP (before `@`) first. If absent or
/// not a valid IP, falls back to async DNS resolution of the hostname.
pub(crate) async fn resolve_bootstrap_nodes(raw: &[String]) -> Vec<String> {
    let mut resolved = Vec::with_capacity(raw.len());
    for s in raw {
        if let Some((host, port)) = resolve_one_bootstrap(s).await {
            resolved.push(format!("{host}:{port}"));
        }
    }
    resolved
}

async fn resolve_one_bootstrap(s: &str) -> Option<(String, u16)> {
    let last_colon = s.rfind(':')?;
    let host_part = &s[..last_colon];
    let port: u16 = s[last_colon + 1..].parse().ok()?;

    if let Some(at_pos) = host_part.find('@') {
        let ip = &host_part[..at_pos];
        let hostname = &host_part[at_pos + 1..];

        if ip.parse::<std::net::IpAddr>().is_ok() {
            return Some((ip.to_string(), port));
        }

        dns_resolve(hostname, port).await
    } else if host_part.parse::<std::net::IpAddr>().is_ok() {
        Some((host_part.to_string(), port))
    } else {
        dns_resolve(host_part, port).await
    }
}

async fn dns_resolve(hostname: &str, port: u16) -> Option<(String, u16)> {
    match tokio::net::lookup_host(format!("{hostname}:{port}")).await {
        Ok(mut addrs) => {
            let addr = addrs.find(|a| a.is_ipv4()).or_else(|| addrs.next())?;
            tracing::debug!(%hostname, %addr, "resolved bootstrap node via DNS");
            Some((addr.ip().to_string(), addr.port()))
        }
        Err(e) => {
            tracing::warn!(%hostname, err = %e, "DNS resolution failed for bootstrap node");
            None
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn make_table() -> Arc<Mutex<RoutingTable>> {
        Arc::new(Mutex::new(RoutingTable::new([0u8; 32])))
    }

    fn make_query_with_k(target: NodeId, k: usize) -> (Query, oneshot::Receiver<QueryResult>) {
        let (tx, rx) = oneshot::channel();
        let table = make_table();
        let mut q = Query::new(1, target, true, 2, None, 3, false, tx, table, [0u8; 32]);
        q.k = k;
        (q, rx)
    }

    fn make_query(target: NodeId) -> (Query, oneshot::Receiver<QueryResult>) {
        make_query_with_k(target, K)
    }

    fn peer(host: &str, port: u16) -> Ipv4Peer {
        Ipv4Peer {
            host: host.into(),
            port,
        }
    }

    fn reply_with_id(id: NodeId, port: u16) -> QueryReply {
        QueryReply {
            from: peer("127.0.0.1", port),
            from_id: Some(id),
            token: None,
            closer_nodes: vec![],
            error: 0,
            value: None,
            rtt: Duration::from_millis(5),
        }
    }

    // ── compare / XOR distance ────────────────────────────────────────────────

    #[test]
    fn test_compare_identical() {
        let target = [0u8; 32];
        let (q, _) = make_query(target);
        let a = [1u8; 32];
        assert_eq!(q.compare(&a, &a), 0);
    }

    #[test]
    fn test_compare_a_closer() {
        // target = 0x00..., a[0]=0x01 (xor=1), b[0]=0x02 (xor=2) → a closer
        let target = [0u8; 32];
        let (q, _) = make_query(target);
        let mut a = [0u8; 32];
        a[0] = 0x01;
        let mut b = [0u8; 32];
        b[0] = 0x02;
        assert!(q.compare(&a, &b) < 0, "a should be closer than b");
    }

    #[test]
    fn test_compare_b_closer() {
        let target = [0u8; 32];
        let (q, _) = make_query(target);
        let mut a = [0u8; 32];
        a[0] = 0x04;
        let mut b = [0u8; 32];
        b[0] = 0x01;
        assert!(q.compare(&a, &b) > 0, "b should be closer than a");
    }

    #[test]
    fn test_compare_tiebreak_second_byte() {
        // first bytes equal, compare on second byte
        let mut target = [0u8; 32];
        target[0] = 0xFF; // same xor for first byte when a[0]=b[0]=0xFF
        let (q, _) = make_query(target);
        let mut a = [0xFFu8; 32];
        a[1] = 0x01; // xor with target[1]=0 → distance 1
        let mut b = [0xFFu8; 32];
        b[1] = 0x02; // xor → distance 2
        assert!(q.compare(&a, &b) < 0);
    }

    // ── push_closest (insertion sort + cap) ──────────────────────────────────

    #[test]
    fn test_push_closest_sorted_order() {
        let target = [0u8; 32];
        let (mut q, _) = make_query_with_k(target, 20);

        let mut id1 = [0u8; 32];
        id1[0] = 0x01;
        let mut id2 = [0u8; 32];
        id2[0] = 0x02;
        let mut id4 = [0u8; 32];
        id4[0] = 0x04;

        // Insert out of order
        q.push_closest(reply_with_id(id4, 4));
        q.push_closest(reply_with_id(id1, 1));
        q.push_closest(reply_with_id(id2, 2));

        assert_eq!(q.closest_replies.len(), 3);
        assert_eq!(q.closest_replies[0].from_id, Some(id1)); // closest first
        assert_eq!(q.closest_replies[1].from_id, Some(id2));
        assert_eq!(q.closest_replies[2].from_id, Some(id4)); // farthest last
    }

    #[test]
    fn test_push_closest_cap_at_k() {
        let target = [0u8; 32];
        let (mut q, _) = make_query_with_k(target, 3);

        for i in 1u8..=5 {
            let mut id = [0u8; 32];
            id[0] = i * 0x10;
            q.push_closest(reply_with_id(id, i as u16));
        }
        assert_eq!(q.closest_replies.len(), 3, "capped at k=3");
    }

    #[test]
    fn test_push_closest_dedup() {
        let target = [0u8; 32];
        let (mut q, _) = make_query_with_k(target, 20);

        let mut id = [0u8; 32];
        id[0] = 0x10;
        q.push_closest(reply_with_id(id, 1));
        q.push_closest(reply_with_id(id, 2)); // same id → duplicate
        assert_eq!(q.closest_replies.len(), 1, "duplicate should be discarded");
    }

    // ── is_closer ─────────────────────────────────────────────────────────────

    #[test]
    fn test_is_closer_empty_set() {
        let target = [0u8; 32];
        let (q, _) = make_query(target);
        assert!(q.is_closer(&[0xFFu8; 32]), "anything is closer when set is empty");
    }

    #[test]
    fn test_is_closer_full_set() {
        let target = [0u8; 32];
        let (mut q, _) = make_query_with_k(target, 2);

        let mut id1 = [0u8; 32];
        id1[0] = 0x01;
        let mut id4 = [0u8; 32];
        id4[0] = 0x04;
        q.push_closest(reply_with_id(id1, 1));
        q.push_closest(reply_with_id(id4, 2));

        let mut closer = [0u8; 32];
        closer[0] = 0x02; // xor=2 < xor(0x04)=4 → closer than farthest
        assert!(q.is_closer(&closer));

        let mut farther = [0u8; 32];
        farther[0] = 0x08; // xor=8 > 4
        assert!(!q.is_closer(&farther));
    }

    // ── DhtConfig defaults ────────────────────────────────────────────────────

    #[test]
    fn test_query_default_k_is_20() {
        let target = [0u8; 32];
        let (q, _) = make_query(target);
        assert_eq!(q.k, 20);
    }

    // ── add_from_table ────────────────────────────────────────────────────────

    #[test]
    fn test_add_from_table_populates_pending() {
        let target = [0u8; 32];
        let (tx, _rx) = oneshot::channel();
        let mut table = RoutingTable::new([0u8; 32]);

        let mut nid = [0u8; 32];
        nid[0] = 0x42;
        table.add(crate::routing_table::Node {
            id: nid,
            host: "127.0.0.1".into(),
            port: 8888,
            token: None,
            added_tick: 0,
            seen_tick: 0,
            pinged_tick: 0,
            down_hints: 0,
        });

        let table_arc = Arc::new(Mutex::new(table));
        let mut q = Query::new(1, target, true, 2, None, 10, false, tx, table_arc, [0u8; 32]);
        assert!(q.pending.is_empty());

        q.add_from_table();

        assert!(q.from_table, "from_table flag should be set");
        assert!(!q.pending.is_empty(), "pending should have the table node");
    }

    // ── Bootstrap node parsing ────────────────────────────────────────────────

    #[test]
    fn test_bootstrap_parse_simple() {
        let (host, port) = parse_bootstrap_str("192.168.1.1:7777").expect("parse");
        assert_eq!(host, "192.168.1.1");
        assert_eq!(port, 7777);
    }

    #[test]
    fn test_bootstrap_parse_with_suggested_ip() {
        let (host, port) = parse_bootstrap_str("10.0.0.1@bootstrap.example.com:24242")
            .expect("parse with @");
        assert_eq!(port, 24242);
        assert_eq!(host, "10.0.0.1");
    }

    #[test]
    fn test_bootstrap_parse_bad() {
        assert!(parse_bootstrap_str("nocolon").is_none());
    }

    #[tokio::test]
    async fn test_resolve_bootstrap_ip_passthrough() {
        let result = resolve_bootstrap_nodes(&["1.2.3.4:1234".to_string()]).await;
        assert_eq!(result, vec!["1.2.3.4:1234"]);
    }

    #[tokio::test]
    async fn test_resolve_bootstrap_suggested_ip_preferred() {
        let result =
            resolve_bootstrap_nodes(&["10.0.0.1@example.invalid:9999".to_string()]).await;
        assert_eq!(result, vec!["10.0.0.1:9999"]);
    }

    #[tokio::test]
    async fn test_resolve_bootstrap_hostname_only() {
        let result = resolve_bootstrap_nodes(&["localhost:5555".to_string()]).await;
        assert_eq!(result, vec!["127.0.0.1:5555"]);
    }

    #[tokio::test]
    async fn test_resolve_bootstrap_bad_hostname_skipped() {
        let result =
            resolve_bootstrap_nodes(&["doesnotexist.invalid:1234".to_string()]).await;
        assert!(result.is_empty());
    }
}
