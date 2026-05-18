use rand::Rng;

use crate::peer::NodeId;

/// K parameter — max nodes per bucket
pub const K: usize = 20;

/// Number of buckets for 32-byte IDs
const NUM_BUCKETS: usize = 256;

/// A peer in the routing table
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub host: String,
    pub port: u16,
    pub token: Option<Vec<u8>>,
    pub added_tick: u64,
    pub seen_tick: u64,
    pub pinged_tick: u64,
    pub down_hints: u32,
}

/// Events emitted by the routing table
#[derive(Debug)]
pub enum TableEvent {
    /// A bucket reached capacity and rejected a node — ping oldest to decide eviction
    BucketFull {
        new_node: Node,
        bucket_index: usize,
    },
}

/// A single k-bucket, nodes sorted by ID for binary search
struct Bucket {
    nodes: Vec<Node>,
}

impl Bucket {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }
}

/// The Kademlia routing table
pub struct RoutingTable {
    id: NodeId,
    k: usize,
    buckets: Vec<Option<Bucket>>,
    size: usize,
    pending_events: Vec<TableEvent>,
}

impl RoutingTable {
    /// Create a new routing table with the given local node ID
    pub fn new(id: NodeId) -> Self {
        Self::with_k(id, K)
    }

    /// Create with custom k value
    pub fn with_k(id: NodeId, k: usize) -> Self {
        let buckets = (0..NUM_BUCKETS).map(|_| None).collect();
        Self {
            id,
            k,
            buckets,
            size: 0,
            pending_events: Vec::new(),
        }
    }

    /// Get the local node ID
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    /// Get total number of nodes
    pub fn len(&self) -> usize {
        self.size
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Add a node. Returns true if added/replaced, false if bucket full.
    /// If bucket full, pushes a `TableEvent::BucketFull` event.
    pub fn add(&mut self, node: Node) -> bool {
        let idx = self.bucket_index(&node.id);

        if self.buckets[idx].is_none() {
            self.buckets[idx] = Some(Bucket::new());
        }

        // Two-phase access: compute position with an immutable borrow, then
        // mutate in a separate scope so the borrow checker sees no overlap.
        let search = {
            let bucket = match self.buckets[idx].as_ref() {
                Some(b) => b,
                None => return false,
            };
            match bucket.nodes.binary_search_by(|n| n.id.cmp(&node.id)) {
                Ok(pos) => Ok(pos),
                Err(pos) => Err((pos, bucket.nodes.len())),
            }
        };

        match search {
            Ok(pos) => {
                if let Some(bucket) = self.buckets[idx].as_mut() {
                    bucket.nodes[pos] = node;
                }
                true
            }
            Err((pos, len)) if len < self.k => {
                if let Some(bucket) = self.buckets[idx].as_mut() {
                    bucket.nodes.insert(pos, node);
                }
                self.size += 1;
                true
            }
            Err(_) => {
                self.pending_events.push(TableEvent::BucketFull {
                    new_node: node,
                    bucket_index: idx,
                });
                false
            }
        }
    }

    /// Remove a node by ID. Returns the removed node if found.
    pub fn remove(&mut self, id: &NodeId) -> Option<Node> {
        let idx = self.bucket_index(id);

        let pos = {
            let bucket = self.buckets[idx].as_ref()?;
            bucket.nodes.binary_search_by(|n| n.id.cmp(id)).ok()?
        };

        let node = {
            let bucket = self.buckets[idx].as_mut()?;
            bucket.nodes.remove(pos)
        };

        self.size -= 1;
        Some(node)
    }

    /// Check if a node with this ID exists
    pub fn has(&self, id: &NodeId) -> bool {
        self.get(id).is_some()
    }

    /// Get a reference to a node by ID
    pub fn get(&self, id: &NodeId) -> Option<&Node> {
        let idx = self.bucket_index(id);
        let bucket = self.buckets[idx].as_ref()?;
        let pos = bucket.nodes.binary_search_by(|n| n.id.cmp(id)).ok()?;
        Some(&bucket.nodes[pos])
    }

    /// Get a mutable reference to a node by ID
    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        let idx = self.bucket_index(id);

        let pos = {
            let bucket = self.buckets[idx].as_ref()?;
            bucket.nodes.binary_search_by(|n| n.id.cmp(id)).ok()?
        };

        let bucket = self.buckets[idx].as_mut()?;
        Some(&mut bucket.nodes[pos])
    }

    /// Find up to `k` closest nodes to `target`.
    ///
    /// Ported from kademlia-routing-table (Node.js v1.0.6):
    /// 1. Start at the bucket for `target` (index `d`).
    /// 2. Scan downward (d → 0) collecting nodes up to k.
    /// 3. If still under k, scan upward (d+1 → NUM_BUCKETS-1).
    pub fn closest(&self, target: &NodeId, k: usize) -> Vec<&Node> {
        let d = self.bucket_index(target);
        let mut result = Vec::with_capacity(k);

        let mut i = d as isize;
        while i >= 0 {
            if result.len() >= k {
                break;
            }
            if let Some(bucket) = &self.buckets[i as usize] {
                for node in &bucket.nodes {
                    if result.len() >= k {
                        break;
                    }
                    result.push(node);
                }
            }
            i -= 1;
        }

        let mut j = d + 1;
        while j < NUM_BUCKETS {
            if result.len() >= k {
                break;
            }
            if let Some(bucket) = &self.buckets[j] {
                for node in &bucket.nodes {
                    if result.len() >= k {
                        break;
                    }
                    result.push(node);
                }
            }
            j += 1;
        }

        result
    }

    /// Rebuild the table with a new local node ID.
    ///
    /// Extracts all nodes, resets buckets with the new ID, and re-inserts
    /// each node. Nodes that no longer fit (due to bucket capacity under the
    /// new distance metric) are dropped. Mirrors the Node.js `_updateNetworkState`
    /// table rebuild in dht-rpc.
    pub fn rebuild_with_id(&mut self, new_id: NodeId) {
        if self.id == new_id {
            return;
        }
        let nodes: Vec<Node> = self
            .buckets
            .iter_mut()
            .filter_map(|b| b.as_mut())
            .flat_map(|b| b.nodes.drain(..))
            .collect();

        self.id = new_id;
        self.size = 0;
        for bucket in self.buckets.iter_mut() {
            *bucket = None;
        }
        self.pending_events.clear();

        for node in nodes {
            if node.id == new_id {
                continue;
            }
            self.add(node);
        }
    }

    /// Return a random node from the table, or `None` if empty.
    pub fn random(&self) -> Option<&Node> {
        if self.size == 0 {
            return None;
        }

        let occupied: Vec<usize> = self
            .buckets
            .iter()
            .enumerate()
            .filter_map(|(i, b)| match b {
                Some(bucket) if !bucket.nodes.is_empty() => Some(i),
                _ => None,
            })
            .collect();

        if occupied.is_empty() {
            return None;
        }

        let mut rng = rand::rng();
        let bucket_idx = occupied[rng.random_range(0..occupied.len())];
        let bucket = self.buckets[bucket_idx].as_ref()?;
        let node_idx = rng.random_range(0..bucket.nodes.len());
        Some(&bucket.nodes[node_idx])
    }

    /// Return up to `limit` nodes ordered by `seen_tick` descending (most
    /// recently seen first).
    ///
    /// When the table holds 8 or more nodes the 5 most-recently-seen are
    /// skipped; they are likely nodes engaged in current traffic and should
    /// not be re-used as NAT-sample ping targets.  Mirrors the candidate
    /// selection in `lib/nat.js:31-59` of the Node.js reference.
    ///
    /// # STUB
    /// Returns an empty `Vec` until T5 implements the real ordering logic.
    #[allow(dead_code)]
    pub(crate) fn recent(&self, _limit: usize) -> Vec<&Node> {
        // STUB: implemented in T5
        Vec::new()
    }

    /// Drain and return all pending events.
    pub fn drain_events(&mut self) -> Vec<TableEvent> {
        std::mem::take(&mut self.pending_events)
    }

    /// Compute the bucket index for `id` using XOR distance from local ID.
    ///
    /// Ported from kademlia-routing-table JS `_diff()`:
    /// ```js
    /// for (let i = 0; i < id.length; i++) {
    ///   const a = id[i], b = this.id[i];
    ///   if (a !== b) return i * 8 + Math.clz32(a ^ b) - 24;
    /// }
    /// return this.rows.length - 1;
    /// ```
    /// `Math.clz32(byte) - 24` = `u8::leading_zeros()` since clz32 on a byte
    /// always starts with 24 extra leading zeros in the 32-bit representation.
    fn bucket_index(&self, id: &NodeId) -> usize {
        for (i, (&a, &b)) in id.iter().zip(self.id.iter()).enumerate() {
            if a != b {
                return i * 8 + (a ^ b).leading_zeros() as usize;
            }
        }
        NUM_BUCKETS - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: NodeId) -> Node {
        Node {
            id,
            host: "127.0.0.1".to_string(),
            port: 8080,
            token: None,
            added_tick: 0,
            seen_tick: 0,
            pinged_tick: 0,
            down_hints: 0,
        }
    }

    /// Build a NodeId from a local ID of [0;32] such that it falls into
    /// a known bucket. For local_id = [0;32]:
    ///   byte[0] = 0x80 >> shift  →  bucket = shift  (0..=7)
    ///   For i-th byte (i>0): set byte[i] = 0x80 >> shift  →  bucket = i*8 + shift
    fn node_id_for_bucket(bucket: usize) -> NodeId {
        let byte_idx = bucket / 8;
        let bit_shift = bucket % 8;
        let mut id = [0u8; 32];
        id[byte_idx] = 0x80u8 >> bit_shift;
        id
    }

    #[test]
    fn test_new_table() {
        let local: NodeId = [0u8; 32];
        let rt = RoutingTable::new(local);
        assert_eq!(rt.id(), &local);
        assert_eq!(rt.len(), 0);
        assert!(rt.is_empty());
    }

    #[test]
    fn test_add_single_node() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let node_id = node_id_for_bucket(1);
        let node = make_node(node_id);

        assert!(rt.add(node));
        assert_eq!(rt.len(), 1);
        assert!(!rt.is_empty());
        assert!(rt.has(&node_id));
        assert!(rt.get(&node_id).is_some());
        assert_eq!(rt.get(&node_id).unwrap().id, node_id);
    }

    #[test]
    fn test_add_duplicate_replaces() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let node_id = node_id_for_bucket(1);

        let mut node1 = make_node(node_id);
        node1.port = 1111;
        rt.add(node1);

        let mut node2 = make_node(node_id);
        node2.port = 2222;
        let added = rt.add(node2);

        assert!(added);
        assert_eq!(rt.len(), 1);
        assert_eq!(rt.get(&node_id).unwrap().port, 2222);
    }

    #[test]
    fn test_add_fills_bucket() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for i in 0..K {
            let mut id = [0u8; 32];
            id[0] = 0x80;
            id[31] = i as u8;
            assert!(rt.add(make_node(id)));
        }

        assert_eq!(rt.len(), K);
        for i in 0..K {
            let mut id = [0u8; 32];
            id[0] = 0x80;
            id[31] = i as u8;
            assert!(rt.has(&id));
        }
    }

    #[test]
    fn test_bucket_full_event() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for i in 0..K {
            let mut id = [0u8; 32];
            id[0] = 0x80;
            id[31] = i as u8;
            assert!(rt.add(make_node(id)));
        }

        let mut overflow_id = [0u8; 32];
        overflow_id[0] = 0x80;
        overflow_id[31] = K as u8;
        let added = rt.add(make_node(overflow_id));

        assert!(!added);
        assert_eq!(rt.len(), K);

        let events = rt.drain_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            TableEvent::BucketFull {
                new_node,
                bucket_index,
            } => {
                assert_eq!(new_node.id, overflow_id);
                assert_eq!(*bucket_index, 0);
            }
        }

        assert!(rt.drain_events().is_empty());
    }

    #[test]
    fn test_remove_node() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let id = node_id_for_bucket(2);
        rt.add(make_node(id));
        assert_eq!(rt.len(), 1);

        let removed = rt.remove(&id);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, id);
        assert_eq!(rt.len(), 0);
        assert!(!rt.has(&id));

        assert!(rt.remove(&id).is_none());
    }

    #[test]
    fn test_closest_basic() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let id_b0 = node_id_for_bucket(0);
        let id_b1 = node_id_for_bucket(1);
        let id_b2 = node_id_for_bucket(2);

        rt.add(make_node(id_b0));
        rt.add(make_node(id_b1));
        rt.add(make_node(id_b2));

        let result = rt.closest(&id_b0, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, id_b0);
        assert_eq!(result[1].id, id_b1);
        assert_eq!(result[2].id, id_b2);
    }

    #[test]
    fn test_closest_wraps_buckets() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let id_b3 = node_id_for_bucket(3);
        let id_b5 = node_id_for_bucket(5);

        rt.add(make_node(id_b3));
        rt.add(make_node(id_b5));

        let target = node_id_for_bucket(4);

        let result = rt.closest(&target, 10);
        assert_eq!(result.len(), 2);

        let ids: Vec<NodeId> = result.iter().map(|n| n.id).collect();
        assert!(ids.contains(&id_b3));
        assert!(ids.contains(&id_b5));
    }

    #[test]
    fn test_bucket_index_self() {
        let local: NodeId = [0x42u8; 32];
        let rt = RoutingTable::new(local);
        assert_eq!(rt.bucket_index(&local), NUM_BUCKETS - 1);
    }

    #[test]
    fn test_bucket_index_max_distance() {
        let local: NodeId = [0u8; 32];
        let rt = RoutingTable::new(local);

        let mut far_id = [0u8; 32];
        far_id[0] = 0x80;
        assert_eq!(rt.bucket_index(&far_id), 0);
    }

    #[test]
    fn test_random_empty() {
        let rt = RoutingTable::new([0u8; 32]);
        assert!(rt.random().is_none());
    }

    #[test]
    fn test_random_nonempty() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let id = node_id_for_bucket(1);
        rt.add(make_node(id));

        let r = rt.random();
        assert!(r.is_some());
        assert_eq!(r.unwrap().id, id);
    }

    #[test]
    fn test_get_mut() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        let id = node_id_for_bucket(2);
        rt.add(make_node(id));

        if let Some(node) = rt.get_mut(&id) {
            node.port = 9999;
            node.down_hints = 3;
        }

        let node = rt.get(&id).unwrap();
        assert_eq!(node.port, 9999);
        assert_eq!(node.down_hints, 3);
    }

    #[test]
    fn recent_limit_zero_returns_empty() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);
        assert_eq!(rt.recent(0).len(), 0, "empty table, limit=0 => empty");

        for i in 0..5_usize {
            let mut n = make_node(node_id_for_bucket(i));
            n.seen_tick = i as u64;
            rt.add(n);
        }
        assert_eq!(rt.recent(0).len(), 0, "populated table, limit=0 => empty");
    }

    #[test]
    fn recent_orders_by_seen_tick_desc() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for (i, tick) in [1u64, 2, 3].iter().enumerate() {
            let mut n = make_node(node_id_for_bucket(i));
            n.seen_tick = *tick;
            rt.add(n);
        }

        let result = rt.recent(3);
        assert_eq!(result.len(), 3, "expected 3 nodes (stub returns 0 — RED)");
        assert_eq!(result[0].seen_tick, 3, "newest first");
        assert_eq!(result[1].seen_tick, 2);
        assert_eq!(result[2].seen_tick, 1, "oldest last");
    }

    #[test]
    fn recent_skips_5_newest_when_cache_geq_8() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for i in 0..10_usize {
            let mut n = make_node(node_id_for_bucket(i));
            n.seen_tick = i as u64;
            rt.add(n);
        }
        assert_eq!(rt.len(), 10);

        let result = rt.recent(4);
        assert_eq!(result.len(), 4, "expected 4 nodes (stub returns 0 — RED)");
        assert_eq!(result[0].seen_tick, 4, "pos 5 from newest");
        assert_eq!(result[1].seen_tick, 3);
        assert_eq!(result[2].seen_tick, 2);
        assert_eq!(result[3].seen_tick, 1, "pos 8 from newest");
    }

    #[test]
    fn recent_caps_at_limit() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for i in 0..20_usize {
            let mut n = make_node(node_id_for_bucket(i));
            n.seen_tick = i as u64;
            rt.add(n);
        }
        assert_eq!(rt.len(), 20);

        let result = rt.recent(4);
        assert_eq!(result.len(), 4, "recent(4) must cap at 4 (stub returns 0 — RED)");
    }

    #[test]
    fn test_closest_respects_k_limit() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::with_k(local, 5);

        for bucket in 0..10 {
            rt.add(make_node(node_id_for_bucket(bucket)));
        }

        let target = [0u8; 32];
        let result = rt.closest(&target, 3);
        assert!(result.len() <= 3);
    }

    #[test]
    fn test_multiple_buckets() {
        let local: NodeId = [0u8; 32];
        let mut rt = RoutingTable::new(local);

        for bucket in 0..8 {
            rt.add(make_node(node_id_for_bucket(bucket)));
        }

        assert_eq!(rt.len(), 8);

        rt.remove(&node_id_for_bucket(3));
        rt.remove(&node_id_for_bucket(5));
        assert_eq!(rt.len(), 6);
        assert!(!rt.has(&node_id_for_bucket(3)));
        assert!(!rt.has(&node_id_for_bucket(5)));
        assert!(rt.has(&node_id_for_bucket(0)));
    }
}
