//! Persistent storage for DHT records published by server nodes.
//!
//! Provides the `Persistent` handler that backs the four record-storing
//! RPC verbs (`ANNOUNCE`, `UNANNOUNCE`, `MUTABLE_PUT`/`GET`, `IMMUTABLE_PUT`/`GET`)
//! with bounded LRU caches sized via `PersistentConfig`. `PersistentStats`
//! exposes per-cache record counts for operators monitoring a running node.
//!
//! The `PersistentConfig` type is part of the public API for callers that
//! run their own DHT node (e.g. the `peeroxide node` CLI). The handler itself
//! is plumbed inside [`crate::rpc::DhtHandle`] and not constructed directly
//! by typical consumers.

#![allow(missing_docs)]
#![deny(clippy::all)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::compact_encoding::{decode_uint, State};
use crate::crypto::{
    ann_signable, hash, mutable_signable, verify_detached, NS_ANNOUNCE, NS_MUTABLE_PUT,
    NS_UNANNOUNCE,
};
use crate::hyperdht_messages::{
    decode_announce_from_bytes, decode_mutable_get_response_from_bytes,
    decode_mutable_put_request_from_bytes, encode_hyper_peer_to_bytes,
    encode_lookup_raw_reply_to_bytes, encode_mutable_get_response_to_bytes, HyperPeer,
    LookupRawReply, MutableGetResponse,
};
use crate::messages::Ipv4Peer;
use crate::peer::NodeId;

fn to_hex(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        use std::fmt::Write;
        write!(s, "{b:02x}").ok();
        s
    })
}

// ── Error codes ───────────────────────────────────────────────────────────────

const SEQ_REUSED: u64 = 16;
const SEQ_TOO_LOW: u64 = 17;
const MAX_RECORDS_PER_LOOKUP: usize = 20;
const MAX_RELAY_ADDRESSES: usize = 3;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the `Persistent` storage.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PersistentConfig {
    pub max_records: usize,
    pub max_record_age: Duration,
    pub max_per_key: usize,
    pub max_lru_size: usize,
    pub max_lru_age: Duration,
}

impl Default for PersistentConfig {
    fn default() -> Self {
        Self {
            max_records: 65536,
            max_record_age: Duration::from_secs(20 * 60),
            max_per_key: 20,
            max_lru_size: 65536,
            max_lru_age: Duration::from_secs(20 * 60),
        }
    }
}

// ── Incoming request / reply types ───────────────────────────────────────────

/// Incoming user-facing request forwarded from the DHT layer.
pub struct IncomingHyperRequest {
    pub command: u64,
    pub target: Option<[u8; 32]>,
    pub token: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
    pub from: Ipv4Peer,
    pub id: Option<NodeId>,
}

/// What the handler wants to send back.
#[non_exhaustive]
pub enum HandlerReply {
    /// Reply with an optional value and token / closer nodes.
    Value(Option<Vec<u8>>),
    /// Reply with an optional value but *without* token / closer nodes.
    ValueNoToken(Option<Vec<u8>>),
    /// Reply with an error code.
    Error(u64),
    /// Do not reply at all (bad / incomplete request).
    Silent,
}

// ── RecordEntry ───────────────────────────────────────────────────────────────

struct RecordEntry {
    public_key: [u8; 32],
    record: Vec<u8>,
    inserted_at: Instant,
}

// ── RecordCache ───────────────────────────────────────────────────────────────

/// LRU-style cache for peer announcement records, keyed by topic hex.
pub struct RecordCache {
    entries: HashMap<String, Vec<RecordEntry>>,
    max_size: usize,
    max_age: Duration,
    max_per_key: usize,
    total: usize,
}

impl RecordCache {
    pub fn new(max_size: usize, max_age: Duration, max_per_key: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            max_age,
            max_per_key,
            total: 0,
        }
    }

    pub fn add(&mut self, key: &str, public_key: [u8; 32], record: Vec<u8>) {
        let key_str = key.to_string();

        if let Some(existing) = self
            .entries
            .get_mut(&key_str)
            .and_then(|b| b.iter_mut().find(|e| e.public_key == public_key))
        {
            existing.record = record;
            existing.inserted_at = Instant::now();
            return;
        }

        let bucket_len = self.entries.get(&key_str).map_or(0, |b| b.len());
        if bucket_len >= self.max_per_key {
            if let Some(oldest_idx) = self
                .entries
                .get(&key_str)
                .and_then(|b| {
                    b.iter()
                        .enumerate()
                        .min_by_key(|(_, e)| e.inserted_at)
                        .map(|(i, _)| i)
                })
            {
                self.entries.get_mut(&key_str).map(|b| b.remove(oldest_idx));
                self.total = self.total.saturating_sub(1);
            }
        }

        if self.total >= self.max_size {
            self.evict_oldest();
        }

        self.entries.entry(key_str).or_default().push(RecordEntry {
            public_key,
            record,
            inserted_at: Instant::now(),
        });
        self.total += 1;
    }

    /// Remove the record for `(key, public_key)`.
    pub fn remove(&mut self, key: &str, public_key: &[u8; 32]) {
        if let Some(bucket) = self.entries.get_mut(key) {
            let before = bucket.len();
            bucket.retain(|e| &e.public_key != public_key);
            let removed = before - bucket.len();
            self.total = self.total.saturating_sub(removed);
            if bucket.is_empty() {
                self.entries.remove(key);
            }
        }
    }

    /// Get up to `max` live records for `key`.
    pub fn get(&mut self, key: &str, max: usize) -> Vec<Vec<u8>> {
        let now = Instant::now();
        let max_age = self.max_age;

        if let Some(bucket) = self.entries.get_mut(key) {
            let before = bucket.len();
            bucket.retain(|e| now.duration_since(e.inserted_at) < max_age);
            let removed = before - bucket.len();
            self.total = self.total.saturating_sub(removed);

            bucket.iter().take(max).map(|e| e.record.clone()).collect()
        } else {
            vec![]
        }
    }

    /// Evict all entries.
    pub fn destroy(&mut self) {
        self.entries.clear();
        self.total = 0;
    }

    pub fn record_count(&self) -> usize {
        self.total
    }

    pub fn topic_count(&self) -> usize {
        self.entries.len()
    }

    fn evict_oldest(&mut self) {
        // Find the globally oldest entry.
        let oldest_key = self
            .entries
            .iter()
            .filter_map(|(k, bucket)| {
                bucket
                    .iter()
                    .map(|e| e.inserted_at)
                    .min()
                    .map(|t| (k.clone(), t))
            })
            .min_by_key(|(_, t)| *t)
            .map(|(k, _)| k);

        if let Some(k) = oldest_key {
            if let Some(bucket) = self.entries.get_mut(&k) {
                if let Some(oldest_idx) = bucket
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, e)| e.inserted_at)
                    .map(|(i, _)| i)
                {
                    bucket.remove(oldest_idx);
                    self.total = self.total.saturating_sub(1);
                }
                if bucket.is_empty() {
                    self.entries.remove(&k);
                }
            }
        }
    }
}

// ── CacheEntry / LruCache ────────────────────────────────────────────────────

struct CacheEntry {
    value: Vec<u8>,
    inserted_at: Instant,
}

/// Simple TTL cache for mutable/immutable data and bump timestamps.
pub struct LruCache {
    entries: HashMap<String, CacheEntry>,
    max_size: usize,
    max_age: Duration,
}

impl LruCache {
    pub fn new(max_size: usize, max_age: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            max_age,
        }
    }

    /// Get a live value, returning `None` if absent or expired.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entry = self.entries.get(key)?;
        if Instant::now().duration_since(entry.inserted_at) >= self.max_age {
            return None;
        }
        Some(entry.value.clone())
    }

    /// Insert or replace a value.
    pub fn set(&mut self, key: impl Into<String>, value: Vec<u8>) {
        // Evict oldest if at capacity (simple: drop a random entry).
        if self.entries.len() >= self.max_size {
            if let Some(oldest_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.inserted_at)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest_key);
            }
        }
        self.entries.insert(
            key.into(),
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn delete(&mut self, key: &str) {
        self.entries.remove(key);
    }

    pub fn destroy(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ── RouterEntry ───────────────────────────────────────────────────────────────

struct RouterEntry {
    #[allow(dead_code)]
    relay: Ipv4Peer,
    record: Vec<u8>,
}

// ── PersistentStats ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PersistentStats {
    pub records: usize,
    pub record_topics: usize,
    pub mutables: usize,
    pub immutables: usize,
    pub router_entries: usize,
}

// ── Persistent ────────────────────────────────────────────────────────────────

/// Server-side persistent storage for HyperDHT commands.
pub struct Persistent {
    records: RecordCache,
    bumps: LruCache,
    refreshes: LruCache,
    mutables: LruCache,
    immutables: LruCache,
    router: HashMap<String, RouterEntry>,
}

impl Persistent {
    pub fn new(config: PersistentConfig) -> Self {
        Self {
            records: RecordCache::new(
                config.max_records,
                config.max_record_age,
                config.max_per_key,
            ),
            bumps: LruCache::new(config.max_lru_size, config.max_lru_age),
            refreshes: LruCache::new(config.max_lru_size, config.max_lru_age),
            mutables: LruCache::new(config.max_lru_size, config.max_lru_age),
            immutables: LruCache::new(config.max_lru_size, config.max_lru_age),
            router: HashMap::new(),
        }
    }

    // ── FIND_PEER ─────────────────────────────────────────────────────────────

    /// Handle an incoming FIND_PEER request.
    pub fn on_find_peer(&self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => t,
            None => return HandlerReply::Silent,
        };

        let key = to_hex(target);
        if let Some(entry) = self.router.get(&key) {
            HandlerReply::Value(Some(entry.record.clone()))
        } else {
            HandlerReply::Value(None)
        }
    }

    // ── LOOKUP ────────────────────────────────────────────────────────────────

    /// Handle an incoming LOOKUP request.
    pub fn on_lookup(&mut self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };

        let key = to_hex(target);
        let mut peers: Vec<HyperPeer> = self
            .records
            .get(&key, MAX_RECORDS_PER_LOOKUP)
            .into_iter()
            .filter_map(|bytes| {
                crate::hyperdht_messages::decode_hyper_peer_from_bytes(&bytes).ok()
            })
            .collect();

        // Append self-announce from router if we haven't hit the max.
        if peers.len() < MAX_RECORDS_PER_LOOKUP {
            if let Some(entry) = self.router.get(&key) {
                if let Ok(peer) =
                    crate::hyperdht_messages::decode_hyper_peer_from_bytes(&entry.record)
                {
                    peers.push(peer);
                }
            }
        }

        let bump_bytes = self.bumps.get(&key);
        let bump: u64 = bump_bytes
            .as_deref()
            .and_then(|b| {
                let mut s = State::from_buffer(b);
                decode_uint(&mut s).ok()
            })
            .unwrap_or(0);

        if peers.is_empty() && bump == 0 {
            return HandlerReply::Value(None);
        }

        let reply = LookupRawReply { peers, bump };
        match encode_lookup_raw_reply_to_bytes(&reply) {
            Ok(bytes) => HandlerReply::Value(Some(bytes)),
            Err(_) => HandlerReply::Value(None),
        }
    }

    // ── ANNOUNCE ──────────────────────────────────────────────────────────────

    /// Handle an incoming ANNOUNCE request.
    pub fn on_announce(
        &mut self,
        req: &IncomingHyperRequest,
        node_id: &[u8; 32],
    ) -> HandlerReply {
        let target = match &req.target {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };
        let token = match &req.token {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };

        let value = match &req.value {
            Some(v) => v.clone(),
            None => return HandlerReply::Silent,
        };

        let mut msg = match decode_announce_from_bytes(&value) {
            Ok(m) => m,
            Err(_) => return HandlerReply::Silent,
        };

        // Handle refresh: if no peer but refresh token exists, restore peer from cache.
        if msg.peer.is_none() {
            if let Some(refresh) = &msg.refresh {
                let refresh_key = to_hex(refresh);
                if let Some(stored) = self.refreshes.get(&refresh_key) {
                    msg.peer = crate::hyperdht_messages::decode_hyper_peer_from_bytes(&stored).ok();
                }
            }
        }

        let peer = match &msg.peer {
            Some(p) => p.clone(),
            None => return HandlerReply::Silent,
        };

        let signature = match &msg.signature {
            Some(s) => *s,
            None => return HandlerReply::Silent,
        };

        // Encode peer for signature verification.
        let peer_encoded = match encode_hyper_peer_to_bytes(&peer) {
            Ok(b) => b,
            Err(_) => return HandlerReply::Silent,
        };

        let refresh_bytes: &[u8] = msg
            .refresh
            .as_ref()
            .map(|r| r.as_slice())
            .unwrap_or(&[]);

        let signable = ann_signable(
            &target,
            &token,
            node_id,
            &peer_encoded,
            refresh_bytes,
            &NS_ANNOUNCE,
        );

        if !verify_detached(&signature, &signable, &peer.public_key) {
            return HandlerReply::Silent;
        }

        // Limit relay addresses.
        let mut stored_peer = peer.clone();
        stored_peer.relay_addresses.truncate(MAX_RELAY_ADDRESSES);

        let target_key = to_hex(target);
        let announce_self = hash(&stored_peer.public_key) == target;

        if announce_self {
            let encoded = match encode_hyper_peer_to_bytes(&stored_peer) {
                Ok(b) => b,
                Err(_) => return HandlerReply::Silent,
            };
            self.router.insert(
                target_key.clone(),
                RouterEntry {
                    relay: req.from.clone(),
                    record: encoded,
                },
            );
            self.records.remove(&target_key, &stored_peer.public_key);
        } else {
            let encoded = match encode_hyper_peer_to_bytes(&stored_peer) {
                Ok(b) => b,
                Err(_) => return HandlerReply::Silent,
            };
            self.records
                .add(&target_key, stored_peer.public_key, encoded);

            // Update bump if message has a bump.
            if msg.bump != 0 {
                let mut bump_state = crate::compact_encoding::State::new();
                crate::compact_encoding::preencode_uint(&mut bump_state, msg.bump);
                bump_state.alloc();
                crate::compact_encoding::encode_uint(&mut bump_state, msg.bump);
                self.bumps.set(target_key.clone(), bump_state.buffer);
            }
        }

        // Store refresh token if present.
        if let Some(refresh) = &msg.refresh {
            let refresh_key = to_hex(refresh);
            let peer_bytes = encode_hyper_peer_to_bytes(&stored_peer).unwrap_or_default();
            self.refreshes.set(refresh_key, peer_bytes);
        }

        HandlerReply::ValueNoToken(None)
    }

    // ── UNANNOUNCE ────────────────────────────────────────────────────────────

    /// Handle an incoming UNANNOUNCE request.
    pub fn on_unannounce(
        &mut self,
        req: &IncomingHyperRequest,
        node_id: &[u8; 32],
    ) -> HandlerReply {
        let target = match &req.target {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };
        let token = match &req.token {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };

        let value = match &req.value {
            Some(v) => v.clone(),
            None => return HandlerReply::Silent,
        };

        let msg = match decode_announce_from_bytes(&value) {
            Ok(m) => m,
            Err(_) => return HandlerReply::Silent,
        };

        let peer = match &msg.peer {
            Some(p) => p.clone(),
            None => return HandlerReply::Silent,
        };

        let signature = match &msg.signature {
            Some(s) => *s,
            None => return HandlerReply::Silent,
        };

        let peer_encoded = match encode_hyper_peer_to_bytes(&peer) {
            Ok(b) => b,
            Err(_) => return HandlerReply::Silent,
        };

        let refresh_bytes: &[u8] = msg
            .refresh
            .as_ref()
            .map(|r| r.as_slice())
            .unwrap_or(&[]);

        let signable = ann_signable(
            &target,
            &token,
            node_id,
            &peer_encoded,
            refresh_bytes,
            &NS_UNANNOUNCE,
        );

        if !verify_detached(&signature, &signable, &peer.public_key) {
            return HandlerReply::Silent;
        }

        let target_key = to_hex(target);
        self.records.remove(&target_key, &peer.public_key);
        self.router.remove(&target_key);

        HandlerReply::ValueNoToken(None)
    }

    // ── MUTABLE_PUT ───────────────────────────────────────────────────────────

    /// Handle an incoming MUTABLE_PUT request.
    pub fn on_mutable_put(&mut self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };
        if req.token.is_none() {
            return HandlerReply::Silent;
        }

        let value = match &req.value {
            Some(v) => v.clone(),
            None => return HandlerReply::Silent,
        };

        let put = match decode_mutable_put_request_from_bytes(&value) {
            Ok(p) => p,
            Err(_) => return HandlerReply::Silent,
        };

        // Verify target == hash(publicKey).
        if hash(&put.public_key) != target {
            return HandlerReply::Silent;
        }

        // Verify signature.
        let signable = mutable_signable(&NS_MUTABLE_PUT, put.seq, &put.value);
        if !verify_detached(&put.signature, &signable, &put.public_key) {
            return HandlerReply::Silent;
        }

        let target_key = to_hex(target);

        // Check seq against existing stored value.
        if let Some(existing_bytes) = self.mutables.get(&target_key) {
            if let Ok(existing) = decode_mutable_get_response_from_bytes(&existing_bytes) {
                if existing.seq == put.seq {
                    return HandlerReply::Error(SEQ_REUSED);
                }
                if existing.seq > put.seq {
                    return HandlerReply::Error(SEQ_TOO_LOW);
                }
            }
        }

        let response = MutableGetResponse {
            seq: put.seq,
            value: put.value,
            signature: put.signature,
        };

        match encode_mutable_get_response_to_bytes(&response) {
            Ok(encoded) => {
                self.mutables.set(target_key, encoded);
                HandlerReply::Value(None)
            }
            Err(_) => HandlerReply::Silent,
        }
    }

    // ── MUTABLE_GET ───────────────────────────────────────────────────────────

    /// Handle an incoming MUTABLE_GET request.
    pub fn on_mutable_get(&self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => t,
            None => return HandlerReply::Silent,
        };

        let value = match &req.value {
            Some(v) => v.clone(),
            None => return HandlerReply::Silent,
        };

        // Decode requested seq (compact uint).
        let requested_seq = {
            let mut s = State::from_buffer(&value);
            decode_uint(&mut s).unwrap_or(0)
        };

        let target_key = to_hex(target);
        if let Some(stored_bytes) = self.mutables.get(&target_key) {
            if let Ok(stored) = decode_mutable_get_response_from_bytes(&stored_bytes) {
                if stored.seq >= requested_seq {
                    return HandlerReply::Value(Some(stored_bytes));
                }
            }
        }

        HandlerReply::Value(None)
    }

    // ── IMMUTABLE_PUT ────────────────────────────────────────────────────────

    /// Handle an incoming IMMUTABLE_PUT request.
    pub fn on_immutable_put(&mut self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => *t,
            None => return HandlerReply::Silent,
        };
        if req.token.is_none() {
            return HandlerReply::Silent;
        }

        let value = match &req.value {
            Some(v) => v.clone(),
            None => return HandlerReply::Silent,
        };

        // Verify target == hash(value).
        if hash(&value) != target {
            return HandlerReply::Silent;
        }

        let target_key = to_hex(target);
        self.immutables.set(target_key, value);

        HandlerReply::Value(None)
    }

    // ── IMMUTABLE_GET ────────────────────────────────────────────────────────

    /// Handle an incoming IMMUTABLE_GET request.
    pub fn on_immutable_get(&self, req: &IncomingHyperRequest) -> HandlerReply {
        let target = match &req.target {
            Some(t) => t,
            None => return HandlerReply::Silent,
        };

        let target_key = to_hex(target);
        HandlerReply::Value(self.immutables.get(&target_key))
    }

    pub fn stats(&self) -> PersistentStats {
        PersistentStats {
            records: self.records.record_count(),
            record_topics: self.records.topic_count(),
            mutables: self.mutables.len(),
            immutables: self.immutables.len(),
            router_entries: self.router.len(),
        }
    }

    pub fn destroy(&mut self) {
        self.records.destroy();
        self.bumps.destroy();
        self.refreshes.destroy();
        self.mutables.destroy();
        self.immutables.destroy();
        self.router.clear();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn make_persistent() -> Persistent {
        Persistent::new(PersistentConfig::default())
    }

    fn make_keypair() -> ([u8; 32], [u8; 64]) {
        let seed = [0x42u8; 32];
        let sk = SigningKey::from_bytes(&seed);
        let pk: [u8; 32] = sk.verifying_key().to_bytes();
        let mut secret = [0u8; 64];
        secret[..32].copy_from_slice(&seed);
        secret[32..].copy_from_slice(&pk);
        (pk, secret)
    }

    fn dummy_peer() -> Ipv4Peer {
        Ipv4Peer {
            host: "127.0.0.1".to_string(),
            port: 1234,
        }
    }

    // ── RecordCache tests ─────────────────────────────────────────────────────

    #[test]
    fn record_cache_add_get() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        cache.add("topic1", [0xaau8; 32], b"record1".to_vec());
        let records = cache.get("topic1", 10);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], b"record1");
    }

    #[test]
    fn record_cache_replace_same_pubkey() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        cache.add("topic1", [0xaau8; 32], b"v1".to_vec());
        cache.add("topic1", [0xaau8; 32], b"v2".to_vec());
        let records = cache.get("topic1", 10);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], b"v2");
    }

    #[test]
    fn record_cache_remove() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        cache.add("topic1", [0xaau8; 32], b"record1".to_vec());
        cache.remove("topic1", &[0xaau8; 32]);
        let records = cache.get("topic1", 10);
        assert!(records.is_empty());
    }

    #[test]
    fn record_cache_ttl_expired() {
        let mut cache = RecordCache::new(100, Duration::from_nanos(1), 20);
        cache.add("topic1", [0xaau8; 32], b"record1".to_vec());
        // Sleep a tiny bit so TTL expires.
        std::thread::sleep(Duration::from_millis(2));
        let records = cache.get("topic1", 10);
        assert!(records.is_empty());
    }

    #[test]
    fn record_cache_max_per_key() {
        let mut cache = RecordCache::new(1000, Duration::from_secs(60), 3);
        for i in 0u8..5 {
            cache.add("topic1", [i; 32], vec![i]);
        }
        let records = cache.get("topic1", 10);
        assert!(records.len() <= 3);
    }

    #[test]
    fn record_cache_destroy() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        cache.add("topic1", [0xaau8; 32], b"record1".to_vec());
        cache.destroy();
        assert!(cache.get("topic1", 10).is_empty());
    }

    // ── LruCache tests ────────────────────────────────────────────────────────

    #[test]
    fn lru_cache_set_get() {
        let mut cache = LruCache::new(100, Duration::from_secs(60));
        cache.set("k1", b"value".to_vec());
        assert_eq!(cache.get("k1"), Some(b"value".to_vec()));
    }

    #[test]
    fn lru_cache_miss() {
        let cache = LruCache::new(100, Duration::from_secs(60));
        assert_eq!(cache.get("nonexistent"), None);
    }

    #[test]
    fn lru_cache_delete() {
        let mut cache = LruCache::new(100, Duration::from_secs(60));
        cache.set("k1", b"value".to_vec());
        cache.delete("k1");
        assert_eq!(cache.get("k1"), None);
    }

    #[test]
    fn lru_cache_ttl() {
        let mut cache = LruCache::new(100, Duration::from_nanos(1));
        cache.set("k1", b"value".to_vec());
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(cache.get("k1"), None);
    }

    // ── on_lookup tests ───────────────────────────────────────────────────────

    #[test]
    fn on_lookup_no_target_is_silent() {
        let mut p = make_persistent();
        let req = IncomingHyperRequest {
            command: crate::hyperdht_messages::LOOKUP,
            target: None,
            token: None,
            value: None,
            from: dummy_peer(),
            id: None,
        };
        matches!(p.on_lookup(&req), HandlerReply::Silent);
    }

    #[test]
    fn on_lookup_empty_returns_none_value() {
        let mut p = make_persistent();
        let req = IncomingHyperRequest {
            command: crate::hyperdht_messages::LOOKUP,
            target: Some([0u8; 32]),
            token: None,
            value: None,
            from: dummy_peer(),
            id: None,
        };
        matches!(p.on_lookup(&req), HandlerReply::Value(None));
    }

    // ── on_immutable_put/get round-trip ───────────────────────────────────────

    #[test]
    fn immutable_put_get_roundtrip() {
        let mut p = make_persistent();
        let data = b"hello world".to_vec();
        let target = hash(&data);

        let put_req = IncomingHyperRequest {
            command: crate::hyperdht_messages::IMMUTABLE_PUT,
            target: Some(target),
            token: Some([0xbbu8; 32]),
            value: Some(data.clone()),
            from: dummy_peer(),
            id: None,
        };
        let reply = p.on_immutable_put(&put_req);
        assert!(matches!(reply, HandlerReply::Value(None)));

        let get_req = IncomingHyperRequest {
            command: crate::hyperdht_messages::IMMUTABLE_GET,
            target: Some(target),
            token: None,
            value: None,
            from: dummy_peer(),
            id: None,
        };
        let reply = p.on_immutable_get(&get_req);
        if let HandlerReply::Value(Some(v)) = reply {
            assert_eq!(v, data);
        } else {
            panic!("expected Value(Some(...))");
        }
    }

    #[test]
    fn immutable_put_bad_hash_is_silent() {
        let mut p = make_persistent();
        let put_req = IncomingHyperRequest {
            command: crate::hyperdht_messages::IMMUTABLE_PUT,
            target: Some([0xffu8; 32]),
            token: Some([0xbbu8; 32]),
            value: Some(b"wrong".to_vec()),
            from: dummy_peer(),
            id: None,
        };
        assert!(matches!(p.on_immutable_put(&put_req), HandlerReply::Silent));
    }

    // ── on_mutable_put/get round-trip ─────────────────────────────────────────

    #[test]
    fn mutable_put_get_roundtrip() {
        let mut p = make_persistent();
        let (pk, sk) = make_keypair();
        let target = hash(&pk);
        let value = b"my value".to_vec();
        let seq: u64 = 1;

        let signable = mutable_signable(&NS_MUTABLE_PUT, seq, &value);
        let sig = crate::crypto::sign_detached(&signable, &sk);

        let put_msg = crate::hyperdht_messages::MutablePutRequest {
            public_key: pk,
            seq,
            value: value.clone(),
            signature: sig,
        };
        let put_bytes =
            crate::hyperdht_messages::encode_mutable_put_request_to_bytes(&put_msg).unwrap();

        let put_req = IncomingHyperRequest {
            command: crate::hyperdht_messages::MUTABLE_PUT,
            target: Some(target),
            token: Some([0u8; 32]),
            value: Some(put_bytes),
            from: dummy_peer(),
            id: None,
        };

        let reply = p.on_mutable_put(&put_req);
        assert!(matches!(reply, HandlerReply::Value(None)));

        // Get with seq=0 (any seq >= 0 matches).
        let mut seq_state = crate::compact_encoding::State::new();
        crate::compact_encoding::preencode_uint(&mut seq_state, 0u64);
        seq_state.alloc();
        crate::compact_encoding::encode_uint(&mut seq_state, 0u64);

        let get_req = IncomingHyperRequest {
            command: crate::hyperdht_messages::MUTABLE_GET,
            target: Some(target),
            token: None,
            value: Some(seq_state.buffer),
            from: dummy_peer(),
            id: None,
        };

        let reply = p.on_mutable_get(&get_req);
        if let HandlerReply::Value(Some(v)) = reply {
            let decoded =
                crate::hyperdht_messages::decode_mutable_get_response_from_bytes(&v).unwrap();
            assert_eq!(decoded.seq, seq);
            assert_eq!(decoded.value, value);
        } else {
            panic!("expected Value(Some(...))");
        }
    }

    #[test]
    fn mutable_put_seq_too_low() {
        let mut p = make_persistent();
        let (pk, sk) = make_keypair();
        let target = hash(&pk);

        let put_with_seq = |seq: u64| {
            let signable = mutable_signable(&NS_MUTABLE_PUT, seq, b"v");
            let sig = crate::crypto::sign_detached(&signable, &sk);
            let put_msg = crate::hyperdht_messages::MutablePutRequest {
                public_key: pk,
                seq,
                value: b"v".to_vec(),
                signature: sig,
            };
            crate::hyperdht_messages::encode_mutable_put_request_to_bytes(&put_msg).unwrap()
        };

        let req = |put_bytes: Vec<u8>| IncomingHyperRequest {
            command: crate::hyperdht_messages::MUTABLE_PUT,
            target: Some(target),
            token: Some([0u8; 32]),
            value: Some(put_bytes),
            from: dummy_peer(),
            id: None,
        };

        // Store seq=5.
        p.on_mutable_put(&req(put_with_seq(5)));

        // Try seq=3 → TOO_LOW.
        assert!(matches!(
            p.on_mutable_put(&req(put_with_seq(3))),
            HandlerReply::Error(17)
        ));
    }

    #[test]
    fn mutable_put_seq_reused() {
        let mut p = make_persistent();
        let (pk, sk) = make_keypair();
        let target = hash(&pk);

        let put_with_seq = |seq: u64| {
            let signable = mutable_signable(&NS_MUTABLE_PUT, seq, b"v");
            let sig = crate::crypto::sign_detached(&signable, &sk);
            let put_msg = crate::hyperdht_messages::MutablePutRequest {
                public_key: pk,
                seq,
                value: b"v".to_vec(),
                signature: sig,
            };
            crate::hyperdht_messages::encode_mutable_put_request_to_bytes(&put_msg).unwrap()
        };

        let req = |put_bytes: Vec<u8>| IncomingHyperRequest {
            command: crate::hyperdht_messages::MUTABLE_PUT,
            target: Some(target),
            token: Some([0u8; 32]),
            value: Some(put_bytes),
            from: dummy_peer(),
            id: None,
        };

        p.on_mutable_put(&req(put_with_seq(5)));
        assert!(matches!(
            p.on_mutable_put(&req(put_with_seq(5))),
            HandlerReply::Error(16)
        ));
    }

    #[test]
    fn on_announce_invalid_signature_is_silent() {
        let mut p = make_persistent();
        let (pk, _sk) = make_keypair();
        let node_id = [0x00u8; 32];
        let target = hash(&pk);

        let peer = crate::hyperdht_messages::HyperPeer {
            public_key: pk,
            relay_addresses: vec![],
        };

        // Use a bad signature.
        let ann = crate::hyperdht_messages::AnnounceMessage {
            peer: Some(peer),
            refresh: None,
            signature: Some([0xffu8; 64]),
            bump: 0,
        };
        let ann_bytes =
            crate::hyperdht_messages::encode_announce_to_bytes(&ann).unwrap();

        let req = IncomingHyperRequest {
            command: crate::hyperdht_messages::ANNOUNCE,
            target: Some(target),
            token: Some([0u8; 32]),
            value: Some(ann_bytes),
            from: dummy_peer(),
            id: Some(node_id),
        };

        assert!(matches!(
            p.on_announce(&req, &node_id),
            HandlerReply::Silent
        ));
    }

    // ── stats tests ───────────────────────────────────────────────────────────

    #[test]
    fn lru_cache_len_empty() {
        let cache = LruCache::new(100, Duration::from_secs(60));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn lru_cache_len_after_set() {
        let mut cache = LruCache::new(100, Duration::from_secs(60));
        cache.set("k1", b"v1".to_vec());
        cache.set("k2", b"v2".to_vec());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn record_cache_record_count() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        assert_eq!(cache.record_count(), 0);
        cache.add("topic1", [0x01u8; 32], b"r1".to_vec());
        cache.add("topic1", [0x02u8; 32], b"r2".to_vec());
        cache.add("topic2", [0x03u8; 32], b"r3".to_vec());
        assert_eq!(cache.record_count(), 3);
    }

    #[test]
    fn record_cache_topic_count() {
        let mut cache = RecordCache::new(100, Duration::from_secs(60), 20);
        assert_eq!(cache.topic_count(), 0);
        cache.add("topic1", [0x01u8; 32], b"r1".to_vec());
        cache.add("topic2", [0x02u8; 32], b"r2".to_vec());
        assert_eq!(cache.topic_count(), 2);
        cache.add("topic1", [0x03u8; 32], b"r3".to_vec());
        assert_eq!(cache.topic_count(), 2);
    }

    #[test]
    fn persistent_stats_empty() {
        let p = make_persistent();
        let s = p.stats();
        assert_eq!(s.records, 0);
        assert_eq!(s.record_topics, 0);
        assert_eq!(s.mutables, 0);
        assert_eq!(s.immutables, 0);
        assert_eq!(s.router_entries, 0);
    }

    #[test]
    fn persistent_stats_after_immutable_put() {
        let mut p = make_persistent();
        let data = b"stats test data".to_vec();
        let target = hash(&data);
        let req = IncomingHyperRequest {
            command: crate::hyperdht_messages::IMMUTABLE_PUT,
            target: Some(target),
            token: Some([0u8; 32]),
            value: Some(data),
            from: dummy_peer(),
            id: None,
        };
        p.on_immutable_put(&req);
        let s = p.stats();
        assert_eq!(s.immutables, 1);
    }
}

