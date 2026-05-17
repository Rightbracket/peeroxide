#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Put,
    Get,
}

pub struct ProgressState {
    pub phase: Phase,
    pub version: u8,
    pub filename: Arc<str>,
    pub bytes_total: AtomicU64,
    pub bytes_done: AtomicU64,
    pub indexes_total: AtomicU32,
    pub indexes_done: AtomicU32,
    pub data_total: AtomicU32,
    pub data_done: AtomicU32,
    /// Cumulative UDP bytes sent at the DHT IO layer. Shared `Arc<AtomicU64>`
    /// with `peeroxide_dht::io::WireCounters` so the display can sample the
    /// live counter without going through a getter call. Default-constructed
    /// states have an unconnected counter that stays at 0 (useful for v1
    /// where wire stats aren't displayed).
    pub wire_bytes_sent: Arc<AtomicU64>,
    /// Cumulative UDP bytes received at the DHT IO layer. See `wire_bytes_sent`.
    pub wire_bytes_received: Arc<AtomicU64>,
    pub start_instant: Instant,
}

impl ProgressState {
    pub fn new(phase: Phase, version: u8, filename: Arc<str>) -> Arc<Self> {
        Self::new_with_wire(
            phase,
            version,
            filename,
            peeroxide_dht::io::WireCounters::default(),
        )
    }

    /// Construct a `ProgressState` connected to a live `WireCounters` so the
    /// renderer can display real DHT wire-byte rates alongside payload rates.
    pub fn new_with_wire(
        phase: Phase,
        version: u8,
        filename: Arc<str>,
        wire: peeroxide_dht::io::WireCounters,
    ) -> Arc<Self> {
        Arc::new(Self {
            phase,
            version,
            filename,
            bytes_total: AtomicU64::new(0),
            bytes_done: AtomicU64::new(0),
            indexes_total: AtomicU32::new(0),
            indexes_done: AtomicU32::new(0),
            data_total: AtomicU32::new(0),
            data_done: AtomicU32::new(0),
            wire_bytes_sent: wire.bytes_sent,
            wire_bytes_received: wire.bytes_received,
            start_instant: Instant::now(),
        })
    }

    pub fn set_length(&self, bytes_total: u64, indexes_total: u32, data_total: u32) {
        self.bytes_total.store(bytes_total, Ordering::Relaxed);
        self.indexes_total.store(indexes_total, Ordering::Relaxed);
        self.data_total.store(data_total, Ordering::Relaxed);
    }

    pub fn inc_index(&self) {
        self.indexes_done.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_data(&self, chunk_bytes: u64) {
        self.data_done.fetch_add(1, Ordering::Relaxed);
        self.bytes_done.fetch_add(chunk_bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_serde() {
        assert_eq!(serde_json::to_string(&Phase::Put).unwrap(), "\"put\"");
        assert_eq!(serde_json::to_string(&Phase::Get).unwrap(), "\"get\"");
    }

    #[test]
    fn set_length_after_start() {
        let state = ProgressState::new(Phase::Put, 2, Arc::<str>::from("file.txt"));
        assert_eq!(state.bytes_total.load(Ordering::Relaxed), 0);
        state.set_length(1000, 3, 5);
        assert_eq!(state.bytes_total.load(Ordering::Relaxed), 1000);
        assert_eq!(state.indexes_total.load(Ordering::Relaxed), 3);
        assert_eq!(state.data_total.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn no_panic_on_zero_bytes() {
        let state = ProgressState::new(Phase::Get, 2, Arc::<str>::from("file.txt"));
        state.inc_data(0);
        assert_eq!(state.data_done.load(Ordering::Relaxed), 1);
        assert_eq!(state.bytes_done.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn new_with_wire_shares_atomics_with_counters() {
        // Verify that incrementing the WireCounters' atomics is visible from
        // the ProgressState (i.e. the Arcs are shared, not cloned by value).
        use std::sync::atomic::AtomicU64;
        let wire = peeroxide_dht::io::WireCounters::from_counters(
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        let state = ProgressState::new_with_wire(
            Phase::Put,
            2,
            Arc::<str>::from("file.txt"),
            wire.clone(),
        );
        wire.bytes_sent.store(12_345, Ordering::Relaxed);
        wire.bytes_received.store(67_890, Ordering::Relaxed);
        assert_eq!(state.wire_bytes_sent.load(Ordering::Relaxed), 12_345);
        assert_eq!(state.wire_bytes_received.load(Ordering::Relaxed), 67_890);
    }

    #[test]
    fn new_default_wire_is_unconnected_zero() {
        // Plain `new` produces a state whose wire counters are independent
        // and stay at 0 forever — useful for v1 paths that don't display
        // wire stats.
        let state = ProgressState::new(Phase::Put, 1, Arc::<str>::from("file.txt"));
        assert_eq!(state.wire_bytes_sent.load(Ordering::Relaxed), 0);
        assert_eq!(state.wire_bytes_received.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_inc() {
        let state = ProgressState::new(Phase::Put, 2, Arc::<str>::from("file.txt"));
        let mut tasks = Vec::with_capacity(64);
        for _ in 0..64 {
            let state = Arc::clone(&state);
            tasks.push(tokio::spawn(async move {
                state.inc_data(65536);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(state.data_done.load(Ordering::Relaxed), 64);
        assert_eq!(state.bytes_done.load(Ordering::Relaxed), 64 * 65536);
    }
}
