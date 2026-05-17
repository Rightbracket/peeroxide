#![allow(dead_code)]

//! `ProgressReporter` — enum facade over the four progress channels.
//!
//! The rest of the codebase only ever interacts with `ProgressReporter`.
//! Construction picks a variant based on `ProgressMode`, and lifecycle /
//! event-dispatch methods fan out to the underlying renderer (or no-op
//! for `Off`). The `Bar` and `Log` renderers run their own internal tick
//! tasks; the `Json` variant is caller-driven and owns a `RateCalculator`
//! so it can fill the rate/eta fields on each progress snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;

use crate::cmd::deaddrop::progress::{
    bar::BarRenderer,
    json::JsonEmitter,
    log::PeriodicLogRenderer,
    mode::ProgressMode,
    rate::RateCalculator,
    state::{Phase, ProgressState},
};

pub enum ProgressReporter {
    Bar(BarRenderer),
    Log(PeriodicLogRenderer),
    Json {
        emitter: JsonEmitter,
        rate: Arc<Mutex<RateCalculator>>,
    },
    Off,
}

impl ProgressReporter {
    pub fn new(mode: ProgressMode, state: Arc<ProgressState>) -> Self {
        match mode {
            ProgressMode::Bar => Self::Bar(BarRenderer::new(state)),
            ProgressMode::PeriodicLog => Self::Log(PeriodicLogRenderer::new(state)),
            ProgressMode::Json => Self::Json {
                emitter: JsonEmitter::new(state),
                rate: Arc::new(Mutex::new(RateCalculator::new())),
            },
            ProgressMode::Off => Self::Off,
        }
    }

    /// Convenience constructor: reads stderr TTY status and args flags, selects mode.
    pub fn from_args(state: Arc<ProgressState>, no_progress: bool, json: bool) -> Self {
        use std::io::IsTerminal;
        let mode = crate::cmd::deaddrop::progress::mode::select(
            std::io::stderr().is_terminal(),
            no_progress,
            json,
        );
        Self::new(mode, state)
    }

    /// Called after initial PUT publish completes.
    /// - Bar/Log: stops the tick, then prints pickup key to stdout.
    /// - Json: emits a `put_result` event (which includes the pickup key).
    /// - Off: prints pickup key to stdout.
    ///
    /// Does NOT consume self — the reporter stays alive for the refresh/ack loop.
    pub async fn emit_initial_publish_complete(&mut self, pickup_key: &str) {
        match self {
            Self::Bar(r) => {
                r.finish_initial().await;
                println!("{pickup_key}");
            }
            Self::Log(r) => {
                r.finish_initial().await;
                println!("{pickup_key}");
            }
            Self::Json { emitter, .. } => {
                emitter.emit_put_result(pickup_key);
            }
            Self::Off => {
                println!("{pickup_key}");
            }
        }
    }

    /// Stop the tick task; leave `self` alive for the PUT refresh-loop
    /// handoff. For Json, emit a `done` event since there is no tick to
    /// stop. Off is a no-op.
    pub async fn finish_initial(&mut self) {
        match self {
            Self::Bar(r) => r.finish_initial().await,
            Self::Log(r) => r.finish_initial().await,
            Self::Json { emitter, .. } => emitter.emit_done(),
            Self::Off => {}
        }
    }

    /// Full shutdown — consumes `self`.
    pub async fn finish(self) {
        match self {
            Self::Bar(r) => r.finish().await,
            Self::Log(r) => r.finish().await,
            Self::Json { emitter, .. } => emitter.emit_done(),
            Self::Off => {}
        }
    }

    /// Called after each data chunk is fetched/stored. Bar and Log
    /// renderers have their own internal tick — this is a no-op for
    /// them. The caller drives explicit progress emission for Json via
    /// `emit_progress_snapshot`.
    pub fn on_chunk_done(&self) {}

    /// Called after each index chunk is fetched (v2 GET). Same as
    /// `on_chunk_done` — internal tick handles Bar/Log; caller drives
    /// Json.
    pub fn on_index_done(&self) {}

    /// Emit a `start` event. Json only; other variants no-op.
    pub fn on_start(&self) {
        if let Self::Json { emitter, .. } = self {
            emitter.emit_start();
        }
    }

    /// Signal completion to the active channel. Equivalent to
    /// `finish_initial` — emits a Json `done` event or stops the
    /// renderer tick task.
    pub async fn on_done(&mut self) {
        self.finish_initial().await;
    }

    /// Emit a `put_result` event with the assembled pickup key. Json
    /// only.
    pub fn on_put_result(&self, key: &str) {
        if let Self::Json { emitter, .. } = self {
            emitter.emit_put_result(key);
        }
    }

    /// Emit a `get_result` event. Json only.
    pub fn on_get_result(&self, bytes: u64, crc: &str, output: Option<&str>) {
        if let Self::Json { emitter, .. } = self {
            emitter.emit_get_result(bytes, crc, output);
        }
    }

    /// Emit an `ack` event for a notify-pickup. Json only.
    pub fn on_ack(&self, pickup_number: u64, peer: &str) {
        if let Self::Json { emitter, .. } = self {
            emitter.emit_ack(pickup_number, peer);
        }
    }

    /// Emit a periodic progress snapshot for the Json channel. Bar/Log
    /// have their own tick tasks and ignore this; Off no-ops. The
    /// caller is expected to invoke this from the orchestrator's tick
    /// loop so the rate/eta fields stay fresh.
    pub async fn emit_progress_snapshot(&self) {
        if let Self::Json { emitter, rate } = self {
            let now = std::time::Instant::now();
            let bytes_done = emitter.state.bytes_done.load(Ordering::Relaxed);
            let total = emitter.state.bytes_total.load(Ordering::Relaxed);
            let mut r = rate.lock().await;
            r.record(now, bytes_done);
            let rate_bps = r.rate_bps();
            let eta = r.eta_secs(total, bytes_done);
            drop(r);
            emitter.emit_progress(rate_bps, eta);
        }
    }

    /// Build a clonable factory for spawning short-lived per-operation
    /// progress bars after the initial publish has finished. Use this
    /// for refresh ticks and need-list republishes — `begin_operation`
    /// returns a fresh transient bar that the caller drives by
    /// `inc_data`/`inc_index` on the returned state and then disposes
    /// with `OperationHandle::finish`.
    ///
    /// The factory inherits wire counters / filename / version from the
    /// reporter so wire-throughput readings stay continuous across
    /// operations.
    pub fn operation_factory(&self) -> OperationFactory {
        let kind = match self {
            Self::Bar(r) => {
                let st = r.state();
                OperationFactoryKind::Bar {
                    wire_sent: st.wire_bytes_sent.clone(),
                    wire_received: st.wire_bytes_received.clone(),
                    filename: st.filename.clone(),
                    version: st.version,
                }
            }
            Self::Log(_) | Self::Json { .. } | Self::Off => OperationFactoryKind::Quiet,
        };
        OperationFactory { kind }
    }
}

/// Cloneable handle that can spawn transient per-operation progress
/// bars. Safe to pass into background tasks (e.g. the need-list
/// watcher) so they can show their own progress without holding a
/// reference to the main reporter.
#[derive(Clone)]
pub struct OperationFactory {
    kind: OperationFactoryKind,
}

#[derive(Clone)]
enum OperationFactoryKind {
    Bar {
        wire_sent: Arc<AtomicU64>,
        wire_received: Arc<AtomicU64>,
        filename: Arc<str>,
        version: u8,
    },
    /// Log / Json / Off: no visible per-operation UI. The handle still
    /// exposes a `ProgressState` so publish helpers can call
    /// `inc_data`/`inc_index` unconditionally without branching.
    Quiet,
}

impl OperationFactory {
    /// Begin a per-operation progress display. The returned handle owns
    /// a fresh `ProgressState` that callers should hand to publish
    /// helpers via `handle.state()`. Drop or call `finish()` when the
    /// operation completes.
    pub fn begin_operation(
        &self,
        bytes_total: u64,
        indexes_total: u32,
        data_total: u32,
    ) -> OperationHandle {
        match &self.kind {
            OperationFactoryKind::Bar {
                wire_sent,
                wire_received,
                filename,
                version,
            } => {
                let wire = peeroxide_dht::io::WireCounters::from_counters(
                    wire_sent.clone(),
                    wire_received.clone(),
                );
                let state = ProgressState::new_with_wire(
                    Phase::Put,
                    *version,
                    filename.clone(),
                    wire,
                );
                state.set_length(bytes_total, indexes_total, data_total);
                let renderer = BarRenderer::new(state.clone());
                OperationHandle {
                    state,
                    inner: OperationInner::Bar(Some(renderer)),
                }
            }
            OperationFactoryKind::Quiet => {
                let state = ProgressState::new(Phase::Put, 2, Arc::<str>::from(""));
                state.set_length(bytes_total, indexes_total, data_total);
                OperationHandle {
                    state,
                    inner: OperationInner::Quiet,
                }
            }
        }
    }
}

/// Handle to an in-flight per-operation progress display.
pub struct OperationHandle {
    state: Arc<ProgressState>,
    inner: OperationInner,
}

enum OperationInner {
    Bar(Option<BarRenderer>),
    Quiet,
}

impl OperationHandle {
    /// Shared progress state for this operation. Hand it to publish
    /// helpers so they can increment data/index counters as work
    /// completes.
    pub fn state(&self) -> Arc<ProgressState> {
        self.state.clone()
    }

    /// Stop the per-operation bar and clear its lines from the
    /// terminal. Quiet variants no-op. Consumes `self`.
    pub async fn finish(mut self) {
        match &mut self.inner {
            OperationInner::Bar(slot) => {
                if let Some(renderer) = slot.take() {
                    renderer.finish_and_clear().await;
                }
            }
            OperationInner::Quiet => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::deaddrop::progress::state::Phase;

    fn make_state() -> Arc<ProgressState> {
        let s = ProgressState::new(Phase::Put, 1, Arc::<str>::from("test.bin"));
        s.set_length(1000, 0, 2);
        s
    }

    #[test]
    fn from_args_off_when_no_progress() {
        let state = make_state();
        let r = ProgressReporter::from_args(state, true, false);
        assert!(matches!(r, ProgressReporter::Off));
    }

    #[test]
    fn from_args_json_when_json_flag() {
        let state = make_state();
        let r = ProgressReporter::from_args(state, false, true);
        assert!(matches!(r, ProgressReporter::Json { .. }));
    }

    #[test]
    fn off_variant_constructs() {
        let r = ProgressReporter::new(ProgressMode::Off, make_state());
        assert!(matches!(r, ProgressReporter::Off));
    }

    #[tokio::test]
    async fn off_variant_lifecycle_is_noop() {
        let mut r = ProgressReporter::new(ProgressMode::Off, make_state());
        r.finish_initial().await;
        r.finish().await;
    }

    #[tokio::test]
    async fn off_event_methods_noop() {
        let r = ProgressReporter::new(ProgressMode::Off, make_state());
        r.on_start();
        r.on_chunk_done();
        r.on_index_done();
        r.on_put_result("key");
        r.on_get_result(100, "crc", None);
        r.on_ack(1, "peer");
        r.emit_progress_snapshot().await;
    }

    #[test]
    fn json_variant_constructs() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        assert!(matches!(r, ProgressReporter::Json { .. }));
    }

    #[tokio::test]
    async fn json_on_start_no_panic() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.on_start();
    }

    #[tokio::test]
    async fn json_on_put_result_no_panic() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.on_put_result("abc123key");
    }

    #[tokio::test]
    async fn json_on_get_result_no_panic() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.on_get_result(5000, "deadbeef", Some("/tmp/out.bin"));
    }

    #[tokio::test]
    async fn json_on_ack_no_panic() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.on_ack(2, "peer-id");
    }

    #[tokio::test]
    async fn json_emit_progress_snapshot_no_panic() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.emit_progress_snapshot().await;
    }

    #[tokio::test]
    async fn json_finish_initial_emits_done() {
        let mut r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.finish_initial().await;
    }

    #[tokio::test]
    async fn json_finish_consumes_and_emits_done() {
        let r = ProgressReporter::new(ProgressMode::Json, make_state());
        r.finish().await;
    }

    #[tokio::test]
    async fn bar_variant_constructs() {
        let r = ProgressReporter::new(ProgressMode::Bar, make_state());
        assert!(matches!(r, ProgressReporter::Bar(_)));
        r.finish().await;
    }

    #[tokio::test]
    async fn log_variant_constructs() {
        let r = ProgressReporter::new(ProgressMode::PeriodicLog, make_state());
        assert!(matches!(r, ProgressReporter::Log(_)));
        r.finish().await;
    }

    #[tokio::test]
    async fn bar_finish_initial_then_finish() {
        let mut r = ProgressReporter::new(ProgressMode::Bar, make_state());
        r.finish_initial().await;
        r.finish().await;
    }

    #[tokio::test]
    async fn bar_operation_factory_begin_creates_visible_bar() {
        let r = ProgressReporter::new(ProgressMode::Bar, make_state());
        let factory = r.operation_factory();
        let op = factory.begin_operation(500, 1, 4);
        // The op state should have been initialized with the requested totals.
        assert_eq!(op.state().bytes_total.load(Ordering::Relaxed), 500);
        assert_eq!(op.state().indexes_total.load(Ordering::Relaxed), 1);
        assert_eq!(op.state().data_total.load(Ordering::Relaxed), 4);
        // Incrementing the state should be reflected.
        op.state().inc_data(100);
        assert_eq!(op.state().bytes_done.load(Ordering::Relaxed), 100);
        assert_eq!(op.state().data_done.load(Ordering::Relaxed), 1);
        op.finish().await;
        r.finish().await;
    }

    #[tokio::test]
    async fn quiet_operation_factory_returns_usable_state() {
        for mode in [ProgressMode::Off, ProgressMode::PeriodicLog, ProgressMode::Json] {
            let r = ProgressReporter::new(mode, make_state());
            let factory = r.operation_factory();
            let op = factory.begin_operation(100, 0, 1);
            op.state().inc_data(50);
            assert_eq!(op.state().bytes_done.load(Ordering::Relaxed), 50);
            op.finish().await;
            r.finish().await;
        }
    }

    #[tokio::test]
    async fn operation_factory_is_clone_and_send() {
        let r = ProgressReporter::new(ProgressMode::Bar, make_state());
        let factory = r.operation_factory();
        let f2 = factory.clone();
        let task = tokio::spawn(async move {
            let op = f2.begin_operation(10, 0, 1);
            op.finish().await;
        });
        task.await.unwrap();
        let op = factory.begin_operation(20, 0, 1);
        op.finish().await;
        r.finish().await;
    }
}
