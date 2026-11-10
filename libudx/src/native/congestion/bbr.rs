//! BBR congestion control.
//!
//! Port of C libudx's `udx_bbr.c`. From the rate sample each incoming
//! packet yields, BBR keeps two estimates:
//!
//! - the bandwidth, as the largest delivery rate of the last ten round trips,
//! - the lowest round-trip time of the last ten seconds,
//!
//! and sets the pacing rate to a gain times the bandwidth, and the
//! congestion window to a gain times what the path holds at that bandwidth
//! over that round-trip time.
//!
//! Units are those of the C code: bandwidth in packets per millisecond, the
//! window in packets, the pacing rate in bytes per millisecond, and round-trip
//! times in whole milliseconds. Moments are microseconds on the stream's
//! clock.

use super::filter::WindowedFilter;
use super::rate::RateSample;
use super::{
    seq_diff, BbrMode, CaState, CongestionController, BBR_BW_FILTER_CYCLES, BBR_CWND_GAIN,
    BBR_CWND_MIN_TARGET, BBR_CYCLE_LEN, BBR_DRAIN_GAIN, BBR_EXTRA_ACKED_MAX_MS,
    BBR_EXTRA_ACKED_WIN_RTTS, BBR_FULL_BW_COUNT, BBR_FULL_BW_THRESH, BBR_HIGH_GAIN,
    BBR_MIN_PROBE_RTT_MODE_MS, BBR_MIN_RTT_INTERVAL_MS, BBR_PACING_GAIN,
    BBR_PACING_MARGIN_PERCENT,
};

/// The window BBR assumes the path holds before it has a round-trip time.
const BDP_WITHOUT_RTT: u32 = 10;

/// BBR's own state. C: `stream->bbr`.
#[derive(Debug, Clone)]
pub(crate) struct BbrState {
    pub(super) mode: BbrMode,
    pub(super) prev_ca_state: CaState,
    /// Largest recent delivery rate, in packets per millisecond.
    bw: WindowedFilter<f64>,
    /// Round trips counted so far.
    rtt_count: u64,
    pub(super) round_start: bool,
    next_rtt_delivered: u32,
    use_packet_conservation: bool,

    // Startup
    pub(super) full_bw: f64,
    full_bw_count: u32,
    pub(super) full_bw_reached: bool,

    // ProbeBw
    cycle_index: usize,
    /// When the current phase of the cycle began, in microseconds.
    cycle_timestamp: u64,

    // ProbeRtt
    /// Lowest round-trip time of the last ten seconds. `u32::MAX` until one
    /// has been measured.
    pub(super) min_rtt_ms: u32,
    /// When it was measured, in microseconds.
    min_rtt_stamp: u64,
    /// When ProbeRtt may end, in microseconds.
    probe_rtt_done_time: Option<u64>,
    probe_rtt_round_done: bool,
    idle_restart: bool,

    has_seen_rtt: bool,
    pub(super) pacing_gain: f64,
    pub(super) cwnd_gain: f64,
    pub(super) prior_cwnd: u32,

    // Acknowledgement aggregation
    extra_acked: [u32; 2],
    extra_acked_win_rtts: u32,
    extra_acked_win_index: usize,
    /// When the current stretch of counting acknowledgements began, in
    /// microseconds.
    ack_epoch_start: u64,
    ack_epoch_acked: u32,
}

impl BbrState {
    /// C: `bbr_init`.
    pub(super) fn new(now_us: u64) -> Self {
        Self {
            mode: BbrMode::Startup,
            prev_ca_state: CaState::Open,
            bw: WindowedFilter::new(0, 0.0),
            rtt_count: 0,
            round_start: false,
            next_rtt_delivered: 0,
            use_packet_conservation: false,
            full_bw: 0.0,
            full_bw_count: 0,
            full_bw_reached: false,
            cycle_index: 0,
            cycle_timestamp: 0,
            min_rtt_ms: u32::MAX,
            min_rtt_stamp: now_us,
            probe_rtt_done_time: None,
            probe_rtt_round_done: false,
            idle_restart: false,
            has_seen_rtt: false,
            pacing_gain: BBR_HIGH_GAIN,
            cwnd_gain: BBR_HIGH_GAIN,
            prior_cwnd: 0,
            extra_acked: [0; 2],
            extra_acked_win_rtts: 0,
            extra_acked_win_index: 0,
            ack_epoch_start: now_us,
            ack_epoch_acked: 0,
        }
    }

    /// The bandwidth estimate, in packets per millisecond.
    fn max_bw(&self) -> f64 {
        self.bw.get()
    }

    fn extra_acked(&self) -> u32 {
        self.extra_acked[0].max(self.extra_acked[1])
    }
}

impl CongestionController {
    /// Pacing rate, in bytes per millisecond, for a bandwidth and a gain.
    /// Never zero, which would stop the stream.
    fn bbr_bw_to_pacing_rate(&self, bw: f64, gain: f64) -> u64 {
        let rate = bw * f64::from(self.mss) * gain * BBR_PACING_MARGIN_PERCENT;
        (rate as u64).max(1)
    }

    /// Paces the initial window over one round trip, or over a millisecond
    /// while no round-trip time is known.
    pub(super) fn bbr_init_pacing_rate_from_rtt(&mut self) {
        let srtt_ms = if self.srtt_ms != 0 {
            self.bbr.has_seen_rtt = true;
            self.srtt_ms
        } else {
            1
        };
        let bw = f64::from(self.cwnd) / f64::from(srtt_ms);
        self.pacing_bytes_per_ms = self.bbr_bw_to_pacing_rate(bw, BBR_HIGH_GAIN);
    }

    fn bbr_set_pacing_rate(&mut self, bw: f64, gain: f64) {
        let rate = self.bbr_bw_to_pacing_rate(bw, gain);

        if !self.bbr.has_seen_rtt && self.srtt_ms != 0 {
            self.bbr_init_pacing_rate_from_rtt();
        }

        // Until the path's full bandwidth is known, the rate only goes up.
        if self.bbr.full_bw_reached || rate > self.pacing_bytes_per_ms {
            self.pacing_bytes_per_ms = rate;
        }
    }

    /// Remembers the window so it can be restored after a spell of holding
    /// it down.
    fn bbr_save_cwnd(&mut self) {
        if self.bbr.prev_ca_state < CaState::Recovery && self.bbr.mode != BbrMode::ProbeRtt {
            self.bbr.prior_cwnd = self.cwnd;
        } else {
            self.bbr.prior_cwnd = self.bbr.prior_cwnd.max(self.cwnd);
        }
    }

    /// Called when a packet is sent with nothing in flight.
    /// C: `bbr_on_transmit_start`.
    pub(crate) fn on_transmit_start(&mut self, now_us: u64) {
        if self.app_limited == 0 {
            return;
        }

        self.bbr.idle_restart = true;
        self.bbr.ack_epoch_start = now_us;
        self.bbr.ack_epoch_acked = 0;

        match self.bbr.mode {
            BbrMode::ProbeBw => self.bbr_set_pacing_rate(self.bbr.max_bw(), 1.0),
            BbrMode::ProbeRtt => self.bbr_check_probe_rtt_done(now_us),
            BbrMode::Startup | BbrMode::Drain => {}
        }
    }

    /// Packets the path holds at bandwidth `bw` over the lowest round-trip
    /// time, times `gain`.
    fn bbr_bdp(&self, bw: f64, gain: f64) -> u32 {
        if self.bbr.min_rtt_ms == u32::MAX {
            return BDP_WITHOUT_RTT;
        }
        (bw * f64::from(self.bbr.min_rtt_ms) * gain) as u32
    }

    /// Extra window that lets the stream keep sending while acknowledgements
    /// arrive in bursts, capped at what the bandwidth delivers in 100 ms.
    fn bbr_ack_aggregation_cwnd(&self) -> u32 {
        if !self.bbr.full_bw_reached {
            return 0;
        }
        let max_aggr_cwnd = (self.bbr.max_bw() * BBR_EXTRA_ACKED_MAX_MS as f64) as u32;
        self.bbr.extra_acked().min(max_aggr_cwnd)
    }

    /// Sets the window on entering and leaving loss recovery. Returns
    /// whether the window is being held to what is in flight, and the window.
    fn bbr_set_cwnd_to_recover_or_restore(
        &mut self,
        rs: &RateSample,
        acked: u32,
        inflight: u32,
    ) -> (bool, u32) {
        let prev_state = self.bbr.prev_ca_state;
        let state = self.ca_state;
        let mut cwnd = self.cwnd;

        if rs.losses > 0 {
            cwnd = cwnd.saturating_sub(rs.losses).max(1);
        }

        if state == CaState::Recovery && prev_state != CaState::Recovery {
            // Entering recovery: send only as much as is acknowledged.
            self.bbr.use_packet_conservation = true;
            self.bbr.next_rtt_delivered = self.delivered;
            cwnd = inflight.saturating_add(acked);
        } else if prev_state >= CaState::Recovery && state == CaState::Open {
            // Leaving recovery: back to the window from before.
            cwnd = cwnd.max(self.bbr.prior_cwnd);
            self.bbr.use_packet_conservation = false;
        }
        self.bbr.prev_ca_state = state;

        if self.bbr.use_packet_conservation {
            return (true, cwnd.max(inflight.saturating_add(acked)));
        }
        (false, cwnd)
    }

    fn bbr_set_cwnd(&mut self, rs: &RateSample, acked: u32, bw: f64, gain: f64, inflight: u32) {
        let mut cwnd = self.cwnd;

        if acked > 0 {
            let (conserving, restored) = self.bbr_set_cwnd_to_recover_or_restore(rs, acked, inflight);
            cwnd = restored;

            if !conserving {
                let target_cwnd = self
                    .bbr_bdp(bw, gain)
                    .saturating_add(self.bbr_ack_aggregation_cwnd());

                if self.bbr.full_bw_reached {
                    cwnd = cwnd.saturating_add(acked).min(target_cwnd);
                } else if cwnd < target_cwnd || self.delivered < 10 {
                    cwnd = cwnd.saturating_add(acked);
                }
                cwnd = cwnd.max(BBR_CWND_MIN_TARGET);
            }
        }

        self.cwnd = cwnd;
        if self.bbr.mode == BbrMode::ProbeRtt {
            self.cwnd = self.cwnd.min(BBR_CWND_MIN_TARGET);
        }
    }

    fn bbr_is_next_cycle_phase(&self, rs: &RateSample, inflight: u32) -> bool {
        let elapsed_us = self.delivered_ts.saturating_sub(self.bbr.cycle_timestamp);
        let is_full_length = elapsed_us > u64::from(self.bbr.min_rtt_ms) * 1_000;

        if self.bbr.pacing_gain == 1.0 {
            return is_full_length;
        }

        let bw = self.bbr.max_bw();
        if self.bbr.pacing_gain > 1.0 {
            // Probing for more: stay until the probe has filled the path.
            return is_full_length
                && (rs.losses > 0 || inflight > self.bbr_bdp(bw, self.bbr.pacing_gain));
        }
        // Draining the probe: leave as soon as the queue is gone.
        is_full_length || inflight <= self.bbr_bdp(bw, 1.0)
    }

    fn bbr_advance_cycle_phase(&mut self) {
        self.bbr.cycle_index = (self.bbr.cycle_index + 1) % BBR_CYCLE_LEN;
        self.bbr.cycle_timestamp = self.delivered_ts;
    }

    fn bbr_update_cycle_phase(&mut self, rs: &RateSample, inflight: u32) {
        if self.bbr.mode == BbrMode::ProbeBw && self.bbr_is_next_cycle_phase(rs, inflight) {
            self.bbr_advance_cycle_phase();
        }
    }

    fn bbr_set_mode(&mut self, mode: BbrMode) {
        if self.bbr.mode != mode {
            tracing::debug!(from = ?self.bbr.mode, to = ?mode, "bbr: mode transition");
            self.bbr.mode = mode;
        }
    }

    fn bbr_reset_probe_bw_mode(&mut self) {
        self.bbr_set_mode(BbrMode::ProbeBw);
        // C starts every stream at the same point of the cycle, where the
        // Linux kernel picks one at random.
        self.bbr.cycle_index = 3;
        self.bbr_advance_cycle_phase();
    }

    fn bbr_reset_mode(&mut self) {
        if self.bbr.full_bw_reached {
            self.bbr_reset_probe_bw_mode();
        } else {
            self.bbr_set_mode(BbrMode::Startup);
        }
    }

    fn bbr_update_bw(&mut self, rs: &RateSample) {
        self.bbr.round_start = false;

        let (Some(delivered), Some(interval_us)) = (rs.delivered, rs.interval_us) else {
            return;
        };
        if interval_us == 0 {
            return;
        }

        // A round trip has passed once a packet sent after the last round
        // began is acknowledged.
        if seq_diff(rs.prior_delivered, self.bbr.next_rtt_delivered) >= 0 {
            self.bbr.next_rtt_delivered = self.delivered;
            self.bbr.rtt_count += 1;
            self.bbr.round_start = true;
            self.bbr.use_packet_conservation = false;
        }

        // Packets per millisecond.
        let bw = f64::from(delivered) * 1_000.0 / interval_us as f64;

        // A sample taken while the application held the stream back says
        // nothing about the path, unless it beats the estimate anyway.
        if !rs.is_app_limited || bw >= self.bbr.max_bw() {
            self.bbr.bw.apply_max(BBR_BW_FILTER_CYCLES, self.bbr.rtt_count, bw);
        }
    }

    /// Tracks how far acknowledgements run ahead of what the bandwidth
    /// estimate predicts.
    fn bbr_update_ack_aggregation(&mut self, rs: &RateSample) {
        let usable = rs.delivered.is_some() && rs.interval_us.is_some_and(|us| us > 0);
        if rs.acked_sacked == 0 || !usable {
            return;
        }

        if self.bbr.round_start {
            self.bbr.extra_acked_win_rtts = (self.bbr.extra_acked_win_rtts + 1).min(0xff);
            if self.bbr.extra_acked_win_rtts >= BBR_EXTRA_ACKED_WIN_RTTS {
                self.bbr.extra_acked_win_rtts = 0;
                self.bbr.extra_acked_win_index ^= 1;
                self.bbr.extra_acked[self.bbr.extra_acked_win_index] = 0;
            }
        }

        let epoch_us = self.delivered_ts.saturating_sub(self.bbr.ack_epoch_start);
        let mut expected_acked = (self.bbr.max_bw() * epoch_us as f64 / 1_000.0) as u32;

        // Start a new epoch when acknowledgements fall behind the estimate,
        // or when the count would overflow.
        let overflows =
            u64::from(self.bbr.ack_epoch_acked) + u64::from(rs.acked_sacked) >= u64::from(u32::MAX);
        if self.bbr.ack_epoch_acked <= expected_acked || overflows {
            self.bbr.ack_epoch_acked = 0;
            self.bbr.ack_epoch_start = self.delivered_ts;
            expected_acked = 0;
        }

        self.bbr.ack_epoch_acked += rs.acked_sacked;
        let extra_acked = (self.bbr.ack_epoch_acked - expected_acked).min(self.cwnd);
        let slot = &mut self.bbr.extra_acked[self.bbr.extra_acked_win_index];
        *slot = (*slot).max(extra_acked);
    }

    /// Decides that Startup has filled the path once the bandwidth estimate
    /// has gone three round trips without growing by a quarter.
    fn bbr_check_full_bw_reached(&mut self, rs: &RateSample) {
        if self.bbr.full_bw_reached || !self.bbr.round_start || rs.is_app_limited {
            return;
        }

        if self.bbr.max_bw() >= self.bbr.full_bw * BBR_FULL_BW_THRESH {
            self.bbr.full_bw = self.bbr.max_bw();
            self.bbr.full_bw_count = 0;
            return;
        }

        self.bbr.full_bw_count += 1;
        self.bbr.full_bw_reached = self.bbr.full_bw_count >= BBR_FULL_BW_COUNT;
    }

    fn bbr_check_drain(&mut self, inflight: u32) {
        if self.bbr.mode == BbrMode::Startup && self.bbr.full_bw_reached {
            // Drain the queue that Startup built up at the bottleneck.
            self.bbr_set_mode(BbrMode::Drain);
            self.ssthresh = self.bbr_bdp(self.bbr.max_bw(), 1.0);
        }

        if self.bbr.mode == BbrMode::Drain && inflight <= self.bbr_bdp(self.bbr.max_bw(), 1.0) {
            self.bbr_reset_probe_bw_mode();
        }
    }

    fn bbr_check_probe_rtt_done(&mut self, now_us: u64) {
        if self.bbr.probe_rtt_done_time.is_none_or(|done| now_us <= done) {
            return;
        }

        self.bbr.min_rtt_stamp = now_us;
        self.cwnd = self.cwnd.max(self.bbr.prior_cwnd);
        self.bbr_reset_mode();
    }

    /// Keeps the lowest round-trip time of the last ten seconds. When that
    /// estimate is older than ten seconds, ProbeRtt holds the window at four
    /// packets for 200 ms and a round trip, to measure it afresh.
    fn bbr_update_min_rtt(&mut self, rs: &RateSample, inflight: u32, now_us: u64) {
        let filter_expired = now_us > self.bbr.min_rtt_stamp + BBR_MIN_RTT_INTERVAL_MS * 1_000;

        if let Some(rtt_ms) = rs.rtt_ms {
            if rtt_ms < self.bbr.min_rtt_ms || filter_expired {
                // Round-trip times are whole milliseconds, so one shorter
                // than that counts as one.
                self.bbr.min_rtt_ms = rtt_ms.max(1);
                self.bbr.min_rtt_stamp = now_us;
            }
        }

        if filter_expired && !self.bbr.idle_restart && self.bbr.mode != BbrMode::ProbeRtt {
            self.bbr_set_mode(BbrMode::ProbeRtt);
            self.bbr_save_cwnd();
            self.bbr.probe_rtt_done_time = None;
        }

        if self.bbr.mode == BbrMode::ProbeRtt {
            // Rate samples taken at this window say nothing about the path.
            self.app_limited = self.delivered.wrapping_add(inflight).max(1);

            if self.bbr.probe_rtt_done_time.is_none() && inflight <= BBR_CWND_MIN_TARGET {
                self.bbr.probe_rtt_done_time = Some(now_us + BBR_MIN_PROBE_RTT_MODE_MS * 1_000);
                self.bbr.probe_rtt_round_done = false;
                self.bbr.next_rtt_delivered = self.delivered;
            } else if self.bbr.probe_rtt_done_time.is_some() {
                if self.bbr.round_start {
                    self.bbr.probe_rtt_round_done = true;
                }
                if self.bbr.probe_rtt_round_done {
                    self.bbr_check_probe_rtt_done(now_us);
                }
            }
        }

        if rs.delivered.is_some_and(|delivered| delivered > 0) {
            self.bbr.idle_restart = false;
        }
    }

    fn bbr_update_gains(&mut self) {
        let (pacing_gain, cwnd_gain) = match self.bbr.mode {
            BbrMode::Startup => (BBR_HIGH_GAIN, BBR_HIGH_GAIN),
            BbrMode::Drain => (BBR_DRAIN_GAIN, BBR_HIGH_GAIN),
            BbrMode::ProbeBw => (BBR_PACING_GAIN[self.bbr.cycle_index], BBR_CWND_GAIN),
            BbrMode::ProbeRtt => (1.0, 1.0),
        };
        self.bbr.pacing_gain = pacing_gain;
        self.bbr.cwnd_gain = cwnd_gain;
    }

    /// Updates the model, the pacing rate and the window from a rate sample.
    /// C: `bbr_main`.
    pub(super) fn bbr_main(&mut self, rs: &RateSample, inflight: u32, now_us: u64) {
        self.bbr_update_bw(rs);
        self.bbr_update_ack_aggregation(rs);
        self.bbr_update_cycle_phase(rs, inflight);
        self.bbr_check_full_bw_reached(rs);
        self.bbr_check_drain(inflight);
        self.bbr_update_min_rtt(rs, inflight, now_us);
        self.bbr_update_gains();

        let bw = self.bbr.max_bw();
        self.bbr_set_pacing_rate(bw, self.bbr.pacing_gain);
        self.bbr_set_cwnd(rs, rs.acked_sacked, bw, self.bbr.cwnd_gain, inflight);
    }

    /// Called when the retransmission timer fires. The bandwidth estimate is
    /// kept, and Startup may run again from it. C: `bbr_on_rto`.
    pub(super) fn bbr_on_rto(&mut self) {
        self.bbr.prev_ca_state = CaState::Loss;
        self.bbr.full_bw = 0.0;
        self.bbr.round_start = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: u32 = 1_180;

    /// A sample of `delivered` packets over `interval_ms`, from a packet
    /// sent when `prior_delivered` packets had been delivered.
    fn sample(delivered: u32, interval_ms: u64, prior_delivered: u32) -> RateSample {
        RateSample {
            prior_delivered,
            prior_timestamp: Some(0),
            delivered: Some(delivered),
            interval_us: Some(interval_ms * 1_000),
            rtt_ms: Some(20),
            acked_sacked: delivered,
            ..RateSample::default()
        }
    }

    /// Runs one round trip in which `delivered` packets arrive over 20 ms.
    fn round(c: &mut CongestionController, delivered: u32, now_ms: u64) {
        let prior = c.delivered;
        c.delivered += delivered;
        c.delivered_ts = now_ms * 1_000;
        c.bbr_main(&sample(delivered, 20, prior), 0, now_ms * 1_000);
    }

    #[test]
    fn a_new_stream_paces_its_initial_window_over_a_millisecond() {
        let c = CongestionController::new(MSS, 0);
        let expected = (10.0 * f64::from(MSS) * BBR_HIGH_GAIN * BBR_PACING_MARGIN_PERCENT) as u64;
        assert_eq!(c.pacing_bytes_per_ms, expected);
        assert_eq!(c.bbr.mode, BbrMode::Startup);
    }

    #[test]
    fn the_first_round_trip_time_resets_the_starting_rate() {
        let mut c = CongestionController::new(MSS, 0);
        c.srtt_ms = 50;
        c.app_limited = 0;
        // Too slow a sample to raise the rate by itself.
        round(&mut c, 1, 20);

        // The initial window of 10 packets, spread over the round-trip time.
        let from_rtt =
            (10.0 / 50.0 * f64::from(MSS) * BBR_HIGH_GAIN * BBR_PACING_MARGIN_PERCENT) as u64;
        assert_eq!(c.pacing_bytes_per_ms, from_rtt);
    }

    #[test]
    fn startup_only_raises_the_pacing_rate() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        round(&mut c, 4_000, 20);
        let fast = c.pacing_bytes_per_ms;
        assert!(fast > 600_000, "rate was {fast}");

        // A slower round does not bring the rate down before the path's
        // bandwidth is known.
        round(&mut c, 10, 40);
        assert_eq!(c.pacing_bytes_per_ms, fast);
    }

    #[test]
    fn the_pacing_rate_is_never_zero() {
        let c = CongestionController::new(MSS, 0);
        assert_eq!(c.bbr_bw_to_pacing_rate(0.0, BBR_HIGH_GAIN), 1);
    }

    #[test]
    fn three_rounds_without_growth_end_startup() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        let mut now_ms = 0;
        // The first round sets the level the next three fail to beat.
        for _ in 0..4 {
            now_ms += 20;
            assert!(!c.bbr.full_bw_reached);
            round(&mut c, 200, now_ms);
        }
        assert!(c.bbr.full_bw_reached);
        // Nothing is in flight, so the queue counts as drained at once.
        assert_eq!(c.bbr.mode, BbrMode::ProbeBw);
    }

    #[test]
    fn growing_bandwidth_keeps_startup_going() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        let mut delivered = 100;
        for step in 1..=6 {
            round(&mut c, delivered, step * 20);
            delivered *= 2;
        }
        assert!(!c.bbr.full_bw_reached);
        assert_eq!(c.bbr.mode, BbrMode::Startup);
    }

    #[test]
    fn app_limited_rounds_do_not_end_startup() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        for step in 1..=6 {
            let prior = c.delivered;
            c.delivered += 200;
            c.delivered_ts = step * 20_000;
            let mut rs = sample(200, 20, prior);
            rs.is_app_limited = true;
            c.bbr_main(&rs, 0, step * 20_000);
        }
        assert!(!c.bbr.full_bw_reached);
    }

    #[test]
    fn the_window_stops_at_its_target_in_startup() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        // 10 packets per millisecond over a 20 ms round trip.
        let target = (10.0 * 20.0 * BBR_HIGH_GAIN) as u32;
        for step in 1..=40 {
            round(&mut c, 200, step * 20);
            if c.bbr.full_bw_reached {
                break;
            }
            // One more acknowledgement's worth past the target at most.
            assert!(c.cwnd < target + 200, "window was {}", c.cwnd);
        }
    }

    #[test]
    fn a_round_trip_below_a_millisecond_counts_as_one() {
        let mut c = CongestionController::new(MSS, 0);
        let mut rs = sample(10, 1, 0);
        rs.rtt_ms = Some(0);
        c.delivered = 10;
        c.bbr_main(&rs, 0, 1_000);
        assert_eq!(c.bbr.min_rtt_ms, 1);
    }

    #[test]
    fn without_a_round_trip_time_the_path_is_taken_to_hold_ten_packets() {
        let c = CongestionController::new(MSS, 0);
        assert_eq!(c.bbr_bdp(500.0, 2.0), BDP_WITHOUT_RTT);
    }

    #[test]
    fn entering_recovery_holds_the_window_to_what_is_in_flight() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;
        round(&mut c, 200, 20);
        assert!(c.cwnd > 100);

        c.ca_state = CaState::Recovery;
        let prior = c.delivered;
        c.delivered += 5;
        c.delivered_ts = 40_000;
        // 30 packets in flight, 5 acknowledged. The packet was sent before
        // the round that began at recovery, so the round has not ended.
        c.bbr_main(&sample(5, 20, prior.wrapping_sub(1)), 30, 40_000);
        assert_eq!(c.cwnd, 35);
    }

    #[test]
    fn probe_rtt_holds_the_window_down_and_then_restores_it() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        let mut now_ms = 0;
        for _ in 0..4 {
            now_ms += 20;
            round(&mut c, 200, now_ms);
        }
        assert_eq!(c.bbr.mode, BbrMode::ProbeBw);
        let window = c.cwnd;
        assert!(window > BBR_CWND_MIN_TARGET);

        // Ten seconds after the lowest round-trip time was taken, with a
        // slower sample, the estimate is due for a fresh measurement.
        now_ms += BBR_MIN_RTT_INTERVAL_MS + 1;
        let prior = c.delivered;
        c.delivered += 1;
        c.delivered_ts = now_ms * 1_000;
        let mut rs = sample(1, 20, prior);
        rs.rtt_ms = Some(30);
        c.bbr_main(&rs, 2, now_ms * 1_000);
        assert_eq!(c.bbr.mode, BbrMode::ProbeRtt);
        assert_eq!(c.cwnd, BBR_CWND_MIN_TARGET);

        // After 200 ms and a round trip at that window, it is over.
        now_ms += BBR_MIN_PROBE_RTT_MODE_MS + 1;
        let prior = c.delivered;
        c.delivered += 1;
        c.delivered_ts = now_ms * 1_000;
        c.bbr_main(&sample(1, 20, prior), 2, now_ms * 1_000);
        assert_eq!(c.bbr.mode, BbrMode::ProbeBw);
        assert!(c.cwnd > BBR_CWND_MIN_TARGET);
    }

    #[test]
    fn a_retransmission_timeout_lets_startup_run_again() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;
        round(&mut c, 200, 20);
        assert!(c.bbr.full_bw > 0.0);

        c.on_rto(c.delivered);
        assert_eq!(c.ca_state, CaState::Loss);
        assert_eq!(c.bbr.full_bw, 0.0);
        assert_eq!(c.bbr.prev_ca_state, CaState::Loss);
    }
}
