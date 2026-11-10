//! Congestion control for a stream: BBR, delivery-rate sampling and pacing.
//!
//! A port of C libudx's `udx_bbr.c` and `udx_rate.c`, and of the pacing in
//! its `udx.c`. Round-trip times, rates and the pacer's ticks are in
//! milliseconds, as in the C code. Moments are microseconds: the stream
//! passes in the microseconds since it was created. The C code counts
//! moments in whole milliseconds as well; [`rate`] says why this port does
//! not.

pub(crate) mod bbr;
pub(crate) mod filter;
pub(crate) mod pacing;
pub(crate) mod rate;

/// Phases of BBR's bandwidth-probing cycle.
pub(crate) const BBR_CYCLE_LEN: usize = 8;
/// Round trips the bandwidth estimate looks back over.
pub(crate) const BBR_BW_FILTER_CYCLES: u64 = BBR_CYCLE_LEN as u64 + 2;
/// How long a lowest round-trip time stands before it is measured again, in
/// milliseconds.
pub(crate) const BBR_MIN_RTT_INTERVAL_MS: u64 = 10_000;
/// Shortest stay in ProbeRtt, in milliseconds.
pub(crate) const BBR_MIN_PROBE_RTT_MODE_MS: u64 = 200;
/// Share of the estimated bandwidth the pacer aims for.
pub(crate) const BBR_PACING_MARGIN_PERCENT: f64 = 0.99;
/// Gain that doubles the sending rate each round trip: 2/ln(2).
pub(crate) const BBR_HIGH_GAIN: f64 = 2.885_39;
/// Gain that drains the queue Startup built.
pub(crate) const BBR_DRAIN_GAIN: f64 = 1.0 / 2.885_39;
/// Window gain once the bandwidth is known.
pub(crate) const BBR_CWND_GAIN: f64 = 2.0;
/// Pacing gain of each phase of the probing cycle.
pub(crate) const BBR_PACING_GAIN: [f64; BBR_CYCLE_LEN] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
/// Smallest window BBR sets, in packets.
pub(crate) const BBR_CWND_MIN_TARGET: u32 = 4;
/// Growth of the bandwidth estimate that says Startup has more to find.
pub(crate) const BBR_FULL_BW_THRESH: f64 = 1.25;
/// Round trips without that growth after which the path counts as full.
pub(crate) const BBR_FULL_BW_COUNT: u32 = 3;
/// Round trips each slot of the aggregation estimate covers.
pub(crate) const BBR_EXTRA_ACKED_WIN_RTTS: u32 = 5;
/// Cap on the aggregation estimate: what the bandwidth delivers in this many
/// milliseconds.
pub(crate) const BBR_EXTRA_ACKED_MAX_MS: u64 = 100;

/// Signed distance from sequence number `b` to `a`, across wraparound.
/// C: `seq_diff`.
pub(crate) fn seq_diff(a: u32, b: u32) -> i32 {
    a.wrapping_sub(b) as i32
}

/// BBR's operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BbrMode {
    /// Doubling the sending rate each round trip to find the bandwidth.
    Startup,
    /// Draining the queue Startup built at the bottleneck.
    Drain,
    /// Steady state, cycling the pacing gain to probe for more bandwidth.
    ProbeBw,
    /// Holding the window down to measure the lowest round-trip time.
    ProbeRtt,
}

/// Where the stream is with respect to loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub(crate) enum CaState {
    /// No loss being repaired.
    Open = 1,
    /// Repairing loss found from acknowledgements.
    Recovery = 2,
    /// Repairing loss after the retransmission timer fired.
    Loss = 3,
}

/// What an incoming packet's acknowledgements came to, for [`CongestionController::on_ack`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct AckSummary {
    /// Packets newly acknowledged.
    pub(crate) delivered: u32,
    /// Packets newly marked lost.
    pub(crate) lost: u32,
    /// Packets still in flight.
    pub(crate) inflight: u32,
    /// The stream's smoothed round-trip time, 0 before the first sample.
    pub(crate) srtt_ms: u32,
    /// The stream's lowest round-trip time, `u32::MAX` before the first.
    pub(crate) rtt_min_ms: u32,
}

/// A stream's congestion control: how much it may have in flight, and how
/// fast it may send. C keeps this state on the stream itself.
#[derive(Debug, Clone)]
pub(crate) struct CongestionController {
    bbr: bbr::BbrState,
    pacing: pacing::TokenBucket,
    /// Packets acknowledged so far.
    pub(crate) delivered: u32,
    /// Packets marked lost so far.
    pub(crate) lost: u32,
    /// While the application limits the stream, the delivered count that
    /// ends the stretch. Zero when the network is the limit.
    app_limited: u32,
    /// When the newest acknowledged packet was sent, in microseconds.
    first_sent_ts: u64,
    /// When delivery last advanced, in microseconds.
    delivered_ts: u64,
    pub(crate) ca_state: CaState,
    /// The next sequence number when loss recovery began. Recovery is over
    /// once an acknowledgement passes it.
    pub(crate) high_seq: u32,
    /// Congestion window, in packets.
    pub(crate) cwnd: u32,
    /// The window a stream returns to after recovery, in packets.
    pub(crate) ssthresh: u32,
    /// Pacing rate, in bytes per millisecond.
    pub(crate) pacing_bytes_per_ms: u64,
    /// Largest payload of a packet, in bytes.
    mss: u32,
    /// Smoothed round-trip time, 0 before the first sample.
    srtt_ms: u32,
}

impl CongestionController {
    /// Initial congestion window, in packets. C: `udx_stream_init`.
    const INITIAL_CWND: u32 = 10;
    /// Initial slow-start threshold. C: `bbr_init`.
    const INITIAL_SSTHRESH: u32 = 0xffff;

    /// Creates the congestion control of a new stream.
    pub(crate) fn new(mss: u32, now_us: u64) -> Self {
        let mut controller = Self {
            bbr: bbr::BbrState::new(now_us),
            pacing: pacing::TokenBucket::new(now_us / 1_000),
            delivered: 0,
            lost: 0,
            // A new stream has not found a limit in the network yet.
            app_limited: u32::MAX,
            first_sent_ts: 0,
            delivered_ts: 0,
            ca_state: CaState::Open,
            high_seq: 0,
            cwnd: Self::INITIAL_CWND,
            ssthresh: Self::INITIAL_SSTHRESH,
            pacing_bytes_per_ms: 0,
            mss,
            srtt_ms: 0,
        };
        controller.bbr_init_pacing_rate_from_rtt();
        controller
    }

    /// Packets the stream may have in flight: the smaller of the congestion
    /// window and what the peer has room for. C: `send_window_in_packets`.
    pub(crate) fn send_window(&self, send_rwnd: u32) -> u32 {
        self.cwnd.min(send_rwnd / self.mss)
    }

    /// Whether a packet may be sent now: the pacer has tokens, and the window
    /// has room. C: `stream_may_send`.
    pub(crate) fn may_send(&mut self, inflight: u32, send_rwnd: u32, now_us: u64) -> bool {
        self.pacing.refill(now_us / 1_000, self.pacing_bytes_per_ms);
        self.pacing.has_tokens() && inflight < self.send_window(send_rwnd)
    }

    /// Takes a sent packet out of the pacer's tokens. Returns whether that
    /// used them up, so that the stream has to wait for a refill.
    pub(crate) fn on_bytes_sent(&mut self, bytes: usize) -> bool {
        self.pacing.consume(bytes as u64)
    }

    /// Updates the model, the pacing rate and the window once an incoming
    /// packet's acknowledgements have been processed. C: the end of
    /// `process_packet`, which calls `udx__rate_gen` and `bbr_main`.
    pub(crate) fn on_ack(&mut self, rs: &mut rate::RateSample, ack: AckSummary, now_us: u64) {
        self.srtt_ms = ack.srtt_ms;
        self.generate_rate_sample(rs, ack.delivered, ack.lost, ack.rtt_min_ms, now_us);
        self.bbr_main(rs, ack.inflight, now_us);

        tracing::trace!(
            mode = ?self.bbr.mode,
            cwnd = self.cwnd,
            pacing_bytes_per_ms = self.pacing_bytes_per_ms,
            delivered = self.delivered,
            inflight = ack.inflight,
            "bbr: on_ack update"
        );
    }

    /// Counts a packet as lost.
    pub(crate) fn on_packet_lost(&mut self) {
        self.lost = self.lost.wrapping_add(1);
    }

    /// Starts loss recovery, which lasts until everything sent so far is
    /// acknowledged. `next_seq` is the next sequence number to be sent.
    pub(crate) fn enter_recovery(&mut self, next_seq: u32) {
        self.ca_state = CaState::Recovery;
        self.high_seq = next_seq;
    }

    /// Ends loss recovery once an acknowledgement passes the point it began
    /// at. C: `process_packet`.
    pub(crate) fn on_ack_received(&mut self, ack: u32) {
        if self.ca_state != CaState::Open && seq_diff(ack, self.high_seq) > 0 {
            if self.ca_state == CaState::Recovery {
                self.cwnd = self.ssthresh;
            }
            self.ca_state = CaState::Open;
        }
    }

    /// Called when the retransmission timer fires. C: `udx_rto_timeout`.
    pub(crate) fn on_rto(&mut self, next_seq: u32) {
        self.high_seq = next_seq;
        self.ca_state = CaState::Loss;
        self.bbr_on_rto();
    }

    /// Updates the packet size after the path's MTU has been found larger.
    pub(crate) fn on_mtu_change(&mut self, old_mtu: usize, new_mtu: usize) {
        if old_mtu > 0 {
            self.cwnd = self.cwnd * new_mtu as u32 / old_mtu as u32;
        }
        self.mss = (new_mtu - crate::native::header::HEADER_SIZE) as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: u32 = 1_180;
    const RWND: u32 = 4_194_304;

    #[test]
    fn a_new_stream_starts_open_with_the_initial_window() {
        let c = CongestionController::new(MSS, 0);
        assert_eq!(c.ca_state, CaState::Open);
        assert_eq!(c.cwnd, CongestionController::INITIAL_CWND);
        assert_eq!(c.ssthresh, CongestionController::INITIAL_SSTHRESH);
        assert!(c.pacing_bytes_per_ms > 0);
    }

    #[test]
    fn the_window_limits_what_is_in_flight() {
        let mut c = CongestionController::new(MSS, 0);
        assert!(c.may_send(0, RWND, 0));
        assert!(c.may_send(c.cwnd - 1, RWND, 0));
        assert!(!c.may_send(c.cwnd, RWND, 0));
    }

    #[test]
    fn the_peer_window_limits_what_is_in_flight() {
        let mut c = CongestionController::new(MSS, 0);
        assert_eq!(c.send_window(3 * MSS), 3);
        assert!(c.may_send(2, 3 * MSS, 0));
        assert!(!c.may_send(3, 3 * MSS, 0));
        assert!(!c.may_send(0, 0, 0));
    }

    #[test]
    fn an_empty_pacer_stops_sending_until_the_next_millisecond() {
        let mut c = CongestionController::new(MSS, 0);
        let mut sent = 0;
        while !c.on_bytes_sent(1_200) {
            sent += 1;
            assert!(sent < 1_000, "the pacer never emptied");
        }
        assert!(!c.may_send(0, RWND, 0));
        assert!(!c.may_send(0, RWND, 999));
        assert!(c.may_send(0, RWND, 1_000));
    }

    #[test]
    fn recovery_ends_once_an_ack_passes_where_it_began() {
        let mut c = CongestionController::new(MSS, 0);
        c.ssthresh = 40;
        c.enter_recovery(100);
        assert_eq!(c.ca_state, CaState::Recovery);

        c.on_ack_received(100);
        assert_eq!(c.ca_state, CaState::Recovery);

        c.on_ack_received(101);
        assert_eq!(c.ca_state, CaState::Open);
        assert_eq!(c.cwnd, 40);
    }

    #[test]
    fn a_timeout_ends_like_recovery_but_keeps_the_window() {
        let mut c = CongestionController::new(MSS, 0);
        c.ssthresh = 40;
        c.on_rto(100);
        assert_eq!(c.ca_state, CaState::Loss);

        let window = c.cwnd;
        c.on_ack_received(101);
        assert_eq!(c.ca_state, CaState::Open);
        assert_eq!(c.cwnd, window);
    }

    #[test]
    fn an_acknowledged_round_grows_the_window_and_the_rate() {
        let mut c = CongestionController::new(MSS, 0);
        let state = rate::SendState {
            unacked_bytes: 64 * 1024,
            inflight: 0,
            retransmit_pending: false,
            all_acked: true,
        };
        let info = c.on_packet_sent(state, 0);
        let rate_before = c.pacing_bytes_per_ms;

        let mut rs = rate::RateSample {
            rtt_ms: Some(1),
            ..rate::RateSample::default()
        };
        let pkt = rate::DeliveredPacket {
            seq: 0,
            time_sent: 0,
            retransmitted: false,
            info,
        };
        c.on_packet_delivered(&mut rs, &pkt);
        let ack = AckSummary {
            delivered: 1,
            lost: 0,
            inflight: 0,
            srtt_ms: 1,
            rtt_min_ms: 1,
        };
        c.on_ack(&mut rs, ack, 1_000);

        assert_eq!(c.cwnd, CongestionController::INITIAL_CWND + 1);
        assert!(c.pacing_bytes_per_ms >= rate_before);
        assert_eq!(c.delivered, 1);
    }
}
