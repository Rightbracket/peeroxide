//! Delivery-rate sampling for BBR.
//!
//! Port of C libudx's `udx_rate.c`. A packet records the state of delivery
//! when it is sent. When it is acknowledged, the difference to the state
//! then says how many packets were delivered over how long.
//!
//! Moments are microseconds on the stream's clock, where the C code counts
//! whole milliseconds. Between two programs on one machine, or two machines
//! on one network, a round trip takes a fraction of a millisecond. Counted in
//! whole milliseconds, the interval of a sample taken there is zero, which
//! makes it unusable, or one, which makes it several times too low, and the
//! stream would be paced far below what the path carries.

use super::{seq_diff, CongestionController};

/// The state of delivery a packet records when it is sent.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PacketRateInfo {
    /// When the newest packet acknowledged by then had been sent, in
    /// microseconds.
    pub(crate) first_sent_ts: u64,
    /// When delivery had last advanced, in microseconds.
    pub(crate) delivered_ts: u64,
    /// Packets delivered by then.
    pub(crate) delivered: u32,
    /// Whether the application, not the network, was limiting the stream.
    pub(crate) is_app_limited: bool,
}

/// What the sender holds when it sends a packet. C reads these off the
/// stream in `udx__rate_pkt_sent` and `udx__rate_check_app_limited`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SendState {
    /// Bytes written by the application and not yet acknowledged.
    pub(crate) unacked_bytes: usize,
    /// Packets in flight, not counting the one being sent.
    pub(crate) inflight: u32,
    /// Whether a lost packet is waiting to be sent again.
    pub(crate) retransmit_pending: bool,
    /// Whether every packet sent so far has been acknowledged.
    pub(crate) all_acked: bool,
}

/// An acknowledged packet, as the rate sampler needs to see it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliveredPacket {
    pub(crate) seq: u32,
    /// When the packet was last sent, in microseconds.
    pub(crate) time_sent: u64,
    /// Whether it was sent more than once.
    pub(crate) retransmitted: bool,
    pub(crate) info: PacketRateInfo,
}

/// What the acknowledgements in one incoming packet say about delivery.
/// C: `udx_rate_sample_t`.
#[derive(Debug, Clone, Default)]
pub(crate) struct RateSample {
    /// Packets delivered when the newest acknowledged packet was sent.
    pub(crate) prior_delivered: u32,
    /// When delivery had last advanced before that packet was sent, in
    /// microseconds. `None` until a packet is acknowledged.
    pub(crate) prior_timestamp: Option<u64>,
    /// Packets delivered over the interval. `None` without a sample.
    pub(crate) delivered: Option<u32>,
    /// The interval the packets were delivered over, in microseconds. `None`
    /// when the sample cannot be used for a rate.
    pub(crate) interval_us: Option<u64>,
    /// Round-trip time of an acknowledged packet. `None` if none was taken.
    pub(crate) rtt_ms: Option<u32>,
    /// Packets newly marked lost.
    pub(crate) losses: u32,
    /// Packets newly acknowledged, cumulatively or selectively.
    pub(crate) acked_sacked: u32,
    pub(crate) is_app_limited: bool,
    pub(crate) is_retrans: bool,
    /// Sequence number of the newest acknowledged packet.
    pub(crate) seq: u32,
    /// How long the sender took to send what the sample covers, in
    /// microseconds.
    pub(crate) snd_interval_us: u64,
}

impl CongestionController {
    /// Records the state of delivery for a packet that is about to be sent.
    /// C: `udx__rate_pkt_sent`.
    pub(crate) fn on_packet_sent(&mut self, state: SendState, now_us: u64) -> PacketRateInfo {
        self.check_app_limited(state);

        if state.all_acked {
            // Nothing is in flight, so the intervals of this flight start now.
            self.first_sent_ts = now_us;
            self.delivered_ts = now_us;
        }

        PacketRateInfo {
            first_sent_ts: self.first_sent_ts,
            delivered_ts: self.delivered_ts,
            delivered: self.delivered,
            is_app_limited: self.app_limited != 0,
        }
    }

    /// Marks the stream as limited by the application when it has less than
    /// a packet of data to send and room to send it.
    /// C: `udx__rate_check_app_limited`.
    fn check_app_limited(&mut self, state: SendState) {
        if state.unacked_bytes < self.mss as usize
            && state.inflight < self.cwnd
            && !state.retransmit_pending
        {
            // Zero means "not limited", so the mark is never zero.
            self.app_limited = self.delivered.wrapping_add(state.inflight).max(1);
        }
    }

    /// Counts an acknowledged packet as delivered and, if it is the newest
    /// one acknowledged so far, takes the sample's starting point from it.
    /// C: `udx__rate_pkt_delivered`, and the `delivered++` of its caller.
    pub(crate) fn on_packet_delivered(&mut self, rs: &mut RateSample, pkt: &DeliveredPacket) {
        self.delivered = self.delivered.wrapping_add(1);

        let sent_after = pkt.time_sent > self.first_sent_ts
            || (pkt.time_sent == self.first_sent_ts && seq_diff(pkt.seq, rs.seq) > 0);

        if rs.prior_delivered == 0 || sent_after {
            rs.prior_delivered = pkt.info.delivered;
            rs.prior_timestamp = Some(pkt.info.delivered_ts);
            rs.is_app_limited = pkt.info.is_app_limited;
            rs.is_retrans = pkt.retransmitted;
            rs.seq = pkt.seq;

            // The send time of the newest acknowledged packet starts the
            // send interval of the packets sent from now on.
            self.first_sent_ts = pkt.time_sent;
            rs.snd_interval_us = pkt.time_sent.saturating_sub(pkt.info.first_sent_ts);
        }
    }

    /// Completes the sample once every acknowledgement in the incoming
    /// packet has been processed. C: `udx__rate_gen`.
    pub(super) fn generate_rate_sample(
        &mut self,
        rs: &mut RateSample,
        delivered: u32,
        lost: u32,
        rtt_min_ms: u32,
        now_us: u64,
    ) {
        // The application-limited stretch ends once everything that was in
        // flight during it has been acknowledged.
        if self.app_limited != 0 && seq_diff(self.delivered, self.app_limited) > 0 {
            self.app_limited = 0;
        }

        if delivered > 0 {
            self.delivered_ts = now_us;
        }

        rs.acked_sacked = delivered;
        rs.losses = lost;

        let Some(prior_timestamp) = rs.prior_timestamp else {
            rs.delivered = None;
            rs.interval_us = None;
            return;
        };

        rs.delivered = Some(self.delivered.wrapping_sub(rs.prior_delivered));

        // The longer of the time it took to send the packets and the time it
        // took to acknowledge them, so that neither a burst of sends nor a
        // burst of acknowledgements overstates the rate.
        let ack_us = now_us.saturating_sub(prior_timestamp);
        let interval_us = rs.snd_interval_us.max(ack_us);

        // An interval shorter than the lowest round-trip time cannot be a
        // real one.
        let rtt_min_us = u64::from(rtt_min_ms) * 1_000;
        rs.interval_us = (interval_us >= rtt_min_us).then_some(interval_us);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: u32 = 1_180;

    fn busy() -> SendState {
        SendState {
            unacked_bytes: 64 * 1024,
            inflight: 3,
            retransmit_pending: false,
            all_acked: false,
        }
    }

    fn delivered(seq: u32, time_sent: u64, info: PacketRateInfo) -> DeliveredPacket {
        DeliveredPacket {
            seq,
            time_sent,
            retransmitted: false,
            info,
        }
    }

    #[test]
    fn a_packet_records_the_state_of_delivery_when_it_is_sent() {
        let mut c = CongestionController::new(MSS, 0);
        let first = c.on_packet_sent(SendState { all_acked: true, ..busy() }, 7);
        assert_eq!(first.first_sent_ts, 7);
        assert_eq!(first.delivered_ts, 7);
        assert_eq!(first.delivered, 0);

        // A packet sent into a flight keeps the flight's starting times.
        let second = c.on_packet_sent(busy(), 9);
        assert_eq!(second.first_sent_ts, 7);
        assert_eq!(second.delivered_ts, 7);
    }

    #[test]
    fn a_stream_with_less_than_a_packet_to_send_is_app_limited() {
        let mut c = CongestionController::new(MSS, 0);
        c.app_limited = 0;

        let info = c.on_packet_sent(busy(), 1);
        assert!(!info.is_app_limited);

        let idle = SendState { unacked_bytes: 100, ..busy() };
        let info = c.on_packet_sent(idle, 2);
        assert!(info.is_app_limited);

        // Not while a lost packet is waiting, or with a full window.
        c.app_limited = 0;
        let resending = SendState { retransmit_pending: true, ..idle };
        assert!(!c.on_packet_sent(resending, 3).is_app_limited);
        let full = SendState { inflight: c.cwnd, ..idle };
        assert!(!c.on_packet_sent(full, 4).is_app_limited);
    }

    #[test]
    fn the_sample_starts_from_the_newest_acknowledged_packet() {
        let mut c = CongestionController::new(MSS, 0);
        let older = PacketRateInfo { first_sent_ts: 10, delivered_ts: 10, delivered: 4, is_app_limited: false };
        let newer = PacketRateInfo { first_sent_ts: 12, delivered_ts: 15, delivered: 6, is_app_limited: true };

        let mut rs = RateSample::default();
        c.on_packet_delivered(&mut rs, &delivered(8, 20, newer));
        c.on_packet_delivered(&mut rs, &delivered(7, 18, older));

        assert_eq!(rs.prior_delivered, 6);
        assert_eq!(rs.prior_timestamp, Some(15));
        assert!(rs.is_app_limited);
        assert_eq!(rs.seq, 8);
        assert_eq!(rs.snd_interval_us, 8);
        assert_eq!(c.delivered, 2);
    }

    #[test]
    fn the_interval_is_the_longer_of_sending_and_acknowledging() {
        let mut c = CongestionController::new(MSS, 0);
        c.delivered = 10;

        // Sent over 8 ms, acknowledged over 5 ms.
        let info = PacketRateInfo { first_sent_ts: 12_000, delivered_ts: 25_000, delivered: 4, is_app_limited: false };
        let mut rs = RateSample::default();
        c.on_packet_delivered(&mut rs, &delivered(8, 20_000, info));
        c.generate_rate_sample(&mut rs, 1, 0, 1, 30_000);

        assert_eq!(rs.delivered, Some(7));
        assert_eq!(rs.interval_us, Some(8_000));
        assert_eq!(rs.acked_sacked, 1);

        // Sent over 2 ms, acknowledged over 9 ms.
        let info = PacketRateInfo { first_sent_ts: 28_000, delivered_ts: 31_000, delivered: 11, is_app_limited: false };
        let mut rs = RateSample::default();
        c.on_packet_delivered(&mut rs, &delivered(9, 30_000, info));
        c.generate_rate_sample(&mut rs, 1, 0, 1, 40_000);

        assert_eq!(rs.interval_us, Some(9_000));
    }

    #[test]
    fn an_interval_below_a_millisecond_is_kept_to_the_microsecond() {
        let mut c = CongestionController::new(MSS, 0);
        c.delivered = 99;

        // 100 packets, sent over 300 microseconds and acknowledged over 400,
        // on a path whose round trip is below a millisecond.
        let info = PacketRateInfo { first_sent_ts: 5_000, delivered_ts: 5_250, delivered: 0, is_app_limited: false };
        let mut rs = RateSample::default();
        c.on_packet_delivered(&mut rs, &delivered(100, 5_300, info));
        c.generate_rate_sample(&mut rs, 1, 0, 0, 5_650);

        assert_eq!(rs.delivered, Some(100));
        assert_eq!(rs.interval_us, Some(400));
    }

    #[test]
    fn an_interval_shorter_than_the_lowest_round_trip_is_not_used() {
        let mut c = CongestionController::new(MSS, 0);
        // Three milliseconds, where no round trip has taken less than 50.
        let info = PacketRateInfo { first_sent_ts: 10_000, delivered_ts: 10_000, delivered: 1, is_app_limited: false };
        let mut rs = RateSample::default();
        c.on_packet_delivered(&mut rs, &delivered(1, 11_000, info));
        c.generate_rate_sample(&mut rs, 1, 0, 50, 13_000);

        assert_eq!(rs.interval_us, None);
        // The count is still there for what does not need a rate.
        assert!(rs.delivered.is_some());
    }

    #[test]
    fn nothing_acknowledged_makes_no_sample() {
        let mut c = CongestionController::new(MSS, 0);
        let mut rs = RateSample::default();
        c.generate_rate_sample(&mut rs, 0, 0, 1, 5);
        assert_eq!(rs.delivered, None);
        assert_eq!(rs.interval_us, None);
    }

    #[test]
    fn the_app_limited_mark_clears_once_its_flight_is_acknowledged() {
        let mut c = CongestionController::new(MSS, 0);
        c.delivered = 5;
        c.app_limited = 8;

        let mut rs = RateSample::default();
        c.generate_rate_sample(&mut rs, 0, 0, 1, 5);
        assert_ne!(c.app_limited, 0);

        c.delivered = 9;
        c.generate_rate_sample(&mut rs, 0, 0, 1, 6);
        assert_eq!(c.app_limited, 0);
    }
}
