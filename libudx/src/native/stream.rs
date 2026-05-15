use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use crate::error::{Result, UdxError};
use super::congestion::CaState;
use super::header::{
    Header, SackRange,
    FLAG_DATA, FLAG_END, FLAG_DESTROY, FLAG_SACK, FLAG_HEARTBEAT,
    HEADER_SIZE, encode_sack, decode_sack,
};

// ── Constants ────────────────────────────────────────────────────────────────

/// Default receive window size (4 MB, matches C libudx).
pub(crate) const DEFAULT_RWND: u32 = 4_194_304;

/// Maximum payload per UDP packet (1200 - 20 byte header).
pub(crate) const MAX_PAYLOAD: usize = 1200 - HEADER_SIZE;

/// Starting MTU (bytes) — matches C libudx `UDX_MTU_BASE`.
pub(crate) const MTU_BASE: usize = 1200;

/// Maximum MTU (bytes) — matches C libudx `UDX_MTU_MAX`.
pub(crate) const MTU_MAX: usize = 1500;

/// Probe size increment (bytes).
pub(crate) const MTU_STEP: usize = 32;

/// Failed probes before settling on current MTU.
pub(crate) const MTU_MAX_PROBES: u32 = 3;

/// Initial retransmission timeout (ms) — matches C libudx.
const INITIAL_RTO_MS: u32 = 1_000;

/// Maximum retransmission timeout (ms) — matches C libudx `UDX_RTO_MAX_MS`.
const MAX_RTO_MS: u32 = 30_000;

/// Maximum consecutive RTO timeouts before stream failure.
const MAX_RTO_TIMEOUTS: u32 = 6;

/// Heartbeat interval when stream is idle (ms).
const HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Maximum number of SACK ranges to include in an ACK packet.
const MAX_SACK_RANGES: usize = 50;

/// Maximum accepted RTT sample (ms) — prevents outlier corruption.
const MAX_RTT_MS: u32 = 30_000;

/// Far-future duration used to effectively disable a timer.
const FAR_FUTURE: std::time::Duration = std::time::Duration::from_secs(86_400);

// ── MTU Probing State ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MtuState {
    Base,
    Search,
    SearchComplete,
}

// ── Sequence number comparison ───────────────────────────────────────────────

/// Returns true if sequence number `a` is strictly after `b` (handles u32 wraparound).
#[inline]
fn seq_after(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b)) as i32 > 0
}

// ── Packet types ─────────────────────────────────────────────────────────────

pub(crate) struct IncomingPacket {
    pub data: Vec<u8>,
    #[allow(dead_code)]
    pub addr: SocketAddr,
}

pub(crate) enum StreamEvent {
    Data(Vec<u8>),
    End,
}

// ── Reliability types ────────────────────────────────────────────────────────

/// A sent packet stored in the outgoing buffer for potential retransmission.
struct SentPacket {
    /// Full wire-format packet bytes (header + payload).
    packet: Vec<u8>,
    /// When this packet was (last) sent — for RTT measurement on first send.
    time_sent: Instant,
    /// Number of times this packet has been retransmitted.
    retransmit_count: u32,
    /// Whether this packet has been selectively acknowledged via SACK.
    sacked: bool,
    /// Whether this packet was sent as an MTU probe (oversized via data_offset padding).
    is_mtu_probe: bool,
    /// Whether this packet has been detected as lost (SACK gap analysis).
    lost: bool,
    rate_info: Option<super::congestion::rate::PacketRateInfo>,
}

/// A received out-of-order packet buffered until gaps are filled.
struct RecvdPacket {
    payload: Vec<u8>,
    flags: u8,
}

struct QueuedPacket {
    packet: Vec<u8>,
    seq: u32,
    remote_addr: SocketAddr,
}

/// Tracks a pending write awaiting remote acknowledgement.
pub(crate) struct PendingWrite {
    pub(crate) ack_threshold: u32,
    pub(crate) tx: Option<oneshot::Sender<Result<()>>>,
}

/// Notification from the send path to the processor task.
pub(crate) enum StreamNotify {
    DataQueued,
    Shutdown,
}

type WriteSenders = Vec<Option<oneshot::Sender<Result<()>>>>;
type AckedRateInfo = Vec<(u32, super::congestion::rate::PacketRateInfo)>;
type StreamMap = Arc<Mutex<std::collections::HashMap<u32, mpsc::UnboundedSender<IncomingPacket>>>>;
type FirewallHook =
    Box<dyn FnOnce(&super::socket::UdxSocket, u16, std::net::IpAddr) -> bool + Send>;

// ── StreamInner ──────────────────────────────────────────────────────────────

pub(crate) struct StreamInner {
    // ── Connection identity ──
    pub(crate) remote_id: u32,
    pub(crate) remote_addr: Option<SocketAddr>,
    pub(crate) connected: bool,
    /// True once `prepare_end()` has been called (FIN queued).
    ended: bool,

    // ── Send state ──
    pub(crate) next_seq: u32,
    pub(crate) pending_writes: Vec<PendingWrite>,
    /// Sent packets awaiting ACK, keyed by sequence number.
    outgoing: BTreeMap<u32, SentPacket>,

    // ── Receive state ──
    pub(crate) next_remote_seq: u32,
    /// Highest cumulative ACK received from remote.
    remote_acked: u32,
    /// Out-of-order receive buffer, keyed by sequence number.
    recv_buf: BTreeMap<u32, RecvdPacket>,

    // ── RTT estimation (Jacobson/Karels, matching C libudx constants) ──
    srtt: u32,
    rttvar: u32,
    rto: u32,
    rto_timeouts: u32,

    // ── Flow control ──
    /// Remote's last advertised receive window.
    send_rwnd: u32,

    // ── MTU probing state ──
    pub(crate) mtu_state: MtuState,
    pub(crate) mtu: usize,
    pub(crate) mtu_probe_size: usize,
    pub(crate) mtu_probe_count: u32,
    pub(crate) mtu_probe_wanted: bool,
    pub(crate) mtu_max: usize,

    // ── IO ──
    pub(crate) udp: Option<Arc<tokio::net::UdpSocket>>,
    pub(crate) read_tx: mpsc::UnboundedSender<StreamEvent>,
    close_tx: Option<oneshot::Sender<()>>,
    pub(crate) notify_tx: Option<mpsc::UnboundedSender<StreamNotify>>,

    // ── Congestion control (BBR) ──
    pub(crate) congestion: super::congestion::CongestionController,
    send_queue: VecDeque<QueuedPacket>,

    // ── Relay ──
    relay_target: Option<Arc<Mutex<StreamInner>>>,

    // ── Firewall hook (set_firewall_hook path) ──
    /// Single-fire hook invoked on the first incoming packet when the stream
    /// starts in listening mode (via `set_firewall_hook`). On `true` the stream
    /// adopts the packet's source as `remote_addr` and transitions to connected.
    firewall_hook: Option<FirewallHook>,
    /// Retained socket clone supplied to `set_firewall_hook`; forwarded to the
    /// hook and cleared after the hook fires.
    firewall_socket: Option<super::socket::UdxSocket>,
}

impl StreamInner {
    // ── Stream termination ──────────────────────────────────────────────────

    /// Drain all pending writes with `StreamClosed`, mark the stream
    /// disconnected, and drop `notify_tx` so future sends fail fast.
    /// Idempotent — safe to call more than once.
    fn terminate(&mut self) {
        self.connected = false;
        self.notify_tx = None;

        for mut pw in self.pending_writes.drain(..) {
            if let Some(tx) = pw.tx.take() {
                let _ = tx.send(Err(UdxError::StreamClosed));
            }
        }
    }

    // ── MTU ──────────────────────────────────────────────────────────────────

    pub(crate) fn max_payload(&self) -> usize {
        self.mtu - HEADER_SIZE
    }

    /// Inflate a DATA packet to `mtu_probe_size` by inserting zero padding after the header.
    /// Sets `data_offset` so the receiver skips the padding via `payload_offset()`.
    /// Returns `true` if the packet was successfully probeified.
    fn mtu_probeify_packet(&mut self, packet: &mut Vec<u8>) -> bool {
        if packet.len() < HEADER_SIZE {
            return false;
        }
        let target_size = self.mtu_probe_size;
        if target_size <= packet.len() {
            return false;
        }

        let padding = target_size - packet.len();
        // data_offset is u8 — can only pad up to 255 bytes.
        // This naturally limits probing to nearly-full packets (matching C libudx).
        if padding > 255 {
            return false;
        }
        // Insert padding between header and payload: shift payload right
        let payload = packet[HEADER_SIZE..].to_vec();
        packet.truncate(HEADER_SIZE);
        packet.extend(std::iter::repeat_n(0u8, padding));
        packet.extend_from_slice(&payload);

        // Set data_offset in header byte [3]
        packet[3] = padding as u8;

        self.mtu_probe_wanted = false;
        tracing::trace!(
            probe_size = target_size,
            padding,
            "mtu: probeify"
        );
        true
    }

    fn on_mtu_probe_acked(&mut self) {
        let old_mtu = self.mtu;
        self.mtu = self.mtu_probe_size;
        self.mtu_probe_count = 0;
        self.mtu_probe_size += MTU_STEP;

        if self.mtu_probe_size > self.mtu_max {
            self.mtu_probe_size = self.mtu_max;
        }

        if self.mtu >= self.mtu_max {
            self.mtu_state = MtuState::SearchComplete;
        } else {
            self.mtu_probe_wanted = true;
        }

        self.congestion.on_mtu_change(old_mtu, self.mtu);
        tracing::debug!(old_mtu, new_mtu = self.mtu, state = ?self.mtu_state, "mtu: probe acked");
    }

    // ── Write preparation ────────────────────────────────────────────────────

    pub(crate) fn prepare_write(
        &mut self,
        data_len: usize,
    ) -> Result<WritePrep> {
        if !self.connected && self.firewall_hook.is_none() {
            return Err(UdxError::Io(std::io::Error::other("stream not connected")));
        }
        if self.ended {
            return Err(UdxError::Io(std::io::Error::other("stream already shut down")));
        }
        let udp = self.udp.clone().ok_or(UdxError::StreamClosed)?;
        // When the firewall hook hasn't fired yet, remote_addr is not known.
        // Use the unspecified sentinel (0.0.0.0:0); the processor will update
        // queued packets once the hook fires and remote_addr is resolved.
        let remote_addr = self
            .remote_addr
            .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        let remote_id = self.remote_id;

        let max_payload = self.max_payload();
        let chunk_count = if data_len == 0 {
            1
        } else {
            data_len.div_ceil(max_payload)
        };
        let first_seq = self.next_seq;
        let ack_threshold = first_seq + chunk_count as u32;
        self.next_seq = ack_threshold;
        let current_ack = self.next_remote_seq;

        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_writes.push(PendingWrite {
            ack_threshold,
            tx: Some(ack_tx),
        });

        Ok(WritePrep {
            udp,
            remote_addr,
            remote_id,
            first_seq,
            current_ack,
            ack_rx,
        })
    }

    pub(crate) fn prepare_end(&mut self) -> Result<WritePrep> {
        if !self.connected {
            return Err(UdxError::Io(std::io::Error::other("stream not connected")));
        }
        if self.ended {
            return Err(UdxError::Io(std::io::Error::other("stream already shut down")));
        }
        self.ended = true;
        let udp = self.udp.clone().ok_or(UdxError::StreamClosed)?;
        let remote_addr = self.remote_addr.ok_or(UdxError::StreamClosed)?;
        let remote_id = self.remote_id;

        let seq = self.next_seq;
        self.next_seq = seq + 1;
        let current_ack = self.next_remote_seq;

        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_writes.push(PendingWrite {
            ack_threshold: seq + 1,
            tx: Some(ack_tx),
        });

        Ok(WritePrep {
            udp,
            remote_addr,
            remote_id,
            first_seq: seq,
            current_ack,
            ack_rx,
        })
    }

    // ── Outgoing packet registration ─────────────────────────────────────────

    /// Store a sent packet in the outgoing buffer for potential retransmission.
    pub(crate) fn register_sent(&mut self, seq: u32, packet: Vec<u8>, is_mtu_probe: bool) {
        let now = Instant::now();
        let inflight = self.inflight();
        let rate_info = self.congestion.on_packet_sent(seq, inflight, now);
        tracing::trace!(seq, is_mtu_probe, "register_sent: packet placed in outgoing");
        self.outgoing.insert(seq, SentPacket {
            packet,
            time_sent: now,
            retransmit_count: 0,
            sacked: false,
            is_mtu_probe,
            lost: false,
            rate_info: Some(rate_info),
        });
    }

    pub(crate) fn inflight(&self) -> u32 {
        self.outgoing.values().filter(|p| !p.sacked && !p.lost).count() as u32
    }

    pub(crate) fn queue_for_send(&mut self, packets: Vec<(u32, Vec<u8>)>, remote_addr: SocketAddr) {
        let count = packets.len();
        for (seq, packet) in packets {
            self.send_queue.push_back(QueuedPacket { packet, seq, remote_addr });
        }
        tracing::trace!(count, queue_len = self.send_queue.len(), "queue_for_send: packets staged");
        if let Some(ref tx) = self.notify_tx {
            let _ = tx.send(StreamNotify::DataQueued);
        }
    }

    pub(crate) fn send_idle(&self) -> bool {
        self.outgoing.is_empty() && self.send_queue.is_empty()
    }

    // ── RTT estimation (Jacobson/Karels) ─────────────────────────────────────

    /// Update the RTT estimator with a new sample.
    ///
    /// Uses standard constants matching C libudx:
    ///   α = 1/8 (SRTT smoothing)
    ///   β = 1/4 (RTTVAR smoothing)
    ///   K = 4   (RTO multiplier for RTTVAR)
    ///   G = 1000ms (minimum RTO granularity)
    fn update_rtt(&mut self, rtt_ms: u32) {
        let rtt = rtt_ms.min(MAX_RTT_MS);
        if self.srtt == 0 {
            // First sample
            self.srtt = rtt;
            self.rttvar = rtt / 2;
        } else {
            let delta = self.srtt.abs_diff(rtt);
            // RTTVAR <- (3/4) * RTTVAR + (1/4) * |SRTT - R|
            self.rttvar = (3 * self.rttvar + delta) / 4;
            // SRTT <- (7/8) * SRTT + (1/8) * R
            self.srtt = (7 * self.srtt + rtt) / 8;
        }
        // RTO <- SRTT + max(G, K * RTTVAR) where K=4, G=1000ms
        self.rto = (self.srtt + 4 * self.rttvar).clamp(1_000, MAX_RTO_MS);
    }

    // ── ACK processing ───────────────────────────────────────────────────────

    /// Process a cumulative ACK from the remote peer.
    ///
    /// Removes ACKed packets from the outgoing buffer, measures RTT from the
    /// earliest non-retransmitted packet, resets the RTO timeout counter, and
    /// returns completed write senders plus rate info for congestion control.
    ///
    /// Congestion control is NOT called here — the caller must combine this
    /// rate info with SACK rate info and loss count, then call `congestion.on_ack`.
    fn on_cumulative_ack(
        &mut self,
        ack: u32,
    ) -> (WriteSenders, AckedRateInfo) {
        if !seq_after(ack, self.remote_acked) {
            return (Vec::new(), Vec::new());
        }

        self.remote_acked = ack;

        // Efficient BTreeMap split: remove all entries with key < ack
        let to_keep = self.outgoing.split_off(&ack);
        let removed = std::mem::replace(&mut self.outgoing, to_keep);

        let mut rtt_measured = false;
        let mut acked_rate_info: Vec<(u32, super::congestion::rate::PacketRateInfo)> = Vec::new();
        for (&seq, pkt) in &removed {
            if !rtt_measured && pkt.retransmit_count == 0 {
                let elapsed = pkt.time_sent.elapsed().as_millis() as u32;
                self.update_rtt(elapsed);
                rtt_measured = true;
            }
            if let Some(ref info) = pkt.rate_info {
                acked_rate_info.push((seq, *info));
            }
            if pkt.is_mtu_probe && self.mtu_state == MtuState::Search {
                self.on_mtu_probe_acked();
            }
        }

        // Successful ACK resets timeout counter
        self.rto_timeouts = 0;

        // Complete pending writes whose ack_threshold <= ack
        let mut completed = Vec::new();
        self.pending_writes.retain_mut(|pw| {
            if pw.ack_threshold <= ack {
                completed.push(pw.tx.take());
                false
            } else {
                true
            }
        });
        (completed, acked_rate_info)
    }

    // ── SACK processing ──────────────────────────────────────────────────────

    /// Process SACK ranges from the remote peer. Marks selectively acknowledged
    /// packets and returns rate info from newly sacked packets for congestion control.
    fn on_sack(&mut self, ranges: &[SackRange]) -> Vec<(u32, super::congestion::rate::PacketRateInfo)> {
        let mut sack_rate_info = Vec::new();
        for range in ranges {
            for seq in range.start..range.end {
                if let Some(pkt) = self.outgoing.get_mut(&seq) {
                    if !pkt.sacked {
                        pkt.sacked = true;
                        if let Some(ref info) = pkt.rate_info {
                            sack_rate_info.push((seq, *info));
                        }
                    }
                }
            }
        }
        sack_rate_info
    }

    // ── Loss detection ───────────────────────────────────────────────────────

    /// Detect lost packets via SACK gap analysis: any non-sacked, non-lost
    /// packet with a sequence number below the highest SACKed sequence is
    /// declared lost. Returns the number of newly detected losses.
    ///
    /// This is a simplified approximation of the C libudx RACK-based loss
    /// detection. It captures the essential behavior (packets below the
    /// highest SACK are considered lost) without the full timing state.
    fn detect_losses(&mut self) -> u32 {
        let highest_sacked = self
            .outgoing
            .iter()
            .filter(|(_, pkt)| pkt.sacked)
            .map(|(&seq, _)| seq)
            .max();

        let Some(highest_sacked) = highest_sacked else {
            return 0;
        };

        let lost_seqs: Vec<u32> = self
            .outgoing
            .iter()
            .filter(|&(&seq, pkt)| !pkt.sacked && !pkt.lost && seq_after(highest_sacked, seq))
            .map(|(&seq, _)| seq)
            .collect();

        for &seq in &lost_seqs {
            if let Some(pkt) = self.outgoing.get_mut(&seq) {
                pkt.lost = true;
            }
        }

        let was_open = self.congestion.ca_state == CaState::Open;

        for &seq in &lost_seqs {
            self.congestion.on_packet_lost(seq);
        }

        // Record the recovery boundary: when all packets up to high_seq
        // are ACK'd, we can exit Recovery → Open.
        if was_open && self.congestion.ca_state == CaState::Recovery {
            if let Some(&max_seq) = self.outgoing.keys().last() {
                self.congestion.high_seq = max_seq;
            }
        }

        lost_seqs.len() as u32
    }

    // ── Receive buffering ────────────────────────────────────────────────────

    /// Handle an incoming DATA or END packet.
    ///
    /// In-order packets are delivered immediately. Out-of-order packets are
    /// buffered. When gaps are filled, consecutive buffered packets are drained
    /// and delivered in order. Returns events to deliver to the application.
    ///
    /// A single packet may carry both FLAG_DATA and FLAG_END (C libudx combines
    /// the last data chunk with the end marker). In that case two events are
    /// produced: Data followed by End.
    fn receive_data(&mut self, seq: u32, payload: Vec<u8>, flags: u8) -> Vec<StreamEvent> {
        let mut events = Vec::new();

        // Duplicate or already-received packet — ignore
        if seq < self.next_remote_seq {
            return events;
        }

        if seq == self.next_remote_seq {
            // In-order: deliver directly
            push_events(&mut events, payload, flags);
            self.next_remote_seq = seq + 1;

            // Drain consecutive buffered packets
            while let Some(recvd) = self.recv_buf.remove(&self.next_remote_seq) {
                push_events(&mut events, recvd.payload, recvd.flags);
                self.next_remote_seq += 1;
            }
        } else {
            // Out-of-order: buffer for later delivery
            self.recv_buf.entry(seq).or_insert(RecvdPacket { payload, flags });
        }

        events
    }

    // ── SACK generation ──────────────────────────────────────────────────────

    /// Compute SACK ranges from the out-of-order receive buffer.
    /// Returns contiguous ranges of received sequence numbers that are
    /// beyond the cumulative ACK point.
    fn sack_ranges(&self) -> Vec<SackRange> {
        if self.recv_buf.is_empty() {
            return Vec::new();
        }

        let mut ranges = Vec::new();
        let mut iter = self.recv_buf.keys();

        if let Some(&first) = iter.next() {
            let mut start = first;
            let mut end = first + 1;

            for &seq in iter {
                if seq == end {
                    // Extend current range
                    end = seq + 1;
                } else {
                    // Gap found: close current range, start new one
                    ranges.push(SackRange { start, end });
                    if ranges.len() >= MAX_SACK_RANGES {
                        return ranges;
                    }
                    start = seq;
                    end = seq + 1;
                }
            }
            ranges.push(SackRange { start, end });
        }
        ranges
    }

    // ── Retransmission ───────────────────────────────────────────────────────

    /// Collect packets that need retransmission (not SACKed, still in outgoing).
    /// Updates retransmit_count and time_sent for each retransmitted packet.
    /// For MTU probes: strips padding, marks failure, and may transition to SearchComplete.
    fn packets_to_retransmit(&mut self) -> Vec<Vec<u8>> {
        let now = Instant::now();
        let mut retransmits = Vec::new();
        let mut probe_failed = false;
        for pkt in self.outgoing.values_mut() {
            if !pkt.sacked {
                if pkt.is_mtu_probe {
                    strip_probe_padding(&mut pkt.packet);
                    pkt.is_mtu_probe = false;
                    probe_failed = true;
                }
                retransmits.push(pkt.packet.clone());
                pkt.retransmit_count += 1;
                pkt.time_sent = now;
            }
        }
        if probe_failed {
            self.mtu_probe_count += 1;
            tracing::debug!(
                failures = self.mtu_probe_count,
                max = MTU_MAX_PROBES,
                "mtu: probe failed (retransmit)"
            );
            if self.mtu_probe_count >= MTU_MAX_PROBES {
                self.mtu_state = MtuState::SearchComplete;
                self.mtu_probe_wanted = false;
            } else {
                self.mtu_probe_wanted = true;
            }
        }
        retransmits
    }

    /// Fast retransmit: collect only packets marked as lost (SACK-detected).
    fn fast_retransmit(&mut self) -> Vec<Vec<u8>> {
        let now = Instant::now();
        let mut retransmits = Vec::new();
        let mut probe_failed = false;
        for pkt in self.outgoing.values_mut() {
            if pkt.lost && !pkt.sacked {
                if pkt.is_mtu_probe {
                    strip_probe_padding(&mut pkt.packet);
                    pkt.is_mtu_probe = false;
                    probe_failed = true;
                }
                retransmits.push(pkt.packet.clone());
                pkt.retransmit_count += 1;
                pkt.time_sent = now;
                pkt.lost = false;
            }
        }
        if probe_failed {
            self.mtu_probe_count += 1;
            tracing::debug!(
                failures = self.mtu_probe_count,
                max = MTU_MAX_PROBES,
                "mtu: probe failed (fast retransmit)"
            );
            if self.mtu_probe_count >= MTU_MAX_PROBES {
                self.mtu_state = MtuState::SearchComplete;
                self.mtu_probe_wanted = false;
            } else {
                self.mtu_probe_wanted = true;
            }
        }
        retransmits
    }
}

/// Strip MTU probe padding from a packet: remove zero bytes between header and payload,
/// reset data_offset to 0.
fn strip_probe_padding(packet: &mut Vec<u8>) {
    if packet.len() < HEADER_SIZE {
        return;
    }
    let data_offset = packet[3] as usize;
    if data_offset == 0 {
        return;
    }
    let payload_start = HEADER_SIZE + data_offset;
    if payload_start <= packet.len() {
        let payload = packet[payload_start..].to_vec();
        packet.truncate(HEADER_SIZE);
        packet.extend_from_slice(&payload);
    }
    packet[3] = 0;
}

/// Push stream events for a received packet. When both FLAG_DATA and FLAG_END
/// are set (combined packet), pushes Data then End.
fn push_events(events: &mut Vec<StreamEvent>, payload: Vec<u8>, flags: u8) {
    let has_data = flags & FLAG_DATA != 0;
    let has_end = flags & FLAG_END != 0;

    if has_data && !payload.is_empty() {
        events.push(StreamEvent::Data(payload));
    } else if has_data {
        // Empty data packet (e.g., keepalive DATA) — skip
    } else if !has_end {
        // Neither DATA nor END — shouldn't happen, but guard
        return;
    }

    if has_end {
        events.push(StreamEvent::End);
    }
}

// ── WritePrep ────────────────────────────────────────────────────────────────

pub(crate) struct WritePrep {
    pub udp: Arc<tokio::net::UdpSocket>,
    pub remote_addr: SocketAddr,
    pub remote_id: u32,
    pub first_seq: u32,
    pub current_ack: u32,
    pub ack_rx: oneshot::Receiver<Result<()>>,
}

// ── UdxStream ────────────────────────────────────────────────────────────────

/// A reliable, ordered, bidirectional byte stream over UDP.
///
/// Implements the UDX protocol with BBR congestion control, SACK-based
/// recovery, and RTT-driven retransmission. Multiple streams can share
/// a single [`super::socket::UdxSocket`] via stream ID multiplexing.
pub struct UdxStream {
    local_id: u32,
    pub(crate) inner: Arc<Mutex<StreamInner>>,
    processor: Mutex<Option<tokio::task::JoinHandle<()>>>,
    incoming_tx: Option<mpsc::UnboundedSender<IncomingPacket>>,
    incoming_rx: Mutex<Option<mpsc::UnboundedReceiver<IncomingPacket>>>,
    pub(crate) read_rx: Option<mpsc::UnboundedReceiver<StreamEvent>>,
    #[allow(dead_code)]
    close_rx: Option<oneshot::Receiver<()>>,
    socket_streams: Mutex<Option<StreamMap>>,
}

impl UdxStream {
    pub(crate) fn new(local_id: u32) -> Self {
        let (read_tx, read_rx) = mpsc::unbounded_channel();
        let (close_tx, close_rx) = oneshot::channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        let inner = StreamInner {
            remote_id: 0,
            remote_addr: None,
            connected: false,
            ended: false,
            next_seq: 0,
            pending_writes: Vec::new(),
            outgoing: BTreeMap::new(),
            next_remote_seq: 0,
            remote_acked: 0,
            recv_buf: BTreeMap::new(),
            srtt: 0,
            rttvar: 0,
            rto: INITIAL_RTO_MS,
            rto_timeouts: 0,
            send_rwnd: DEFAULT_RWND,
            mtu_state: MtuState::Base,
            mtu: MTU_BASE,
            mtu_probe_size: MTU_BASE + MTU_STEP,
            mtu_probe_count: 0,
            mtu_probe_wanted: false,
            mtu_max: MTU_MAX,
            udp: None,
            read_tx,
            close_tx: Some(close_tx),
            notify_tx: None,
            congestion: super::congestion::CongestionController::new(
                crate::native::stream::MAX_PAYLOAD as u32,
                INITIAL_RTO_MS,
            ),
            send_queue: VecDeque::new(),
            relay_target: None,
            firewall_hook: None,
            firewall_socket: None,
        };

        Self {
            local_id,
            inner: Arc::new(Mutex::new(inner)),
            processor: Mutex::new(None),
            incoming_tx: Some(incoming_tx),
            incoming_rx: Mutex::new(Some(incoming_rx)),
            read_rx: Some(read_rx),
            close_rx: Some(close_rx),
            socket_streams: Mutex::new(None),
        }
    }

    /// Consume this stream and return a tokio-compatible async I/O adapter.
    pub fn into_async_stream(mut self) -> super::async_stream::UdxAsyncStream {
        let processor = self
            .processor
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // Prevent Drop from deregistering — the processor task is still running
        self.socket_streams.lock().unwrap_or_else(|e| e.into_inner()).take();
        let Some(read_rx) = self.read_rx.take() else {
            return super::async_stream::UdxAsyncStream::new(
                Arc::clone(&self.inner),
                mpsc::unbounded_channel().1,
                None,
            );
        };
        let inner = Arc::clone(&self.inner);
        super::async_stream::UdxAsyncStream::new(inner, read_rx, processor)
    }

    /// Returns the current effective MTU (Maximum Transmission Unit) in bytes.
    pub fn effective_mtu(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.mtu
    }

    /// Connect to a remote stream identified by `remote_id` at `remote_addr`.
    pub async fn connect(
        &self,
        socket: &super::socket::UdxSocket,
        remote_id: u32,
        remote_addr: SocketAddr,
    ) -> Result<()> {
        let udp = socket.udp_arc()?;
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();

        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.remote_id = remote_id;
            inner.remote_addr = Some(remote_addr);
            inner.udp = Some(udp);
            inner.connected = true;
            inner.notify_tx = Some(notify_tx);
        }

        let tx = self
            .incoming_tx
            .as_ref()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("stream already consumed")))?
            .clone();
        socket.register_stream(self.local_id, tx)?;

        *self.socket_streams.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(socket.streams_ref());

        let incoming_rx = self
            .incoming_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("processor already started")))?;
        let inner = Arc::clone(&self.inner);
        let streams_for_cleanup = socket.streams_ref();
        let local_id_for_cleanup = self.local_id;
        let handle = tokio::spawn(async move {
            process_incoming(inner, incoming_rx, notify_rx).await;
            if let Ok(mut map) = streams_for_cleanup.lock() {
                map.remove(&local_id_for_cleanup);
            }
        });
        *self.processor.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);

        tracing::debug!(local_id = self.local_id, remote_id, ?remote_addr, "stream connected");
        Ok(())
    }

    /// Write `data` reliably. Resolves when the remote peer ACKs all packets.
    pub async fn write(&self, data: &[u8]) -> Result<()> {
        let prep = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.prepare_write(data.len())?
        };

        send_data_packets(
            &self.inner,
            &prep.udp,
            prep.remote_addr,
            prep.remote_id,
            prep.first_seq,
            prep.current_ack,
            data,
        )
        .await?;

        prep.ack_rx.await.map_err(|_| UdxError::StreamClosed)?
    }

    /// Read the next message. Returns `None` on EOF or stream close.
    pub async fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let rx = self.read_rx.as_mut().ok_or(UdxError::StreamClosed)?;
        match rx.recv().await {
            Some(StreamEvent::Data(d)) => Ok(Some(d)),
            Some(StreamEvent::End) => {
                self.read_rx = None;
                Ok(None)
            }
            None => {
                self.read_rx = None;
                Ok(None)
            }
        }
    }

    /// Send a write-end (FIN) and wait for the remote peer to ACK it.
    pub async fn shutdown(&self) -> Result<()> {
        let prep = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.prepare_end()?
        };

        let header = Header {
            type_flags: FLAG_END,
            data_offset: 0,
            remote_id: prep.remote_id,
            recv_window: DEFAULT_RWND,
            seq: prep.first_seq,
            ack: prep.current_ack,
        };
        let packet = header.encode();

        {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            guard.queue_for_send(vec![(prep.first_seq, packet.to_vec())], prep.remote_addr);
        }

        prep.ack_rx.await.map_err(|_| UdxError::StreamClosed)?
    }

    /// Set up packet-level relay forwarding to `destination`.
    ///
    /// Both streams must be on the same runtime. Relayed packets bypass
    /// congestion control — only the `remote_id` header field is rewritten.
    pub fn relay_to(&self, destination: &UdxStream) -> Result<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.relay_target = Some(Arc::clone(&destination.inner));
        tracing::debug!("relay_to configured");
        Ok(())
    }

    /// Register a single-fire firewall hook for deferred 4-tuple commitment.
    ///
    /// The stream is registered with `socket` for demux immediately (so incoming
    /// packets addressed to `self.local_id` are routed here), but `connected`
    /// stays `false` until the hook fires.  On the first incoming packet the hook
    /// is called with `(socket, src_port, src_ip)`; if it returns `true` the
    /// stream adopts that source as its remote address and becomes connected,
    /// then processes the packet normally.  If it returns `false` the processor
    /// exits and the stream is permanently closed.
    ///
    /// `remote_id` is the remote stream's local ID (known from the noise handshake
    /// payload) and is required for correct ACK construction.
    ///
    // NOTE: this hook is single-fire (`FnOnce`) to match the Node
    // Hyperswarm reference. A re-armable variant (`Fn`, fired on every
    // unknown-source packet) would let the stream re-bind its 4-tuple
    // mid-stream — useful for NAT rebinding (when a NAT's external
    // mapping for the local port changes during a long-lived connection,
    // typically after an idle period or under aggressive NAT timers)
    // and for follow-the-mobile-peer scenarios (a peer that roams
    // between networks/interfaces without tearing the stream down).
    // We don't ship that variant today — the reference doesn't, and
    // rebinding mid-stream has subtle interactions with congestion
    // control, RTT estimation, and the secret-stream cipher state. If
    // we ever need it, add a sibling `set_firewall_hook_rearmable` that
    // takes an `Fn` and re-arms after each fire; do NOT change this
    // method's signature.
    pub fn set_firewall_hook<F>(
        &self,
        socket: &super::socket::UdxSocket,
        remote_id: u32,
        hook: F,
    ) -> Result<()>
    where
        F: FnOnce(&super::socket::UdxSocket, u16, std::net::IpAddr) -> bool + Send + 'static,
    {
        let udp = socket.udp_arc()?;
        let (notify_tx, notify_rx) = mpsc::unbounded_channel();

        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.remote_id = remote_id;
            inner.udp = Some(udp);
            inner.notify_tx = Some(notify_tx);
            inner.firewall_hook = Some(Box::new(hook));
            inner.firewall_socket = Some(socket.clone());
        }

        let tx = self
            .incoming_tx
            .as_ref()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("stream already consumed")))?
            .clone();
        socket.register_stream(self.local_id, tx)?;

        *self.socket_streams.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(socket.streams_ref());

        let incoming_rx = self
            .incoming_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("processor already started")))?;
        let inner = Arc::clone(&self.inner);
        let streams_for_cleanup = socket.streams_ref();
        let local_id_for_cleanup = self.local_id;
        let handle = tokio::spawn(async move {
            process_incoming(inner, incoming_rx, notify_rx).await;
            if let Ok(mut map) = streams_for_cleanup.lock() {
                map.remove(&local_id_for_cleanup);
            }
        });
        *self.processor.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);

        tracing::debug!(local_id = self.local_id, remote_id, "stream listening (firewall hook set)");
        Ok(())
    }

    /// Destroy the stream, sending a DESTROY packet to the remote peer.
    pub async fn destroy(mut self) -> Result<()> {
        let destroy_info = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.connected {
                inner.udp.clone().zip(inner.remote_addr).map(|(udp, addr)| {
                    let header = Header {
                        type_flags: FLAG_DESTROY,
                        data_offset: 0,
                        remote_id: inner.remote_id,
                        recv_window: 0,
                        seq: inner.next_seq,
                        ack: inner.next_remote_seq,
                    };
                    (udp, addr, header)
                })
            } else {
                None
            }
        };

        if let Some((udp, addr, header)) = destroy_info {
            let _ = udp.send_to(&header.encode(), addr).await;
        }

        let processor_handle = self
            .processor
            .lock()
            .ok()
            .and_then(|mut g| g.take());
        if let Some(handle) = processor_handle {
            handle.abort();
            let _ = handle.await;
        }

        self.incoming_tx = None;

        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.terminate();
            if let Some(tx) = inner.close_tx.take() {
                let _ = tx.send(());
            }
        }

        if let Some(streams) = self.socket_streams.lock().unwrap_or_else(|e| e.into_inner()).take() {
            streams.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.local_id);
        }

        tracing::debug!(local_id = self.local_id, "stream destroyed");
        Ok(())
    }
}

impl Drop for UdxStream {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.processor.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
        if let Ok(mut guard) = self.socket_streams.lock() {
            if let Some(streams) = guard.take() {
                if let Ok(mut map) = streams.lock() {
                    map.remove(&self.local_id);
                }
            }
        }
    }
}

// ── Processor task (reliability engine) ──────────────────────────────────────

async fn process_incoming(
    inner: Arc<Mutex<StreamInner>>,
    mut incoming_rx: mpsc::UnboundedReceiver<IncomingPacket>,
    mut notify_rx: mpsc::UnboundedReceiver<StreamNotify>,
) {
    use tokio::time::{self, Duration, Instant as TokioInstant};

    let mut shutting_down = false;

    // RTO timer starts far in the future (no data in flight initially).
    let rto_sleep = time::sleep(FAR_FUTURE);
    tokio::pin!(rto_sleep);

    let pacing_sleep = time::sleep(FAR_FUTURE);
    tokio::pin!(pacing_sleep);

    let mut keepalive = time::interval(Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    keepalive.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // ── Incoming packet processing ───────────────────────────
            packet = incoming_rx.recv() => {
                let Some(packet) = packet else { break };

                // ── Relay fast-path ──────────────────────────────
                let relay_info = {
                    let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.relay_target.as_ref().map(|target| {
                        let tgt = target.lock().unwrap_or_else(|e| e.into_inner());
                        (tgt.remote_id, tgt.remote_addr, tgt.udp.clone())
                    })
                };

                if let Some((dest_remote_id, dest_addr, dest_udp)) = relay_info {
                    let mut fwd = packet.data;
                    if fwd.len() >= 8 {
                        fwd[4..8].copy_from_slice(&dest_remote_id.to_le_bytes());
                    }
                    if let (Some(udp), Some(addr)) = (dest_udp, dest_addr) {
                        let _ = udp.send_to(&fwd, addr).await;
                    }
                    if fwd.len() > 2 && fwd[2] & FLAG_DESTROY != 0 {
                        tracing::debug!("relay: DESTROY received, closing relay stream");
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(tx) = guard.close_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    continue;
                }

                // ── Firewall hook: single-fire 4-tuple commit ────────────
                // NOTE: this hook is single-fire (`FnOnce`) to match the Node
                // Hyperswarm reference. A re-armable variant (`Fn`, fired on every
                // unknown-source packet) would let the stream re-bind its 4-tuple
                // mid-stream — useful for NAT rebinding (when a NAT's external
                // mapping for the local port changes during a long-lived connection,
                // typically after an idle period or under aggressive NAT timers)
                // and for follow-the-mobile-peer scenarios (a peer that roams
                // between networks/interfaces without tearing the stream down).
                // We don't ship that variant today — the reference doesn't, and
                // rebinding mid-stream has subtle interactions with congestion
                // control, RTT estimation, and the secret-stream cipher state. If
                // we ever need it, add a sibling `set_firewall_hook_rearmable` that
                // takes an `Fn` and re-arms after each fire; do NOT change this
                // method's signature.
                {
                    let is_connected = inner.lock().unwrap_or_else(|e| e.into_inner()).connected;
                    if !is_connected {
                        let hook_and_socket = {
                            let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                            guard.firewall_hook.take().zip(guard.firewall_socket.take())
                        };
                        match hook_and_socket {
                            Some((hook, sock)) => {
                                if hook(&sock, packet.addr.port(), packet.addr.ip()) {
                                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                                    guard.remote_addr = Some(packet.addr);
                                    guard.connected = true;
                                    // Patch any packets queued before the hook fired
                                    // (they were staged with the 0.0.0.0:0 sentinel).
                                    let sentinel: std::net::SocketAddr =
                                        "0.0.0.0:0".parse().unwrap();
                                    for qp in &mut guard.send_queue {
                                        if qp.remote_addr == sentinel {
                                            qp.remote_addr = packet.addr;
                                        }
                                    }
                                    if let Some(ref tx) = guard.notify_tx {
                                        let _ = tx.send(StreamNotify::DataQueued);
                                    }
                                    tracing::debug!(addr = ?packet.addr, "firewall hook: accepted");
                                } else {
                                    tracing::debug!(addr = ?packet.addr, "firewall hook: rejected");
                                    break;
                                }
                            }
                            None => continue,
                        }
                    }
                }

                let header = match Header::decode(&packet.data) {
                    Ok(h) => h,
                    Err(_) => continue,
                };

                tracing::trace!(
                    flags = header.type_flags,
                    seq = header.seq,
                    ack = header.ack,
                    data_offset = header.data_offset,
                    "process_incoming: received packet"
                );

                // ── ACK + SACK processing ────────────────────────────
                let has_ack = header.ack > 0;
                let has_sack = header.has_flag(FLAG_SACK) && header.data_offset > 0;

                if has_ack || has_sack {
                    // Decode SACK ranges from wire format (outside lock)
                    let sack_ranges = if has_sack {
                        let sack_end = header.payload_offset();
                        if sack_end <= packet.data.len() {
                            decode_sack(&packet.data[HEADER_SIZE..sack_end]).ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let (completed, outgoing_empty, fast_retransmits, udp_for_retransmit, addr_for_retransmit) = {
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        guard.send_rwnd = header.recv_window;

                        let (write_completions, mut acked_rate_info) = if has_ack {
                            guard.on_cumulative_ack(header.ack)
                        } else {
                            (Vec::new(), Vec::new())
                        };

                        if let Some(ref ranges) = sack_ranges {
                            let sack_info = guard.on_sack(ranges);
                            acked_rate_info.extend(sack_info);
                        }

                        let lost_count = guard.detect_losses();

                        if !acked_rate_info.is_empty() || lost_count > 0 {
                            let inflight = guard.inflight();
                            let now = Instant::now();
                            guard.congestion.on_ack(&acked_rate_info, lost_count, inflight, now);
                        }

                        let retransmits = guard.fast_retransmit();
                        let udp = guard.udp.clone();
                        let addr = guard.remote_addr;

                        let empty = guard.outgoing.is_empty();
                        (write_completions, empty, retransmits, udp, addr)
                    };

                    // Notify completed writes (outside lock)
                    for tx in completed.into_iter().flatten() {
                        let _ = tx.send(Ok(()));
                    }

                    // Fast retransmit lost packets (outside lock)
                    if let (Some(udp), Some(addr)) = (udp_for_retransmit, addr_for_retransmit) {
                        for packet_bytes in fast_retransmits {
                            let udp = Arc::clone(&udp);
                            tokio::spawn(async move {
                                let _ = udp.send_to(&packet_bytes, addr).await;
                            });
                        }
                    }

                    // Reset RTO timer based on outgoing state
                    if outgoing_empty {
                        rto_sleep.as_mut().reset(TokioInstant::now() + FAR_FUTURE);
                    } else {
                        let rto = inner.lock().unwrap_or_else(|e| e.into_inner()).rto as u64;
                        rto_sleep.as_mut().reset(
                            TokioInstant::now() + Duration::from_millis(rto),
                        );
                    }

                    {
                        let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        if !guard.send_queue.is_empty() {
                            drop(guard);
                            pacing_sleep.as_mut().reset(TokioInstant::now());
                        }
                    }
                } else {
                    // No ACK/SACK — still update recv window
                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.send_rwnd = header.recv_window;
                }

                // ── DATA / END processing (unified — handles combined DATA+END packets) ──
                let is_data = header.has_flag(FLAG_DATA);
                let is_end = header.has_flag(FLAG_END);
                if is_data || is_end {
                    let payload_start = header.payload_offset();
                    let payload = if payload_start <= packet.data.len() {
                        packet.data[payload_start..].to_vec()
                    } else {
                        Vec::new()
                    };
                    let recv_flags = (if is_data { FLAG_DATA } else { 0 })
                        | (if is_end { FLAG_END } else { 0 });

                    let (events, read_tx, udp, remote_addr, remote_id, ack_val, sack_ranges) = {
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        let events = guard.receive_data(header.seq, payload, recv_flags);
                        let sacks = guard.sack_ranges();
                        let ack_val = guard.next_remote_seq;
                        (
                            events,
                            guard.read_tx.clone(),
                            guard.udp.clone(),
                            guard.remote_addr,
                            guard.remote_id,
                            ack_val,
                            sacks,
                        )
                    };
                    for event in events {
                        let _ = read_tx.send(event);
                    }
                    send_ack(&udp, remote_addr, remote_id, ack_val, &sack_ranges);
                }

                // ── DESTROY processing ───────────────────────────────
                if header.has_flag(FLAG_DESTROY) {
                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = guard.read_tx.send(StreamEvent::End);
                    if let Some(tx) = guard.close_tx.take() {
                        let _ = tx.send(());
                    }
                    break;
                }

                // ── HEARTBEAT response ───────────────────────────────
                if header.has_flag(FLAG_HEARTBEAT) {
                    let (udp, remote_addr, remote_id, ack_val) = {
                        let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        (
                            guard.udp.clone(),
                            guard.remote_addr,
                            guard.remote_id,
                            guard.next_remote_seq,
                        )
                    };
                    send_ack(&udp, remote_addr, remote_id, ack_val, &[]);
                }
            }

            Some(notify) = notify_rx.recv() => {
                match notify {
                    StreamNotify::DataQueued => {
                        pacing_sleep.as_mut().reset(TokioInstant::now());
                    }
                    StreamNotify::Shutdown => {
                        shutting_down = true;
                        pacing_sleep.as_mut().reset(TokioInstant::now());
                    }
                }
            }

            _ = &mut pacing_sleep => {
                let (packets_to_send, should_arm_rto, new_rto_ms) = {
                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    let mut batch: Vec<(Vec<u8>, SocketAddr)> = Vec::new();
                    let was_outgoing_empty = guard.outgoing.is_empty();

                    while !guard.send_queue.is_empty() {
                        let inflight = guard.inflight() + batch.len() as u32;
                        let queued_bytes = guard.send_queue.iter()
                            .map(|q| q.packet.len())
                            .sum::<usize>();
                        guard.congestion.check_app_limited(queued_bytes, inflight);

                        if !guard.congestion.can_send(inflight, guard.send_rwnd) {
                            break;
                        }

                        let Some(front) = guard.send_queue.front() else {
                            break;
                        };
                        let packet_len = front.packet.len() as u32;

                        let now = std::time::Instant::now();
                        let delay = guard.congestion.pacing_delay(packet_len, now);
                        if delay > std::time::Duration::ZERO {
                            pacing_sleep.as_mut().reset(TokioInstant::now() + delay);
                            break;
                        }

                        let Some(queued) = guard.send_queue.pop_front() else {
                            break;
                        };
                        let mut packet = queued.packet;

                        let is_data = packet.len() >= HEADER_SIZE && packet[2] & FLAG_DATA != 0;
                        if is_data && guard.mtu_state == MtuState::Base {
                            guard.mtu_state = MtuState::Search;
                            guard.mtu_probe_wanted = true;
                        }

                        let is_mtu_probe = if is_data
                            && guard.mtu_state == MtuState::Search
                            && guard.mtu_probe_wanted
                        {
                            guard.mtu_probeify_packet(&mut packet)
                        } else {
                            false
                        };

                        guard.register_sent(queued.seq, packet.clone(), is_mtu_probe);
                        guard.congestion.pacing.try_consume(
                            packet.len() as u32, std::time::Instant::now(),
                        );
                        batch.push((packet, queued.remote_addr));
                    }

                    let should_arm = was_outgoing_empty && !guard.outgoing.is_empty();
                    let rto = guard.rto as u64;
                    (batch, should_arm, rto)
                };

                let udp_clone = {
                    let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.udp.clone()
                };
                if let Some(udp) = udp_clone {
                    for (pkt, addr) in &packets_to_send {
                        let _ = udp.send_to(pkt, *addr).await;
                    }
                }

                if should_arm_rto {
                    rto_sleep.as_mut().reset(
                        TokioInstant::now() + Duration::from_millis(new_rto_ms),
                    );
                }

                let has_more = !inner.lock().unwrap_or_else(|e| e.into_inner()).send_queue.is_empty();
                if shutting_down && !has_more {
                    break;
                }
                if has_more {
                    pacing_sleep.as_mut().reset(TokioInstant::now() + Duration::from_millis(1));
                } else {
                    pacing_sleep.as_mut().reset(TokioInstant::now() + FAR_FUTURE);
                }
            }

            _ = &mut rto_sleep => {
                let (retransmits, udp, remote_addr, new_rto, should_close) = {
                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());

                    if guard.outgoing.is_empty() {
                        // False alarm — nothing to retransmit
                        rto_sleep.as_mut().reset(TokioInstant::now() + FAR_FUTURE);
                        continue;
                    }

                    guard.rto_timeouts += 1;

                    if guard.rto_timeouts >= MAX_RTO_TIMEOUTS {
                        // Exceeded max timeouts — fail the stream
                        (Vec::new(), None, None, 0u64, true)
                    } else {
                        guard.congestion.on_rto();
                        if let Some(&max_seq) = guard.outgoing.keys().last() {
                            guard.congestion.high_seq = max_seq;
                        }
                        guard.rto = (guard.rto * 2).min(MAX_RTO_MS);
                        let retransmits = guard.packets_to_retransmit();
                        let rto = guard.rto as u64;
                        (retransmits, guard.udp.clone(), guard.remote_addr, rto, false)
                    }
                };

                if should_close {
                    // Fail all pending writes with timeout error
                    let pending: Vec<_> = {
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        guard.pending_writes.drain(..).collect()
                    };
                    for mut pw in pending {
                        if let Some(tx) = pw.tx.take() {
                            let _ = tx.send(Err(UdxError::Io(
                                std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "RTO timeout exceeded",
                                ),
                            )));
                        }
                    }
                    tracing::warn!("stream failed: exceeded max RTO timeouts");
                    break;
                }

                // Retransmit unACKed/unSACKed packets
                if let (Some(udp), Some(addr)) = (udp, remote_addr) {
                    tracing::debug!(
                        count = retransmits.len(),
                        rto_ms = new_rto,
                        "retransmitting packets"
                    );
                    for packet_bytes in retransmits {
                        let udp = Arc::clone(&udp);
                        tokio::spawn(async move {
                            let _ = udp.send_to(&packet_bytes, addr).await;
                        });
                    }
                }

                // Schedule next RTO check
                rto_sleep.as_mut().reset(
                    TokioInstant::now() + Duration::from_millis(new_rto),
                );
            }

            // ── Keepalive heartbeat ──────────────────────────────────
            _ = keepalive.tick() => {
                let (should_send, udp, addr, remote_id, ack_val) = {
                    let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.connected && guard.outgoing.is_empty() && guard.relay_target.is_none() {
                        (
                            true,
                            guard.udp.clone(),
                            guard.remote_addr,
                            guard.remote_id,
                            guard.next_remote_seq,
                        )
                    } else {
                        (false, None, None, 0, 0)
                    }
                };

                if should_send {
                    if let (Some(udp), Some(addr)) = (udp, addr) {
                        let header = Header {
                            type_flags: FLAG_HEARTBEAT,
                            data_offset: 0,
                            remote_id,
                            recv_window: DEFAULT_RWND,
                            seq: 0,
                            ack: ack_val,
                        };
                        let packet = header.encode();
                        tokio::spawn(async move {
                            let _ = udp.send_to(&packet, addr).await;
                        });
                    }
                }
            }
        }
    }

    inner.lock().unwrap_or_else(|e| e.into_inner()).terminate();
}

// ── Packet building ──────────────────────────────────────────────────────────

/// Build a wire-format DATA packet (header + payload).
pub(crate) fn build_data_packet(
    remote_id: u32,
    seq: u32,
    ack: u32,
    payload: &[u8],
) -> Vec<u8> {
    let header = Header {
        type_flags: FLAG_DATA,
        data_offset: 0,
        remote_id,
        recv_window: DEFAULT_RWND,
        seq,
        ack,
    };
    let mut packet = Vec::with_capacity(HEADER_SIZE + payload.len());
    packet.extend_from_slice(&header.encode());
    packet.extend_from_slice(payload);
    packet
}

// ── send_data_packets ────────────────────────────────────────────────────────

/// Build, register, and send DATA packets for a write.
///
/// Packets are stored in the outgoing buffer *before* being sent on the wire,
/// ensuring the retransmission engine can resend them if ACKs don't arrive.
pub(crate) async fn send_data_packets(
    inner: &Arc<Mutex<StreamInner>>,
    _udp: &tokio::net::UdpSocket,
    remote_addr: SocketAddr,
    remote_id: u32,
    first_seq: u32,
    current_ack: u32,
    data: &[u8],
) -> Result<()> {
    let max_payload = inner.lock().unwrap_or_else(|e| e.into_inner()).max_payload();
    let chunks: Vec<&[u8]> = if data.is_empty() {
        vec![&[]]
    } else {
        data.chunks(max_payload).collect()
    };

    let mut packets: Vec<(u32, Vec<u8>)> = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        let seq = first_seq + i as u32;
        packets.push((seq, build_data_packet(remote_id, seq, current_ack, chunk)));
    }

    {
        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
        if guard.outgoing.is_empty() && guard.send_queue.is_empty() {
            guard.congestion.on_transmit_start(std::time::Instant::now());
        }
        guard.queue_for_send(packets, remote_addr);
    }
    Ok(())
}

// ── send_ack ─────────────────────────────────────────────────────────────────

/// Send an ACK packet, optionally including SACK ranges.
///
/// When `sack_ranges` is non-empty, the packet includes FLAG_SACK with
/// the SACK data encoded after the header (using `data_offset` to indicate
/// the extra bytes).
fn send_ack(
    udp: &Option<Arc<tokio::net::UdpSocket>>,
    remote_addr: Option<SocketAddr>,
    remote_id: u32,
    ack: u32,
    sack_ranges: &[SackRange],
) {
    if let (Some(udp), Some(addr)) = (udp.as_ref(), remote_addr) {
        let udp = Arc::clone(udp);

        if sack_ranges.is_empty() {
            // Pure ACK — just the 20-byte header
            let header = Header {
                type_flags: 0,
                data_offset: 0,
                remote_id,
                recv_window: DEFAULT_RWND,
                seq: 0,
                ack,
            };
            let packet = header.encode();
            tokio::spawn(async move {
                let _ = udp.send_to(&packet, addr).await;
            });
        } else {
            // ACK with SACK ranges
            let sack_byte_len = sack_ranges.len() * 8;
            let header = Header {
                type_flags: FLAG_SACK,
                data_offset: sack_byte_len as u8,
                remote_id,
                recv_window: DEFAULT_RWND,
                seq: 0,
                ack,
            };

            let mut packet = Vec::with_capacity(HEADER_SIZE + sack_byte_len);
            let header_bytes = header.encode();
            packet.extend_from_slice(&header_bytes);
            let mut sack_buf = vec![0u8; sack_byte_len];
            encode_sack(sack_ranges, &mut sack_buf);
            packet.extend_from_slice(&sack_buf);

            tokio::spawn(async move {
                let _ = udp.send_to(&packet, addr).await;
            });
        }
    }
}

#[cfg(test)]
mod mtu_tests {
    use super::*;

    fn make_stream_inner() -> StreamInner {
        let (read_tx, _read_rx) = mpsc::unbounded_channel();
        StreamInner {
            remote_id: 0,
            remote_addr: None,
            connected: false,
            next_seq: 0,
            pending_writes: Vec::new(),
            outgoing: BTreeMap::new(),
            next_remote_seq: 0,
            remote_acked: 0,
            recv_buf: BTreeMap::new(),
            srtt: 0,
            rttvar: 0,
            rto: INITIAL_RTO_MS,
            rto_timeouts: 0,
            send_rwnd: DEFAULT_RWND,
            mtu_state: MtuState::Base,
            mtu: MTU_BASE,
            mtu_probe_size: MTU_BASE + MTU_STEP,
            mtu_probe_count: 0,
            mtu_probe_wanted: false,
            mtu_max: MTU_MAX,
            udp: None,
            read_tx,
            close_tx: None,
            notify_tx: None,
            congestion: crate::native::congestion::CongestionController::new(
                MAX_PAYLOAD as u32,
                INITIAL_RTO_MS,
            ),
            send_queue: VecDeque::new(),
            relay_target: None,
            ended: false,
            firewall_hook: None,
            firewall_socket: None,
        }
    }

    // T3: MTU state starts at BASE
    #[test]
    fn mtu_initial_state_is_base() {
        let inner = make_stream_inner();
        assert_eq!(inner.mtu_state, MtuState::Base);
        assert_eq!(inner.mtu, MTU_BASE);
        assert!(!inner.mtu_probe_wanted);
        assert_eq!(inner.mtu_probe_count, 0);
        assert_eq!(inner.mtu_probe_size, MTU_BASE + MTU_STEP);
        assert_eq!(inner.mtu_max, MTU_MAX);
    }

    // T5: MTU probe failure falls back gracefully — retransmit strips padding and increments failure count
    #[test]
    fn mtu_probe_failure_fallback() {
        let mut inner = make_stream_inner();
        inner.mtu_state = MtuState::Search;
        inner.mtu = MTU_BASE;

        let payload = vec![0xABu8; MTU_BASE - HEADER_SIZE];
        let packet = build_data_packet(0, 0, 0, &payload);
        inner.outgoing.insert(0, SentPacket {
            packet,
            time_sent: Instant::now(),
            retransmit_count: 0,
            sacked: false,
            is_mtu_probe: true,
            lost: false,
            rate_info: None,
        });

        let retransmits = inner.packets_to_retransmit();
        assert_eq!(retransmits.len(), 1);
        assert_eq!(inner.mtu_probe_count, 1);
        assert!(!inner.outgoing[&0].is_mtu_probe, "probe flag should be cleared after retransmit");
        assert_eq!(inner.outgoing[&0].packet[3], 0, "data_offset should be reset");
        assert_eq!(inner.mtu_state, MtuState::Search, "still searching after 1 failure");
        assert!(inner.mtu_probe_wanted, "should want another probe after non-terminal failure");
    }

    // T6: Probe uses data_offset padding — probeify inflates packet to probe size
    #[test]
    fn mtu_probe_uses_data_offset_padding() {
        let mut inner = make_stream_inner();
        inner.mtu_state = MtuState::Search;
        inner.mtu_probe_size = MTU_BASE + MTU_STEP;

        let payload = vec![0xCDu8; MTU_BASE - HEADER_SIZE];
        let mut packet = build_data_packet(0, 0, 0, &payload);
        let original_len = packet.len();
        assert_eq!(original_len, MTU_BASE);

        let ok = inner.mtu_probeify_packet(&mut packet);
        assert!(ok, "probeify should succeed for nearly-full packet");
        assert_eq!(packet.len(), MTU_BASE + MTU_STEP, "packet should be inflated to probe size");
        assert_eq!(packet[3] as usize, MTU_STEP, "data_offset should equal padding size");

        let payload_start = HEADER_SIZE + MTU_STEP;
        assert_eq!(&packet[payload_start..], &payload[..], "original payload should be intact after padding");
    }

    // T7: Probe ACK advances MTU and scales congestion window
    #[test]
    fn mtu_probe_ack_advances_mtu() {
        let mut inner = make_stream_inner();
        inner.mtu_state = MtuState::Search;
        inner.mtu = MTU_BASE;
        inner.mtu_probe_size = MTU_BASE + MTU_STEP;

        inner.on_mtu_probe_acked();

        assert_eq!(inner.mtu, MTU_BASE + MTU_STEP);
        assert_eq!(inner.mtu_probe_count, 0);
        assert_eq!(inner.mtu_probe_size, MTU_BASE + 2 * MTU_STEP);
        assert!(inner.mtu_probe_wanted, "should want next probe");
        assert_eq!(inner.mtu_state, MtuState::Search, "still searching — haven't reached max");
    }

    // T8: MTU_MAX_PROBES consecutive failures → SearchComplete
    #[test]
    fn mtu_max_probe_failures_completes_search() {
        let mut inner = make_stream_inner();
        inner.mtu_state = MtuState::Search;
        inner.mtu = MTU_BASE + MTU_STEP;

        for i in 0..MTU_MAX_PROBES {
            let payload = vec![0xEFu8; MTU_BASE - HEADER_SIZE];
            let packet = build_data_packet(0, i, 0, &payload);
            inner.outgoing.insert(i, SentPacket {
                packet,
                time_sent: Instant::now(),
                retransmit_count: 0,
                sacked: false,
                is_mtu_probe: true,
                lost: false,
                rate_info: None,
            });

            let _ = inner.packets_to_retransmit();
        }

        assert_eq!(inner.mtu_state, MtuState::SearchComplete);
        assert!(!inner.mtu_probe_wanted);
        assert_eq!(inner.mtu, MTU_BASE + MTU_STEP, "MTU frozen at last successful value");
    }
}
