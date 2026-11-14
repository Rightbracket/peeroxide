use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::error::{Result, UdxError};
use super::congestion::filter::WindowedFilter;
use super::congestion::pacing::REFILL_INTERVAL_MS;
use super::congestion::rate::{DeliveredPacket, PacketRateInfo, RateSample, SendState};
use super::congestion::{AckSummary, CaState, CongestionController};
use super::header::{
    Header, SackRange,
    FLAG_DATA, FLAG_END, FLAG_DESTROY, FLAG_SACK, FLAG_HEARTBEAT,
    HEADER_SIZE, encode_sack, decode_sack,
};

// ── Constants ────────────────────────────────────────────────────────────────

/// Default receive window size (4 MB, matches C libudx).
pub(crate) const DEFAULT_RWND: u32 = 4_194_304;

/// What IPv4 and UDP put in front of a UDX packet, in bytes.
const IPV4_UDP_OVERHEAD: usize = 20 + 8;

/// What IPv6 and UDP put in front of a UDX packet, in bytes.
const IPV6_UDP_OVERHEAD: usize = 40 + 8;

/// Payload of a packet at the starting MTU over IPv4. A stream reckons with
/// it until it is connected and knows which protocol carries it.
pub(crate) const MAX_PAYLOAD: usize = MTU_BASE - IPV4_UDP_OVERHEAD - HEADER_SIZE;

/// Starting MTU (bytes) — matches C libudx `UDX_MTU_BASE`. An MTU is the
/// size of a whole IP packet, as in C: the IP and UDP headers, the UDX
/// header and the payload.
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

/// How often the retransmission timer may fire for one packet before the
/// stream fails — matches C libudx `UDX_MAX_RTO_TIMEOUTS`.
const MAX_RTO_TIMEOUTS: u32 = 6;

/// Heartbeat interval when stream is idle (ms).
const HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Bytes a SACK range takes in an acknowledgement: where it starts and where
/// it ends.
const SACK_RANGE_SIZE: usize = 8;

/// Maximum accepted RTT sample (ms) — prevents outlier corruption.
const MAX_RTT_MS: u32 = 30_000;

/// Far-future duration used to effectively disable a timer.
const FAR_FUTURE: Duration = Duration::from_secs(86_400);

/// Window the lowest round-trip time is taken over (ms) — matches C libudx
/// `UDX_RTT_MIN_WINDOW_MS`.
const RTT_MIN_WINDOW_MS: u64 = 300_000;

/// Wait before a tail loss probe while no round-trip time is known (ms).
const DEFAULT_PTO_MS: u64 = 1_000;

/// Allowance for the peer delaying its acknowledgement of a lone packet
/// (ms) — matches C libudx `UDX_TLP_MAX_ACK_DELAY`.
const TLP_MAX_ACK_DELAY_MS: u64 = 2;

/// Most packets the processor puts on the wire before it looks at incoming
/// packets again.
const MAX_SEND_BATCH: usize = 64;

/// Most incoming packets a task takes in before it sends what it owes: one
/// acknowledgement for all of them, and whatever they released.
pub(crate) const MAX_RECV_BATCH: usize = 256;

/// How much may be written and not yet acknowledged, on top of the send
/// window, before a stream asks its writer to wait.
/// C: `UDX_HIGH_WATERMARK`.
pub(crate) const HIGH_WATERMARK: usize = 262_144;

/// How long a stream whose owner has let go of it goes on delivering what
/// was written, counted from the last acknowledgement that got it further.
const LINGER_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Decide whether a UDX packet is plausible as the *first* arrival on an
/// unconnected stream awaiting its firewall-hook 4-tuple commit.
///
/// This is a defence-in-depth check; the socket-layer demux already enforces
/// header magic + version. Here we additionally reject flag combinations that
/// no legitimate fresh stream would lead with:
/// - `type_flags == 0`: a flagless packet carries no meaningful UDX intent
///   and looks like an unauthenticated probe.
/// - `FLAG_DESTROY`: a peer cannot legitimately tear down a stream that has
///   not yet been agreed (no prior packets, no negotiated state).
/// - `FLAG_SACK` in isolation: SACK ranges are meaningless before we have
///   sent any data to be selectively acknowledged.
///
/// Returns `false` to indicate "drop this packet, leave the hook armed for
/// the next inbound arrival". Returns `true` to permit hook firing and
/// 4-tuple commit.
#[inline]
fn is_plausible_first_packet(header: &Header) -> bool {
    if header.type_flags == 0 {
        return false;
    }
    if header.has_flag(FLAG_DESTROY) {
        return false;
    }
    if header.type_flags == FLAG_SACK {
        return false;
    }
    true
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
    /// Full wire-format packet bytes (header + payload). Whoever puts the
    /// packet on the wire holds the same bytes, so that a packet is
    /// allocated once, however often it is sent.
    packet: Arc<[u8]>,
    /// When this packet was last sent.
    time_sent: Instant,
    /// When this packet was first sent.
    first_sent: Instant,
    /// How many times this packet has been sent.
    transmits: u32,
    /// How many times the retransmission timer has fired for this packet.
    rto_timeouts: u32,
    /// Whether this packet was sent as an MTU probe (oversized via data_offset padding).
    is_mtu_probe: bool,
    /// Whether this packet has been declared lost and waits to be sent again.
    lost: bool,
    /// The state of delivery when this packet was last sent.
    rate_info: PacketRateInfo,
}

/// A received out-of-order packet buffered until gaps are filled.
struct RecvdPacket {
    payload: Vec<u8>,
    flags: u8,
}

/// A write in the write queue: bytes the application has handed to the
/// stream, of which some are not in a packet yet.
/// C: `udx_stream_write_buf_t`.
struct QueuedWrite {
    data: Vec<u8>,
    /// How many bytes of `data` have been put into packets.
    taken: usize,
    /// Whether the stream ends with this write.
    end: bool,
    /// Told once the remote has acknowledged all of the write.
    tx: Option<oneshot::Sender<Result<()>>>,
}

/// A write whose last byte is in a packet, waiting for that packet to be
/// acknowledged.
pub(crate) struct PendingWrite {
    pub(crate) ack_threshold: u32,
    pub(crate) tx: Option<oneshot::Sender<Result<()>>>,
}

/// Notification to the processor task.
pub(crate) enum StreamNotify {
    /// Something waits to be sent: data that was written, or an
    /// acknowledgement that the socket had no room for.
    DataQueued,
    Shutdown,
}

type WriteSenders = Vec<Option<oneshot::Sender<Result<()>>>>;
/// A packet picked to go out, and where it goes. The bytes of a packet with
/// data are the ones kept until it is acknowledged, not a copy of them.
type Outbound = (Arc<[u8]>, SocketAddr);
type StreamMap = super::socket::StreamMap;
type FirewallHook =
    Box<dyn FnOnce(&super::socket::UdxSocket, u16, std::net::IpAddr) -> bool + Send>;

// ── StreamInner ──────────────────────────────────────────────────────────────

pub(crate) struct StreamInner {
    // ── Connection identity ──
    pub(crate) remote_id: u32,
    pub(crate) remote_addr: Option<SocketAddr>,
    pub(crate) connected: bool,
    /// True once the end of the stream has been queued.
    ended: bool,
    /// True once the stream is over: closed, destroyed, or given up on.
    terminated: bool,
    /// Why the stream failed, if it did. Its reader and writer report it.
    failure: Option<std::io::ErrorKind>,
    /// Whether the stream was over before everything written to it had been
    /// acknowledged.
    lost_writes: bool,

    // ── Send state ──
    /// Bytes written and not yet in a packet, in the order they were
    /// written. C: `write_queue`.
    write_queue: VecDeque<QueuedWrite>,
    /// Whether the processor has been told of queued data that it has not
    /// looked at yet.
    notify_pending: bool,
    /// The task waiting for room to write, if one is.
    write_waker: Option<Waker>,
    /// The sequence number the next new packet takes. Packets are cut from
    /// the write queue, and numbered, as they are sent, so every number
    /// below this one has been sent.
    pub(crate) next_seq: u32,
    /// Writes whose last byte is in a packet, waiting for that packet to be
    /// acknowledged.
    pending_writes: Vec<PendingWrite>,
    /// Sent packets awaiting ACK, keyed by sequence number. A packet leaves
    /// when it is acknowledged, cumulatively or selectively.
    outgoing: BTreeMap<u32, SentPacket>,
    /// The packets in flight, in the order they were last sent: sent, and
    /// neither acknowledged nor declared lost.
    inflight_queue: BTreeSet<(Instant, u32)>,
    /// Sequence numbers of lost packets waiting to be sent again. An entry
    /// whose packet has been acknowledged meanwhile is skipped when reached.
    retransmit_queue: VecDeque<u32>,
    /// Packets acknowledged selectively, above the cumulative ACK.
    sacks: u32,
    /// Payload bytes written by the application and not yet acknowledged.
    unacked_bytes: usize,

    // ── Receive state ──
    pub(crate) next_remote_seq: u32,
    /// Highest cumulative ACK received from remote.
    remote_acked: u32,
    /// Out-of-order receive buffer, keyed by sequence number.
    recv_buf: BTreeMap<u32, RecvdPacket>,
    /// Whether an acknowledgement is owed to the remote.
    ack_wanted: bool,

    // ── Clock ──
    /// When the stream was created. Its clocks count from here: microseconds
    /// for congestion control, whole milliseconds for the pacer's ticks and
    /// the lowest round-trip time.
    epoch: Instant,

    // ── RTT estimation (Jacobson/Karels, matching C libudx constants) ──
    srtt: u32,
    rttvar: u32,
    rto: u32,
    /// Lowest round-trip time of the last five minutes (ms). `u32::MAX`
    /// until one has been measured.
    rtt_min: WindowedFilter<u32>,

    // ── Loss detection (RACK-TLP, RFC 8985) ──
    /// Round-trip time of the most recently delivered packet.
    rack_rtt: Duration,
    /// When the most recently sent packet that has been delivered was sent.
    rack_time_sent: Option<Instant>,
    /// The sequence number after that packet's.
    rack_next_seq: u32,
    /// The highest sequence number acknowledged so far, plus one.
    rack_fack: u32,
    /// Whether the path has delivered packets out of order.
    reordering_seen: bool,
    /// Whether a tail loss probe is in flight.
    tlp_in_flight: bool,
    /// The sequence number the probe carried.
    tlp_end_seq: u32,
    /// Whether the probe was a packet sent before, not new data.
    tlp_is_retrans: bool,
    /// Whether a round-trip time has been measured since the last probe.
    tlp_permitted: bool,
    /// Whether a probe is due to be sent.
    tlp_wanted: bool,

    // ── Timers ──
    /// When the retransmission timer fires.
    rto_deadline: Option<Instant>,
    /// When the tail loss probe is due.
    tlp_deadline: Option<Instant>,
    /// When packets that may only be out of order are checked again.
    rack_reo_deadline: Option<Instant>,
    /// When the pacer has tokens again.
    pacing_deadline: Option<Instant>,

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
    pub(crate) congestion: CongestionController,

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
    /// The state of a stream that has not been connected yet.
    fn new(
        read_tx: mpsc::UnboundedSender<StreamEvent>,
        close_tx: Option<oneshot::Sender<()>>,
    ) -> Self {
        let epoch = Instant::now();
        Self {
            remote_id: 0,
            remote_addr: None,
            connected: false,
            ended: false,
            terminated: false,
            failure: None,
            lost_writes: false,
            write_queue: VecDeque::new(),
            notify_pending: false,
            write_waker: None,
            next_seq: 0,
            pending_writes: Vec::new(),
            outgoing: BTreeMap::new(),
            inflight_queue: BTreeSet::new(),
            retransmit_queue: VecDeque::new(),
            sacks: 0,
            unacked_bytes: 0,
            next_remote_seq: 0,
            remote_acked: 0,
            recv_buf: BTreeMap::new(),
            ack_wanted: false,
            epoch,
            srtt: 0,
            rttvar: 0,
            rto: INITIAL_RTO_MS,
            rtt_min: WindowedFilter::new(0, u32::MAX),
            rack_rtt: Duration::ZERO,
            rack_time_sent: None,
            rack_next_seq: 0,
            rack_fack: 0,
            reordering_seen: false,
            tlp_in_flight: false,
            tlp_end_seq: 0,
            tlp_is_retrans: false,
            tlp_permitted: false,
            tlp_wanted: false,
            rto_deadline: None,
            tlp_deadline: None,
            rack_reo_deadline: None,
            pacing_deadline: None,
            send_rwnd: DEFAULT_RWND,
            mtu_state: MtuState::Base,
            mtu: MTU_BASE,
            mtu_probe_size: MTU_BASE + MTU_STEP,
            mtu_probe_count: 0,
            mtu_probe_wanted: false,
            mtu_max: MTU_MAX,
            udp: None,
            read_tx,
            close_tx,
            notify_tx: None,
            congestion: CongestionController::new(MAX_PAYLOAD as u32, 0),
            relay_target: None,
            firewall_hook: None,
            firewall_socket: None,
        }
    }

    // ── Stream termination ──────────────────────────────────────────────────

    /// Fail every write that has not been acknowledged with `StreamClosed`,
    /// mark the stream disconnected, and drop `notify_tx` so future sends
    /// fail fast. Idempotent — safe to call more than once.
    fn terminate(&mut self) {
        if !self.terminated && !self.drained() {
            self.lost_writes = true;
        }
        self.connected = false;
        self.terminated = true;
        self.notify_tx = None;
        self.fail_writes(|| UdxError::StreamClosed);
        // Nothing more arrives: a reader that is waiting finds the end.
        let _ = self.read_tx.send(StreamEvent::End);
        // A writer waiting for room finds the stream closed.
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
    }

    /// Fail every write that has not been acknowledged.
    fn fail_writes(&mut self, error: impl Fn() -> UdxError) {
        for write in self.write_queue.drain(..) {
            if let Some(tx) = write.tx {
                let _ = tx.send(Err(error()));
            }
        }
        for mut pw in self.pending_writes.drain(..) {
            if let Some(tx) = pw.tx.take() {
                let _ = tx.send(Err(error()));
            }
        }
    }

    // ── MTU ──────────────────────────────────────────────────────────────────

    /// What IP and UDP put in front of each of the stream's packets, in
    /// bytes. An address not yet known is taken for the larger, IPv6, as in
    /// C, and so is an IPv4 address mapped into IPv6.
    fn ip_overhead(&self) -> usize {
        match self.remote_addr {
            Some(SocketAddr::V4(_)) => IPV4_UDP_OVERHEAD,
            _ => IPV6_UDP_OVERHEAD,
        }
    }

    /// Bytes of payload in a full packet. C: `udx__max_payload`.
    pub(crate) fn max_payload(&self) -> usize {
        self.mtu - self.ip_overhead() - HEADER_SIZE
    }

    /// Record where the remote is. The protocol that reaches it decides how
    /// much of a packet is left for payload.
    fn set_remote_addr(&mut self, addr: SocketAddr) {
        self.remote_addr = Some(addr);
        self.congestion.on_mtu_change(self.max_payload());
    }

    /// Keep the search for the path's MTU within the MTU of the link the
    /// remote is reached over, where the system tells it: a larger packet
    /// would not leave this machine. C: the end of `udx_stream_connect`.
    fn limit_mtu_to_link(&mut self, link_mtu: Option<usize>) {
        let Some(link_mtu) = link_mtu.filter(|&mtu| mtu <= MTU_MAX) else {
            self.mtu_max = MTU_MAX;
            return;
        };
        if link_mtu <= MTU_BASE {
            // Nothing above the starting size to search.
            self.mtu_state = MtuState::SearchComplete;
        }
        self.mtu_max = link_mtu;
    }

    /// Inflate a DATA packet into an IP packet of `mtu_probe_size` bytes by
    /// inserting zero padding after the header.
    /// Sets `data_offset` so the receiver skips the padding via `payload_offset()`.
    /// Returns `true` if the packet was successfully probeified.
    fn mtu_probeify_packet(&mut self, packet: &mut Vec<u8>) -> bool {
        if packet.len() < HEADER_SIZE {
            return false;
        }
        // The probe is an IP packet of the size being tried, so the datagram
        // is that less what IP and UDP put in front of it.
        let target_size = self.mtu_probe_size - self.ip_overhead();
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

        self.congestion.on_mtu_change(self.max_payload());
        tracing::debug!(old_mtu, new_mtu = self.mtu, state = ?self.mtu_state, "mtu: probe acked");
    }

    // ── Writing ──────────────────────────────────────────────────────────────
    //
    // A write goes into the write queue, and packets are cut from the queue
    // as they are sent, as in C. So a packet is as large as the path's MTU
    // allows at that moment, writes smaller than a packet share one, and a
    // packet's sequence number is the order it went out in.

    /// Whether the stream can take a write.
    fn check_writable(&self) -> Result<()> {
        if let Some(error) = self.failure() {
            return Err(UdxError::Io(error));
        }
        if self.terminated {
            return Err(UdxError::StreamClosed);
        }
        if !self.connected && self.firewall_hook.is_none() {
            return Err(UdxError::Io(std::io::Error::other("stream not connected")));
        }
        if self.ended {
            return Err(UdxError::Io(std::io::Error::other("stream already shut down")));
        }
        if self.udp.is_none() {
            return Err(UdxError::StreamClosed);
        }
        Ok(())
    }

    /// The error of a stream that gave up on its remote, for its reader and
    /// its writer.
    pub(crate) fn failure(&self) -> Option<std::io::Error> {
        self.failure
            .map(|kind| std::io::Error::new(kind, "RTO timeout exceeded"))
    }

    /// The error of a stream that could not deliver all that was written to
    /// it: it gave up on its remote, or was closed first.
    pub(crate) fn write_error(&self) -> Option<std::io::Error> {
        self.failure().or_else(|| {
            self.lost_writes.then(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "UDX stream closed before everything written was acknowledged",
                )
            })
        })
    }

    /// Queue `data` to be sent, and return a receiver that is told once the
    /// remote has acknowledged all of it. C: `udx_stream_write`.
    fn queue_write(&mut self, data: Vec<u8>) -> Result<oneshot::Receiver<Result<()>>> {
        self.check_writable()?;
        let (tx, rx) = oneshot::channel();
        self.push_write(QueuedWrite { data, taken: 0, end: false, tx: Some(tx) });
        Ok(rx)
    }

    /// Queue the end of the stream, after everything written so far, and
    /// return a receiver that is told once the remote has acknowledged it.
    /// C: `udx_stream_write_end`.
    pub(crate) fn queue_end(&mut self) -> Result<oneshot::Receiver<Result<()>>> {
        if let Some(error) = self.failure() {
            return Err(UdxError::Io(error));
        }
        if self.terminated {
            return Err(UdxError::StreamClosed);
        }
        if !self.connected {
            return Err(UdxError::Io(std::io::Error::other("stream not connected")));
        }
        if self.ended {
            return Err(UdxError::Io(std::io::Error::other("stream already shut down")));
        }
        if self.udp.is_none() || self.remote_addr.is_none() {
            return Err(UdxError::StreamClosed);
        }
        self.ended = true;
        let (tx, rx) = oneshot::channel();
        self.push_write(QueuedWrite { data: Vec::new(), taken: 0, end: true, tx: Some(tx) });
        Ok(rx)
    }

    /// Take as much of `buf` as the stream has room for, for a writer that
    /// does not wait for acknowledgements: the adapter. `None` means the
    /// stream is full, and `waker` is woken once it has room again.
    ///
    /// C: the return value of `udx_stream_write`, and `on_drain`. As there,
    /// a write is taken whole while the stream is under its limit; here no
    /// more than the high-water mark of it at a time, so that one very
    /// large write is not copied whole.
    pub(crate) fn write_some(&mut self, buf: &[u8], waker: &Waker) -> Result<Option<usize>> {
        self.check_writable()?;
        if buf.is_empty() {
            return Ok(Some(0));
        }
        if self.unacked_bytes >= self.write_limit() {
            if !self.write_waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
                self.write_waker = Some(waker.clone());
            }
            return Ok(None);
        }
        let len = buf.len().min(HIGH_WATERMARK);
        self.push_write(QueuedWrite { data: buf[..len].to_vec(), taken: 0, end: false, tx: None });
        Ok(Some(len))
    }

    /// How many bytes may be written and unacknowledged before a writer is
    /// asked to wait. C: `UDX_HIGH_WATERMARK + send_window_in_bytes(stream)`.
    fn write_limit(&self) -> usize {
        let window = (self.congestion.cwnd as usize)
            .saturating_mul(self.max_payload())
            .min(self.send_rwnd as usize);
        HIGH_WATERMARK + window
    }

    /// Wake the writer that waits for room, once there is some.
    fn wake_writer(&mut self) {
        if self.write_waker.is_some() && self.unacked_bytes < self.write_limit() {
            if let Some(waker) = self.write_waker.take() {
                waker.wake();
            }
        }
    }

    fn push_write(&mut self, write: QueuedWrite) {
        self.unacked_bytes += write.data.len();
        self.write_queue.push_back(write);
        self.notify_processor();
    }

    /// Tell the stream's own task that something waits to be sent. One
    /// notification is enough until the task has looked.
    fn notify_processor(&mut self) {
        if !self.notify_pending {
            if let Some(ref tx) = self.notify_tx {
                self.notify_pending = tx.send(StreamNotify::DataQueued).is_ok();
            }
        }
    }

    /// Cut the next packet from the write queue, which must not be empty,
    /// into `packet`: a packet's worth of bytes, from as many writes as that
    /// takes. The packet is numbered here, and the writes it completes wait
    /// for its acknowledgement. C: the packet built in `send_stream_packets`.
    fn cut_packet(&mut self, packet: &mut Vec<u8>) -> u32 {
        let capacity = self.max_payload();
        let seq = self.next_seq;
        let mut flags = 0;
        packet.clear();
        packet.reserve(HEADER_SIZE + capacity);
        packet.resize(HEADER_SIZE, 0);

        while let Some(write) = self.write_queue.front_mut() {
            let room = capacity - (packet.len() - HEADER_SIZE);
            let rest = &write.data[write.taken..];
            let len = rest.len().min(room);
            if len > 0 {
                packet.extend_from_slice(&rest[..len]);
                write.taken += len;
                flags |= FLAG_DATA;
            }
            if write.taken < write.data.len() {
                // The packet is full.
                break;
            }

            // All of the write is in packets, and this one is its last.
            let Some(write) = self.write_queue.pop_front() else {
                break;
            };
            if let Some(tx) = write.tx {
                self.pending_writes.push(PendingWrite {
                    ack_threshold: seq.wrapping_add(1),
                    tx: Some(tx),
                });
            }
            if write.end {
                flags |= FLAG_END;
                break;
            }
        }

        // An empty write takes a packet all the same, to be acknowledged by.
        if flags == 0 {
            flags = FLAG_DATA;
        }

        let header = Header {
            type_flags: flags,
            data_offset: 0,
            remote_id: self.remote_id,
            recv_window: DEFAULT_RWND,
            seq,
            ack: self.next_remote_seq,
        };
        packet[..HEADER_SIZE].copy_from_slice(&header.encode());
        self.next_seq = seq.wrapping_add(1);
        seq
    }

    // ── Send bookkeeping ─────────────────────────────────────────────────────

    /// Whole milliseconds from the stream's creation to `now`: the clock the
    /// pacer ticks on, and the one the lowest round-trip time is kept by.
    fn millis(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_millis() as u64
    }

    /// Microseconds from the stream's creation to `now`: the clock its
    /// congestion control runs on.
    fn micros(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.epoch).as_micros() as u64
    }

    /// Packets in flight: sent, and neither acknowledged nor declared lost.
    pub(crate) fn inflight(&self) -> u32 {
        self.inflight_queue.len() as u32
    }

    /// Whether every packet sent so far has been acknowledged.
    fn all_acked(&self) -> bool {
        self.remote_acked == self.next_seq
    }

    /// Whether everything written has been acknowledged.
    fn drained(&self) -> bool {
        self.write_queue.is_empty() && self.outgoing.is_empty()
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

    /// Bounds a round-trip sample. A machine that slept between sending a
    /// packet and receiving its acknowledgement measures the sleep as well,
    /// so a sample over five seconds that is far outside what the stream has
    /// seen is cut down to the edge of it. C: `clamp_rtt`.
    fn clamp_rtt(&self, rtt_ms: u64) -> u32 {
        let rtt_ms = rtt_ms.min(u64::from(MAX_RTT_MS)) as u32;
        if self.srtt == 0 {
            return rtt_ms;
        }
        let outlier_threshold = self.srtt + 5 * self.rttvar;
        if rtt_ms > outlier_threshold && rtt_ms > 5_000 {
            return outlier_threshold.min(MAX_RTT_MS);
        }
        rtt_ms
    }

    // ── ACK processing ───────────────────────────────────────────────────────

    /// Process the acknowledgement an incoming packet carries: its cumulative
    /// ACK and its SACK ranges. Returns the senders of the writes that are
    /// now complete.
    ///
    /// Acknowledged packets leave the outgoing buffer and feed the RTT
    /// estimate, loss detection and the rate sample that congestion control
    /// is then run on. C: the acknowledgement half of `process_packet`.
    fn process_ack(&mut self, ack: u32, sack_ranges: &[SackRange], now: Instant) -> WriteSenders {
        // An acknowledgement of something that was never sent.
        if seq_after(ack, self.next_seq) {
            return Vec::new();
        }

        let now_ms = self.millis(now);
        let delivered_before = self.congestion.delivered;
        let lost_before = self.congestion.lost;
        let ack_advanced = seq_after(ack, self.remote_acked);
        let data_inflight = !self.all_acked();

        // RFC 8985, 7.4.2
        if self.tlp_in_flight {
            self.detect_loss_repaired_by_loss_probe(ack);
        }

        self.congestion.on_ack_received(ack);

        let mut rs = RateSample::default();
        let mut completed = Vec::new();

        if ack_advanced {
            let acked_range = ack.wrapping_sub(self.remote_acked);
            let kept = self.outgoing.split_off(&ack);
            let acked = std::mem::replace(&mut self.outgoing, kept);

            // Sequence numbers no longer in the buffer were acknowledged
            // selectively before.
            let sacked_before = acked_range.saturating_sub(acked.len() as u32);
            self.sacks = self.sacks.saturating_sub(sacked_before);

            for (seq, pkt) in acked {
                self.on_packet_acked(seq, pkt, now, now_ms, &mut rs);
            }
            self.remote_acked = ack;

            // Complete pending writes whose ack_threshold <= ack
            self.pending_writes.retain_mut(|pw| {
                if pw.ack_threshold <= ack {
                    completed.push(pw.tx.take());
                    false
                } else {
                    true
                }
            });
        }

        for range in sack_ranges {
            if !self.sack_is_valid(range) {
                continue;
            }
            // Only the packets still held, however wide the range claims to be.
            // A valid range may still cross the wrap of the sequence space.
            let sacked: Vec<u32> = if range.start <= range.end {
                self.outgoing.range(range.start..range.end).map(|(&seq, _)| seq).collect()
            } else {
                self.outgoing
                    .range(range.start..)
                    .chain(self.outgoing.range(..range.end))
                    .map(|(&seq, _)| seq)
                    .collect()
            };
            for seq in sacked {
                if let Some(pkt) = self.outgoing.remove(&seq) {
                    self.sacks += 1;
                    self.on_packet_acked(seq, pkt, now, now_ms, &mut rs);
                }
            }
        }

        let delivered = self.congestion.delivered.wrapping_sub(delivered_before);
        // Taken before loss detection runs below, as in C. The losses this
        // acknowledgement reveals are therefore not in the rate sample, there
        // or here, and congestion control does not shrink the window by them.
        // It is told of them apart, to see when Startup has filled the path.
        let lost = self.congestion.lost.wrapping_sub(lost_before);

        // RFC 8985, 7.2
        if ack_advanced && self.congestion.ca_state == CaState::Open && self.sacks == 0 {
            self.schedule_loss_probe(now);
        }

        if delivered > 0 {
            if self.all_acked() {
                self.rto_deadline = None;
                self.tlp_deadline = None;
            } else {
                self.rto_deadline = Some(now + Duration::from_millis(u64::from(self.rto)));
            }

            // RFC 8985, 6.2 step 5
            self.rack_detect_loss_and_arm_timer(now);
        }

        // With nothing in flight, nothing could be acknowledged, and there is
        // no sample to run congestion control on.
        if data_inflight {
            let summary = AckSummary {
                delivered,
                lost,
                newly_lost: self.congestion.lost.wrapping_sub(lost_before),
                inflight: self.inflight(),
                srtt_ms: self.srtt,
                rtt_min_ms: self.rtt_min.get(),
            };
            let now_us = self.micros(now);
            self.congestion.on_ack(&mut rs, summary, now_us);
        }

        completed
    }

    /// Whether a SACK range lies within what was sent and is not yet
    /// acknowledged. C: `udx_sack_is_valid`.
    fn sack_is_valid(&self, range: &SackRange) -> bool {
        !seq_after(range.end, self.next_seq)
            && seq_after(range.end, range.start)
            && seq_after(self.next_seq, range.start)
            && !seq_after(self.remote_acked, range.start)
    }

    /// Account for one acknowledged packet, which has just left the outgoing
    /// buffer. C: `ack_packet`.
    fn on_packet_acked(
        &mut self,
        seq: u32,
        pkt: SentPacket,
        now: Instant,
        now_ms: u64,
        rs: &mut RateSample,
    ) {
        if pkt.is_mtu_probe && self.mtu_state == MtuState::Search {
            self.on_mtu_probe_acked();
        }

        // A packet declared lost is no longer in flight. Its entry in the
        // retransmit queue is skipped when it is reached.
        if !pkt.lost {
            self.inflight_queue.remove(&(pkt.time_sent, seq));
        }

        let retransmitted = pkt.transmits > 1;
        let rtt = now.saturating_duration_since(pkt.time_sent);
        let rtt_ms = self.clamp_rtt(rtt.as_millis() as u64);
        // An acknowledgement that comes sooner after a packet was last sent
        // than a round trip takes answers an earlier sending of it.
        let answers_earlier = retransmitted && rtt_ms < self.rtt_min.get();

        let delivered = DeliveredPacket {
            seq,
            time_sent: self.micros(pkt.time_sent),
            first_sent: self.micros(pkt.first_sent),
            retransmitted,
            answers_earlier,
            info: pkt.rate_info,
        };
        self.congestion.on_packet_delivered(rs, &delivered);

        let next = seq.wrapping_add(1);

        // An acknowledgement of a packet sent more than once does not say
        // which transmission it answers, so it is not a round-trip sample.
        if !retransmitted {
            // RFC 8985, 6.2 step 1
            self.rtt_min.apply_min(RTT_MIN_WINDOW_MS, now_ms, rtt_ms);
            self.update_rtt(rtt_ms);
            self.tlp_permitted = true;
        }

        // RFC 8985, 6.2 step 2: remember the most recently sent packet that
        // has been delivered. A retransmitted one counts when the time since
        // it was last sent is a possible round trip.
        if !answers_earlier {
            self.rack_rtt = rtt;
            rs.rtt_ms = Some(rtt_ms);

            let newest = match self.rack_time_sent {
                None => true,
                Some(rack_time_sent) => {
                    pkt.time_sent > rack_time_sent
                        || (pkt.time_sent == rack_time_sent && seq_after(next, self.rack_next_seq))
                }
            };
            if newest {
                self.rack_time_sent = Some(pkt.time_sent);
                self.rack_next_seq = next;
            }
        }

        // RFC 8985, 6.2 step 3: an acknowledgement below the highest one
        // seen, for a packet sent once, means the path reordered packets.
        if seq_after(next, self.rack_fack) {
            self.rack_fack = next;
        } else if seq_after(self.rack_fack, next) && !retransmitted {
            self.reordering_seen = true;
        }

        self.unacked_bytes = self.unacked_bytes.saturating_sub(payload_len(&pkt.packet));
    }

    // ── Loss detection (RACK-TLP, RFC 8985) ──────────────────────────────────

    /// How long a packet may trail one sent after it before it counts as lost
    /// rather than out of order. RFC 8985, 6.2 step 4. C: `rack_update_reo_wnd`.
    fn rack_reo_wnd(&self) -> Duration {
        if !self.reordering_seen {
            // Without reordering on the path, a gap is a loss once recovery
            // is under way or three packets have arrived past it.
            if self.congestion.ca_state != CaState::Open || self.sacks >= 3 {
                return Duration::ZERO;
            }
        }

        let quarter_min_rtt = self.rtt_min.get() / 4;
        Duration::from_millis(u64::from(quarter_min_rtt.min(self.srtt)))
    }

    /// Declare lost every packet in flight that was sent long enough before
    /// one that has since been delivered. Returns how long to wait before
    /// looking again at the packets that may still only be out of order.
    /// C: `rack_detect_loss`.
    fn rack_detect_loss(&mut self, now: Instant) -> Option<Duration> {
        let rack_time_sent = self.rack_time_sent?;
        let reo_wnd = self.rack_reo_wnd();
        let mut timeout = Duration::ZERO;
        let mut lost = Vec::new();

        // Oldest first, and no further than the newest delivered packet.
        for &(time_sent, seq) in &self.inflight_queue {
            if time_sent > rack_time_sent {
                break;
            }
            let sent_before = time_sent < rack_time_sent
                || seq_after(self.rack_next_seq, seq.wrapping_add(1));
            if !sent_before {
                continue;
            }

            let deadline = time_sent + self.rack_rtt + reo_wnd;
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                lost.push(seq);
            } else {
                timeout = timeout.max(remaining);
            }
        }

        let resending = lost.len();
        let mut mtu_probes_lost = 0;
        for seq in lost {
            if self.mark_lost(seq) {
                mtu_probes_lost += 1;
            }
        }

        // A lost MTU probe says the probe was too large for the path, not
        // that the path is congested.
        if resending > mtu_probes_lost && self.congestion.ca_state == CaState::Open {
            // Recover until everything sent so far is acknowledged.
            self.congestion.enter_recovery(self.next_seq);
            // RFC 8985, 7.1
            self.tlp_in_flight = false;
            self.tlp_is_retrans = false;
        }

        (!timeout.is_zero()).then_some(timeout)
    }

    /// C: `rack_detect_loss_and_arm_timer`.
    fn rack_detect_loss_and_arm_timer(&mut self, now: Instant) {
        self.rack_reo_deadline = self.rack_detect_loss(now).map(|timeout| now + timeout);
    }

    /// Declare a packet in flight lost, and queue it to be sent again.
    /// Returns whether it was an MTU probe.
    fn mark_lost(&mut self, seq: u32) -> bool {
        let Some(pkt) = self.outgoing.get_mut(&seq) else {
            return false;
        };
        if pkt.lost {
            return false;
        }

        pkt.lost = true;
        self.inflight_queue.remove(&(pkt.time_sent, seq));
        self.retransmit_queue.push_back(seq);
        self.congestion.on_packet_lost();

        let was_mtu_probe = pkt.is_mtu_probe;
        if was_mtu_probe {
            // It goes out again as an ordinary packet.
            strip_probe_padding(&mut pkt.packet);
            pkt.is_mtu_probe = false;
            self.on_mtu_probe_lost();
        }
        was_mtu_probe
    }

    /// Count a failed MTU probe. After `MTU_MAX_PROBES` of them the search
    /// settles on the current MTU.
    fn on_mtu_probe_lost(&mut self) {
        self.mtu_probe_count += 1;
        tracing::debug!(
            failures = self.mtu_probe_count,
            max = MTU_MAX_PROBES,
            "mtu: probe lost"
        );
        if self.mtu_probe_count >= MTU_MAX_PROBES {
            self.mtu_state = MtuState::SearchComplete;
            self.mtu_probe_wanted = false;
        } else {
            self.mtu_probe_wanted = true;
        }
    }

    /// A tail loss probe that was acknowledged has done its work.
    /// RFC 8985, 7.4.2. C: `detect_loss_repaired_by_loss_probe`.
    fn detect_loss_repaired_by_loss_probe(&mut self, ack: u32) {
        if !self.tlp_in_flight || seq_after(self.tlp_end_seq, ack) {
            return;
        }
        // A probe that repeated an old packet is only settled once the
        // acknowledgement has passed it.
        if !self.tlp_is_retrans || seq_after(ack, self.tlp_end_seq) {
            self.tlp_in_flight = false;
        }
    }

    /// Arm the tail loss probe, or move it. Without one, the loss of the
    /// last packets of a flight would only show when the retransmission
    /// timer fires. RFC 8985, 7.2. C: `schedule_loss_probe`.
    ///
    /// The probe waits two round trips. The smoothed round-trip time is in
    /// whole milliseconds, so on a path that answers in less than one it is
    /// zero. C takes zero to mean that no round trip has been measured, and
    /// waits a second. Here a measured round trip below a millisecond counts
    /// as one, as it does for BBR.
    fn schedule_loss_probe(&mut self, now: Instant) {
        let mut pto_ms = DEFAULT_PTO_MS;
        let measured = self.rtt_min.get() != u32::MAX;
        if measured {
            pto_ms = u64::from(self.srtt.max(1)) * 2;
            if self.inflight() == 1 {
                pto_ms += TLP_MAX_ACK_DELAY_MS;
            }
        }

        let mut deadline = now + Duration::from_millis(pto_ms);
        // Never later than the retransmission timer.
        if let Some(rto_deadline) = self.rto_deadline {
            deadline = deadline.min(rto_deadline);
        }
        self.tlp_deadline = Some(deadline);
    }

    /// Arm the timers that watch over a packet just sent.
    /// C: `arm_stream_timers`.
    fn arm_stream_timers(&mut self, now: Instant, sent_tlp: bool) {
        if self.rto_deadline.is_none() {
            self.rto_deadline = Some(now + Duration::from_millis(u64::from(self.rto)));
        }

        // RFC 8985, 7.2
        if self.congestion.ca_state != CaState::Open || self.sacks > 0 {
            self.tlp_deadline = None;
        } else if !sent_tlp {
            self.schedule_loss_probe(now);
        }
    }

    /// The tail loss probe is due. RFC 8985, 7.3. C: `udx_tlp_timeout`.
    fn on_tlp_timeout(&mut self, now: Instant) {
        self.tlp_deadline = None;

        if self.all_acked() {
            return;
        }

        // One probe at a time, and none until a round-trip time has been
        // measured since the last.
        if self.tlp_in_flight || !self.tlp_permitted {
            self.schedule_loss_probe(now);
            return;
        }

        self.tlp_wanted = true;
    }

    /// The retransmission timer fired: nothing was acknowledged for a whole
    /// timeout. Returns whether the stream has to give up.
    /// C: `udx_rto_timeout`.
    fn on_rto_timeout(&mut self, now: Instant) -> bool {
        self.rto_deadline = None;

        if self.all_acked() {
            return false;
        }

        self.congestion.on_rto(self.next_seq);
        // RFC 8985, 7.1
        self.tlp_in_flight = false;
        self.tlp_is_retrans = false;

        self.rto_deadline = Some(now + Duration::from_millis(u64::from(self.rto) * 2));

        // Rebuilt below, in sequence order.
        self.retransmit_queue.clear();

        // RFC 8985, 6.3: the first unacknowledged packet is lost, and so is
        // every packet whose own round trip has run out.
        let reo_wnd = self.rack_reo_wnd();
        let mut mtu_probes_lost = 0;
        for (&seq, pkt) in self.outgoing.iter_mut() {
            if pkt.lost {
                self.retransmit_queue.push_back(seq);
                continue;
            }

            let timed_out = pkt.time_sent + self.rack_rtt + reo_wnd < now;
            if seq != self.remote_acked && !timed_out {
                continue;
            }

            if pkt.rto_timeouts >= MAX_RTO_TIMEOUTS {
                return true;
            }

            pkt.lost = true;
            pkt.rto_timeouts += 1;
            self.inflight_queue.remove(&(pkt.time_sent, seq));
            self.retransmit_queue.push_back(seq);
            self.congestion.on_packet_lost();

            if pkt.is_mtu_probe {
                strip_probe_padding(&mut pkt.packet);
                pkt.is_mtu_probe = false;
                mtu_probes_lost += 1;
            }
        }

        for _ in 0..mtu_probes_lost {
            self.on_mtu_probe_lost();
        }
        false
    }

    // ── Timers ──────────────────────────────────────────────────────────────

    /// The earliest moment one of the stream's timers is due.
    fn next_deadline(&self) -> Option<Instant> {
        [
            self.rto_deadline,
            self.tlp_deadline,
            self.rack_reo_deadline,
            self.pacing_deadline,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Run the timers that are due. Returns whether the stream has to give
    /// up because the remote stopped acknowledging.
    fn on_timers(&mut self, now: Instant) -> bool {
        let due = |deadline: Option<Instant>| deadline.is_some_and(|at| at <= now);

        if due(self.rto_deadline) && self.on_rto_timeout(now) {
            return true;
        }
        if due(self.tlp_deadline) {
            self.on_tlp_timeout(now);
        }
        if due(self.rack_reo_deadline) {
            self.rack_detect_loss_and_arm_timer(now);
        }
        if due(self.pacing_deadline) {
            // The pacer refills itself when the stream next tries to send.
            self.pacing_deadline = None;
        }
        false
    }

    // ── Receive buffering ────────────────────────────────────────────────────

    /// Take in what a packet carries for the reader: its data, the end of
    /// the stream, or both. The reader is handed what is now in order, and
    /// the stream owes the remote an acknowledgement, which a heartbeat
    /// draws too. C: the receiving half of `process_packet`.
    fn receive_packet(&mut self, header: &Header, mut data: Vec<u8>) {
        let is_data = header.has_flag(FLAG_DATA);
        let is_end = header.has_flag(FLAG_END);

        if is_data || is_end {
            let payload_start = header.payload_offset();
            if payload_start <= data.len() {
                data.drain(..payload_start);
            } else {
                data.clear();
            }
            let flags = (if is_data { FLAG_DATA } else { 0 }) | (if is_end { FLAG_END } else { 0 });

            for event in self.receive_data(header.seq, data, flags) {
                let _ = self.read_tx.send(event);
            }
            // Acknowledged once, after the last packet of the batch.
            self.ack_wanted = true;
        }

        if header.has_flag(FLAG_HEARTBEAT) {
            self.ack_wanted = true;
        }
    }

    /// Whether a packet says something that the sending side has not heard:
    /// it acknowledges more, names ranges, or advertises another window.
    fn has_news_for_sender(&self, header: &Header) -> bool {
        seq_after(header.ack, self.remote_acked)
            || header.recv_window != self.send_rwnd
            || header.has_flag(FLAG_SACK)
    }

    /// The acknowledgement the stream owes the remote, if it owes one and
    /// has a remote to send it to. From then on it owes none.
    fn take_ack(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        if !self.ack_wanted || !self.connected {
            return None;
        }
        let remote_addr = self.remote_addr?;
        self.ack_wanted = false;
        Some((self.build_ack(), remote_addr))
    }

    /// The acknowledgement that was taken found no room in the socket. The
    /// stream owes it again, and its own task, which can wait for room, is
    /// told so.
    fn ack_not_sent(&mut self) {
        self.ack_wanted = true;
        self.notify_processor();
    }

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

        // Duplicate or already-received packet — ignore. C: `seq_compare`.
        if seq != self.next_remote_seq && !seq_after(seq, self.next_remote_seq) {
            return events;
        }

        if seq == self.next_remote_seq {
            // In-order: deliver directly
            push_events(&mut events, payload, flags);
            self.next_remote_seq = seq.wrapping_add(1);

            // Drain consecutive buffered packets
            while let Some(recvd) = self.recv_buf.remove(&self.next_remote_seq) {
                push_events(&mut events, recvd.payload, recvd.flags);
                self.next_remote_seq = self.next_remote_seq.wrapping_add(1);
            }
        } else {
            // Out-of-order: buffer for later delivery
            self.recv_buf.entry(seq).or_insert(RecvdPacket { payload, flags });
        }

        events
    }

    // ── SACK generation ──────────────────────────────────────────────────────

    /// How many SACK ranges an acknowledgement may carry: as many as fit a
    /// packet of the starting MTU, which every path carries.
    ///
    /// The remote learns of a gap only from the ranges around it, and the
    /// ranges are the lowest ones. So what it can repair in one round trip is
    /// what one acknowledgement can name, and a burst of losses is repaired
    /// the faster the more ranges fit. C libudx sends 51 at most and reads as
    /// many as a packet holds.
    fn max_sack_ranges(&self) -> usize {
        (MTU_BASE - self.ip_overhead() - HEADER_SIZE) / SACK_RANGE_SIZE
    }

    /// Compute SACK ranges from the out-of-order receive buffer.
    /// Returns contiguous ranges of received sequence numbers that are
    /// beyond the cumulative ACK point, lowest first.
    fn sack_ranges(&self) -> Vec<SackRange> {
        if self.recv_buf.is_empty() {
            return Vec::new();
        }

        let max_ranges = self.max_sack_ranges();
        let mut ranges = Vec::new();
        let mut iter = self.recv_buf.keys();

        if let Some(&first) = iter.next() {
            let mut start = first;
            let mut end = first.wrapping_add(1);

            for &seq in iter {
                if seq == end {
                    // Extend current range
                    end = seq.wrapping_add(1);
                } else {
                    // Gap found: close current range, start new one
                    ranges.push(SackRange { start, end });
                    if ranges.len() >= max_ranges {
                        return ranges;
                    }
                    start = seq;
                    end = seq.wrapping_add(1);
                }
            }
            ranges.push(SackRange { start, end });
        }
        ranges
    }

    // ── Sending ──────────────────────────────────────────────────────────────

    /// What congestion control needs to know when a packet goes out.
    fn send_state(&self) -> SendState {
        SendState {
            unacked_bytes: self.unacked_bytes,
            inflight: self.inflight(),
            retransmit_pending: !self.retransmit_queue.is_empty(),
            all_acked: self.all_acked(),
        }
    }

    /// Take a sent packet out of the pacer's tokens, and wake the stream for
    /// the refill if that used them up.
    fn on_bytes_sent(&mut self, bytes: usize, now: Instant) {
        if self.congestion.on_bytes_sent(bytes) {
            // The pacer refills when its clock next ticks.
            let next_tick = self.millis(now) + REFILL_INTERVAL_MS;
            self.pacing_deadline = Some(self.epoch + Duration::from_millis(next_tick));
        }
    }

    /// Pick what the stream may send now, and record it as sent. The caller
    /// puts it on the wire. Returns whether more is waiting that this batch
    /// had no room for.
    ///
    /// The order is that of C's `send_stream_packets`: the acknowledgement
    /// the stream owes, then lost packets, then new data. Lost packets and
    /// new data go out only while the pacer has tokens and the window has
    /// room.
    fn collect_sends(&mut self, now: Instant, out: &mut Vec<Outbound>) -> bool {
        if !self.connected {
            return false;
        }
        let Some(remote_addr) = self.remote_addr else {
            return false;
        };

        if let Some((ack, to)) = self.take_ack() {
            out.push((ack.into(), to));
        }

        let now_us = self.micros(now);
        let inflight_was_empty = self.inflight_queue.is_empty();

        while let Some(&seq) = self.retransmit_queue.front() {
            if !self.outgoing.get(&seq).is_some_and(|pkt| pkt.lost) {
                // Acknowledged while it waited.
                self.retransmit_queue.pop_front();
                continue;
            }
            if out.len() >= MAX_SEND_BATCH {
                return true;
            }
            if !self.congestion.may_send(self.inflight(), self.send_rwnd, now_us) {
                break;
            }

            let rate_info = self.congestion.on_packet_sent(self.send_state(), now_us);
            self.retransmit_queue.pop_front();
            let Some(pkt) = self.outgoing.get_mut(&seq) else {
                continue;
            };
            pkt.lost = false;
            pkt.time_sent = now;
            pkt.transmits += 1;
            pkt.rate_info = rate_info;
            let packet = Arc::clone(&pkt.packet);

            self.inflight_queue.insert((now, seq));
            self.on_bytes_sent(packet.len(), now);
            if inflight_was_empty {
                self.congestion.on_transmit_start(now_us);
            }
            self.arm_stream_timers(now, false);
            out.push((packet, remote_addr));
        }

        // Where each new packet is put together. What is kept of it, and
        // what goes on the wire, is then one copy.
        let mut cut = Vec::new();
        while !self.write_queue.is_empty() {
            // A tail loss probe goes out whatever the pacer and the window say.
            let tlp = self.tlp_wanted;
            if !tlp {
                if out.len() >= MAX_SEND_BATCH {
                    return true;
                }
                if !self.congestion.may_send(self.inflight(), self.send_rwnd, now_us) {
                    break;
                }
            }

            let seq = self.cut_packet(&mut cut);

            let is_data = cut[2] & FLAG_DATA != 0;
            if is_data && self.mtu_state == MtuState::Base {
                self.mtu_state = MtuState::Search;
                self.mtu_probe_wanted = true;
            }
            let is_mtu_probe = is_data
                && self.mtu_state == MtuState::Search
                && self.mtu_probe_wanted
                && self.mtu_probeify_packet(&mut cut);
            let packet: Arc<[u8]> = Arc::from(cut.as_slice());

            let rate_info = self.congestion.on_packet_sent(self.send_state(), now_us);
            tracing::trace!(seq, is_mtu_probe, "collect_sends: packet placed in outgoing");
            self.outgoing.insert(seq, SentPacket {
                packet: Arc::clone(&packet),
                time_sent: now,
                first_sent: now,
                transmits: 1,
                rto_timeouts: 0,
                is_mtu_probe,
                lost: false,
                rate_info,
            });
            self.inflight_queue.insert((now, seq));

            self.on_bytes_sent(packet.len(), now);
            if inflight_was_empty {
                self.congestion.on_transmit_start(now_us);
            }

            if tlp {
                self.tlp_wanted = false;
                self.tlp_is_retrans = false;
                self.tlp_in_flight = true;
                self.tlp_end_seq = seq;
                self.tlp_permitted = false;
            }
            self.arm_stream_timers(now, tlp);
            out.push((packet, remote_addr));
        }

        if self.tlp_wanted && self.write_queue.is_empty() {
            // RFC 8985, 7.3: with no new data to probe with, the newest
            // packet goes out again.
            self.tlp_wanted = false;
            let seq = self.next_seq.wrapping_sub(1);
            let state = self.send_state();
            if let Some(pkt) = self.outgoing.get_mut(&seq).filter(|pkt| !pkt.lost) {
                let rate_info = self.congestion.on_packet_sent(state, now_us);
                self.inflight_queue.remove(&(pkt.time_sent, seq));
                pkt.time_sent = now;
                pkt.transmits += 1;
                pkt.rate_info = rate_info;
                let packet = Arc::clone(&pkt.packet);

                self.inflight_queue.insert((now, seq));
                self.tlp_is_retrans = true;
                self.tlp_in_flight = true;
                self.tlp_end_seq = seq;
                self.tlp_permitted = false;
                self.arm_stream_timers(now, true);
                out.push((packet, remote_addr));
            }
        }

        false
    }

    /// Build the acknowledgement of everything received so far, with SACK
    /// ranges for what arrived out of order.
    ///
    /// The ranges are the packet's whole payload, which is how C libudx
    /// sends them and all it needs to read them. A list short enough also
    /// says its length in the header's one-byte `data_offset`: this crate's
    /// releases up to 1.7.4 read only ranges announced there. A longer list
    /// leaves `data_offset` at zero, as C libudx always does.
    fn build_ack(&self) -> Vec<u8> {
        let sack_ranges = self.sack_ranges();
        let sack_byte_len = sack_ranges.len() * SACK_RANGE_SIZE;
        let header = Header {
            type_flags: if sack_ranges.is_empty() { 0 } else { FLAG_SACK },
            data_offset: u8::try_from(sack_byte_len).unwrap_or(0),
            remote_id: self.remote_id,
            recv_window: DEFAULT_RWND,
            seq: 0,
            ack: self.next_remote_seq,
        };

        let mut packet = vec![0u8; HEADER_SIZE + sack_byte_len];
        header.encode_into(&mut packet);
        encode_sack(&sack_ranges, &mut packet[HEADER_SIZE..]);
        packet
    }
}

/// Payload bytes a wire-format DATA or END packet carries, not counting MTU
/// probe padding.
fn payload_len(packet: &[u8]) -> usize {
    let padding = packet.get(3).map_or(0, |&offset| usize::from(offset));
    packet.len().saturating_sub(HEADER_SIZE + padding)
}

/// Strip MTU probe padding from a packet: remove zero bytes between header and payload,
/// reset data_offset to 0. The packet becomes a copy of its own: whoever is
/// putting the padded one on the wire still holds that one.
fn strip_probe_padding(packet: &mut Arc<[u8]>) {
    if packet.len() < HEADER_SIZE {
        return;
    }
    let data_offset = packet[3] as usize;
    if data_offset == 0 {
        return;
    }
    let mut stripped = packet.to_vec();
    let payload_start = HEADER_SIZE + data_offset;
    if payload_start <= stripped.len() {
        stripped.drain(HEADER_SIZE..payload_start);
    }
    stripped[3] = 0;
    *packet = stripped.into();
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

        let inner = StreamInner::new(read_tx, Some(close_tx));

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

    /// The size of the largest IP packet the stream sends at present, in
    /// bytes: the IP and UDP headers, the UDX header and the payload. It
    /// starts at 1200 and grows as larger packets are found to arrive. C
    /// libudx and udx-native call it the stream's MTU.
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
        let link_mtu = super::pmtu::link_mtu(remote_addr);

        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.remote_id = remote_id;
            inner.set_remote_addr(remote_addr);
            inner.limit_mtu_to_link(link_mtu);
            inner.udp = Some(udp);
            inner.connected = true;
            inner.notify_tx = Some(notify_tx);
        }

        let tx = self
            .incoming_tx
            .as_ref()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("stream already consumed")))?
            .clone();
        socket.register_stream(self.local_id, Arc::clone(&self.inner), tx)?;

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
        let acked = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.queue_write(data.to_vec())?
        };

        acked.await.map_err(|_| UdxError::StreamClosed)?
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
        let acked = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.queue_end()?
        };

        acked.await.map_err(|_| UdxError::StreamClosed)?
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
        socket.register_stream(self.local_id, Arc::clone(&self.inner), tx)?;

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

/// Where the SACK ranges of an incoming packet end, counted from its start.
///
/// This crate puts the length of the ranges in `data_offset`. C libudx leaves
/// `data_offset` at zero and sends the ranges as the whole payload of a
/// packet that carries nothing else. Both are read.
fn sack_end(header: &Header, len: usize) -> usize {
    let carries_data = header.has_flag(FLAG_DATA) || header.has_flag(FLAG_END);
    if header.data_offset > 0 {
        header.payload_offset()
    } else if carries_data {
        HEADER_SIZE
    } else {
        len
    }
}

/// The SACK ranges an incoming packet carries.
fn sack_ranges_in(header: &Header, data: &[u8]) -> Vec<SackRange> {
    if !header.has_flag(FLAG_SACK) {
        return Vec::new();
    }

    // A range cut short by the end of the packet is left out.
    data.get(HEADER_SIZE..sack_end(header, data.len()))
        .and_then(|bytes| decode_sack(&bytes[..bytes.len() / 8 * 8]).ok())
        .unwrap_or_default()
}

// ── Packets taken in where they are received ─────────────────────────────────

/// What became of a packet that a socket's receive task gave to a stream.
pub(crate) struct TakenIn {
    /// Whether the packet made the stream owe the remote an acknowledgement.
    /// The task that took the packet in sends it ([`send_ack`]). One that
    /// the stream owed already is on its way from whoever was told so.
    pub(crate) ack_owed: bool,
    /// Whether the acknowledgement is not to be kept waiting for more
    /// packets: this one left a gap in what has arrived, or is one the
    /// stream had already, or ends the stream. The remote is waiting to hear
    /// of each.
    pub(crate) ack_now: bool,
    /// What the stream's own task still has to see: the whole packet, when
    /// the stream could not take it in here, or only what the packet says to
    /// the sending side.
    pub(crate) for_its_task: Option<Vec<u8>>,
}

/// Take in a packet in the task that received it, as C libudx does: its
/// `process_packet` runs in the callback that reads the socket.
///
/// The data a packet carries is handed to the reader here, and the
/// acknowledgement it is owed goes out from the same task ([`send_ack`]). So
/// a packet that arrives costs no hand-over to the stream's own task, which
/// on a runtime with several threads is most often a hand-over to another
/// thread.
///
/// What a packet says to the sending side stays with the stream's own task,
/// which is the one that sends: what it acknowledges, the ranges it names,
/// and the window it advertises. A packet that carries nothing else goes
/// there whole. So do the packets of a stream that is not connected yet, or
/// that relays to another stream, and a packet that destroys the stream.
pub(crate) fn take_in(inner: &Mutex<StreamInner>, header: &Header, data: Vec<u8>) -> TakenIn {
    let for_the_reader = header.has_flag(FLAG_DATA)
        || header.has_flag(FLAG_END)
        || header.has_flag(FLAG_HEARTBEAT);
    if !for_the_reader || header.has_flag(FLAG_DESTROY) {
        return TakenIn { ack_owed: false, ack_now: false, for_its_task: Some(data) };
    }

    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
    if !guard.connected || guard.relay_target.is_some() {
        return TakenIn { ack_owed: false, ack_now: false, for_its_task: Some(data) };
    }

    tracing::trace!(
        flags = header.type_flags,
        seq = header.seq,
        ack = header.ack,
        "take_in: received packet"
    );

    let for_its_task = guard
        .has_news_for_sender(header)
        .then(|| sender_part(header, &data));
    let owed_already = guard.ack_wanted;
    let carries_data = header.has_flag(FLAG_DATA) || header.has_flag(FLAG_END);
    let had_already = carries_data
        && header.seq != guard.next_remote_seq
        && !seq_after(header.seq, guard.next_remote_seq);
    guard.receive_packet(header, data);
    let ack_now = had_already || !guard.recv_buf.is_empty() || header.has_flag(FLAG_END);

    TakenIn { ack_owed: guard.ack_wanted && !owed_already, ack_now, for_its_task }
}

/// What a packet says to the sending side, as a packet of its own: its
/// header without the flags for what it carries, and its SACK ranges if it
/// has any.
fn sender_part(header: &Header, data: &[u8]) -> Vec<u8> {
    // Ranges that the packet is too short to hold are no ranges.
    let sack_end = sack_end(header, data.len());
    let end = if header.has_flag(FLAG_SACK) && sack_end <= data.len() {
        sack_end
    } else {
        HEADER_SIZE
    };

    let mut part = data[..end].to_vec();
    part[2] = header.type_flags & FLAG_SACK;
    // The ranges are the whole payload now, so a list too long for the
    // one-byte length is read without it.
    part[3] = u8::try_from(end - HEADER_SIZE).unwrap_or(0);
    part
}

/// Send the acknowledgement a stream owes, from the task that took in the
/// packets it acknowledges. C: the state packet of `send_stream_packets`.
///
/// A socket that has no room for it leaves the acknowledgement to the
/// stream's own task, which waits for room.
pub(crate) fn send_ack(inner: &Mutex<StreamInner>, udp: &tokio::net::UdpSocket) {
    let ack = inner.lock().unwrap_or_else(|e| e.into_inner()).take_ack();
    let Some((packet, addr)) = ack else {
        return;
    };

    if let Err(error) = udp.try_send_to(&packet, addr) {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            inner.lock().unwrap_or_else(|e| e.into_inner()).ack_not_sent();
        }
    }
}

/// Process one incoming packet. Returns `false` when the stream is over:
/// the firewall hook turned the remote away, or the remote destroyed the
/// stream.
async fn handle_incoming(inner: &Arc<Mutex<StreamInner>>, packet: IncomingPacket) -> bool {
    // ── Header decode (early — defense in depth) ─────────
    // The socket-layer demux (`UdxSocket::ensure_recv_loop`) only
    // routes packets whose header magic+version pass `Header::decode`,
    // so reaching this point implies a well-formed UDX header. We
    // re-decode here regardless: it is cheap (a 20-byte parse), it
    // makes the firewall-hook gate immediately below independent of
    // the socket-layer contract, and it lets the gate inspect the
    // first packet's flag bits before committing the 4-tuple.
    let header = match Header::decode(&packet.data) {
        Ok(h) => h,
        Err(_) => return true,
    };

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
            // Defense in depth: drop implausible first packets
            // without firing the hook. Leaves the hook armed for
            // the legitimate first arrival. UDX header magic+version
            // are already enforced by the socket demux; here we
            // additionally reject flag combinations that cannot
            // legitimately begin a fresh stream.
            if !is_plausible_first_packet(&header) {
                tracing::debug!(
                    addr = ?packet.addr,
                    flags = header.type_flags,
                    "firewall hook: dropping implausible first packet (hook left armed)"
                );
                return true;
            }
            let hook_and_socket = {
                let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                guard.firewall_hook.take().zip(guard.firewall_socket.take())
            };
            match hook_and_socket {
                Some((hook, sock)) => {
                    if hook(&sock, packet.addr.port(), packet.addr.ip()) {
                        let link_mtu = super::pmtu::link_mtu(packet.addr);
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        guard.set_remote_addr(packet.addr);
                        guard.limit_mtu_to_link(link_mtu);
                        guard.connected = true;
                        // What was written before the hook fired can go out
                        // now: a packet is addressed when it is sent.
                        if let Some(ref tx) = guard.notify_tx {
                            let _ = tx.send(StreamNotify::DataQueued);
                        }
                        tracing::debug!(addr = ?packet.addr, "firewall hook: accepted");
                    } else {
                        tracing::debug!(addr = ?packet.addr, "firewall hook: rejected");
                        return false;
                    }
                }
                None => return true,
            }
        }
    }

    // ── Relay fast-path ──────────────────────────────
    //
    // Deliberately placed *after* the firewall-hook gate above,
    // not before it: a relay-bridged stream is typically wired
    // up with both `set_firewall_hook` (to learn its own real
    // 4-tuple from the first packet its owner sends) and
    // `relay_to` (to forward everything to its paired stream)
    // configured before any packets arrive. If this fast-path
    // ran first unconditionally, it would return past the
    // firewall-hook gate on every packet forever — the hook
    // would never fire, `remote_addr`/`connected` would never
    // be set, and (since the *destination* stream in a pair has
    // the exact same problem simultaneously) neither side of a
    // relayed pairing could ever adopt its real address, so
    // nothing would ever forward (observed empirically: both
    // ends of a blind-relay pairing retransmitted until RTO
    // exhaustion with zero bytes ever bridged). Running the
    // hook gate first lets the triggering packet commit this
    // stream's 4-tuple, and *this same packet* still gets
    // relayed below rather than being absorbed into normal
    // (unread) stream processing.
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
            return false;
        }
        return true;
    }

    tracing::trace!(
        flags = header.type_flags,
        seq = header.seq,
        ack = header.ack,
        data_offset = header.data_offset,
        "process_incoming: received packet"
    );

    // Decoded outside the lock.
    let sack_ranges = sack_ranges_in(&header, &packet.data);

    let completed = {
        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.send_rwnd = header.recv_window;

        // ── ACK + SACK processing ────────────────────────────
        let completed = if header.ack > 0 || !sack_ranges.is_empty() {
            guard.process_ack(header.ack, &sack_ranges, Instant::now())
        } else {
            Vec::new()
        };

        // ── DATA, END and HEARTBEAT processing ───────────────
        guard.receive_packet(&header, packet.data);

        // The acknowledgement, or a larger window, may have made room for
        // a writer that was asked to wait.
        guard.wake_writer();

        completed
    };

    // Notify completed writes (outside lock)
    for tx in completed.into_iter().flatten() {
        let _ = tx.send(Ok(()));
    }

    // ── DESTROY processing ───────────────────────────────
    if header.has_flag(FLAG_DESTROY) {
        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
        let _ = guard.read_tx.send(StreamEvent::End);
        if let Some(tx) = guard.close_tx.take() {
            let _ = tx.send(());
        }
        return false;
    }

    true
}

async fn process_incoming(
    inner: Arc<Mutex<StreamInner>>,
    mut incoming_rx: mpsc::UnboundedReceiver<IncomingPacket>,
    mut notify_rx: mpsc::UnboundedReceiver<StreamNotify>,
) {
    use tokio::time::{self, Instant as TokioInstant};

    let mut shutting_down = false;
    // While the owner is gone: the acknowledgement the stream had reached
    // when it last got further, and until when it is given to get further
    // again.
    let mut linger: Option<(u32, Instant)> = None;

    // One timer serves every deadline of the stream. It is moved when a
    // deadline falls before the moment it is set to, and left alone when the
    // deadlines move later, as the retransmission timer does on every
    // acknowledgement: it then fires early, finds nothing due, and is set
    // again.
    let timer = time::sleep(FAR_FUTURE);
    tokio::pin!(timer);
    let mut timer_at: Option<Instant> = None;

    let mut keepalive = time::interval(Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    keepalive.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut outbound: Vec<Outbound> = Vec::with_capacity(MAX_SEND_BATCH + 1);
    // Whether the last batch left packets that may be sent at once.
    let mut more_to_send = false;

    loop {
        tokio::select! {
            biased;

            // ── Incoming packet processing ───────────────────────────
            // Everything that has arrived is taken in before anything is
            // sent, so that one acknowledgement answers all of it, and what
            // goes out reflects every acknowledgement that came in.
            packet = incoming_rx.recv() => {
                let Some(mut packet) = packet else { break };
                let mut taken = 1;
                let alive = loop {
                    if !handle_incoming(&inner, packet).await {
                        break false;
                    }
                    if taken == MAX_RECV_BATCH {
                        break true;
                    }
                    match incoming_rx.try_recv() {
                        Ok(next) => {
                            packet = next;
                            taken += 1;
                        }
                        Err(_) => break true,
                    }
                };
                if !alive {
                    break;
                }
            }

            Some(notify) = notify_rx.recv() => {
                if matches!(notify, StreamNotify::Shutdown) {
                    shutting_down = true;
                }
            }

            // ── Timers ──────────────────────────────────────────────
            // Only a wake-up: the timers that are due run below.
            _ = &mut timer => {
                timer.as_mut().reset(TokioInstant::now() + FAR_FUTURE);
                timer_at = None;
            }

            // ── Keepalive heartbeat ──────────────────────────────────
            _ = keepalive.tick() => {
                let heartbeat = {
                    let guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.connected && guard.outgoing.is_empty() && guard.relay_target.is_none() {
                        guard.udp.clone().zip(guard.remote_addr).map(|(udp, addr)| {
                            let header = Header {
                                type_flags: FLAG_HEARTBEAT,
                                data_offset: 0,
                                remote_id: guard.remote_id,
                                recv_window: DEFAULT_RWND,
                                seq: 0,
                                ack: guard.next_remote_seq,
                            };
                            (udp, addr, header.encode())
                        })
                    } else {
                        None
                    }
                };

                if let Some((udp, addr, packet)) = heartbeat {
                    let _ = udp.send_to(&packet, addr).await;
                }
            }

            // Nothing else is waiting, and there is more to send.
            _ = std::future::ready(()), if more_to_send => {}
        }

        // ── Timers that are due, then everything that may be sent now ──
        let now = Instant::now();
        let (gave_up, udp, mut next_deadline, undelivered) = {
            let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
            // Whatever has been queued up to here is seen below.
            guard.notify_pending = false;
            let gave_up = guard.on_timers(now);
            more_to_send = !gave_up && guard.collect_sends(now, &mut outbound);
            if gave_up {
                // The remote has not answered for too long. Whoever writes
                // to the stream, or reads from it, is told so.
                guard.failure = Some(std::io::ErrorKind::TimedOut);
                guard.fail_writes(|| {
                    UdxError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "RTO timeout exceeded",
                    ))
                });
            }
            // What the stream could still deliver, by the acknowledgement
            // it has reached.
            let undelivered = (guard.connected && !guard.drained()).then_some(guard.remote_acked);
            (gave_up, guard.udp.clone(), guard.next_deadline(), undelivered)
        };

        if gave_up {
            tracing::warn!("stream failed: exceeded max RTO timeouts");
            break;
        }

        match udp {
            Some(udp) => {
                for (packet, addr) in outbound.drain(..) {
                    let _ = udp.send_to(&packet, addr).await;
                }
            }
            None => outbound.clear(),
        }

        if shutting_down {
            // The owner has let go of the stream. What it wrote is still
            // delivered, for as long as the remote goes on acknowledging.
            let Some(acked) = undelivered else {
                break;
            };
            match linger {
                Some((seen, deadline)) if seen == acked => {
                    if now >= deadline {
                        tracing::debug!("stream let go of: gave up delivering the rest");
                        break;
                    }
                }
                _ => linger = Some((acked, now + LINGER_TIMEOUT)),
            }
            if let Some((_, deadline)) = linger {
                next_deadline = Some(next_deadline.map_or(deadline, |at| at.min(deadline)));
            }
        }

        if let Some(deadline) = next_deadline {
            if timer_at.is_none_or(|at| deadline < at) {
                timer.as_mut().reset(TokioInstant::from_std(deadline));
                timer_at = Some(deadline);
            }
        }
    }

    inner.lock().unwrap_or_else(|e| e.into_inner()).terminate();
}

// ── Packet building ──────────────────────────────────────────────────────────

/// Build a wire-format DATA packet (header + payload).
#[cfg(test)]
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

#[cfg(test)]
mod mtu_tests {
    use super::*;

    const V4: &str = "192.0.2.1:9";
    const V6: &str = "[2001:db8::1]:9";

    fn make_stream_inner() -> StreamInner {
        let (read_tx, _read_rx) = mpsc::unbounded_channel();
        StreamInner::new(read_tx, None)
    }

    /// A stream that knows where its remote is.
    fn stream_to(remote: &str) -> StreamInner {
        let mut inner = make_stream_inner();
        inner.set_remote_addr(remote.parse().unwrap());
        inner
    }

    /// Put a full-sized packet in flight as an MTU probe.
    fn send_probe(inner: &mut StreamInner, seq: u32) {
        let payload = vec![0xABu8; MAX_PAYLOAD];
        let packet = build_data_packet(0, seq, 0, &payload);
        let now = Instant::now();
        let rate_info = inner.congestion.on_packet_sent(inner.send_state(), 0);
        inner.outgoing.insert(seq, SentPacket {
            packet: packet.into(),
            time_sent: now,
            first_sent: now,
            transmits: 1,
            rto_timeouts: 0,
            is_mtu_probe: true,
            lost: false,
            rate_info,
        });
        inner.inflight_queue.insert((now, seq));
        inner.next_seq = seq + 1;
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

    // T5: MTU probe failure falls back gracefully — a lost probe is stripped of its padding, queued to be
    // sent again as an ordinary packet, and counted as a failure
    #[test]
    fn mtu_probe_failure_fallback() {
        let mut inner = make_stream_inner();
        inner.mtu_state = MtuState::Search;
        inner.mtu = MTU_BASE;

        send_probe(&mut inner, 0);
        assert_eq!(inner.inflight(), 1);

        assert!(inner.mark_lost(0), "the lost packet was a probe");
        assert_eq!(inner.inflight(), 0);
        assert_eq!(inner.retransmit_queue.front(), Some(&0));
        assert_eq!(inner.mtu_probe_count, 1);
        assert!(!inner.outgoing[&0].is_mtu_probe, "probe flag should be cleared after retransmit");
        assert_eq!(inner.outgoing[&0].packet[3], 0, "data_offset should be reset");
        assert_eq!(inner.mtu_state, MtuState::Search, "still searching after 1 failure");
        assert!(inner.mtu_probe_wanted, "should want another probe after non-terminal failure");
    }

    #[test]
    fn a_lost_probe_is_stripped_in_a_copy_of_its_own() {
        let mut inner = stream_to(V4);
        inner.mtu_state = MtuState::Search;
        inner.mtu_probe_size = MTU_BASE + MTU_STEP;
        let payload = vec![0xCDu8; MAX_PAYLOAD];
        let mut packet = build_data_packet(0, 0, 0, &payload);
        assert!(inner.mtu_probeify_packet(&mut packet));

        // The padded probe is on its way out when it is declared lost.
        let on_the_wire: Arc<[u8]> = packet.into();
        let mut kept = Arc::clone(&on_the_wire);
        strip_probe_padding(&mut kept);

        assert_eq!(kept[3], 0);
        assert_eq!(&kept[HEADER_SIZE..], &payload[..]);
        assert_eq!(usize::from(on_the_wire[3]), MTU_STEP, "the one being sent is left as it was");
        assert_eq!(on_the_wire.len(), kept.len() + MTU_STEP);
    }

    // T6: Probe uses data_offset padding — probeify inflates packet to probe size
    #[test]
    fn mtu_probe_uses_data_offset_padding() {
        let mut inner = stream_to(V4);
        inner.mtu_state = MtuState::Search;
        inner.mtu_probe_size = MTU_BASE + MTU_STEP;

        let payload = vec![0xCDu8; MAX_PAYLOAD];
        let mut packet = build_data_packet(0, 0, 0, &payload);
        let original_len = packet.len();
        assert_eq!(original_len, MTU_BASE - IPV4_UDP_OVERHEAD);

        let ok = inner.mtu_probeify_packet(&mut packet);
        assert!(ok, "probeify should succeed for nearly-full packet");
        assert_eq!(
            packet.len(),
            MTU_BASE + MTU_STEP - IPV4_UDP_OVERHEAD,
            "packet should be inflated to probe size"
        );
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
            send_probe(&mut inner, i);
            inner.mark_lost(i);
        }

        assert_eq!(inner.mtu_state, MtuState::SearchComplete);
        assert!(!inner.mtu_probe_wanted);
        assert_eq!(inner.mtu, MTU_BASE + MTU_STEP, "MTU frozen at last successful value");
    }

    #[test]
    fn a_full_packet_is_an_ip_packet_of_the_mtu() {
        // IPv4: 20 bytes of IP header, 8 of UDP, 20 of UDX.
        let v4 = stream_to(V4);
        assert_eq!(v4.max_payload(), 1200 - 20 - 8 - 20);
        assert_eq!(v4.congestion.mss as usize, v4.max_payload());

        // The IPv6 header is 40 bytes.
        let v6 = stream_to(V6);
        assert_eq!(v6.max_payload(), 1200 - 40 - 8 - 20);
        assert_eq!(v6.congestion.mss as usize, v6.max_payload());
    }

    #[test]
    fn an_ipv4_address_mapped_into_ipv6_is_reckoned_as_ipv6() {
        let mapped = stream_to("[::ffff:192.0.2.1]:9");
        assert_eq!(mapped.max_payload(), stream_to(V6).max_payload());
    }

    #[test]
    fn a_probe_is_an_ip_packet_of_the_size_tried() {
        for (remote, overhead) in [(V4, IPV4_UDP_OVERHEAD), (V6, IPV6_UDP_OVERHEAD)] {
            let mut inner = stream_to(remote);
            inner.mtu_state = MtuState::Search;
            let payload = vec![0xEEu8; inner.max_payload()];
            let mut packet = build_data_packet(0, 0, 0, &payload);
            assert_eq!(packet.len() + overhead, MTU_BASE, "a full packet fills the MTU");

            assert!(inner.mtu_probeify_packet(&mut packet));
            assert_eq!(packet.len() + overhead, MTU_BASE + MTU_STEP);
        }
    }

    #[test]
    fn the_search_ends_at_the_mtu_of_the_link() {
        let mut inner = stream_to(V4);
        inner.limit_mtu_to_link(Some(1400));
        inner.mtu_state = MtuState::Search;

        let mut probed = Vec::new();
        while inner.mtu_state == MtuState::Search {
            probed.push(inner.mtu_probe_size);
            inner.on_mtu_probe_acked();
        }

        assert_eq!(probed.first(), Some(&(MTU_BASE + MTU_STEP)));
        assert_eq!(probed.last(), Some(&1400), "the last probe is the link's size exactly");
        assert_eq!(inner.mtu, 1400);
        assert_eq!(inner.max_payload(), 1400 - IPV4_UDP_OVERHEAD - HEADER_SIZE);
        assert_eq!(inner.congestion.mss as usize, inner.max_payload());
    }

    #[test]
    fn a_link_no_larger_than_the_starting_size_is_not_searched() {
        let mut inner = stream_to(V4);
        inner.limit_mtu_to_link(Some(MTU_BASE));

        assert_eq!(inner.mtu_state, MtuState::SearchComplete);
        assert_eq!(inner.mtu, MTU_BASE);
    }

    #[test]
    fn a_link_larger_than_the_largest_packet_leaves_the_search_as_it_is() {
        for link_mtu in [None, Some(MTU_MAX), Some(9000), Some(65_536)] {
            let mut inner = stream_to(V4);
            inner.limit_mtu_to_link(link_mtu);

            assert_eq!(inner.mtu_max, MTU_MAX, "link MTU {link_mtu:?}");
            assert_eq!(inner.mtu_state, MtuState::Base, "link MTU {link_mtu:?}");
        }
    }
}

#[cfg(test)]
mod firewall_gate_tests {
    use super::*;

    fn header_with_flags(type_flags: u8) -> Header {
        Header {
            type_flags,
            data_offset: 0,
            remote_id: 42,
            recv_window: DEFAULT_RWND,
            seq: 1,
            ack: 0,
        }
    }

    #[test]
    fn first_packet_with_data_flag_is_plausible() {
        let hdr = header_with_flags(FLAG_DATA);
        assert!(is_plausible_first_packet(&hdr));
    }

    #[test]
    fn first_packet_with_heartbeat_is_plausible() {
        let hdr = header_with_flags(FLAG_HEARTBEAT);
        assert!(is_plausible_first_packet(&hdr));
    }

    #[test]
    fn first_packet_with_data_and_sack_is_plausible() {
        let hdr = header_with_flags(FLAG_DATA | FLAG_SACK);
        assert!(is_plausible_first_packet(&hdr));
    }

    #[test]
    fn first_packet_with_end_is_plausible() {
        let hdr = header_with_flags(FLAG_END);
        assert!(
            is_plausible_first_packet(&hdr),
            "a zero-length stream may legitimately lead with FLAG_END"
        );
    }

    #[test]
    fn bare_first_packet_is_rejected() {
        let hdr = header_with_flags(0);
        assert!(
            !is_plausible_first_packet(&hdr),
            "type_flags == 0 carries no meaningful intent and must not commit the 4-tuple"
        );
    }

    #[test]
    fn destroy_only_first_packet_is_rejected() {
        let hdr = header_with_flags(FLAG_DESTROY);
        assert!(
            !is_plausible_first_packet(&hdr),
            "FLAG_DESTROY before any agreed state is illegitimate"
        );
    }

    #[test]
    fn destroy_combined_with_data_is_still_rejected() {
        let hdr = header_with_flags(FLAG_DESTROY | FLAG_DATA);
        assert!(
            !is_plausible_first_packet(&hdr),
            "FLAG_DESTROY in any combination is illegitimate for a first packet"
        );
    }

    #[test]
    fn sack_only_first_packet_is_rejected() {
        let hdr = header_with_flags(FLAG_SACK);
        assert!(
            !is_plausible_first_packet(&hdr),
            "SACK-only first packet is meaningless before we have sent any data"
        );
    }
}

#[cfg(test)]
mod sack_tests {
    use super::*;

    #[test]
    fn sack_ranges_are_read_in_both_wire_forms() {
        let ranges = [SackRange { start: 4, end: 6 }, SackRange { start: 9, end: 10 }];
        let mut encoded = [0u8; 16];
        encode_sack(&ranges, &mut encoded);
        let packet = |header: &Header| [&header.encode()[..], &encoded[..]].concat();

        // This crate's form: the length of the ranges in `data_offset`.
        let header = Header {
            type_flags: FLAG_SACK,
            data_offset: 16,
            remote_id: 1,
            recv_window: DEFAULT_RWND,
            seq: 0,
            ack: 3,
        };
        assert_eq!(sack_ranges_in(&header, &packet(&header)), ranges);

        // C libudx's form: `data_offset` zero, the ranges as the payload.
        let header = Header { data_offset: 0, ..header };
        assert_eq!(sack_ranges_in(&header, &packet(&header)), ranges);

        // A range the packet ends in the middle of is left out.
        let mut cut = packet(&header);
        cut.truncate(cut.len() - 3);
        assert_eq!(sack_ranges_in(&header, &cut), ranges[..1]);

        // The payload of a packet that carries data is not read as ranges.
        let header = Header { type_flags: FLAG_SACK | FLAG_DATA, ..header };
        assert!(sack_ranges_in(&header, &packet(&header)).is_empty());

        // Nor is anything without the flag.
        let header = Header { type_flags: 0, ..header };
        assert!(sack_ranges_in(&header, &packet(&header)).is_empty());
    }
}

/// The send path, acknowledgement processing, loss detection and the timers,
/// on a stream that never touches a socket: `collect_sends` hands back what
/// would go on the wire, and every step is given the time it happens at.
#[cfg(test)]
mod engine_tests {
    use super::*;

    const REMOTE: &str = "127.0.0.1:9";

    /// A connected stream with MTU probing switched off, so that every packet
    /// is an ordinary one.
    fn connected() -> StreamInner {
        with_reader().0
    }

    /// A stream as [`connected`] makes it, and what its reader is handed.
    fn with_reader() -> (StreamInner, mpsc::UnboundedReceiver<StreamEvent>) {
        let (read_tx, read_rx) = mpsc::unbounded_channel();
        let mut inner = StreamInner::new(read_tx, None);
        inner.connected = true;
        inner.set_remote_addr(REMOTE.parse().unwrap());
        inner.mtu_state = MtuState::SearchComplete;
        (inner, read_rx)
    }

    /// The bytes the reader has been handed so far, and whether it has been
    /// handed the end of the stream.
    fn read(reader: &mut mpsc::UnboundedReceiver<StreamEvent>) -> (Vec<u8>, bool) {
        let mut bytes = Vec::new();
        let mut ended = false;
        while let Ok(event) = reader.try_recv() {
            match event {
                StreamEvent::Data(data) => bytes.extend(data),
                StreamEvent::End => ended = true,
            }
        }
        (bytes, ended)
    }

    /// A packet as a socket receives it, and its header.
    fn incoming(type_flags: u8, seq: u32, ack: u32, payload: &[u8]) -> (Header, Vec<u8>) {
        let header = Header {
            type_flags,
            data_offset: 0,
            remote_id: 0,
            recv_window: DEFAULT_RWND,
            seq,
            ack,
        };
        let mut data = header.encode().to_vec();
        data.extend_from_slice(payload);
        (header, data)
    }

    /// A packet whose header says that `between` comes before its payload:
    /// SACK ranges, or the padding of an MTU probe.
    fn incoming_with(type_flags: u8, ack: u32, between: &[u8], payload: &[u8]) -> (Header, Vec<u8>) {
        let (mut header, _) = incoming(type_flags, 0, ack, &[]);
        header.data_offset = u8::try_from(between.len()).unwrap();
        let mut data = header.encode().to_vec();
        data.extend_from_slice(between);
        data.extend_from_slice(payload);
        (header, data)
    }

    /// Queue `count` full packets of new data, as one write.
    fn queue(inner: &mut StreamInner, count: u32) {
        write(inner, count as usize * MAX_PAYLOAD);
    }

    /// Queue a write of `len` bytes. The receiver is told once the remote
    /// has acknowledged it.
    fn write(inner: &mut StreamInner, len: usize) -> oneshot::Receiver<Result<()>> {
        let (tx, rx) = oneshot::channel();
        inner.push_write(QueuedWrite { data: vec![7u8; len], taken: 0, end: false, tx: Some(tx) });
        rx
    }

    /// Queue the end of the stream.
    fn end(inner: &mut StreamInner) {
        let (tx, _rx) = oneshot::channel();
        inner.push_write(QueuedWrite { data: Vec::new(), taken: 0, end: true, tx: Some(tx) });
    }

    /// How many full packets of data wait in the write queue.
    fn waiting(inner: &StreamInner) -> usize {
        let bytes: usize = inner.write_queue.iter().map(|w| w.data.len() - w.taken).sum();
        bytes / MAX_PAYLOAD
    }

    /// Counts how often it is woken.
    #[derive(Default)]
    struct Woken(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Woken {
        fn count(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A connected stream that has a socket, as a stream needs to be
    /// written to.
    async fn writable() -> StreamInner {
        let mut inner = connected();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        inner.udp = Some(Arc::new(socket));
        inner
    }

    /// Everything the stream puts on the wire at `now`.
    fn send(inner: &mut StreamInner, now: Instant) -> Vec<Vec<u8>> {
        let mut sent = Vec::new();
        let mut batch = Vec::new();
        loop {
            let more = inner.collect_sends(now, &mut batch);
            sent.extend(batch.drain(..).map(|(packet, _)| packet.to_vec()));
            if !more {
                return sent;
            }
        }
    }

    /// Sequence numbers of the DATA packets among `packets`.
    fn data_seqs(packets: &[Vec<u8>]) -> Vec<u32> {
        packets
            .iter()
            .filter_map(|packet| Header::decode(packet).ok())
            .filter(|header| header.has_flag(FLAG_DATA))
            .map(|header| header.seq)
            .collect()
    }

    fn millis(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    /// Ten packets in flight, of which 3, 4 and 5 have been acknowledged
    /// selectively after 20 ms, with two more waiting to be sent. Returns the
    /// stream and the time of the acknowledgement.
    fn with_a_gap() -> (StreamInner, Instant) {
        let mut inner = connected();
        queue(&mut inner, 12);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        let now = t0 + millis(20);
        inner.process_ack(0, &[SackRange { start: 3, end: 6 }], now);
        (inner, now)
    }

    #[test]
    fn the_first_flight_is_the_initial_window() {
        let mut inner = connected();
        queue(&mut inner, 30);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(data_seqs(&sent), (0..10).collect::<Vec<_>>());
        assert_eq!(inner.inflight(), 10);
        assert_eq!(waiting(&inner), 20);

        // The timers that watch over the flight are armed.
        assert_eq!(inner.rto_deadline, Some(t0 + millis(u64::from(INITIAL_RTO_MS))));
        assert!(inner.tlp_deadline.is_some());
    }

    #[test]
    fn more_than_a_batch_is_handed_out_in_batches() {
        let mut inner = connected();
        inner.congestion.cwnd = 1_000;
        inner.congestion.pacing_bytes_per_ms = 1_000_000;
        queue(&mut inner, 150);

        // A millisecond in, the pacer holds enough for all of it.
        let now = inner.epoch + millis(1);
        let mut batch = Vec::new();
        assert!(inner.collect_sends(now, &mut batch));
        assert_eq!(batch.len(), MAX_SEND_BATCH);

        batch.clear();
        assert!(inner.collect_sends(now, &mut batch));
        batch.clear();
        assert!(!inner.collect_sends(now, &mut batch));
        assert_eq!(batch.len(), 150 - 2 * MAX_SEND_BATCH);
    }

    #[test]
    fn an_acknowledgement_makes_room_and_completes_the_write() {
        let mut inner = connected();
        // A write of five packets, and another behind it.
        let _first = write(&mut inner, 5 * MAX_PAYLOAD);
        queue(&mut inner, 25);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        let written = inner.unacked_bytes;
        assert_eq!(written, 30 * MAX_PAYLOAD);

        let now = t0 + millis(20);
        let completed = inner.process_ack(5, &[], now);
        assert_eq!(completed.len(), 1);
        assert!(inner.pending_writes.is_empty());
        assert_eq!(inner.remote_acked, 5);
        assert_eq!(inner.inflight(), 5);
        assert_eq!(inner.congestion.delivered, 5);
        assert_eq!(inner.unacked_bytes, written - 5 * MAX_PAYLOAD);
        assert_eq!(inner.srtt, 20);

        // In Startup the window grows by what was acknowledged, so twice as
        // much goes out as came back.
        let sent = send(&mut inner, now);
        assert_eq!(data_seqs(&sent), (10..20).collect::<Vec<_>>());
    }

    #[test]
    fn an_acknowledgement_of_what_was_never_sent_is_ignored() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        inner.process_ack(4, &[], t0 + millis(20));
        assert_eq!(inner.remote_acked, 0);
        assert_eq!(inner.inflight(), 3);
    }

    #[test]
    fn three_selective_acknowledgements_mark_what_came_before_them_lost() {
        let (inner, _) = with_a_gap();

        assert_eq!(inner.sacks, 3);
        assert_eq!(inner.retransmit_queue, [0, 1, 2]);
        assert_eq!(inner.congestion.lost, 3);
        assert_eq!(inner.congestion.ca_state, CaState::Recovery);
        // Sent no earlier than the newest packet delivered, 6 to 9 may still
        // arrive.
        assert_eq!(inner.inflight(), 4);
    }

    #[test]
    fn lost_packets_go_out_again_before_new_data() {
        let (mut inner, now) = with_a_gap();

        // Recovery holds the window to what is in flight and what was
        // acknowledged: room for the three lost packets, and no more.
        let sent = send(&mut inner, now);
        assert_eq!(data_seqs(&sent), [0, 1, 2]);
        assert_eq!(inner.outgoing[&0].transmits, 2);
        assert_eq!(inner.inflight(), 7);
        assert_eq!(waiting(&inner), 2);
        // No tail loss probe while a gap is being repaired.
        assert_eq!(inner.tlp_deadline, None);

        // A later acknowledgement of an older packet does not count them
        // lost again: they were sent after it.
        inner.process_ack(0, &[SackRange { start: 6, end: 7 }], now + millis(5));
        assert!(inner.retransmit_queue.is_empty());
        assert_eq!(inner.outgoing[&0].transmits, 2);
        assert_eq!(inner.congestion.lost, 3);
    }

    #[test]
    fn recovery_ends_once_everything_sent_before_it_is_acknowledged() {
        let (mut inner, now) = with_a_gap();
        send(&mut inner, now);

        inner.process_ack(10, &[], now + millis(20));
        assert_eq!(inner.congestion.ca_state, CaState::Recovery);

        // The two packets that were waiting are past the point recovery
        // began at.
        send(&mut inner, now + millis(20));
        inner.process_ack(11, &[], now + millis(40));
        assert_eq!(inner.congestion.ca_state, CaState::Open);
        assert_eq!(inner.sacks, 0);
    }

    /// Two flights of ten packets, sent 20 ms apart, with the first five
    /// packets acknowledged and ten more waiting. Returns the stream and the
    /// time the second flight went out.
    fn with_two_flights() -> (StreamInner, Instant) {
        let mut inner = connected();
        queue(&mut inner, 30);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        let now = t0 + millis(20);
        inner.process_ack(5, &[], now);
        let sent = send(&mut inner, now);
        assert_eq!(data_seqs(&sent), (10..20).collect::<Vec<_>>());
        assert_eq!(inner.congestion.delivered_in_time, 5);
        (inner, now)
    }

    #[test]
    fn a_packet_reported_late_is_delivered_but_not_in_time() {
        let (mut inner, sent_at) = with_two_flights();

        // Two packets of the second flight are acknowledged. The remote has
        // the rest of the first flight as well, and does not say so yet.
        let now = sent_at + millis(20);
        inner.process_ack(5, &[SackRange { start: 10, end: 12 }], now);
        assert_eq!(inner.congestion.delivered, 7);
        assert_eq!(inner.congestion.delivered_in_time, 7);

        // When it does, those packets had arrived before the two did.
        inner.process_ack(5, &[SackRange { start: 7, end: 10 }], now + millis(1));
        assert_eq!(inner.congestion.delivered, 10);
        assert_eq!(inner.congestion.delivered_in_time, 7);
    }

    #[test]
    fn an_acknowledgement_sooner_than_a_round_trip_answers_the_first_sending() {
        let (mut inner, sent_at) = with_two_flights();
        let now = sent_at + millis(20);
        inner.process_ack(5, &[SackRange { start: 10, end: 12 }], now);

        // 5 to 9 count as lost, and the window has room for two of them.
        let sent = send(&mut inner, now);
        assert_eq!(data_seqs(&sent), [5, 6]);
        assert_eq!(inner.outgoing[&5].time_sent, now);
        assert_eq!(inner.outgoing[&5].first_sent, inner.epoch);

        // They are acknowledged 5 ms later, where a round trip takes 20. It
        // is their first sending that arrived, before the second flight did.
        inner.process_ack(7, &[], now + millis(5));
        assert_eq!(inner.congestion.delivered, 9);
        assert_eq!(inner.congestion.delivered_in_time, 7);

        // The next two are acknowledged a round trip after they were sent
        // again, which that sending may have taken.
        let sent = send(&mut inner, now + millis(5));
        assert_eq!(data_seqs(&sent), [7, 8]);
        inner.process_ack(9, &[], now + millis(25));
        assert_eq!(inner.congestion.delivered, 11);
        assert_eq!(inner.congestion.delivered_in_time, 9);
    }

    #[test]
    fn a_single_gap_waits_out_the_reordering_window() {
        let mut inner = connected();
        queue(&mut inner, 10);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        // Packet 1 is acknowledged and packet 0 is not: lost, or overtaken?
        let now = t0 + millis(40);
        inner.process_ack(0, &[SackRange { start: 1, end: 2 }], now);
        assert!(inner.retransmit_queue.is_empty());
        assert_eq!(inner.congestion.ca_state, CaState::Open);
        // A quarter of the lowest round-trip time is allowed for it to turn up.
        assert_eq!(inner.rack_reo_deadline, Some(now + millis(10)));

        assert!(!inner.on_timers(now + millis(10)));
        assert_eq!(inner.retransmit_queue, [0]);
        assert_eq!(inner.congestion.ca_state, CaState::Recovery);
    }

    #[test]
    fn the_retransmission_timer_resends_what_has_timed_out() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        // Nothing comes back for a whole timeout.
        let timeout = t0 + millis(u64::from(INITIAL_RTO_MS));
        assert!(!inner.on_timers(timeout));
        assert_eq!(inner.congestion.ca_state, CaState::Loss);
        assert_eq!(inner.retransmit_queue, [0, 1, 2]);
        assert_eq!(inner.inflight(), 0);
        // The timer backs off.
        assert_eq!(inner.rto_deadline, Some(timeout + millis(2 * u64::from(INITIAL_RTO_MS))));

        let sent = send(&mut inner, timeout);
        assert_eq!(data_seqs(&sent), [0, 1, 2]);
        assert_eq!(inner.outgoing[&2].transmits, 2);
    }

    #[test]
    fn the_stream_gives_up_once_a_packet_has_timed_out_six_times() {
        let mut inner = connected();
        queue(&mut inner, 1);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        for _ in 0..MAX_RTO_TIMEOUTS {
            let now = inner.rto_deadline.expect("armed");
            assert!(!inner.on_timers(now), "gave up early");
            // Out it goes again.
            assert_eq!(data_seqs(&send(&mut inner, now)), [0]);
        }

        let now = inner.rto_deadline.expect("armed");
        assert!(inner.on_timers(now));
        // 1 s, then 2 s six times: C libudx gives up at the same moment.
        assert_eq!(now.duration_since(t0), millis(13_000));
    }

    #[test]
    fn a_tail_loss_probe_sends_the_newest_packet_again() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        // The first packet is acknowledged after 20 ms. Nothing is heard of
        // the other two, and no later packet could show that they are lost.
        let acked_at = t0 + millis(20);
        inner.process_ack(1, &[], acked_at);
        // The probe waits two round trips.
        let probe_at = acked_at + millis(40);
        assert_eq!(inner.tlp_deadline, Some(probe_at));

        assert!(!inner.on_timers(probe_at));
        let sent = send(&mut inner, probe_at);
        assert_eq!(data_seqs(&sent), [2]);
        assert!(inner.tlp_in_flight);
        assert_eq!(inner.outgoing[&2].transmits, 2);

        // The acknowledgement of the probe settles it.
        inner.process_ack(3, &[], probe_at + millis(20));
        assert!(!inner.tlp_in_flight);
        assert!(inner.all_acked());
        assert_eq!(inner.rto_deadline, None);
    }

    #[test]
    fn a_tail_loss_probe_carries_new_data_when_there_is_some() {
        let mut inner = connected();
        queue(&mut inner, 12);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        let acked_at = t0 + millis(20);
        inner.process_ack(1, &[], acked_at);
        // The window is full, with two packets waiting.
        inner.congestion.cwnd = inner.inflight();
        assert!(send(&mut inner, acked_at).is_empty());

        let probe_at = inner.tlp_deadline.expect("armed");
        assert!(!inner.on_timers(probe_at));

        // The probe is the next waiting packet, whatever the window says.
        let sent = send(&mut inner, probe_at);
        assert_eq!(data_seqs(&sent), [10]);
        assert!(inner.tlp_in_flight);
        assert!(!inner.tlp_is_retrans);
        assert_eq!(waiting(&inner), 1);
    }

    #[test]
    fn a_write_larger_than_a_packet_is_cut_into_several() {
        let mut inner = connected();
        let _written = write(&mut inner, 2 * MAX_PAYLOAD + 1);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(data_seqs(&sent), [0, 1, 2]);
        let sizes: Vec<usize> = sent.iter().map(|packet| payload_len(packet)).collect();
        assert_eq!(sizes, [MAX_PAYLOAD, MAX_PAYLOAD, 1]);

        // The write is complete when its last packet is acknowledged.
        assert!(inner.process_ack(2, &[], t0 + millis(20)).is_empty());
        assert_eq!(inner.process_ack(3, &[], t0 + millis(21)).len(), 1);
        assert_eq!(inner.unacked_bytes, 0);
    }

    #[test]
    fn writes_smaller_than_a_packet_share_one() {
        let mut inner = connected();
        let _length = write(&mut inner, 3);
        let _body = write(&mut inner, 500);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        assert_eq!(payload_len(&sent[0]), 503);

        // Both are acknowledged with that packet.
        assert_eq!(inner.process_ack(1, &[], t0 + millis(20)).len(), 2);
    }

    #[test]
    fn a_packet_is_numbered_and_addressed_when_it_is_sent() {
        let mut inner = connected();
        inner.remote_id = 7;
        queue(&mut inner, 2);
        // Nothing is numbered until it goes out.
        assert_eq!(inner.next_seq, 0);

        // The remote's data arrives before the stream gets to send.
        inner.next_remote_seq = 5;
        let t0 = inner.epoch;
        let sent = send(&mut inner, t0);
        assert_eq!(inner.next_seq, 2);

        let header = Header::decode(&sent[1]).unwrap();
        assert_eq!((header.remote_id, header.seq, header.ack), (7, 1, 5));
    }

    #[test]
    fn what_is_kept_of_a_packet_is_what_goes_on_the_wire() {
        let mut inner = connected();
        queue(&mut inner, 1);
        let t0 = inner.epoch;

        let mut out = Vec::new();
        inner.collect_sends(t0, &mut out);
        assert_eq!(out.len(), 1);
        assert!(Arc::ptr_eq(&out[0].0, &inner.outgoing[&0].packet), "one copy, not two");

        // Sent again, it is still that copy.
        assert!(!inner.mark_lost(0));
        out.clear();
        inner.collect_sends(t0 + millis(20), &mut out);
        assert_eq!(out.len(), 1);
        assert!(Arc::ptr_eq(&out[0].0, &inner.outgoing[&0].packet));
        assert_eq!(inner.outgoing[&0].transmits, 2);
    }

    #[test]
    fn a_packet_is_cut_at_the_mtu_of_the_moment_it_is_sent() {
        let mut inner = connected();
        inner.congestion.cwnd = 1;
        let _written = write(&mut inner, 4_000);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        assert_eq!(payload_len(&sent[0]), MAX_PAYLOAD);

        // The path's MTU is found larger while the rest of the write waits.
        inner.mtu = MTU_MAX;
        inner.congestion.on_mtu_change(inner.max_payload());
        let now = t0 + millis(20);
        inner.process_ack(1, &[], now);

        let sent = send(&mut inner, now);
        assert_eq!(payload_len(&sent[0]), MTU_MAX - IPV4_UDP_OVERHEAD - HEADER_SIZE);
    }

    #[test]
    fn the_end_of_the_stream_rides_on_the_last_data() {
        let mut inner = connected();
        let _written = write(&mut inner, 100);
        end(&mut inner);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        let header = Header::decode(&sent[0]).unwrap();
        assert_eq!(header.type_flags, FLAG_DATA | FLAG_END);
        assert_eq!(payload_len(&sent[0]), 100);

        // The write and the end are acknowledged with it.
        assert_eq!(inner.process_ack(1, &[], t0 + millis(20)).len(), 2);
        assert!(inner.drained());
    }

    #[test]
    fn the_end_of_the_stream_alone_is_a_packet_without_data() {
        let mut inner = connected();
        end(&mut inner);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].len(), HEADER_SIZE);
        assert_eq!(Header::decode(&sent[0]).unwrap().type_flags, FLAG_END);
        assert_eq!(inner.process_ack(1, &[], t0 + millis(20)).len(), 1);
    }

    #[test]
    fn an_empty_write_takes_a_packet_to_be_acknowledged_by() {
        let mut inner = connected();
        let _written = write(&mut inner, 0);
        let t0 = inner.epoch;

        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].len(), HEADER_SIZE);
        assert_eq!(Header::decode(&sent[0]).unwrap().type_flags, FLAG_DATA);
        assert_eq!(inner.process_ack(1, &[], t0 + millis(20)).len(), 1);
    }

    #[tokio::test]
    async fn a_writer_waits_at_the_high_water_mark_for_an_acknowledgement() {
        let mut inner = writable().await;
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));

        // The limit is the high-water mark on top of the window, which is
        // ten packets on a new stream.
        let limit = HIGH_WATERMARK + 10 * MAX_PAYLOAD;
        assert_eq!(inner.write_limit(), limit);

        // A write is taken a high-water mark at a time, and whole while
        // the stream is under its limit, even when that takes it over.
        let buf = vec![7u8; 2 * HIGH_WATERMARK];
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(HIGH_WATERMARK));
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(HIGH_WATERMARK));
        assert_eq!(inner.unacked_bytes, 2 * HIGH_WATERMARK);

        // Over the limit, the writer is asked to wait.
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), None);
        assert_eq!(woken.count(), 0);

        // Acknowledgements bring the stream back under its limit.
        let t0 = inner.epoch;
        let mut now = t0;
        while inner.unacked_bytes >= inner.write_limit() {
            let sent = send(&mut inner, now);
            assert!(!sent.is_empty());
            now += millis(20);
            let ack = inner.next_seq;
            inner.process_ack(ack, &[], now);
        }
        inner.wake_writer();
        assert_eq!(woken.count(), 1);
        assert!(inner.write_some(&buf, &waker).unwrap().is_some());

        // Nobody is woken twice for the same wait.
        inner.wake_writer();
        assert_eq!(woken.count(), 1);
    }

    #[tokio::test]
    async fn an_acknowledgement_that_arrives_wakes_the_writer() {
        let mut inner = writable().await;
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));

        // No room beyond what is in flight.
        inner.send_rwnd = 0;
        let buf = vec![7u8; HIGH_WATERMARK];
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(HIGH_WATERMARK));
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), None);
        inner.tlp_wanted = true;
        let t0 = inner.epoch;
        assert_eq!(send(&mut inner, t0).len(), 1);
        let inner = Arc::new(Mutex::new(inner));

        // The acknowledgement of that packet, with a window again.
        let header = Header {
            type_flags: 0,
            data_offset: 0,
            remote_id: 0,
            recv_window: DEFAULT_RWND,
            seq: 0,
            ack: 1,
        };
        let packet = IncomingPacket { data: header.encode().to_vec(), addr: REMOTE.parse().unwrap() };
        assert!(handle_incoming(&inner, packet).await);

        assert_eq!(woken.count(), 1);
    }

    #[tokio::test]
    async fn a_stream_closed_before_its_writes_were_acknowledged_says_so() {
        let mut inner = writable().await;
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let written = inner.queue_write(vec![7u8; 100]).unwrap();
        assert!(inner.write_error().is_none());

        inner.send_rwnd = 0;
        let buf = vec![7u8; HIGH_WATERMARK];
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(HIGH_WATERMARK));
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), None);

        inner.terminate();

        // The writer that waited for room is woken, to find the stream
        // closed. The write that waited for its acknowledgement fails.
        assert_eq!(woken.count(), 1);
        assert!(matches!(inner.write_some(&buf, &waker), Err(UdxError::StreamClosed)));
        assert!(matches!(written.await, Ok(Err(UdxError::StreamClosed))));
        let error = inner.write_error().expect("writes were lost");
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn a_stream_closed_with_nothing_owed_reports_no_error() {
        let mut inner = connected();
        queue(&mut inner, 2);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        inner.process_ack(2, &[], t0 + millis(20));

        inner.terminate();
        assert!(inner.write_error().is_none());
    }

    #[tokio::test]
    async fn nothing_can_be_written_after_the_end_of_the_stream() {
        let mut inner = writable().await;
        let waker = Waker::from(Arc::new(Woken::default()));
        let _ended = inner.queue_end().unwrap();

        assert!(inner.write_some(&[1], &waker).is_err());
        assert!(inner.queue_write(vec![1]).is_err());
        assert!(inner.queue_end().is_err());
    }

    #[test]
    fn the_probe_does_not_wait_a_second_on_a_path_faster_than_a_millisecond() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        // Before a round trip has been measured, it waits a second.
        assert_eq!(inner.tlp_deadline, Some(t0 + millis(DEFAULT_PTO_MS)));

        // Acknowledged after 300 microseconds: zero, in whole milliseconds.
        let now = t0 + Duration::from_micros(300);
        inner.process_ack(1, &[], now);
        assert_eq!(inner.srtt, 0);
        assert_eq!(inner.tlp_deadline, Some(now + millis(2)));

        // With one packet left in flight, its acknowledgement may be held back.
        inner.process_ack(2, &[], now);
        assert_eq!(inner.tlp_deadline, Some(now + millis(2 + TLP_MAX_ACK_DELAY_MS)));
    }

    #[test]
    fn an_emptied_pacer_wakes_the_stream_at_the_next_millisecond() {
        let mut inner = connected();
        inner.congestion.cwnd = 100;
        inner.congestion.pacing_bytes_per_ms = 1_000;
        queue(&mut inner, 20);
        let t0 = inner.epoch;

        // Five milliseconds in, the pacer holds 5000 bytes: four packets,
        // and a fifth on what is left.
        let now = t0 + Duration::from_micros(5_300);
        assert_eq!(send(&mut inner, now).len(), 5);
        assert_eq!(inner.pacing_deadline, Some(t0 + millis(6)));
        assert_eq!(inner.next_deadline(), inner.pacing_deadline);

        // Nothing more within that millisecond.
        assert!(send(&mut inner, t0 + Duration::from_micros(5_900)).is_empty());

        // A millisecond's worth at the next tick: one packet.
        let tick = t0 + millis(6);
        assert!(!inner.on_timers(tick));
        assert_eq!(inner.pacing_deadline, None);
        assert_eq!(send(&mut inner, tick).len(), 1);
    }

    #[test]
    fn an_owed_acknowledgement_goes_out_first_and_once() {
        let mut inner = connected();
        queue(&mut inner, 2);
        let t0 = inner.epoch;

        // Two packets arrive in order, then one past a gap.
        inner.receive_data(0, vec![1], FLAG_DATA);
        inner.receive_data(1, vec![2], FLAG_DATA);
        inner.receive_data(3, vec![4], FLAG_DATA);
        inner.ack_wanted = true;

        let sent = send(&mut inner, t0);
        let ack = Header::decode(&sent[0]).unwrap();
        assert!(ack.has_flag(FLAG_SACK));
        assert!(!ack.has_flag(FLAG_DATA));
        assert_eq!(ack.ack, 2);
        assert_eq!(sack_ranges_in(&ack, &sent[0]), [SackRange { start: 3, end: 4 }]);
        assert_eq!(data_seqs(&sent[1..]), [0, 1]);

        // Nothing is owed after that.
        assert!(!inner.ack_wanted);
        assert!(send(&mut inner, t0).is_empty());
    }

    #[test]
    fn an_acknowledgement_names_as_many_ranges_as_fit_one_packet() {
        let mut inner = connected();
        // Every other packet of four hundred: two hundred ranges.
        for seq in (1..400).step_by(2) {
            inner.receive_data(seq, vec![0], FLAG_DATA);
        }

        let packet = inner.build_ack();
        let header = Header::decode(&packet).unwrap();
        // A packet of the starting MTU: 144 ranges over IPv4.
        assert_eq!(packet.len(), MTU_BASE - IPV4_UDP_OVERHEAD);
        // More bytes of ranges than the one-byte length can say, so the
        // packet says none, as C libudx's do.
        assert_eq!(header.data_offset, 0);

        let ranges = sack_ranges_in(&header, &packet);
        assert_eq!(ranges.len(), 144);
        assert_eq!(ranges[0], SackRange { start: 1, end: 2 });
        assert_eq!(ranges[143], SackRange { start: 287, end: 288 });
    }

    #[test]
    fn a_short_list_of_ranges_also_says_its_length_in_the_header() {
        let mut inner = connected();
        // 31 ranges are 248 bytes, the most the one-byte length can say.
        for seq in (1..62).step_by(2) {
            inner.receive_data(seq, vec![0], FLAG_DATA);
        }
        let packet = inner.build_ack();
        let header = Header::decode(&packet).unwrap();
        assert_eq!(header.data_offset, 248);
        assert_eq!(packet.len(), HEADER_SIZE + 248);
        assert_eq!(sack_ranges_in(&header, &packet).len(), 31);

        // One range more, and the length is left out.
        inner.receive_data(63, vec![0], FLAG_DATA);
        let packet = inner.build_ack();
        let header = Header::decode(&packet).unwrap();
        assert_eq!(header.data_offset, 0);
        assert_eq!(packet.len(), HEADER_SIZE + 256);
        assert_eq!(sack_ranges_in(&header, &packet).len(), 32);
    }

    #[test]
    fn a_sender_takes_every_range_of_a_long_acknowledgement() {
        let mut inner = connected();
        inner.congestion.cwnd = 400;
        let _written = write(&mut inner, 400 * MAX_PAYLOAD);
        let t0 = inner.epoch;
        // The pacer lets a few packets out each millisecond.
        let mut sent = 0;
        for ms in 0..1_000 {
            sent += send(&mut inner, t0 + millis(ms)).len();
            if sent == 400 {
                break;
            }
        }
        assert_eq!(sent, 400);

        // The remote has every other packet of the first 288: 144 ranges.
        let ranges: Vec<SackRange> = (1..288)
            .step_by(2)
            .map(|seq| SackRange { start: seq, end: seq + 1 })
            .collect();
        assert_eq!(ranges.len(), 144);
        inner.process_ack(0, &ranges, t0 + millis(1_040));

        assert_eq!(inner.sacks, 144);
        assert!(!inner.outgoing.contains_key(&287), "the last range named is taken");
        assert!(inner.outgoing.contains_key(&289), "a packet past the ranges is still awaited");
    }

    #[tokio::test]
    async fn a_selective_acknowledgement_from_c_libudx_is_applied() {
        let mut inner = connected();
        queue(&mut inner, 5);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        let inner = Arc::new(Mutex::new(inner));

        // As C libudx sends it: `data_offset` zero, the ranges as the payload.
        let header = Header {
            type_flags: FLAG_SACK,
            data_offset: 0,
            remote_id: 0,
            recv_window: DEFAULT_RWND,
            seq: 0,
            ack: 1,
        };
        let mut data = header.encode().to_vec();
        data.resize(HEADER_SIZE + 8, 0);
        encode_sack(&[SackRange { start: 2, end: 4 }], &mut data[HEADER_SIZE..]);
        let packet = IncomingPacket { data, addr: REMOTE.parse().unwrap() };

        assert!(handle_incoming(&inner, packet).await);

        let inner = inner.lock().unwrap();
        assert_eq!(inner.remote_acked, 1);
        assert_eq!(inner.sacks, 2);
        assert_eq!(inner.outgoing.keys().copied().collect::<Vec<_>>(), [1, 4]);
    }

    // ── Packets taken in where they are received ─────────────────────────

    #[test]
    fn data_is_taken_in_where_it_is_received() {
        let (inner, mut reader) = with_reader();
        let inner = Mutex::new(inner);

        let (header, data) = incoming(FLAG_DATA, 0, 0, b"first");
        let taken = take_in(&inner, &header, data);
        assert!(taken.ack_owed);
        assert!(taken.for_its_task.is_none(), "it says nothing new to the sending side");

        // The end of the stream rides on the last data.
        let (header, data) = incoming(FLAG_DATA | FLAG_END, 1, 0, b" last");
        let taken = take_in(&inner, &header, data);
        assert!(!taken.ack_owed, "one acknowledgement answers both");
        assert!(taken.for_its_task.is_none());

        assert_eq!(read(&mut reader), (b"first last".to_vec(), true));
        assert_eq!(inner.lock().unwrap().next_remote_seq, 2);
    }

    #[test]
    fn a_probe_is_taken_in_without_its_padding() {
        let (inner, mut reader) = with_reader();
        let inner = Mutex::new(inner);

        let (header, data) = incoming_with(FLAG_DATA, 0, &[0u8; 4], b"payload");
        assert!(take_in(&inner, &header, data).ack_owed);

        assert_eq!(read(&mut reader).0, b"payload");
    }

    #[test]
    fn a_heartbeat_draws_an_acknowledgement_and_hands_the_reader_nothing() {
        let (inner, mut reader) = with_reader();
        let inner = Mutex::new(inner);

        let (header, data) = incoming(FLAG_HEARTBEAT, 0, 0, &[]);
        let taken = take_in(&inner, &header, data);

        assert!(taken.ack_owed);
        assert!(taken.for_its_task.is_none());
        assert_eq!(read(&mut reader), (Vec::new(), false));
        assert_eq!(inner.lock().unwrap().next_remote_seq, 0);
    }

    #[test]
    fn the_acknowledgement_of_what_was_taken_in_is_taken_once() {
        let (inner, _reader) = with_reader();
        let inner = Mutex::new(inner);
        for seq in 0..3 {
            let (header, data) = incoming(FLAG_DATA, seq, 0, b"data");
            take_in(&inner, &header, data);
        }

        let mut inner = inner.lock().unwrap();
        let (ack, to) = inner.take_ack().expect("an acknowledgement is owed");
        assert_eq!(to, REMOTE.parse::<SocketAddr>().unwrap());
        assert_eq!(Header::decode(&ack).unwrap().ack, 3);

        assert!(inner.take_ack().is_none());
        let t0 = inner.epoch;
        assert!(send(&mut inner, t0).is_empty(), "the stream's own task sends none either");
    }

    #[test]
    fn an_acknowledgement_is_not_kept_waiting_for_a_gap_a_repeat_or_the_end() {
        let (inner, _reader) = with_reader();
        let inner = Mutex::new(inner);
        let take = |type_flags, seq| {
            let (header, data) = incoming(type_flags, seq, 0, b"data");
            take_in(&inner, &header, data).ack_now
        };

        assert!(!take(FLAG_DATA, 0), "in order: more is on its way");
        assert!(take(FLAG_DATA, 2), "a packet is missing before it");
        assert!(take(FLAG_DATA, 3), "and still is");
        assert!(!take(FLAG_DATA, 1), "the gap is filled");
        assert!(take(FLAG_DATA, 1), "the remote sent it twice");
        assert!(!take(FLAG_HEARTBEAT, 0), "a heartbeat is no data");
        assert!(take(FLAG_DATA | FLAG_END, 4), "nothing follows the end");
    }

    #[test]
    fn an_acknowledgement_alone_is_left_to_the_streams_own_task() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        let inner = Mutex::new(inner);

        let (header, data) = incoming(0, 0, 2, &[]);
        let taken = take_in(&inner, &header, data.clone());

        assert!(!taken.ack_owed);
        assert_eq!(taken.for_its_task, Some(data));
        let inner = inner.lock().unwrap();
        assert_eq!(inner.remote_acked, 0, "the sending side is not touched here");
        assert_eq!(inner.outgoing.len(), 3);
    }

    #[tokio::test]
    async fn what_data_says_to_the_sending_side_goes_to_the_streams_own_task() {
        let (mut inner, mut reader) = with_reader();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        let inner = Arc::new(Mutex::new(inner));

        // Data that also acknowledges two of the three packets sent.
        let (header, data) = incoming(FLAG_DATA, 0, 2, b"reply");
        let taken = take_in(&inner, &header, data);
        assert!(taken.ack_owed);
        assert_eq!(read(&mut reader).0, b"reply");
        assert_eq!(inner.lock().unwrap().remote_acked, 0, "the sending side is not touched here");

        // Its own task is given the header alone, as an acknowledgement.
        let part = taken.for_its_task.expect("the acknowledgement is news");
        assert_eq!(part.len(), HEADER_SIZE);
        let said = Header::decode(&part).unwrap();
        assert_eq!((said.type_flags, said.data_offset, said.ack), (0, 0, 2));

        let packet = IncomingPacket { data: part, addr: REMOTE.parse().unwrap() };
        assert!(handle_incoming(&inner, packet).await);
        {
            let inner = inner.lock().unwrap();
            assert_eq!(inner.remote_acked, 2);
            assert_eq!(inner.outgoing.len(), 1);
            assert_eq!(inner.next_remote_seq, 1, "the data is not taken in twice");
        }
        assert!(read(&mut reader).0.is_empty());

        // More data that acknowledges nothing further stays out of its way.
        let (header, data) = incoming(FLAG_DATA, 1, 2, b"more");
        assert!(take_in(&inner, &header, data).for_its_task.is_none());

        // Another window is news again.
        let (mut header, mut data) = incoming(FLAG_DATA, 2, 2, b"more");
        header.recv_window = DEFAULT_RWND / 2;
        header.encode_into(&mut data);
        let part = take_in(&inner, &header, data).for_its_task.expect("the window is news");
        assert_eq!(Header::decode(&part).unwrap().recv_window, DEFAULT_RWND / 2);
    }

    #[test]
    fn packets_a_stream_cannot_take_in_go_to_its_own_task_whole() {
        fn left_whole(inner: StreamInner, type_flags: u8) {
            let inner = Mutex::new(inner);
            let (header, data) = incoming(type_flags, 0, 0, b"data");
            let taken = take_in(&inner, &header, data.clone());

            assert!(!taken.ack_owed);
            assert_eq!(taken.for_its_task, Some(data));
            let inner = inner.lock().unwrap();
            assert_eq!(inner.next_remote_seq, 0);
            assert!(!inner.ack_wanted);
        }

        // Not connected yet: its own task asks the firewall hook first.
        let mut unconnected = connected();
        unconnected.connected = false;
        left_whole(unconnected, FLAG_DATA);

        // Relaying: its own task passes the packet on to the other stream.
        let mut relaying = connected();
        relaying.relay_target = Some(Arc::new(Mutex::new(connected())));
        left_whole(relaying, FLAG_DATA);

        // A packet that destroys the stream, whatever else it carries.
        left_whole(connected(), FLAG_DATA | FLAG_DESTROY);
    }

    #[test]
    fn the_ranges_a_packet_names_go_to_the_sending_side_with_its_header() {
        let range = [SackRange { start: 4, end: 6 }];
        let mut ranges = [0u8; 8];
        encode_sack(&range, &mut ranges);

        // This crate's form, on a packet with data: the header says how many
        // bytes of ranges come before the data.
        let (header, data) = incoming_with(FLAG_DATA | FLAG_SACK, 3, &ranges, b"payload");
        let part = sender_part(&header, &data);
        let said = Header::decode(&part).unwrap();
        assert_eq!(part.len(), HEADER_SIZE + 8);
        assert_eq!((said.type_flags, said.ack), (FLAG_SACK, 3));
        assert_eq!(sack_ranges_in(&said, &part), range);

        // C libudx's form, on a heartbeat: the ranges are the whole payload.
        let (header, data) = incoming(FLAG_HEARTBEAT | FLAG_SACK, 0, 3, &ranges);
        let part = sender_part(&header, &data);
        let said = Header::decode(&part).unwrap();
        assert_eq!((said.type_flags, said.ack), (FLAG_SACK, 3));
        assert_eq!(sack_ranges_in(&said, &part), range);

        // The padding of an MTU probe is not ranges.
        let (header, data) = incoming_with(FLAG_DATA, 3, &[0u8; 8], b"payload");
        let part = sender_part(&header, &data);
        assert_eq!(part.len(), HEADER_SIZE);
        assert_eq!(Header::decode(&part).unwrap().data_offset, 0);

        // Ranges that the packet is too short to hold are no ranges, as
        // when the packet is read whole.
        let (mut header, mut data) = incoming(FLAG_HEARTBEAT | FLAG_SACK, 0, 3, &ranges);
        header.data_offset = 16;
        header.encode_into(&mut data);
        assert!(sack_ranges_in(&header, &data).is_empty());
        let part = sender_part(&header, &data);
        assert!(sack_ranges_in(&Header::decode(&part).unwrap(), &part).is_empty());
    }

    #[tokio::test]
    async fn the_acknowledgement_goes_out_from_the_task_that_took_the_data_in() {
        let remote = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.writable().await.unwrap();

        let (mut inner, _reader) = with_reader();
        inner.set_remote_addr(remote.local_addr().unwrap());
        let inner = Mutex::new(inner);
        let (header, data) = incoming(FLAG_DATA, 0, 0, b"data");
        assert!(take_in(&inner, &header, data).ack_owed);

        send_ack(&inner, &socket);

        let mut buf = [0u8; 64];
        let (len, from) = remote.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, socket.local_addr().unwrap());
        assert_eq!(len, HEADER_SIZE);
        assert_eq!(Header::decode(&buf[..len]).unwrap().ack, 1);

        // The stream owes none after that, so the next packet to arrive is
        // not another acknowledgement.
        assert!(!inner.lock().unwrap().ack_wanted);
        send_ack(&inner, &socket);
        socket.send_to(b"next", remote.local_addr().unwrap()).await.unwrap();
        let (len, _) = remote.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"next");
    }

    #[test]
    fn an_acknowledgement_the_socket_had_no_room_for_is_left_to_the_streams_own_task() {
        let (mut inner, _reader) = with_reader();
        let (notify_tx, mut notify_rx) = mpsc::unbounded_channel();
        inner.notify_tx = Some(notify_tx);
        inner.receive_data(0, vec![1], FLAG_DATA);
        inner.ack_wanted = true;

        assert!(inner.take_ack().is_some());
        inner.ack_not_sent();

        // Its own task is told, once, and sends it.
        assert!(matches!(notify_rx.try_recv(), Ok(StreamNotify::DataQueued)));
        inner.ack_not_sent();
        assert!(notify_rx.try_recv().is_err());
        let t0 = inner.epoch;
        let sent = send(&mut inner, t0);
        assert_eq!(sent.len(), 1);
        assert_eq!(Header::decode(&sent[0]).unwrap().ack, 1);
    }

    // ── Sequence numbers a remote chooses, up to the wrap and across it ──────

    /// A packet that carries an acknowledgement and nothing else, with its
    /// ranges as C libudx sends them: the whole payload.
    fn acknowledgement(ack: u32, ranges: &[SackRange]) -> IncomingPacket {
        let type_flags = if ranges.is_empty() { 0 } else { FLAG_SACK };
        let (_, mut data) = incoming(type_flags, 0, ack, &[]);
        data.resize(HEADER_SIZE + ranges.len() * SACK_RANGE_SIZE, 0);
        encode_sack(ranges, &mut data[HEADER_SIZE..]);
        IncomingPacket { data, addr: REMOTE.parse().unwrap() }
    }

    #[test]
    fn a_range_is_valid_only_within_what_was_sent_and_is_not_yet_acknowledged() {
        let mut inner = connected();
        let valid =
            |inner: &StreamInner, start: u32, end: u32| inner.sack_is_valid(&SackRange { start, end });

        // In flight: 2 to 9.
        inner.remote_acked = 2;
        inner.next_seq = 10;
        assert!(valid(&inner, 3, 6));
        assert!(valid(&inner, 2, 10), "all that is in flight");
        assert!(!valid(&inner, 1, 4), "it starts below what is acknowledged");
        assert!(!valid(&inner, 8, 11), "it ends past what was sent");
        assert!(!valid(&inner, 10, 11), "it names what was never sent");
        assert!(!valid(&inner, 5, 5), "it is empty");
        assert!(!valid(&inner, 6, 3), "it ends before it starts");
        assert!(!valid(&inner, 0xFFFF_FFF0, 0x10), "it wraps, and nothing in flight does");
        assert!(!valid(&inner, 0xFFFF_FFF0, 5), "it wraps, and nothing in flight does");
        // Its end is not past what was sent, and its start is not below what
        // is acknowledged, as sequence numbers compare. Its start is half the
        // sequence space away from what was sent, all the same.
        assert!(!valid(&inner, 0x4000_000B, 0xC000_000A), "it starts nowhere near what was sent");

        // In flight across the wrap of the sequence space: the last five
        // numbers and the first five.
        inner.remote_acked = u32::MAX - 4;
        inner.next_seq = 5;
        assert!(valid(&inner, u32::MAX - 2, 2), "across the wrap");
        assert!(valid(&inner, u32::MAX - 4, 5), "all that is in flight");
        assert!(valid(&inner, u32::MAX - 1, 0), "up to the wrap");
        assert!(valid(&inner, 0, 5), "from the wrap on");
        assert!(!valid(&inner, u32::MAX - 5, 1), "it starts below what is acknowledged");
        assert!(!valid(&inner, u32::MAX, 6), "it ends past what was sent");
        assert!(!valid(&inner, 5, 6), "it names what was never sent");
        assert!(!valid(&inner, 2, u32::MAX), "it ends before it starts");
    }

    #[tokio::test]
    async fn a_range_that_wraps_past_the_highest_number_does_not_stop_the_stream() {
        let mut inner = connected();
        let mut written = write(&mut inner, 3 * MAX_PAYLOAD);
        let t0 = inner.epoch;
        assert_eq!(send(&mut inner, t0).len(), 3);
        let inner = Arc::new(Mutex::new(inner));

        // Its end is after its start as sequence numbers go, and before it
        // as numbers. A remote can send it at any time, and the stream has
        // packets in flight, as it has throughout a transfer.
        let wrapped = SackRange { start: 0xFFFF_FFF0, end: 0x10 };
        assert!(handle_incoming(&inner, acknowledgement(0, &[wrapped])).await);
        {
            let inner = inner.lock().unwrap();
            assert_eq!(inner.sacks, 0, "it names nothing that is in flight");
            assert_eq!(inner.inflight(), 3);
        }
        assert!(written.try_recv().is_err(), "nothing is acknowledged yet");

        // The stream's task is still there for the acknowledgement that
        // completes the write, so its writer does not wait for good.
        assert!(handle_incoming(&inner, acknowledgement(3, &[])).await);
        assert!(matches!(written.try_recv(), Ok(Ok(()))));
    }

    #[test]
    fn a_range_across_the_wrap_takes_what_it_names_on_both_sides_of_it() {
        let mut inner = connected();
        // A stream that has all but the last three numbers behind it.
        let first = u32::MAX - 2;
        inner.next_seq = first;
        inner.remote_acked = first;
        inner.rack_next_seq = first;
        inner.rack_fack = first;
        queue(&mut inner, 6);
        let t0 = inner.epoch;
        assert_eq!(data_seqs(&send(&mut inner, t0)), [first, first + 1, u32::MAX, 0, 1, 2]);

        // The remote has the three in the middle.
        inner.process_ack(first, &[SackRange { start: u32::MAX, end: 2 }], t0 + millis(20));

        assert_eq!(inner.sacks, 3);
        assert_eq!(inner.outgoing.keys().copied().collect::<Vec<_>>(), [2, first, first + 1]);
    }

    #[test]
    fn data_with_the_highest_number_is_old_to_a_new_stream_and_draws_a_plain_acknowledgement() {
        let (inner, mut reader) = with_reader();
        let inner = Mutex::new(inner);

        // Far ahead as numbers go, and behind as sequence numbers do.
        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"x");
        let taken = take_in(&inner, &header, data);
        assert!(taken.ack_owed);
        assert!(taken.ack_now, "as far as the stream can tell, it had the packet already");

        let mut inner = inner.lock().unwrap();
        assert!(inner.recv_buf.is_empty(), "it is not kept");
        assert_eq!(read(&mut reader), (Vec::new(), false));
        // The acknowledgement is built in the task that receives for every
        // stream of the socket, and for the socket's datagrams.
        let (ack, _) = inner.take_ack().expect("an acknowledgement is owed");
        let header = Header::decode(&ack).unwrap();
        assert!(!header.has_flag(FLAG_SACK));
        assert_eq!(header.ack, 0);
        assert_eq!(ack.len(), HEADER_SIZE);
    }

    #[test]
    fn a_packet_ahead_of_a_gap_just_below_the_wrap_is_named_in_a_range_that_ends_past_it() {
        let (mut inner, mut reader) = with_reader();
        inner.next_remote_seq = u32::MAX - 2;

        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"c");
        inner.receive_packet(&header, data);
        assert_eq!(read(&mut reader), (Vec::new(), false), "it waits for what comes before it");
        assert_eq!(inner.sack_ranges(), [SackRange { start: u32::MAX, end: 0 }]);

        // The gap is filled, and the reader is handed all of it in order.
        for (seq, payload) in [(u32::MAX - 2, b"a"), (u32::MAX - 1, b"b")] {
            let (header, data) = incoming(FLAG_DATA, seq, 0, payload);
            inner.receive_packet(&header, data);
        }
        assert_eq!(read(&mut reader), (b"abc".to_vec(), false));
        assert_eq!(inner.next_remote_seq, 0, "the next number is the first one again");
        assert!(inner.sack_ranges().is_empty());
    }

    #[test]
    fn packets_ahead_of_a_gap_on_both_sides_of_the_wrap_are_named_and_then_read_in_order() {
        let (mut inner, mut reader) = with_reader();
        inner.next_remote_seq = u32::MAX - 2;
        let receive = |inner: &mut StreamInner, seq: u32, payload: &[u8]| {
            let (header, data) = incoming(FLAG_DATA, seq, 0, payload);
            inner.receive_packet(&header, data);
        };
        let range = |start: u32, end: u32| SackRange { start, end };

        // The ranges are named by number, lowest first, so what was received
        // past the wrap comes before what was received up to it.
        receive(&mut inner, u32::MAX, b"c");
        receive(&mut inner, 1, b"e");
        assert_eq!(inner.sack_ranges(), [range(1, 2), range(u32::MAX, 0)]);
        receive(&mut inner, u32::MAX - 1, b"b");
        assert_eq!(inner.sack_ranges(), [range(1, 2), range(u32::MAX - 1, 0)]);
        receive(&mut inner, 0, b"d");
        assert_eq!(inner.sack_ranges(), [range(0, 2), range(u32::MAX - 1, 0)]);
        assert_eq!(read(&mut reader), (Vec::new(), false), "all of it waits for what comes before it");

        // The gap is filled, and the reader is handed all of it in order.
        receive(&mut inner, u32::MAX - 2, b"a");
        assert_eq!(read(&mut reader), (b"abcde".to_vec(), false));
        assert_eq!(inner.next_remote_seq, 2);
        assert!(inner.sack_ranges().is_empty());
    }

    #[test]
    fn a_packet_is_told_old_from_new_across_the_wrap() {
        let (mut inner, mut reader) = with_reader();
        inner.next_remote_seq = u32::MAX;
        let inner = Mutex::new(inner);

        // One ahead of what is awaited, on the far side of the wrap: kept.
        let (header, data) = incoming(FLAG_DATA, 0, 0, b" later");
        assert!(take_in(&inner, &header, data).ack_now, "it left a gap");
        assert_eq!(inner.lock().unwrap().sack_ranges(), [SackRange { start: 0, end: 1 }]);

        // What is awaited arrives, and the reader has both.
        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"sooner");
        take_in(&inner, &header, data);
        assert_eq!(read(&mut reader), (b"sooner later".to_vec(), false));
        assert_eq!(inner.lock().unwrap().next_remote_seq, 1);

        // The same packet again is one the stream has already, though its
        // number is the highest there is.
        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"sooner");
        let taken = take_in(&inner, &header, data);
        assert!(taken.ack_now, "the remote is told at once that it arrived");
        assert!(inner.lock().unwrap().recv_buf.is_empty(), "and it is not kept a second time");
        assert_eq!(read(&mut reader), (Vec::new(), false));
    }
}
