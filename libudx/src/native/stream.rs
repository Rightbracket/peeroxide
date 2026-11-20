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
use super::recv_buffer::{Frame, RecvBuffer, RecvReader};
use super::header::{
    Header, SackRange,
    FLAG_DATA, FLAG_END, FLAG_DESTROY, FLAG_SACK, FLAG_HEARTBEAT,
    HEADER_SIZE, encode_sack, decode_sack,
};

// ── Constants ────────────────────────────────────────────────────────────────

/// Default receive window size (4 MB, matches C libudx).
pub(crate) const DEFAULT_RWND: u32 = 4_194_304;

/// The most a stream without a limit on its read buffer holds ahead of a
/// gap, in bytes: the window it advertises. A remote could otherwise have
/// it hold any amount by never sending one packet, however fast its reader
/// is.
const MAX_OUT_OF_ORDER: usize = DEFAULT_RWND as usize;

/// A stream holds no more packets ahead of a gap than its widest window
/// divided by this. Each takes an entry in an index as well as its bytes,
/// so a remote that sent a byte in each would otherwise have the index
/// outgrow the bytes many times. A sender counts its window in full packets
/// of its own, which are larger than this on any link that carries UDX.
const OUT_OF_ORDER_FRAME_SIZE: usize = 256;

/// The lowest limit a stream's read buffer takes. Lower, and the stream
/// would have no window to advertise: see [`StreamInner::recv_window`].
pub(crate) const MIN_READ_BUFFER_LIMIT: u32 = 8_192;

/// Payload of the largest packet a stream is sent: one at the largest MTU,
/// over IPv4.
const FULL_PACKET: usize = MTU_MAX - IPV4_UDP_OVERHEAD - HEADER_SIZE;

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

/// Recoveries without a packet sent again for nothing, after which the
/// reordering window narrows back to a quarter of the lowest round trip.
/// RFC 8985, 6.2 step 4.
const REO_WND_PERSIST: u32 = 16;

/// The longest a receiver holds back the SACK ranges past a gap, to let a
/// packet that is only late arrive first. See [`StreamInner::sack_hold`].
const MAX_SACK_HOLD: Duration = Duration::from_millis(10);

/// How many packets loss detection sent again a sender remembers, to tell
/// whether one its remote reports twice is one of them.
const RACK_RESENT_KEPT: usize = 1_024;

/// How many filled gaps a receiver remembers, to learn from a packet that
/// then arrives again how long the path held it back.
const RECENT_FILLS: usize = 64;

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

/// How many packets past `ack` a packet's selective acknowledgements report
/// arrived. A range below `ack` tells of a packet that arrived twice, and is
/// left out.
fn sack_count(ack: u32, ranges: &[SackRange]) -> u32 {
    ranges
        .iter()
        .filter(|range| !seq_after(ack, range.start))
        .fold(0, |count, range| count.saturating_add(range.end.wrapping_sub(range.start)))
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
    /// Whether loss detection last declared it lost, because one sent after
    /// it was delivered first: a packet the path only held back looks so.
    /// Not when the retransmission timer did.
    lost_to_rack: bool,
    /// The state of delivery when this packet was last sent.
    rate_info: PacketRateInfo,
}

/// A packet that arrived ahead of a gap, held until the gap is filled. Its
/// payload is in the stream's out-of-order arena.
struct RecvdPacket {
    offset: usize,
    len: usize,
    flags: u8,
    /// When it arrived: the gap below it has been open since about then.
    arrived: Instant,
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
    /// The packets that arrived ahead of a gap, by sequence number. Their
    /// payloads are in `recv_arena`.
    recv_buf: BTreeMap<u32, RecvdPacket>,
    /// The payloads of the packets in `recv_buf`, one after another in the
    /// order they arrived, with the space of those that have gone on to the
    /// reader in between.
    recv_arena: Vec<u8>,
    /// The bytes of payload in `recv_buf`.
    recv_buf_held: usize,
    /// What has arrived in order and the reader has not taken yet.
    recv: Arc<Mutex<RecvBuffer>>,
    /// Packets that arrived again after they had been taken, to report
    /// below the acknowledgement, as TCP's D-SACK does (RFC 2883).
    recv_dsack: Option<SackRange>,
    /// How many packets have been taken in order, up to `u32::MAX`: a
    /// number below the acknowledgement further back than that was never
    /// taken, and did not arrive twice.
    recv_taken: u32,
    /// The acknowledgement last sent, and how far past it that reported
    /// packets arrived. What it reported is not held back again: a remote
    /// takes an acknowledgement that reports less as an older one.
    recv_reported: (u32, u32),
    /// The gaps filled lately: the packet that filled each, and how long the
    /// gap had been open.
    recv_fills: VecDeque<(u32, Duration)>,
    /// How long the path has been seen to hold packets back behind later
    /// ones. See [`sack_hold`](Self::sack_hold).
    recv_reorder_extent: Duration,
    /// When SACK ranges held back are due to be reported.
    sack_hold_deadline: Option<Instant>,
    /// The most the stream holds for its reader, if it has a limit.
    read_limit: Option<u32>,
    /// The most bytes the stream holds written and not yet acknowledged, if
    /// it has a limit beyond its remote's window.
    write_cap: Option<u32>,
    /// Wakes the writers of [`UdxStream::write`] that wait for room under
    /// `write_cap`, and on failure or close.
    room: Arc<tokio::sync::Notify>,
    /// The receive window in the packet that went out last.
    advertised_rwnd: u32,
    /// How many packets the stream has turned away for want of room.
    turned_away: u64,
    /// True once the reader has been handed the end of the stream. Nothing
    /// that arrives after it is taken. C: `UDX_STREAM_ENDED_REMOTE`.
    remote_ended: bool,
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
    /// The lowest round-trip time, in microseconds, and the smoothed one,
    /// zero before the first sample. Loss detection reckons in them: a path
    /// whose round trip takes a few milliseconds may hold packets back by
    /// less than one.
    rtt_min_us: WindowedFilter<u64>,
    srtt_us: u64,
    /// The reordering window, in quarters of the lowest round trip. It
    /// widens by one each round trip in which a packet turns out to have
    /// been sent again for nothing. RFC 8985, 6.2 step 4.
    reo_wnd_mult: u32,
    /// Recoveries left before the window narrows back.
    reo_wnd_persist: u32,
    /// Until the acknowledgement passes this, the window has widened in this
    /// round trip already.
    dsack_round: Option<u32>,
    /// An acknowledgement being taken in showed a packet sent again for
    /// nothing: it came sooner after the packet went out again than a
    /// round trip takes.
    resent_for_nothing: bool,
    /// The packets loss detection sent again lately, to tell whether one the
    /// remote reports twice is one it took for lost too soon.
    rack_resent: VecDeque<u32>,
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
    /// When the remote's window, which has no room, is asked about again.
    zwp_deadline: Option<Instant>,

    // ── Flow control ──
    /// Remote's last advertised receive window.
    send_rwnd: u32,
    /// The sequence and acknowledgement numbers of the packet that window
    /// came in. A packet older than it does not change the window.
    /// C: `send_wl1`, `send_wl2`.
    send_wl1: u32,
    send_wl2: u32,
    /// How many packets past its acknowledgement the packet that window came
    /// in reported arrived. Of packets with the same numbers, one that
    /// reports fewer is older.
    send_wl_reported: u32,
    /// When the remote last said that it has no room for a packet, while
    /// that is the last it said of its window.
    no_room_heard: Option<Instant>,
    /// Whether a packet is due to go out although the remote's window has
    /// no room for it: a zero-window probe.
    zwp_wanted: bool,

    // ── MTU probing state ──
    pub(crate) mtu_state: MtuState,
    pub(crate) mtu: usize,
    pub(crate) mtu_probe_size: usize,
    pub(crate) mtu_probe_count: u32,
    pub(crate) mtu_probe_wanted: bool,
    pub(crate) mtu_max: usize,

    // ── IO ──
    pub(crate) udp: Option<Arc<tokio::net::UdpSocket>>,
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
    /// The stream waits on its firewall hook: it is set, or is being called
    /// and has not answered yet. The hook is taken out of the stream to be
    /// called, so `firewall_hook` alone does not tell. Cleared under the same
    /// lock that connects the stream, or closes it when the hook turns the
    /// remote away.
    firewall_pending: bool,
}

impl StreamInner {
    /// The state of a stream that has not been connected yet.
    fn new(recv: Arc<Mutex<RecvBuffer>>, close_tx: Option<oneshot::Sender<()>>) -> Self {
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
            recv_arena: Vec::new(),
            recv_buf_held: 0,
            recv,
            recv_dsack: None,
            recv_taken: 0,
            recv_reported: (0, 0),
            recv_fills: VecDeque::new(),
            recv_reorder_extent: Duration::ZERO,
            sack_hold_deadline: None,
            read_limit: None,
            write_cap: None,
            room: Arc::new(tokio::sync::Notify::new()),
            advertised_rwnd: DEFAULT_RWND,
            turned_away: 0,
            remote_ended: false,
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
            rtt_min_us: WindowedFilter::new(0, u64::MAX),
            srtt_us: 0,
            reo_wnd_mult: 1,
            reo_wnd_persist: REO_WND_PERSIST,
            dsack_round: None,
            resent_for_nothing: false,
            rack_resent: VecDeque::new(),
            tlp_in_flight: false,
            tlp_end_seq: 0,
            tlp_is_retrans: false,
            tlp_permitted: false,
            tlp_wanted: false,
            rto_deadline: None,
            tlp_deadline: None,
            rack_reo_deadline: None,
            pacing_deadline: None,
            zwp_deadline: None,
            send_rwnd: DEFAULT_RWND,
            send_wl1: 0,
            send_wl2: 0,
            send_wl_reported: 0,
            no_room_heard: None,
            zwp_wanted: false,
            mtu_state: MtuState::Base,
            mtu: MTU_BASE,
            mtu_probe_size: MTU_BASE + MTU_STEP,
            mtu_probe_count: 0,
            mtu_probe_wanted: false,
            mtu_max: MTU_MAX,
            udp: None,
            close_tx,
            notify_tx: None,
            congestion: CongestionController::new(MAX_PAYLOAD as u32, 0),
            relay_target: None,
            firewall_hook: None,
            firewall_socket: None,
            firewall_pending: false,
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
        self.recv_buffer().end();
        // A writer waiting for room finds the stream closed.
        if let Some(waker) = self.write_waker.take() {
            waker.wake();
        }
    }

    /// Fail every write that has not been acknowledged.
    fn fail_writes(&mut self, error: impl Fn() -> UdxError) {
        // A writer of `UdxStream::write` waiting to be taken finds the error.
        self.room.notify_waiters();
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
            // Packets go out with "don't fragment", so one larger than the
            // link is refused, not sent in pieces: cut them to the link.
            // C libudx keeps the starting size, and its streams time out on
            // such a link. A link too small for a header and a byte of
            // payload carries nothing either way.
            if link_mtu < MTU_BASE && link_mtu > self.ip_overhead() + HEADER_SIZE {
                self.mtu = link_mtu;
                self.congestion.on_mtu_change(self.max_payload());
            }
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
        if !self.connected && !self.firewall_pending {
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
        self.queue_end_inner(false)
    }

    /// As [`queue_end`](Self::queue_end), and also on a stream still waiting
    /// on its firewall hook: the end then goes out after what was written,
    /// once the hook lets a remote in. For the async adapter's shutdown,
    /// whose writes are already queued that way, and
    /// [`UdxStream::shutdown_graceful`].
    pub(crate) fn queue_end_awaiting_firewall(&mut self) -> Result<oneshot::Receiver<Result<()>>> {
        self.queue_end_inner(true)
    }

    fn queue_end_inner(&mut self, awaiting_firewall_ok: bool) -> Result<oneshot::Receiver<Result<()>>> {
        if let Some(error) = self.failure() {
            return Err(UdxError::Io(error));
        }
        if self.terminated {
            return Err(UdxError::StreamClosed);
        }
        let awaiting_firewall = awaiting_firewall_ok && self.firewall_pending;
        if !self.connected && !awaiting_firewall {
            return Err(UdxError::Io(std::io::Error::other("stream not connected")));
        }
        if self.ended {
            return Err(UdxError::Io(std::io::Error::other("stream already shut down")));
        }
        // A packet is addressed when it is sent, so the remote's address is
        // needed only once the stream is connected.
        if self.udp.is_none() || (self.connected && self.remote_addr.is_none()) {
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
        let mut len = buf.len().min(HIGH_WATERMARK);
        // With a limit of its own, the stream takes no more than the room
        // there is.
        if self.write_cap.is_some() {
            len = len.min(self.write_limit() - self.unacked_bytes);
        }
        self.push_write(QueuedWrite { data: buf[..len].to_vec(), taken: 0, end: false, tx: None });
        Ok(Some(len))
    }

    /// How many bytes may be written and unacknowledged before a writer is
    /// asked to wait. C: `UDX_HIGH_WATERMARK + send_window_in_bytes(stream)`,
    /// and no more than the stream's own limit, if it has one.
    fn write_limit(&self) -> usize {
        let window = (self.congestion.cwnd as usize)
            .saturating_mul(self.max_payload())
            .min(self.send_rwnd as usize);
        let limit = HIGH_WATERMARK + window;
        self.write_cap.map_or(limit, |cap| limit.min(cap as usize))
    }

    /// Wake the writer that waits for room, once there is some, and the
    /// writers of [`UdxStream::write`] that wait to be taken.
    fn wake_writer(&mut self) {
        if self.write_waker.is_some() && self.unacked_bytes < self.write_limit() {
            if let Some(waker) = self.write_waker.take() {
                waker.wake();
            }
        }
        if self.write_cap.is_some() {
            self.room.notify_waiters();
        }
    }

    fn push_write(&mut self, write: QueuedWrite) {
        self.unacked_bytes += write.data.len();
        self.write_queue.push_back(write);
        // A stream with nothing on its way and no room at its remote asks
        // about the room at once, with this data. C: `_udx_stream_write`.
        if self.outgoing.is_empty() && self.window_closed() {
            self.zwp_wanted = true;
        }
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
            recv_window: self.advertise(),
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

    /// Whether the stream is still waiting on its firewall hook, which lets
    /// a remote in or turns it away on the first packet that arrives.
    pub(crate) fn awaiting_firewall(&self) -> bool {
        self.firewall_pending
    }

    /// Whether everything written has been acknowledged.
    pub(crate) fn drained(&self) -> bool {
        self.write_queue.is_empty() && self.outgoing.is_empty()
    }

    // ── Flow control ─────────────────────────────────────────────────────────
    //
    // Every packet a stream sends advertises a receive window, and a stream
    // keeps what it has in flight within the window its remote advertises.
    // C: `get_recv_rwnd`, and the send window update in `process_packet`.

    /// Limit what the stream holds for its reader, in bytes, or lift the
    /// limit.
    pub(crate) fn set_read_limit(&mut self, limit: Option<u32>) {
        self.read_limit = limit.map(|limit| limit.max(MIN_READ_BUFFER_LIMIT));
        self.recv_buffer().set_limited(limit.is_some());
        // The remote is told the window that goes with it.
        if self.recv_window() != self.advertised_rwnd {
            self.ack_wanted = true;
            self.notify_processor();
        }
    }

    /// The limit on what the stream holds for its reader, if it has one.
    pub(crate) fn read_limit(&self) -> Option<u32> {
        self.read_limit
    }

    /// Limit what the stream holds written and not yet acknowledged, in
    /// bytes, or lift the limit. A limit below [`MIN_READ_BUFFER_LIMIT`] is
    /// raised to that. Lowering it below what is held loses nothing: the
    /// stream sends no more, and takes no more writes, until acknowledgements
    /// bring it under the limit.
    pub(crate) fn set_write_cap(&mut self, limit: Option<u32>) {
        self.write_cap = limit.map(|limit| limit.max(MIN_READ_BUFFER_LIMIT));
        // A higher limit, or none, can make room at once.
        self.wake_writer();
        self.room.notify_waiters();
        self.notify_processor();
    }

    /// The window the stream sends within, in bytes: its remote's, and no
    /// more than its own limit on what it holds unacknowledged.
    fn send_allowance(&self) -> u32 {
        self.write_cap.map_or(self.send_rwnd, |cap| self.send_rwnd.min(cap))
    }

    /// Whether a write of `len` bytes is taken now from a writer that waits
    /// for its acknowledgement, [`UdxStream::write`]: always without a
    /// limit; with one, if it fits, or if nothing else is unacknowledged, so
    /// that a write larger than the limit is taken in the end.
    pub(crate) fn takes_write(&self, len: usize) -> bool {
        self.write_cap.is_none_or(|cap| {
            self.unacked_bytes == 0 || self.unacked_bytes + len <= cap as usize
        })
    }

    /// The limit on what the stream holds written and not yet
    /// acknowledged, if it has one.
    pub(crate) fn write_cap(&self) -> Option<u32> {
        self.write_cap
    }

    /// What has arrived in order and the reader has not taken yet. Locked
    /// after the stream, never before it.
    fn recv_buffer(&self) -> std::sync::MutexGuard<'_, RecvBuffer> {
        self.recv.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The reader's end of what the stream holds for it.
    pub(crate) fn recv_reader(&self) -> RecvReader {
        RecvReader::new(Arc::clone(&self.recv))
    }

    /// What the stream holds for its reader, in bytes of payload: what has
    /// arrived in order and the reader has not taken, and what arrived out
    /// of order.
    ///
    /// A stream whose reader is gone holds nothing for it: what the reader
    /// had not taken went with it, and what arrives in order is let go.
    fn held(&self) -> usize {
        self.unread_by_reader() + self.recv_buf_held
    }

    /// What has arrived in order and the reader has not taken, in bytes.
    /// Only a stream with a limit asks, so that a stream without one and its
    /// reader do not wait on each other.
    fn unread_by_reader(&self) -> usize {
        self.recv_buffer().len()
    }

    /// Whether a stream with a limit takes the packet its reader is waiting
    /// for: while it holds less than its limit, or once the reader has taken
    /// everything it was handed.
    ///
    /// Only that packet lets what waits behind it through to the reader. A
    /// stream that was given a limit, or a lower one, while more than that
    /// waited behind a gap would otherwise turn it away for good: the reader
    /// could not make room, having nothing left to take. What waits behind
    /// it is not dropped instead: the remote was told of it, and no longer
    /// holds it.
    fn takes_awaited(&self, limit: usize) -> bool {
        self.held() < limit || self.unread_by_reader() == 0
    }

    /// The most packets the stream holds ahead of a gap: its widest window
    /// in packets of [`OUT_OF_ORDER_FRAME_SIZE`].
    fn max_out_of_order_packets(&self) -> usize {
        let widest = self.read_limit.map_or(MAX_OUT_OF_ORDER, |limit| limit as usize);
        widest / OUT_OF_ORDER_FRAME_SIZE
    }

    /// The receive window to advertise, in bytes of payload: the room the
    /// stream has. C: `get_recv_rwnd`.
    ///
    /// A sender counts its window in full packets of its own, C libudx as
    /// `send_rwnd / max_payload` and this crate as `send_rwnd / mss`, and a
    /// packet carries no more than that. So a sender of any size that keeps
    /// to the window sends no more than the room there is, and is not
    /// turned away.
    ///
    /// A stream without a limit advertises the room it has left ahead of a
    /// gap, of [`MAX_OUT_OF_ORDER`]. A remote counts in flight only what is
    /// neither acknowledged nor selectively acknowledged, so a window that
    /// left out what is held ahead of a gap would let it send past that.
    /// While there is a gap, the window is never less than one full packet,
    /// the one that fills it. With nothing held ahead of a gap, which is how
    /// a stream mostly is, it is `DEFAULT_RWND`, as C libudx advertises for
    /// a program that does not tell it how much it is holding, and
    /// udx-native does not.
    ///
    /// A stream with a limit advertises the room it has left under the
    /// limit, less a full packet, which it keeps for the packet its reader
    /// is waiting for. Room for less than a full packet is advertised as
    /// none: a sender counts its window in whole packets, and C libudx asks
    /// about a window again only when it is zero. While a packet is missing
    /// that the stream would take, the window is never less than that one
    /// packet, so that a sender with no window left can send it again.
    fn recv_window(&self) -> u32 {
        let gap = !self.recv_buf.is_empty();
        let Some(limit) = self.read_limit else {
            let window = MAX_OUT_OF_ORDER.saturating_sub(self.recv_buf_held);
            return if gap { window.max(FULL_PACKET) } else { window } as u32;
        };
        let limit = limit as usize;
        let window = limit.saturating_sub(self.held() + FULL_PACKET);
        if window >= FULL_PACKET {
            window as u32
        } else if gap && self.takes_awaited(limit) {
            FULL_PACKET as u32
        } else {
            0
        }
    }

    /// The receive window for a packet that is about to go out. It is
    /// remembered as what the remote was told last.
    fn advertise(&mut self) -> u32 {
        self.advertised_rwnd = self.recv_window();
        self.advertised_rwnd
    }

    /// Whether the stream takes in a packet numbered `seq` that carries
    /// `len` bytes.
    ///
    /// A stream without a limit takes the packet its reader is waiting for,
    /// and any other while what it holds ahead of a gap stays within
    /// [`MAX_OUT_OF_ORDER`], the window it advertises.
    ///
    /// A stream with a limit takes the packet the reader is waiting for
    /// while it holds less than its limit ([`takes_awaited`](Self::takes_awaited)).
    /// That lets what waits behind it through to the reader, and can take
    /// the stream over its limit by that one packet. Any other packet is
    /// taken if it leaves room for a full packet, which is kept for the one
    /// awaited: packets that arrived ahead of a gap could otherwise fill the
    /// stream and keep out the one that closes the gap.
    ///
    /// So a stream never holds more than its limit and one packet, unless
    /// it was given the limit while it held more. A packet that carries more
    /// than a full one, which no stream sends, is taken only if it fits
    /// under the limit.
    ///
    /// Either way the stream holds no more than
    /// [`max_out_of_order_packets`](Self::max_out_of_order_packets) ahead of
    /// a gap, so that tiny packets cannot have it hold many times their
    /// bytes in bookkeeping.
    ///
    /// A packet that has been taken is never dropped again. The remote has
    /// been told of it, and no longer holds it to send it again.
    fn has_room_for(&self, seq: u32, len: usize) -> bool {
        let awaited = seq == self.next_remote_seq;
        if !awaited && self.recv_buf.len() >= self.max_out_of_order_packets() {
            return false;
        }
        let Some(limit) = self.read_limit else {
            return awaited || self.recv_buf_held + len <= MAX_OUT_OF_ORDER;
        };
        let limit = limit as usize;
        if awaited {
            // A packet that carries nothing is not held.
            let fits = len <= FULL_PACKET || self.held() + len <= limit;
            len == 0 || (self.takes_awaited(limit) && fits)
        } else {
            self.held() + len + FULL_PACKET <= limit
        }
    }

    /// The reader has taken data from a stream that has a limit. The remote
    /// may be waiting for room, so it is told once the window is a quarter
    /// of the widest it can be more than the stream last advertised, or a
    /// full packet more if that is more. Told of less, it would send in
    /// dribbles. A remote that was told there was no room is also told of
    /// any room there is once the reader has taken all it was handed, since
    /// reading can make no more: what waits ahead of a gap can keep the
    /// stream over its limit, and leave room only for the packet that fills
    /// the gap. It does not depend on being told: a sender asks about a
    /// window that has no room, but only every retransmission timeout or
    /// two.
    pub(crate) fn reader_took(&mut self) {
        let Some(limit) = self.read_limit else {
            return;
        };
        let step = (limit as usize / 4).max(FULL_PACKET) as u32;
        let window = self.recv_window();
        let reopened = self.advertised_rwnd == 0 && window > 0 && self.unread_by_reader() == 0;
        if reopened || window >= self.advertised_rwnd.saturating_add(step) {
            self.ack_wanted = true;
            self.notify_processor();
        }
    }

    /// Whether the remote's window has no room for a packet. C looks for a
    /// window of zero. One smaller than a packet lets nothing out either.
    fn window_closed(&self) -> bool {
        (self.send_rwnd as usize) < self.max_payload()
    }

    /// Take the receive window a packet advertises, unless the packet is
    /// older than the one the window in use came in: packets overtake one
    /// another. C: the send window update in `process_packet`.
    ///
    /// While the window has no room for a packet, a timer asks about it
    /// again. Each packet that says so starts the timer afresh. C:
    /// `zwp_timer`.
    fn take_window(&mut self, header: &Header, sack_ranges: &[SackRange], now: Instant) {
        // A packet that acknowledges what was never sent, or less than
        // another has, says nothing of the window either.
        if seq_after(header.ack, self.next_seq) || seq_after(self.remote_acked, header.ack) {
            return;
        }
        let newer = seq_after(header.seq, self.send_wl1)
            || (header.seq == self.send_wl1 && !seq_after(self.send_wl2, header.ack));
        if !newer {
            return;
        }

        // Packets that acknowledge only, while a gap is open, all carry the
        // same numbers. Of those, one that reports fewer packets past the
        // gap than the window in use came with is older: it says there is
        // more room than there now is. A closed window opens on any of them,
        // should a remote report fewer for another reason, as one that
        // reports a number of ranges at most may.
        let reported = sack_count(header.ack, sack_ranges);
        let same = header.seq == self.send_wl1 && header.ack == self.send_wl2;
        if same
            && header.recv_window > self.send_rwnd
            && reported < self.send_wl_reported
            && !self.window_closed()
        {
            return;
        }

        self.send_rwnd = header.recv_window;
        self.send_wl1 = header.seq;
        self.send_wl2 = header.ack;
        self.send_wl_reported = reported;

        if self.window_closed() {
            self.no_room_heard = Some(now);
            self.zwp_deadline = Some(now + Duration::from_millis(u64::from(self.rto)));
        } else {
            self.no_room_heard = None;
            self.zwp_deadline = None;
            self.zwp_wanted = false;
        }
    }

    /// The remote's window has had no room for a packet for a whole
    /// timeout. One packet goes out all the same: the acknowledgement it
    /// draws says what room there is by now. A remote on C libudx says
    /// nothing of its own accord when it has room again.
    /// C: `udx_zwp_timeout`.
    fn on_zwp_timeout(&mut self, now: Instant) {
        self.zwp_deadline = None;
        if !self.window_closed() {
            return;
        }
        self.zwp_wanted = true;
        self.zwp_deadline = Some(now + Duration::from_millis(u64::from(self.rto)));
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

        let in_recovery = self.congestion.ca_state == CaState::Recovery;
        self.congestion.on_ack_received(ack);
        let left_recovery = in_recovery && self.congestion.ca_state == CaState::Open;
        // A remote reports nothing below its acknowledgement but a packet
        // that arrived twice. A range from an older acknowledgement that was
        // overtaken lies above that one's acknowledgement.
        // It widens the reordering window only for a packet that loss
        // detection sent again: one the retransmission timer or a tail loss
        // probe sent again says nothing of reordering.
        let dsack = sack_ranges
            .iter()
            .filter(|range| seq_after(ack, range.start))
            .any(|range| self.forget_rack_resent(range));
        self.congestion.reorder_tolerance_us = self.rack_reo_wnd().as_micros() as u64;

        let mut rs = RateSample::default();
        let mut completed = Vec::new();

        if ack_advanced {
            let acked_range = ack.wrapping_sub(self.remote_acked);
            let (acked, acked_past_the_wrap) = self.take_acked(ack);

            // Sequence numbers no longer in the buffer were acknowledged
            // selectively before.
            let held = (acked.len() + acked_past_the_wrap.len()) as u32;
            let sacked_before = acked_range.saturating_sub(held);
            self.sacks = self.sacks.saturating_sub(sacked_before);

            for (seq, pkt) in acked.into_iter().chain(acked_past_the_wrap) {
                self.on_packet_acked(seq, pkt, now, now_ms, &mut rs);
            }
            self.remote_acked = ack;

            // Complete pending writes whose ack_threshold <= ack, as
            // sequence numbers compare.
            self.pending_writes.retain_mut(|pw| {
                if !seq_after(pw.ack_threshold, ack) {
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

        let resent_for_nothing = std::mem::take(&mut self.resent_for_nothing) || dsack;
        self.update_reo_wnd(resent_for_nothing, left_recovery);

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

    /// Take the packets a cumulative acknowledgement of `ack` covers out of
    /// the outgoing buffer: those numbered from `remote_acked` up to it,
    /// oldest first. They come in two parts. The second is the ones past the
    /// wrap of the sequence space, and is empty unless the packets held run
    /// across it.
    fn take_acked(&mut self, ack: u32) -> (BTreeMap<u32, SentPacket>, BTreeMap<u32, SentPacket>) {
        if self.remote_acked <= self.next_seq {
            // Nothing held is past the wrap: number order is sequence order.
            let kept = self.outgoing.split_off(&ack);
            return (std::mem::replace(&mut self.outgoing, kept), BTreeMap::new());
        }

        // The packets held run up to the highest number and on from zero.
        let mut before_the_wrap = self.outgoing.split_off(&self.remote_acked);
        if ack > self.remote_acked {
            // The acknowledgement stops short of the wrap.
            let mut kept = before_the_wrap.split_off(&ack);
            self.outgoing.append(&mut kept);
            (before_the_wrap, BTreeMap::new())
        } else {
            // It is past the wrap, and so is all that it leaves.
            let kept = self.outgoing.split_off(&ack);
            (before_the_wrap, std::mem::replace(&mut self.outgoing, kept))
        }
    }

    /// The sequence numbers of the packets held, oldest first. All of them
    /// are at or after `remote_acked`, so those numbered below it are past
    /// the wrap of the sequence space, and come last.
    fn outgoing_in_order(&self) -> impl Iterator<Item = u32> + '_ {
        self.outgoing
            .range(self.remote_acked..)
            .chain(self.outgoing.range(..self.remote_acked))
            .map(|(&seq, _)| seq)
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
        let rtt_us = rtt.as_micros() as u64;
        // An acknowledgement that comes sooner after a packet was last sent
        // than a round trip takes answers an earlier sending of it: the
        // packet was sent again for nothing.
        let answers_earlier = retransmitted && rtt_us < self.rtt_min_us.get();
        // Proof that loss detection took the packet for lost too soon needs
        // a margin: the lowest round trip measured so far can still be
        // above the path's, on a path whose round trips vary. An answer to
        // the first sending comes a fraction of a round trip after the
        // second, which went out a round trip and the reordering window
        // after the first.
        let min_us = self.rtt_min_us.get();
        if retransmitted && pkt.lost_to_rack && min_us != u64::MAX && rtt_us < min_us / 2 {
            self.resent_for_nothing = true;
        }

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
            self.rtt_min_us.apply_min(RTT_MIN_WINDOW_MS, now_ms, rtt_us);
            self.srtt_us = if self.srtt_us == 0 { rtt_us } else { (7 * self.srtt_us + rtt_us) / 8 };
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

        let rtt_min_us = self.rtt_min_us.get();
        if rtt_min_us == u64::MAX {
            return Duration::ZERO;
        }
        let window = rtt_min_us / 4 * u64::from(self.reo_wnd_mult);
        Duration::from_micros(window.min(self.srtt_us))
    }

    /// Whether loss detection sent a packet in `range` again lately. It is
    /// forgotten: the remote reports each packet that arrived twice once.
    fn forget_rack_resent(&mut self, range: &SackRange) -> bool {
        let found = self
            .rack_resent
            .iter()
            .position(|&seq| !seq_after(range.start, seq) && seq_after(range.end, seq));
        found.map(|i| self.rack_resent.remove(i)).is_some()
    }

    /// Widen the reordering window once a round trip in which a packet was
    /// sent again for nothing, and narrow it back after that many
    /// recoveries without one. RFC 8985, 6.2 step 4.
    ///
    /// The remote tells of such a packet by reporting it below its
    /// acknowledgement, as TCP's D-SACK does (RFC 2883): it arrived twice.
    /// A remote that does not, as C libudx, still gives it away when the
    /// acknowledgement of a packet comes sooner after it was sent again than
    /// a round trip takes.
    ///
    /// Either also shows that the path reorders packets, which a packet sent
    /// again cannot show by being acknowledged below the highest one (6.2
    /// step 3): which of the two arrived is not known. Without it, the
    /// window would be closed throughout recovery on a path that holds
    /// packets back for longer than three others take to arrive, and every
    /// one held back would go out again.
    fn update_reo_wnd(&mut self, resent_for_nothing: bool, left_recovery: bool) {
        if resent_for_nothing {
            self.reordering_seen = true;
        }
        if self.dsack_round.is_some_and(|end| !seq_after(end, self.remote_acked)) {
            self.dsack_round = None;
        }
        if resent_for_nothing && self.dsack_round.is_none() {
            self.dsack_round = Some(self.next_seq);
            self.reo_wnd_mult = self.reo_wnd_mult.saturating_add(1);
            self.reo_wnd_persist = REO_WND_PERSIST;
        } else if left_recovery {
            self.reo_wnd_persist = self.reo_wnd_persist.saturating_sub(1);
            if self.reo_wnd_persist == 0 {
                self.reo_wnd_mult = 1;
                self.reo_wnd_persist = REO_WND_PERSIST;
            }
        }
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
                lost.push((time_sent, seq));
            } else {
                timeout = timeout.max(remaining);
            }
        }

        // Packets sent at one moment come out of the queue by number. Where
        // the packets held run across the wrap of the sequence space, that
        // is not the order they were sent in, which is the order they go
        // out again in.
        if self.next_seq < self.remote_acked {
            let oldest = self.remote_acked;
            lost.sort_by_key(|&(time_sent, seq)| (time_sent, seq.wrapping_sub(oldest)));
        }

        let resending = lost.len();
        let mut mtu_probes_lost = 0;
        for (_, seq) in lost {
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
        pkt.lost_to_rack = true;
        self.inflight_queue.remove(&(pkt.time_sent, seq));
        self.retransmit_queue.push_back(seq);
        self.congestion.on_packet_lost();
        if self.rack_resent.len() == RACK_RESENT_KEPT {
            self.rack_resent.pop_front();
        }
        self.rack_resent.push_back(seq);

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

        // A packet that went out before the remote last said that it has no
        // room was turned away, not lost on the way: the remote answered.
        // It goes out again all the same, to ask for room, and it is not
        // held against a remote that goes on answering. TCP keeps a
        // connection open on the same terms (RFC 9293, 3.8.6.1).
        let no_room_heard = self.no_room_heard.filter(|_| self.window_closed());
        let turned_away =
            |pkt: &SentPacket| no_room_heard.is_some_and(|heard| heard >= pkt.time_sent);
        let oldest = self.outgoing_in_order().next().and_then(|seq| self.outgoing.get(&seq));
        if !oldest.is_some_and(turned_away) {
            self.congestion.on_rto(self.next_seq);
        }
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
        let held: Vec<u32> = self.outgoing_in_order().collect();
        for seq in held {
            let Some(pkt) = self.outgoing.get_mut(&seq) else {
                continue;
            };
            if pkt.lost {
                self.retransmit_queue.push_back(seq);
                continue;
            }

            let timed_out = pkt.time_sent + self.rack_rtt + reo_wnd < now;
            if seq != self.remote_acked && !timed_out {
                continue;
            }

            if !turned_away(pkt) {
                if pkt.rto_timeouts >= MAX_RTO_TIMEOUTS {
                    return true;
                }
                pkt.rto_timeouts += 1;
                self.congestion.on_packet_lost();
            }

            pkt.lost = true;
            pkt.lost_to_rack = false;
            self.inflight_queue.remove(&(pkt.time_sent, seq));
            self.retransmit_queue.push_back(seq);

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
            self.zwp_deadline,
            self.sack_hold_deadline,
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
        if due(self.zwp_deadline) {
            self.on_zwp_timeout(now);
        }
        if due(self.pacing_deadline) {
            // The pacer refills itself when the stream next tries to send.
            self.pacing_deadline = None;
        }
        if due(self.sack_hold_deadline) {
            // Ranges held back are due: the remote is told of them.
            self.sack_hold_deadline = None;
            self.ack_wanted = true;
        }
        false
    }

    // ── Receive buffering ────────────────────────────────────────────────────

    /// Take in what a packet carries for the reader: its data, the end of
    /// the stream, or both. The reader is handed what is now in order, and
    /// the stream owes the remote an acknowledgement, which a heartbeat
    /// draws too. C: the receiving half of `process_packet`.
    #[cfg(test)]
    fn receive_packet(&mut self, header: &Header, data: &[u8]) {
        self.receive_packet_at(header, data, Instant::now());
    }

    /// As [`receive_packet`](Self::receive_packet), for a packet that
    /// arrived at `now`.
    fn receive_packet_at(&mut self, header: &Header, data: &[u8], now: Instant) {
        let is_data = header.has_flag(FLAG_DATA);
        let is_end = header.has_flag(FLAG_END);

        if is_data || is_end {
            // Only a packet with data has a payload; an end alone has none.
            let payload = match data.get(header.payload_offset()..) {
                Some(payload) if is_data => payload,
                _ => &[],
            };
            let flags = (if is_data { FLAG_DATA } else { 0 }) | (if is_end { FLAG_END } else { 0 });
            self.receive_data(header.seq, payload, flags, now);
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
    #[cfg(test)]
    fn take_ack(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        self.take_ack_at(Instant::now())
    }

    /// As [`take_ack`](Self::take_ack), at `now`.
    fn take_ack_at(&mut self, now: Instant) -> Option<(Vec<u8>, SocketAddr)> {
        if !self.ack_wanted || !self.connected {
            return None;
        }
        let remote_addr = self.remote_addr?;
        self.ack_wanted = false;
        Some((self.build_ack_at(now), remote_addr))
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
    /// What is in order goes to the reader at once. What arrives ahead of a
    /// gap is copied into the out-of-order arena, and goes on to the reader,
    /// in order, once the gap is filled.
    ///
    /// A single packet may carry both FLAG_DATA and FLAG_END (C libudx
    /// combines the last data chunk with the end marker): the reader is
    /// handed the data, then the end.
    ///
    /// The end of the stream is delivered once. Nothing that arrives after
    /// it is taken, and what had arrived numbered after it is let go: no
    /// reader looks behind the end, so it would be held for nobody.
    fn receive_data(&mut self, seq: u32, payload: &[u8], flags: u8, now: Instant) {
        // Nothing follows the end of a stream.
        if self.remote_ended {
            return;
        }

        // Duplicate or already-received packet — ignore. C: `seq_compare`.
        // The remote is told of it: it sent the packet again for nothing.
        if seq != self.next_remote_seq && !seq_after(seq, self.next_remote_seq) {
            if self.next_remote_seq.wrapping_sub(seq) <= self.recv_taken {
                self.arrived_again(seq);
            }
            return;
        }

        // One that arrived out of order before, and is still held.
        if seq != self.next_remote_seq && self.recv_buf.contains_key(&seq) {
            return;
        }

        // A stream takes what it has room for. The rest it turns away, and
        // the remote sends it again.
        if !self.has_room_for(seq, payload.len()) {
            self.turned_away += 1;
            return;
        }

        if seq != self.next_remote_seq {
            // Out of order: held until the gap is filled.
            let offset = self.recv_arena.len();
            self.recv_arena.extend_from_slice(payload);
            self.recv_buf_held += payload.len();
            self.recv_buf.insert(seq, RecvdPacket { offset, len: payload.len(), flags, arrived: now });
            return;
        }

        // It fills a gap: how long was it open?
        let gap_opened = self
            .recv_buf
            .range(self.next_remote_seq..)
            .chain(self.recv_buf.range(..self.next_remote_seq))
            .next()
            .map(|(_, recvd)| recvd.arrived);
        if let Some(opened) = gap_opened {
            self.gap_filled(seq, now.saturating_duration_since(opened));
        }

        let mut reader = self.recv.lock().unwrap_or_else(|e| e.into_inner());
        reader.push(payload);
        self.remote_ended = flags & FLAG_END != 0;
        self.next_remote_seq = seq.wrapping_add(1);
        self.recv_taken = self.recv_taken.saturating_add(1);

        // What waited behind it, as far as the end.
        while !self.remote_ended {
            let Some(recvd) = self.recv_buf.remove(&self.next_remote_seq) else {
                break;
            };
            reader.push(&self.recv_arena[recvd.offset..recvd.offset + recvd.len]);
            self.recv_buf_held -= recvd.len;
            self.remote_ended = recvd.flags & FLAG_END != 0;
            self.next_remote_seq = self.next_remote_seq.wrapping_add(1);
            self.recv_taken = self.recv_taken.saturating_add(1);
        }

        if self.remote_ended {
            reader.end();
            self.recv_buf.clear();
            self.recv_buf_held = 0;
        }
        drop(reader);
        self.tidy_recv_arena();
    }

    /// Let go of the arena's space for packets that have gone on to the
    /// reader: all of it once none is left, and otherwise, once more of it
    /// is free than taken, by moving the rest together. Each byte is moved
    /// no more often than room was made for one, so this costs a copy per
    /// byte at most.
    fn tidy_recv_arena(&mut self) {
        if self.recv_buf.is_empty() {
            self.recv_arena.clear();
            if self.recv_arena.capacity() > MAX_OUT_OF_ORDER {
                self.recv_arena.shrink_to(MAX_OUT_OF_ORDER);
            }
            return;
        }
        if self.recv_arena.len() <= 2 * self.recv_buf_held {
            return;
        }
        let mut arena = Vec::with_capacity(self.recv_buf_held);
        for recvd in self.recv_buf.values_mut() {
            let offset = arena.len();
            arena.extend_from_slice(&self.recv_arena[recvd.offset..recvd.offset + recvd.len]);
            recvd.offset = offset;
        }
        self.recv_arena = arena;
    }

    // ── Reordering, as the receiving side sees it ───────────────────────────
    //
    // A sender takes a packet for lost once one sent after it is reported
    // delivered, and a while has passed (RACK, RFC 8985). On a path that
    // holds packets back by more than that while, it sends them again for
    // nothing and shrinks its window for a congestion that is not there. C
    // libudx's window is a quarter of the lowest round trip, in whole
    // milliseconds: none at all on a path of a few. So the receiving side
    // learns how long the path holds packets back, and holds back the
    // ranges past a gap for that long ([`sack_hold`](Self::sack_hold)),
    // whoever sends.

    /// How long ranges past a gap are held back: as long as the path has
    /// been seen to hold packets back, and no more than [`MAX_SACK_HOLD`].
    /// A loss is reported that much later.
    ///
    /// The extent is learnt from packets that filled a gap and then arrived
    /// again, which proves the first was the late original, and from gaps
    /// that filled before the hold ran out, which the remote was never told
    /// of. A gap filled by a packet that came once, after it was reported,
    /// was a loss, and teaches nothing.
    fn sack_hold(&self) -> Duration {
        self.recv_reorder_extent.min(MAX_SACK_HOLD)
    }

    /// The packet numbered `seq` filled a gap that had been open for `open`.
    fn gap_filled(&mut self, seq: u32, open: Duration) {
        if open <= self.sack_hold() {
            self.learn_reordering(open);
        }
        if self.recv_fills.len() == RECENT_FILLS {
            self.recv_fills.pop_front();
        }
        self.recv_fills.push_back((seq, open));
    }

    /// The packet numbered `seq` arrived again, after it had been taken.
    fn arrived_again(&mut self, seq: u32) {
        let next = seq.wrapping_add(1);
        self.recv_dsack = Some(match self.recv_dsack.take() {
            Some(range) if range.end == seq => SackRange { start: range.start, end: next },
            _ => SackRange { start: seq, end: next },
        });
        // If it filled a gap, the first of it was the late original.
        if let Some(i) = self.recv_fills.iter().position(|&(filled, _)| filled == seq) {
            if let Some((_, open)) = self.recv_fills.remove(i) {
                self.learn_reordering(open);
            }
        }
    }

    /// The path held a packet back for `late`. The extent follows the
    /// longest at once, and shorter ones slowly.
    fn learn_reordering(&mut self, late: Duration) {
        let extent = self.recv_reorder_extent;
        self.recv_reorder_extent = if late >= extent { late } else { extent - (extent - late) / 16 };
    }

    // ── SACK generation ──────────────────────────────────────────────────────

    /// How many SACK ranges an acknowledgement may carry: as many as fit a
    /// packet of the starting MTU, which every path carries.
    ///
    /// The remote learns of a gap only from the ranges around it, and the
    /// ranges are the lowest ones. So what it can repair in one round trip is
    /// what one acknowledgement can name, and a burst of losses is repaired
    /// the faster the more ranges fit. C libudx sends 51 at most and reads as
    /// many as a packet holds. An acknowledgement is no larger than a packet
    /// of the starting MTU, or of the stream's MTU where that is smaller.
    fn max_sack_ranges(&self) -> usize {
        (self.mtu.min(MTU_BASE) - self.ip_overhead() - HEADER_SIZE) / SACK_RANGE_SIZE
    }

    /// Compute SACK ranges from the out-of-order receive buffer.
    /// Returns contiguous ranges of received sequence numbers that are
    /// beyond the cumulative ACK point, in sequence order: the ones nearest
    /// it first.
    #[cfg(test)]
    fn sack_ranges(&self) -> Vec<SackRange> {
        self.sack_ranges_at(Instant::now()).0
    }

    /// The ranges an acknowledgement sent at `now` names: those past gaps
    /// open for at least [`sack_hold`](Self::sack_hold), up to the first
    /// gap that is younger. Also when the rest are due, if any are held
    /// back.
    fn sack_ranges_at(&self, now: Instant) -> (Vec<SackRange>, Option<Instant>) {
        let mut ranges = self.all_sack_ranges();
        let hold = self.sack_hold();
        if hold.is_zero() {
            return (ranges, None);
        }
        let (acked, reported) = self.recv_reported;
        let reported = if acked == self.next_remote_seq { reported } else { acked };
        let young = ranges.iter().position(|range| {
            !seq_after(reported, range.start)
                && self.recv_buf.get(&range.start).is_some_and(|recvd| now < recvd.arrived + hold)
        });
        let due = young.and_then(|i| self.recv_buf.get(&ranges[i].start)).map(|recvd| recvd.arrived + hold);
        if let Some(i) = young {
            ranges.truncate(i);
        }
        (ranges, due)
    }

    fn all_sack_ranges(&self) -> Vec<SackRange> {
        if self.recv_buf.is_empty() {
            return Vec::new();
        }

        let max_ranges = self.max_sack_ranges();
        let mut ranges = Vec::new();
        // Everything held is ahead of `next_remote_seq`, so what is numbered
        // below it is past the wrap of the sequence space, and comes last.
        let mut iter = self
            .recv_buf
            .range(self.next_remote_seq..)
            .chain(self.recv_buf.range(..self.next_remote_seq))
            .map(|(&seq, _)| seq);

        if let Some(first) = iter.next() {
            let mut start = first;
            let mut end = first.wrapping_add(1);

            for seq in iter {
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
    /// room. One packet goes out without room in the remote's window when
    /// that window is being asked about: a zero-window probe.
    fn collect_sends(&mut self, now: Instant, out: &mut Vec<Outbound>) -> bool {
        if !self.connected {
            return false;
        }
        let Some(remote_addr) = self.remote_addr else {
            return false;
        };

        if let Some((ack, to)) = self.take_ack_at(now) {
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
            // A zero-window probe goes out whatever the window says.
            let zwp = self.zwp_wanted;
            if !self.congestion.may_send(self.inflight(), self.send_allowance(), now_us)
                && !(zwp && self.congestion.may_probe(now_us))
            {
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
            if zwp {
                self.zwp_wanted = false;
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
            // A zero-window probe goes out whatever the window says. It is
            // new data only while nothing sent is unacknowledged. After
            // that the oldest packet of what is, which goes out again when
            // the retransmission timer has fired, is the probe.
            let zwp = self.zwp_wanted && self.outgoing.is_empty();
            if !tlp {
                if out.len() >= MAX_SEND_BATCH {
                    return true;
                }
                if !self.congestion.may_send(self.inflight(), self.send_allowance(), now_us)
                    && !(zwp && self.congestion.may_probe(now_us))
                {
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
                lost_to_rack: false,
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
            // A packet that goes out while a probe is wanted is the probe.
            self.zwp_wanted = false;
            self.arm_stream_timers(now, tlp);
            out.push((packet, remote_addr));
        }

        if self.zwp_wanted && self.write_queue.is_empty() && self.retransmit_queue.is_empty() {
            // Nothing waits to be sent, so there is no room to ask about.
            self.zwp_wanted = false;
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
    ///
    /// The sequence number is the one the stream's next packet takes, as in
    /// C. A sender goes by it to tell which of two packets is the newer, and
    /// so whose window counts.
    #[cfg(test)]
    fn build_ack(&mut self) -> Vec<u8> {
        self.build_ack_at(Instant::now())
    }

    /// As [`build_ack`](Self::build_ack), at `now`. A packet that arrived
    /// twice is reported first, below the acknowledgement.
    fn build_ack_at(&mut self, now: Instant) -> Vec<u8> {
        let (mut sack_ranges, held_until) = self.sack_ranges_at(now);
        self.sack_hold_deadline = held_until;
        let reach = sack_ranges.last().map_or(self.next_remote_seq, |range| range.end);
        self.recv_reported = (self.next_remote_seq, reach);
        if let Some(twice) = self.recv_dsack.take() {
            sack_ranges.insert(0, twice);
            sack_ranges.truncate(self.max_sack_ranges());
        }
        let sack_byte_len = sack_ranges.len() * SACK_RANGE_SIZE;
        let header = Header {
            type_flags: if sack_ranges.is_empty() { 0 } else { FLAG_SACK },
            data_offset: u8::try_from(sack_byte_len).unwrap_or(0),
            remote_id: self.remote_id,
            recv_window: self.advertise(),
            seq: self.next_seq,
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
    /// What the stream holds for its reader. `None` once the reader has
    /// been handed the end.
    reader: Option<RecvReader>,
    #[allow(dead_code)]
    close_rx: Option<oneshot::Receiver<()>>,
    socket_streams: Mutex<Option<StreamMap>>,
}

impl UdxStream {
    pub(crate) fn new(local_id: u32) -> Self {
        let (close_tx, close_rx) = oneshot::channel();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        let inner = StreamInner::new(RecvBuffer::shared(), Some(close_tx));
        let reader = inner.recv_reader();

        Self {
            local_id,
            inner: Arc::new(Mutex::new(inner)),
            processor: Mutex::new(None),
            incoming_tx: Some(incoming_tx),
            incoming_rx: Mutex::new(Some(incoming_rx)),
            reader: Some(reader),
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
        let reader = match self.reader.take() {
            Some(reader) => reader,
            // Its reader was handed the end: so is the adapter's.
            None => {
                let ended = RecvBuffer::shared();
                ended.lock().unwrap_or_else(|e| e.into_inner()).end();
                RecvReader::new(ended)
            }
        };
        reader.read_bytes();
        let inner = Arc::clone(&self.inner);
        super::async_stream::UdxAsyncStream::new(inner, reader, processor)
    }

    /// The size of the largest IP packet the stream sends at present, in
    /// bytes: the IP and UDP headers, the UDX header and the payload. It
    /// starts at 1200 and grows as larger packets are found to arrive. C
    /// libudx and udx-native call it the stream's MTU.
    pub fn effective_mtu(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.mtu
    }

    /// Limit what the stream holds for its reader, in bytes, or lift the
    /// limit with `None`. A stream has no limit until it is given one.
    ///
    /// A stream keeps what arrives until its reader has taken it. Without a
    /// limit it keeps all it takes in order, however far behind the reader
    /// is, and tells the remote that it has room for 4 MB more. That is what
    /// C libudx does for a program that does not tell it how much it is
    /// holding, and udx-native does not. What arrives ahead of a missing
    /// packet it keeps only up to 4 MB, and leaves out of the room it tells
    /// of, which C libudx and udx-native do not: a remote could otherwise
    /// have it hold any amount by never sending that packet.
    ///
    /// The limit is on the bytes of data the stream holds: what has arrived
    /// and the reader has not taken. With a limit, the stream advertises the
    /// room it has left, less one full packet kept for the packet the reader
    /// is waiting for, as its receive window, as C libudx does for a program
    /// that sets `get_read_buffer_size`. A remote that keeps to it, whatever
    /// the size of its packets, is not turned away, and sends no faster than
    /// the reader reads. What arrives when the stream is full is turned
    /// away, and the remote sends it again: the stream never holds more than
    /// the limit and one packet, whatever the remote does.
    ///
    /// Packets are copied into the stream's buffers as they arrive, so the
    /// memory the stream holds is the data and little more. A packet that
    /// waits ahead of a missing one also takes an entry in an index, so the
    /// stream holds no more such packets than one for every 256 bytes of its
    /// limit, or of 4 MB without one. A remote that sends very small packets
    /// can make a stream read through [`read`](Self::read), which hands over
    /// a packet at a time, hold up to about three times its limit, two bytes
    /// for each packet's length. A
    /// [`UdxAsyncStream`](crate::UdxAsyncStream) reads bytes and does not.
    ///
    /// A limit can be given, raised, lowered or lifted at any time. A stream
    /// that holds more than a new limit keeps it, since the remote was told
    /// of it and no longer holds it, and takes nothing more until its reader
    /// is back under the limit. The packet that fills a gap is taken once
    /// the reader has taken everything it was handed, however much waits
    /// behind it.
    ///
    /// A reader that keeps up is not slowed down, with one exception: the
    /// window is also the most a remote has on its way at one time. Over a
    /// round trip of 100 ms, a limit of 1 MiB allows about 9.9 MB a second,
    /// and one of 8 MiB about 79.
    ///
    /// The limit is for a program whose reader can be slower than the
    /// network, such as one that writes what it reads to a disk, and for
    /// one that takes streams from remotes it does not trust. A limit below
    /// 8,192 bytes is raised to that.
    ///
    /// A remote on C libudx asks a full stream for room by sending it a
    /// packet, and gives up on the stream once the same packet has been
    /// turned away for about thirteen seconds. So a reader that stops
    /// reading for longer than that, while such a remote has more to send,
    /// loses the stream. A remote on this crate goes on asking for as long
    /// as the stream answers.
    pub fn set_read_buffer_limit(&self, limit: Option<u32>) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set_read_limit(limit);
    }

    /// The limit on what the stream holds for its reader, in bytes, if it
    /// has one. See [`set_read_buffer_limit`](Self::set_read_buffer_limit).
    pub fn read_buffer_limit(&self) -> Option<u32> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).read_limit()
    }

    /// Limit what the stream holds written and not yet acknowledged, in
    /// bytes, or lift the limit with `None`, the default.
    ///
    /// Without a limit, the stream sends as much as its remote's window and
    /// its congestion window allow, and holds every packet until it is
    /// acknowledged. So the remote decides how much memory the stream's
    /// writes take: as much as the window it advertises, which a remote on
    /// this crate with a read buffer limit of 32 MiB puts at about 32 MiB.
    /// A limit puts a bound on that, as `tcp_wmem` does for TCP. The stream
    /// then has no more than the limit in flight, and a writer waits while
    /// the limit is taken: a write through the adapter takes no more than
    /// the room there is, and [`write`](Self::write) waits until there is
    /// room for its data.
    ///
    /// The limit is also the most the stream has on its way at one time,
    /// so it bounds how fast the stream sends: about 0.94 × the limit ÷ the
    /// round trip, as the remote's window does. Over a round trip of 100 ms,
    /// a limit of 8 MiB allows about 79 MB a second, and one of 1 MiB about
    /// 9.9 MB a second. It only ever lowers what the stream sends, so it
    /// works with any remote.
    ///
    /// A limit below 8,192 bytes is raised to that. Lowering the limit
    /// below what the stream holds loses nothing: it sends no more, and
    /// takes no more writes, until acknowledgements bring it under the
    /// limit.
    pub fn set_write_buffer_limit(&self, limit: Option<u32>) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set_write_cap(limit);
    }

    /// The limit on what the stream holds written and not yet acknowledged,
    /// in bytes, if it has one. See
    /// [`set_write_buffer_limit`](Self::set_write_buffer_limit).
    pub fn write_buffer_limit(&self) -> Option<u32> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).write_cap()
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
    ///
    /// On a stream with a write buffer limit
    /// ([`set_write_buffer_limit`](Self::set_write_buffer_limit)), a write
    /// that does not fit under the limit waits until acknowledgements make
    /// room for it. A write larger than the limit is taken once nothing
    /// else is unacknowledged.
    pub async fn write(&self, data: &[u8]) -> Result<()> {
        let room = Arc::clone(&self.inner.lock().unwrap_or_else(|e| e.into_inner()).room);
        let acked = loop {
            // Listening before looking, so that room made in between is not
            // missed.
            let notified = room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                if inner.takes_write(data.len()) {
                    break inner.queue_write(data.to_vec())?;
                }
                inner.check_writable()?;
            }
            notified.await;
        };

        acked.await.map_err(|_| UdxError::StreamClosed)?
    }

    /// Read the next message. Returns `None` on EOF or stream close.
    pub async fn read(&mut self) -> Result<Option<Vec<u8>>> {
        let reader = self.reader.as_ref().ok_or(UdxError::StreamClosed)?;
        let frame = std::future::poll_fn(|cx| reader.lock().poll_frame(cx)).await;
        match frame {
            Frame::Data(data) => {
                if reader.limited() {
                    self.inner.lock().unwrap_or_else(|e| e.into_inner()).reader_took();
                }
                Ok(Some(data))
            }
            Frame::End => {
                self.reader = None;
                Ok(None)
            }
        }
    }

    /// Send a write-end (FIN) and wait for the remote peer to ACK it.
    ///
    /// Fails at once with "stream not connected" on a stream that still
    /// waits on its firewall hook; [`shutdown_graceful`](Self::shutdown_graceful)
    /// waits for the hook instead.
    pub async fn shutdown(&self) -> Result<()> {
        let acked = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.queue_end()?
        };

        acked.await.map_err(|_| UdxError::StreamClosed)?
    }

    /// Send a write-end (FIN) after everything written, and wait for the
    /// remote peer to ACK it — on a stream that still waits on its firewall
    /// hook ([`set_firewall_hook`](Self::set_firewall_hook)) as well.
    ///
    /// On a connected stream this is [`shutdown`](Self::shutdown). On a
    /// stream waiting on its hook, where `shutdown` fails at once with
    /// "stream not connected", the FIN is queued after what was written and
    /// goes out once the hook lets a remote in, even if nothing was written:
    /// the remote then reads the end of the stream. It fails if the hook
    /// turns the remote away, and with "stream not connected" on a stream
    /// that is neither connected nor waiting on a hook.
    pub async fn shutdown_graceful(&self) -> Result<()> {
        let acked = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.queue_end_awaiting_firewall()?
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
            inner.firewall_pending = true;
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
pub(crate) fn take_in(inner: &Mutex<StreamInner>, header: &Header, data: &[u8]) -> TakenIn {
    let for_the_reader = header.has_flag(FLAG_DATA)
        || header.has_flag(FLAG_END)
        || header.has_flag(FLAG_HEARTBEAT);
    if !for_the_reader || header.has_flag(FLAG_DESTROY) {
        return TakenIn { ack_owed: false, ack_now: false, for_its_task: Some(data.to_vec()) };
    }

    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
    if !guard.connected || guard.relay_target.is_some() {
        return TakenIn { ack_owed: false, ack_now: false, for_its_task: Some(data.to_vec()) };
    }

    tracing::trace!(
        flags = header.type_flags,
        seq = header.seq,
        ack = header.ack,
        "take_in: received packet"
    );

    let for_its_task = guard
        .has_news_for_sender(header)
        .then(|| sender_part(header, data));
    let owed_already = guard.ack_wanted;
    let carries_data = header.has_flag(FLAG_DATA) || header.has_flag(FLAG_END);
    let had_already = carries_data
        && header.seq != guard.next_remote_seq
        && !seq_after(header.seq, guard.next_remote_seq);
    let turned_away_before = guard.turned_away;
    guard.receive_packet_at(header, data, Instant::now());
    // A remote whose packet was turned away is waiting to hear of the room.
    let turned_away = guard.turned_away != turned_away_before;
    let ack_now =
        had_already || turned_away || !guard.recv_buf.is_empty() || header.has_flag(FLAG_END);

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
    let ack = inner.lock().unwrap_or_else(|e| e.into_inner()).take_ack_at(Instant::now());
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
                        guard.firewall_pending = false;
                        // What was written before the hook fired can go out
                        // now: a packet is addressed when it is sent.
                        if let Some(ref tx) = guard.notify_tx {
                            let _ = tx.send(StreamNotify::DataQueued);
                        }
                        tracing::debug!(addr = ?packet.addr, "firewall hook: accepted");
                    } else {
                        // Closed in the same step, so that no one sees a
                        // stream that neither waits on its hook nor is
                        // closed, with what was written still queued.
                        let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                        guard.firewall_pending = false;
                        guard.terminate();
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
        guard.take_window(&header, &sack_ranges, Instant::now());

        // ── ACK + SACK processing ────────────────────────────
        // An acknowledgement number of zero says nothing while nothing has
        // been acknowledged. Past the wrap of the sequence space it says
        // that everything up to the highest number has arrived.
        let acknowledges = header.ack > 0 || seq_after(header.ack, guard.remote_acked);
        let completed = if acknowledges || !sack_ranges.is_empty() {
            guard.process_ack(header.ack, &sack_ranges, Instant::now())
        } else {
            Vec::new()
        };

        // ── DATA, END and HEARTBEAT processing ───────────────
        guard.receive_packet_at(&header, &packet.data, Instant::now());

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
        guard.recv_buffer().end();
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
                    let mut guard = inner.lock().unwrap_or_else(|e| e.into_inner());
                    if guard.connected && guard.outgoing.is_empty() && guard.relay_target.is_none() {
                        let recv_window = guard.advertise();
                        guard.udp.clone().zip(guard.remote_addr).map(|(udp, addr)| {
                            let header = Header {
                                type_flags: FLAG_HEARTBEAT,
                                data_offset: 0,
                                remote_id: guard.remote_id,
                                recv_window,
                                seq: guard.next_seq,
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
            let undelivered = ((guard.connected || guard.firewall_pending) && !guard.drained())
                .then_some(guard.remote_acked);
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
        StreamInner::new(RecvBuffer::shared(), None)
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
            lost_to_rack: false,
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

    /// A link smaller than the starting size: every packet goes out with
    /// "don't fragment", so one larger than the link would be refused
    /// (`EMSGSIZE`) and the stream would time out. Its packets are cut to
    /// the link instead.
    #[test]
    fn a_link_smaller_than_the_starting_size_sets_the_mtu() {
        let mut inner = stream_to(V4);
        inner.limit_mtu_to_link(Some(1000));

        assert_eq!(inner.mtu_state, MtuState::SearchComplete);
        assert_eq!(inner.mtu, 1000);
        assert_eq!(inner.max_payload(), 1000 - IPV4_UDP_OVERHEAD - HEADER_SIZE);
        assert_eq!(inner.congestion.mss as usize, inner.max_payload());
        let largest_ack = IPV4_UDP_OVERHEAD + HEADER_SIZE + inner.max_sack_ranges() * SACK_RANGE_SIZE;
        assert!(largest_ack <= 1000, "an acknowledgement of {largest_ack} bytes");
    }

    /// A link too small for a header and a byte of payload is left as it
    /// was: no packet of the stream fits it.
    #[test]
    fn a_link_too_small_for_a_packet_leaves_the_mtu_as_it_is() {
        let mut inner = stream_to(V4);
        inner.limit_mtu_to_link(Some(IPV4_UDP_OVERHEAD + HEADER_SIZE));

        assert_eq!(inner.mtu, MTU_BASE);
        assert!(inner.max_payload() > 0);
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
    fn with_reader() -> (StreamInner, RecvReader) {
        let mut inner = StreamInner::new(RecvBuffer::shared(), None);
        let read_rx = inner.recv_reader();
        inner.connected = true;
        inner.set_remote_addr(REMOTE.parse().unwrap());
        inner.mtu_state = MtuState::SearchComplete;
        (inner, read_rx)
    }

    /// The bytes the reader has been handed so far, and whether it has been
    /// handed the end of the stream.
    fn read(reader: &mut RecvReader) -> (Vec<u8>, bool) {
        let mut bytes = Vec::new();
        let mut ended = false;
        while let Ok(event) = reader.try_recv() {
            match event {
                Frame::Data(data) => bytes.extend(data),
                Frame::End => ended = true,
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

    /// A stream with room for a thousand packets at its remote and in its
    /// congestion window, and a pacer that holds nothing back from a
    /// millisecond after its epoch, the time to send from.
    fn roomy() -> (StreamInner, Instant) {
        let mut inner = connected();
        inner.congestion.cwnd = 1_000;
        inner.congestion.pacing_bytes_per_ms = 1_000_000;
        let t0 = inner.epoch + millis(1);
        (inner, t0)
    }

    #[test]
    fn a_stream_with_a_write_limit_has_no_more_than_that_in_flight() {
        let (mut inner, t0) = roomy();
        inner.set_write_cap(Some(20 * MAX_PAYLOAD as u32));
        queue(&mut inner, 100);
        assert_eq!(data_seqs(&send(&mut inner, t0)), (0..20).collect::<Vec<_>>());

        // An acknowledgement of five makes room for five more.
        let t1 = t0 + millis(20);
        inner.process_ack(5, &[], t1);
        assert_eq!(data_seqs(&send(&mut inner, t1)), (20..25).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_writer_waits_at_the_write_limit() {
        let mut inner = writable().await;
        inner.congestion.cwnd = 1_000;
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let limit = 100_000;
        inner.set_write_cap(Some(limit as u32));
        assert_eq!(inner.write_limit(), limit);

        // A write takes no more than the room there is, and a writer at the
        // limit is asked to wait.
        let buf = vec![7u8; 60_000];
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(60_000));
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(40_000));
        assert_eq!(inner.unacked_bytes, limit);
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), None);

        // An acknowledgement makes room, and wakes it.
        let t0 = inner.epoch;
        send(&mut inner, t0);
        inner.process_ack(1, &[], t0 + millis(20));
        inner.wake_writer();
        assert_eq!(woken.count(), 1);
        assert_eq!(inner.write_some(&buf, &waker).unwrap(), Some(MAX_PAYLOAD));
    }

    #[test]
    fn a_lower_write_limit_holds_back_what_is_queued_and_loses_nothing() {
        let (mut inner, t0) = roomy();
        queue(&mut inner, 40);
        assert_eq!(data_seqs(&send(&mut inner, t0)).len(), 40);
        queue(&mut inner, 40);

        // Lowered below what is in flight: nothing more goes out until
        // what is in flight is under it.
        inner.set_write_cap(Some(10 * MAX_PAYLOAD as u32));
        assert!(send(&mut inner, t0).is_empty());
        let t1 = t0 + millis(20);
        inner.process_ack(30, &[], t1);
        assert!(send(&mut inner, t1).is_empty());
        inner.process_ack(35, &[], t1);
        assert_eq!(data_seqs(&send(&mut inner, t1)), (40..45).collect::<Vec<_>>());

        // Everything written arrives.
        let mut now = t1;
        while !inner.drained() {
            now += millis(20);
            let ack = inner.next_seq;
            inner.process_ack(ack, &[], now);
            send(&mut inner, now);
        }
        assert_eq!(inner.next_seq, 80);
    }

    #[test]
    fn a_write_larger_than_the_write_limit_is_taken_once_nothing_else_is_unacknowledged() {
        let mut inner = connected();
        assert!(inner.takes_write(100_000));
        inner.set_write_cap(Some(10_000));
        assert!(inner.takes_write(100_000));

        write(&mut inner, 6_000);
        assert!(inner.takes_write(4_000));
        assert!(!inner.takes_write(4_001));
        assert!(!inner.takes_write(100_000));
    }

    #[test]
    fn a_write_limit_below_eight_kilobytes_is_raised_to_that() {
        let mut inner = connected();
        inner.set_write_cap(Some(100));
        assert_eq!(inner.write_cap(), Some(MIN_READ_BUFFER_LIMIT));
        inner.set_write_cap(None);
        assert_eq!(inner.write_cap(), None);
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
        inner.receive_data(0, &[1], FLAG_DATA, Instant::now());
        inner.receive_data(1, &[2], FLAG_DATA, Instant::now());
        inner.receive_data(3, &[4], FLAG_DATA, Instant::now());
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
            inner.receive_data(seq, &[0], FLAG_DATA, Instant::now());
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
            inner.receive_data(seq, &[0], FLAG_DATA, Instant::now());
        }
        let packet = inner.build_ack();
        let header = Header::decode(&packet).unwrap();
        assert_eq!(header.data_offset, 248);
        assert_eq!(packet.len(), HEADER_SIZE + 248);
        assert_eq!(sack_ranges_in(&header, &packet).len(), 31);

        // One range more, and the length is left out.
        inner.receive_data(63, &[0], FLAG_DATA, Instant::now());
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
        let taken = take_in(&inner, &header, &data);
        assert!(taken.ack_owed);
        assert!(taken.for_its_task.is_none(), "it says nothing new to the sending side");

        // The end of the stream rides on the last data.
        let (header, data) = incoming(FLAG_DATA | FLAG_END, 1, 0, b" last");
        let taken = take_in(&inner, &header, &data);
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
        assert!(take_in(&inner, &header, &data).ack_owed);

        assert_eq!(read(&mut reader).0, b"payload");
    }

    #[test]
    fn a_heartbeat_draws_an_acknowledgement_and_hands_the_reader_nothing() {
        let (inner, mut reader) = with_reader();
        let inner = Mutex::new(inner);

        let (header, data) = incoming(FLAG_HEARTBEAT, 0, 0, &[]);
        let taken = take_in(&inner, &header, &data);

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
            take_in(&inner, &header, &data);
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
            take_in(&inner, &header, &data).ack_now
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
        let taken = take_in(&inner, &header, &data);

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
        let taken = take_in(&inner, &header, &data);
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
        assert!(take_in(&inner, &header, &data).for_its_task.is_none());

        // Another window is news again.
        let (mut header, mut data) = incoming(FLAG_DATA, 2, 2, b"more");
        header.recv_window = DEFAULT_RWND / 2;
        header.encode_into(&mut data);
        let part = take_in(&inner, &header, &data).for_its_task.expect("the window is news");
        assert_eq!(Header::decode(&part).unwrap().recv_window, DEFAULT_RWND / 2);
    }

    #[test]
    fn packets_a_stream_cannot_take_in_go_to_its_own_task_whole() {
        fn left_whole(inner: StreamInner, type_flags: u8) {
            let inner = Mutex::new(inner);
            let (header, data) = incoming(type_flags, 0, 0, b"data");
            let taken = take_in(&inner, &header, &data);

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
        assert!(take_in(&inner, &header, &data).ack_owed);

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
        inner.receive_data(0, &[1], FLAG_DATA, Instant::now());
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
        let taken = take_in(&inner, &header, &data);
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
        inner.receive_packet(&header, &data);
        assert_eq!(read(&mut reader), (Vec::new(), false), "it waits for what comes before it");
        assert_eq!(inner.sack_ranges(), [SackRange { start: u32::MAX, end: 0 }]);

        // The gap is filled, and the reader is handed all of it in order.
        for (seq, payload) in [(u32::MAX - 2, b"a"), (u32::MAX - 1, b"b")] {
            let (header, data) = incoming(FLAG_DATA, seq, 0, payload);
            inner.receive_packet(&header, &data);
        }
        assert_eq!(read(&mut reader), (b"abc".to_vec(), false));
        assert_eq!(inner.next_remote_seq, 0, "the next number is the first one again");
        assert!(inner.sack_ranges().is_empty());
    }

    #[test]
    fn packets_ahead_of_a_gap_on_both_sides_of_the_wrap_are_named_and_then_read_in_order() {
        let (mut inner, mut reader) = with_reader();
        inner.next_remote_seq = u32::MAX - 4;
        let receive = |inner: &mut StreamInner, seq: u32, payload: &[u8]| {
            let (header, data) = incoming(FLAG_DATA, seq, 0, payload);
            inner.receive_packet(&header, &data);
        };
        let range = |start: u32, end: u32| SackRange { start, end };

        // The ranges come in sequence order: what was received up to the
        // wrap before what was received past it.
        receive(&mut inner, u32::MAX - 2, b"c");
        receive(&mut inner, u32::MAX, b"e");
        assert_eq!(inner.sack_ranges(), [range(u32::MAX - 2, u32::MAX - 1), range(u32::MAX, 0)]);
        receive(&mut inner, 1, b"g");
        assert_eq!(
            inner.sack_ranges(),
            [range(u32::MAX - 2, u32::MAX - 1), range(u32::MAX, 0), range(1, 2)]
        );
        // A range runs on across the wrap.
        receive(&mut inner, u32::MAX - 1, b"d");
        assert_eq!(inner.sack_ranges(), [range(u32::MAX - 2, 0), range(1, 2)]);
        receive(&mut inner, 0, b"f");
        assert_eq!(inner.sack_ranges(), [range(u32::MAX - 2, 2)]);
        assert_eq!(read(&mut reader), (Vec::new(), false), "all of it waits for what comes before it");

        // The gap is filled, and the reader is handed all of it in order.
        receive(&mut inner, u32::MAX - 4, b"a");
        receive(&mut inner, u32::MAX - 3, b"b");
        assert_eq!(read(&mut reader), (b"abcdefg".to_vec(), false));
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
        assert!(take_in(&inner, &header, &data).ack_now, "it left a gap");
        assert_eq!(inner.lock().unwrap().sack_ranges(), [SackRange { start: 0, end: 1 }]);

        // What is awaited arrives, and the reader has both.
        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"sooner");
        take_in(&inner, &header, &data);
        assert_eq!(read(&mut reader), (b"sooner later".to_vec(), false));
        assert_eq!(inner.lock().unwrap().next_remote_seq, 1);

        // The same packet again is one the stream has already, though its
        // number is the highest there is.
        let (header, data) = incoming(FLAG_DATA, u32::MAX, 0, b"sooner");
        let taken = take_in(&inner, &header, &data);
        assert!(taken.ack_now, "the remote is told at once that it arrived");
        assert!(inner.lock().unwrap().recv_buf.is_empty(), "and it is not kept a second time");
        assert_eq!(read(&mut reader), (Vec::new(), false));
    }

    // ── A stream whose own sequence numbers wrap ──────────────────────────────

    /// A stream as [`with_reader`] makes it whose sequence numbers, in both
    /// directions, begin at `first`, where a new stream's begin at zero.
    fn starting_at(first: u32) -> (StreamInner, RecvReader) {
        let (mut inner, reader) = with_reader();
        number_from(&mut inner, first);
        inner.next_remote_seq = first;
        // C: `udx_stream_set_ack`.
        inner.send_wl1 = first;
        (inner, reader)
    }

    /// Have a stream that has sent nothing yet number its packets from
    /// `first`, as if all the numbers before it were behind it.
    fn number_from(inner: &mut StreamInner, first: u32) {
        inner.next_seq = first;
        inner.remote_acked = first;
        // C: `udx_stream_set_seq`.
        inner.send_wl2 = first;
        inner.rack_next_seq = first;
        inner.rack_fack = first;
        inner.tlp_end_seq = first;
        inner.congestion.high_seq = first;
    }

    /// A stream that has `left` packets to send before its sequence numbers
    /// wrap.
    fn before_the_wrap(left: u32) -> StreamInner {
        starting_at(0u32.wrapping_sub(left)).0
    }

    /// The sequence numbers of the packets a stream holds, oldest first.
    fn held(inner: &StreamInner) -> Vec<u32> {
        inner.outgoing_in_order().collect()
    }

    #[test]
    fn a_cumulative_acknowledgement_covers_what_is_before_it_across_the_wrap() {
        let mut inner = before_the_wrap(3);
        queue(&mut inner, 8);
        let t0 = inner.epoch;
        let first = u32::MAX - 2;
        assert_eq!(data_seqs(&send(&mut inner, t0)), [first, first + 1, u32::MAX, 0, 1, 2, 3, 4]);

        // Short of the wrap: the packets past it have lower numbers, and
        // are still owed.
        inner.process_ack(u32::MAX, &[], t0 + millis(20));
        assert_eq!(inner.remote_acked, u32::MAX);
        assert_eq!(held(&inner), [u32::MAX, 0, 1, 2, 3, 4]);
        assert_eq!(inner.congestion.delivered, 2);

        // Up to the wrap: the remote awaits the first number again.
        inner.process_ack(0, &[], t0 + millis(21));
        assert_eq!(inner.remote_acked, 0);
        assert_eq!(held(&inner), [0, 1, 2, 3, 4]);
        assert_eq!(inner.congestion.delivered, 3);

        // Past it.
        inner.process_ack(3, &[], t0 + millis(22));
        assert_eq!(held(&inner), [3, 4]);
        assert_eq!(inner.congestion.delivered, 6);
        assert_eq!(inner.inflight(), 2);
    }

    #[test]
    fn one_acknowledgement_covers_packets_on_both_sides_of_the_wrap() {
        let mut inner = before_the_wrap(3);
        queue(&mut inner, 8);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        // The remote has the highest number and the first one, and then
        // everything up to the third.
        inner.process_ack(u32::MAX - 2, &[SackRange { start: u32::MAX, end: 1 }], t0 + millis(20));
        assert_eq!(inner.sacks, 2);
        inner.process_ack(2, &[], t0 + millis(22));

        assert_eq!(inner.remote_acked, 2);
        assert_eq!(held(&inner), [2, 3, 4]);
        assert_eq!(inner.sacks, 0, "what was acknowledged selectively is behind the acknowledgement");
        assert_eq!(inner.congestion.delivered, 5);
    }

    #[test]
    fn a_write_is_complete_once_its_last_packet_is_acknowledged_and_not_before_across_the_wrap() {
        let mut inner = before_the_wrap(2);
        // Three writes of two packets each. The first ends on the highest
        // number, and the other two are past the wrap.
        let _written: Vec<_> = (0..3).map(|_| write(&mut inner, 2 * MAX_PAYLOAD)).collect();
        let t0 = inner.epoch;
        assert_eq!(send(&mut inner, t0).len(), 6);

        // One packet short of the first write's end. The writes past the
        // wrap end on lower numbers than this acknowledgement.
        assert!(inner.process_ack(u32::MAX, &[], t0 + millis(20)).is_empty());
        assert_eq!(inner.pending_writes.len(), 3);

        assert_eq!(inner.process_ack(0, &[], t0 + millis(21)).len(), 1);
        assert_eq!(inner.process_ack(3, &[], t0 + millis(22)).len(), 1);
        assert_eq!(inner.process_ack(4, &[], t0 + millis(23)).len(), 1);
        assert!(inner.pending_writes.is_empty());
        assert!(inner.all_acked());
    }

    #[tokio::test]
    async fn an_acknowledgement_numbered_zero_counts_once_the_numbers_have_wrapped() {
        let mut inner = before_the_wrap(3);
        let mut written = write(&mut inner, 3 * MAX_PAYLOAD);
        let t0 = inner.epoch;
        assert_eq!(data_seqs(&send(&mut inner, t0)), [u32::MAX - 2, u32::MAX - 1, u32::MAX]);
        let inner = Arc::new(Mutex::new(inner));

        // The remote has all three, and awaits the first number again.
        assert!(handle_incoming(&inner, acknowledgement(0, &[])).await);
        assert!(matches!(written.try_recv(), Ok(Ok(()))));
        assert!(inner.lock().unwrap().all_acked());
    }

    #[test]
    fn the_retransmission_timer_resends_in_the_order_of_sending_across_the_wrap() {
        let mut inner = before_the_wrap(2);
        queue(&mut inner, 5);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        let timeout = t0 + millis(u64::from(INITIAL_RTO_MS));
        assert!(!inner.on_timers(timeout));
        assert_eq!(inner.retransmit_queue, [u32::MAX - 1, u32::MAX, 0, 1, 2]);
        assert_eq!(data_seqs(&send(&mut inner, timeout)), [u32::MAX - 1, u32::MAX, 0, 1, 2]);
    }

    #[test]
    fn packets_lost_from_one_flight_go_out_again_in_the_order_of_sending_across_the_wrap() {
        let mut inner = before_the_wrap(2);
        queue(&mut inner, 12);
        let t0 = inner.epoch;
        // One flight, sent at one moment: the last two numbers, and eight
        // from zero on.
        send(&mut inner, t0);

        // The remote has the fourth, fifth and sixth of them.
        let now = t0 + millis(20);
        inner.process_ack(u32::MAX - 1, &[SackRange { start: 1, end: 4 }], now);
        assert_eq!(inner.sacks, 3);
        assert_eq!(inner.retransmit_queue, [u32::MAX - 1, u32::MAX, 0]);
        assert_eq!(data_seqs(&send(&mut inner, now)), [u32::MAX - 1, u32::MAX, 0]);
    }

    #[test]
    fn an_acknowledgement_names_the_ranges_nearest_what_it_awaits_across_the_wrap() {
        let (mut inner, _reader) = with_reader();
        // The stream awaits a packet 100 short of the wrap, and has every
        // other one of the 400 after it: 200 ranges, 50 of them short of
        // the wrap. That is more than an acknowledgement names.
        let awaited = 0u32.wrapping_sub(100);
        inner.next_remote_seq = awaited;
        for nth in (1..400).step_by(2) {
            inner.receive_data(awaited.wrapping_add(nth), &[0], FLAG_DATA, Instant::now());
        }

        let ranges = inner.sack_ranges();
        assert_eq!(ranges.len(), inner.max_sack_ranges());
        assert!(ranges.len() < 200);
        let mut start = awaited.wrapping_add(1);
        for range in ranges {
            assert_eq!(range, SackRange { start, end: start.wrapping_add(1) });
            start = start.wrapping_add(2);
        }
    }

    /// What passed between two streams, and when, in milliseconds. Sequence
    /// numbers are counted from where the streams' numbers began.
    #[derive(Debug, PartialEq)]
    enum Passed {
        /// A packet with data left the sender: its number, length and flags.
        Data(u64, u32, usize, u8),
        /// The receiver acknowledged: the number it awaits, and its ranges.
        Ack(u64, u32, Vec<(u32, u32)>),
        /// The reader was handed this many bytes.
        Read(u64, usize),
        /// The sender went from repairing no loss to repairing some, or back.
        Repairing(u64, CaState),
        /// The write was complete.
        Written(u64),
    }

    /// Whether a path loses the `nth` packet it is given, at `ms`, and how
    /// long it takes with it if not. It carries nothing for over a second,
    /// loses one packet in sixteen of the rest, and holds one in five back
    /// by up to 9 ms, so that later ones pass it.
    fn fate(nth: u64, ms: u64) -> Option<u64> {
        if (80..1_150).contains(&ms) {
            return None;
        }
        let mut x = (nth + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 31;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 29;
        if x % 16 == 0 {
            return None;
        }
        let held_back = if (x >> 8) % 5 == 0 { (x >> 16) % 10 } else { 0 };
        Some(10 + held_back)
    }

    /// Send `packets` full packets from one stream to another over the path
    /// that [`fate`] describes, both streams numbering from `first`. Returns
    /// what passed, and the bytes the reader was handed.
    fn transfer_over_a_poor_path(first: u32, packets: usize) -> (Vec<Passed>, Vec<u8>) {
        let (mut sender, _) = starting_at(first);
        let (mut receiver, mut reader) = starting_at(first);
        let (tx, mut written) = oneshot::channel();
        let data: Vec<u8> = (0..packets * MAX_PAYLOAD).map(|i| (i % 251) as u8).collect();
        let len = data.len();
        sender.push_write(QueuedWrite { data, taken: 0, end: false, tx: Some(tx) });

        let t0 = sender.epoch;
        let mut passed = Vec::new();
        let mut received = Vec::new();
        // What is on the path: when it arrives, the order it was sent in,
        // whether it is on its way to the receiver, and the packet.
        let mut path: Vec<(u64, u64, bool, Vec<u8>)> = Vec::new();
        let mut sent = 0;
        let mut repairing = CaState::Open;
        let mut complete = false;

        for ms in 0..60_000 {
            let now = t0 + millis(ms);

            let (mut due, later): (Vec<_>, Vec<_>) =
                std::mem::take(&mut path).into_iter().partition(|packet| packet.0 <= ms);
            path = later;
            due.sort_by_key(|packet| (packet.0, packet.1));
            for (_, _, to_receiver, packet) in due {
                let header = Header::decode(&packet).unwrap();
                if to_receiver {
                    receiver.receive_packet_at(&header, &packet, now);
                } else {
                    let ranges = sack_ranges_in(&header, &packet);
                    for tx in sender.process_ack(header.ack, &ranges, now).into_iter().flatten() {
                        let _ = tx.send(Ok(()));
                    }
                }
            }

            let (bytes, _) = read(&mut reader);
            if !bytes.is_empty() {
                passed.push(Passed::Read(ms, bytes.len()));
                received.extend(bytes);
            }
            if matches!(written.try_recv(), Ok(Ok(()))) {
                passed.push(Passed::Written(ms));
                complete = true;
            }
            if complete && received.len() == len {
                return (passed, received);
            }

            receiver.on_timers(now);
            if let Some((ack, _)) = receiver.take_ack_at(now) {
                let header = Header::decode(&ack).unwrap();
                let ranges = sack_ranges_in(&header, &ack)
                    .iter()
                    .map(|range| (range.start.wrapping_sub(first), range.end.wrapping_sub(first)))
                    .collect();
                passed.push(Passed::Ack(ms, header.ack.wrapping_sub(first), ranges));
                if let Some(takes) = fate(sent, ms) {
                    path.push((ms + takes, sent, false, ack));
                }
                sent += 1;
            }

            assert!(!sender.on_timers(now), "the sender gave up after {ms} ms");
            for packet in send(&mut sender, now) {
                let header = Header::decode(&packet).unwrap();
                passed.push(Passed::Data(ms, header.seq.wrapping_sub(first), packet.len(), header.type_flags));
                if let Some(takes) = fate(sent, ms) {
                    path.push((ms + takes, sent, true, packet));
                }
                sent += 1;
            }
            if sender.congestion.ca_state != repairing {
                repairing = sender.congestion.ca_state;
                passed.push(Passed::Repairing(ms, repairing));
            }
        }
        panic!("the transfer was not over after a minute: {} of {len} bytes read", received.len());
    }

    #[test]
    fn a_transfer_over_a_poor_path_runs_the_same_wherever_the_wrap_falls() {
        const PACKETS: usize = 1_200;
        let (expected, data) = transfer_over_a_poor_path(0, PACKETS);
        assert!(data.iter().enumerate().all(|(i, &byte)| byte == (i % 251) as u8));

        // The path gave the stream every kind of repair to do.
        let numbers: Vec<u32> = expected
            .iter()
            .filter_map(|passed| match passed {
                Passed::Data(_, seq, _, _) => Some(*seq),
                _ => None,
            })
            .collect();
        let sent_again = numbers.len() - PACKETS;
        assert!(sent_again > 50, "{sent_again} packets were sent again");
        let most_ranges = expected
            .iter()
            .filter_map(|passed| match passed {
                Passed::Ack(_, _, ranges) => Some(ranges.len()),
                _ => None,
            })
            .max();
        assert!(most_ranges > Some(5), "an acknowledgement named {most_ranges:?} ranges at most");
        for state in [CaState::Recovery, CaState::Loss] {
            let seen = expected.iter().any(|passed| matches!(passed, Passed::Repairing(_, s) if *s == state));
            assert!(seen, "the sender was never in {state:?}");
        }

        // The wrap falls in the first flight, in the flights after it, among
        // the packets that the path's silence costs, in the repair after
        // that, and at the very end.
        for left in [1, 2, 9, 10, 11, 37, 64, 150, 300, 600, 900, 1_199, 1_200] {
            let (passed, read) = transfer_over_a_poor_path(0u32.wrapping_sub(left), PACKETS);
            let differs = passed.iter().zip(&expected).position(|(here, there)| here != there);
            if let Some(at) = differs {
                panic!(
                    "with {left} packets to go before the wrap, what passes differs: {:?}, and without a wrap {:?}",
                    passed[at], expected[at]
                );
            }
            assert_eq!(passed.len(), expected.len(), "with {left} packets to go before the wrap");
            assert!(read == data, "with {left} packets to go before the wrap");
        }
    }

    #[tokio::test]
    async fn two_streams_carry_data_both_ways_across_the_wrap() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn bound() -> (crate::native::socket::UdxSocket, SocketAddr) {
            let socket = crate::native::socket::UdxSocket::new();
            socket.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
            let addr = socket.local_addr().await.unwrap();
            (socket, addr)
        }
        // Have `sender` number its packets from `first`, and `receiver` await
        // that number.
        fn numbered_from(sender: &UdxStream, receiver: &UdxStream, first: u32) {
            number_from(&mut sender.inner.lock().unwrap(), first);
            let mut receiver = receiver.inner.lock().unwrap();
            receiver.next_remote_seq = first;
            receiver.send_wl1 = first;
        }

        let (socket_a, addr_a) = bound().await;
        let (socket_b, addr_b) = bound().await;
        let a = UdxStream::new(1);
        let b = UdxStream::new(2);
        // Each end has a few packets to send before its numbers wrap.
        numbered_from(&a, &b, 0u32.wrapping_sub(40));
        numbered_from(&b, &a, 0u32.wrapping_sub(7));
        a.connect(&socket_a, 2, addr_b).await.unwrap();
        b.connect(&socket_b, 1, addr_a).await.unwrap();

        // Two hundred packets and more, each way at once.
        let from_a: Vec<u8> = (0..300_000).map(|i| (i % 251) as u8).collect();
        let from_b: Vec<u8> = (0..300_000).map(|i| (i % 241) as u8).collect();
        let (mut read_a, mut write_a) = tokio::io::split(a.into_async_stream());
        let (mut read_b, mut write_b) = tokio::io::split(b.into_async_stream());
        let mut at_a = Vec::new();
        let mut at_b = Vec::new();

        let both_ways = async {
            tokio::join!(
                async {
                    write_a.write_all(&from_a).await.unwrap();
                    write_a.shutdown().await.unwrap();
                },
                async {
                    write_b.write_all(&from_b).await.unwrap();
                    write_b.shutdown().await.unwrap();
                },
                async { read_a.read_to_end(&mut at_a).await.unwrap() },
                async { read_b.read_to_end(&mut at_b).await.unwrap() },
            )
        };
        tokio::time::timeout(Duration::from_secs(20), both_ways)
            .await
            .expect("the transfer stalled");

        assert!(at_a == from_b, "{} of {} bytes arrived, or not as sent", at_a.len(), from_b.len());
        assert!(at_b == from_a, "{} of {} bytes arrived, or not as sent", at_b.len(), from_a.len());
    }

    // ── Flow control ──────────────────────────────────────────────────────────

    /// The payload of the packets most of these tests send.
    const PACKET: usize = 1_000;

    /// A stream as [`with_reader`] makes it that holds no more than `limit`
    /// bytes for its reader.
    fn limited(limit: usize) -> (StreamInner, RecvReader) {
        let (mut inner, reader) = with_reader();
        inner.set_read_limit(Some(limit as u32));
        (inner, reader)
    }

    /// A packet with `len` bytes of data, numbered `seq`, arrives. Returns
    /// whether the stream took it.
    fn arrives(inner: &mut StreamInner, seq: u32, len: usize) -> bool {
        let turned_away = inner.turned_away;
        let (header, data) = incoming(FLAG_DATA, seq, 0, &vec![seq as u8; len]);
        inner.receive_packet(&header, &data);
        inner.turned_away == turned_away
    }

    /// The reader takes up to `packets` of the packets it has been handed,
    /// and tells the stream, as a reader does. Returns how many bytes it
    /// took.
    fn reader_takes(
        inner: &mut StreamInner,
        reader: &mut RecvReader,
        packets: usize,
    ) -> usize {
        let mut bytes = 0;
        for _ in 0..packets {
            let Ok(Frame::Data(data)) = reader.try_recv() else {
                break;
            };
            if reader.limited() {
                inner.reader_took();
            }
            bytes += data.len();
        }
        bytes
    }

    /// The window in the acknowledgement the stream would send now.
    fn window(inner: &mut StreamInner) -> usize {
        Header::decode(&inner.build_ack()).unwrap().recv_window as usize
    }

    /// What a remote says of its window: a packet numbered `seq` that
    /// acknowledges `ack` and advertises `window`.
    fn says(seq: u32, ack: u32, window: u32) -> IncomingPacket {
        let (mut header, mut data) = incoming(0, seq, ack, &[]);
        header.recv_window = window;
        header.encode_into(&mut data);
        IncomingPacket { data, addr: REMOTE.parse().unwrap() }
    }

    /// The stream hears from its remote, at `now`: it acknowledges `ack` and
    /// has room for `window` bytes.
    fn hears(inner: &mut StreamInner, ack: u32, window: u32, now: Instant) {
        let (mut header, _) = incoming(0, inner.send_wl1, ack, &[]);
        header.recv_window = window;
        inner.take_window(&header, &[], now);
        if seq_after(ack, inner.remote_acked) {
            inner.process_ack(ack, &[], now);
        }
    }

    #[test]
    fn a_stream_without_a_limit_takes_everything_in_order_and_advertises_the_same_window_for_it() {
        let (mut inner, mut reader) = with_reader();
        assert_eq!(inner.read_limit(), None);

        // More than the window it advertises, in order to a reader that
        // takes nothing.
        for seq in 0..5_000 {
            assert!(arrives(&mut inner, seq, MAX_PAYLOAD));
        }
        assert_eq!(inner.turned_away, 0);
        assert_eq!(window(&mut inner), DEFAULT_RWND as usize);

        // Its packets with data say the same.
        queue(&mut inner, 1);
        let t0 = inner.epoch;
        let sent = send(&mut inner, t0);
        assert_eq!(data_seqs(&sent), [0]);
        assert!(sent.iter().all(|packet| Header::decode(packet).unwrap().recv_window == DEFAULT_RWND));

        assert_eq!(read(&mut reader).0.len(), 5_000 * MAX_PAYLOAD);
    }

    #[test]
    fn a_stream_without_a_limit_holds_no_more_than_its_window_ahead_of_a_gap() {
        let (mut inner, mut reader) = with_reader();

        // Packet 0 is missing. What arrives ahead of it is left out of the
        // window: a remote no longer counts it in flight once it has been
        // selectively acknowledged.
        for seq in 1..=10 {
            assert!(arrives(&mut inner, seq, MAX_PAYLOAD));
        }
        assert_eq!(window(&mut inner), MAX_OUT_OF_ORDER - 10 * MAX_PAYLOAD);

        // A remote that keeps sending what follows the gap all the same has
        // the stream hold as much as `MAX_OUT_OF_ORDER`, and no more. The
        // window keeps room for the packet that fills the gap.
        let ahead = MAX_OUT_OF_ORDER / MAX_PAYLOAD;
        for seq in 11..10_000 {
            assert_eq!(arrives(&mut inner, seq, MAX_PAYLOAD), seq as usize <= ahead);
        }
        assert_eq!(inner.recv_buf.len(), ahead);
        assert!(inner.held() <= MAX_OUT_OF_ORDER);
        assert_eq!(inner.turned_away, (9_999 - ahead) as u64);
        assert_eq!(window(&mut inner), FULL_PACKET);

        // What has been taken is kept.
        assert_eq!(inner.sack_ranges(), [SackRange { start: 1, end: ahead as u32 + 1 }]);

        // The missing packet is taken, all of it goes to the reader, and so
        // does what was turned away when it comes again.
        assert!(arrives(&mut inner, 0, MAX_PAYLOAD));
        assert!(inner.recv_buf.is_empty());
        assert_eq!(inner.next_remote_seq, ahead as u32 + 1);
        assert_eq!(window(&mut inner), DEFAULT_RWND as usize);
        for seq in ahead as u32 + 1..10_000 {
            assert!(arrives(&mut inner, seq, MAX_PAYLOAD));
        }
        assert_eq!(read(&mut reader).0.len(), 10_000 * MAX_PAYLOAD);

        // A byte in each packet, or none: the stream holds no more packets
        // than its window has room for at `OUT_OF_ORDER_FRAME_SIZE` each.
        for len in [1, 0] {
            let (mut inner, _reader) = with_reader();
            for seq in 1..40_000 {
                arrives(&mut inner, seq, len);
            }
            assert_eq!(inner.recv_buf.len(), MAX_OUT_OF_ORDER / OUT_OF_ORDER_FRAME_SIZE);
            assert_eq!(inner.held(), inner.recv_buf.len() * len);
        }
    }

    /// A stream without a limit that holds `ahead` packets of 1,000 bytes
    /// behind a missing packet 0, and has handed its reader nothing.
    fn behind_a_gap(ahead: u32) -> (StreamInner, RecvReader) {
        let (mut inner, reader) = with_reader();
        for seq in 1..=ahead {
            assert!(arrives(&mut inner, seq, 1_000));
        }
        (inner, reader)
    }

    #[test]
    fn a_limit_given_to_a_stream_holding_more_than_it_behind_a_gap_lets_the_gap_be_filled() {
        const LIMIT: usize = 16_384;
        let (mut inner, mut reader) = behind_a_gap(100);
        assert!(inner.held() > LIMIT);

        inner.set_read_limit(Some(LIMIT as u32));

        // The remote is told of room for the missing packet, and it is
        // taken when it comes again.
        assert_eq!(window(&mut inner), FULL_PACKET);
        assert!(arrives(&mut inner, 0, 1_000));
        assert!(inner.recv_buf.is_empty());
        assert_eq!(inner.next_remote_seq, 101);

        // Nothing more is taken until the reader is back under the limit.
        assert_eq!(window(&mut inner), 0);
        assert!(!arrives(&mut inner, 101, 1_000));
        assert_eq!(reader_takes(&mut inner, &mut reader, usize::MAX), 101_000);
        assert!(arrives(&mut inner, 101, 1_000));
    }

    #[test]
    fn a_lower_limit_does_not_keep_out_the_packet_that_fills_a_gap() {
        const LIMIT: usize = 16_384;
        let (mut inner, mut reader) = limited(8 * LIMIT);
        for seq in 1..=50 {
            assert!(arrives(&mut inner, seq, 1_000));
        }
        assert!(inner.held() > LIMIT);

        inner.set_read_limit(Some(LIMIT as u32));
        assert_eq!(window(&mut inner), FULL_PACKET);
        assert!(arrives(&mut inner, 0, 1_000));
        assert!(inner.recv_buf.is_empty());
        assert_eq!(read(&mut reader).0.len(), 51_000);
    }

    #[test]
    fn the_packet_that_fills_a_second_gap_is_taken_once_the_reader_has_taken_everything() {
        const LIMIT: usize = 16_384;
        // Packets 1 to 10 wait behind packet 0, and 12 to 40 behind 11.
        let (mut inner, mut reader) = behind_a_gap(10);
        for seq in 12..=40 {
            assert!(arrives(&mut inner, seq, 1_000));
        }
        inner.set_read_limit(Some(LIMIT as u32));
        assert!(arrives(&mut inner, 0, 1_000));
        assert_eq!(inner.next_remote_seq, 11);

        // The reader has not taken what it was handed, and the stream holds
        // more than its limit: packet 11 is turned away, as on any full
        // stream.
        assert_eq!(window(&mut inner), 0);
        assert!(!arrives(&mut inner, 11, 1_000));

        // Some of it is not enough, while what waits behind 11 is over the
        // limit by itself.
        reader_takes(&mut inner, &mut reader, 5);
        assert!(!arrives(&mut inner, 11, 1_000));

        // All of it is, and the remote, which was told there was no room,
        // is told at once that there is: it need not wait to ask.
        inner.ack_wanted = false;
        reader_takes(&mut inner, &mut reader, usize::MAX);
        assert!(inner.ack_wanted, "the window reopened unannounced");
        assert_eq!(window(&mut inner), FULL_PACKET);
        assert!(arrives(&mut inner, 11, 1_000));
        assert!(inner.recv_buf.is_empty());
        assert_eq!(inner.next_remote_seq, 41);
        assert_eq!(read(&mut reader).0.len(), 30_000);
    }

    #[test]
    fn a_stream_with_a_limit_advertises_the_room_it_has_left() {
        const LIMIT: usize = 65_536;
        let (mut inner, mut reader) = limited(LIMIT);
        assert_eq!(inner.read_limit(), Some(LIMIT as u32));

        // The window is the bytes left under the limit, less a full packet,
        // which is kept for the packet the reader awaits.
        let room = |held: usize| LIMIT - held - FULL_PACKET;
        assert_eq!(window(&mut inner), room(0));

        // What the reader has not taken counts, until it takes it.
        for seq in 0..10 {
            assert!(arrives(&mut inner, seq, PACKET));
        }
        assert_eq!(window(&mut inner), room(10 * PACKET));
        assert_eq!(reader_takes(&mut inner, &mut reader, 4), 4 * PACKET);
        assert_eq!(window(&mut inner), room(6 * PACKET));

        // So does what arrived out of order.
        assert!(arrives(&mut inner, 11, PACKET));
        assert_eq!(window(&mut inner), room(7 * PACKET));

        // A packet takes the bytes it carries.
        assert!(arrives(&mut inner, 12, 1));
        let left = room(7 * PACKET + 1);
        assert_eq!(window(&mut inner), left);

        // The packets with data that the stream sends say the same.
        queue(&mut inner, 1);
        let t0 = inner.epoch;
        let sent = send(&mut inner, t0);
        assert_eq!(data_seqs(&sent), [0]);
        assert!(sent.iter().all(|packet| Header::decode(packet).unwrap().recv_window as usize == left));

        // Without the limit, the window leaves out only what is held ahead
        // of a gap.
        inner.set_read_limit(None);
        assert_eq!(inner.read_limit(), None);
        assert_eq!(window(&mut inner), MAX_OUT_OF_ORDER - PACKET - 1);
    }

    #[test]
    fn a_limit_too_low_to_leave_a_window_is_raised() {
        let (mut inner, _reader) = limited(1);
        assert_eq!(inner.read_limit(), Some(MIN_READ_BUFFER_LIMIT));
        assert_eq!(window(&mut inner), MIN_READ_BUFFER_LIMIT as usize - FULL_PACKET);
    }

    #[test]
    fn room_for_less_than_a_full_packet_is_advertised_as_none() {
        const LIMIT: usize = 32_768;
        let (mut inner, _reader) = limited(LIMIT);

        // The window before each packet of 200 bytes, up to the one that is
        // turned away.
        let mut windows = Vec::new();
        let mut turned_away = false;
        for seq in 0..1_000 {
            windows.push(window(&mut inner));
            if !arrives(&mut inner, seq, 200) {
                turned_away = true;
                break;
            }
        }
        assert!(turned_away, "the stream took a thousand packets");
        let taken = windows.len() - 1;

        // C libudx asks about a window of zero, and waits for ever on one
        // that is smaller than a packet.
        assert!(windows.iter().all(|&window| window == 0 || window >= FULL_PACKET), "{windows:?}");
        // Each packet of 200 bytes takes 200 bytes of room.
        let open = windows.iter().take_while(|&&window| window > 0).count();
        assert_eq!(open, (LIMIT - 2 * FULL_PACKET) / 200 + 1);
        assert!(windows[open..].iter().all(|&window| window == 0));

        // The stream went on taking packets up to its limit all the same.
        assert_eq!(taken, LIMIT.div_ceil(200));
    }

    #[test]
    fn a_full_stream_turns_packets_away_until_its_reader_has_made_room() {
        const LIMIT: usize = 16_384;
        let (mut inner, mut reader) = limited(LIMIT);

        // The packet the reader awaits is taken while the stream holds less
        // than its limit, which takes it over by less than a packet.
        let mut taken = 0;
        while taken < 1_000 && arrives(&mut inner, taken, PACKET) {
            taken += 1;
        }
        let full = LIMIT.div_ceil(PACKET) as u32;
        assert_eq!(taken, full);
        assert_eq!(inner.held(), full as usize * PACKET);
        assert_eq!(inner.next_remote_seq, full);

        // The packet that was turned away is not acknowledged, and the
        // remote is told that there is no room.
        assert_eq!(inner.turned_away, 1);
        let (ack, _) = inner.take_ack().expect("the remote is answered");
        let header = Header::decode(&ack).unwrap();
        assert_eq!((header.ack, header.recv_window, header.type_flags), (full, 0, 0));

        // However often it comes.
        for _ in 0..100 {
            assert!(!arrives(&mut inner, full, PACKET));
        }
        assert_eq!(inner.held(), full as usize * PACKET);

        // Once the reader has taken a packet, the stream is under its limit.
        assert_eq!(reader_takes(&mut inner, &mut reader, 1), PACKET);
        assert!(arrives(&mut inner, full, PACKET));
        assert_eq!(inner.next_remote_seq, full + 1);
        assert_eq!(read(&mut reader).0.len(), full as usize * PACKET);
    }

    #[test]
    fn the_remote_is_told_at_once_of_a_packet_that_was_turned_away() {
        let limit = MIN_READ_BUFFER_LIMIT as usize;
        let (inner, _reader) = limited(limit);
        let inner = Mutex::new(inner);
        let take = |seq| {
            let (header, data) = incoming(FLAG_DATA, seq, 0, &[7u8; PACKET]);
            take_in(&inner, &header, &data).ack_now
        };

        // These packets take the stream over its limit.
        let full = limit.div_ceil(PACKET) as u32;
        for seq in 0..full {
            assert!(!take(seq), "in order, and there was room for it");
        }
        assert!(inner.lock().unwrap().held() >= limit);
        assert!(take(full), "there was no room for it");
        assert_eq!(inner.lock().unwrap().turned_away, 1);
    }

    #[test]
    fn packets_ahead_of_a_gap_leave_room_for_the_one_that_is_missing() {
        const LIMIT: usize = 16_384;
        let (mut inner, mut reader) = limited(LIMIT);

        // Packet 0 is missing. The ones after it are taken while the room
        // of a full packet is left over.
        let mut seq = 1;
        while seq < 1_000 && arrives(&mut inner, seq, PACKET) {
            seq += 1;
        }
        let ahead = seq as usize - 1;
        assert_eq!(ahead, (LIMIT - FULL_PACKET) / PACKET);
        assert_eq!(inner.held(), ahead * PACKET);

        // The remote is told of room for that one packet. With none, a
        // sender whose window is used up could not send it again.
        assert_eq!(window(&mut inner), FULL_PACKET);

        // What has been taken is kept, whatever else is turned away: the
        // remote was told of it, and no longer holds it.
        for seq in 100..200 {
            assert!(!arrives(&mut inner, seq, PACKET));
        }
        assert_eq!(inner.sack_ranges(), [SackRange { start: 1, end: seq }]);
        // One of them that comes again is one the stream has: it is not
        // turned away, and not counted twice.
        assert!(arrives(&mut inner, 5, PACKET));
        assert_eq!(inner.held(), ahead * PACKET);

        // The missing packet is taken, and all of it goes to the reader.
        assert!(arrives(&mut inner, 0, PACKET));
        assert!(inner.recv_buf.is_empty());
        assert_eq!(inner.next_remote_seq, seq);
        assert_eq!(read(&mut reader).0.len(), (ahead + 1) * PACKET);
    }

    #[test]
    fn a_remote_that_takes_no_notice_of_the_window_cannot_fill_a_stream_past_its_limit() {
        const LIMIT: usize = 65_536;

        // In order, to a reader that takes nothing, three times over.
        let (mut inner, mut reader) = limited(LIMIT);
        for _ in 0..3 {
            for seq in 0..5_000 {
                arrives(&mut inner, seq, MAX_PAYLOAD);
            }
        }
        let held = inner.held();
        assert!(held <= LIMIT + FULL_PACKET, "it holds {held} bytes");
        assert!(inner.turned_away > 10_000);
        assert_eq!(read(&mut reader).0.len(), held);

        // Ahead of a gap.
        let (mut inner, _reader) = limited(LIMIT);
        for seq in 1..5_000 {
            arrives(&mut inner, seq, MAX_PAYLOAD);
        }
        assert!(inner.held() <= LIMIT, "it holds {} bytes", inner.held());
        assert_eq!(inner.recv_buf.len() * MAX_PAYLOAD, inner.held());

        // A byte in each packet: a byte held for each, up to the limit.
        let (mut inner, mut reader) = limited(LIMIT);
        for seq in 0..100_000 {
            arrives(&mut inner, seq, 1);
        }
        assert_eq!(read(&mut reader).0.len(), LIMIT);

        // Ahead of a gap, and with nothing in them at all, each also takes
        // an entry in the index: no more of them than the limit has room
        // for at `OUT_OF_ORDER_FRAME_SIZE` each.
        for len in [1, 0] {
            let (mut inner, _reader) = limited(LIMIT);
            for seq in 1..100_000 {
                arrives(&mut inner, seq, len);
            }
            assert_eq!(inner.recv_buf.len(), LIMIT / OUT_OF_ORDER_FRAME_SIZE);
        }
    }

    #[test]
    fn a_packet_larger_than_any_stream_sends_has_to_fit_under_the_limit() {
        const LIMIT: usize = 16_384;
        const LARGE: usize = 60_000;

        // The packet the reader awaits is taken while the stream holds less
        // than its limit: a full packet, and no more than that.
        let (mut inner, _reader) = limited(LIMIT);
        assert!(!arrives(&mut inner, 0, LARGE), "it is larger than the limit");
        // As many as leave less room under the limit than a full packet
        // and a byte.
        let under = ((LIMIT - FULL_PACKET) / PACKET + 1) as u32;
        for seq in 0..under {
            assert!(arrives(&mut inner, seq, PACKET));
        }
        assert!(LIMIT - under as usize * PACKET < FULL_PACKET + 1);
        assert!(!arrives(&mut inner, under, FULL_PACKET + 1), "it does not fit under the limit");
        assert!(arrives(&mut inner, under, FULL_PACKET), "a full packet is taken under the limit");
        assert_eq!(inner.held(), under as usize * PACKET + FULL_PACKET);

        // Where it fits, it is taken like any other.
        let (mut inner, mut reader) = limited(4 * LARGE);
        assert!(arrives(&mut inner, 0, LARGE));
        assert_eq!(read(&mut reader).0.len(), LARGE);

        // A remote that sends nothing else gets a stream no further than
        // one that keeps to full packets.
        let (mut inner, _reader) = limited(LIMIT);
        for seq in 0..200 {
            arrives(&mut inner, seq, LARGE);
            arrives(&mut inner, seq, 9_000);
        }
        assert!(inner.held() <= LIMIT + FULL_PACKET, "it holds {} bytes", inner.held());
    }

    #[test]
    fn the_end_of_a_stream_is_handed_over_once_and_nothing_after_it_is_taken() {
        /// A packet of the kind given arrives, numbered `seq`, with three
        /// bytes in it if it carries data.
        fn arrives_as(inner: &mut StreamInner, type_flags: u8, seq: u32) {
            let payload: &[u8] = if type_flags & FLAG_DATA != 0 { b"abc" } else { &[] };
            let (header, data) = incoming(type_flags, seq, 0, payload);
            inner.receive_packet(&header, &data);
        }

        /// Everything the reader has been handed: the bytes of each packet,
        /// and `None` for the end of the stream.
        fn handed(reader: &mut RecvReader) -> Vec<Option<Vec<u8>>> {
            let mut events = Vec::new();
            while let Ok(event) = reader.try_recv() {
                events.push(match event {
                    Frame::Data(data) => Some(data),
                    Frame::End => None,
                });
            }
            events
        }

        let abc = || Some(b"abc".to_vec());

        // Without a limit and with one.
        for limit in [None, Some(MIN_READ_BUFFER_LIMIT)] {
            let (mut inner, mut reader) = with_reader();
            inner.set_read_limit(limit);

            arrives_as(&mut inner, FLAG_DATA, 0);
            arrives_as(&mut inner, FLAG_END, 1);
            assert_eq!(handed(&mut reader), [abc(), None]);

            // More ends, more data, and data ahead of a gap: 100,000 packets.
            for seq in 2..50_002 {
                let kind = [FLAG_END, FLAG_DATA, FLAG_DATA | FLAG_END][seq as usize % 3];
                arrives_as(&mut inner, kind, seq);
                arrives_as(&mut inner, FLAG_DATA, 100_000 + seq);
            }
            assert!(handed(&mut reader).is_empty(), "the reader was handed more after the end");
            assert_eq!(inner.next_remote_seq, 2);
            assert!(inner.recv_buf.is_empty());
            assert_eq!(inner.recv_buf_held, 0);

            // An end that comes again is acknowledged again: the remote may
            // not have heard.
            inner.take_ack();
            arrives_as(&mut inner, FLAG_END, 1);
            let (ack, _) = inner.take_ack().expect("the end is acknowledged again");
            assert_eq!(Header::decode(&ack).unwrap().ack, 2);
        }

        // An end that arrived ahead of a gap ends the stream when the gap
        // closes, and what was numbered after it is let go.
        let (mut inner, mut reader) = limited(MIN_READ_BUFFER_LIMIT as usize);
        arrives_as(&mut inner, FLAG_DATA | FLAG_END, 2);
        arrives_as(&mut inner, FLAG_DATA, 3);
        arrives_as(&mut inner, FLAG_END, 4);
        assert_eq!(inner.recv_buf.len(), 3);
        arrives_as(&mut inner, FLAG_DATA, 0);
        arrives_as(&mut inner, FLAG_DATA, 1);
        assert_eq!(handed(&mut reader), [abc(), abc(), abc(), None]);
        assert_eq!(inner.next_remote_seq, 3);
        assert!(inner.recv_buf.is_empty());
        assert_eq!(inner.recv_buf_held, 0);
    }

    #[test]
    fn a_stream_whose_reader_is_gone_holds_nothing_for_it() {
        const LIMIT: usize = 16_384;
        let (mut inner, reader) = limited(LIMIT);
        let mut taken = 0;
        while taken < 1_000 && arrives(&mut inner, taken, PACKET) {
            taken += 1;
        }
        assert_eq!(taken, LIMIT.div_ceil(PACKET) as u32);
        assert_eq!(window(&mut inner), 0);

        // What it was handed went with the reader. What arrives now is
        // acknowledged and dropped, as on a stream without a limit.
        drop(reader);
        assert_eq!(inner.held(), 0);
        assert_eq!(window(&mut inner), LIMIT - FULL_PACKET);
        for seq in taken..1_000 {
            assert!(arrives(&mut inner, seq, PACKET));
        }
        assert_eq!(inner.next_remote_seq, 1_000);
        assert_eq!(inner.held(), 0);

        // What arrives ahead of a gap is held as before, within the limit.
        for seq in 1_001..2_000 {
            arrives(&mut inner, seq, PACKET);
        }
        assert!(inner.held() <= LIMIT, "it holds {} bytes", inner.held());
        assert_eq!(inner.held(), inner.recv_buf.len() * PACKET);
        assert_eq!(inner.recv_buf.len(), (LIMIT - FULL_PACKET) / PACKET);
    }

    #[test]
    fn a_reader_that_makes_room_has_the_remote_told_once_there_is_enough_to_tell_of() {
        const LIMIT: usize = 65_536;
        let (mut inner, mut reader) = limited(LIMIT);
        let (notify_tx, mut notified) = mpsc::unbounded_channel();
        inner.notify_tx = Some(notify_tx);

        // Filled until it advertises no room, and the remote has been told.
        let mut seq = 0;
        while seq < 1_000 && window(&mut inner) > 0 {
            assert!(arrives(&mut inner, seq, PACKET));
            seq += 1;
        }
        assert!(inner.take_ack().is_some());
        assert!(notified.try_recv().is_err());

        // The reader takes a packet at a time. The remote hears of it once
        // the window has grown by a quarter of the widest it can be.
        let step = LIMIT / 4;
        let mut freed = 0;
        while !inner.ack_wanted && freed < PACKET * seq as usize {
            assert!(notified.try_recv().is_err());
            freed += reader_takes(&mut inner, &mut reader, 1);
        }
        let room = inner.recv_window() as usize;
        assert!(room >= step && room - PACKET < step, "{room} bytes of room");
        assert_eq!(freed % PACKET, 0);
        assert!(matches!(notified.try_recv(), Ok(StreamNotify::DataQueued)));

        // It is told in an acknowledgement of what it knows was received.
        let (ack, _) = inner.take_ack().expect("the remote is told");
        let header = Header::decode(&ack).unwrap();
        assert_eq!((header.ack, header.recv_window as usize), (seq, room));

        // And not again until as much room again has been made.
        inner.notify_pending = false;
        for _ in 0..step.div_ceil(PACKET) - 1 {
            reader_takes(&mut inner, &mut reader, 1);
            assert!(!inner.ack_wanted);
        }
        reader_takes(&mut inner, &mut reader, 1);
        assert!(inner.ack_wanted);

        // A stream without a limit tells nobody.
        let (mut inner, mut reader) = with_reader();
        for seq in 0..5_000 {
            arrives(&mut inner, seq, MAX_PAYLOAD);
        }
        inner.take_ack();
        while reader_takes(&mut inner, &mut reader, 100) > 0 {}
        assert!(!inner.ack_wanted);
    }

    #[test]
    fn an_acknowledgement_carries_the_number_of_the_streams_next_packet() {
        let (mut inner, _reader) = with_reader();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);

        // C libudx goes by it to tell which of two packets is the newer,
        // and takes the window of that one.
        assert!(arrives(&mut inner, 0, 10));
        let (ack, _) = inner.take_ack().expect("an acknowledgement is owed");
        let header = Header::decode(&ack).unwrap();
        assert_eq!((header.seq, header.ack), (3, 1));
    }

    #[tokio::test]
    async fn a_window_is_taken_from_the_newest_packet_only() {
        let mut inner = connected();
        queue(&mut inner, 3);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        let inner = Arc::new(Mutex::new(inner));
        let window = || inner.lock().unwrap().send_rwnd;

        assert!(handle_incoming(&inner, says(5, 1, 50_000)).await);
        assert_eq!(window(), 50_000);

        // One that the remote sent before that one, and that arrives after.
        assert!(handle_incoming(&inner, says(4, 1, 40_000)).await);
        assert_eq!(window(), 50_000);
        // One that acknowledges less than has been acknowledged.
        assert!(handle_incoming(&inner, says(6, 0, 30_000)).await);
        assert_eq!(window(), 50_000);
        // One that acknowledges what was never sent.
        assert!(handle_incoming(&inner, says(6, 9, 30_000)).await);
        assert_eq!(window(), 50_000);

        // The same number again is no older, and a higher one is newer.
        assert!(handle_incoming(&inner, says(5, 1, 20_000)).await);
        assert_eq!(window(), 20_000);
        assert!(handle_incoming(&inner, says(6, 2, 60_000)).await);
        assert_eq!(window(), 60_000);

        // Across the wrap of the remote's sequence numbers as well.
        inner.lock().unwrap().send_wl1 = u32::MAX;
        assert!(handle_incoming(&inner, says(u32::MAX - 1, 2, 10_000)).await);
        assert_eq!(window(), 60_000);
        assert!(handle_incoming(&inner, says(1, 2, 10_000)).await);
        assert_eq!(window(), 10_000);
    }

    #[test]
    fn a_sender_asks_about_a_window_that_has_no_room_for_as_long_as_it_is_answered() {
        let mut inner = connected();
        let t0 = inner.epoch;
        hears(&mut inner, 0, 0, t0);

        // A write to a stream with nothing on its way asks at once, with a
        // packet of what was written, and with no more than that.
        queue(&mut inner, 20);
        assert_eq!(data_seqs(&send(&mut inner, t0)), [0]);
        queue(&mut inner, 5);
        assert!(send(&mut inner, t0 + millis(1)).is_empty());

        // The remote turns the packet away, and says that it has no room.
        // The same packet asks again, and again, for two minutes.
        let mut now = t0 + millis(20);
        hears(&mut inner, 0, 0, now);
        let mut asked = 0;
        while now < t0 + millis(120_000) {
            now += millis(20);
            assert!(!inner.on_timers(now), "the stream gave up on a remote that answers");
            let sent = send(&mut inner, now);
            if !sent.is_empty() {
                assert_eq!(data_seqs(&sent), [0], "the packet that was turned away asks");
                hears(&mut inner, 0, 0, now);
                asked += 1;
            }
        }
        assert!((55..=65).contains(&asked), "it asked {asked} times");
        assert_eq!(inner.outgoing.len(), 1, "nothing else was sent");
        assert_eq!(inner.outgoing[&0].rto_timeouts, 0, "a packet turned away is not one lost");
        assert_eq!(inner.congestion.ca_state, CaState::Open, "nor is the path congested");

        // A remote that stops answering is given up on, as any is.
        let silent_since = now;
        let gave_up = loop {
            now += millis(20);
            if inner.on_timers(now) {
                break now;
            }
            send(&mut inner, now);
            assert!(now < silent_since + millis(60_000), "the stream never gave up");
        };
        let waited = gave_up.duration_since(silent_since);
        assert!(waited > millis(10_000) && waited < millis(20_000), "it gave up after {waited:?}");
    }

    #[test]
    fn a_sender_goes_on_once_its_remote_has_room_again() {
        let mut inner = connected();
        let t0 = inner.epoch;
        hears(&mut inner, 0, 0, t0);
        queue(&mut inner, 6);
        assert_eq!(data_seqs(&send(&mut inner, t0)), [0]);
        hears(&mut inner, 0, 0, t0 + millis(20));
        assert!(inner.zwp_deadline.is_some());

        // The packet that was turned away has timed out, and waits.
        let now = t0 + millis(1_000);
        assert!(!inner.on_timers(now));
        assert!(send(&mut inner, now).is_empty());

        // Room: it goes out first, and what was written after it.
        hears(&mut inner, 0, DEFAULT_RWND, now + millis(5));
        assert!(inner.zwp_deadline.is_none());
        assert_eq!(data_seqs(&send(&mut inner, now + millis(5))), [0, 1, 2, 3, 4, 5]);
    }

    /// The stream hears from its remote, at `now`: it acknowledges `ack`,
    /// reports `ranges` past it and has room for `window` bytes.
    fn hears_of(inner: &mut StreamInner, ack: u32, ranges: &[SackRange], window: u32, now: Instant) {
        let (mut header, _) = incoming(0, inner.send_wl1, ack, &[]);
        header.recv_window = window;
        inner.take_window(&header, ranges, now);
    }

    #[test]
    fn an_older_acknowledgement_that_arrives_late_does_not_widen_the_window() {
        let mut inner = connected();
        let t0 = inner.epoch;
        queue(&mut inner, 10);
        send(&mut inner, t0);
        let past = |end| [SackRange { start: 1, end }];

        // Packet 0 is missing. The later acknowledgement, with less room as
        // more is held past the gap, overtook the earlier one.
        hears_of(&mut inner, 0, &past(8), 50_000, t0 + millis(10));
        hears_of(&mut inner, 0, &past(4), 60_000, t0 + millis(11));
        assert_eq!(inner.send_rwnd, 50_000);

        // The remote's reader made room, with nothing more past the gap.
        hears_of(&mut inner, 0, &past(8), 70_000, t0 + millis(12));
        assert_eq!(inner.send_rwnd, 70_000);
        // And what the remote holds past the gap leaves less.
        hears_of(&mut inner, 0, &past(4), 40_000, t0 + millis(13));
        assert_eq!(inner.send_rwnd, 40_000);
    }

    #[test]
    fn a_closed_window_opens_on_any_acknowledgement_that_says_there_is_room() {
        let mut inner = connected();
        let t0 = inner.epoch;
        queue(&mut inner, 10);
        send(&mut inner, t0);
        let past = |end| [SackRange { start: 1, end }];

        // Reporting less than before, as a remote that reports a number of
        // ranges at most may do: the stream waits on none to say more.
        hears_of(&mut inner, 0, &past(8), 0, t0 + millis(10));
        hears_of(&mut inner, 0, &past(4), 60_000, t0 + millis(11));
        assert_eq!(inner.send_rwnd, 60_000);
    }

    #[test]
    fn a_probe_that_is_taken_is_followed_by_one_of_new_data() {
        // A remote on C libudx takes whatever arrives, and says what room it
        // has in the acknowledgement.
        let mut inner = connected();
        let t0 = inner.epoch;
        hears(&mut inner, 0, 0, t0);
        queue(&mut inner, 5);
        assert_eq!(data_seqs(&send(&mut inner, t0)), [0]);

        // Taken, and still no room.
        let heard = t0 + millis(20);
        hears(&mut inner, 1, 0, heard);
        assert!(inner.outgoing.is_empty());

        // The stream asks again a timeout later, and not before.
        let due = heard + millis(u64::from(INITIAL_RTO_MS));
        assert!(!inner.on_timers(due - millis(1)));
        assert!(send(&mut inner, due - millis(1)).is_empty());
        assert!(!inner.on_timers(due));
        assert_eq!(data_seqs(&send(&mut inner, due)), [1]);

        // Taken as well, and with room now.
        hears(&mut inner, 2, DEFAULT_RWND, due + millis(20));
        assert_eq!(data_seqs(&send(&mut inner, due + millis(20))), [2, 3, 4]);
    }

    #[test]
    fn a_window_smaller_than_a_packet_is_asked_about_like_one_of_zero() {
        let mut inner = connected();
        let t0 = inner.epoch;
        queue(&mut inner, 3);

        // No packet fits. C libudx waits here until its remote says more,
        // which a remote with nothing to say does not.
        hears(&mut inner, 0, MAX_PAYLOAD as u32 - 1, t0);
        assert!(send(&mut inner, t0).is_empty());
        let due = t0 + millis(u64::from(INITIAL_RTO_MS));
        assert!(!inner.on_timers(due - millis(1)));
        assert!(send(&mut inner, due - millis(1)).is_empty());
        assert!(!inner.on_timers(due));
        assert_eq!(data_seqs(&send(&mut inner, due)), [0]);

        // Room for a whole packet is room.
        let mut inner = connected();
        let t0 = inner.epoch;
        queue(&mut inner, 3);
        hears(&mut inner, 0, MAX_PAYLOAD as u32, t0);
        assert!(inner.zwp_deadline.is_none());
        assert_eq!(data_seqs(&send(&mut inner, t0)), [0]);
    }

    /// How a transfer to a reader went.
    #[derive(Debug, PartialEq)]
    struct Flow {
        /// Whether the reader was handed all that was written, as written.
        intact: bool,
        /// How long the transfer took, in milliseconds.
        took: u64,
        /// When the sender gave up on its remote, if it did.
        gave_up: Option<u64>,
        /// The most the receiving stream held for its reader at one time.
        most_held: usize,
        /// How many packets it turned away.
        turned_away: u64,
        /// The most it held ahead of a gap, in bytes as packets count.
        most_ahead: usize,
        /// The most packets the sender had in flight at one time.
        most_in_flight: u32,
        /// When each packet with data left the sender.
        sent_at: Vec<u64>,
    }

    /// A path that loses nothing and takes ten milliseconds.
    fn clean(_nth: u64, _ms: u64) -> Option<u64> {
        Some(10)
    }

    /// Send `packets` full packets from one stream to another, whose reader
    /// takes `reads(ms)` of them in millisecond `ms`. Both streams number
    /// from `first`. The receiving stream holds no more than `limit` for
    /// its reader, if that is one. `path` says what becomes of each packet,
    /// as [`fate`] does. A sender that does not heed the window takes its
    /// remote to have room whatever it says.
    fn transfer_to_a_reader(
        first: u32,
        packets: usize,
        limit: Option<usize>,
        path: fn(u64, u64) -> Option<u64>,
        heeds_the_window: bool,
        reads: impl Fn(u64) -> usize,
    ) -> Flow {
        transfer_at_mtu(MTU_BASE, first, packets, limit, path, heeds_the_window, reads)
    }

    /// As [`transfer_to_a_reader`], from a sender whose MTU is `mtu`.
    fn transfer_at_mtu(
        mtu: usize,
        first: u32,
        packets: usize,
        limit: Option<usize>,
        path: fn(u64, u64) -> Option<u64>,
        heeds_the_window: bool,
        reads: impl Fn(u64) -> usize,
    ) -> Flow {
        let (mut sender, _) = starting_at(first);
        sender.mtu = mtu;
        sender.mtu_max = mtu;
        let payload = sender.max_payload();
        sender.congestion.on_mtu_change(payload);
        let (mut receiver, mut reader) = starting_at(first);
        receiver.set_read_limit(limit.map(|limit| limit as u32));
        let (tx, mut written) = oneshot::channel();
        let data: Vec<u8> = (0..packets * MAX_PAYLOAD).map(|i| (i % 251) as u8).collect();
        sender.push_write(QueuedWrite { data: data.clone(), taken: 0, end: false, tx: Some(tx) });

        let t0 = sender.epoch;
        let mut flow = Flow {
            intact: false,
            took: 0,
            gave_up: None,
            most_held: 0,
            turned_away: 0,
            most_ahead: 0,
            most_in_flight: 0,
            sent_at: Vec::new(),
        };
        let mut received = Vec::new();
        // What is on the path: when it arrives, the order it was sent in,
        // whether it is on its way to the receiver, and the packet.
        let mut on_path: Vec<(u64, u64, bool, Vec<u8>)> = Vec::new();
        let mut nth = 0;
        let mut complete = false;

        for ms in 0..300_000 {
            let now = t0 + millis(ms);
            flow.took = ms;

            let (mut due, later): (Vec<_>, Vec<_>) =
                std::mem::take(&mut on_path).into_iter().partition(|packet| packet.0 <= ms);
            on_path = later;
            due.sort_by_key(|packet| (packet.0, packet.1));
            for (_, _, to_receiver, packet) in due {
                let header = Header::decode(&packet).unwrap();
                if to_receiver {
                    receiver.receive_packet_at(&header, &packet, now);
                    flow.most_held = flow.most_held.max(receiver.held());
                    flow.most_ahead = flow.most_ahead.max(receiver.recv_buf_held);
                } else {
                    let ranges = sack_ranges_in(&header, &packet);
                    if heeds_the_window {
                        sender.take_window(&header, &ranges, now);
                    }
                    for tx in sender.process_ack(header.ack, &ranges, now).into_iter().flatten() {
                        let _ = tx.send(Ok(()));
                    }
                }
            }

            for _ in 0..reads(ms) {
                let Ok(Frame::Data(bytes)) = reader.try_recv() else {
                    break;
                };
                if reader.limited() {
                    receiver.reader_took();
                }
                received.extend(bytes);
            }
            complete |= matches!(written.try_recv(), Ok(Ok(())));
            if complete && received.len() == data.len() {
                break;
            }

            receiver.on_timers(now);
            if let Some((ack, _)) = receiver.take_ack_at(now) {
                if let Some(takes) = path(nth, ms) {
                    on_path.push((ms + takes, nth, false, ack));
                }
                nth += 1;
            }

            if sender.on_timers(now) {
                flow.gave_up = Some(ms);
                break;
            }
            for packet in send(&mut sender, now) {
                flow.sent_at.push(ms);
                if let Some(takes) = path(nth, ms) {
                    on_path.push((ms + takes, nth, true, packet));
                }
                nth += 1;
            }
            flow.most_in_flight = flow.most_in_flight.max(sender.inflight());
        }

        flow.intact = received == data;
        flow.turned_away = receiver.turned_away;
        flow
    }

    #[test]
    fn a_slow_reader_holds_its_remote_back_and_loses_nothing() {
        const LIMIT: usize = 65_536;
        // The reader takes a packet every other millisecond, 576 KB a second,
        // from a path that carries far more.
        let reads = |ms| usize::from(ms % 2 == 0);
        let flow = transfer_to_a_reader(0, 1_000, Some(LIMIT), clean, true, reads);

        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);
        assert!(flow.most_held <= LIMIT + MAX_PAYLOAD, "it held {} bytes", flow.most_held);
        assert!(flow.most_held > LIMIT / 2, "it held {} bytes", flow.most_held);
        assert_eq!(flow.turned_away, 0, "a remote that keeps to the window is not turned away");
        // The reader sets the pace: a thousand packets in two seconds.
        assert!((2_000..2_100).contains(&flow.took), "it took {} ms", flow.took);
        // And the sender sent each packet once.
        assert_eq!(flow.sent_at.len(), 1_000);
    }

    #[test]
    fn a_reader_that_keeps_up_is_not_slowed_down() {
        let all = |_| usize::MAX;
        let without = transfer_to_a_reader(0, 3_000, None, clean, true, all);
        assert!(without.intact);

        // A limit that leaves at least the window every stream advertises:
        // nothing goes differently at all.
        let as_wide = MAX_OUT_OF_ORDER + FULL_PACKET;
        assert_eq!(transfer_to_a_reader(0, 3_000, Some(as_wide), clean, true, all), without);

        // A limit is also the most a remote has on its way. So a low one
        // does slow a transfer that a path with a long round trip could
        // carry faster: here to 55 packets every 20 ms.
        const LIMIT: usize = 65_536;
        let low = transfer_to_a_reader(0, 3_000, Some(LIMIT), clean, true, all);
        assert!(low.intact);
        assert_eq!(low.turned_away, 0);
        assert_eq!(low.most_in_flight as usize, (LIMIT - FULL_PACKET) / MAX_PAYLOAD);
        assert!(without.most_in_flight > 2 * low.most_in_flight);
        assert!(low.took > 2 * without.took, "{} ms, and {} ms without", low.took, without.took);
    }

    #[test]
    fn a_reader_that_stops_for_a_minute_does_not_lose_the_stream() {
        const LIMIT: usize = 65_536;
        let reads = |ms| if (500..60_500).contains(&ms) { 0 } else { 2 };
        let flow = transfer_to_a_reader(0, 2_000, Some(LIMIT), clean, true, reads);

        assert_eq!(flow.gave_up, None);
        assert!(flow.intact);
        assert!(flow.most_held <= LIMIT + MAX_PAYLOAD, "it held {} bytes", flow.most_held);
        // The minute, and what the reader takes for the rest.
        assert!((61_000..61_400).contains(&flow.took), "it took {} ms", flow.took);

        // While the reader stood still, the sender asked for room and sent
        // nothing else: a packet every second or two.
        let asked = flow.sent_at.iter().filter(|&&ms| (2_000..60_000).contains(&ms)).count();
        assert!((25..=65).contains(&asked), "{asked} packets went out");
    }

    #[test]
    fn a_transfer_to_a_slow_reader_gets_through_a_poor_path_wherever_the_wrap_falls() {
        const LIMIT: usize = 65_536;
        let flow = transfer_to_a_reader(0, 1_200, Some(LIMIT), fate, true, |_| 1);

        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);
        assert!(flow.most_held <= LIMIT + MAX_PAYLOAD, "it held {} bytes", flow.most_held);
        assert!(flow.most_held > LIMIT / 2, "it held {} bytes", flow.most_held);

        for left in [1, 9, 37, 150, 600, 1_199] {
            let first = 0u32.wrapping_sub(left);
            let wrapped = transfer_to_a_reader(first, 1_200, Some(LIMIT), fate, true, |_| 1);
            assert!(wrapped == flow, "with {left} packets to go before the wrap");
        }
    }

    #[test]
    fn a_remote_that_takes_no_notice_of_the_window_is_held_to_the_limit_in_a_transfer() {
        const LIMIT: usize = 65_536;
        let flow = transfer_to_a_reader(0, 2_000, Some(LIMIT), clean, false, |_| 1);
        assert!(flow.most_held <= LIMIT + MAX_PAYLOAD, "it held {} bytes", flow.most_held);
        assert!(flow.turned_away > 1_000);
        // What it sends again gets through as the reader makes room.
        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);

        // The same remote has a stream without a limit hold most of it.
        let unlimited = transfer_to_a_reader(0, 2_000, None, clean, false, |_| 1);
        assert!(unlimited.intact);
        assert!(unlimited.most_held > 30 * LIMIT, "it held {} bytes", unlimited.most_held);

        // To a reader that takes nothing, it gets nowhere, and gives up as
        // it does on a path that loses its packets.
        let stopped = transfer_to_a_reader(0, 2_000, Some(LIMIT), clean, false, |_| 0);
        assert!(stopped.most_held <= LIMIT + MAX_PAYLOAD, "it held {} bytes", stopped.most_held);
        assert!(stopped.gave_up.is_some_and(|ms| ms < 20_000));
    }

    // ── The window in bytes, whatever the sender's packets ──────────────────
    //
    // A sender counts its window in packets of its own size: C libudx as
    // `send_rwnd / max_payload`, this crate as `send_rwnd / mss`. The window
    // is the room a stream has, in bytes, so a sender of any size that keeps
    // to it fits.

    #[test]
    fn a_sender_on_a_link_smaller_than_the_starting_mtu_that_keeps_to_the_window_is_not_turned_away() {
        const LIMIT: usize = 65_536;
        // A packet of 952 bytes of payload: a link of 1,000 bytes.
        let reads = |ms| usize::from(ms % 2 == 0);
        let flow = transfer_at_mtu(1_000, 0, 1_000, Some(LIMIT), clean, true, reads);

        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);
        assert_eq!(flow.turned_away, 0, "a remote that keeps to the window is not turned away");
        assert!(flow.most_held <= LIMIT + FULL_PACKET, "it held {} bytes", flow.most_held);
    }

    #[test]
    fn a_sender_on_a_lossy_link_smaller_than_the_starting_mtu_is_not_turned_away_without_a_limit() {
        let all = |_| usize::MAX;
        let flow = transfer_at_mtu(1_000, 0, 20_000, None, long_and_lossy, true, all);
        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);
        assert_eq!(flow.turned_away, 0);
    }

    #[test]
    fn a_sender_of_the_largest_packets_can_fill_the_limit() {
        const LIMIT: usize = 65_536;
        let all = |_| usize::MAX;
        let flow = transfer_at_mtu(MTU_MAX, 0, 3_000, Some(LIMIT), clean, true, all);
        assert!(flow.intact);
        assert_eq!(flow.turned_away, 0);
        // All but the packet kept for the one awaited is the sender's to fill.
        let payload = MTU_MAX - IPV4_UDP_OVERHEAD - HEADER_SIZE;
        let room = LIMIT - FULL_PACKET;
        assert_eq!(flow.most_in_flight as usize, room / payload, "packets of {payload} bytes in {room}");
    }

    #[test]
    fn a_stream_with_a_limit_advertises_the_bytes_it_has_room_for() {
        const LIMIT: usize = 65_536;
        let (mut inner, _reader) = limited(LIMIT);
        assert_eq!(window(&mut inner), LIMIT - FULL_PACKET);
        for seq in 0..10 {
            assert!(arrives(&mut inner, seq, PACKET));
        }
        assert_eq!(window(&mut inner), LIMIT - 10 * PACKET - FULL_PACKET);
        // A byte in each packet takes a byte of room.
        for seq in 10..20 {
            assert!(arrives(&mut inner, seq, 1));
        }
        assert_eq!(window(&mut inner), LIMIT - 10 * PACKET - 10 - FULL_PACKET);
    }

    // ── Paths that reorder packets ──────────────────────────────────────────
    //
    // A packet that arrives behind one sent after it may only be late. A
    // sender that takes it for lost sends it again for nothing, and shrinks
    // its window for a congestion that is not there.

    /// Mixes `nth` into a number with no pattern, for paths that treat each
    /// packet differently.
    fn mix(nth: u64) -> u64 {
        let mut x = (nth + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 31;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^ (x >> 29)
    }

    /// A path that loses nothing and takes 1 to 3 ms with each packet, so
    /// that packets often pass one another: a round trip of 2 to 6 ms.
    fn reorders(nth: u64, _ms: u64) -> Option<u64> {
        Some(1 + mix(nth) % 3)
    }

    /// The same path, without passing: 2 ms each way.
    fn in_order(_nth: u64, _ms: u64) -> Option<u64> {
        Some(2)
    }

    #[test]
    fn a_path_that_reorders_but_loses_nothing_has_nothing_sent_again() {
        const PACKETS: usize = 5_000;
        let all = |_| usize::MAX;
        let steady = transfer_to_a_reader(0, PACKETS, None, in_order, true, all);
        let reordered = transfer_to_a_reader(0, PACKETS, None, reorders, true, all);
        assert!(steady.intact && reordered.intact);
        assert_eq!(reordered.gave_up, None);

        let resent = reordered.sent_at.len() - PACKETS;
        assert!(resent <= PACKETS / 100, "{resent} packets went out again on a path that lost none");
        assert!(
            reordered.took <= 2 * steady.took,
            "{} ms, where the same path without the passing took {} ms",
            reordered.took,
            steady.took
        );
    }

    /// A connected stream that has measured one round trip of `rtt_us`, and
    /// has seen its path reorder packets.
    fn measured(rtt_us: u64) -> StreamInner {
        let mut inner = connected();
        queue(&mut inner, 1);
        let t0 = inner.epoch;
        send(&mut inner, t0);
        inner.process_ack(1, &[], t0 + Duration::from_micros(rtt_us));
        inner.reordering_seen = true;
        inner
    }

    /// The stream sends `count` packets at `at`.
    fn sends(inner: &mut StreamInner, count: u32, at: Instant) {
        queue(inner, count);
        send(inner, at);
    }

    #[test]
    fn the_reordering_window_is_reckoned_in_microseconds() {
        // A quarter of the lowest round trip: 900 microseconds, where whole
        // milliseconds made it none.
        assert_eq!(measured(3_600).rack_reo_wnd(), Duration::from_micros(900));
    }

    /// The stream sends packet `seq` again at `at`, as loss detection does
    /// with one it takes for lost.
    fn resends(inner: &mut StreamInner, seq: u32, at: Instant) {
        assert!(!inner.mark_lost(seq));
        assert_eq!(data_seqs(&send(inner, at)), [seq]);
    }

    #[test]
    fn a_packet_the_remote_reports_twice_widens_the_reordering_window_once_a_round_trip() {
        let mut inner = measured(3_600);
        let rtt = Duration::from_micros(3_600);
        let twice = |seq: u32| [SackRange { start: seq, end: seq + 1 }];
        let t1 = inner.epoch + millis(10);

        // Packets 1, 2 and 3 go out, and 1 and 2 go out again, taken for
        // lost. The acknowledgement of 1 also reports, below it, that 1 had
        // arrived twice: it was only late.
        sends(&mut inner, 3, t1);
        resends(&mut inner, 1, t1);
        resends(&mut inner, 2, t1);
        inner.process_ack(2, &twice(1), t1 + rtt);
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));

        // Again in the same round trip: no wider.
        inner.process_ack(3, &twice(2), t1 + rtt);
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));

        // Nor for a packet that loss detection did not send again: one the
        // retransmission timer or a tail loss probe sent again says nothing
        // of reordering.
        inner.process_ack(4, &[], t1 + rtt);
        inner.process_ack(4, &twice(3), t1 + rtt);
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));

        // Each next round trip widens it by a quarter of the round trip, up
        // to the smoothed round trip.
        let mut at = t1 + rtt;
        for expected in [2_700, 3_600, 3_600] {
            inner.process_ack(inner.next_seq, &[], at);
            let seq = inner.next_seq;
            sends(&mut inner, 1, at);
            resends(&mut inner, seq, at);
            at += rtt;
            inner.process_ack(seq + 1, &twice(seq), at);
            assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(expected));
        }
    }

    #[test]
    fn a_packet_sent_again_for_nothing_shows_the_path_reorders() {
        // A packet taken for lost and sent again cannot show reordering by
        // being acknowledged below the highest one (RFC 8985, 6.2 step 3):
        // which of the two arrived is not known. That only the first was
        // needed is.
        let twice = |seq: u32| [SackRange { start: seq, end: seq + 1 }];
        for remote_reports_it in [true, false] {
            let mut inner = measured(3_600);
            inner.reordering_seen = false;
            let t1 = inner.epoch + millis(10);
            sends(&mut inner, 2, t1);
            resends(&mut inner, 1, t1 + millis(1));
            let ranges: &[SackRange] = if remote_reports_it { &twice(1) } else { &[] };
            inner.process_ack(3, ranges, t1 + Duration::from_micros(1_500));
            assert!(inner.reordering_seen, "reported twice: {remote_reports_it}");
        }
    }

    #[test]
    fn an_acknowledgement_sooner_than_a_round_trip_after_a_resend_widens_the_reordering_window() {
        // A remote that does not report what arrived twice, as C libudx.
        let mut inner = measured(3_600);
        let t1 = inner.epoch + millis(10);
        sends(&mut inner, 1, t1);
        resends(&mut inner, 1, t1 + millis(1));

        // Half a millisecond after it went out again: the first one arrived.
        inner.process_ack(2, &[], t1 + Duration::from_micros(1_500));
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));

        // A round trip after it went out again, a little quicker than the
        // lowest one measured so far: the second one arrived, on a path whose
        // round trips vary. No wider.
        let t2 = t1 + millis(20);
        sends(&mut inner, 1, t2);
        resends(&mut inner, 2, t2 + millis(4));
        inner.process_ack(3, &[], t2 + millis(4) + Duration::from_micros(3_300));
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));
    }

    #[test]
    fn the_reordering_window_narrows_again_after_sixteen_recoveries_without_a_packet_sent_for_nothing() {
        let mut inner = measured(3_600);
        let t1 = inner.epoch + millis(10);
        sends(&mut inner, 2, t1);
        resends(&mut inner, 1, t1);
        inner.process_ack(2, &[SackRange { start: 1, end: 2 }], t1 + millis(4));
        assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(1_800));

        let mut at = t1 + millis(4);
        for recovery in 1..=16 {
            sends(&mut inner, 1, at);
            inner.congestion.enter_recovery(inner.next_seq.wrapping_sub(1));
            at += millis(4);
            inner.process_ack(inner.next_seq, &[], at);
            assert_eq!(inner.congestion.ca_state, CaState::Open);
            let expected = if recovery < 16 { 1_800 } else { 900 };
            assert_eq!(inner.rack_reo_wnd(), Duration::from_micros(expected), "after {recovery}");
        }
    }

    /// The ranges an acknowledgement the stream would send at `now` names.
    fn named(inner: &mut StreamInner, now: Instant) -> (u32, Vec<SackRange>) {
        let ack = inner.build_ack_at(now);
        let header = Header::decode(&ack).unwrap();
        (header.ack, sack_ranges_in(&header, &ack))
    }

    /// A packet numbered `seq` with a byte of data arrives at `at`.
    fn arrives_at(inner: &mut StreamInner, seq: u32, at: Instant) {
        let (header, data) = incoming(FLAG_DATA, seq, 0, &[seq as u8]);
        inner.receive_packet_at(&header, &data, at);
    }

    #[test]
    fn a_packet_that_arrives_twice_is_reported_below_the_acknowledgement_once() {
        let (mut inner, _reader) = with_reader();
        let t0 = inner.epoch;
        arrives_at(&mut inner, 0, t0);
        arrives_at(&mut inner, 1, t0);
        arrives_at(&mut inner, 0, t0);

        assert_eq!(named(&mut inner, t0), (2, vec![SackRange { start: 0, end: 1 }]));
        assert_eq!(named(&mut inner, t0), (2, vec![]));
    }

    /// A stream that has learnt that its path holds packets back by `late`:
    /// packet 0 arrived `late` after packet 1, and then again.
    fn learnt_reordering(late: Duration) -> (StreamInner, RecvReader, Instant) {
        let (mut inner, reader) = with_reader();
        let t0 = inner.epoch;
        arrives_at(&mut inner, 1, t0);
        arrives_at(&mut inner, 0, t0 + late);
        arrives_at(&mut inner, 0, t0 + late + millis(1));
        named(&mut inner, t0 + late + millis(1));
        (inner, reader, t0 + late + millis(2))
    }

    #[test]
    fn a_gap_younger_than_the_reordering_the_path_showed_is_not_reported_yet() {
        let (mut inner, _reader, t1) = learnt_reordering(millis(2));

        // Packet 2 is missing. The ranges past it wait as long as packets
        // were seen to be held back, and the stream wakes to send them then.
        arrives_at(&mut inner, 3, t1);
        assert_eq!(named(&mut inner, t1), (2, vec![]));
        assert_eq!(named(&mut inner, t1 + Duration::from_micros(1_999)), (2, vec![]));
        assert_eq!(inner.next_deadline(), Some(t1 + millis(2)));
        inner.ack_wanted = false;
        assert!(!inner.on_timers(t1 + millis(2)));
        assert!(inner.ack_wanted);
        assert_eq!(named(&mut inner, t1 + millis(2)), (2, vec![SackRange { start: 3, end: 4 }]));

        // Once it arrives, everything up to the next gap is acknowledged.
        arrives_at(&mut inner, 5, t1 + millis(3));
        arrives_at(&mut inner, 4, t1 + millis(4));
        assert_eq!(named(&mut inner, t1 + millis(4)).0, 2);
        arrives_at(&mut inner, 2, t1 + millis(4));
        assert_eq!(named(&mut inner, t1 + millis(4)), (6, vec![]));
    }

    #[test]
    fn a_range_once_reported_is_not_held_back_again_when_a_late_packet_joins_it() {
        let (mut inner, _reader, t1) = learnt_reordering(millis(2));

        // Packet 2 is missing; 4 was reported past it.
        arrives_at(&mut inner, 4, t1);
        assert_eq!(named(&mut inner, t1 + millis(2)), (2, vec![SackRange { start: 4, end: 5 }]));

        // 3 arrives just now, ahead of 4: the range is still reported.
        arrives_at(&mut inner, 3, t1 + millis(3));
        assert_eq!(named(&mut inner, t1 + millis(3)), (2, vec![SackRange { start: 3, end: 5 }]));
    }

    #[test]
    fn a_gap_is_not_held_back_for_more_than_ten_milliseconds() {
        let (mut inner, _reader, t1) = learnt_reordering(millis(50));
        arrives_at(&mut inner, 3, t1);
        assert_eq!(named(&mut inner, t1 + millis(9)), (2, vec![]));
        assert_eq!(named(&mut inner, t1 + millis(10)), (2, vec![SackRange { start: 3, end: 4 }]));
    }

    #[test]
    fn a_gap_is_reported_at_once_on_a_path_not_seen_to_reorder() {
        let (mut inner, _reader) = with_reader();
        let t0 = inner.epoch;
        arrives_at(&mut inner, 1, t0);
        assert_eq!(named(&mut inner, t0), (0, vec![SackRange { start: 1, end: 2 }]));

        // A gap filled late by a packet that came once was a loss, sent again.
        arrives_at(&mut inner, 0, t0 + millis(30));
        arrives_at(&mut inner, 3, t0 + millis(31));
        assert_eq!(named(&mut inner, t0 + millis(31)), (2, vec![SackRange { start: 3, end: 4 }]));
    }

    /// A round trip of 250 ms: 113 to 137 ms each way, so that packets pass
    /// one another, and one packet in 500 lost.
    fn far_and_jittery(nth: u64, _ms: u64) -> Option<u64> {
        let x = mix(nth);
        (x % 500 != 0).then_some(113 + (x >> 9) % 25)
    }

    /// The same path, without passing: 125 ms each way, and the same packets
    /// lost.
    fn far(nth: u64, _ms: u64) -> Option<u64> {
        (mix(nth) % 500 != 0).then_some(125)
    }

    #[test]
    fn a_long_path_that_reorders_is_as_fast_as_one_that_does_not() {
        const PACKETS: usize = 20_000;
        let all = |_| usize::MAX;
        let steady = transfer_to_a_reader(0, PACKETS, None, far, true, all);
        let reordered = transfer_to_a_reader(0, PACKETS, None, far_and_jittery, true, all);
        assert!(steady.intact && reordered.intact);
        assert_eq!(reordered.gave_up, None);

        let resent = reordered.sent_at.len() - PACKETS;
        let lost = steady.sent_at.len() - PACKETS;
        assert!(resent <= 2 * lost + 20, "{resent} packets went out again, for {lost} lost in order");
        assert!(
            reordered.took * 4 <= steady.took * 5,
            "{} ms, where the same path without the passing took {} ms",
            reordered.took,
            steady.took
        );
    }

    /// A path with a round trip of 200 ms, which is long enough for a
    /// window of 4 MB to be in flight, that holds one packet in seven back
    /// by 30 ms, so that later ones pass it, and loses one in 4,001.
    fn long_and_lossy(nth: u64, _ms: u64) -> Option<u64> {
        let held_back = if nth % 7 == 0 { 30 } else { 0 };
        (nth % 4_001 != 4_000).then_some(100 + held_back)
    }

    #[test]
    fn a_sender_that_keeps_to_the_window_is_not_turned_away_by_a_stream_without_a_limit() {
        let all = |_| usize::MAX;
        let flow = transfer_to_a_reader(0, 20_000, None, long_and_lossy, true, all);
        assert!(flow.intact);
        assert_eq!(flow.gave_up, None);
        // The path carried a full window, and the stream came to hold most
        // of one ahead of a gap.
        assert!(flow.most_in_flight as usize * MAX_PAYLOAD > DEFAULT_RWND as usize * 3 / 4);
        assert!(flow.most_ahead > MAX_OUT_OF_ORDER / 2, "it held {} bytes", flow.most_ahead);
        // Packets that a remote selectively acknowledges leave its flight,
        // and a loss found late leaves a gap open for more than a round
        // trip. The window says what room is left ahead of the gap, so the
        // remote sends no more than that.
        assert_eq!(flow.turned_away, 0);
        assert!(flow.most_ahead <= MAX_OUT_OF_ORDER, "it held {} bytes", flow.most_ahead);
    }

    #[test]
    fn a_path_that_holds_back_one_packet_in_seven_has_little_more_than_its_losses_sent_again() {
        let all = |_| usize::MAX;
        let flow = transfer_to_a_reader(0, 20_000, None, long_and_lossy, true, all);
        assert!(flow.intact);
        // One packet in 4,001 is lost, and every one held back turns up.
        let resent = flow.sent_at.len() - 20_000;
        assert!(resent <= 2 * 5 + 5, "{resent} packets went out again, for 5 lost");
    }

    /// Two streams connected to each other over the loopback interface, and
    /// the sockets they are on.
    async fn pair() -> (UdxStream, UdxStream, [crate::native::socket::UdxSocket; 2]) {
        async fn bound() -> (crate::native::socket::UdxSocket, SocketAddr) {
            let socket = crate::native::socket::UdxSocket::new();
            socket.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
            let addr = socket.local_addr().await.unwrap();
            (socket, addr)
        }
        let (socket_a, addr_a) = bound().await;
        let (socket_b, addr_b) = bound().await;
        let a = UdxStream::new(1);
        let b = UdxStream::new(2);
        a.connect(&socket_a, 2, addr_b).await.unwrap();
        b.connect(&socket_b, 1, addr_a).await.unwrap();
        (a, b, [socket_a, socket_b])
    }

    #[tokio::test]
    async fn a_limit_holds_a_fast_writer_to_what_a_slow_reader_takes() {
        const LIMIT: usize = 131_072;
        let (a, mut b, _sockets) = pair().await;
        b.set_read_buffer_limit(Some(LIMIT as u32));
        assert_eq!(b.read_buffer_limit(), Some(LIMIT as u32));
        let receiver = Arc::clone(&b.inner);

        let data: Vec<u8> = (0..3_000_000).map(|i| (i % 251) as u8).collect();
        let writing = async { a.write(&data).await.unwrap() };
        let reading = async {
            let mut read = Vec::new();
            let mut most_held = 0;
            while read.len() < data.len() {
                most_held = most_held.max(receiver.lock().unwrap().held());
                let chunk = b.read().await.unwrap().expect("the stream ended early");
                read.extend(chunk);
                // A reader slower than the loopback interface.
                if read.len() % 65_536 < MAX_PAYLOAD {
                    tokio::time::sleep(millis(2)).await;
                }
            }
            (read, most_held)
        };
        let both = async { tokio::join!(writing, reading) };
        let ((), (read, most_held)) = tokio::time::timeout(Duration::from_secs(30), both)
            .await
            .expect("the transfer stalled");

        assert!(read == data, "{} of {} bytes arrived, or not as sent", read.len(), data.len());
        assert!(most_held <= LIMIT + FULL_PACKET, "the stream held {most_held} bytes");
        assert!(most_held > LIMIT / 4, "the stream held {most_held} bytes");
    }

    #[tokio::test]
    async fn a_reader_that_stops_for_a_while_gets_everything_once_it_reads_again() {
        const LIMIT: usize = 65_536;
        let (a, mut b, _sockets) = pair().await;
        b.set_read_buffer_limit(Some(LIMIT as u32));
        let receiver = Arc::clone(&b.inner);
        let sender = Arc::clone(&a.inner);

        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 251) as u8).collect();
        let writing = async { a.write(&data).await.unwrap() };
        let reading = async {
            // Long enough for the writer to ask for room more than once.
            tokio::time::sleep(millis(2_500)).await;
            let held = receiver.lock().unwrap().held();
            let waiting = sender.lock().unwrap().unacked_bytes;
            let mut read = Vec::new();
            while read.len() < data.len() {
                read.extend(b.read().await.unwrap().expect("the stream ended early"));
            }
            (read, held, waiting)
        };
        let both = async { tokio::join!(writing, reading) };
        let ((), (read, held, waiting)) = tokio::time::timeout(Duration::from_secs(30), both)
            .await
            .expect("the transfer stalled");

        // The stream was full, and the rest waited with the writer.
        assert!(held <= LIMIT + FULL_PACKET, "the stream held {held} bytes");
        assert!(held > LIMIT / 2, "the stream held {held} bytes");
        assert!(waiting > data.len() - 2 * LIMIT, "{waiting} bytes waited with the writer");
        assert!(read == data, "{} of {} bytes arrived, or not as sent", read.len(), data.len());
    }

    #[tokio::test]
    async fn a_full_stream_says_so_in_all_it_sends_and_says_when_it_has_room_again() {
        use tokio::net::UdpSocket;

        // The next packet of one kind that the stream sends, if it sends one
        // within `wait`: one with data, a heartbeat, or an acknowledgement,
        // which has no flags.
        async fn next(remote: &UdpSocket, kind: u8, wait: Duration) -> Option<Header> {
            let mut buf = [0u8; 2_048];
            let packets = async {
                loop {
                    let (len, _) = remote.recv_from(&mut buf).await.unwrap();
                    let header = Header::decode(&buf[..len]).unwrap();
                    if header.type_flags == kind {
                        return header;
                    }
                }
            };
            tokio::time::timeout(wait, packets).await.ok()
        }

        let limit = MIN_READ_BUFFER_LIMIT as usize;
        let socket = crate::native::socket::UdxSocket::new();
        socket.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = socket.local_addr().await.unwrap();
        // The remote is a socket that says what the test has it say.
        let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut stream = UdxStream::new(1);
        stream.set_read_buffer_limit(Some(limit as u32));
        stream.connect(&socket, 2, remote.local_addr().unwrap()).await.unwrap();

        // A stream with a limit starts by saying what room it has.
        let hello = next(&remote, 0, millis(2_000)).await.expect("word of the room");
        assert_eq!(hello.recv_window as usize, limit - FULL_PACKET);

        // The stream sends a packet of its own, so that its sequence numbers
        // are past zero.
        let _acked = stream.inner.lock().unwrap().queue_write(b"x".to_vec()).unwrap();
        let first = next(&remote, FLAG_DATA, millis(2_000)).await.expect("the stream's packet");
        assert_eq!(first.seq, 0);
        assert_eq!(first.recv_window as usize, limit - FULL_PACKET);

        // The remote acknowledges it, and sends more than the stream holds.
        for seq in 0..12 {
            let packet = build_data_packet(1, seq, 1, &[7u8; PACKET]);
            remote.send_to(&packet, addr).await.unwrap();
        }
        // The acknowledgements it answers with: a few, and then no more.
        let mut last = None;
        for _ in 0..100 {
            let Some(header) = next(&remote, 0, millis(300)).await else {
                break;
            };
            last = Some(header);
        }
        // The first `full` packets take the stream over its limit. It
        // acknowledges those, says that it has no room, and numbers what it
        // says as C libudx does.
        let full = limit.div_ceil(PACKET) as u32;
        let last = last.expect("an acknowledgement");
        assert_eq!((last.ack, last.recv_window, last.seq), (full, 0, 1));

        // Its heartbeat says the same.
        let heartbeat = next(&remote, FLAG_HEARTBEAT, millis(1_500)).await.expect("a heartbeat");
        assert_eq!((heartbeat.ack, heartbeat.recv_window, heartbeat.seq), (full, 0, 1));

        // The reader takes what there is, and the remote hears of the room
        // without having to ask.
        for _ in 0..full {
            let read = tokio::time::timeout(millis(2_000), stream.read()).await;
            let read = read.expect("the reader was not handed every packet").unwrap();
            assert_eq!(read.map(|data| data.len()), Some(PACKET));
        }
        let update = next(&remote, 0, millis(500)).await.expect("word of the room");
        assert_eq!(update.ack, full);
        assert!(update.recv_window as usize >= FULL_PACKET, "room for {}", update.recv_window);
    }
}
