use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::error::{Result, UdxError};

/// Send buffer to ask the system for, in bytes — matches C libudx
/// `UDX_DEFAULT_SNDBUF_SIZE`.
const SEND_BUFFER_SIZE: usize = 212_992;

/// Receive buffers to ask the system for, in bytes, largest first — matches
/// C libudx `udx_socket_bind`.
const RECV_BUFFER_SIZES: [usize; 4] = [1024 * 1024, 512 * 1024, 256 * 1024, 208 * 1024];

/// Ask the system for the socket buffers C libudx asks for.
///
/// A stream sends a burst of packets each time it is woken, and the receiver
/// drops what its buffer cannot hold until its task runs. The defaults hold
/// some fifty packets on Windows and under two hundred on Linux. The socket
/// works with whatever the system grants, so a refusal is not an error.
fn size_buffers(udp: &tokio::net::UdpSocket) {
    let socket = socket2::SockRef::from(udp);
    let _ = socket.set_send_buffer_size(SEND_BUFFER_SIZE);

    // Some systems grant what they can of a size, others refuse a size they
    // cannot grant in full, so step down until one is accepted.
    for size in RECV_BUFFER_SIZES {
        if socket.set_recv_buffer_size(size).is_ok() {
            break;
        }
    }
}

/// An incoming unreliable datagram received on a [`UdxSocket`].
#[derive(Debug, Clone)]
pub struct Datagram {
    /// Raw payload bytes.
    pub data: Vec<u8>,
    /// Source address of the datagram.
    pub addr: SocketAddr,
}

/// Whether a receive error only reports that a datagram this socket sent
/// earlier was not delivered.
///
/// Windows hands the ICMP error that such a datagram draws to the socket's
/// next receive: `WSAECONNRESET` for "port unreachable", and `WSAENETRESET`
/// for "time to live exceeded", which a low-TTL holepunch probe draws from a
/// router. Neither says anything about the socket, which still sends and
/// receives. Other systems report nothing on an unconnected socket.
fn reports_undelivered_datagram(err: &std::io::Error) -> bool {
    // `std` gives `WSAENETRESET` no `ErrorKind` of its own.
    const WSAENETRESET: i32 = 10052;

    err.kind() == std::io::ErrorKind::ConnectionReset
        || (cfg!(windows) && err.raw_os_error() == Some(WSAENETRESET))
}

/// A stream, as its socket's receive task reaches it.
#[derive(Clone)]
pub(crate) struct StreamEntry {
    /// The stream's state, to take in a packet where it is received.
    pub(crate) inner: Arc<Mutex<super::stream::StreamInner>>,
    /// The way to the stream's own task, for what that task has to see.
    pub(crate) incoming: mpsc::UnboundedSender<super::stream::IncomingPacket>,
}

/// The streams of a socket, by the id that their packets carry.
pub(crate) type StreamMap = Arc<Mutex<HashMap<u32, StreamEntry>>>;

/// Whether the receive task keeps the acknowledgements it owes waiting for
/// more datagrams once the socket is empty.
///
/// C libudx acknowledges on the turn of its loop after the one that read the
/// packets, and reads again before it does, so that one acknowledgement
/// answers what arrived in between. Whether waiting pays depends on what a
/// datagram costs to send. On Windows it costs several times what a turn
/// does, and fewer acknowledgements carry a stream further. On Linux it
/// costs about what a turn does, and a stream was measured no faster for
/// the waiting, so elsewhere acknowledgements go out as soon as the socket
/// is empty.
const ACKS_WAIT: bool = cfg!(windows);

/// While datagrams keep arriving, the receive task acknowledges once for
/// this many of them.
///
/// An acknowledgement costs a datagram to send, and costs the remote, which
/// is busy sending, the work of taking it in. Between two programs on one
/// Windows machine a stream carries about a fifth more with one
/// acknowledgement for sixty packets than with one for four or five.
const ACK_EVERY: usize = 64;

/// How long the receive task waits for the next datagram of a flow before
/// it sends the acknowledgements it owes. A remote that waits for an
/// acknowledgement before it sends more is kept waiting this long at most.
const ACK_WAIT: Duration = Duration::from_micros(50);

/// A turn that takes longer than this is too long to keep an acknowledgement
/// waiting for: the thread has other work to do.
const SLOW_TURN: Duration = Duration::from_micros(100);

/// After a slow turn, how many times acknowledgements go out at once before
/// a turn is tried again.
const SLOW_TURN_PAUSE: u32 = 64;

/// Decides, each time acknowledgements are owed and the socket is empty,
/// whether they wait another turn of the event loop for more datagrams.
///
/// They always wait one turn, as in C libudx. They wait longer only while
/// datagrams arrive faster than the waiting runs out. A wait that a datagram
/// ends says that a flow is under way, and from then on the acknowledgements
/// wait up to [`ACK_WAIT`] for each next datagram, until [`ACK_EVERY`] have
/// been read. A wait that nothing ends says that the flow has stopped, and
/// the next wait is one turn again. So a path too slow to fill a wait costs
/// one turn for each acknowledgement, as before, and a remote that has
/// stopped to wait for an acknowledgement gets it within `ACK_WAIT`.
struct AckWait {
    enabled: bool,
    /// How many more times acknowledgements go out at once.
    paused_for: u32,
    /// Whether the last wait was ended by a datagram.
    in_flow: bool,
    /// How long acknowledgements have waited since a datagram was last read.
    waited: Duration,
}

impl AckWait {
    fn new(enabled: bool) -> Self {
        Self { enabled, paused_for: 0, in_flow: false, waited: Duration::ZERO }
    }

    /// Whether the acknowledgements owed wait another turn, `read` datagrams
    /// having been read since acknowledgements last went out.
    fn wanted(&mut self, read: usize) -> bool {
        if !self.enabled || read >= ACK_EVERY {
            return false;
        }
        if self.paused_for > 0 {
            self.paused_for -= 1;
            return false;
        }
        if self.in_flow { self.waited < ACK_WAIT } else { self.waited.is_zero() }
    }

    /// Take note of a turn that the acknowledgements waited, and of how long
    /// it took.
    fn took(&mut self, turn: Duration) {
        // A turn too short for the clock to measure still counts as waiting.
        self.waited += turn.max(Duration::from_nanos(1));
        if turn > SLOW_TURN {
            self.paused_for = SLOW_TURN_PAUSE;
        }
    }

    /// Take note of a datagram that was read.
    fn arrived(&mut self) {
        if !self.waited.is_zero() {
            self.in_flow = true;
            self.waited = Duration::ZERO;
        }
    }

    /// Take note that the acknowledgements went out.
    fn acknowledged(&mut self) {
        if !self.waited.is_zero() {
            self.in_flow = false;
            self.waited = Duration::ZERO;
        }
    }
}

/// Send the acknowledgements that streams owe for what they took in.
fn acknowledge(
    udp: &tokio::net::UdpSocket,
    owed: &mut Vec<Arc<Mutex<super::stream::StreamInner>>>,
) {
    for stream in owed.drain(..) {
        super::stream::send_ack(&stream, udp);
    }
}

/// How long a socket keeps a packet that came for a stream not registered on
/// it ([`EarlyPackets`]). A remote sends a packet again when it has heard
/// nothing for its first retransmission timeout, a second, so by then the
/// packet is no longer the stream's only chance at it.
const EARLY_FOR: Duration = Duration::from_secs(1);

/// How many bytes of such packets a socket keeps at most. A remote that
/// dials a stream sends a few packets before it hears back.
const EARLY_BYTES: usize = 64 * 1024;

/// Packets that came for a stream not registered on the socket, kept a
/// little while and handed to the stream when it registers
/// ([`UdxSocket::register_stream`]), oldest first.
///
/// A remote can send to a stream before the stream is registered: a server
/// dials a client as soon as it has the client's handshake, and its reply,
/// which tells the client the id of the server's stream, comes after, through
/// a relay. C libudx registers a stream as it is made, and takes such a
/// packet in. Here it would be lost, and the stream would wait for the remote
/// to send it again.
#[derive(Default)]
struct EarlyPackets {
    packets: std::collections::VecDeque<EarlyPacket>,
    bytes: usize,
}

struct EarlyPacket {
    /// The stream it is for: the id it carries.
    id: u32,
    data: Vec<u8>,
    addr: SocketAddr,
    came: Instant,
}

impl EarlyPackets {
    /// Keep `data`, which came for the stream `id` from `addr` at `now`,
    /// letting the oldest go to make room for it.
    fn keep(&mut self, id: u32, data: &[u8], addr: SocketAddr, now: Instant) {
        if data.len() > EARLY_BYTES {
            return;
        }
        self.let_go_before(now);
        while self.bytes + data.len() > EARLY_BYTES {
            let Some(oldest) = self.packets.pop_front() else {
                break;
            };
            self.bytes -= oldest.data.len();
        }
        self.bytes += data.len();
        self.packets.push_back(EarlyPacket {
            id,
            data: data.to_vec(),
            addr,
            came: now,
        });
    }

    /// The packets kept for the stream `id`, in the order they came.
    fn take(&mut self, id: u32, now: Instant) -> Vec<(Vec<u8>, SocketAddr)> {
        self.let_go_before(now);
        if !self.packets.iter().any(|packet| packet.id == id) {
            return Vec::new();
        }
        let (taken, kept): (std::collections::VecDeque<_>, _) =
            self.packets.drain(..).partition(|packet| packet.id == id);
        self.packets = kept;
        taken
            .into_iter()
            .map(|packet| {
                self.bytes -= packet.data.len();
                (packet.data, packet.addr)
            })
            .collect()
    }

    /// Let go of the packets kept for [`EARLY_FOR`] or longer.
    fn let_go_before(&mut self, now: Instant) {
        while let Some(oldest) = self.packets.front() {
            if now.saturating_duration_since(oldest.came) < EARLY_FOR {
                break;
            }
            self.bytes -= oldest.data.len();
            self.packets.pop_front();
        }
    }
}

/// Hand a packet kept for a stream to it as the receive task would have, and
/// send at once the acknowledgement that the packet makes the stream owe.
fn hand_over(entry: &StreamEntry, data: Vec<u8>, addr: SocketAddr, udp: &tokio::net::UdpSocket) {
    let Ok(header) = super::header::Header::decode(&data) else {
        return;
    };
    let taken = super::stream::take_in(&entry.inner, &header, data);
    if let Some(data) = taken.for_its_task {
        let _ = entry
            .incoming
            .send(super::stream::IncomingPacket { data, addr });
    }
    if taken.ack_owed {
        super::stream::send_ack(&entry.inner, udp);
    }
}

struct UdxSocketInner {
    udp: OnceLock<Arc<tokio::net::UdpSocket>>,
    recv_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    streams: StreamMap,
    fallback_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Datagram>>>>,
    /// Taken under the lock of `streams`, after it.
    early: Arc<Mutex<EarlyPackets>>,
}

impl UdxSocketInner {
    fn new() -> Self {
        Self {
            udp: OnceLock::new(),
            recv_task: Mutex::new(None),
            streams: Arc::new(Mutex::new(HashMap::new())),
            fallback_tx: Arc::new(Mutex::new(None)),
            early: Arc::new(Mutex::new(EarlyPackets::default())),
        }
    }

    fn udp_arc(&self) -> Result<Arc<tokio::net::UdpSocket>> {
        Ok(Arc::clone(
            self.udp
                .get()
                .ok_or_else(|| UdxError::Io(std::io::Error::other("socket not bound")))?,
        ))
    }

    fn ensure_recv_loop(&self) -> Result<()> {
        let mut guard = self.recv_task.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref handle) = *guard {
            if !handle.is_finished() {
                return Ok(());
            }
        }

        let udp = self.udp_arc()?;
        let streams = Arc::clone(&self.streams);
        let fallback_tx = Arc::clone(&self.fallback_tx);
        let early = Arc::clone(&self.early);

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            // The streams that came to owe an acknowledgement for what they
            // took in since acknowledgements last went out.
            let mut owed: Vec<Arc<Mutex<super::stream::StreamInner>>> = Vec::new();
            // Datagrams read since acknowledgements last went out.
            let mut read = 0;
            let mut wait = AckWait::new(ACKS_WAIT);
            // Whether a stream that is owed for is not to be kept waiting.
            let mut ack_now = false;


            loop {
                if read >= super::stream::MAX_RECV_BATCH {
                    // The socket has not run empty for a while. What is owed
                    // goes out, and the other tasks get to run, the readers
                    // of these streams among them.
                    acknowledge(&udp, &mut owed);
                    wait.acknowledged();
                    read = 0;
                    ack_now = false;
                    tokio::task::yield_now().await;
                }

                let (len, addr) = match udp.try_recv_from(&mut buf) {
                    Ok(received) => received,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        // The socket is empty.
                        if !owed.is_empty() && !ack_now && wait.wanted(read) {

                            let started = Instant::now();
                            tokio::task::yield_now().await;
                            wait.took(started.elapsed());
                            // What arrived meanwhile is read first.
                            continue;
                        }
                        acknowledge(&udp, &mut owed);
                        wait.acknowledged();
                        read = 0;
                        ack_now = false;
                        match udp.readable().await {
                            Ok(()) => continue,
                            Err(_) => break,
                        }
                    }
                    // An earlier datagram was not delivered. The socket
                    // itself is fine, so keep receiving, as libuv does.
                    Err(err) if reports_undelivered_datagram(&err) => {
                        read += 1;
                        continue;
                    }
                    Err(_) => break,
                };
                read += 1;
                wait.arrived();
                let packet = buf[..len].to_vec();

                if len >= super::header::HEADER_SIZE {
                    if let Ok(hdr) = super::header::Header::decode(&packet) {
                        let stream = {
                            let streams = streams.lock().unwrap_or_else(|e| e.into_inner());
                            let stream = streams.get(&hdr.remote_id).cloned();
                            if stream.is_none() {
                                // Kept for the stream in case it is about to
                                // register, under the lock it registers under,
                                // so that it finds this or what comes after.
                                early.lock().unwrap_or_else(|e| e.into_inner()).keep(
                                    hdr.remote_id,
                                    &packet,
                                    addr,
                                    Instant::now(),
                                );
                            }
                            stream
                        };
                        if let Some(stream) = stream {
                            // The stream takes in what the packet carries
                            // for its reader, here. Its own task sees what
                            // is left for it.
                            let taken = super::stream::take_in(&stream.inner, &hdr, packet);
                            if let Some(data) = taken.for_its_task {
                                let _ = stream
                                    .incoming
                                    .send(super::stream::IncomingPacket { data, addr });
                            }
                            ack_now |= taken.ack_now;
                            if taken.ack_owed {
                                owed.push(stream.inner);
                            }
                            continue;
                        }
                    }
                }

                let guard = fallback_tx.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(ref tx) = *guard {
                    let _ = tx.send(Datagram { data: packet, addr });
                }
            }
        });

        *guard = Some(handle);
        Ok(())
    }

    fn close_impl(&self) {
        if let Ok(mut guard) = self.recv_task.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

impl Drop for UdxSocketInner {
    fn drop(&mut self) {
        self.close_impl();
    }
}

/// A UDP socket used for UDX stream transport and unreliable datagrams.
///
/// `UdxSocket` is a cheap-clone handle: cloning it increments a reference
/// count and all clones share the same underlying UDP socket. The socket is
/// closed (receive loop aborted) when the *last* clone is dropped.
///
/// Incoming packets are demultiplexed: UDX stream packets (identified by
/// header magic + stream ID) are routed to the owning [`super::stream::UdxStream`],
/// while non-UDX packets are delivered as [`Datagram`]s via [`recv_start`](Self::recv_start).
#[derive(Clone)]
pub struct UdxSocket {
    inner: Arc<UdxSocketInner>,
}

impl UdxSocket {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(UdxSocketInner::new()),
        }
    }

    /// Bind the socket to a local address. Returns an error if already bound.
    ///
    /// On Linux the socket's packets are marked "don't fragment", as C libudx
    /// marks them, so a datagram larger than the path carries is dropped
    /// rather than split. A stream keeps its packets within what it has found
    /// the path to carry. A datagram sent with [`send_to`](Self::send_to) is
    /// the caller's to keep small enough.
    pub async fn bind(&self, addr: SocketAddr) -> Result<()> {
        let socket = tokio::net::UdpSocket::bind(addr).await?;
        size_buffers(&socket);
        #[cfg(target_os = "linux")]
        {
            // The socket works without it, as it does on the other systems:
            // a refusal is not an error.
            if let Err(error) = super::pmtu::set_dont_fragment(&socket, addr.is_ipv6()) {
                tracing::debug!(%error, "udx: could not mark the socket's packets don't fragment");
            }
        }
        self.inner
            .udp
            .set(Arc::new(socket))
            .map_err(|_| UdxError::Io(std::io::Error::other("socket already bound")))?;
        Ok(())
    }

    /// Return the local address this socket is bound to.
    pub async fn local_addr(&self) -> Result<SocketAddr> {
        let udp = self
            .inner
            .udp
            .get()
            .ok_or_else(|| UdxError::Io(std::io::Error::other("socket not bound")))?;
        Ok(udp.local_addr()?)
    }

    /// Set the IP time-to-live (IPv4 `IP_TTL` / IPv6 hop limit) used for
    /// outgoing datagrams sent on this socket.
    ///
    /// A low TTL is used by NAT-holepunching probes that intentionally
    /// expire before reaching the remote peer (birthday-attack probes that
    /// must not trigger a "connection refused" ICMP response visible to the
    /// remote NAT), while the default TTL is used for regular traffic.
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        let udp = self.inner.udp_arc()?;
        Ok(udp.set_ttl(ttl)?)
    }

    /// Return the currently configured IP time-to-live for this socket.
    pub fn ttl(&self) -> Result<u32> {
        let udp = self.inner.udp_arc()?;
        Ok(udp.ttl()?)
    }

    /// Get a shared reference to the underlying UDP socket.
    pub(crate) fn udp_arc(&self) -> Result<Arc<tokio::net::UdpSocket>> {
        self.inner.udp_arc()
    }

    /// Register a stream to receive packets addressed to `local_id`: its
    /// state, to take in what it can where it is received, and the way to
    /// its own task for the rest. The packets that came for it shortly
    /// before ([`EarlyPackets`]) it takes in first.
    pub(crate) fn register_stream(
        &self,
        local_id: u32,
        inner: Arc<Mutex<super::stream::StreamInner>>,
        incoming: mpsc::UnboundedSender<super::stream::IncomingPacket>,
    ) -> Result<()> {
        let entry = StreamEntry { inner, incoming };
        {
            let mut streams = self.inner.streams.lock().unwrap_or_else(|e| e.into_inner());
            streams.insert(local_id, entry.clone());
            // Under the lock the receive task looks streams up under, so that
            // it hands the stream nothing newer before these.
            let early = self
                .inner
                .early
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(local_id, Instant::now());
            if !early.is_empty() {
                let udp = self.inner.udp_arc()?;
                for (data, addr) in early {
                    hand_over(&entry, data, addr, &udp);
                }
            }
        }
        self.inner.ensure_recv_loop()?;
        Ok(())
    }

    pub(crate) fn streams_ref(&self) -> StreamMap {
        Arc::clone(&self.inner.streams)
    }

    /// Send an unreliable datagram to `addr`. Fire-and-forget.
    pub fn send_to(&self, data: &[u8], addr: SocketAddr) -> Result<()> {
        let udp = self.inner.udp_arc()?;
        let data = data.to_vec();
        tokio::spawn(async move {
            let _ = udp.send_to(&data, addr).await;
        });
        Ok(())
    }

    /// Begin receiving non-stream datagrams on this socket.
    pub fn recv_start(&self) -> Result<mpsc::UnboundedReceiver<Datagram>> {
        let (tx, rx) = mpsc::unbounded_channel();
        *self
            .inner
            .fallback_tx
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(tx);
        self.inner.ensure_recv_loop()?;
        Ok(rx)
    }

    /// Close the socket and stop the receive loop.
    ///
    /// Consumes `self`. Because `UdxSocket` is a clone handle, the socket is
    /// only actually closed when every clone has either been dropped or
    /// `close`d and the internal `Arc` reaches zero.
    pub async fn close(self) -> Result<()> {
        let inner = self.inner.clone();
        drop(self);
        if Arc::strong_count(&inner) == 0 {
            inner.close_impl();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};
    use std::time::Duration;

    use super::{
        reports_undelivered_datagram, AckWait, EarlyPackets, ACK_EVERY, ACK_WAIT, EARLY_BYTES,
        EARLY_FOR, SLOW_TURN, SLOW_TURN_PAUSE,
    };

    fn from(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn kept_packets_go_to_the_stream_they_came_for_in_the_order_they_came() {
        let now = std::time::Instant::now();
        let mut early = EarlyPackets::default();
        early.keep(1, b"first", from(1000), now);
        early.keep(2, b"other", from(2000), now);
        early.keep(1, b"second", from(1001), now);

        assert_eq!(
            early.take(1, now),
            vec![(b"first".to_vec(), from(1000)), (b"second".to_vec(), from(1001))]
        );
        assert!(early.take(1, now).is_empty(), "a packet is handed over once");
        assert_eq!(early.take(2, now), vec![(b"other".to_vec(), from(2000))]);
        assert_eq!(early.bytes, 0);
    }

    #[test]
    fn a_kept_packet_is_let_go_once_the_remote_would_send_it_again() {
        let now = std::time::Instant::now();
        let mut early = EarlyPackets::default();
        early.keep(1, b"old", from(1000), now);
        early.keep(1, b"new", from(1000), now + EARLY_FOR / 2);

        assert_eq!(
            early.take(1, now + EARLY_FOR),
            vec![(b"new".to_vec(), from(1000))],
            "kept for less than EARLY_FOR"
        );
        assert_eq!(early.bytes, 0);
    }

    #[test]
    fn the_oldest_kept_packets_make_room_for_new_ones() {
        let now = std::time::Instant::now();
        let mut early = EarlyPackets::default();
        let packet = vec![0u8; EARLY_BYTES / 4];
        for id in 0..5 {
            early.keep(id, &packet, from(1000), now);
        }

        assert!(early.take(0, now).is_empty(), "the oldest was let go");
        for id in 1..5 {
            assert_eq!(early.take(id, now).len(), 1, "stream {id}");
        }

        early.keep(9, &vec![0u8; EARLY_BYTES + 1], from(1000), now);
        assert!(early.take(9, now).is_empty(), "more than it keeps in all is not kept");
        assert_eq!(early.bytes, 0);
    }

    #[test]
    fn connection_reset_reports_an_undelivered_datagram() {
        let err = Error::from(ErrorKind::ConnectionReset);
        assert!(reports_undelivered_datagram(&err));
    }

    #[cfg(windows)]
    #[test]
    fn windows_icmp_errors_report_an_undelivered_datagram() {
        const WSAECONNRESET: i32 = 10054; // port unreachable
        const WSAENETRESET: i32 = 10052; // time to live exceeded

        for code in [WSAECONNRESET, WSAENETRESET] {
            let err = Error::from_raw_os_error(code);
            assert!(reports_undelivered_datagram(&err), "os error {code}");
        }
    }

    #[test]
    fn other_errors_do_not_report_an_undelivered_datagram() {
        for err in [
            Error::from(ErrorKind::PermissionDenied),
            Error::other("socket closed"),
        ] {
            assert!(!reports_undelivered_datagram(&err), "{err}");
        }
    }

    #[test]
    fn acknowledgements_wait_only_where_that_is_switched_on() {
        assert!(AckWait::new(true).wanted(1));
        assert!(!AckWait::new(false).wanted(1));
    }

    const TURN: Duration = Duration::from_micros(2);

    /// A wait that a datagram has just ended, so that a flow is under way.
    fn in_a_flow() -> AckWait {
        let mut wait = AckWait::new(true);
        assert!(wait.wanted(1));
        wait.took(TURN);
        wait.arrived();
        wait
    }

    #[test]
    fn acknowledgements_wait_one_turn_when_nothing_follows() {
        let mut wait = AckWait::new(true);

        for _ in 0..3 {
            assert!(wait.wanted(1));
            wait.took(TURN);
            assert!(!wait.wanted(1), "one turn, and no more");
            wait.acknowledged();
        }
    }

    #[test]
    fn a_turn_too_short_to_measure_still_counts_as_one() {
        let mut wait = AckWait::new(true);
        assert!(wait.wanted(1));
        wait.took(Duration::ZERO);
        assert!(!wait.wanted(1));
    }

    #[test]
    fn a_datagram_that_ends_a_wait_starts_a_flow() {
        let mut wait = in_a_flow();

        // Each next datagram is waited for until the wait runs out.
        let mut waited = Duration::ZERO;
        while waited < ACK_WAIT {
            assert!(wait.wanted(2), "after {waited:?}");
            wait.took(TURN);
            waited += TURN;
        }
        assert!(!wait.wanted(2));
    }

    #[test]
    fn the_wait_for_the_next_datagram_starts_anew_with_each_one() {
        let mut wait = in_a_flow();

        for read in 2..10 {
            // Most of a wait goes by, and then a datagram comes.
            wait.took(ACK_WAIT - TURN);
            assert!(wait.wanted(read));
            wait.arrived();
        }
    }

    #[test]
    fn a_flow_is_acknowledged_once_for_every_so_many_datagrams() {
        let mut wait = in_a_flow();
        assert!(wait.wanted(ACK_EVERY - 1));
        assert!(!wait.wanted(ACK_EVERY));

        // The flow goes on after the acknowledgement.
        wait.acknowledged();
        wait.took(TURN);
        assert!(wait.wanted(1), "more than one turn is waited");
    }

    #[test]
    fn a_wait_that_nothing_ends_stops_the_flow() {
        let mut wait = in_a_flow();
        wait.took(ACK_WAIT);
        assert!(!wait.wanted(2));
        wait.acknowledged();

        // One turn again.
        assert!(wait.wanted(1));
        wait.took(TURN);
        assert!(!wait.wanted(1));
    }

    #[test]
    fn a_slow_turn_pauses_the_waiting_and_then_a_turn_is_tried_again() {
        let mut wait = AckWait::new(true);

        assert!(wait.wanted(1));
        wait.took(SLOW_TURN);
        wait.acknowledged();
        assert!(wait.wanted(1), "a turn of the limit itself is not slow");

        wait.took(SLOW_TURN + Duration::from_micros(1));
        wait.acknowledged();
        for _ in 0..SLOW_TURN_PAUSE {
            assert!(!wait.wanted(1));
        }
        assert!(wait.wanted(1));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_bound_socket_marks_its_packets_dont_fragment() {
        use rustix::net::sockopt::{ip_mtu_discover, Ipv4PathMtuDiscovery};

        let socket = super::UdxSocket::new();
        socket.bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

        let udp = socket.udp_arc().unwrap();
        assert_eq!(ip_mtu_discover(&*udp).unwrap(), Ipv4PathMtuDiscovery::PROBE);
    }
}
