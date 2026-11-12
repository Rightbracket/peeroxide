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

/// Whether the receive task gives the event loop one turn before it sends
/// the acknowledgements it owes, and reads the socket once more first.
///
/// C libudx acknowledges on the turn of its loop after the one that read the
/// packets, and reads again before it does, so that one acknowledgement
/// answers what arrived in between. Whether that pays depends on what a
/// datagram costs to send. On Windows it costs several times what a turn
/// does, and fewer acknowledgements carry a stream further. Elsewhere it
/// costs about what a turn does, and a stream was measured slower for the
/// waiting, so acknowledgements go out as soon as the socket is empty.
const ACK_AFTER_A_TURN: bool = cfg!(windows);

/// A turn that takes longer than this is too long to keep an acknowledgement
/// waiting for: the thread has other work to do.
const SLOW_TURN: Duration = Duration::from_micros(100);

/// After a slow turn, how many times acknowledgements go out at once before
/// a turn is tried again.
const SLOW_TURN_PAUSE: u32 = 64;

/// Decides, each time acknowledgements are owed and the socket is empty,
/// whether they wait one turn of the event loop.
struct AckTurns {
    enabled: bool,
    /// How many more times acknowledgements go out at once.
    paused_for: u32,
}

impl AckTurns {
    fn new(enabled: bool) -> Self {
        Self { enabled, paused_for: 0 }
    }

    /// Whether the acknowledgements owed now wait a turn.
    fn wanted(&mut self) -> bool {
        if !self.enabled {
            return false;
        }
        if self.paused_for > 0 {
            self.paused_for -= 1;
            return false;
        }
        true
    }

    /// Take note of how long a turn took.
    fn took(&mut self, turn: Duration) {
        if turn > SLOW_TURN {
            self.paused_for = SLOW_TURN_PAUSE;
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

struct UdxSocketInner {
    udp: OnceLock<Arc<tokio::net::UdpSocket>>,
    recv_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    streams: StreamMap,
    fallback_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Datagram>>>>,
}

impl UdxSocketInner {
    fn new() -> Self {
        Self {
            udp: OnceLock::new(),
            recv_task: Mutex::new(None),
            streams: Arc::new(Mutex::new(HashMap::new())),
            fallback_tx: Arc::new(Mutex::new(None)),
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

        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            // The streams that came to owe an acknowledgement for what they
            // took in since acknowledgements last went out.
            let mut owed: Vec<Arc<Mutex<super::stream::StreamInner>>> = Vec::new();
            // Datagrams read since acknowledgements last went out.
            let mut read = 0;
            let mut turns = AckTurns::new(ACK_AFTER_A_TURN);
            let mut turn_given = false;

            loop {
                if read >= super::stream::MAX_RECV_BATCH {
                    // The socket has not run empty for a while. What is owed
                    // goes out, and the other tasks get to run, the readers
                    // of these streams among them.
                    acknowledge(&udp, &mut owed);
                    read = 0;
                    turn_given = false;
                    tokio::task::yield_now().await;
                }

                let (len, addr) = match udp.try_recv_from(&mut buf) {
                    Ok(received) => received,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        // The socket is empty.
                        if !owed.is_empty() && !turn_given && turns.wanted() {
                            turn_given = true;
                            let started = Instant::now();
                            tokio::task::yield_now().await;
                            turns.took(started.elapsed());
                            // What arrived meanwhile is read first.
                            continue;
                        }
                        acknowledge(&udp, &mut owed);
                        read = 0;
                        turn_given = false;
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
                let packet = buf[..len].to_vec();

                if len >= super::header::HEADER_SIZE {
                    if let Ok(hdr) = super::header::Header::decode(&packet) {
                        let stream = streams
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get(&hdr.remote_id)
                            .cloned();
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
    pub async fn bind(&self, addr: SocketAddr) -> Result<()> {
        let socket = tokio::net::UdpSocket::bind(addr).await?;
        size_buffers(&socket);
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
    /// its own task for the rest.
    pub(crate) fn register_stream(
        &self,
        local_id: u32,
        inner: Arc<Mutex<super::stream::StreamInner>>,
        incoming: mpsc::UnboundedSender<super::stream::IncomingPacket>,
    ) -> Result<()> {
        self.inner
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(local_id, StreamEntry { inner, incoming });
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

    use super::{reports_undelivered_datagram, AckTurns, SLOW_TURN, SLOW_TURN_PAUSE};

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
    fn acknowledgements_wait_a_turn_only_where_that_is_switched_on() {
        assert!(AckTurns::new(true).wanted());
        assert!(!AckTurns::new(false).wanted());
    }

    #[test]
    fn a_slow_turn_pauses_the_waiting_and_then_a_turn_is_tried_again() {
        let mut turns = AckTurns::new(true);

        turns.took(SLOW_TURN);
        assert!(turns.wanted(), "a turn of the limit itself is not slow");

        turns.took(SLOW_TURN + Duration::from_micros(1));
        for _ in 0..SLOW_TURN_PAUSE {
            assert!(!turns.wanted());
        }
        assert!(turns.wanted());
    }
}
