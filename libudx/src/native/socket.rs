use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::mpsc;

use crate::error::{Result, UdxError};

/// An incoming unreliable datagram received on a [`UdxSocket`].
#[derive(Debug, Clone)]
pub struct Datagram {
    /// Raw payload bytes.
    pub data: Vec<u8>,
    /// Source address of the datagram.
    pub addr: SocketAddr,
}

struct UdxSocketInner {
    udp: OnceLock<Arc<tokio::net::UdpSocket>>,
    recv_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<super::stream::IncomingPacket>>>>,
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
            while let Ok((len, addr)) = udp.recv_from(&mut buf).await {
                let packet = buf[..len].to_vec();

                if len >= super::header::HEADER_SIZE {
                    if let Ok(hdr) = super::header::Header::decode(&packet) {
                        let guard = streams.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(tx) = guard.get(&hdr.remote_id) {
                            let _ = tx.send(super::stream::IncomingPacket {
                                data: packet,
                                addr,
                            });
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

    /// Register a stream to receive packets addressed to `local_id`.
    pub(crate) fn register_stream(
        &self,
        local_id: u32,
        tx: mpsc::UnboundedSender<super::stream::IncomingPacket>,
    ) -> Result<()> {
        self.inner
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(local_id, tx);
        self.inner.ensure_recv_loop()?;
        Ok(())
    }

    pub(crate) fn streams_ref(
        &self,
    ) -> Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<super::stream::IncomingPacket>>>> {
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
