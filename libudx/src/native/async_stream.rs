use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};

use crate::error::Result as UdxResult;
use super::header::{Header, FLAG_END};
use super::stream::{StreamEvent, StreamInner, DEFAULT_RWND, build_data_packet};

/// Adapter that implements [`tokio::io::AsyncRead`] and [`tokio::io::AsyncWrite`]
/// over a [`super::stream::UdxStream`].
///
/// Created via [`super::stream::UdxStream::into_async_stream`]. Higher layers
/// (e.g. SecretStream) wrap this for encrypted I/O.
pub struct UdxAsyncStream {
    inner: Arc<Mutex<StreamInner>>,
    read_rx: mpsc::UnboundedReceiver<StreamEvent>,
    read_buf: Vec<u8>,
    read_pos: usize,
    read_eof: bool,
    pending_ack: Option<oneshot::Receiver<UdxResult<()>>>,
    processor: Option<tokio::task::JoinHandle<()>>,
    fin_queued: bool,
}

impl UdxAsyncStream {
    pub(crate) fn new(
        inner: Arc<Mutex<StreamInner>>,
        read_rx: mpsc::UnboundedReceiver<StreamEvent>,
        processor: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            inner,
            read_rx,
            read_buf: Vec::new(),
            read_pos: 0,
            read_eof: false,
            pending_ack: None,
            processor,
            fin_queued: false,
        }
    }
}

impl AsyncRead for UdxAsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_eof {
            return Poll::Ready(Ok(()));
        }

        if self.read_pos < self.read_buf.len() {
            let remaining = &self.read_buf[self.read_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.read_pos += n;
            if self.read_pos >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match self.read_rx.poll_recv(cx) {
            Poll::Ready(Some(StreamEvent::Data(data))) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.read_buf = data;
                    self.read_pos = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(StreamEvent::End)) | Poll::Ready(None) => {
                self.read_eof = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UdxAsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(ref mut ack_rx) = self.pending_ack {
            match Pin::new(ack_rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.pending_ack = None;
                    if let Err(e) = result {
                        return Poll::Ready(Err(io::Error::other(e.to_string())));
                    }
                }
                Poll::Ready(Err(_)) => {
                    self.pending_ack = None;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "UDX stream closed",
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let (prep, max_payload) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let p = inner
                .prepare_write(buf.len())
                .map_err(|e| io::Error::other(e.to_string()))?;
            let mp = inner.max_payload();
            (p, mp)
        };

        let data = buf.to_vec();
        let len = data.len();
        let remote_addr = prep.remote_addr;
        let remote_id = prep.remote_id;
        let first_seq = prep.first_seq;
        let current_ack = prep.current_ack;

        let chunks: Vec<Vec<u8>> = if data.is_empty() {
            vec![vec![]]
        } else {
            data.chunks(max_payload).map(|c| c.to_vec()).collect()
        };

        let mut wire_packets: Vec<Vec<u8>> = Vec::with_capacity(chunks.len());
        for (i, chunk) in chunks.iter().enumerate() {
            let seq = first_seq + i as u32;
            wire_packets.push(build_data_packet(remote_id, seq, current_ack, chunk));
        }

        {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let packets: Vec<(u32, Vec<u8>)> = wire_packets
                .into_iter()
                .enumerate()
                .map(|(i, pkt)| (first_seq + i as u32, pkt))
                .collect();

            guard.queue_for_send(packets, remote_addr);
        }

        self.pending_ack = Some(prep.ack_rx);

        if let Some(ref mut ack) = self.pending_ack {
            match Pin::new(ack).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.pending_ack = None;
                    if let Err(e) = result {
                        return Poll::Ready(Err(io::Error::other(e.to_string())));
                    }
                    Poll::Ready(Ok(len))
                }
                Poll::Ready(Err(_)) => {
                    self.pending_ack = None;
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "UDX stream closed",
                    )))
                }
                Poll::Pending => Poll::Ready(Ok(len)),
            }
        } else {
            Poll::Ready(Ok(len))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref mut ack_rx) = self.pending_ack {
            match Pin::new(ack_rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.pending_ack = None;
                    if let Err(e) = result {
                        return Poll::Ready(Err(io::Error::other(e.to_string())));
                    }
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(_)) => {
                    self.pending_ack = None;
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "UDX stream closed",
                    )))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(ref mut ack_rx) = self.pending_ack {
            match Pin::new(ack_rx).poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.pending_ack = None;
                    if let Err(e) = result {
                        return Poll::Ready(Err(io::Error::other(e.to_string())));
                    }
                    if self.fin_queued {
                        return Poll::Ready(Ok(()));
                    }
                    // Write ACK resolved — fall through to queue FIN
                }
                Poll::Ready(Err(_)) => {
                    self.pending_ack = None;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.connected {
            return Poll::Ready(Ok(()));
        }
        let remote_addr = inner.remote_addr;
        if let Some(addr) = remote_addr {
            let remote_id = inner.remote_id;
            let seq = inner.next_seq;
            inner.next_seq = seq + 1;
            let ack = inner.next_remote_seq;

            let header = Header {
                type_flags: FLAG_END,
                data_offset: 0,
                remote_id,
                recv_window: DEFAULT_RWND,
                seq,
                ack,
            };
            let packet = header.encode().to_vec();

            let (ack_tx, ack_rx) = oneshot::channel();
            inner.pending_writes.push(super::stream::PendingWrite {
                ack_threshold: seq + 1,
                tx: Some(ack_tx),
            });

            inner.queue_for_send(vec![(seq, packet)], addr);
            drop(inner);

            self.fin_queued = true;
            self.pending_ack = Some(ack_rx);
            return self.as_mut().poll_shutdown(cx);
        }
        Poll::Ready(Ok(()))
    }
}

impl Unpin for UdxAsyncStream {}

impl Drop for UdxAsyncStream {
    fn drop(&mut self) {
        let _ = self.processor.take();
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref tx) = guard.notify_tx {
            let _ = tx.send(super::stream::StreamNotify::Shutdown);
        }
    }
}
