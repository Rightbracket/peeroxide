use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, oneshot};

use crate::error::{Result as UdxResult, UdxError};
use super::stream::{StreamEvent, StreamInner, Unread};

/// Adapter that implements [`tokio::io::AsyncRead`] and [`tokio::io::AsyncWrite`]
/// over a [`super::stream::UdxStream`].
///
/// Created via [`super::stream::UdxStream::into_async_stream`]. Higher layers
/// (e.g. SecretStream) wrap this for encrypted I/O.
///
/// # Writing
///
/// Writing works as it does on a TCP socket. A write is taken into the
/// stream's send buffer and returns: it does not wait for the remote to
/// acknowledge it. Once more has been written and not acknowledged than the
/// stream allows (262,144 bytes on top of its send window, as in C libudx),
/// a write waits until enough has been acknowledged.
///
/// `flush` has nothing to wait for. `shutdown` sends the end of the stream
/// after everything written, and returns once the remote has acknowledged
/// it: that is how to know that everything arrived.
///
/// A stream dropped without `shutdown` goes on delivering what was written,
/// in the background, for as long as the tokio runtime runs and the remote
/// keeps acknowledging. A program that exits right after its last write
/// should await `shutdown` first.
pub struct UdxAsyncStream {
    inner: Arc<Mutex<StreamInner>>,
    read_rx: mpsc::UnboundedReceiver<StreamEvent>,
    /// The count of what the reader has not taken yet.
    unread: Arc<Unread>,
    read_buf: Vec<u8>,
    read_pos: usize,
    read_eof: bool,
    /// Told once the remote has acknowledged the end of the stream.
    end_acked: Option<oneshot::Receiver<UdxResult<()>>>,
    processor: Option<tokio::task::JoinHandle<()>>,
    /// Whether the end of the stream has been queued.
    fin_queued: bool,
}

impl UdxAsyncStream {
    pub(crate) fn new(
        inner: Arc<Mutex<StreamInner>>,
        read_rx: mpsc::UnboundedReceiver<StreamEvent>,
        processor: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        let unread = inner.lock().unwrap_or_else(|e| e.into_inner()).unread();
        Self {
            inner,
            read_rx,
            unread,
            read_buf: Vec::new(),
            read_pos: 0,
            read_eof: false,
            end_acked: None,
            processor,
            fin_queued: false,
        }
    }

    /// The size of the largest IP packet the stream sends at present, in
    /// bytes. See [`UdxStream::effective_mtu`](crate::UdxStream::effective_mtu).
    pub fn effective_mtu(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).mtu
    }

    /// Limit what the stream holds for its reader, in bytes, or lift the
    /// limit with `None`. See
    /// [`UdxStream::set_read_buffer_limit`](crate::UdxStream::set_read_buffer_limit).
    ///
    /// What a read leaves of a packet stays with the adapter and is not
    /// counted: less than one packet.
    pub fn set_read_buffer_limit(&self, limit: Option<u32>) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).set_read_limit(limit);
    }

    /// The limit on what the stream holds for its reader, in bytes, if it
    /// has one.
    pub fn read_buffer_limit(&self) -> Option<u32> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).read_limit()
    }
}

/// The error an I/O trait reports for a stream's error. An I/O error keeps
/// its kind.
fn into_io(error: UdxError) -> io::Error {
    match error {
        UdxError::Io(error) => error,
        UdxError::StreamClosed => io::Error::new(io::ErrorKind::BrokenPipe, "UDX stream closed"),
        other => io::Error::other(other.to_string()),
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
                if self.unread.taken(data.len()) {
                    self.inner.lock().unwrap_or_else(|e| e.into_inner()).reader_took();
                }
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
                // A stream that gave up on its remote ends with that error,
                // not as if the remote had ended it.
                let failure = self.inner.lock().unwrap_or_else(|e| e.into_inner()).failure();
                match failure {
                    Some(error) => Poll::Ready(Err(error)),
                    None => Poll::Ready(Ok(())),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UdxAsyncStream {
    /// Takes `buf`, or the first 262,144 bytes of it, into the stream's send
    /// buffer. Waits only while more is unacknowledged than the stream
    /// allows.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.write_some(buf, cx.waker()) {
            Ok(Some(len)) => Poll::Ready(Ok(len)),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(into_io(error))),
        }
    }

    /// Returns at once: what was written is already on its way. Reports the
    /// error of a stream that could not deliver what was written.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match inner.write_error() {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Ready(Ok(())),
        }
    }

    /// Sends the end of the stream after everything written, and returns
    /// once the remote has acknowledged it, and with it all that was
    /// written.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.fin_queued {
            let queued = {
                let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(error) = inner.write_error() {
                    return Poll::Ready(Err(error));
                }
                // Nothing written, and nowhere to send the end: done. A
                // stream waiting on its firewall hook with writes queued
                // sends the end after them once the hook lets a remote in.
                if !inner.connected && (!inner.awaiting_firewall() || inner.drained()) {
                    return Poll::Ready(Ok(()));
                }
                inner.queue_end_awaiting_firewall()
            };
            match queued {
                Ok(acked) => self.end_acked = Some(acked),
                // Closed already, with nothing left to deliver.
                Err(_) => return Poll::Ready(Ok(())),
            }
            self.fin_queued = true;
        }

        let Some(acked) = self.end_acked.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match Pin::new(acked).poll(cx) {
            Poll::Ready(result) => {
                self.end_acked = None;
                match result {
                    Ok(Ok(())) => Poll::Ready(Ok(())),
                    Ok(Err(error)) => Poll::Ready(Err(into_io(error))),
                    Err(_) => Poll::Ready(Err(into_io(UdxError::StreamClosed))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Unpin for UdxAsyncStream {}

impl Drop for UdxAsyncStream {
    /// Lets go of the stream. Its processor task goes on until what was
    /// written has been acknowledged, or cannot be delivered.
    fn drop(&mut self) {
        let _ = self.processor.take();
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ref tx) = guard.notify_tx {
            let _ = tx.send(super::stream::StreamNotify::Shutdown);
        }
    }
}
