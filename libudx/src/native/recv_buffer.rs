//! What a stream holds for its reader: the data that has arrived in order and
//! that the reader has not taken yet, in one buffer of bytes.
//!
//! The stream appends to it, under its own lock and then this one, and the
//! reader takes from the front, under this lock alone. Neither holds this
//! lock while it does anything else, so a reader that is slow to come back
//! never holds up what arrives.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use tokio::io::ReadBuf;
use tokio::sync::watch;

/// What is done with the buffer's capacity once it is empty: a buffer that
/// grew past this for a burst lets the memory go.
const KEPT_CAPACITY: usize = 256 * 1024;

/// What a reader that takes packets is handed next.
#[derive(Debug, PartialEq)]
pub(crate) enum Frame {
    /// The data of the next packet, as the remote sent it.
    Data(Vec<u8>),
    /// The end of the stream: nothing follows.
    End,
}

/// What a read into a caller's buffer came to.
#[derive(Debug, PartialEq)]
pub(crate) enum ReadOutcome {
    /// This many bytes were read.
    Read(usize),
    /// The stream has ended, and everything before the end has been read.
    End,
}

/// The data a stream holds for its reader, and whether more can follow.
pub(crate) struct RecvBuffer {
    /// What the reader has not taken yet starts at `start`.
    bytes: Vec<u8>,
    start: usize,
    /// The length of each packet in `bytes`, for a reader that takes a
    /// packet at a time ([`crate::UdxStream::read`]). `None` for a reader
    /// of bytes ([`crate::UdxAsyncStream`]).
    frames: Option<VecDeque<u16>>,
    /// Nothing more is appended: the remote ended the stream, or the stream
    /// was closed. The reader is handed the end once it has taken the rest.
    ended: bool,
    /// The reader has gone: what arrives is let go, and nothing is held.
    reader_gone: bool,
    /// The stream has a limit on what it holds, so the reader tells it of
    /// what it takes.
    limited: bool,
    /// The reader waiting for data.
    waker: Option<Waker>,
    /// Whether the buffer holds data for its reader, for whoever waits until
    /// it holds none (`UdxStreamHandle::released`). Sent only when it
    /// changes.
    holds_tx: watch::Sender<bool>,
}

impl RecvBuffer {
    /// A buffer for a reader that takes a packet at a time, as
    /// [`crate::UdxStream::read`] does until the stream becomes a
    /// [`crate::UdxAsyncStream`].
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            start: 0,
            frames: Some(VecDeque::new()),
            ended: false,
            reader_gone: false,
            limited: false,
            waker: None,
            holds_tx: watch::Sender::new(false),
        }
    }

    /// Told whether the buffer holds data for its reader.
    pub(crate) fn holds(&self) -> watch::Receiver<bool> {
        self.holds_tx.subscribe()
    }

    fn set_holds(&self, holds: bool) {
        if *self.holds_tx.borrow() != holds {
            self.holds_tx.send_replace(holds);
        }
    }

    pub(crate) fn shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new()))
    }

    /// Bytes held for the reader.
    pub(crate) fn len(&self) -> usize {
        self.bytes.len() - self.start
    }

    /// Hand `data`, the payload of the next packet in order, to the reader.
    /// A packet without data hands nothing over.
    pub(crate) fn push(&mut self, data: &[u8]) {
        if data.is_empty() || self.ended || self.reader_gone {
            return;
        }
        if let Some(frames) = self.frames.as_mut() {
            // A UDP datagram carries less than 64 KiB.
            frames.push_back(u16::try_from(data.len()).unwrap_or(u16::MAX));
        }
        self.bytes.extend_from_slice(data);
        self.set_holds(true);
        self.wake();
    }

    /// Nothing more follows what has been handed over.
    pub(crate) fn end(&mut self) {
        self.ended = true;
        self.free_if_done();
        self.wake();
    }

    /// Let go of what is held for the reader, unread: the stream was
    /// destroyed.
    pub(crate) fn discard(&mut self) {
        self.bytes = Vec::new();
        self.start = 0;
        if let Some(frames) = self.frames.as_mut() {
            *frames = VecDeque::new();
        }
        self.set_holds(false);
    }

    /// Nothing more is held or will be: let go of the memory as well.
    fn free_if_done(&mut self) {
        if self.ended && self.len() == 0 {
            self.bytes = Vec::new();
            self.start = 0;
        }
    }

    pub(crate) fn set_limited(&mut self, limited: bool) {
        self.limited = limited;
    }

    #[cfg(test)]
    pub(crate) fn reader_gone(&self) -> bool {
        self.reader_gone
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Take `len` bytes off the front.
    fn consume(&mut self, len: usize) {
        self.start += len;
        if self.start == self.bytes.len() {
            self.bytes.clear();
            self.start = 0;
            self.set_holds(false);
            self.free_if_done();
            if self.bytes.capacity() > KEPT_CAPACITY {
                self.bytes.shrink_to(KEPT_CAPACITY);
            }
        } else if self.start > self.bytes.len() / 2 {
            // Move what is left to the front, which costs no more than
            // what was taken to make the room.
            self.bytes.copy_within(self.start.., 0);
            self.bytes.truncate(self.bytes.len() - self.start);
            self.start = 0;
        }
    }

    /// The next packet's data, or the end of the stream. `Pending` while
    /// neither has arrived; the reader is then woken once one has.
    pub(crate) fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Frame> {
        match self.try_frame() {
            Some(frame) => Poll::Ready(frame),
            None => {
                self.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// As [`poll_frame`](Self::poll_frame), without waiting.
    pub(crate) fn try_frame(&mut self) -> Option<Frame> {
        if self.len() > 0 {
            let len = match self.frames.as_mut() {
                Some(frames) => frames.pop_front().map_or(self.len(), usize::from),
                None => self.len(),
            };
            let data = self.bytes[self.start..self.start + len].to_vec();
            self.consume(len);
            return Some(Frame::Data(data));
        }
        self.ended.then_some(Frame::End)
    }

    /// Copy what fits of the data held into `buf`.
    pub(crate) fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<ReadOutcome> {
        if self.len() > 0 {
            let len = self.len().min(buf.remaining());
            buf.put_slice(&self.bytes[self.start..self.start + len]);
            self.consume(len);
            return Poll::Ready(ReadOutcome::Read(len));
        }
        if self.ended {
            return Poll::Ready(ReadOutcome::End);
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// The reader's end of a [`RecvBuffer`]. Dropping it lets go of what the
/// stream holds for the reader.
pub(crate) struct RecvReader {
    buffer: Arc<Mutex<RecvBuffer>>,
    /// The end has been handed over by `try_recv`, which hands it over
    /// once, as a channel would.
    #[cfg(test)]
    end_taken: bool,
}

impl RecvReader {
    pub(crate) fn new(buffer: Arc<Mutex<RecvBuffer>>) -> Self {
        Self {
            buffer,
            #[cfg(test)]
            end_taken: false,
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, RecvBuffer> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// From now on the reader takes bytes, not packets.
    pub(crate) fn read_bytes(&self) {
        self.lock().frames = None;
    }

    /// Whether the stream is to be told of what the reader takes.
    pub(crate) fn limited(&self) -> bool {
        self.lock().limited
    }

    /// The next packet's data, or the end once, if either has arrived.
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<Frame, ()> {
        if self.end_taken {
            return Err(());
        }
        let frame = self.lock().try_frame().ok_or(())?;
        self.end_taken = frame == Frame::End;
        Ok(frame)
    }
}

impl Drop for RecvReader {
    fn drop(&mut self) {
        let mut buffer = self.lock();
        buffer.reader_gone = true;
        buffer.bytes = Vec::new();
        buffer.start = 0;
        buffer.set_holds(false);
        if let Some(frames) = buffer.frames.as_mut() {
            *frames = VecDeque::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the reader is handed, up to and including the end.
    fn frames(buffer: &mut RecvBuffer) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(frame) = buffer.try_frame() {
            let end = frame == Frame::End;
            frames.push(frame);
            if end {
                break;
            }
        }
        frames
    }

    #[test]
    fn a_reader_of_packets_takes_them_one_at_a_time_then_the_end() {
        let mut buffer = RecvBuffer::new();
        buffer.push(b"one");
        buffer.push(b"");
        buffer.push(b"three");
        assert_eq!(buffer.len(), 8);
        buffer.end();
        buffer.push(b"after the end");
        assert_eq!(
            frames(&mut buffer),
            [Frame::Data(b"one".to_vec()), Frame::Data(b"three".to_vec()), Frame::End]
        );
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn a_reader_of_bytes_takes_what_fits_across_packets() {
        let mut buffer = RecvBuffer::new();
        buffer.frames = None;
        buffer.push(b"hello ");
        buffer.push(b"world");
        let mut cx = Context::from_waker(Waker::noop());
        let mut space = [0u8; 8];
        let mut buf = ReadBuf::new(&mut space);
        assert_eq!(buffer.poll_read(&mut cx, &mut buf), Poll::Ready(ReadOutcome::Read(8)));
        assert_eq!(buf.filled(), b"hello wo");
        assert_eq!(buffer.len(), 3);
        let mut space = [0u8; 8];
        let mut buf = ReadBuf::new(&mut space);
        assert_eq!(buffer.poll_read(&mut cx, &mut buf), Poll::Ready(ReadOutcome::Read(3)));
        assert_eq!(buf.filled(), b"rld");
        assert!(buffer.poll_read(&mut cx, &mut buf).is_pending());
        buffer.end();
        assert_eq!(buffer.poll_read(&mut cx, &mut buf), Poll::Ready(ReadOutcome::End));
    }

    #[test]
    fn a_buffer_ended_and_read_holds_no_memory() {
        let mut buffer = RecvBuffer::new();
        buffer.frames = None;
        buffer.push(&[7; 1000]);
        buffer.end();
        assert_eq!(frames(&mut buffer).len(), 2);
        assert_eq!(buffer.bytes.capacity(), 0);
        assert!(!*buffer.holds().borrow());
    }

    #[test]
    fn what_was_taken_does_not_stay_in_memory() {
        let mut buffer = RecvBuffer::new();
        let packet = vec![7u8; 1_000];
        for _ in 0..1_000 {
            buffer.push(&packet);
            buffer.push(&packet);
            let Some(Frame::Data(_)) = buffer.try_frame() else { panic!() };
        }
        // A thousand packets are held, and the buffer is no larger than
        // twice that, however many went through it.
        assert_eq!(buffer.len(), 1_000 * 1_000);
        assert!(buffer.bytes.len() <= 2 * buffer.len());
    }

    #[test]
    fn a_reader_that_goes_lets_go_of_what_is_held_and_of_what_arrives() {
        let buffer = RecvBuffer::shared();
        let reader = RecvReader::new(Arc::clone(&buffer));
        buffer.lock().unwrap().push(&[1u8; 100]);
        drop(reader);
        let mut buffer = buffer.lock().unwrap();
        assert_eq!(buffer.len(), 0);
        buffer.push(&[1u8; 100]);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.reader_gone());
    }
}
