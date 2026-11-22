//! Protomux — Protocol multiplexer over a framed stream.
//!
//! Wire-compatible with Node.js `protomux@3.10.1` (holepunchto/protomux).
//!
//! Multiplexes multiple named protocol channels over a single stream. Each
//! channel supports multiple message types.
//!
//! # Wire Format
//!
//! All frames use compact-encoding. The first decoded `uint` is the channel ID:
//!
//! - **Channel ID 0** → control message. Second `uint` is the control type:
//!   - `0` = batch
//!   - `1` = open channel
//!   - `2` = reject channel
//!   - `3` = close channel
//! - **Channel ID > 0** → data message on that channel. Second `uint` is the
//!   message type index, followed by the encoded payload.
//!
//! ## Control: Open (type 1)
//! ```text
//! [uint:0][uint:1][uint:localId][string:protocol][buffer:id][handshake?]
//! ```
//!
//! ## Control: Close (type 3)
//! ```text
//! [uint:0][uint:3][uint:localId]
//! ```
//!
//! ## Control: Reject (type 2)
//! ```text
//! [uint:0][uint:2][uint:remoteId]
//! ```
//!
//! ## Data message (non-batched)
//! ```text
//! [uint:channelId][uint:messageType][payload]
//! ```
//!
//! ## Control: Batch (type 0)
//! ```text
//! [uint:0][uint:0][uint:firstChannelId]
//!   [buffer:msg1][buffer:msg2]...
//!   [0x00][uint:nextChannelId]
//!   [buffer:msg3]...
//! ```
//! Inside each batch buffer: `[uint:messageType][payload]`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, trace, warn};

use crate::compact_encoding::{self as c, State};

// ── Constants ────────────────────────────────────────────────────────────────

const CTRL_BATCH: u64 = 0;
const CTRL_OPEN: u64 = 1;
const CTRL_REJECT: u64 = 2;
const CTRL_CLOSE: u64 = 3;

/// Maximum size of a single batch frame (8 MiB, matching Node.js).
#[allow(dead_code)]
const MAX_BATCH: usize = 8 * 1024 * 1024;

/// How long a channel the remote opened waits for a local channel to pair
/// with it before it is rejected. Node rejects it once the tick it arrived in
/// is over; a mux that reads on its own task gives the local side a moment.
const UNCLAIMED_OPEN_GRACE: Duration = Duration::from_secs(1);

/// How many channels the remote opened may wait for a pair at once; an open
/// past that is rejected at once.
const MAX_UNCLAIMED_OPENS: usize = 64;

/// How many bytes of messages for channels not yet paired the mux holds
/// before it stops reading, as Node's `MAX_BUFFERED`.
const MAX_BUFFERED: usize = 32 * 1024;

/// What a held message costs besides its payload.
const PENDING_MESSAGE_OVERHEAD: usize = 32;

/// What an open waiting for a pair costs besides its protocol, id and
/// handshake.
const OPEN_OVERHEAD: usize = 128;

/// How many bytes of notices a [`PairListener`] that does not keep up may
/// have queued.
const MAX_QUEUED_NOTICES: usize = 64 * 1024;

/// What a notice costs besides its protocol and id.
const NOTICE_OVERHEAD: usize = 64;

/// How many bytes of replies (rejects) the mux may owe a remote that is not
/// reading them before it ends.
const MAX_UNSENT_REPLIES: usize = 64 * 1024;

// ── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
/// Errors returned by the Protomux encoder, decoder, and channel runtime.
#[non_exhaustive]
pub enum ProtomuxError {
    /// Encoding failed while serializing a frame.
    #[error("encoding error: {0}")]
    Encoding(#[from] c::EncodingError),

    /// The underlying framed stream was closed.
    #[error("stream closed")]
    StreamClosed,

    /// The incoming frame was malformed.
    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    /// The channel was already closed.
    #[error("channel closed")]
    ChannelClosed,

    /// No local channel exists for the requested ID.
    #[error("channel not found: local_id={0}")]
    ChannelNotFound(u64),

    /// An internal mux invariant was violated.
    #[error("internal error: {0}")]
    Internal(String),

    /// [`Mux::accept`] was given an open that is not the next to be paired
    /// for its protocol and id: an earlier one waits, or the request is of
    /// another mux.
    #[error("not the next open to accept")]
    NotNextOpen,
}

/// Result type used by Protomux operations.
pub type Result<T> = std::result::Result<T, ProtomuxError>;

// ── Frame encoding ───────────────────────────────────────────────────────────

/// Encode a channel Open control frame.
///
/// Wire: `[uint:0][uint:1][uint:localId][string:protocol][buffer:id][handshake?]`
pub fn encode_open(local_id: u64, protocol: &str, id: Option<&[u8]>, handshake: Option<&[u8]>) -> Vec<u8> {
    let mut state = State::new();
    // Preencode
    state.end = 2; // header: [0, 1]
    c::preencode_uint(&mut state, local_id);
    c::preencode_string(&mut state, protocol);
    c::preencode_buffer(&mut state, id);
    if let Some(hs) = handshake {
        state.end += hs.len();
    }
    state.alloc();
    // Encode
    state.buffer[0] = 0;
    state.buffer[1] = 1;
    state.start = 2;
    c::encode_uint(&mut state, local_id);
    c::encode_string(&mut state, protocol);
    c::encode_buffer(&mut state, id);
    if let Some(hs) = handshake {
        state.buffer[state.start..state.start + hs.len()].copy_from_slice(hs);
        state.start += hs.len();
    }
    state.buffer
}

/// Encode a channel Close control frame.
///
/// Wire: `[uint:0][uint:3][uint:localId]`
pub fn encode_close(local_id: u64) -> Vec<u8> {
    let mut state = State::new();
    state.end = 2;
    c::preencode_uint(&mut state, local_id);
    state.alloc();
    state.buffer[0] = 0;
    state.buffer[1] = 3;
    state.start = 2;
    c::encode_uint(&mut state, local_id);
    state.buffer
}

/// Encode a channel Reject control frame.
///
/// Wire: `[uint:0][uint:2][uint:remoteId]`
pub fn encode_reject(remote_id: u64) -> Vec<u8> {
    let mut state = State::new();
    state.end = 2;
    c::preencode_uint(&mut state, remote_id);
    state.alloc();
    state.buffer[0] = 0;
    state.buffer[1] = 2;
    state.start = 2;
    c::encode_uint(&mut state, remote_id);
    state.buffer
}

/// Encode a data message (non-batched).
///
/// Wire: `[uint:channelId][uint:messageType][payload]`
pub fn encode_message(channel_id: u64, message_type: u64, payload: &[u8]) -> Vec<u8> {
    let mut state = State::new();
    c::preencode_uint(&mut state, channel_id);
    c::preencode_uint(&mut state, message_type);
    state.end += payload.len();
    state.alloc();
    c::encode_uint(&mut state, channel_id);
    c::encode_uint(&mut state, message_type);
    state.buffer[state.start..state.start + payload.len()].copy_from_slice(payload);
    state.start += payload.len();
    state.buffer
}

/// Encode a batch frame from a list of (channelId, inner_buffer) pairs.
///
/// `inner_buffer` is `[uint:messageType][payload]` (already encoded).
///
/// Wire: `[uint:0][uint:0][uint:firstChannelId][buffer:msg]...[0x00][uint:nextId][buffer:msg]...`
pub fn encode_batch(entries: &[(u64, Vec<u8>)]) -> Vec<u8> {
    if entries.is_empty() {
        return Vec::new();
    }

    let mut state = State::new();
    // Header: 2 bytes (0, 0)
    state.end = 2;
    // First channel ID
    c::preencode_uint(&mut state, entries[0].0);

    let mut prev_id = entries[0].0;
    for (channel_id, buf) in entries {
        if *channel_id != prev_id {
            state.end += 1; // 0x00 separator
            c::preencode_uint(&mut state, *channel_id);
            prev_id = *channel_id;
        }
        c::preencode_buffer(&mut state, Some(buf));
    }

    state.alloc();
    state.buffer[0] = 0;
    state.buffer[1] = 0;
    state.start = 2;

    prev_id = entries[0].0;
    c::encode_uint(&mut state, prev_id);

    for (channel_id, buf) in entries {
        if *channel_id != prev_id {
            state.buffer[state.start] = 0;
            state.start += 1;
            c::encode_uint(&mut state, *channel_id);
            prev_id = *channel_id;
        }
        c::encode_buffer(&mut state, Some(buf));
    }

    state.buffer
}

// ── Frame decoding ───────────────────────────────────────────────────────────

/// Decoded control frame.
#[derive(Debug, Clone, PartialEq)]
pub enum ControlFrame {
    /// Opened channel metadata from the remote peer.
    Open {
        /// Remote local channel ID.
        local_id: u64,
        /// Protocol name.
        protocol: String,
        /// Optional sub-channel identifier.
        id: Option<Vec<u8>>,
        /// Optional handshake payload.
        handshake_state: Option<Vec<u8>>,
    },
    /// Remote requested channel closure.
    Close {
        /// Remote local channel ID.
        local_id: u64,
    },
    /// Remote rejected the channel.
    Reject {
        /// Remote channel ID.
        remote_id: u64,
    },
}

/// A single item from a decoded batch.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchItem {
    /// Channel ID that the batched message belongs to.
    pub channel_id: u64,
    /// Raw inner batch payload.
    pub data: Vec<u8>,
}

/// Decoded frame from the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedFrame {
    /// A control message (open/close/reject).
    Control(ControlFrame),
    /// A batch of channel messages.
    Batch(Vec<BatchItem>),
    /// A single data message on a channel.
    Message {
        /// Channel ID.
        channel_id: u64,
        /// Message type index.
        message_type: u64,
        /// Message payload.
        payload: Vec<u8>,
    },
}

/// Decode a raw frame from the wire.
pub fn decode_frame(data: &[u8]) -> Result<DecodedFrame> {
    if data.is_empty() {
        return Err(ProtomuxError::InvalidFrame("empty frame".into()));
    }

    let mut state = State::from_buffer(data);
    let channel_id = c::decode_uint(&mut state)?;

    if channel_id == 0 {
        // Control message
        let ctrl_type = c::decode_uint(&mut state)?;
        match ctrl_type {
            CTRL_OPEN => {
                let local_id = c::decode_uint(&mut state)?;
                let protocol = c::decode_string(&mut state)?;
                let id = c::decode_buffer(&mut state)?;
                // Remaining bytes are handshake data (if any)
                let handshake_state = if state.start < state.end {
                    Some(state.buffer[state.start..state.end].to_vec())
                } else {
                    None
                };
                Ok(DecodedFrame::Control(ControlFrame::Open {
                    local_id,
                    protocol,
                    id,
                    handshake_state,
                }))
            }
            CTRL_CLOSE => {
                let local_id = c::decode_uint(&mut state)?;
                Ok(DecodedFrame::Control(ControlFrame::Close { local_id }))
            }
            CTRL_REJECT => {
                let remote_id = c::decode_uint(&mut state)?;
                Ok(DecodedFrame::Control(ControlFrame::Reject { remote_id }))
            }
            CTRL_BATCH => {
                let mut items = Vec::new();
                let mut current_channel = c::decode_uint(&mut state)?;
                let end = state.end;

                while state.start < end {
                    let len = usize::try_from(c::decode_uint(&mut state)?).unwrap_or(usize::MAX);
                    if len == 0 {
                        // Channel switch
                        current_channel = c::decode_uint(&mut state)?;
                        continue;
                    }
                    if len > end.saturating_sub(state.start) {
                        return Err(ProtomuxError::InvalidFrame(
                            "batch item overflows frame".into(),
                        ));
                    }
                    let data = state.buffer[state.start..state.start + len].to_vec();
                    state.start += len;
                    items.push(BatchItem {
                        channel_id: current_channel,
                        data,
                    });
                }

                Ok(DecodedFrame::Batch(items))
            }
            other => Err(ProtomuxError::InvalidFrame(format!(
                "unknown control type: {other}"
            ))),
        }
    } else {
        // Data message on a channel
        let message_type = c::decode_uint(&mut state)?;
        let payload = state.buffer[state.start..state.end].to_vec();
        Ok(DecodedFrame::Message {
            channel_id,
            message_type,
            payload,
        })
    }
}

// ── FramedStream trait ───────────────────────────────────────────────────────

/// A stream that delivers and accepts complete frames.
///
/// The underlying transport (e.g. secretstream) handles framing. Each
/// `read_frame` call returns one complete Protomux frame.
pub trait FramedStream: Send + 'static {
    /// Read the next complete frame. Returns `None` on EOF.
    ///
    /// Over a stream that cannot be split, the mux drops this future before
    /// it completes when it has something else to do first (a command from a
    /// handle, or an open to reject), and reads again later: the read must
    /// then lose nothing of the frame, as reading from a channel does not.
    fn read_frame(
        &mut self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<Vec<u8>>>> + Send;

    /// Write a complete frame.
    fn write_frame(
        &mut self,
        data: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    /// The stream as halves that can be read and written at the same time,
    /// or the stream itself if it cannot be split. By default it cannot.
    ///
    /// A [`Mux`] over a stream that can be split goes on reading while a
    /// write waits for the remote to make room. Over one that cannot, it
    /// reads nothing until a write is done, so two ends that both write
    /// more than the transport between them holds wait on each other for
    /// good. A [`SecretStream`](crate::secret_stream::SecretStream) can be
    /// split.
    fn into_framed_halves(self) -> std::result::Result<FramedHalves, Self>
    where
        Self: Sized,
    {
        Err(self)
    }
}

/// The read half of a framed stream. See [`FramedHalves`].
pub(crate) trait FramedRead: Send + 'static {
    /// Read the next complete frame. Returns `None` on EOF.
    fn read_frame(
        &mut self,
    ) -> impl std::future::Future<Output = std::io::Result<Option<Vec<u8>>>> + Send;
}

/// The write half of a framed stream. See [`FramedHalves`].
pub(crate) trait FramedWrite: Send + 'static {
    /// Write a complete frame.
    fn write_frame(
        &mut self,
        data: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
}

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// [`FramedRead`] as a trait object.
trait DynFramedRead: Send {
    fn read_frame_boxed(&mut self) -> BoxFuture<'_, std::io::Result<Option<Vec<u8>>>>;
}

impl<R: FramedRead> DynFramedRead for R {
    fn read_frame_boxed(&mut self) -> BoxFuture<'_, std::io::Result<Option<Vec<u8>>>> {
        Box::pin(self.read_frame())
    }
}

/// [`FramedWrite`] as a trait object.
trait DynFramedWrite: Send {
    fn write_frame_boxed<'a>(&'a mut self, data: &'a [u8]) -> BoxFuture<'a, std::io::Result<()>>;
}

impl<W: FramedWrite> DynFramedWrite for W {
    fn write_frame_boxed<'a>(&'a mut self, data: &'a [u8]) -> BoxFuture<'a, std::io::Result<()>> {
        Box::pin(self.write_frame(data))
    }
}

/// The halves of a [`FramedStream`] that can be read and written at the same
/// time: what [`FramedStream::into_framed_halves`] returns for a stream that
/// can be split.
pub struct FramedHalves {
    read: Box<dyn DynFramedRead>,
    write: Box<dyn DynFramedWrite>,
}

impl FramedHalves {
    pub(crate) fn new(read: impl FramedRead, write: impl FramedWrite) -> Self {
        Self { read: Box::new(read), write: Box::new(write) }
    }
}

impl std::fmt::Debug for FramedHalves {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedHalves").finish_non_exhaustive()
    }
}

// ── Channel events & commands ────────────────────────────────────────────────

/// Events dispatched to individual channel handles.
#[derive(Debug)]
#[non_exhaustive]
pub enum ChannelEvent {
    /// Remote opened the channel (with optional handshake data).
    Opened {
        /// Optional handshake payload from the remote peer.
        handshake: Option<Vec<u8>>,
    },
    /// Received a message on this channel.
    Message {
        /// Message type index.
        message_type: u32,
        /// Message payload.
        data: Vec<u8>,
    },
    /// Channel was closed.
    Closed {
        /// `true` if the remote peer initiated the close.
        is_remote: bool,
    },
}

/// Internal commands from handles to the Protomux actor.
enum MuxCommand {
    CreateChannel {
        protocol: String,
        id: Option<Vec<u8>>,
        handshake: Option<Vec<u8>>,
        /// The remote's open to pair with, from a [`PairRequest`], and
        /// nothing else.
        claim: Option<OpenRef>,
        response: oneshot::Sender<Result<ChannelHandle>>,
    },
    SendMessage {
        local_id: u32,
        generation: u64,
        message_type: u32,
        data: Vec<u8>,
    },
    CloseChannel {
        local_id: u32,
        generation: u64,
    },
    Pair {
        key: String,
        pairing: Pairing,
    },
    Unpair {
        key: String,
        registration: u64,
    },
    Cork,
    Uncork,
}

/// Raw channel handle returned from create — internal.
struct ChannelHandle {
    local_id: u32,
    /// Which use of `local_id` this is: a handle of an earlier channel that
    /// had the id is not let reach a later one.
    generation: u64,
    event_rx: mpsc::UnboundedReceiver<ChannelEvent>,
}

// ── Channel ──────────────────────────────────────────────────────────────────

/// Handle for a single Protomux channel.
///
/// Channels are created via [`Mux::create_channel`] and support sending
/// and receiving typed messages.
#[non_exhaustive]
pub struct Channel {
    local_id: u32,
    generation: u64,
    protocol: String,
    id: Option<Vec<u8>>,
    cmd_tx: mpsc::UnboundedSender<MuxCommand>,
    event_rx: mpsc::UnboundedReceiver<ChannelEvent>,
    opened: bool,
    closed: bool,
}

impl Channel {
    /// The local channel ID (1-indexed).
    pub fn local_id(&self) -> u32 {
        self.local_id
    }

    /// The protocol name for this channel.
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// The optional sub-channel identifier.
    pub fn id(&self) -> Option<&[u8]> {
        self.id.as_deref()
    }

    /// Whether the channel has been fully opened (both sides exchanged Open).
    pub fn is_opened(&self) -> bool {
        self.opened
    }

    /// Send a typed message on this channel.
    ///
    /// `message_type` is the 0-based index into the channel's message list.
    /// `data` is the already-encoded message payload. Once the channel has
    /// been closed, by either side, what is sent on it is dropped.
    pub fn send(&self, message_type: u32, data: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ProtomuxError::ChannelClosed);
        }
        self.cmd_tx
            .send(MuxCommand::SendMessage {
                local_id: self.local_id,
                generation: self.generation,
                message_type,
                data: data.to_vec(),
            })
            .map_err(|_| ProtomuxError::StreamClosed)
    }

    /// Receive the next event on this channel.
    ///
    /// Returns `None` when the channel or stream has been closed.
    pub async fn recv(&mut self) -> Option<ChannelEvent> {
        self.event_rx.recv().await
    }

    /// Wait until the channel is fully opened (both sides exchanged Open).
    ///
    /// Returns the remote handshake data if any.
    pub async fn wait_opened(&mut self) -> Result<Option<Vec<u8>>> {
        if self.opened {
            return Ok(None);
        }
        loop {
            match self.event_rx.recv().await {
                Some(ChannelEvent::Opened { handshake }) => {
                    self.opened = true;
                    return Ok(handshake);
                }
                Some(ChannelEvent::Closed { .. }) => {
                    self.closed = true;
                    return Err(ProtomuxError::ChannelClosed);
                }
                Some(ChannelEvent::Message { .. }) => {
                    // Messages arriving before fully opened — skip for now.
                    // Node.js buffers these; we can add buffering later.
                    trace!("message received before channel fully opened, dropping");
                }
                None => return Err(ProtomuxError::StreamClosed),
            }
        }
    }

    /// Close this channel.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        let _ = self.cmd_tx.send(MuxCommand::CloseChannel {
            local_id: self.local_id,
            generation: self.generation,
        });
    }
}

// ── Mux handle ───────────────────────────────────────────────────────────────

/// Handle for interacting with a running Protomux instance.
///
/// Create with [`Mux::new`], which returns this handle and a future to spawn.
#[non_exhaustive]
pub struct Mux {
    cmd_tx: mpsc::UnboundedSender<MuxCommand>,
}

impl Mux {
    /// Create a new Protomux over a framed stream.
    ///
    /// Returns the handle and a future that drives the multiplexer. The future
    /// must be spawned (e.g. via `tokio::spawn`), on a Tokio runtime with its
    /// timer enabled.
    ///
    /// Over a stream that can be split ([`FramedStream::into_framed_halves`]),
    /// as a [`SecretStream`](crate::secret_stream::SecretStream) can, the mux
    /// reads and writes at the same time. Over one that cannot, it reads
    /// nothing while a write waits.
    ///
    /// The mux handles what its handles ask of it before the frames it reads,
    /// so a channel created (or a [`Self::pair`] listener registered) as soon
    /// as the mux starts is ready when the remote's first open arrives.
    ///
    /// ```ignore
    /// let (mux, run) = Mux::new(stream);
    /// tokio::spawn(run);
    /// let channel = mux.create_channel("my-protocol", None, None).await?;
    /// ```
    pub fn new<S: FramedStream>(stream: S) -> (Self, impl std::future::Future<Output = ()> + Send) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let task = async move {
            match stream.into_framed_halves() {
                Ok(halves) => run_split_mux(halves, cmd_rx).await,
                Err(stream) => run_mux(stream, cmd_rx).await,
            }
        };
        (Self { cmd_tx }, task)
    }

    /// Create a new channel on this mux.
    ///
    /// The channel is immediately opened (Open frame sent). Use
    /// [`Channel::wait_opened`] to wait for the remote side to reciprocate.
    pub async fn create_channel(
        &self,
        protocol: &str,
        id: Option<Vec<u8>>,
        handshake: Option<Vec<u8>>,
    ) -> Result<Channel> {
        self.open_channel(protocol, id, handshake, None).await
    }

    /// Create the channel that pairs with the open a [`PairListener`] told
    /// of. Fails with [`ProtomuxError::ChannelClosed`], and opens nothing,
    /// if that open is no longer waiting: rejected for waiting too long, or
    /// closed by the remote. The remote pairs our answers with its opens of
    /// the same protocol and id in the order it sent them, so they are
    /// accepted in that order: an open with an earlier one of its protocol
    /// and id still waiting fails with [`ProtomuxError::NotNextOpen`], as
    /// does a request of another mux.
    pub async fn accept(&self, request: &PairRequest, handshake: Option<Vec<u8>>) -> Result<Channel> {
        self.open_channel(&request.protocol, request.id.clone(), handshake, Some(request.open))
            .await
    }

    async fn open_channel(
        &self,
        protocol: &str,
        id: Option<Vec<u8>>,
        handshake: Option<Vec<u8>>,
        claim: Option<OpenRef>,
    ) -> Result<Channel> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(MuxCommand::CreateChannel {
                protocol: protocol.to_string(),
                id: id.clone(),
                handshake,
                claim,
                response: resp_tx,
            })
            .map_err(|_| ProtomuxError::StreamClosed)?;

        let handle = resp_rx
            .await
            .map_err(|_| ProtomuxError::StreamClosed)??;

        Ok(Channel {
            local_id: handle.local_id,
            generation: handle.generation,
            protocol: protocol.to_string(),
            id,
            cmd_tx: self.cmd_tx.clone(),
            event_rx: handle.event_rx,
            opened: false,
            closed: false,
        })
    }

    /// Start batching writes.
    pub fn cork(&self) {
        let _ = self.cmd_tx.send(MuxCommand::Cork);
    }

    /// Flush batched writes.
    pub fn uncork(&self) {
        let _ = self.cmd_tx.send(MuxCommand::Uncork);
    }

    /// Be told of channels the remote opens for `protocol`, as Node's
    /// `mux.pair()`: with `id` given, of those opened with that id; with
    /// `None`, of those opened with any id that no listener for their exact
    /// id is told of.
    ///
    /// A channel the remote opens is paired with a local channel of the same
    /// protocol and id: one already waiting, or one created (with
    /// [`Self::create_channel`]) within a second of the open, where Node
    /// waits for its `pair` callback to return. An open nothing pairs with in
    /// that time is rejected, and so is one that arrives while 64 wait. (A
    /// remote that leaves 64 KiB of rejects unread has the mux end.)
    ///
    /// The listener is told of each open that finds no local channel waiting,
    /// so that one can be created for it with [`Self::accept`], as long as it
    /// keeps up: an open
    /// whose notice would put more than 64 KiB of them in its queue is not
    /// told of, and waits out its second like any other.
    ///
    /// Registering again for the same protocol and id replaces the earlier
    /// listener. Dropping the listener stops it.
    pub fn pair(&self, protocol: &str, id: Option<Vec<u8>>) -> PairListener {
        static NEXT_REGISTRATION: AtomicU64 = AtomicU64::new(0);
        let registration = NEXT_REGISTRATION.fetch_add(1, Ordering::Relaxed);
        let key = MuxState::channel_key(protocol, id.as_deref());
        let (notify, requests) = mpsc::channel(MAX_UNCLAIMED_OPENS);
        let queued = Arc::new(AtomicUsize::new(0));
        let _ = self.cmd_tx.send(MuxCommand::Pair {
            key: key.clone(),
            pairing: Pairing {
                registration,
                notify,
                queued: Arc::clone(&queued),
            },
        });
        PairListener {
            requests,
            queued,
            cmd_tx: self.cmd_tx.clone(),
            key,
            registration,
        }
    }
}

/// A channel the remote opened that no local channel was waiting for; see
/// [`Mux::pair`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PairRequest {
    /// The protocol the remote opened the channel for.
    pub protocol: String,
    /// The id the remote opened the channel with.
    pub id: Option<Vec<u8>>,
    open: OpenRef,
}

/// Which of the remote's opens a [`PairRequest`] is of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenRef {
    /// The mux the open came to.
    mux: u64,
    remote_id: u32,
    /// Tells this open from a later one at the same id.
    seq: u64,
}

/// What a notice counts against a listener's queue.
fn notice_size(protocol: &str, id: Option<&[u8]>) -> usize {
    protocol.len() + id.map_or(0, <[u8]>::len) + NOTICE_OVERHEAD
}

/// Tells of the channels the remote opens for a protocol; made by
/// [`Mux::pair`]. Dropping it stops it.
pub struct PairListener {
    requests: mpsc::Receiver<PairRequest>,
    /// What the notices in `requests` count; see [`MAX_QUEUED_NOTICES`].
    queued: Arc<AtomicUsize>,
    cmd_tx: mpsc::UnboundedSender<MuxCommand>,
    key: String,
    registration: u64,
}

impl PairListener {
    /// The next channel the remote opened that no local channel was waiting
    /// for. Pair with it within a second with [`Mux::accept`]. Returns `None` once the mux has ended, or another
    /// listener has replaced this one.
    pub async fn recv(&mut self) -> Option<PairRequest> {
        let request = self.requests.recv().await?;
        self.queued
            .fetch_sub(notice_size(&request.protocol, request.id.as_deref()), Ordering::Relaxed);
        Some(request)
    }
}

impl Drop for PairListener {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(MuxCommand::Unpair {
            key: std::mem::take(&mut self.key),
            registration: self.registration,
        });
    }
}

impl std::fmt::Debug for PairListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairListener").field("key", &self.key).finish_non_exhaustive()
    }
}

impl Clone for Mux {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
        }
    }
}

// ── Mux actor state ──────────────────────────────────────────────────────────

struct LocalChannel {
    generation: u64,
    /// The remote's id for the channel, once it has answered our open.
    remote_id: Option<u32>,
    event_tx: mpsc::UnboundedSender<ChannelEvent>,
    opened: bool,
    closed: bool,
}

struct RemoteChannel {
    /// Handshake state buffer from the Open frame (consumed on pairing).
    handshake_state: Option<Vec<u8>>,
    /// Paired local channel's index (set when pairing completes).
    local_id: Option<u32>,
    /// Messages buffered before the channel was paired locally.
    pending: Vec<(u64, Vec<u8>)>,
    /// The `infos` key of the channel's protocol and id.
    key: String,
    /// When the channel, not yet paired, is rejected.
    deadline: Option<Instant>,
    /// What the channel, not yet paired, counts against [`MAX_BUFFERED`]:
    /// its open and the messages held for it.
    held: usize,
    /// Tells this open from a later one at the same id.
    seq: u64,
}

impl RemoteChannel {
    fn is_due(&self, deadline: Instant) -> bool {
        self.local_id.is_none() && self.deadline == Some(deadline)
    }
}

/// What a message held for a channel not yet paired counts against
/// [`MAX_BUFFERED`].
fn pending_size(payload: &[u8]) -> usize {
    payload.len() + PENDING_MESSAGE_OVERHEAD
}

struct ChannelInfo {
    /// Local channel IDs that sent Open and have not been answered yet; a
    /// channel closed before its answer stays, so that the answer is not
    /// taken for that of the next channel to use its id, as in Node.
    outgoing: Vec<u32>,
    /// Remote channel IDs received via Open that haven't been paired yet.
    incoming: Vec<u32>,
}

/// A [`Mux::pair`] listener, as the mux holds it.
struct Pairing {
    registration: u64,
    notify: mpsc::Sender<PairRequest>,
    queued: Arc<AtomicUsize>,
}

/// What is left of a batch the mux stopped reading part way through.
struct BatchRest {
    state: State,
    channel: u64,
}

/// A frame for the writer.
struct Outgoing {
    frame: Vec<u8>,
    /// Whether the frame answers the remote (a reject), and so counts
    /// against [`MAX_UNSENT_REPLIES`] until it is written.
    reply: bool,
}

type WriteQueue = VecDeque<Outgoing>;

struct MuxState {
    /// Tells this mux's [`PairRequest`]s from another's.
    mux_id: u64,
    local: Vec<Option<LocalChannel>>,
    free_ids: Vec<u32>,
    next_generation: u64,
    next_open_seq: u64,
    /// The remote's channels, by their id.
    remote: HashMap<u32, RemoteChannel>,
    /// The highest id the remote has opened a channel at: it may open the
    /// next one, or one at or below it that is free.
    remote_ids: u32,
    infos: HashMap<String, ChannelInfo>,
    /// Cork state: when >0, writes are batched.
    cork_depth: u32,
    /// Batched entries: (local_channel_id, inner_data).
    batch: Vec<(u64, Vec<u8>)>,
    /// The bytes of the items in `batch`.
    batch_bytes: usize,
    /// The remote channels not yet paired, by deadline. An entry whose
    /// channel has since been paired, closed or reopened is skipped.
    unclaimed: VecDeque<(Instant, u32)>,
    /// How many remote channels are not yet paired.
    unclaimed_count: usize,
    /// What the remote channels not yet paired hold.
    buffered: usize,
    /// The listeners of [`Mux::pair`], by `infos` key.
    pairings: HashMap<String, Pairing>,
    /// A batch the mux stopped part way through, to go on with first.
    batch_rest: Option<BatchRest>,
    /// The bytes of replies queued and not yet written.
    unsent_replies: Arc<AtomicUsize>,
}

impl MuxState {
    fn new() -> Self {
        static NEXT_MUX: AtomicU64 = AtomicU64::new(0);
        Self {
            mux_id: NEXT_MUX.fetch_add(1, Ordering::Relaxed),
            local: Vec::new(),
            free_ids: Vec::new(),
            next_generation: 0,
            next_open_seq: 0,
            remote: HashMap::new(),
            remote_ids: 0,
            infos: HashMap::new(),
            cork_depth: 0,
            batch: Vec::new(),
            batch_bytes: 0,
            unclaimed: VecDeque::new(),
            unclaimed_count: 0,
            buffered: 0,
            pairings: HashMap::new(),
            batch_rest: None,
            unsent_replies: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Allocate a local channel ID (0-indexed internal, 1-indexed on wire).
    fn alloc_local_id(&mut self) -> u32 {
        if let Some(id) = self.free_ids.pop() {
            id
        } else {
            let id = self.local.len() as u32;
            self.local.push(None);
            id
        }
    }

    /// Get the channel key for info lookup.
    fn channel_key(protocol: &str, id: Option<&[u8]>) -> String {
        match id {
            Some(id) => format!("{protocol}##{}", hex::encode(id)),
            None => format!("{protocol}##"),
        }
    }

    /// Whether the mux holds too much for channels not yet paired to read
    /// more, as Node's `_pauseMaybe`.
    fn paused(&self) -> bool {
        self.buffered > MAX_BUFFERED
    }

    /// Whether the mux reads its next frame: not while paused, nor while a
    /// batch it stopped part way through waits.
    fn reading(&self) -> bool {
        !self.paused() && self.batch_rest.is_none()
    }

    /// When the next remote channel not yet paired may be due to be rejected.
    fn next_deadline(&self) -> Option<Instant> {
        self.unclaimed.front().map(|(deadline, _)| *deadline)
    }

    /// Remove the `infos` entry at `key` if nothing waits in it, as Node's
    /// `_gc`.
    fn gc_info(&mut self, key: &str) {
        if self
            .infos
            .get(key)
            .is_some_and(|info| info.outgoing.is_empty() && info.incoming.is_empty())
        {
            self.infos.remove(key);
        }
    }

    /// Forget the remote channel `remote_id`, which is not yet paired,
    /// giving back what it holds.
    fn drop_unclaimed(&mut self, remote_id: u32) {
        let Some(rc) = self.remote.remove(&remote_id) else {
            return;
        };
        self.unclaimed_count -= 1;
        self.buffered -= rc.held;
        if let Some(info) = self.infos.get_mut(&rc.key) {
            info.incoming.retain(|&id| id != remote_id);
        }
        self.gc_info(&rc.key);
    }

    /// The local channel at `local_id` (on the wire), if it is the one of
    /// `generation`.
    fn local_of(&mut self, local_id: u32, generation: u64) -> Option<&mut LocalChannel> {
        let internal_id = local_id.checked_sub(1)? as usize;
        self.local
            .get_mut(internal_id)?
            .as_mut()
            .filter(|ch| ch.generation == generation)
    }
}

// ── Mux actor run loop ───────────────────────────────────────────────────────

/// Wait until `deadline`, or forever if there is none.
async fn sleep_until_due(timer: std::pin::Pin<&mut tokio::time::Sleep>, deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => {
            let mut timer = timer;
            timer.as_mut().reset(deadline);
            timer.await;
        }
        None => std::future::pending().await,
    }
}

/// Drive the Protomux. This future must be spawned.
async fn run_mux<S: FramedStream>(mut stream: S, mut cmd_rx: mpsc::UnboundedReceiver<MuxCommand>) {
    let mut state = MuxState::new();
    let mut write_queue = WriteQueue::new();
    // Made at the start, so that a runtime without a timer fails here and
    // not on a remote's open.
    let timer = tokio::time::sleep_until(Instant::now());
    tokio::pin!(timer);

    loop {
        if let Err(e) = resume_batch(&mut state, &mut write_queue) {
            warn!(err = %e, "protomux frame error");
            shutdown_all(&mut state);
            return;
        }
        // Flush pending writes before entering select
        while let Some(out) = write_queue.pop_front() {
            if let Err(e) = stream.write_frame(&out.frame).await {
                warn!(err = %e, "protomux write error");
                shutdown_all(&mut state);
                return;
            }
            if out.reply {
                state.unsent_replies.fetch_sub(out.frame.len(), Ordering::Relaxed);
            }
        }

        let reading = state.reading();
        let deadline = state.next_deadline();
        tokio::select! {
            // A command sent before a frame arrives is handled first: a
            // channel created at once is waiting when the remote's open
            // comes.
            biased;

            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        handle_command(cmd, &mut state, &mut write_queue);
                        // Drain all immediately available commands
                        while let Ok(cmd) = cmd_rx.try_recv() {
                            handle_command(cmd, &mut state, &mut write_queue);
                        }
                    }
                    None => {
                        debug!("protomux all handles dropped");
                        shutdown_all(&mut state);
                        return;
                    }
                }
            }
            () = sleep_until_due(timer.as_mut(), deadline) => {
                if let Err(e) = expire_unclaimed(&mut state, &mut write_queue, Instant::now()) {
                    warn!(err = %e, "protomux frame error");
                    shutdown_all(&mut state);
                    return;
                }
            }
            result = stream.read_frame(), if reading => {
                match result {
                    Ok(Some(data)) => {
                        if data.is_empty() {
                            continue; // Ignore empty frames (Node.js compat)
                        }
                        if let Err(e) = handle_incoming_frame(&data, &mut state, &mut write_queue) {
                            warn!(err = %e, "protomux frame error");
                            shutdown_all(&mut state);
                            return;
                        }
                    }
                    Ok(None) => {
                        debug!("protomux stream EOF");
                        shutdown_all(&mut state);
                        return;
                    }
                    Err(e) => {
                        warn!(err = %e, "protomux read error");
                        shutdown_all(&mut state);
                        return;
                    }
                }
            }
        }
    }
}

/// How many frames the reader of a split stream reads ahead of the mux.
const FRAMES_READ_AHEAD: usize = 16;

/// How many bytes of frames the reader of a split stream reads ahead of the
/// mux, besides the one frame it holds while it waits for room: so that a mux
/// that has stopped reading (see [`MAX_BUFFERED`]) does not hold 16 large
/// frames. A larger frame goes alone.
const BYTES_READ_AHEAD: usize = 1024 * 1024;

/// A frame the reader of a split stream read, and its share of
/// [`BYTES_READ_AHEAD`], given back once the mux has handled it.
type ReadAhead = (std::io::Result<Option<Vec<u8>>>, Option<tokio::sync::OwnedSemaphorePermit>);

/// Drive the Protomux over a stream split into halves.
///
/// The reader and the writer each run on their own, so the mux goes on
/// reading while a write waits for the remote to make room, and a read is
/// never cancelled part way through a frame.
async fn run_split_mux(halves: FramedHalves, cmd_rx: mpsc::UnboundedReceiver<MuxCommand>) {
    let FramedHalves { mut read, mut write } = halves;
    let (incoming_tx, incoming_rx) = mpsc::channel(FRAMES_READ_AHEAD);
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Outgoing>();
    let (write_failed_tx, write_failed_rx) = oneshot::channel();
    let unsent_replies = Arc::new(AtomicUsize::new(0));

    let read_budget = Arc::new(tokio::sync::Semaphore::new(BYTES_READ_AHEAD));

    // Ends only when the mux does.
    let reader = async move {
        loop {
            let result = read.read_frame_boxed().await;
            let last = !matches!(result, Ok(Some(_)));
            let len = result.as_ref().map_or(0, |frame| frame.as_ref().map_or(0, Vec::len));
            let permit = if last {
                None
            } else {
                let share = len.clamp(1, BYTES_READ_AHEAD) as u32;
                match Arc::clone(&read_budget).acquire_many_owned(share).await {
                    Ok(permit) => Some(permit),
                    Err(_) => break,
                }
            };
            if incoming_tx.send((result, permit)).await.is_err() || last {
                break;
            }
            // A frame as large as the whole budget is let through alone: the
            // next is not read until the mux has taken it.
            if len >= BYTES_READ_AHEAD {
                match read_budget.acquire_many(BYTES_READ_AHEAD as u32).await {
                    Ok(_all) => {}
                    Err(_) => break,
                }
            }
        }
        std::future::pending::<()>().await
    };
    // Ends once the mux has nothing more for it, or a write fails.
    let written_replies = Arc::clone(&unsent_replies);
    let writer = async move {
        while let Some(out) = outgoing_rx.recv().await {
            if let Err(e) = write.write_frame_boxed(&out.frame).await {
                let _ = write_failed_tx.send(e);
                return;
            }
            if out.reply {
                written_replies.fetch_sub(out.frame.len(), Ordering::Relaxed);
            }
        }
    };
    let mux = split_mux_state(cmd_rx, incoming_rx, outgoing_tx, write_failed_rx, unsent_replies);
    tokio::pin!(reader, writer, mux);

    let mut written = false;
    let flush = loop {
        tokio::select! {
            flush = &mut mux => break flush,
            () = &mut reader => {}
            () = &mut writer, if !written => written = true,
        }
    };
    // What was sent before the handles were dropped still goes out, as it
    // does over a stream that cannot be split.
    if flush && !written {
        writer.await;
    }
}

/// The state of a mux over a split stream: it takes in what the reader read
/// and the commands of its handles, and hands frames to the writer. Returns
/// whether the writer should finish writing them, which it should once the
/// handles are dropped, and not once the stream has failed.
async fn split_mux_state(
    mut cmd_rx: mpsc::UnboundedReceiver<MuxCommand>,
    mut incoming_rx: mpsc::Receiver<ReadAhead>,
    outgoing_tx: mpsc::UnboundedSender<Outgoing>,
    mut write_failed_rx: oneshot::Receiver<std::io::Error>,
    unsent_replies: Arc<AtomicUsize>,
) -> bool {
    let mut state = MuxState::new();
    state.unsent_replies = unsent_replies;
    let mut write_queue = WriteQueue::new();
    let mut writer_done = false;
    // Made at the start, so that a runtime without a timer fails here and
    // not on a remote's open.
    let timer = tokio::time::sleep_until(Instant::now());
    tokio::pin!(timer);

    loop {
        if let Err(e) = resume_batch(&mut state, &mut write_queue) {
            warn!(err = %e, "protomux frame error");
            shutdown_all(&mut state);
            return false;
        }
        for out in write_queue.drain(..) {
            let _ = outgoing_tx.send(out);
        }

        let reading = state.reading();
        let deadline = state.next_deadline();
        tokio::select! {
            // As in `run_mux`, commands first.
            biased;

            cmd = cmd_rx.recv() => match cmd {
                Some(cmd) => {
                    handle_command(cmd, &mut state, &mut write_queue);
                    // Drain all immediately available commands
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        handle_command(cmd, &mut state, &mut write_queue);
                    }
                }
                None => {
                    debug!("protomux all handles dropped");
                    shutdown_all(&mut state);
                    for out in write_queue.drain(..) {
                        let _ = outgoing_tx.send(out);
                    }
                    return true;
                }
            },
            failed = &mut write_failed_rx, if !writer_done => {
                writer_done = true;
                if let Ok(e) = failed {
                    warn!(err = %e, "protomux write error");
                    shutdown_all(&mut state);
                    return false;
                }
            }
            () = sleep_until_due(timer.as_mut(), deadline) => {
                if let Err(e) = expire_unclaimed(&mut state, &mut write_queue, Instant::now()) {
                    warn!(err = %e, "protomux frame error");
                    shutdown_all(&mut state);
                    return false;
                }
            }
            // Past `MAX_BUFFERED` the reader is left to wait, and the remote
            // with it, as Node pauses its stream.
            incoming = incoming_rx.recv(), if reading => match incoming.map(|(frame, _share)| frame) {
                Some(Ok(Some(data))) => {
                    // Empty frames are ignored (Node.js compat).
                    if !data.is_empty() {
                        if let Err(e) = handle_incoming_frame(&data, &mut state, &mut write_queue) {
                            warn!(err = %e, "protomux frame error");
                            shutdown_all(&mut state);
                            return false;
                        }
                    }
                }
                Some(Ok(None)) | None => {
                    debug!("protomux stream EOF");
                    shutdown_all(&mut state);
                    return false;
                }
                Some(Err(e)) => {
                    warn!(err = %e, "protomux read error");
                    shutdown_all(&mut state);
                    return false;
                }
            },
        }

        for out in write_queue.drain(..) {
            let _ = outgoing_tx.send(out);
        }
    }
}

fn shutdown_all(state: &mut MuxState) {
    for ch in state.local.iter_mut().flatten() {
        if !ch.closed {
            ch.closed = true;
            let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
        }
    }
}

fn handle_command(cmd: MuxCommand, state: &mut MuxState, write_queue: &mut WriteQueue) {
    match cmd {
        MuxCommand::CreateChannel {
            protocol,
            id,
            handshake,
            claim,
            response,
        } => {
            let result = match claim {
                Some(open) => accept_channel_internal(state, write_queue, &protocol, id, handshake, open),
                None => create_channel_internal(state, write_queue, &protocol, id, handshake),
            };
            let _ = response.send(result);
        }
        MuxCommand::SendMessage {
            local_id,
            generation,
            message_type,
            data,
        } => {
            send_message_internal(state, write_queue, local_id, generation, message_type, &data);
        }
        MuxCommand::CloseChannel { local_id, generation } => {
            close_channel_internal(state, write_queue, local_id, generation);
        }
        MuxCommand::Pair { key, pairing } => {
            state.pairings.insert(key, pairing);
        }
        MuxCommand::Unpair { key, registration } => {
            if state.pairings.get(&key).is_some_and(|p| p.registration == registration) {
                state.pairings.remove(&key);
            }
        }
        MuxCommand::Cork => {
            state.cork_depth += 1;
        }
        MuxCommand::Uncork => {
            if state.cork_depth > 0 {
                state.cork_depth -= 1;
                if state.cork_depth == 0 && !state.batch.is_empty() {
                    flush_batch(state, write_queue);
                }
            }
        }
    }
}

fn create_channel_internal(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake: Option<Vec<u8>>,
) -> Result<ChannelHandle> {
    let key = MuxState::channel_key(protocol, id.as_deref());
    // A channel the remote opened for this protocol and id, if one waits.
    let waiting = state.infos.get_mut(&key).and_then(|info| {
        (!info.incoming.is_empty()).then(|| info.incoming.remove(0))
    });
    open_local_channel(state, write_queue, protocol, id, handshake, key, waiting)
}

/// Create the channel that pairs with the remote's `open`, if it still waits.
fn accept_channel_internal(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake: Option<Vec<u8>>,
    open: OpenRef,
) -> Result<ChannelHandle> {
    if open.mux != state.mux_id {
        return Err(ProtomuxError::NotNextOpen);
    }
    let key = MuxState::channel_key(protocol, id.as_deref());
    let waits = state
        .remote
        .get(&open.remote_id)
        .is_some_and(|rc| rc.local_id.is_none() && rc.seq == open.seq && rc.key == key);
    if !waits {
        return Err(ProtomuxError::ChannelClosed);
    }
    // The remote takes our open for the answer to its oldest open of the
    // key, as Node's `_onopensession` does.
    let info = state
        .infos
        .get_mut(&key)
        .ok_or_else(|| ProtomuxError::Internal("waiting open has no info".into()))?;
    if info.incoming.first() != Some(&open.remote_id) {
        return Err(ProtomuxError::NotNextOpen);
    }
    info.incoming.remove(0);
    open_local_channel(state, write_queue, protocol, id, handshake, key, Some(open.remote_id))
}

/// Create a local channel, paired with the remote's channel `waiting` if
/// given, or else waiting for the remote to answer its open.
fn open_local_channel(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake: Option<Vec<u8>>,
    key: String,
    waiting: Option<u32>,
) -> Result<ChannelHandle> {
    let internal_id = state.alloc_local_id();
    let local_id_wire = internal_id + 1; // 1-indexed on wire
    let generation = state.next_generation;
    state.next_generation += 1;

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    state.local[internal_id as usize] = Some(LocalChannel {
        generation,
        remote_id: waiting,
        event_tx: event_tx.clone(),
        opened: waiting.is_some(),
        closed: false,
    });

    let frame = encode_open(local_id_wire as u64, protocol, id.as_deref(), handshake.as_deref());
    enqueue_control(state, write_queue, frame);

    if let Some(remote_id) = waiting {
        // Paired: take what the remote's open left for it.
        let rc = state
            .remote
            .get_mut(&remote_id)
            .ok_or_else(|| ProtomuxError::Internal("waiting remote channel missing".into()))?;
        rc.local_id = Some(internal_id);
        rc.deadline = None;
        let remote_hs = rc.handshake_state.take();
        let pending = std::mem::take(&mut rc.pending);
        let held = std::mem::take(&mut rc.held);
        state.unclaimed_count -= 1;
        state.buffered -= held;
        state.gc_info(&key);

        let _ = event_tx.send(ChannelEvent::Opened { handshake: remote_hs });
        for (msg_type, data) in pending {
            let _ = event_tx.send(ChannelEvent::Message {
                message_type: msg_type as u32,
                data,
            });
        }
    } else {
        state
            .infos
            .entry(key)
            .or_insert_with(|| ChannelInfo {
                outgoing: Vec::new(),
                incoming: Vec::new(),
            })
            .outgoing
            .push(local_id_wire);
    }

    Ok(ChannelHandle {
        local_id: local_id_wire,
        generation,
        event_rx,
    })
}

fn send_message_internal(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    local_id: u32,
    generation: u64,
    message_type: u32,
    data: &[u8],
) {
    if !state.local_of(local_id, generation).is_some_and(|ch| !ch.closed) {
        return;
    }

    if state.cork_depth > 0 {
        // Batch mode: encode inner message (type + payload)
        let mut inner_state = State::new();
        c::preencode_uint(&mut inner_state, message_type as u64);
        inner_state.end += data.len();
        inner_state.alloc();
        c::encode_uint(&mut inner_state, message_type as u64);
        inner_state.buffer[inner_state.start..inner_state.start + data.len()]
            .copy_from_slice(data);
        push_batch(state, write_queue, local_id as u64, inner_state.buffer);
    } else {
        let frame = encode_message(local_id as u64, message_type as u64, data);
        enqueue_write(write_queue, frame);
    }
}

fn close_channel_internal(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    local_id: u32,
    generation: u64,
) {
    let Some(ch) = state.local_of(local_id, generation) else {
        return;
    };
    if ch.closed {
        return;
    }
    ch.closed = true;
    let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: false });
    let remote_id = ch.remote_id;

    enqueue_control(state, write_queue, encode_close(local_id as u64));

    let internal_id = local_id - 1;
    state.local[internal_id as usize] = None;
    // As in Node's `_close`: once the remote has answered our open, its id
    // is free at once, what it still sends on it is dropped, and our id may
    // be used again. Until it has, our id waits for its answer, open or
    // reject, so that the answer is not taken for the next channel's.
    if let Some(remote_id) = remote_id {
        state.remote.remove(&remote_id);
        state.free_ids.push(internal_id);
    }
}

/// Queue a frame of ours.
fn enqueue_write(write_queue: &mut WriteQueue, frame: Vec<u8>) {
    write_queue.push_back(Outgoing { frame, reply: false });
}

/// What an item adds to a batch at most besides its bytes: its length, and
/// a switch to its channel.
const BATCH_ITEM_OVERHEAD: usize = 20;

/// Add an item to the batch, sending the batch first if the item would take
/// it past [`MAX_BATCH`], as Node's `_pushBatch` does once it is there: so
/// that items each small enough to send are never put in a frame too large
/// to. An item past it on its own goes out after the batch as the frame it
/// would be uncorked, no larger.
fn push_batch(state: &mut MuxState, write_queue: &mut WriteQueue, channel: u64, item: Vec<u8>) {
    let size = item.len() + BATCH_ITEM_OVERHEAD;
    if !state.batch.is_empty() && state.batch_bytes + size > MAX_BATCH {
        flush_batch(state, write_queue);
    }
    if size > MAX_BATCH {
        let mut frame = State::new();
        c::preencode_uint(&mut frame, channel);
        frame.alloc();
        c::encode_uint(&mut frame, channel);
        let mut frame = frame.buffer;
        frame.extend_from_slice(&item);
        enqueue_write(write_queue, frame);
        return;
    }
    state.batch_bytes += size;
    state.batch.push((channel, item));
}

/// Send what is in the batch.
fn flush_batch(state: &mut MuxState, write_queue: &mut WriteQueue) {
    state.batch_bytes = 0;
    let entries = std::mem::take(&mut state.batch);
    let frame = encode_batch(&entries);
    if !frame.is_empty() {
        enqueue_write(write_queue, frame);
    }
}

/// Queue a control frame of ours (an open or close). While corked it goes in
/// the batch, as Node's `_write0` puts it, so that it keeps its place among
/// the messages: a close after the channel's last messages, and an open of a
/// reused id after the messages of the channel that had it before.
fn enqueue_control(state: &mut MuxState, write_queue: &mut WriteQueue, frame: Vec<u8>) {
    if state.cork_depth > 0 {
        push_batch(state, write_queue, 0, frame[1..].to_vec());
    } else {
        enqueue_write(write_queue, frame);
    }
}

/// Queue a frame that answers the remote. A remote that has the mux owe it
/// more than [`MAX_UNSENT_REPLIES`] of them, while not reading them, has the
/// mux end.
fn enqueue_reply(state: &MuxState, write_queue: &mut WriteQueue, frame: Vec<u8>) -> Result<()> {
    let unsent = state.unsent_replies.fetch_add(frame.len(), Ordering::Relaxed) + frame.len();
    write_queue.push_back(Outgoing { frame, reply: true });
    if unsent > MAX_UNSENT_REPLIES {
        return Err(ProtomuxError::InvalidFrame("remote is not reading the mux's replies".into()));
    }
    Ok(())
}

// ── Incoming frame handling ──────────────────────────────────────────────────

fn handle_incoming_frame(
    data: &[u8],
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
) -> Result<()> {
    // A batch is handled as it is read, item by item, so that the mux can
    // stop part way through when it holds too much.
    let mut s = State::from_buffer(data);
    if c::decode_uint(&mut s)? == 0 && c::decode_uint(&mut s)? == CTRL_BATCH {
        let channel = c::decode_uint(&mut s)?;
        return handle_batch(s, channel, state, write_queue);
    }

    let frame = decode_frame(data)?;
    match frame {
        DecodedFrame::Control(ctrl) => handle_control(ctrl, state, write_queue),
        DecodedFrame::Message {
            channel_id,
            message_type,
            payload,
        } => {
            if let Ok(channel_id) = u32::try_from(channel_id) {
                dispatch_message(state, channel_id, message_type, &payload);
            }
            Ok(())
        }
        DecodedFrame::Batch(_) => Err(ProtomuxError::Internal("batch not handled as one".into())),
    }
}

/// Handle the items of a batch from `s`, its next item on `channel`, until it
/// ends or the mux holds too much, when what is left waits in `batch_rest`.
fn handle_batch(
    mut s: State,
    mut channel: u64,
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
) -> Result<()> {
    let end = s.end;
    while s.start < end {
        if state.paused() {
            state.batch_rest = Some(BatchRest { state: s, channel });
            return Ok(());
        }
        let len = usize::try_from(c::decode_uint(&mut s)?).unwrap_or(usize::MAX);
        if len == 0 {
            // Channel switch
            channel = c::decode_uint(&mut s)?;
            continue;
        }
        if len > end.saturating_sub(s.start) {
            return Err(ProtomuxError::InvalidFrame("batch item overflows frame".into()));
        }
        let item = s.start..s.start + len;
        s.start += len;
        dispatch_batch_item(state, write_queue, channel, &s.buffer[item])?;
    }
    Ok(())
}

/// Go on with a batch the mux stopped part way through, if it may.
fn resume_batch(state: &mut MuxState, write_queue: &mut WriteQueue) -> Result<()> {
    if state.paused() {
        return Ok(());
    }
    match state.batch_rest.take() {
        Some(BatchRest { state: s, channel }) => handle_batch(s, channel, state, write_queue),
        None => Ok(()),
    }
}

fn handle_control(
    ctrl: ControlFrame,
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
) -> Result<()> {
    match ctrl {
        ControlFrame::Open {
            local_id: remote_local_id,
            protocol,
            id,
            handshake_state,
        } => {
            handle_remote_open(state, write_queue, remote_local_id, &protocol, id, handshake_state)
        }
        ControlFrame::Close { local_id: remote_local_id } => {
            handle_remote_close(state, write_queue, remote_local_id)
        }
        ControlFrame::Reject { remote_id } => {
            handle_remote_reject(state, remote_id);
            Ok(())
        }
    }
}

fn handle_remote_open(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    remote_local_id: u64,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake_state: Option<Vec<u8>>,
) -> Result<()> {
    // Reject remote_id == 0 (control session)
    if remote_local_id == 0 {
        return enqueue_reply(state, write_queue, encode_reject(0));
    }

    // A remote opens each channel at an id it has used before or at the next
    // one, as Node's `_onopensession` allows: an id further on would have us
    // grow the list of remote channels to any length it chose.
    if remote_local_id > u64::from(state.remote_ids) + 1 {
        return Err(ProtomuxError::InvalidFrame(format!(
            "remote channel id {remote_local_id} skips ahead of {}",
            state.remote_ids
        )));
    }
    let remote_id = remote_local_id as u32; // Remote's local_id is our remote_id
    if state.remote.contains_key(&remote_id) {
        return Err(ProtomuxError::InvalidFrame("duplicate remote channel ID".into()));
    }
    state.remote_ids = state.remote_ids.max(remote_id);

    let key = MuxState::channel_key(protocol, id.as_deref());

    // A local channel waiting for this protocol and id pairs with it.
    let waiting = state.infos.get_mut(&key).and_then(|info| {
        (!info.outgoing.is_empty()).then(|| info.outgoing.remove(0))
    });
    if let Some(pos) = waiting {
        state.gc_info(&key);
        let internal_id = pos - 1;
        let Some(ch) = state.local[internal_id as usize].as_mut() else {
            // Closed before this answer came, which frees its id; the
            // remote has our close. As Node's `_onopensession`.
            state.free_ids.push(internal_id);
            return Ok(());
        };
        ch.remote_id = Some(remote_id);
        ch.opened = true;
        let _ = ch.event_tx.send(ChannelEvent::Opened {
            handshake: handshake_state,
        });
        state.remote.insert(
            remote_id,
            RemoteChannel {
                handshake_state: None,
                local_id: Some(internal_id),
                pending: Vec::new(),
                key,
                deadline: None,
                held: 0,
                seq: 0,
            },
        );

        debug!(
            protocol,
            local_id = pos,
            remote_id,
            "protomux channel fully opened"
        );
        return Ok(());
    }

    // Past the cap an open is rejected at once, as Node rejects one nothing
    // is registered for; Node holds no more than a tick's worth.
    if state.unclaimed_count >= MAX_UNCLAIMED_OPENS {
        debug!(protocol, remote_id, "protomux rejecting open: too many waiting for a pair");
        return enqueue_reply(state, write_queue, encode_reject(remote_local_id));
    }

    // No local channel yet: it waits a moment for one, and a listener of
    // `Mux::pair` is told of it.
    let held = protocol.len()
        + 2 * key.len()
        + handshake_state.as_ref().map_or(0, Vec::len)
        + OPEN_OVERHEAD;
    let deadline = Instant::now() + UNCLAIMED_OPEN_GRACE;
    let seq = state.next_open_seq;
    state.next_open_seq += 1;
    state.infos
        .entry(key.clone())
        .or_insert_with(|| ChannelInfo {
            outgoing: Vec::new(),
            incoming: Vec::new(),
        })
        .incoming
        .push(remote_id);
    let wildcard = MuxState::channel_key(protocol, None);
    let pairing = state.pairings.get(&key).or_else(|| state.pairings.get(&wildcard));
    if let Some(pairing) = pairing {
        let size = notice_size(protocol, id.as_deref());
        let queued = pairing.queued.fetch_add(size, Ordering::Relaxed) + size;
        let sent = queued <= MAX_QUEUED_NOTICES
            && pairing
                .notify
                .try_send(PairRequest {
                    protocol: protocol.to_string(),
                    id,
                    open: OpenRef { mux: state.mux_id, remote_id, seq },
                })
                .is_ok();
        if !sent {
            pairing.queued.fetch_sub(size, Ordering::Relaxed);
        }
    }
    state.remote.insert(
        remote_id,
        RemoteChannel {
            handshake_state,
            local_id: None,
            pending: Vec::new(),
            key,
            deadline: Some(deadline),
            held,
            seq,
        },
    );
    state.buffered += held;
    state.unclaimed.push_back((deadline, remote_id));
    state.unclaimed_count += 1;
    // Entries of channels since closed or paired wait for their deadline to
    // be skipped; a remote that opens and closes fast is not let pile them up.
    if state.unclaimed.len() > 2 * MAX_UNCLAIMED_OPENS {
        let remote = &state.remote;
        state.unclaimed.retain(|&(deadline, remote_id)| {
            remote.get(&remote_id).is_some_and(|rc| rc.is_due(deadline))
        });
    }
    debug!(
        protocol,
        remote_id,
        "protomux remote channel queued (awaiting local create)"
    );

    Ok(())
}

/// Reject the remote channels that have waited too long for a pair.
fn expire_unclaimed(state: &mut MuxState, write_queue: &mut WriteQueue, now: Instant) -> Result<()> {
    while let Some(&(deadline, remote_id)) = state.unclaimed.front() {
        if deadline > now {
            break;
        }
        state.unclaimed.pop_front();
        if state.remote.get(&remote_id).is_some_and(|rc| rc.is_due(deadline)) {
            debug!(remote_id, "protomux rejecting open: nothing paired with it");
            state.drop_unclaimed(remote_id);
            enqueue_reply(state, write_queue, encode_reject(remote_id as u64))?;
        }
    }
    Ok(())
}

fn handle_remote_close(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    remote_local_id: u64,
) -> Result<()> {
    let Ok(remote_id) = u32::try_from(remote_local_id) else {
        return Ok(());
    };
    let Some(rc) = state.remote.get(&remote_id) else {
        return Ok(());
    };
    let Some(internal_id) = rc.local_id else {
        // Closed before it was paired. Node rejects it once nothing pairs
        // with it, and the remote's id for it waits for that answer.
        state.drop_unclaimed(remote_id);
        return enqueue_reply(state, write_queue, encode_reject(remote_local_id));
    };
    // As Node's `_close`: both ids are free once the remote closes.
    state.remote.remove(&remote_id);
    if let Some(ch) = state.local[internal_id as usize].take() {
        if !ch.closed {
            let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
        }
        state.free_ids.push(internal_id);
    }
    Ok(())
}

fn handle_remote_reject(state: &mut MuxState, remote_id: u64) {
    // Remote rejected one of our channels. As in Node's `_onrejectsession`,
    // only an open still waiting for its answer can be rejected; a reject
    // for any other id is ignored.
    let Ok(local_id_wire) = u32::try_from(remote_id) else {
        return;
    };
    let Some((key, info)) = state
        .infos
        .iter_mut()
        .find(|(_, info)| info.outgoing.contains(&local_id_wire))
    else {
        return;
    };
    info.outgoing.retain(|&id| id != local_id_wire);
    let key = key.clone();
    state.gc_info(&key);

    // The id is free, whether its channel is still open or was closed
    // before this answer came.
    let internal_id = local_id_wire - 1;
    if let Some(ch) = state.local[internal_id as usize].take() {
        if !ch.closed {
            let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
        }
    }
    state.free_ids.push(internal_id);
}

fn dispatch_message(state: &mut MuxState, remote_id: u32, message_type: u64, payload: &[u8]) {
    let Some(rc) = state.remote.get_mut(&remote_id) else {
        return;
    };

    if let Some(local_id) = rc.local_id {
        if let Some(ch) = state.local.get(local_id as usize).and_then(|c| c.as_ref()) {
            if ch.opened && !ch.closed {
                let _ = ch.event_tx.send(ChannelEvent::Message {
                    message_type: message_type as u32,
                    data: payload.to_vec(),
                });
            }
        }
        return;
    }

    // Held until a local channel pairs with it, or it is rejected.
    let size = pending_size(payload);
    rc.held += size;
    state.buffered += size;
    rc.pending.push((message_type, payload.to_vec()));
}

fn dispatch_batch_item(
    state: &mut MuxState,
    write_queue: &mut WriteQueue,
    channel_id: u64,
    data: &[u8],
) -> Result<()> {
    // A control frame batched by a corked Node peer (`_write0`), without its
    // leading channel 0. Node decodes it as a control frame; a batch within a
    // batch is never sent.
    if channel_id == 0 {
        // Refuse a batch within a batch before decoding any of it.
        let mut s = State::from_buffer(data);
        if c::decode_uint(&mut s)? == CTRL_BATCH {
            return Err(ProtomuxError::InvalidFrame("batch within a batch".into()));
        }
        let mut frame = Vec::with_capacity(1 + data.len());
        frame.push(0);
        frame.extend_from_slice(data);
        return match decode_frame(&frame)? {
            DecodedFrame::Control(ctrl) => handle_control(ctrl, state, write_queue),
            _ => Err(ProtomuxError::InvalidFrame("batch within a batch".into())),
        };
    }

    // Each batch item's data is: [uint:messageType][payload]
    let mut s = State::from_buffer(data);
    let message_type = c::decode_uint(&mut s)?;
    if let Ok(channel_id) = u32::try_from(channel_id) {
        dispatch_message(state, channel_id, message_type, &s.buffer[s.start..s.end]);
    }
    Ok(())
}

// ── hex helper (minimal, no external dep) ────────────────────────────────────

mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Frame encoding/decoding round-trips ──

    #[test]
    fn open_frame_roundtrip() {
        let frame = encode_open(1, "blind-relay", None, None);
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Control(ControlFrame::Open {
                local_id: 1,
                protocol: "blind-relay".into(),
                id: None,
                handshake_state: None,
            })
        );
    }

    #[test]
    fn open_frame_with_id_and_handshake() {
        let id = vec![0xAA, 0xBB, 0xCC];
        let hs = vec![1, 2, 3, 4, 5];
        let frame = encode_open(5, "test-proto", Some(&id), Some(&hs));
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Control(ControlFrame::Open {
                local_id: 5,
                protocol: "test-proto".into(),
                id: Some(id),
                handshake_state: Some(hs),
            })
        );
    }

    #[test]
    fn close_frame_roundtrip() {
        let frame = encode_close(3);
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Control(ControlFrame::Close { local_id: 3 })
        );
    }

    #[test]
    fn reject_frame_roundtrip() {
        let frame = encode_reject(7);
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Control(ControlFrame::Reject { remote_id: 7 })
        );
    }

    #[test]
    fn message_frame_roundtrip() {
        let payload = vec![10, 20, 30];
        let frame = encode_message(2, 0, &payload);
        let decoded = decode_frame(&frame).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Message {
                channel_id: 2,
                message_type: 0,
                payload,
            }
        );
    }

    #[test]
    fn batch_frame_roundtrip() {
        let entries = vec![
            (1u64, vec![0, 10, 20]), // channel 1, inner data
            (1, vec![0, 30, 40]),    // channel 1 again
            (2, vec![1, 50]),        // channel 2
        ];
        let frame = encode_batch(&entries);
        let decoded = decode_frame(&frame).unwrap();
        match decoded {
            DecodedFrame::Batch(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].channel_id, 1);
                assert_eq!(items[0].data, vec![0, 10, 20]);
                assert_eq!(items[1].channel_id, 1);
                assert_eq!(items[1].data, vec![0, 30, 40]);
                assert_eq!(items[2].channel_id, 2);
                assert_eq!(items[2].data, vec![1, 50]);
            }
            other => panic!("expected Batch, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch() {
        let frame = encode_batch(&[]);
        assert!(frame.is_empty());
    }

    // ── Async integration tests ──

    /// In-memory framed stream for testing.
    struct MemStream {
        rx: mpsc::UnboundedReceiver<Vec<u8>>,
        tx: mpsc::UnboundedSender<Vec<u8>>,
    }

    impl FramedStream for MemStream {
        async fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.rx.recv().await)
        }

        async fn write_frame(&mut self, data: &[u8]) -> std::io::Result<()> {
            self.tx
                .send(data.to_vec())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "closed"))
        }
    }

    /// Create a pair of connected MemStreams.
    fn mem_pair() -> (MemStream, MemStream) {
        let (tx_a, rx_b) = mpsc::unbounded_channel();
        let (tx_b, rx_a) = mpsc::unbounded_channel();
        (
            MemStream { rx: rx_a, tx: tx_a },
            MemStream { rx: rx_b, tx: tx_b },
        )
    }

    #[tokio::test]
    async fn two_mux_open_channel() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        // Both sides create a channel with the same protocol
        let mut ch_a = mux_a
            .create_channel("test-protocol", None, None)
            .await
            .unwrap();
        let mut ch_b = mux_b
            .create_channel("test-protocol", None, None)
            .await
            .unwrap();

        // Both should eventually open
        let hs_a = ch_a.wait_opened().await.unwrap();
        let hs_b = ch_b.wait_opened().await.unwrap();
        assert!(hs_a.is_none());
        assert!(hs_b.is_none());
        assert!(ch_a.is_opened());
        assert!(ch_b.is_opened());
    }

    #[tokio::test]
    async fn send_receive_messages() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let mut ch_a = mux_a
            .create_channel("echo", None, None)
            .await
            .unwrap();
        let mut ch_b = mux_b
            .create_channel("echo", None, None)
            .await
            .unwrap();

        ch_a.wait_opened().await.unwrap();
        ch_b.wait_opened().await.unwrap();

        // A → B
        ch_a.send(0, b"hello from A").unwrap();
        let event = ch_b.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, 0);
                assert_eq!(data, b"hello from A");
            }
            other => panic!("expected Message, got {other:?}"),
        }

        // B → A
        ch_b.send(1, b"reply from B").unwrap();
        let event = ch_a.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, 1);
                assert_eq!(data, b"reply from B");
            }
            other => panic!("expected Message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn channel_close() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let mut ch_a = mux_a
            .create_channel("close-test", None, None)
            .await
            .unwrap();
        let mut ch_b = mux_b
            .create_channel("close-test", None, None)
            .await
            .unwrap();

        ch_a.wait_opened().await.unwrap();
        ch_b.wait_opened().await.unwrap();

        // A closes
        ch_a.close();

        // B should see close event
        let event = ch_b.recv().await.unwrap();
        match event {
            ChannelEvent::Closed { is_remote } => assert!(is_remote),
            other => panic!("expected Closed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_channels() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        // Open two different channels
        let mut ch_a1 = mux_a
            .create_channel("proto-1", None, None)
            .await
            .unwrap();
        let mut ch_a2 = mux_a
            .create_channel("proto-2", None, None)
            .await
            .unwrap();
        let mut ch_b1 = mux_b
            .create_channel("proto-1", None, None)
            .await
            .unwrap();
        let mut ch_b2 = mux_b
            .create_channel("proto-2", None, None)
            .await
            .unwrap();

        ch_a1.wait_opened().await.unwrap();
        ch_a2.wait_opened().await.unwrap();
        ch_b1.wait_opened().await.unwrap();
        ch_b2.wait_opened().await.unwrap();

        // Send on channel 1
        ch_a1.send(0, b"on channel 1").unwrap();
        let ev = ch_b1.recv().await.unwrap();
        match ev {
            ChannelEvent::Message { data, .. } => assert_eq!(data, b"on channel 1"),
            other => panic!("expected Message, got {other:?}"),
        }

        // Send on channel 2
        ch_a2.send(0, b"on channel 2").unwrap();
        let ev = ch_b2.recv().await.unwrap();
        match ev {
            ChannelEvent::Message { data, .. } => assert_eq!(data, b"on channel 2"),
            other => panic!("expected Message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handshake_exchange() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let mut ch_a = mux_a
            .create_channel("hs-test", None, Some(vec![1, 2, 3]))
            .await
            .unwrap();
        let mut ch_b = mux_b
            .create_channel("hs-test", None, Some(vec![4, 5, 6]))
            .await
            .unwrap();

        let hs_a = ch_a.wait_opened().await.unwrap();
        let hs_b = ch_b.wait_opened().await.unwrap();

        // Each side receives the other's handshake
        assert_eq!(hs_a, Some(vec![4, 5, 6]));
        assert_eq!(hs_b, Some(vec![1, 2, 3]));
    }

    // ── Over SecretStream ──

    use crate::secret_stream::SecretStream;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    /// Two ends of one SecretStream session over `a` and `b`.
    async fn secret_pair<T>(a: T, b: T) -> (SecretStream<T>, SecretStream<T>)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (a, b) = tokio::try_join!(
            SecretStream::from_session(true, a, [1; 32], [2; 32], [3; 64], [4; 32]),
            SecretStream::from_session(false, b, [2; 32], [1; 32], [3; 64], [5; 32]),
        )
        .expect("both ends of the session");
        (a, b)
    }

    /// The next message on `channel`, within ten seconds.
    async fn message(channel: &mut Channel) -> Vec<u8> {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), channel.recv())
            .await
            .expect("a message within ten seconds");
        match event {
            Some(ChannelEvent::Message { data, .. }) => data,
            other => panic!("expected a message, got {other:?}"),
        }
    }

    /// Two muxes over `a` and `b`, each with an open channel to the other.
    async fn open_channels<T>(a: SecretStream<T>, b: SecretStream<T>) -> (Mux, Channel, Mux, Channel)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mux_a, run_a) = Mux::new(a);
        let (mux_b, run_b) = Mux::new(b);
        tokio::spawn(run_a);
        tokio::spawn(run_b);
        let mut ch_a = mux_a.create_channel("test", None, None).await.unwrap();
        let mut ch_b = mux_b.create_channel("test", None, None).await.unwrap();
        ch_a.wait_opened().await.unwrap();
        ch_b.wait_opened().await.unwrap();
        (mux_a, ch_a, mux_b, ch_b)
    }

    #[tokio::test]
    async fn both_ends_sending_more_than_the_transport_holds_at_once_do_not_deadlock() {
        // A transport that holds 64 KiB, as a stream with a read buffer
        // limit holds its limit: a write beyond it waits for the other end
        // to read.
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (a, b) = secret_pair(a, b).await;
        let (_mux_a, mut ch_a, _mux_b, mut ch_b) = open_channels(a, b).await;

        let from_a = vec![0xA; 1 << 20];
        let from_b = vec![0xB; 1 << 20];
        ch_a.send(0, &from_a).unwrap();
        ch_b.send(0, &from_b).unwrap();

        assert!(message(&mut ch_b).await == from_a);
        assert!(message(&mut ch_a).await == from_b);
    }

    /// A transport whose writes go out a few bytes at a time, giving way to
    /// other tasks in between, so the other end reads each frame in pieces.
    struct Dribble<T> {
        inner: T,
        give_way: bool,
    }

    impl<T: AsyncRead + Unpin> AsyncRead for Dribble<T> {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for Dribble<T> {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.give_way = !self.give_way;
            if self.give_way {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let len = buf.len().min(5);
            Pin::new(&mut self.inner).poll_write(cx, &buf[..len])
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn sending_while_a_frame_arrives_in_pieces_loses_none_of_it() {
        let (a, b) = tokio::io::duplex(1 << 20);
        let dribble = |inner| Dribble { inner, give_way: false };
        let (a, b) = secret_pair(dribble(a), dribble(b)).await;
        let (_mux_a, mut ch_a, _mux_b, mut ch_b) = open_channels(a, b).await;

        // Each end sends while frames from the other are part way in, so a
        // send arrives at its mux while that mux is reading a frame.
        const MESSAGES: usize = 200;
        let numbered = |from: u8, n: usize| vec![from.wrapping_add(n as u8); 1_000];
        for n in 0..MESSAGES {
            ch_a.send(0, &numbered(0xA, n)).unwrap();
            ch_b.send(0, &numbered(0xB, n)).unwrap();
            tokio::task::yield_now().await;
        }
        for n in 0..MESSAGES {
            assert!(message(&mut ch_b).await == numbered(0xA, n), "message {n} from a");
            assert!(message(&mut ch_a).await == numbered(0xB, n), "message {n} from b");
        }
    }

    // ── Frames a remote chooses ──

    fn handle(state: &mut MuxState, queue: &mut WriteQueue, frame: &[u8]) -> Result<()> {
        handle_incoming_frame(frame, state, queue)
    }

    fn open(state: &mut MuxState, queue: &mut WriteQueue, id: u64, protocol: &str) {
        handle(state, queue, &encode_open(id, protocol, None, None)).unwrap();
    }

    fn create(state: &mut MuxState, queue: &mut WriteQueue, protocol: &str) -> ChannelHandle {
        create_channel_internal(state, queue, protocol, None, None).unwrap()
    }

    fn close(state: &mut MuxState, queue: &mut WriteQueue, ch: &ChannelHandle) {
        close_channel_internal(state, queue, ch.local_id, ch.generation);
    }

    fn last_frame(queue: &mut WriteQueue) -> Option<Vec<u8>> {
        queue.pop_back().map(|out| out.frame)
    }

    #[test]
    fn an_open_that_skips_ahead_is_an_error_and_allocates_nothing() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        for id in [0xFFFF_FFFF, u64::MAX, 2] {
            let frame = encode_open(id, "p", None, None);
            assert!(handle(&mut state, &mut queue, &frame).is_err(), "{id}");
            assert!(state.remote.is_empty(), "{id}");
            assert_eq!(state.remote_ids, 0, "{id}");
        }

        for id in [1, 2] {
            open(&mut state, &mut queue, id, "p");
        }
        assert_eq!((state.remote.len(), state.remote_ids), (2, 2));
    }

    #[test]
    fn a_reject_for_a_channel_never_opened_is_ignored() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        state.alloc_local_id();
        for id in [0, 1, 1, 9, u64::MAX] {
            handle(&mut state, &mut queue, &encode_reject(id)).unwrap();
        }
        assert!(state.free_ids.is_empty(), "{:?}", state.free_ids);
    }

    #[test]
    fn a_reject_closes_only_an_open_waiting_for_its_pair() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let mut rejected = create(&mut state, &mut queue, "p");
        for _ in 0..2 {
            handle(&mut state, &mut queue, &encode_reject(rejected.local_id as u64)).unwrap();
        }
        assert!(matches!(
            rejected.event_rx.try_recv(),
            Ok(ChannelEvent::Closed { is_remote: true })
        ));
        assert_eq!(state.free_ids, vec![0]);

        // Once paired, the channel is not the remote's to reject.
        let paired = create(&mut state, &mut queue, "q");
        open(&mut state, &mut queue, 1, "q");
        handle(&mut state, &mut queue, &encode_reject(paired.local_id as u64)).unwrap();
        assert!(state.local[0].is_some());
        assert!(state.free_ids.is_empty());
    }

    #[test]
    fn a_channel_closed_before_its_answer_keeps_its_id_until_the_answer() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let closed = create(&mut state, &mut queue, "p");
        close(&mut state, &mut queue, &closed);
        let next = create(&mut state, &mut queue, "p");
        assert_ne!(next.local_id, closed.local_id);

        // The remote's open answers the closed channel, not the next one.
        open(&mut state, &mut queue, 1, "p");
        assert!(!state.remote.contains_key(&1));
        assert!(state.local[next.local_id as usize - 1].as_ref().is_some_and(|ch| !ch.opened));
        assert_eq!(state.free_ids, vec![closed.local_id - 1]);

        // A reject answers one too, and frees its id.
        let rejected = create(&mut state, &mut queue, "r");
        assert_eq!(rejected.local_id, closed.local_id);
        close(&mut state, &mut queue, &rejected);
        let other = create(&mut state, &mut queue, "r");
        handle(&mut state, &mut queue, &encode_reject(rejected.local_id as u64)).unwrap();
        assert!(state.local[other.local_id as usize - 1].is_some(), "the reject closed the next channel");
        assert_eq!(state.free_ids, vec![rejected.local_id - 1]);
    }

    /// A corked Node peer batches its control frames on channel 0
    /// (`_write0`): the open in the batch counts, so the next open follows it.
    #[test]
    fn an_open_in_a_batch_is_handled_as_one() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let batched_open = encode_open(1, "p", None, None)[1..].to_vec();
        let batch = encode_batch(&[(0, batched_open)]);
        handle(&mut state, &mut queue, &batch).unwrap();
        assert_eq!(state.remote.len(), 1);

        open(&mut state, &mut queue, 2, "p");
        assert_eq!(state.remote.len(), 2);

        let nested = encode_batch(&[(0, batch[1..].to_vec())]);
        assert!(handle(&mut state, &mut queue, &nested).is_err());
    }

    #[test]
    fn a_batch_item_longer_than_any_frame_is_an_error() {
        let mut frame = vec![0, 0, 1, 0xff];
        frame.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode_frame(&frame), Err(ProtomuxError::InvalidFrame(_))));
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        assert!(matches!(
            handle(&mut state, &mut queue, &frame),
            Err(ProtomuxError::InvalidFrame(_))
        ));
    }

    // ── Opens nothing pairs with (Node's pairing model) ──

    #[tokio::test(start_paused = true)]
    async fn an_open_nothing_pairs_with_is_rejected_after_a_moment() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        open(&mut state, &mut queue, 1, "p");
        handle(&mut state, &mut queue, &encode_message(1, 0, &[7; 100])).unwrap();
        assert!(state.buffered > 0);

        expire_unclaimed(&mut state, &mut queue, Instant::now() + UNCLAIMED_OPEN_GRACE / 2).unwrap();
        assert!(queue.is_empty(), "rejected before its time");

        expire_unclaimed(&mut state, &mut queue, Instant::now() + UNCLAIMED_OPEN_GRACE).unwrap();
        assert_eq!(last_frame(&mut queue), Some(encode_reject(1)));
        assert!(state.remote.is_empty());
        assert!(state.infos.is_empty());
        assert_eq!((state.unclaimed_count, state.buffered), (0, 0));

        // The remote may open the id again.
        open(&mut state, &mut queue, 1, "p");
        assert_eq!(state.unclaimed_count, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_open_paired_in_time_is_not_rejected() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        open(&mut state, &mut queue, 1, "p");
        let mut ch = create(&mut state, &mut queue, "p");
        assert!(matches!(ch.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        assert_eq!((state.unclaimed_count, state.buffered), (0, 0));
        queue.clear();

        expire_unclaimed(&mut state, &mut queue, Instant::now() + UNCLAIMED_OPEN_GRACE * 2).unwrap();
        assert!(queue.is_empty());
        assert!(state.remote.get(&1).is_some_and(|rc| rc.local_id.is_some()));
        assert!(state.infos.is_empty());
    }

    #[test]
    fn opens_past_the_cap_are_rejected_at_once() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        for id in 1..=MAX_UNCLAIMED_OPENS as u64 {
            open(&mut state, &mut queue, id, "p");
        }
        assert!(queue.is_empty());

        let past = MAX_UNCLAIMED_OPENS as u64 + 1;
        open(&mut state, &mut queue, past, "p");
        assert_eq!(last_frame(&mut queue), Some(encode_reject(past)));
        assert_eq!(state.unclaimed_count, MAX_UNCLAIMED_OPENS);
        assert!(!state.remote.contains_key(&(past as u32)));
        // The id counts as used: the next open follows it.
        open(&mut state, &mut queue, past + 1, "p");
        assert_eq!(last_frame(&mut queue), Some(encode_reject(past + 1)));
    }

    #[test]
    fn while_corked_a_close_follows_the_channels_messages() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let ch = create(&mut state, &mut queue, "p");
        open(&mut state, &mut queue, 1, "p");
        queue.clear();

        handle_command(MuxCommand::Cork, &mut state, &mut queue);
        send_message_internal(&mut state, &mut queue, ch.local_id, ch.generation, 0, b"last");
        close(&mut state, &mut queue, &ch);
        let reused = create(&mut state, &mut queue, "q");
        assert_eq!(reused.local_id, ch.local_id);
        assert!(queue.is_empty(), "a frame went out ahead of the batch");
        handle_command(MuxCommand::Uncork, &mut state, &mut queue);

        // The batch, decoded as a Node peer decodes it: the message, then the
        // close, then the open of the reused id.
        let frame = last_frame(&mut queue).unwrap();
        let mut peer = MuxState::new();
        let mut peer_queue = WriteQueue::new();
        let mut peer_ch = create(&mut peer, &mut peer_queue, "p");
        open(&mut peer, &mut peer_queue, 1, "p");
        handle(&mut peer, &mut peer_queue, &frame).unwrap();
        assert!(matches!(peer_ch.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        assert!(matches!(
            peer_ch.event_rx.try_recv(),
            Ok(ChannelEvent::Message { data, .. }) if data == b"last"
        ));
        assert!(matches!(peer_ch.event_rx.try_recv(), Ok(ChannelEvent::Closed { is_remote: true })));
        assert!(peer.remote.get(&1).is_some_and(|rc| rc.local_id.is_none()), "the reused id's open");
    }

    #[test]
    fn a_corked_batch_is_sent_in_parts_of_at_most_max_batch() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let ch = create(&mut state, &mut queue, "p");
        queue.clear();
        handle_command(MuxCommand::Cork, &mut state, &mut queue);
        let item = vec![7; 1024 * 1024];
        for _ in 0..20 {
            send_message_internal(&mut state, &mut queue, ch.local_id, ch.generation, 0, &item);
        }
        handle_command(MuxCommand::Uncork, &mut state, &mut queue);
        assert!(queue.len() >= 3, "{} frames", queue.len());
        assert!(queue.iter().all(|out| out.frame.len() <= MAX_BATCH));
        let total: usize = queue.iter().map(|out| out.frame.len()).sum();
        assert!(total >= 20 * item.len());

        // Two items, each small enough to send, are not put in one frame
        // too large to.
        queue.clear();
        handle_command(MuxCommand::Cork, &mut state, &mut queue);
        let (first, second) = (vec![1; 7 * 1024 * 1024], vec![2; 10 * 1024 * 1024]);
        send_message_internal(&mut state, &mut queue, ch.local_id, ch.generation, 0, &first);
        send_message_internal(&mut state, &mut queue, ch.local_id, ch.generation, 0, &second);
        handle_command(MuxCommand::Uncork, &mut state, &mut queue);
        let sizes: Vec<usize> = queue.iter().map(|out| out.frame.len()).collect();
        assert_eq!(sizes.len(), 2, "{sizes:?}");
        assert!(sizes.iter().all(|&size| size < second.len() + 64), "{sizes:?}");

        // An item too large for a batch goes out, after the batch before it,
        // as the frame it would be uncorked: corking adds nothing to it.
        queue.clear();
        handle_command(MuxCommand::Cork, &mut state, &mut queue);
        send_message_internal(&mut state, &mut queue, ch.local_id, ch.generation, 0, b"before");
        let handshake = vec![3; 16_777_198 - 32];
        let corked = create_channel_internal(&mut state, &mut queue, "big", None, Some(handshake.clone()))
            .unwrap();
        handle_command(MuxCommand::Uncork, &mut state, &mut queue);
        let frames: Vec<Vec<u8>> = queue.drain(..).map(|out| out.frame).collect();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], encode_batch(&[(ch.local_id as u64, b"\0before".to_vec())]));
        assert_eq!(frames[1], encode_open(corked.local_id as u64, "big", None, Some(&handshake)));
    }

    #[tokio::test(start_paused = true)]
    async fn accept_pairs_with_the_open_it_was_told_of_and_no_other() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let (notify, mut requests) = mpsc::channel(MAX_UNCLAIMED_OPENS);
        state.pairings.insert(
            MuxState::channel_key("p", None),
            Pairing { registration: 0, notify, queued: Arc::new(AtomicUsize::new(0)) },
        );
        open(&mut state, &mut queue, 1, "p");
        let first = requests.try_recv().unwrap();
        // Closed, and the id opened again for another channel.
        handle(&mut state, &mut queue, &encode_close(1)).unwrap();
        open(&mut state, &mut queue, 1, "p");
        let second = requests.try_recv().unwrap();
        queue.clear();

        let stale = accept_channel_internal(&mut state, &mut queue, "p", None, None, first.open);
        assert!(matches!(stale, Err(ProtomuxError::ChannelClosed)));
        assert!(queue.is_empty(), "opened a channel for a stale notice");

        let mut ch = accept_channel_internal(&mut state, &mut queue, "p", None, None, second.open).unwrap();
        assert!(matches!(ch.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        assert_eq!(state.unclaimed_count, 0);
        assert!(state.infos.is_empty());

        // Two opens of one protocol and id are accepted in the order the
        // remote sent them, which is the order it takes our answers in.
        open(&mut state, &mut queue, 2, "p");
        open(&mut state, &mut queue, 3, "p");
        let (third, fourth) = (requests.try_recv().unwrap(), requests.try_recv().unwrap());
        let early = accept_channel_internal(&mut state, &mut queue, "p", None, None, fourth.open);
        assert!(matches!(early, Err(ProtomuxError::NotNextOpen)));
        accept_channel_internal(&mut state, &mut queue, "p", None, None, third.open).unwrap();
        accept_channel_internal(&mut state, &mut queue, "p", None, None, fourth.open).unwrap();

        // A request is of the mux it came to.
        let mut other = MuxState::new();
        open(&mut other, &mut queue, 1, "p");
        open(&mut state, &mut queue, 4, "p");
        let mut foreign = requests.try_recv().unwrap();
        foreign.open.remote_id = 1;
        foreign.open.seq = 0;
        let wrong = accept_channel_internal(&mut other, &mut queue, "p", None, None, foreign.open);
        assert!(matches!(wrong, Err(ProtomuxError::NotNextOpen)));
        handle(&mut state, &mut queue, &encode_close(4)).unwrap();

        // Expired, it cannot be accepted either.
        open(&mut state, &mut queue, 5, "p");
        let expired = requests.try_recv().unwrap();
        expire_unclaimed(&mut state, &mut queue, Instant::now() + UNCLAIMED_OPEN_GRACE).unwrap();
        let late = accept_channel_internal(&mut state, &mut queue, "p", None, None, expired.open);
        assert!(matches!(late, Err(ProtomuxError::ChannelClosed)));
    }

    #[test]
    fn ids_the_remote_is_done_with_take_no_room() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        for id in 1..=10_000 {
            open(&mut state, &mut queue, id, "p");
            handle(&mut state, &mut queue, &encode_close(id)).unwrap();
            queue.clear();
        }
        assert_eq!(state.remote_ids, 10_000);
        assert!(state.remote.is_empty());
        assert!(state.unclaimed.len() <= 2 * MAX_UNCLAIMED_OPENS + 1);
    }

    #[test]
    fn messages_held_for_an_unpaired_channel_pause_reading_until_it_pairs() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        open(&mut state, &mut queue, 1, "p");
        let mut sent = 0;
        while !state.paused() {
            handle(&mut state, &mut queue, &encode_message(1, 0, &[sent as u8; 1000])).unwrap();
            sent += 1;
        }
        assert!(sent > MAX_BUFFERED / 1100, "paused after {sent}");
        assert!(!state.reading());

        let mut ch = create(&mut state, &mut queue, "p");
        assert!(state.reading());
        assert_eq!(state.buffered, 0);
        assert!(matches!(ch.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        for n in 0..sent {
            assert!(matches!(
                ch.event_rx.try_recv(),
                Ok(ChannelEvent::Message { data, .. }) if data == vec![n as u8; 1000]
            ));
        }
    }

    #[test]
    fn a_batch_stops_where_the_mux_holds_too_much_and_goes_on_later() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        open(&mut state, &mut queue, 1, "p");
        let items: Vec<(u64, Vec<u8>)> = (0..1000u16)
            .map(|n| {
                let mut item = vec![0]; // message type
                item.extend_from_slice(&n.to_le_bytes());
                (1, item)
            })
            .collect();
        handle(&mut state, &mut queue, &encode_batch(&items)).unwrap();
        assert!(state.paused());
        assert!(state.batch_rest.is_some());
        let held = state.remote[&1].pending.len();
        assert!(held < 1000, "held all {held}");

        let mut ch = create(&mut state, &mut queue, "p");
        resume_batch(&mut state, &mut queue).unwrap();
        assert!(state.batch_rest.is_none());
        assert!(matches!(ch.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        for n in 0..1000u16 {
            assert!(matches!(
                ch.event_rx.try_recv(),
                Ok(ChannelEvent::Message { data, .. }) if data == n.to_le_bytes()
            ), "message {n}");
        }
    }

    #[test]
    fn what_an_open_waiting_for_a_pair_carries_counts_against_the_buffer() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let big = encode_open(1, "p", Some(&[1; 20_000]), Some(&[2; 20_000]));
        handle(&mut state, &mut queue, &big).unwrap();
        assert!(state.paused());

        handle(&mut state, &mut queue, &encode_close(1)).unwrap();
        assert_eq!(state.buffered, 0);
        assert!(state.reading());
    }

    #[test]
    fn a_remote_close_of_an_unpaired_channel_is_answered_and_frees_what_it_held() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        open(&mut state, &mut queue, 1, "p");
        handle(&mut state, &mut queue, &encode_message(1, 0, &[1; 100])).unwrap();
        handle(&mut state, &mut queue, &encode_close(1)).unwrap();
        // Node's opener waits for this answer to free its id.
        assert_eq!(last_frame(&mut queue), Some(encode_reject(1)));
        assert_eq!((state.unclaimed_count, state.buffered), (0, 0));
        assert!(state.infos.is_empty());

        // A channel created after it does not pair with it.
        create(&mut state, &mut queue, "p");
        assert!(state.local[0].as_ref().is_some_and(|ch| !ch.opened));
    }

    #[test]
    fn a_channel_closed_here_frees_the_remotes_id() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let ch = create(&mut state, &mut queue, "p");
        open(&mut state, &mut queue, 1, "p");
        close(&mut state, &mut queue, &ch);
        assert!(state.remote.is_empty());
        assert_eq!(state.free_ids, vec![0]);

        // What the remote still sends on it is dropped, not held.
        handle(&mut state, &mut queue, &encode_message(1, 0, &[1; 100])).unwrap();
        assert_eq!(state.buffered, 0);
        // And it may open the id again.
        open(&mut state, &mut queue, 1, "q");
        assert_eq!(state.unclaimed_count, 1);
    }

    #[test]
    fn a_channel_the_remote_closed_frees_its_id_and_its_handle_reaches_no_other() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let mut old = create(&mut state, &mut queue, "p");
        open(&mut state, &mut queue, 1, "p");
        handle(&mut state, &mut queue, &encode_close(1)).unwrap();
        assert!(matches!(old.event_rx.try_recv(), Ok(ChannelEvent::Opened { .. })));
        assert!(matches!(old.event_rx.try_recv(), Ok(ChannelEvent::Closed { is_remote: true })));
        assert_eq!(state.free_ids, vec![0]);

        let new = create(&mut state, &mut queue, "q");
        assert_eq!(new.local_id, old.local_id);
        queue.clear();
        send_message_internal(&mut state, &mut queue, old.local_id, old.generation, 0, b"stale");
        close(&mut state, &mut queue, &old);
        assert!(queue.is_empty(), "the old handle reached the new channel");
        assert!(state.local[0].as_ref().is_some_and(|ch| ch.generation == new.generation));
    }

    #[test]
    fn channels_opened_and_closed_leave_no_infos_behind() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        for n in 0..100u32 {
            let protocol = format!("p{n}");
            let ch = create(&mut state, &mut queue, &protocol);
            open(&mut state, &mut queue, 1, &protocol);
            handle(&mut state, &mut queue, &encode_close(1)).unwrap();
            close(&mut state, &mut queue, &ch);

            let rejected = create(&mut state, &mut queue, &protocol);
            handle(&mut state, &mut queue, &encode_reject(rejected.local_id as u64)).unwrap();

            let closed = create(&mut state, &mut queue, &protocol);
            close(&mut state, &mut queue, &closed);
            handle(&mut state, &mut queue, &encode_reject(closed.local_id as u64)).unwrap();
        }
        assert!(state.infos.is_empty(), "{} infos left", state.infos.len());
        assert_eq!(state.local.len(), 1);
    }

    #[test]
    fn a_remote_that_does_not_read_our_rejects_has_the_mux_end() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let mut ended = false;
        for _ in 0..MAX_UNSENT_REPLIES {
            open(&mut state, &mut queue, 1, "p");
            if handle(&mut state, &mut queue, &encode_close(1)).is_err() {
                ended = true;
                break;
            }
        }
        assert!(ended);
    }

    #[tokio::test]
    async fn a_listener_is_told_of_an_open_and_a_channel_made_for_it_pairs() {
        let (stream_a, stream_b) = mem_pair();
        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);
        let mut listener = mux_b.pair("p", None);
        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let mut ch_a = mux_a.create_channel("p", Some(vec![9]), None).await.unwrap();
        let request = listener.recv().await.unwrap();
        assert_eq!((request.protocol.as_str(), request.id.as_deref()), ("p", Some(&[9][..])));
        assert_eq!(listener.queued.load(Ordering::Relaxed), 0);
        let mut ch_b = mux_b.accept(&request, None).await.unwrap();
        ch_a.wait_opened().await.unwrap();
        ch_b.wait_opened().await.unwrap();

        // A listener for the exact id is told instead of the one for any id.
        let mut exact = mux_b.pair("p", Some(vec![8]));
        let _ch = mux_a.create_channel("p", Some(vec![8]), None).await.unwrap();
        assert_eq!(exact.recv().await.unwrap().id, Some(vec![8]));
        assert!(listener.requests.try_recv().is_err());

        // Once dropped, no listener is told, and the open is rejected.
        drop(exact);
        drop(listener);
        let mut unwanted = mux_a.create_channel("p", Some(vec![7]), None).await.unwrap();
        let result = tokio::time::timeout(UNCLAIMED_OPEN_GRACE * 5, unwanted.wait_opened())
            .await
            .expect("an open nothing pairs with is rejected");
        assert!(matches!(result, Err(ProtomuxError::ChannelClosed)));
    }

    #[test]
    fn a_listener_that_does_not_keep_up_is_not_sent_more_than_its_share() {
        let mut state = MuxState::new();
        let mut queue = WriteQueue::new();
        let (notify, mut requests) = mpsc::channel(MAX_UNCLAIMED_OPENS);
        let queued = Arc::new(AtomicUsize::new(0));
        state.pairings.insert(
            MuxState::channel_key("p", None),
            Pairing { registration: 0, notify, queued: Arc::clone(&queued) },
        );
        for id in 1..=10 {
            let frame = encode_open(id, "p", Some(&[id as u8; 20_000]), None);
            handle(&mut state, &mut queue, &frame).unwrap();
        }
        assert!(queued.load(Ordering::Relaxed) <= MAX_QUEUED_NOTICES);
        let mut told = 0;
        while requests.try_recv().is_ok() {
            told += 1;
        }
        assert_eq!(told, MAX_QUEUED_NOTICES / notice_size("p", Some(&[0; 20_000])));
    }
}
