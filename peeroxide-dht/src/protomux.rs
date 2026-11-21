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

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
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
        response: oneshot::Sender<Result<ChannelHandle>>,
    },
    SendMessage {
        local_id: u32,
        message_type: u32,
        data: Vec<u8>,
    },
    CloseChannel {
        local_id: u32,
    },
    Cork,
    Uncork,
}

/// Raw channel handle returned from create — internal.
struct ChannelHandle {
    local_id: u32,
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
    /// `data` is the already-encoded message payload.
    pub fn send(&self, message_type: u32, data: &[u8]) -> Result<()> {
        if self.closed {
            return Err(ProtomuxError::ChannelClosed);
        }
        self.cmd_tx
            .send(MuxCommand::SendMessage {
                local_id: self.local_id,
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
    /// must be spawned (e.g. via `tokio::spawn`).
    ///
    /// Over a stream that can be split ([`FramedStream::into_framed_halves`]),
    /// as a [`SecretStream`](crate::secret_stream::SecretStream) can, the mux
    /// reads and writes at the same time. Over one that cannot, it reads
    /// nothing while a write waits.
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
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(MuxCommand::CreateChannel {
                protocol: protocol.to_string(),
                id: id.clone(),
                handshake,
                response: resp_tx,
            })
            .map_err(|_| ProtomuxError::StreamClosed)?;

        let handle = resp_rx
            .await
            .map_err(|_| ProtomuxError::StreamClosed)??;

        Ok(Channel {
            local_id: handle.local_id,
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
}

impl Clone for Mux {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
        }
    }
}

// ── Mux actor state ──────────────────────────────────────────────────────────

#[allow(dead_code)] // Fields used for channel matching in M7.3+
struct LocalChannel {
    protocol: String,
    id: Option<Vec<u8>>,
    remote_id: Option<u32>,
    event_tx: mpsc::UnboundedSender<ChannelEvent>,
    opened: bool,
    closed: bool,
}

struct RemoteChannel {
    /// Handshake state buffer from the Open frame (consumed on pairing).
    handshake_state: Option<Vec<u8>>,
    /// Paired local channel ID (set when pairing completes).
    local_id: Option<u32>,
    /// Messages buffered before the channel was fully opened locally.
    pending: Vec<(u64, Vec<u8>)>,
}

struct ChannelInfo {
    /// Local channel IDs that sent Open but haven't been paired yet.
    outgoing: Vec<u32>,
    /// Remote channel IDs received via Open that haven't been paired yet.
    incoming: Vec<u32>,
}

struct MuxState {
    local: Vec<Option<LocalChannel>>,
    free_ids: Vec<u32>,
    remote: Vec<Option<RemoteChannel>>,
    infos: HashMap<String, ChannelInfo>,
    /// Cork state: when >0, writes are batched.
    cork_depth: u32,
    /// Batched entries: (local_channel_id, inner_data).
    batch: Vec<(u64, Vec<u8>)>,
}

impl MuxState {
    fn new() -> Self {
        Self {
            local: Vec::new(),
            free_ids: Vec::new(),
            remote: Vec::new(),
            infos: HashMap::new(),
            cork_depth: 0,
            batch: Vec::new(),
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
}

// ── Mux actor run loop ───────────────────────────────────────────────────────

/// Drive the Protomux. This future must be spawned.
async fn run_mux<S: FramedStream>(mut stream: S, mut cmd_rx: mpsc::UnboundedReceiver<MuxCommand>) {
    let mut state = MuxState::new();
    let mut write_queue: VecDeque<Vec<u8>> = VecDeque::new();

    loop {
        // Flush pending writes before entering select
        while let Some(frame) = write_queue.pop_front() {
            if let Err(e) = stream.write_frame(&frame).await {
                warn!(err = %e, "protomux write error");
                shutdown_all(&mut state);
                return;
            }
        }

        tokio::select! {
            result = stream.read_frame() => {
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
        }
    }
}

/// How many frames the reader of a split stream reads ahead of the mux.
const FRAMES_READ_AHEAD: usize = 16;

/// Drive the Protomux over a stream split into halves.
///
/// The reader and the writer each run on their own, so the mux goes on
/// reading while a write waits for the remote to make room, and a read is
/// never cancelled part way through a frame.
async fn run_split_mux(halves: FramedHalves, cmd_rx: mpsc::UnboundedReceiver<MuxCommand>) {
    let FramedHalves { mut read, mut write } = halves;
    let (incoming_tx, incoming_rx) = mpsc::channel(FRAMES_READ_AHEAD);
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (write_failed_tx, write_failed_rx) = oneshot::channel();

    // Ends only when the mux does.
    let reader = async move {
        loop {
            let result = read.read_frame_boxed().await;
            let last = !matches!(result, Ok(Some(_)));
            if incoming_tx.send(result).await.is_err() || last {
                break;
            }
        }
        std::future::pending::<()>().await
    };
    // Ends once the mux has nothing more for it, or a write fails.
    let writer = async move {
        while let Some(frame) = outgoing_rx.recv().await {
            if let Err(e) = write.write_frame_boxed(&frame).await {
                let _ = write_failed_tx.send(e);
                return;
            }
        }
    };
    let mux = split_mux_state(cmd_rx, incoming_rx, outgoing_tx, write_failed_rx);
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
    mut incoming_rx: mpsc::Receiver<std::io::Result<Option<Vec<u8>>>>,
    outgoing_tx: mpsc::UnboundedSender<Vec<u8>>,
    mut write_failed_rx: oneshot::Receiver<std::io::Error>,
) -> bool {
    let mut state = MuxState::new();
    let mut write_queue: VecDeque<Vec<u8>> = VecDeque::new();
    let mut writer_done = false;

    loop {
        tokio::select! {
            incoming = incoming_rx.recv() => match incoming {
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
                    for frame in write_queue.drain(..) {
                        let _ = outgoing_tx.send(frame);
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
        }

        for frame in write_queue.drain(..) {
            let _ = outgoing_tx.send(frame);
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

fn handle_command(cmd: MuxCommand, state: &mut MuxState, write_queue: &mut VecDeque<Vec<u8>>) {
    match cmd {
        MuxCommand::CreateChannel {
            protocol,
            id,
            handshake,
            response,
        } => {
            let result = create_channel_internal(state, write_queue, &protocol, id, handshake);
            let _ = response.send(result);
        }
        MuxCommand::SendMessage {
            local_id,
            message_type,
            data,
        } => {
            send_message_internal(state, write_queue, local_id, message_type, &data);
        }
        MuxCommand::CloseChannel { local_id } => {
            close_channel_internal(state, write_queue, local_id);
        }
        MuxCommand::Cork => {
            state.cork_depth += 1;
        }
        MuxCommand::Uncork => {
            if state.cork_depth > 0 {
                state.cork_depth -= 1;
                if state.cork_depth == 0 && !state.batch.is_empty() {
                    let entries: Vec<(u64, Vec<u8>)> = std::mem::take(&mut state.batch);
                    let frame = encode_batch(&entries);
                    if !frame.is_empty() {
                        write_queue.push_back(frame);
                    }
                }
            }
        }
    }
}

fn create_channel_internal(
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake: Option<Vec<u8>>,
) -> Result<ChannelHandle> {
    let internal_id = state.alloc_local_id();
    let local_id_wire = internal_id + 1; // 1-indexed on wire

    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let key = MuxState::channel_key(protocol, id.as_deref());

    let ch = LocalChannel {
        protocol: protocol.to_string(),
        id: id.clone(),
        remote_id: None,
        event_tx: event_tx.clone(),
        opened: false,
        closed: false,
    };
    state.local[internal_id as usize] = Some(ch);

    // Check if remote already sent Open for this protocol+id
    let info = state.infos.entry(key).or_insert_with(|| ChannelInfo {
        outgoing: Vec::new(),
        incoming: Vec::new(),
    });

    if let Some(remote_id) = info.incoming.first().copied() {
        // Pair with pending remote channel
        info.incoming.remove(0);

        if let Some(ch) = &mut state.local[internal_id as usize] {
            ch.remote_id = Some(remote_id);
        }

        // Get handshake state from remote
        let remote_hs = if let Some(rc) = state.remote.get_mut(remote_id as usize - 1) {
            if let Some(rc) = rc.as_mut() {
                rc.local_id = Some(internal_id);
                rc.handshake_state.take()
            } else {
                None
            }
        } else {
            None
        };

        // Send our Open
        let frame = encode_open(
            local_id_wire as u64,
            protocol,
            id.as_deref(),
            handshake.as_deref(),
        );
        enqueue_write(state, write_queue, frame);

        // Fully open: notify the channel
        let _ = event_tx.send(ChannelEvent::Opened {
            handshake: remote_hs,
        });
        if let Some(ch) = &mut state.local[internal_id as usize] {
            ch.opened = true;
        }

        // Drain any buffered messages
        if let Some(Some(rc)) = state.remote.get_mut(remote_id as usize - 1) {
            let pending: Vec<_> = std::mem::take(&mut rc.pending);
            for (msg_type, data) in pending {
                let _ = event_tx.send(ChannelEvent::Message {
                    message_type: msg_type as u32,
                    data,
                });
            }
        }
    } else {
        // No pending remote — register as outgoing and send Open
        info.outgoing.push(local_id_wire);

        let frame = encode_open(
            local_id_wire as u64,
            protocol,
            id.as_deref(),
            handshake.as_deref(),
        );
        enqueue_write(state, write_queue, frame);
    }

    Ok(ChannelHandle {
        local_id: local_id_wire,
        event_rx,
    })
}

fn send_message_internal(
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
    local_id: u32,
    message_type: u32,
    data: &[u8],
) {
    let internal_id = (local_id - 1) as usize;
    if internal_id >= state.local.len() {
        return;
    }
    let Some(Some(_ch)) = state.local.get(internal_id) else {
        return;
    };

    if state.cork_depth > 0 {
        // Batch mode: encode inner message (type + payload)
        let mut inner_state = State::new();
        c::preencode_uint(&mut inner_state, message_type as u64);
        inner_state.end += data.len();
        inner_state.alloc();
        c::encode_uint(&mut inner_state, message_type as u64);
        inner_state.buffer[inner_state.start..inner_state.start + data.len()]
            .copy_from_slice(data);
        state.batch.push((local_id as u64, inner_state.buffer));
    } else {
        let frame = encode_message(local_id as u64, message_type as u64, data);
        write_queue.push_back(frame);
    }
}

fn close_channel_internal(
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
    local_id: u32,
) {
    let internal_id = (local_id - 1) as usize;
    if internal_id >= state.local.len() {
        return;
    }

    if let Some(ch) = &mut state.local[internal_id] {
        if ch.closed {
            return;
        }
        ch.closed = true;
        let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: false });
    }

    // Send Close frame
    let frame = encode_close(local_id as u64);
    enqueue_write(state, write_queue, frame);

    // Free the local ID, and take back an open still waiting for its pair, so
    // that a later open from the remote cannot pair with the id's next user.
    state.local[internal_id] = None;
    state.free_ids.push(internal_id as u32);
    for info in state.infos.values_mut() {
        info.outgoing.retain(|&id| id != local_id);
    }
}

fn enqueue_write(state: &MuxState, write_queue: &mut VecDeque<Vec<u8>>, frame: Vec<u8>) {
    if state.cork_depth > 0 {
        // When corked, control frames still go directly
        // (Node.js `_write0` sends through batch for control messages, but
        //  we simplify: control frames always go directly.)
        write_queue.push_back(frame);
    } else {
        write_queue.push_back(frame);
    }
}

// ── Incoming frame handling ──────────────────────────────────────────────────

fn handle_incoming_frame(
    data: &[u8],
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
) -> Result<()> {
    let frame = decode_frame(data)?;
    match frame {
        DecodedFrame::Control(ctrl) => handle_control(ctrl, state, write_queue),
        DecodedFrame::Message {
            channel_id,
            message_type,
            payload,
        } => {
            dispatch_message(state, channel_id as u32, message_type, &payload);
            Ok(())
        }
        DecodedFrame::Batch(items) => {
            for item in items {
                dispatch_batch_item(state, write_queue, item)?;
            }
            Ok(())
        }
    }
}

fn handle_control(
    ctrl: ControlFrame,
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
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
            handle_remote_close(state, remote_local_id);
            Ok(())
        }
        ControlFrame::Reject { remote_id } => {
            handle_remote_reject(state, remote_id);
            Ok(())
        }
    }
}

fn handle_remote_open(
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
    remote_local_id: u64,
    protocol: &str,
    id: Option<Vec<u8>>,
    handshake_state: Option<Vec<u8>>,
) -> Result<()> {
    // Reject remote_id == 0 (control session)
    if remote_local_id == 0 {
        let frame = encode_reject(0);
        write_queue.push_back(frame);
        return Ok(());
    }

    // A remote opens each channel at an id it has used before or at the next
    // one, as Node's `_onopensession` allows: an id further on would have us
    // grow the list of remote channels to any length it chose.
    if remote_local_id > state.remote.len() as u64 + 1 {
        return Err(ProtomuxError::InvalidFrame(format!(
            "remote channel id {remote_local_id} skips ahead of {}",
            state.remote.len()
        )));
    }
    let remote_id = remote_local_id as u32; // Remote's local_id is our remote_id
    let rid = (remote_id - 1) as usize;
    if rid == state.remote.len() {
        state.remote.push(None);
    }

    if state.remote[rid].is_some() {
        return Err(ProtomuxError::InvalidFrame("duplicate remote channel ID".into()));
    }

    let key = MuxState::channel_key(protocol, id.as_deref());
    let info = state.infos.entry(key).or_insert_with(|| ChannelInfo {
        outgoing: Vec::new(),
        incoming: Vec::new(),
    });

    // Check if we have an outgoing channel waiting for this protocol+id
    if let Some(pos) = info.outgoing.first().copied() {
        info.outgoing.remove(0);
        let internal_id = (pos - 1) as usize;

        state.remote[rid] = Some(RemoteChannel {
            handshake_state: handshake_state.clone(),
            local_id: Some(internal_id as u32),
            pending: Vec::new(),
        });

        if let Some(ch) = &mut state.local[internal_id] {
            ch.remote_id = Some(remote_id);
            ch.opened = true;
            let _ = ch.event_tx.send(ChannelEvent::Opened {
                handshake: handshake_state,
            });
        }

        debug!(
            protocol,
            local_id = pos,
            remote_id,
            "protomux channel fully opened"
        );
    } else {
        // No local channel yet — buffer as incoming
        state.remote[rid] = Some(RemoteChannel {
            handshake_state,
            local_id: None,
            pending: Vec::new(),
        });
        info.incoming.push(remote_id);
        debug!(
            protocol,
            remote_id,
            "protomux remote channel queued (awaiting local create)"
        );
    }

    Ok(())
}

fn handle_remote_close(state: &mut MuxState, remote_local_id: u64) {
    let remote_id = remote_local_id as u32;
    if remote_id == 0 {
        return;
    }
    let rid = (remote_id - 1) as usize;

    if let Some(Some(rc)) = state.remote.get(rid) {
        if let Some(local_id) = rc.local_id {
            let internal_id = local_id as usize;
            if let Some(ch) = state.local.get_mut(internal_id).and_then(|c| c.as_mut()) {
                if !ch.closed {
                    ch.closed = true;
                    let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
                }
            }
        }
    }

    if rid < state.remote.len() {
        state.remote[rid] = None;
    }
}

fn handle_remote_reject(state: &mut MuxState, remote_id: u64) {
    // Remote rejected one of our channels. As in Node's `_onrejectsession`,
    // only an open still waiting for its pair can be rejected; a reject for
    // any other id is ignored.
    let Ok(local_id_wire) = u32::try_from(remote_id) else {
        return;
    };
    let Some(info) = state
        .infos
        .values_mut()
        .find(|info| info.outgoing.contains(&local_id_wire))
    else {
        return;
    };
    info.outgoing.retain(|&id| id != local_id_wire);

    let internal_id = (local_id_wire - 1) as usize;
    // A channel closed locally has given its id back already.
    let Some(mut ch) = state.local.get_mut(internal_id).and_then(Option::take) else {
        return;
    };
    if !ch.closed {
        ch.closed = true;
        let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
    }
    state.free_ids.push(internal_id as u32);
}

fn dispatch_message(state: &mut MuxState, remote_id: u32, message_type: u64, payload: &[u8]) {
    if remote_id == 0 {
        return;
    }
    let rid = (remote_id - 1) as usize;

    let Some(Some(rc)) = state.remote.get_mut(rid) else {
        return;
    };

    if let Some(local_id) = rc.local_id {
        let internal_id = local_id as usize;
        if let Some(ch) = state.local.get(internal_id).and_then(|c| c.as_ref()) {
            if ch.opened {
                let _ = ch.event_tx.send(ChannelEvent::Message {
                    message_type: message_type as u32,
                    data: payload.to_vec(),
                });
                return;
            }
        }
    }

    // Buffer the message (channel not yet fully opened)
    rc.pending.push((message_type, payload.to_vec()));
}

fn dispatch_batch_item(
    state: &mut MuxState,
    write_queue: &mut VecDeque<Vec<u8>>,
    item: BatchItem,
) -> Result<()> {
    // A control frame batched by a corked Node peer (`_write0`), without its
    // leading channel 0. Node decodes it as a control frame; a batch within a
    // batch is never sent.
    if item.channel_id == 0 {
        // Refuse a batch within a batch before decoding any of it.
        let mut s = State::from_buffer(&item.data);
        if c::decode_uint(&mut s)? == CTRL_BATCH {
            return Err(ProtomuxError::InvalidFrame("batch within a batch".into()));
        }
        let mut frame = Vec::with_capacity(1 + item.data.len());
        frame.push(0);
        frame.extend_from_slice(&item.data);
        return match decode_frame(&frame)? {
            DecodedFrame::Control(ctrl) => handle_control(ctrl, state, write_queue),
            _ => Err(ProtomuxError::InvalidFrame("batch within a batch".into())),
        };
    }

    // Each batch item's data is: [uint:messageType][payload]
    let mut s = State::from_buffer(&item.data);
    let message_type = c::decode_uint(&mut s)?;
    let payload = s.buffer[s.start..s.end].to_vec();
    dispatch_message(state, item.channel_id as u32, message_type, &payload);
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

    #[test]
    fn an_open_that_skips_ahead_is_an_error_and_allocates_nothing() {
        let mut state = MuxState::new();
        let mut queue = VecDeque::new();
        for id in [0xFFFF_FFFF, u64::MAX, 2] {
            let frame = encode_open(id, "p", None, None);
            assert!(handle_incoming_frame(&frame, &mut state, &mut queue).is_err(), "{id}");
            assert!(state.remote.is_empty(), "{id}");
        }

        for id in [1, 2] {
            let frame = encode_open(id, "p", None, None);
            handle_incoming_frame(&frame, &mut state, &mut queue).unwrap();
        }
        assert_eq!(state.remote.len(), 2);
    }

    #[test]
    fn a_reject_for_a_channel_never_opened_is_ignored() {
        let mut state = MuxState::new();
        let mut queue = VecDeque::new();
        state.alloc_local_id();
        for id in [0, 1, 1, 9, u64::MAX] {
            handle_incoming_frame(&encode_reject(id), &mut state, &mut queue).unwrap();
        }
        assert!(state.free_ids.is_empty(), "{:?}", state.free_ids);
    }

    #[test]
    fn a_reject_closes_only_an_open_waiting_for_its_pair() {
        let mut state = MuxState::new();
        let mut queue = VecDeque::new();
        let mut rejected = create_channel_internal(&mut state, &mut queue, "p", None, None).unwrap();
        for _ in 0..2 {
            let frame = encode_reject(rejected.local_id as u64);
            handle_incoming_frame(&frame, &mut state, &mut queue).unwrap();
        }
        assert!(matches!(
            rejected.event_rx.try_recv(),
            Ok(ChannelEvent::Closed { is_remote: true })
        ));
        assert_eq!(state.free_ids, vec![0]);

        // Once paired, the channel is not the remote's to reject.
        let paired = create_channel_internal(&mut state, &mut queue, "q", None, None).unwrap();
        handle_incoming_frame(&encode_open(1, "q", None, None), &mut state, &mut queue).unwrap();
        let frame = encode_reject(paired.local_id as u64);
        handle_incoming_frame(&frame, &mut state, &mut queue).unwrap();
        assert!(state.local[0].is_some());
        assert!(state.free_ids.is_empty());
    }

    #[test]
    fn an_open_does_not_pair_with_the_next_user_of_a_closed_channels_id() {
        let mut state = MuxState::new();
        let mut queue = VecDeque::new();
        let closed = create_channel_internal(&mut state, &mut queue, "p", None, None).unwrap();
        close_channel_internal(&mut state, &mut queue, closed.local_id);
        let reused = create_channel_internal(&mut state, &mut queue, "r", None, None).unwrap();
        assert_eq!(reused.local_id, closed.local_id);

        handle_incoming_frame(&encode_open(1, "p", None, None), &mut state, &mut queue).unwrap();
        let remote = state.remote[0].as_ref().unwrap();
        assert_eq!(remote.local_id, None, "paired with a channel of another protocol");
    }

    /// A corked Node peer batches its control frames on channel 0
    /// (`_write0`): the open in the batch counts, so the next open follows it.
    #[test]
    fn an_open_in_a_batch_is_handled_as_one() {
        let mut state = MuxState::new();
        let mut queue = VecDeque::new();
        let batched_open = encode_open(1, "p", None, None)[1..].to_vec();
        let batch = encode_batch(&[(0, batched_open)]);
        handle_incoming_frame(&batch, &mut state, &mut queue).unwrap();
        assert_eq!(state.remote.len(), 1);

        handle_incoming_frame(&encode_open(2, "p", None, None), &mut state, &mut queue).unwrap();
        assert_eq!(state.remote.len(), 2);

        let nested = encode_batch(&[(0, batch[1..].to_vec())]);
        assert!(handle_incoming_frame(&nested, &mut state, &mut queue).is_err());
    }

    #[test]
    fn a_batch_item_longer_than_any_frame_is_an_error() {
        let mut frame = vec![0, 0, 1, 0xff];
        frame.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode_frame(&frame), Err(ProtomuxError::InvalidFrame(_))));
    }
}
