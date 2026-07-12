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
                    let len = c::decode_uint(&mut state)? as usize;
                    if len == 0 {
                        // Channel switch
                        current_channel = c::decode_uint(&mut state)?;
                        continue;
                    }
                    if state.start + len > end {
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
    /// ```ignore
    /// let (mux, run) = Mux::new(stream);
    /// tokio::spawn(run);
    /// let channel = mux.create_channel("my-protocol", None, None).await?;
    /// ```
    pub fn new<S: FramedStream>(stream: S) -> (Self, impl std::future::Future<Output = ()> + Send) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let task = run_mux(stream, cmd_rx);
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
                    let entries: Vec<(u64, Vec<u8>)> = state.batch.drain(..).collect();
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
            let pending: Vec<_> = rc.pending.drain(..).collect();
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

    // Free the local ID
    state.local[internal_id] = None;
    state.free_ids.push(internal_id as u32);
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
                dispatch_batch_item(state, item)?;
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
    let remote_id = remote_local_id as u32; // Remote's local_id is our remote_id

    // Reject remote_id == 0 (control session)
    if remote_id == 0 {
        let frame = encode_reject(0);
        write_queue.push_back(frame);
        return Ok(());
    }

    // Grow remote channel list if needed
    let rid = (remote_id - 1) as usize;
    while state.remote.len() <= rid {
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
    // Remote rejected one of our channels
    let local_id_wire = remote_id as u32;
    let internal_id = (local_id_wire - 1) as usize;

    if let Some(ch) = state.local.get_mut(internal_id).and_then(|c| c.as_mut()) {
        if !ch.closed {
            ch.closed = true;
            let _ = ch.event_tx.send(ChannelEvent::Closed { is_remote: true });
        }
    }

    state.local[internal_id] = None;
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

fn dispatch_batch_item(state: &mut MuxState, item: BatchItem) -> Result<()> {
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
}
