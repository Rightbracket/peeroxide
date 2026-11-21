use crate::compact_encoding::{
    self, decode_fixed32, decode_fixed64, decode_uint, encode_fixed32, encode_fixed64, encode_uint,
    preencode_uint, EncodingError, State,
};
use crate::messages::Ipv4Peer;

/// Type alias for results returned by encoding and decoding functions in this module.
pub type Result<T> = std::result::Result<T, EncodingError>;

// ── HyperDHT command IDs ────────────────────────────────────────────────────

/// Command ID for peer handshake messages.
pub const PEER_HANDSHAKE: u64 = 0;
/// Command ID for peer hole-punch messages.
pub const PEER_HOLEPUNCH: u64 = 1;
/// Command ID for find-peer queries.
pub const FIND_PEER: u64 = 2;
/// Command ID for DHT lookup queries.
pub const LOOKUP: u64 = 3;
/// Command ID for announce messages.
pub const ANNOUNCE: u64 = 4;
/// Command ID for unannounce messages.
pub const UNANNOUNCE: u64 = 5;
/// Command ID for mutable put requests.
pub const MUTABLE_PUT: u64 = 6;
/// Command ID for mutable get requests.
pub const MUTABLE_GET: u64 = 7;
/// Command ID for immutable put requests.
pub const IMMUTABLE_PUT: u64 = 8;
/// Command ID for immutable get requests.
pub const IMMUTABLE_GET: u64 = 9;

// ── Handshake routing modes ─────────────────────────────────────────────────

/// Routing mode indicating the message originates from the client.
pub const MODE_FROM_CLIENT: u64 = 0;
/// Routing mode indicating the message originates from the server.
pub const MODE_FROM_SERVER: u64 = 1;
/// Routing mode indicating the message originates from a relay node.
pub const MODE_FROM_RELAY: u64 = 2;
/// Routing mode indicating the message originates from a second relay node.
pub const MODE_FROM_SECOND_RELAY: u64 = 3;
/// Routing mode indicating this is a reply message.
pub const MODE_REPLY: u64 = 4;

// ── Firewall constants ──────────────────────────────────────────────────────

/// Firewall state is unknown.
pub const FIREWALL_UNKNOWN: u64 = 0;
/// Firewall is open — NAT maps all connections to the same public address and port.
pub const FIREWALL_OPEN: u64 = 1;
/// Firewall uses a consistent port mapping for the same destination IP.
pub const FIREWALL_CONSISTENT: u64 = 2;
/// Firewall uses random port mapping for each new destination.
pub const FIREWALL_RANDOM: u64 = 3;

// ── Error constants ─────────────────────────────────────────────────────────

/// No error.
pub const ERROR_NONE: u64 = 0;
/// The operation was aborted.
pub const ERROR_ABORTED: u64 = 1;
/// Protocol version mismatch between peers.
pub const ERROR_VERSION_MISMATCH: u64 = 2;
/// The remote is busy; try again later.
pub const ERROR_TRY_LATER: u64 = 3;
/// The sequence number has already been used.
pub const ERROR_SEQ_REUSED: u64 = 16;
/// The sequence number is too low.
pub const ERROR_SEQ_TOO_LOW: u64 = 17;

// ── HyperPeer ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
/// A HyperDHT peer identified by its public key and optional relay addresses.
pub struct HyperPeer {
    /// The 32-byte Ed25519 public key identifying this peer.
    pub public_key: [u8; 32],
    /// IPv4 relay addresses through which this peer can be reached.
    pub relay_addresses: Vec<Ipv4Peer>,
}

/// Pre-encodes a [`HyperPeer`] to calculate the required buffer size.
pub fn preencode_hyper_peer(state: &mut State, peer: &HyperPeer) {
    state.end += 32;
    preencode_ipv4_peer_array(state, &peer.relay_addresses);
}

/// Encodes a [`HyperPeer`] into the compact-encoding buffer.
pub fn encode_hyper_peer(state: &mut State, peer: &HyperPeer) -> Result<()> {
    encode_fixed32(state, &peer.public_key);
    encode_ipv4_peer_array(state, &peer.relay_addresses)
}

/// Decodes a [`HyperPeer`] from the compact-encoding buffer.
pub fn decode_hyper_peer(state: &mut State) -> Result<HyperPeer> {
    let public_key = decode_fixed32(state)?;
    let relay_addresses = decode_ipv4_peer_array(state)?;
    Ok(HyperPeer {
        public_key,
        relay_addresses,
    })
}

/// Serializes a [`HyperPeer`] to a byte vector.
pub fn encode_hyper_peer_to_bytes(peer: &HyperPeer) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_hyper_peer(&mut state, peer);
    state.alloc();
    encode_hyper_peer(&mut state, peer)?;
    Ok(state.buffer)
}

/// Deserializes a [`HyperPeer`] from bytes.
pub fn decode_hyper_peer_from_bytes(buf: &[u8]) -> Result<HyperPeer> {
    let mut state = State::from_buffer(buf);
    decode_hyper_peer(&mut state)
}

// ── AnnounceMessage ─────────────────────────────────────────────────────────

const FLAG_PEER: u64 = 0x1;
const FLAG_REFRESH: u64 = 0x2;
const FLAG_SIGNATURE: u64 = 0x4;
const FLAG_BUMP: u64 = 0x8;

#[derive(Debug, Clone)]
/// Wire message for announcing a peer on the DHT.
pub struct AnnounceMessage {
    /// The peer being announced, if present.
    pub peer: Option<HyperPeer>,
    /// A 32-byte refresh token, if present.
    pub refresh: Option<[u8; 32]>,
    /// A 64-byte signature over the announcement, if present.
    pub signature: Option<[u8; 64]>,
    /// Bump counter used to force re-announcement.
    pub bump: u64,
}

/// Pre-encodes an [`AnnounceMessage`] to calculate the required buffer size.
pub fn preencode_announce(state: &mut State, m: &AnnounceMessage) {
    state.end += 1; // flags byte (always fits in 1 byte, max 15)
    if let Some(peer) = &m.peer {
        preencode_hyper_peer(state, peer);
    }
    if m.refresh.is_some() {
        state.end += 32;
    }
    if m.signature.is_some() {
        state.end += 64;
    }
    if m.bump != 0 {
        preencode_uint(state, m.bump);
    }
}

/// Encodes an [`AnnounceMessage`] into the compact-encoding buffer.
pub fn encode_announce(state: &mut State, m: &AnnounceMessage) -> Result<()> {
    let flags = (if m.peer.is_some() { FLAG_PEER } else { 0 })
        | (if m.refresh.is_some() { FLAG_REFRESH } else { 0 })
        | (if m.signature.is_some() { FLAG_SIGNATURE } else { 0 })
        | (if m.bump != 0 { FLAG_BUMP } else { 0 });
    encode_uint(state, flags);
    if let Some(peer) = &m.peer {
        encode_hyper_peer(state, peer)?;
    }
    if let Some(refresh) = &m.refresh {
        encode_fixed32(state, refresh);
    }
    if let Some(sig) = &m.signature {
        encode_fixed64(state, sig);
    }
    if m.bump != 0 {
        encode_uint(state, m.bump);
    }
    Ok(())
}

/// Decodes an [`AnnounceMessage`] from the compact-encoding buffer.
pub fn decode_announce(state: &mut State) -> Result<AnnounceMessage> {
    let flags = decode_uint(state)?;
    let peer = if flags & FLAG_PEER != 0 {
        Some(decode_hyper_peer(state)?)
    } else {
        None
    };
    let refresh = if flags & FLAG_REFRESH != 0 {
        Some(decode_fixed32(state)?)
    } else {
        None
    };
    let signature = if flags & FLAG_SIGNATURE != 0 {
        Some(decode_fixed64(state)?)
    } else {
        None
    };
    let bump = if flags & FLAG_BUMP != 0 {
        decode_uint(state)?
    } else {
        0
    };
    Ok(AnnounceMessage {
        peer,
        refresh,
        signature,
        bump,
    })
}

/// Serializes an [`AnnounceMessage`] to a byte vector.
pub fn encode_announce_to_bytes(m: &AnnounceMessage) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_announce(&mut state, m);
    state.alloc();
    encode_announce(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes an [`AnnounceMessage`] from bytes.
pub fn decode_announce_from_bytes(buf: &[u8]) -> Result<AnnounceMessage> {
    let mut state = State::from_buffer(buf);
    decode_announce(&mut state)
}

// ── LookupRawReply ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
/// Raw reply message returned from a DHT lookup containing matching peers.
pub struct LookupRawReply {
    /// The list of peers returned by the lookup.
    pub peers: Vec<HyperPeer>,
    /// Bump counter echoed from the corresponding announce.
    pub bump: u64,
}

/// Pre-encodes a [`LookupRawReply`] to calculate the required buffer size.
pub fn preencode_lookup_raw_reply(state: &mut State, m: &LookupRawReply) {
    preencode_uint(state, m.peers.len() as u64);
    for peer in &m.peers {
        preencode_hyper_peer(state, peer);
    }
    preencode_uint(state, m.bump);
}

/// Encodes a [`LookupRawReply`] into the compact-encoding buffer.
pub fn encode_lookup_raw_reply(state: &mut State, m: &LookupRawReply) -> Result<()> {
    encode_uint(state, m.peers.len() as u64);
    for peer in &m.peers {
        encode_hyper_peer(state, peer)?;
    }
    encode_uint(state, m.bump);
    Ok(())
}

/// Decodes a [`LookupRawReply`] from the compact-encoding buffer.
pub fn decode_lookup_raw_reply(state: &mut State) -> Result<LookupRawReply> {
    // A peer is a 32-byte key and at least the 1-byte length of its relays.
    let count = compact_encoding::decode_array_len(state, 33)?;
    let mut peers = Vec::with_capacity(count);
    for _ in 0..count {
        peers.push(decode_hyper_peer(state)?);
    }
    let bump = if state.start < state.end {
        decode_uint(state)?
    } else {
        0
    };
    Ok(LookupRawReply { peers, bump })
}

/// Serializes a [`LookupRawReply`] to a byte vector.
pub fn encode_lookup_raw_reply_to_bytes(m: &LookupRawReply) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_lookup_raw_reply(&mut state, m);
    state.alloc();
    encode_lookup_raw_reply(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`LookupRawReply`] from bytes.
pub fn decode_lookup_raw_reply_from_bytes(buf: &[u8]) -> Result<LookupRawReply> {
    let mut state = State::from_buffer(buf);
    decode_lookup_raw_reply(&mut state)
}

// ── MutablePutRequest ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Request to store a mutable value on the DHT.
pub struct MutablePutRequest {
    /// The 32-byte public key of the value's owner.
    pub public_key: [u8; 32],
    /// Monotonically increasing sequence number for this value.
    pub seq: u64,
    /// The value payload to store.
    pub value: Vec<u8>,
    /// A 64-byte Ed25519 signature over `(seq, value)`.
    pub signature: [u8; 64],
}

/// Pre-encodes a [`MutablePutRequest`] to calculate the required buffer size.
pub fn preencode_mutable_put_request(state: &mut State, m: &MutablePutRequest) {
    state.end += 32;
    preencode_uint(state, m.seq);
    compact_encoding::preencode_buffer(state, Some(&m.value));
    state.end += 64;
}

/// Encodes a [`MutablePutRequest`] into the compact-encoding buffer.
pub fn encode_mutable_put_request(state: &mut State, m: &MutablePutRequest) -> Result<()> {
    encode_fixed32(state, &m.public_key);
    encode_uint(state, m.seq);
    compact_encoding::encode_buffer(state, Some(&m.value));
    encode_fixed64(state, &m.signature);
    Ok(())
}

/// Decodes a [`MutablePutRequest`] from the compact-encoding buffer.
pub fn decode_mutable_put_request(state: &mut State) -> Result<MutablePutRequest> {
    let public_key = decode_fixed32(state)?;
    let seq = decode_uint(state)?;
    let value = compact_encoding::decode_buffer(state)?.unwrap_or_default();
    let signature = decode_fixed64(state)?;
    Ok(MutablePutRequest {
        public_key,
        seq,
        value,
        signature,
    })
}

/// Serializes a [`MutablePutRequest`] to a byte vector.
pub fn encode_mutable_put_request_to_bytes(m: &MutablePutRequest) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_mutable_put_request(&mut state, m);
    state.alloc();
    encode_mutable_put_request(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`MutablePutRequest`] from bytes.
pub fn decode_mutable_put_request_from_bytes(buf: &[u8]) -> Result<MutablePutRequest> {
    let mut state = State::from_buffer(buf);
    decode_mutable_put_request(&mut state)
}

// ── MutableGetResponse ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Response carrying the current value for a mutable DHT entry.
pub struct MutableGetResponse {
    /// Sequence number of the stored value.
    pub seq: u64,
    /// The stored value payload.
    pub value: Vec<u8>,
    /// A 64-byte Ed25519 signature over `(seq, value)`.
    pub signature: [u8; 64],
}

/// Pre-encodes a [`MutableGetResponse`] to calculate the required buffer size.
pub fn preencode_mutable_get_response(state: &mut State, m: &MutableGetResponse) {
    preencode_uint(state, m.seq);
    compact_encoding::preencode_buffer(state, Some(&m.value));
    state.end += 64;
}

/// Encodes a [`MutableGetResponse`] into the compact-encoding buffer.
pub fn encode_mutable_get_response(state: &mut State, m: &MutableGetResponse) -> Result<()> {
    encode_uint(state, m.seq);
    compact_encoding::encode_buffer(state, Some(&m.value));
    encode_fixed64(state, &m.signature);
    Ok(())
}

/// Decodes a [`MutableGetResponse`] from the compact-encoding buffer.
pub fn decode_mutable_get_response(state: &mut State) -> Result<MutableGetResponse> {
    let seq = decode_uint(state)?;
    let value = compact_encoding::decode_buffer(state)?.unwrap_or_default();
    let signature = decode_fixed64(state)?;
    Ok(MutableGetResponse {
        seq,
        value,
        signature,
    })
}

/// Serializes a [`MutableGetResponse`] to a byte vector.
pub fn encode_mutable_get_response_to_bytes(m: &MutableGetResponse) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_mutable_get_response(&mut state, m);
    state.alloc();
    encode_mutable_get_response(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`MutableGetResponse`] from bytes.
pub fn decode_mutable_get_response_from_bytes(buf: &[u8]) -> Result<MutableGetResponse> {
    let mut state = State::from_buffer(buf);
    decode_mutable_get_response(&mut state)
}

// ── MutableSignable ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// The signable payload for a mutable DHT entry, used to compute the signature.
pub struct MutableSignable {
    /// Sequence number being signed.
    pub seq: u64,
    /// Value being signed.
    pub value: Vec<u8>,
}

/// Pre-encodes a [`MutableSignable`] to calculate the required buffer size.
pub fn preencode_mutable_signable(state: &mut State, m: &MutableSignable) {
    preencode_uint(state, m.seq);
    compact_encoding::preencode_buffer(state, Some(&m.value));
}

/// Encodes a [`MutableSignable`] into the compact-encoding buffer.
pub fn encode_mutable_signable(state: &mut State, m: &MutableSignable) -> Result<()> {
    encode_uint(state, m.seq);
    compact_encoding::encode_buffer(state, Some(&m.value));
    Ok(())
}

/// Decodes a [`MutableSignable`] from the compact-encoding buffer.
pub fn decode_mutable_signable(state: &mut State) -> Result<MutableSignable> {
    let seq = decode_uint(state)?;
    let value = compact_encoding::decode_buffer(state)?.unwrap_or_default();
    Ok(MutableSignable { seq, value })
}

/// Serializes a [`MutableSignable`] to a byte vector.
pub fn encode_mutable_signable_to_bytes(m: &MutableSignable) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_mutable_signable(&mut state, m);
    state.alloc();
    encode_mutable_signable(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`MutableSignable`] from bytes.
pub fn decode_mutable_signable_from_bytes(buf: &[u8]) -> Result<MutableSignable> {
    let mut state = State::from_buffer(buf);
    decode_mutable_signable(&mut state)
}

// ── ipv4 peer array helpers ─────────────────────────────────────────────────

fn preencode_ipv4_peer_array(state: &mut State, peers: &[Ipv4Peer]) {
    preencode_uint(state, peers.len() as u64);
    state.end += peers.len() * 6;
}

fn encode_ipv4_peer_array(state: &mut State, peers: &[Ipv4Peer]) -> Result<()> {
    encode_uint(state, peers.len() as u64);
    for peer in peers {
        compact_encoding::encode_ipv4_address(state, &peer.host, peer.port)?;
    }
    Ok(())
}

fn decode_ipv4_peer_array(state: &mut State) -> Result<Vec<Ipv4Peer>> {
    let len = compact_encoding::decode_array_len(state, 6)?;
    let mut peers = Vec::with_capacity(len);
    for _ in 0..len {
        let (host, port) = compact_encoding::decode_ipv4_address(state)?;
        peers.push(Ipv4Peer { host, port });
    }
    Ok(peers)
}

fn preencode_ipv6_peer_array(state: &mut State, peers: &[Ipv4Peer]) {
    preencode_uint(state, peers.len() as u64);
    state.end += peers.len() * 18; // 16 bytes IPv6 + 2 bytes port
}

fn encode_ipv6_peer_array(state: &mut State, peers: &[Ipv4Peer]) -> Result<()> {
    encode_uint(state, peers.len() as u64);
    for peer in peers {
        compact_encoding::encode_ipv6_address(state, &peer.host, peer.port)?;
    }
    Ok(())
}

fn decode_ipv6_peer_array(state: &mut State) -> Result<Vec<Ipv4Peer>> {
    let len = compact_encoding::decode_array_len(state, 18)?;
    let mut peers = Vec::with_capacity(len);
    for _ in 0..len {
        let (host, port) = compact_encoding::decode_ipv6_address(state)?;
        peers.push(Ipv4Peer { host, port });
    }
    Ok(peers)
}

// ── HandshakeMessage (PEER_HANDSHAKE wire format) ───────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Wire message exchanged during the PEER_HANDSHAKE phase.
pub struct HandshakeMessage {
    /// Routing mode for this handshake (one of the `MODE_*` constants).
    pub mode: u64,
    /// Raw Noise protocol handshake bytes.
    pub noise: Vec<u8>,
    /// Optional IPv4 address of the connecting peer.
    pub peer_address: Option<Ipv4Peer>,
    /// Optional IPv4 address of the relay through which this message is routed.
    pub relay_address: Option<Ipv4Peer>,
}

/// Pre-encodes a [`HandshakeMessage`] to calculate the required buffer size.
pub fn preencode_handshake(state: &mut State, m: &HandshakeMessage) {
    preencode_uint(state, 0); // flags
    preencode_uint(state, m.mode);
    compact_encoding::preencode_buffer(state, Some(&m.noise));
    if m.peer_address.is_some() {
        state.end += 6;
    }
    if m.relay_address.is_some() {
        state.end += 6;
    }
}

/// Encodes a [`HandshakeMessage`] into the compact-encoding buffer.
pub fn encode_handshake(state: &mut State, m: &HandshakeMessage) -> Result<()> {
    let flags = (if m.peer_address.is_some() { 1u64 } else { 0 })
        | (if m.relay_address.is_some() { 2 } else { 0 });
    encode_uint(state, flags);
    encode_uint(state, m.mode);
    compact_encoding::encode_buffer(state, Some(&m.noise));
    if let Some(addr) = &m.peer_address {
        compact_encoding::encode_ipv4_address(state, &addr.host, addr.port)?;
    }
    if let Some(addr) = &m.relay_address {
        compact_encoding::encode_ipv4_address(state, &addr.host, addr.port)?;
    }
    Ok(())
}

/// Decodes a [`HandshakeMessage`] from the compact-encoding buffer.
pub fn decode_handshake(state: &mut State) -> Result<HandshakeMessage> {
    let flags = decode_uint(state)?;
    let mode = decode_uint(state)?;
    let noise = compact_encoding::decode_buffer(state)?.unwrap_or_default();
    let peer_address = if flags & 1 != 0 {
        let (host, port) = compact_encoding::decode_ipv4_address(state)?;
        Some(Ipv4Peer { host, port })
    } else {
        None
    };
    let relay_address = if flags & 2 != 0 {
        let (host, port) = compact_encoding::decode_ipv4_address(state)?;
        Some(Ipv4Peer { host, port })
    } else {
        None
    };
    Ok(HandshakeMessage {
        mode,
        noise,
        peer_address,
        relay_address,
    })
}

/// Serializes a [`HandshakeMessage`] to a byte vector.
pub fn encode_handshake_to_bytes(m: &HandshakeMessage) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_handshake(&mut state, m);
    state.alloc();
    encode_handshake(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`HandshakeMessage`] from bytes.
pub fn decode_handshake_from_bytes(buf: &[u8]) -> Result<HandshakeMessage> {
    let mut state = State::from_buffer(buf);
    decode_handshake(&mut state)
}

// ── HolepunchMessage (PEER_HOLEPUNCH wire format) ───────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Wire message exchanged during the PEER_HOLEPUNCH phase.
pub struct HolepunchMessage {
    /// Routing mode for this holepunch message (one of the `MODE_*` constants).
    pub mode: u64,
    /// Session identifier for this holepunch attempt.
    pub id: u64,
    /// Encrypted holepunch payload bytes.
    pub payload: Vec<u8>,
    /// Optional IPv4 address of the peer involved in the holepunch.
    pub peer_address: Option<Ipv4Peer>,
}

/// Pre-encodes a [`HolepunchMessage`] to calculate the required buffer size.
pub fn preencode_holepunch_msg(state: &mut State, m: &HolepunchMessage) {
    preencode_uint(state, 0); // flags
    preencode_uint(state, m.mode);
    preencode_uint(state, m.id);
    compact_encoding::preencode_buffer(state, Some(&m.payload));
    if m.peer_address.is_some() {
        state.end += 6;
    }
}

/// Encodes a [`HolepunchMessage`] into the compact-encoding buffer.
pub fn encode_holepunch_msg(state: &mut State, m: &HolepunchMessage) -> Result<()> {
    let flags: u64 = if m.peer_address.is_some() { 1 } else { 0 };
    encode_uint(state, flags);
    encode_uint(state, m.mode);
    encode_uint(state, m.id);
    compact_encoding::encode_buffer(state, Some(&m.payload));
    if let Some(addr) = &m.peer_address {
        compact_encoding::encode_ipv4_address(state, &addr.host, addr.port)?;
    }
    Ok(())
}

/// Decodes a [`HolepunchMessage`] from the compact-encoding buffer.
pub fn decode_holepunch_msg(state: &mut State) -> Result<HolepunchMessage> {
    let flags = decode_uint(state)?;
    let mode = decode_uint(state)?;
    let id = decode_uint(state)?;
    let payload = compact_encoding::decode_buffer(state)?.unwrap_or_default();
    let peer_address = if flags & 1 != 0 {
        let (host, port) = compact_encoding::decode_ipv4_address(state)?;
        Some(Ipv4Peer { host, port })
    } else {
        None
    };
    Ok(HolepunchMessage {
        mode,
        id,
        payload,
        peer_address,
    })
}

/// Serializes a [`HolepunchMessage`] to a byte vector.
pub fn encode_holepunch_msg_to_bytes(m: &HolepunchMessage) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_holepunch_msg(&mut state, m);
    state.alloc();
    encode_holepunch_msg(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`HolepunchMessage`] from bytes.
pub fn decode_holepunch_msg_from_bytes(buf: &[u8]) -> Result<HolepunchMessage> {
    let mut state = State::from_buffer(buf);
    decode_holepunch_msg(&mut state)
}

// ── RelayInfo ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// A pair of addresses describing a relay-assisted path between two peers.
pub struct RelayInfo {
    /// The IPv4 address of the relay node.
    pub relay_address: Ipv4Peer,
    /// The IPv4 address of the remote peer as seen by the relay.
    pub peer_address: Ipv4Peer,
}

fn preencode_relay_info(_state: &mut State, _m: &RelayInfo) {
    _state.end += 12; // 2 × ipv4 (4 bytes IP + 2 bytes port)
}

fn encode_relay_info(state: &mut State, m: &RelayInfo) -> Result<()> {
    compact_encoding::encode_ipv4_address(state, &m.relay_address.host, m.relay_address.port)?;
    compact_encoding::encode_ipv4_address(state, &m.peer_address.host, m.peer_address.port)?;
    Ok(())
}

fn decode_relay_info(state: &mut State) -> Result<RelayInfo> {
    let (rhost, rport) = compact_encoding::decode_ipv4_address(state)?;
    let (phost, pport) = compact_encoding::decode_ipv4_address(state)?;
    Ok(RelayInfo {
        relay_address: Ipv4Peer {
            host: rhost,
            port: rport,
        },
        peer_address: Ipv4Peer {
            host: phost,
            port: pport,
        },
    })
}

fn preencode_relay_info_array(state: &mut State, arr: &[RelayInfo]) {
    preencode_uint(state, arr.len() as u64);
    for item in arr {
        preencode_relay_info(state, item);
    }
}

fn encode_relay_info_array(state: &mut State, arr: &[RelayInfo]) -> Result<()> {
    encode_uint(state, arr.len() as u64);
    for item in arr {
        encode_relay_info(state, item)?;
    }
    Ok(())
}

fn decode_relay_info_array(state: &mut State) -> Result<Vec<RelayInfo>> {
    let len = compact_encoding::decode_array_len(state, 12)?;
    let mut arr = Vec::with_capacity(len);
    for _ in 0..len {
        arr.push(decode_relay_info(state)?);
    }
    Ok(arr)
}

// ── HolepunchInfo ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata for an ongoing hole-punch session, including relay candidates.
pub struct HolepunchInfo {
    /// Unique session identifier for this hole-punch attempt.
    pub id: u64,
    /// List of relay-peer address pairs available for this hole-punch.
    pub relays: Vec<RelayInfo>,
}

fn preencode_holepunch_info(state: &mut State, m: &HolepunchInfo) {
    preencode_uint(state, m.id);
    preencode_relay_info_array(state, &m.relays);
}

fn encode_holepunch_info(state: &mut State, m: &HolepunchInfo) -> Result<()> {
    encode_uint(state, m.id);
    encode_relay_info_array(state, &m.relays)?;
    Ok(())
}

fn decode_holepunch_info(state: &mut State) -> Result<HolepunchInfo> {
    let id = decode_uint(state)?;
    let relays = decode_relay_info_array(state)?;
    Ok(HolepunchInfo { id, relays })
}

// ── UdxInfo ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// UDX transport parameters exchanged during the Noise handshake.
pub struct UdxInfo {
    /// UDX protocol version advertised by the sender.
    pub version: u64,
    /// Whether the sender's UDP socket can be reused across connections.
    pub reusable_socket: bool,
    /// Local UDX stream identifier chosen by the sender.
    pub id: u64,
    /// Initial sequence number for the UDX stream.
    pub seq: u64,
}

fn preencode_udx_info(state: &mut State, m: &UdxInfo) {
    state.end += 2; // version + features
    preencode_uint(state, m.id);
    preencode_uint(state, m.seq);
}

fn encode_udx_info(state: &mut State, m: &UdxInfo) -> Result<()> {
    encode_uint(state, 1); // version always 1
    encode_uint(state, if m.reusable_socket { 1 } else { 0 });
    encode_uint(state, m.id);
    encode_uint(state, m.seq);
    Ok(())
}

fn decode_udx_info(state: &mut State) -> Result<UdxInfo> {
    let version = decode_uint(state)?;
    let features = decode_uint(state)?;
    let id = decode_uint(state)?;
    let seq = decode_uint(state)?;
    Ok(UdxInfo {
        version,
        reusable_socket: (features & 1) != 0,
        id,
        seq,
    })
}

// ── SecretStreamInfo ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Secret stream parameters exchanged during the Noise handshake.
pub struct SecretStreamInfo {
    /// Secret stream protocol version advertised by the sender.
    pub version: u64,
}

fn preencode_secret_stream_info(state: &mut State, _m: &SecretStreamInfo) {
    preencode_uint(state, 1);
}

fn encode_secret_stream_info(state: &mut State, _m: &SecretStreamInfo) -> Result<()> {
    encode_uint(state, 1);
    Ok(())
}

fn decode_secret_stream_info(state: &mut State) -> Result<SecretStreamInfo> {
    let version = decode_uint(state)?;
    Ok(SecretStreamInfo { version })
}

// ── RelayThroughInfo ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Information about a relay node through which a connection should be routed.
pub struct RelayThroughInfo {
    /// Protocol version for the relay-through capability.
    pub version: u64,
    /// 32-byte public key of the relay node.
    pub public_key: [u8; 32],
    /// 32-byte authorization token for connecting through the relay.
    pub token: [u8; 32],
}

fn preencode_relay_through_info(state: &mut State, _m: &RelayThroughInfo) {
    preencode_uint(state, 1); // version
    preencode_uint(state, 0); // flags
    state.end += 64; // public_key + token
}

fn encode_relay_through_info(state: &mut State, m: &RelayThroughInfo) -> Result<()> {
    encode_uint(state, 1);
    encode_uint(state, 0);
    encode_fixed32(state, &m.public_key);
    encode_fixed32(state, &m.token);
    Ok(())
}

fn decode_relay_through_info(state: &mut State) -> Result<RelayThroughInfo> {
    let version = decode_uint(state)?;
    let _flags = decode_uint(state)?;
    let public_key = decode_fixed32(state)?;
    let token = decode_fixed32(state)?;
    Ok(RelayThroughInfo {
        version,
        public_key,
        token,
    })
}

// ── NoisePayload (exchanged inside Noise handshake) ─────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Payload carried inside the Noise protocol handshake messages.
pub struct NoisePayload {
    /// Protocol version of this payload (must be `1` for valid messages).
    pub version: u64,
    /// Error code from the sender (one of the `ERROR_*` constants).
    pub error: u64,
    /// Firewall type reported by the sender (one of the `FIREWALL_*` constants).
    pub firewall: u64,
    /// Optional hole-punch session info, present when hole-punching is in progress.
    pub holepunch: Option<HolepunchInfo>,
    /// IPv4 addresses the sender is reachable on.
    pub addresses4: Vec<Ipv4Peer>,
    /// IPv6 addresses the sender is reachable on.
    pub addresses6: Vec<Ipv4Peer>,
    /// Optional UDX transport parameters.
    pub udx: Option<UdxInfo>,
    /// Optional secret stream parameters.
    pub secret_stream: Option<SecretStreamInfo>,
    /// Optional relay-through info for relayed connections.
    pub relay_through: Option<RelayThroughInfo>,
    /// Optional list of relay IPv4 addresses available for this peer.
    pub relay_addresses: Option<Vec<Ipv4Peer>>,
}

const NP_FLAG_HOLEPUNCH: u64 = 1;
const NP_FLAG_ADDRESSES4: u64 = 2;
const NP_FLAG_ADDRESSES6: u64 = 4;
const NP_FLAG_UDX: u64 = 8;
const NP_FLAG_SECRET_STREAM: u64 = 16;
const NP_FLAG_RELAY_THROUGH: u64 = 32;
const NP_FLAG_RELAY_ADDRESSES: u64 = 64;

/// Pre-encodes a [`NoisePayload`] to calculate the required buffer size.
pub fn preencode_noise_payload(state: &mut State, m: &NoisePayload) {
    state.end += 4; // version + flags + error + firewall (each 1 byte for small values)
    if let Some(hp) = &m.holepunch {
        preencode_holepunch_info(state, hp);
    }
    if !m.addresses4.is_empty() {
        preencode_ipv4_peer_array(state, &m.addresses4);
    }
    if !m.addresses6.is_empty() {
        preencode_ipv6_peer_array(state, &m.addresses6);
    }
    if let Some(udx) = &m.udx {
        preencode_udx_info(state, udx);
    }
    if let Some(ss) = &m.secret_stream {
        preencode_secret_stream_info(state, ss);
    }
    if let Some(rt) = &m.relay_through {
        preencode_relay_through_info(state, rt);
    }
    if let Some(ra) = &m.relay_addresses {
        preencode_ipv4_peer_array(state, ra);
    }
}

/// Encodes a [`NoisePayload`] into the compact-encoding buffer.
pub fn encode_noise_payload(state: &mut State, m: &NoisePayload) -> Result<()> {
    let mut flags = 0u64;
    if m.holepunch.is_some() {
        flags |= NP_FLAG_HOLEPUNCH;
    }
    if !m.addresses4.is_empty() {
        flags |= NP_FLAG_ADDRESSES4;
    }
    if !m.addresses6.is_empty() {
        flags |= NP_FLAG_ADDRESSES6;
    }
    if m.udx.is_some() {
        flags |= NP_FLAG_UDX;
    }
    if m.secret_stream.is_some() {
        flags |= NP_FLAG_SECRET_STREAM;
    }
    if m.relay_through.is_some() {
        flags |= NP_FLAG_RELAY_THROUGH;
    }
    if m.relay_addresses.is_some() {
        flags |= NP_FLAG_RELAY_ADDRESSES;
    }

    encode_uint(state, 1); // version
    encode_uint(state, flags);
    encode_uint(state, m.error);
    encode_uint(state, m.firewall);

    if let Some(hp) = &m.holepunch {
        encode_holepunch_info(state, hp)?;
    }
    if !m.addresses4.is_empty() {
        encode_ipv4_peer_array(state, &m.addresses4)?;
    }
    if !m.addresses6.is_empty() {
        encode_ipv6_peer_array(state, &m.addresses6)?;
    }
    if let Some(udx) = &m.udx {
        encode_udx_info(state, udx)?;
    }
    if let Some(ss) = &m.secret_stream {
        encode_secret_stream_info(state, ss)?;
    }
    if let Some(rt) = &m.relay_through {
        encode_relay_through_info(state, rt)?;
    }
    if let Some(ra) = &m.relay_addresses {
        encode_ipv4_peer_array(state, ra)?;
    }
    Ok(())
}

/// Decodes a [`NoisePayload`] from the compact-encoding buffer.
pub fn decode_noise_payload(state: &mut State) -> Result<NoisePayload> {
    let version = decode_uint(state)?;
    if version != 1 {
        return Ok(NoisePayload {
            version,
            error: 0,
            firewall: 0,
            holepunch: None,
            addresses4: vec![],
            addresses6: vec![],
            udx: None,
            secret_stream: None,
            relay_through: None,
            relay_addresses: None,
        });
    }
    let flags = decode_uint(state)?;
    let error = decode_uint(state)?;
    let firewall = decode_uint(state)?;

    let holepunch = if flags & NP_FLAG_HOLEPUNCH != 0 {
        Some(decode_holepunch_info(state)?)
    } else {
        None
    };
    let addresses4 = if flags & NP_FLAG_ADDRESSES4 != 0 {
        decode_ipv4_peer_array(state)?
    } else {
        vec![]
    };
    let addresses6 = if flags & NP_FLAG_ADDRESSES6 != 0 {
        decode_ipv6_peer_array(state)?
    } else {
        vec![]
    };
    let udx = if flags & NP_FLAG_UDX != 0 {
        Some(decode_udx_info(state)?)
    } else {
        None
    };
    let secret_stream = if flags & NP_FLAG_SECRET_STREAM != 0 {
        Some(decode_secret_stream_info(state)?)
    } else {
        None
    };
    let relay_through = if flags & NP_FLAG_RELAY_THROUGH != 0 {
        Some(decode_relay_through_info(state)?)
    } else {
        None
    };
    let relay_addresses = if flags & NP_FLAG_RELAY_ADDRESSES != 0 {
        Some(decode_ipv4_peer_array(state)?)
    } else {
        None
    };

    Ok(NoisePayload {
        version,
        error,
        firewall,
        holepunch,
        addresses4,
        addresses6,
        udx,
        secret_stream,
        relay_through,
        relay_addresses,
    })
}

/// Serializes a [`NoisePayload`] to a byte vector.
pub fn encode_noise_payload_to_bytes(m: &NoisePayload) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_noise_payload(&mut state, m);
    state.alloc();
    encode_noise_payload(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`NoisePayload`] from bytes.
pub fn decode_noise_payload_from_bytes(buf: &[u8]) -> Result<NoisePayload> {
    let mut state = State::from_buffer(buf);
    decode_noise_payload(&mut state)
}

// ── HolepunchPayload (encrypted, exchanged during hole-punch rounds) ────────

#[derive(Debug, Clone, PartialEq, Eq)]
/// Encrypted payload exchanged between peers during each hole-punch round.
pub struct HolepunchPayload {
    /// Error code from the sender (one of the `ERROR_*` constants).
    pub error: u64,
    /// Firewall type reported by the sender (one of the `FIREWALL_*` constants).
    pub firewall: u64,
    /// Current hole-punch round number.
    pub round: u64,
    /// Whether the sender has already established a direct connection.
    pub connected: bool,
    /// Whether the sender is actively sending hole-punch packets.
    pub punching: bool,
    /// List of local IPv4 addresses the sender is reachable on, if present.
    pub addresses: Option<Vec<Ipv4Peer>>,
    /// The remote address the sender is trying to punch to, if known.
    pub remote_address: Option<Ipv4Peer>,
    /// A 32-byte token sent by the initiator to prove liveness.
    pub token: Option<[u8; 32]>,
    /// A 32-byte token sent by the remote peer to prove liveness.
    pub remote_token: Option<[u8; 32]>,
}

const HP_FLAG_CONNECTED: u64 = 1;
const HP_FLAG_PUNCHING: u64 = 2;
const HP_FLAG_ADDRESSES: u64 = 4;
const HP_FLAG_REMOTE_ADDRESS: u64 = 8;
const HP_FLAG_TOKEN: u64 = 16;
const HP_FLAG_REMOTE_TOKEN: u64 = 32;

/// Pre-encodes a [`HolepunchPayload`] to calculate the required buffer size.
pub fn preencode_holepunch_payload(state: &mut State, m: &HolepunchPayload) {
    state.end += 4; // flags + error + firewall + round
    if let Some(addrs) = &m.addresses {
        preencode_ipv4_peer_array(state, addrs);
    }
    if m.remote_address.is_some() {
        state.end += 6;
    }
    if m.token.is_some() {
        state.end += 32;
    }
    if m.remote_token.is_some() {
        state.end += 32;
    }
}

/// Encodes a [`HolepunchPayload`] into the compact-encoding buffer.
pub fn encode_holepunch_payload(state: &mut State, m: &HolepunchPayload) -> Result<()> {
    let mut flags = 0u64;
    if m.connected {
        flags |= HP_FLAG_CONNECTED;
    }
    if m.punching {
        flags |= HP_FLAG_PUNCHING;
    }
    if m.addresses.is_some() {
        flags |= HP_FLAG_ADDRESSES;
    }
    if m.remote_address.is_some() {
        flags |= HP_FLAG_REMOTE_ADDRESS;
    }
    if m.token.is_some() {
        flags |= HP_FLAG_TOKEN;
    }
    if m.remote_token.is_some() {
        flags |= HP_FLAG_REMOTE_TOKEN;
    }

    encode_uint(state, flags);
    encode_uint(state, m.error);
    encode_uint(state, m.firewall);
    encode_uint(state, m.round);

    if let Some(addrs) = &m.addresses {
        encode_ipv4_peer_array(state, addrs)?;
    }
    if let Some(addr) = &m.remote_address {
        compact_encoding::encode_ipv4_address(state, &addr.host, addr.port)?;
    }
    if let Some(token) = &m.token {
        encode_fixed32(state, token);
    }
    if let Some(token) = &m.remote_token {
        encode_fixed32(state, token);
    }
    Ok(())
}

/// Decodes a [`HolepunchPayload`] from the compact-encoding buffer.
pub fn decode_holepunch_payload(state: &mut State) -> Result<HolepunchPayload> {
    let flags = decode_uint(state)?;
    let error = decode_uint(state)?;
    let firewall = decode_uint(state)?;
    let round = decode_uint(state)?;

    let addresses = if flags & HP_FLAG_ADDRESSES != 0 {
        Some(decode_ipv4_peer_array(state)?)
    } else {
        None
    };
    let remote_address = if flags & HP_FLAG_REMOTE_ADDRESS != 0 {
        let (host, port) = compact_encoding::decode_ipv4_address(state)?;
        Some(Ipv4Peer { host, port })
    } else {
        None
    };
    let token = if flags & HP_FLAG_TOKEN != 0 {
        Some(decode_fixed32(state)?)
    } else {
        None
    };
    let remote_token = if flags & HP_FLAG_REMOTE_TOKEN != 0 {
        Some(decode_fixed32(state)?)
    } else {
        None
    };

    Ok(HolepunchPayload {
        error,
        firewall,
        round,
        connected: (flags & HP_FLAG_CONNECTED) != 0,
        punching: (flags & HP_FLAG_PUNCHING) != 0,
        addresses,
        remote_address,
        token,
        remote_token,
    })
}

/// Serializes a [`HolepunchPayload`] to a byte vector.
pub fn encode_holepunch_payload_to_bytes(m: &HolepunchPayload) -> Result<Vec<u8>> {
    let mut state = State::new();
    preencode_holepunch_payload(&mut state, m);
    state.alloc();
    encode_holepunch_payload(&mut state, m)?;
    Ok(state.buffer)
}

/// Deserializes a [`HolepunchPayload`] from bytes.
pub fn decode_holepunch_payload_from_bytes(buf: &[u8]) -> Result<HolepunchPayload> {
    let mut state = State::from_buffer(buf);
    decode_holepunch_payload(&mut state)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peer(pk: u8) -> HyperPeer {
        HyperPeer {
            public_key: [pk; 32],
            relay_addresses: vec![],
        }
    }

    fn make_peer_with_relay(pk: u8) -> HyperPeer {
        HyperPeer {
            public_key: [pk; 32],
            relay_addresses: vec![Ipv4Peer {
                host: "10.0.0.1".into(),
                port: 8080,
            }],
        }
    }

    #[test]
    fn hyper_peer_roundtrip_no_relays() {
        let peer = make_peer(0xaa);
        let bytes = encode_hyper_peer_to_bytes(&peer).unwrap();
        assert_eq!(bytes.len(), 33); // 32 pubkey + 1 uint(0)
        let decoded = decode_hyper_peer_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.public_key, [0xaa; 32]);
        assert!(decoded.relay_addresses.is_empty());
    }

    #[test]
    fn hyper_peer_roundtrip_with_relay() {
        let peer = make_peer_with_relay(0xbb);
        let bytes = encode_hyper_peer_to_bytes(&peer).unwrap();
        let decoded = decode_hyper_peer_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.public_key, [0xbb; 32]);
        assert_eq!(decoded.relay_addresses.len(), 1);
        assert_eq!(decoded.relay_addresses[0].host, "10.0.0.1");
        assert_eq!(decoded.relay_addresses[0].port, 8080);
    }

    #[test]
    fn announce_flags_all_absent() {
        let m = AnnounceMessage {
            peer: None,
            refresh: None,
            signature: None,
            bump: 0,
        };
        let bytes = encode_announce_to_bytes(&m).unwrap();
        assert_eq!(bytes, vec![0x00]); // flags = 0
        let decoded = decode_announce_from_bytes(&bytes).unwrap();
        assert!(decoded.peer.is_none());
        assert!(decoded.refresh.is_none());
        assert!(decoded.signature.is_none());
        assert_eq!(decoded.bump, 0);
    }

    #[test]
    fn announce_peer_only() {
        let m = AnnounceMessage {
            peer: Some(make_peer(0xaa)),
            refresh: None,
            signature: None,
            bump: 0,
        };
        let bytes = encode_announce_to_bytes(&m).unwrap();
        assert_eq!(bytes[0], 0x01); // flag peer
        let decoded = decode_announce_from_bytes(&bytes).unwrap();
        let p = decoded.peer.unwrap();
        assert_eq!(p.public_key, [0xaa; 32]);
        assert!(decoded.refresh.is_none());
        assert!(decoded.signature.is_none());
        assert_eq!(decoded.bump, 0);
    }

    #[test]
    fn announce_all_fields() {
        let m = AnnounceMessage {
            peer: Some(make_peer(0xaa)),
            refresh: Some([0xbb; 32]),
            signature: Some([0xcc; 64]),
            bump: 42,
        };
        let bytes = encode_announce_to_bytes(&m).unwrap();
        assert_eq!(bytes[0], 0x0f); // all 4 flags
        let decoded = decode_announce_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.peer.unwrap().public_key, [0xaa; 32]);
        assert_eq!(decoded.refresh, Some([0xbb; 32]));
        assert_eq!(decoded.signature, Some([0xcc; 64]));
        assert_eq!(decoded.bump, 42);
    }

    #[test]
    fn announce_bump_only() {
        let m = AnnounceMessage {
            peer: None,
            refresh: None,
            signature: None,
            bump: 100,
        };
        let bytes = encode_announce_to_bytes(&m).unwrap();
        assert_eq!(bytes[0], 0x08); // FLAG_BUMP
        let decoded = decode_announce_from_bytes(&bytes).unwrap();
        assert!(decoded.peer.is_none());
        assert_eq!(decoded.bump, 100);
    }

    #[test]
    fn lookup_raw_reply_empty() {
        let m = LookupRawReply {
            peers: vec![],
            bump: 0,
        };
        let bytes = encode_lookup_raw_reply_to_bytes(&m).unwrap();
        let decoded = decode_lookup_raw_reply_from_bytes(&bytes).unwrap();
        assert!(decoded.peers.is_empty());
        assert_eq!(decoded.bump, 0);
    }

    #[test]
    fn lookup_raw_reply_with_peers() {
        let m = LookupRawReply {
            peers: vec![make_peer(0xaa), make_peer(0xbb)],
            bump: 7,
        };
        let bytes = encode_lookup_raw_reply_to_bytes(&m).unwrap();
        let decoded = decode_lookup_raw_reply_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.peers.len(), 2);
        assert_eq!(decoded.peers[0].public_key, [0xaa; 32]);
        assert_eq!(decoded.peers[1].public_key, [0xbb; 32]);
        assert_eq!(decoded.bump, 7);
    }

    /// `[0xff][u64 LE count]`: a count as a peer might send it.
    fn lookup_reply_claiming(count: u64) -> Vec<u8> {
        let mut bytes = vec![0xff];
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes
    }

    #[test]
    fn lookup_raw_reply_rejects_a_count_past_the_array_limit() {
        for count in [1 << 40, u64::MAX] {
            let err = decode_lookup_raw_reply_from_bytes(&lookup_reply_claiming(count)).unwrap_err();
            assert!(matches!(err, EncodingError::ArrayTooLarge(_)), "{count}: {err:?}");
        }
    }

    #[test]
    fn lookup_raw_reply_rejects_a_count_its_bytes_cannot_hold() {
        // Two peers' worth of bytes, three peers claimed.
        let m = LookupRawReply {
            peers: vec![make_peer(0xaa), make_peer(0xbb)],
            bump: 0,
        };
        let mut bytes = encode_lookup_raw_reply_to_bytes(&m).unwrap();
        bytes[0] = 3;
        let err = decode_lookup_raw_reply_from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, EncodingError::OutOfBounds { .. }), "{err:?}");

        let err = decode_lookup_raw_reply_from_bytes(&lookup_reply_claiming(1 << 20)).unwrap_err();
        assert!(matches!(err, EncodingError::OutOfBounds { .. }), "{err:?}");
    }

    #[test]
    fn mutable_put_request_roundtrip() {
        let m = MutablePutRequest {
            public_key: [0x11; 32],
            seq: 3,
            value: b"hello".to_vec(),
            signature: [0x22; 64],
        };
        let bytes = encode_mutable_put_request_to_bytes(&m).unwrap();
        let decoded = decode_mutable_put_request_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn mutable_get_response_roundtrip() {
        let m = MutableGetResponse {
            seq: 5,
            value: b"world".to_vec(),
            signature: [0x33; 64],
        };
        let bytes = encode_mutable_get_response_to_bytes(&m).unwrap();
        let decoded = decode_mutable_get_response_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn mutable_signable_roundtrip() {
        let m = MutableSignable {
            seq: 42,
            value: b"data".to_vec(),
        };
        let bytes = encode_mutable_signable_to_bytes(&m).unwrap();
        let decoded = decode_mutable_signable_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn command_constants() {
        assert_eq!(PEER_HANDSHAKE, 0);
        assert_eq!(PEER_HOLEPUNCH, 1);
        assert_eq!(FIND_PEER, 2);
        assert_eq!(LOOKUP, 3);
        assert_eq!(ANNOUNCE, 4);
        assert_eq!(UNANNOUNCE, 5);
        assert_eq!(MUTABLE_PUT, 6);
        assert_eq!(MUTABLE_GET, 7);
        assert_eq!(IMMUTABLE_PUT, 8);
        assert_eq!(IMMUTABLE_GET, 9);
    }

    // ── HandshakeMessage tests ──────────────────────────────────────────────

    fn make_addr(host: &str, port: u16) -> Ipv4Peer {
        Ipv4Peer {
            host: host.into(),
            port,
        }
    }

    #[test]
    fn handshake_no_addresses() {
        let m = HandshakeMessage {
            mode: MODE_FROM_CLIENT,
            noise: vec![0xab; 48],
            peer_address: None,
            relay_address: None,
        };
        let bytes = encode_handshake_to_bytes(&m).unwrap();
        let decoded = decode_handshake_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn handshake_with_peer_address() {
        let m = HandshakeMessage {
            mode: MODE_FROM_SERVER,
            noise: vec![0xcd; 32],
            peer_address: Some(make_addr("192.168.1.1", 3000)),
            relay_address: None,
        };
        let bytes = encode_handshake_to_bytes(&m).unwrap();
        let decoded = decode_handshake_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn handshake_with_relay_address() {
        let m = HandshakeMessage {
            mode: MODE_FROM_RELAY,
            noise: vec![0xef; 16],
            peer_address: None,
            relay_address: Some(make_addr("10.0.0.1", 8080)),
        };
        let bytes = encode_handshake_to_bytes(&m).unwrap();
        let decoded = decode_handshake_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn handshake_both_addresses() {
        let m = HandshakeMessage {
            mode: MODE_REPLY,
            noise: vec![0x11; 64],
            peer_address: Some(make_addr("1.2.3.4", 1234)),
            relay_address: Some(make_addr("5.6.7.8", 5678)),
        };
        let bytes = encode_handshake_to_bytes(&m).unwrap();
        let decoded = decode_handshake_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn handshake_empty_noise() {
        let m = HandshakeMessage {
            mode: MODE_FROM_SECOND_RELAY,
            noise: vec![],
            peer_address: None,
            relay_address: None,
        };
        let bytes = encode_handshake_to_bytes(&m).unwrap();
        let decoded = decode_handshake_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    // ── HolepunchMessage tests ──────────────────────────────────────────────

    #[test]
    fn holepunch_msg_no_peer_address() {
        let m = HolepunchMessage {
            mode: MODE_FROM_CLIENT,
            id: 42,
            payload: vec![0xaa; 24],
            peer_address: None,
        };
        let bytes = encode_holepunch_msg_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_msg_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn holepunch_msg_with_peer_address() {
        let m = HolepunchMessage {
            mode: MODE_FROM_RELAY,
            id: 9999,
            payload: vec![0xbb; 48],
            peer_address: Some(make_addr("10.0.0.5", 4000)),
        };
        let bytes = encode_holepunch_msg_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_msg_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn holepunch_msg_empty_payload() {
        let m = HolepunchMessage {
            mode: MODE_REPLY,
            id: 0,
            payload: vec![],
            peer_address: None,
        };
        let bytes = encode_holepunch_msg_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_msg_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    // ── NoisePayload tests ──────────────────────────────────────────────────

    #[test]
    fn noise_payload_minimal() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_NONE,
            firewall: FIREWALL_UNKNOWN,
            holepunch: None,
            addresses4: vec![],
            addresses6: vec![],
            udx: None,
            secret_stream: None,
            relay_through: None,
            relay_addresses: None,
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        assert_eq!(bytes, vec![1, 0, 0, 0]); // version=1, flags=0, error=0, firewall=0
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn noise_payload_with_addresses4() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_NONE,
            firewall: FIREWALL_OPEN,
            holepunch: None,
            addresses4: vec![make_addr("1.2.3.4", 1000), make_addr("5.6.7.8", 2000)],
            addresses6: vec![],
            udx: None,
            secret_stream: None,
            relay_through: None,
            relay_addresses: None,
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn noise_payload_with_holepunch_info() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_NONE,
            firewall: FIREWALL_CONSISTENT,
            holepunch: Some(HolepunchInfo {
                id: 7,
                relays: vec![RelayInfo {
                    relay_address: make_addr("10.0.0.1", 8080),
                    peer_address: make_addr("192.168.1.1", 3000),
                }],
            }),
            addresses4: vec![],
            addresses6: vec![],
            udx: None,
            secret_stream: None,
            relay_through: None,
            relay_addresses: None,
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn noise_payload_with_udx_and_secret_stream() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_NONE,
            firewall: FIREWALL_RANDOM,
            holepunch: None,
            addresses4: vec![make_addr("1.2.3.4", 1000)],
            addresses6: vec![],
            udx: Some(UdxInfo {
                version: 1,
                reusable_socket: true,
                id: 100,
                seq: 200,
            }),
            secret_stream: Some(SecretStreamInfo { version: 1 }),
            relay_through: None,
            relay_addresses: None,
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn noise_payload_all_fields() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_ABORTED,
            firewall: FIREWALL_CONSISTENT,
            holepunch: Some(HolepunchInfo {
                id: 42,
                relays: vec![],
            }),
            addresses4: vec![make_addr("1.2.3.4", 1000)],
            addresses6: vec![make_addr("2001:db8::1", 2000)],
            udx: Some(UdxInfo {
                version: 1,
                reusable_socket: false,
                id: 1,
                seq: 0,
            }),
            secret_stream: Some(SecretStreamInfo { version: 1 }),
            relay_through: Some(RelayThroughInfo {
                version: 1,
                public_key: [0xaa; 32],
                token: [0xbb; 32],
            }),
            relay_addresses: Some(vec![make_addr("10.0.0.1", 8080)]),
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn noise_payload_version_mismatch_returns_defaults() {
        // Version != 1 should return empty defaults
        let bytes = vec![2]; // version=2
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.error, 0);
        assert_eq!(decoded.firewall, 0);
        assert!(decoded.holepunch.is_none());
        assert!(decoded.addresses4.is_empty());
        assert!(decoded.udx.is_none());
    }

    #[test]
    fn noise_payload_relay_through() {
        let m = NoisePayload {
            version: 1,
            error: ERROR_NONE,
            firewall: FIREWALL_OPEN,
            holepunch: None,
            addresses4: vec![],
            addresses6: vec![],
            udx: None,
            secret_stream: None,
            relay_through: Some(RelayThroughInfo {
                version: 1,
                public_key: [0xdd; 32],
                token: [0xee; 32],
            }),
            relay_addresses: None,
        };
        let bytes = encode_noise_payload_to_bytes(&m).unwrap();
        let decoded = decode_noise_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    // ── HolepunchPayload tests ──────────────────────────────────────────────

    #[test]
    fn holepunch_payload_minimal() {
        let m = HolepunchPayload {
            error: ERROR_NONE,
            firewall: FIREWALL_UNKNOWN,
            round: 0,
            connected: false,
            punching: false,
            addresses: None,
            remote_address: None,
            token: None,
            remote_token: None,
        };
        let bytes = encode_holepunch_payload_to_bytes(&m).unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0]); // flags=0, error=0, firewall=0, round=0
        let decoded = decode_holepunch_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn holepunch_payload_connected_punching() {
        let m = HolepunchPayload {
            error: ERROR_NONE,
            firewall: FIREWALL_CONSISTENT,
            round: 3,
            connected: true,
            punching: true,
            addresses: None,
            remote_address: None,
            token: None,
            remote_token: None,
        };
        let bytes = encode_holepunch_payload_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
        // flags should have bits 0 and 1 set (CONNECTED|PUNCHING = 3)
        assert_eq!(bytes[0], 3);
    }

    #[test]
    fn holepunch_payload_with_addresses() {
        let m = HolepunchPayload {
            error: ERROR_NONE,
            firewall: FIREWALL_OPEN,
            round: 1,
            connected: false,
            punching: true,
            addresses: Some(vec![
                make_addr("1.2.3.4", 1000),
                make_addr("5.6.7.8", 2000),
            ]),
            remote_address: Some(make_addr("10.0.0.1", 8080)),
            token: None,
            remote_token: None,
        };
        let bytes = encode_holepunch_payload_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn holepunch_payload_with_tokens() {
        let m = HolepunchPayload {
            error: ERROR_NONE,
            firewall: FIREWALL_RANDOM,
            round: 5,
            connected: false,
            punching: false,
            addresses: None,
            remote_address: None,
            token: Some([0xaa; 32]),
            remote_token: Some([0xbb; 32]),
        };
        let bytes = encode_holepunch_payload_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn holepunch_payload_all_fields() {
        let m = HolepunchPayload {
            error: ERROR_ABORTED,
            firewall: FIREWALL_CONSISTENT,
            round: 10,
            connected: true,
            punching: true,
            addresses: Some(vec![make_addr("192.168.1.1", 3000)]),
            remote_address: Some(make_addr("10.0.0.5", 4000)),
            token: Some([0xcc; 32]),
            remote_token: Some([0xdd; 32]),
        };
        let bytes = encode_holepunch_payload_to_bytes(&m).unwrap();
        let decoded = decode_holepunch_payload_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, m);
        // flags = CONNECTED(1) | PUNCHING(2) | ADDRESSES(4) | REMOTE_ADDRESS(8) | TOKEN(16) | REMOTE_TOKEN(32) = 63
        assert_eq!(bytes[0], 63);
    }

    // ── Sub-type roundtrip tests ────────────────────────────────────────────

    #[test]
    fn relay_info_roundtrip() {
        let ri = RelayInfo {
            relay_address: make_addr("10.0.0.1", 8080),
            peer_address: make_addr("192.168.1.1", 3000),
        };
        let mut state = State::new();
        preencode_relay_info(&mut state, &ri);
        state.alloc();
        encode_relay_info(&mut state, &ri).unwrap();
        let mut state2 = State::from_buffer(&state.buffer);
        let decoded = decode_relay_info(&mut state2).unwrap();
        assert_eq!(decoded, ri);
    }

    #[test]
    fn udx_info_roundtrip() {
        let ui = UdxInfo {
            version: 1,
            reusable_socket: true,
            id: 42,
            seq: 100,
        };
        let mut state = State::new();
        preencode_udx_info(&mut state, &ui);
        state.alloc();
        encode_udx_info(&mut state, &ui).unwrap();
        let mut state2 = State::from_buffer(&state.buffer);
        let decoded = decode_udx_info(&mut state2).unwrap();
        assert_eq!(decoded, ui);
    }

    #[test]
    fn relay_through_info_roundtrip() {
        let rt = RelayThroughInfo {
            version: 1,
            public_key: [0xaa; 32],
            token: [0xbb; 32],
        };
        let mut state = State::new();
        preencode_relay_through_info(&mut state, &rt);
        state.alloc();
        encode_relay_through_info(&mut state, &rt).unwrap();
        let mut state2 = State::from_buffer(&state.buffer);
        let decoded = decode_relay_through_info(&mut state2).unwrap();
        assert_eq!(decoded, rt);
    }

    #[test]
    fn mode_and_firewall_constants() {
        assert_eq!(MODE_FROM_CLIENT, 0);
        assert_eq!(MODE_FROM_SERVER, 1);
        assert_eq!(MODE_FROM_RELAY, 2);
        assert_eq!(MODE_FROM_SECOND_RELAY, 3);
        assert_eq!(MODE_REPLY, 4);
        assert_eq!(FIREWALL_UNKNOWN, 0);
        assert_eq!(FIREWALL_OPEN, 1);
        assert_eq!(FIREWALL_CONSISTENT, 2);
        assert_eq!(FIREWALL_RANDOM, 3);
    }

    #[test]
    fn error_constants() {
        assert_eq!(ERROR_NONE, 0);
        assert_eq!(ERROR_ABORTED, 1);
        assert_eq!(ERROR_VERSION_MISMATCH, 2);
        assert_eq!(ERROR_TRY_LATER, 3);
        assert_eq!(ERROR_SEQ_REUSED, 16);
        assert_eq!(ERROR_SEQ_TOO_LOW, 17);
    }
}
