use crate::compact_encoding::{self, EncodingError, State};
use crate::peer::NodeId;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
/// DHT RPC command kinds.
pub enum Command {
    /// Pings a peer.
    Ping = 0,
    /// Pings a peer over NAT.
    PingNat = 1,
    /// Finds the closest nodes to a target.
    FindNode = 2,
    /// Reports a down-hint for a peer.
    DownHint = 3,
    /// Sends a delayed ping.
    DelayedPing = 4,
}

impl Command {
    /// Converts a numeric code into a [`Command`].
    pub fn from_u64(v: u64) -> Option<Self> {
        match v {
            0 => Some(Self::Ping),
            1 => Some(Self::PingNat),
            2 => Some(Self::FindNode),
            3 => Some(Self::DownHint),
            4 => Some(Self::DelayedPing),
            _ => None,
        }
    }
}

const VERSION: u8 = 0b11;
const REQUEST_ID: u8 = VERSION;
const RESPONSE_ID: u8 = (0b0001 << 4) | VERSION;

const FLAG_ID: u8 = 0b0000_0001;
const FLAG_TOKEN: u8 = 0b0000_0010;
const FLAG_INTERNAL_OR_CLOSER: u8 = 0b0000_0100;
const FLAG_TARGET_OR_ERROR: u8 = 0b0000_1000;
const FLAG_VALUE: u8 = 0b0001_0000;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// A peer network address (host and port).
///
/// Despite the name, this type may carry either an IPv4 or IPv6 address
/// depending on context—for example, `addresses6` fields decode into
/// `Vec<Ipv4Peer>`. The name is inherited from the upstream JavaScript
/// implementation.
pub struct Ipv4Peer {
    /// The peer host (IPv4 or IPv6 address string).
    pub host: String,
    /// The peer port.
    pub port: u16,
}

#[derive(Debug, Clone)]
/// A DHT request message.
pub struct Request {
    /// The transaction identifier.
    pub tid: u16,
    /// The destination peer.
    pub to: Ipv4Peer,
    /// The optional sender id.
    pub id: Option<NodeId>,
    /// The optional request token.
    pub token: Option<[u8; 32]>,
    /// Whether the request is internal.
    pub internal: bool,
    /// The numeric command code.
    pub command: u64,
    /// The optional target node id.
    pub target: Option<NodeId>,
    /// The optional payload value.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
/// A DHT response message.
pub struct Response {
    /// The transaction identifier.
    pub tid: u16,
    /// The destination peer.
    pub to: Ipv4Peer,
    /// The optional sender id.
    pub id: Option<NodeId>,
    /// The optional response token.
    pub token: Option<[u8; 32]>,
    /// The closer peers returned by the lookup.
    pub closer_nodes: Vec<Ipv4Peer>,
    /// The response error code.
    pub error: u64,
    /// The optional payload value.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
/// A decoded DHT wire message.
pub enum Message {
    /// A request message.
    Request(Request),
    /// A response message.
    Response(Response),
}

fn preencode_ipv4_peer(state: &mut State, _peer: &Ipv4Peer) {
    state.end += 6;
}

fn encode_ipv4_peer(state: &mut State, peer: &Ipv4Peer) -> Result<(), EncodingError> {
    compact_encoding::encode_ipv4(state, &peer.host)?;
    let bytes = peer.port.to_le_bytes();
    state.buffer[state.start] = bytes[0];
    state.buffer[state.start + 1] = bytes[1];
    state.start += 2;
    Ok(())
}

fn decode_ipv4_peer(state: &mut State) -> Result<Ipv4Peer, EncodingError> {
    let host = compact_encoding::decode_ipv4(state)?;
    let port = compact_encoding::decode_uint16(state)?;
    Ok(Ipv4Peer { host, port })
}

fn preencode_ipv4_array(state: &mut State, peers: &[Ipv4Peer]) {
    compact_encoding::preencode_uint(state, peers.len() as u64);
    state.end += peers.len() * 6;
}

fn encode_ipv4_array(state: &mut State, peers: &[Ipv4Peer]) -> Result<(), EncodingError> {
    compact_encoding::encode_uint(state, peers.len() as u64);
    for peer in peers {
        encode_ipv4_peer(state, peer)?;
    }
    Ok(())
}

fn decode_ipv4_array(state: &mut State) -> Result<Vec<Ipv4Peer>, EncodingError> {
    let len = compact_encoding::decode_uint(state)? as usize;
    if len > 1048576 {
        return Err(EncodingError::ArrayTooLarge(len));
    }
    let mut peers = Vec::with_capacity(len);
    for _ in 0..len {
        peers.push(decode_ipv4_peer(state)?);
    }
    Ok(peers)
}

/// Pre-encodes a [`Request`] to calculate the required buffer size.
pub fn preencode_request(state: &mut State, req: &Request) {
    state.end += 1; // type_version
    state.end += 1; // flags
    state.end += 2; // tid (uint16)
    preencode_ipv4_peer(state, &req.to); // to peer

    if req.id.is_some() {
        state.end += 32;
    }
    if req.token.is_some() {
        state.end += 32;
    }
    compact_encoding::preencode_uint(state, req.command);
    if req.target.is_some() {
        state.end += 32;
    }
    if req.value.is_some() {
        compact_encoding::preencode_buffer(state, req.value.as_deref());
    }
}

/// Encodes a [`Request`] into the compact-encoding buffer.
pub fn encode_request(state: &mut State, req: &Request) -> Result<(), EncodingError> {
    compact_encoding::encode_uint8(state, REQUEST_ID);

    let mut flags: u8 = 0;
    if req.id.is_some() {
        flags |= FLAG_ID;
    }
    if req.token.is_some() {
        flags |= FLAG_TOKEN;
    }
    if req.internal {
        flags |= FLAG_INTERNAL_OR_CLOSER;
    }
    if req.target.is_some() {
        flags |= FLAG_TARGET_OR_ERROR;
    }
    if req.value.is_some() {
        flags |= FLAG_VALUE;
    }
    compact_encoding::encode_uint8(state, flags);

    compact_encoding::encode_uint16(state, req.tid);
    encode_ipv4_peer(state, &req.to)?;

    if let Some(id) = &req.id {
        compact_encoding::encode_fixed32(state, id);
    }
    if let Some(token) = &req.token {
        compact_encoding::encode_fixed32(state, token);
    }
    compact_encoding::encode_uint(state, req.command);
    if let Some(target) = &req.target {
        compact_encoding::encode_fixed32(state, target);
    }
    if req.value.is_some() {
        compact_encoding::encode_buffer(state, req.value.as_deref());
    }

    Ok(())
}

/// Pre-encodes a [`Response`] to calculate the required buffer size.
pub fn preencode_response(state: &mut State, res: &Response) {
    state.end += 1; // type_version
    state.end += 1; // flags
    state.end += 2; // tid (uint16)
    preencode_ipv4_peer(state, &res.to); // to peer

    if res.id.is_some() {
        state.end += 32;
    }
    if res.token.is_some() {
        state.end += 32;
    }
    if !res.closer_nodes.is_empty() {
        preencode_ipv4_array(state, &res.closer_nodes);
    }
    if res.error != 0 {
        compact_encoding::preencode_uint(state, res.error);
    }
    if res.value.is_some() {
        compact_encoding::preencode_buffer(state, res.value.as_deref());
    }
}

/// Encodes a [`Response`] into the compact-encoding buffer.
pub fn encode_response(state: &mut State, res: &Response) -> Result<(), EncodingError> {
    compact_encoding::encode_uint8(state, RESPONSE_ID);

    let mut flags: u8 = 0;
    if res.id.is_some() {
        flags |= FLAG_ID;
    }
    if res.token.is_some() {
        flags |= FLAG_TOKEN;
    }
    if !res.closer_nodes.is_empty() {
        flags |= FLAG_INTERNAL_OR_CLOSER;
    }
    if res.error != 0 {
        flags |= FLAG_TARGET_OR_ERROR;
    }
    if res.value.is_some() {
        flags |= FLAG_VALUE;
    }
    compact_encoding::encode_uint8(state, flags);

    compact_encoding::encode_uint16(state, res.tid);
    encode_ipv4_peer(state, &res.to)?;

    if let Some(id) = &res.id {
        compact_encoding::encode_fixed32(state, id);
    }
    if let Some(token) = &res.token {
        compact_encoding::encode_fixed32(state, token);
    }
    if !res.closer_nodes.is_empty() {
        encode_ipv4_array(state, &res.closer_nodes)?;
    }
    if res.error != 0 {
        compact_encoding::encode_uint(state, res.error);
    }
    if res.value.is_some() {
        compact_encoding::encode_buffer(state, res.value.as_deref());
    }

    Ok(())
}

/// Decodes a [`Message`] from the compact-encoding buffer.
pub fn decode_message(buf: &[u8]) -> Result<Message, EncodingError> {
    if buf.is_empty() {
        return Err(EncodingError::OutOfBounds {
            need: 1,
            have: 0,
        });
    }

    let type_version = buf[0];
    let version = type_version & 0x0F;
    if version != VERSION {
        return Err(EncodingError::InvalidIpFamily(type_version));
    }

    let mut state = State::from_buffer(buf);
    state.start = 1; // skip type_version already consumed

    let flags = compact_encoding::decode_uint8(&mut state)?;
    let tid = compact_encoding::decode_uint16(&mut state)?;
    let to = decode_ipv4_peer(&mut state)?;

    let id = if flags & FLAG_ID != 0 {
        Some(compact_encoding::decode_fixed32(&mut state)?)
    } else {
        None
    };

    let token = if flags & FLAG_TOKEN != 0 {
        Some(compact_encoding::decode_fixed32(&mut state)?)
    } else {
        None
    };

    let is_response = type_version & 0xF0 != 0;

    if is_response {
        let closer_nodes = if flags & FLAG_INTERNAL_OR_CLOSER != 0 {
            decode_ipv4_array(&mut state)?
        } else {
            Vec::new()
        };

        let error = if flags & FLAG_TARGET_OR_ERROR != 0 {
            compact_encoding::decode_uint(&mut state)?
        } else {
            0
        };

        let value = if flags & FLAG_VALUE != 0 {
            compact_encoding::decode_buffer(&mut state)?
        } else {
            None
        };

        Ok(Message::Response(Response {
            tid,
            to,
            id,
            token,
            closer_nodes,
            error,
            value,
        }))
    } else {
        let internal = flags & FLAG_INTERNAL_OR_CLOSER != 0;

        let command = compact_encoding::decode_uint(&mut state)?;

        let target = if flags & FLAG_TARGET_OR_ERROR != 0 {
            Some(compact_encoding::decode_fixed32(&mut state)?)
        } else {
            None
        };

        let value = if flags & FLAG_VALUE != 0 {
            compact_encoding::decode_buffer(&mut state)?
        } else {
            None
        };

        Ok(Message::Request(Request {
            tid,
            to,
            id,
            token,
            internal,
            command,
            target,
            value,
        }))
    }
}

/// Serializes a [`Request`] to a byte vector.
pub fn encode_request_to_bytes(req: &Request) -> Result<Vec<u8>, EncodingError> {
    let mut state = State::new();
    preencode_request(&mut state, req);
    state.alloc();
    encode_request(&mut state, req)?;
    Ok(state.buffer)
}

/// Serializes a [`Response`] to a byte vector.
pub fn encode_response_to_bytes(res: &Response) -> Result<Vec<u8>, EncodingError> {
    let mut state = State::new();
    preencode_response(&mut state, res);
    state.alloc();
    encode_response(&mut state, res)?;
    Ok(state.buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_request() -> Request {
        Request {
            tid: 42,
            to: Ipv4Peer {
                host: "127.0.0.1".into(),
                port: 49737,
            },
            id: None,
            token: None,
            internal: false,
            command: Command::Ping as u64,
            target: None,
            value: None,
        }
    }

    fn make_test_response() -> Response {
        Response {
            tid: 42,
            to: Ipv4Peer {
                host: "127.0.0.1".into(),
                port: 49737,
            },
            id: None,
            token: None,
            closer_nodes: Vec::new(),
            error: 0,
            value: None,
        }
    }

    #[test]
    fn request_roundtrip_minimal() {
        let req = make_test_request();
        let bytes = encode_request_to_bytes(&req).unwrap();
        assert_eq!(bytes[0], REQUEST_ID);

        let msg = decode_message(&bytes).unwrap();
        let Message::Request(decoded) = msg else {
            panic!("expected request");
        };
        assert_eq!(decoded.tid, 42);
        assert_eq!(decoded.command, Command::Ping as u64);
        assert!(!decoded.internal);
        assert!(decoded.id.is_none());
        assert!(decoded.token.is_none());
        assert!(decoded.target.is_none());
        assert!(decoded.value.is_none());
    }

    #[test]
    fn response_roundtrip_minimal() {
        let res = make_test_response();
        let bytes = encode_response_to_bytes(&res).unwrap();
        assert_eq!(bytes[0], RESPONSE_ID);

        let msg = decode_message(&bytes).unwrap();
        let Message::Response(decoded) = msg else {
            panic!("expected response");
        };
        assert_eq!(decoded.tid, 42);
        assert!(decoded.closer_nodes.is_empty());
        assert_eq!(decoded.error, 0);
        assert!(decoded.id.is_none());
        assert!(decoded.token.is_none());
        assert!(decoded.value.is_none());
    }

    #[test]
    fn request_roundtrip_full() {
        let req = Request {
            tid: 1000,
            to: Ipv4Peer {
                host: "10.0.0.1".into(),
                port: 8080,
            },
            id: Some([0xAA; 32]),
            token: Some([0xBB; 32]),
            internal: true,
            command: Command::FindNode as u64,
            target: Some([0xCC; 32]),
            value: Some(vec![1, 2, 3, 4]),
        };
        let bytes = encode_request_to_bytes(&req).unwrap();
        let msg = decode_message(&bytes).unwrap();
        let Message::Request(decoded) = msg else {
            panic!("expected request");
        };
        assert_eq!(decoded.tid, 1000);
        assert_eq!(decoded.to.host, "10.0.0.1");
        assert_eq!(decoded.to.port, 8080);
        assert_eq!(decoded.id, Some([0xAA; 32]));
        assert_eq!(decoded.token, Some([0xBB; 32]));
        assert!(decoded.internal);
        assert_eq!(decoded.command, Command::FindNode as u64);
        assert_eq!(decoded.target, Some([0xCC; 32]));
        assert_eq!(decoded.value, Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn response_roundtrip_with_closer_nodes() {
        let res = Response {
            tid: 500,
            to: Ipv4Peer {
                host: "192.168.1.1".into(),
                port: 3000,
            },
            id: Some([0x11; 32]),
            token: Some([0x22; 32]),
            closer_nodes: vec![
                Ipv4Peer {
                    host: "10.0.0.1".into(),
                    port: 8080,
                },
                Ipv4Peer {
                    host: "10.0.0.2".into(),
                    port: 9090,
                },
            ],
            error: 0,
            value: Some(b"hello".to_vec()),
        };
        let bytes = encode_response_to_bytes(&res).unwrap();
        let msg = decode_message(&bytes).unwrap();
        let Message::Response(decoded) = msg else {
            panic!("expected response");
        };
        assert_eq!(decoded.tid, 500);
        assert_eq!(decoded.closer_nodes.len(), 2);
        assert_eq!(decoded.closer_nodes[0].host, "10.0.0.1");
        assert_eq!(decoded.closer_nodes[0].port, 8080);
        assert_eq!(decoded.closer_nodes[1].host, "10.0.0.2");
        assert_eq!(decoded.closer_nodes[1].port, 9090);
        assert_eq!(decoded.value, Some(b"hello".to_vec()));
    }

    #[test]
    fn response_roundtrip_with_error() {
        let res = Response {
            tid: 100,
            to: Ipv4Peer {
                host: "127.0.0.1".into(),
                port: 5000,
            },
            id: None,
            token: None,
            closer_nodes: Vec::new(),
            error: 42,
            value: None,
        };
        let bytes = encode_response_to_bytes(&res).unwrap();
        let msg = decode_message(&bytes).unwrap();
        let Message::Response(decoded) = msg else {
            panic!("expected response");
        };
        assert_eq!(decoded.error, 42);
    }

    #[test]
    fn request_type_byte() {
        assert_eq!(REQUEST_ID, 0x03);
    }

    #[test]
    fn response_type_byte() {
        assert_eq!(RESPONSE_ID, 0x13);
    }

    #[test]
    fn decode_empty_buffer_fails() {
        assert!(decode_message(&[]).is_err());
    }

    #[test]
    fn decode_wrong_version_fails() {
        assert!(decode_message(&[0x01]).is_err());
    }

    #[test]
    fn ipv4_array_roundtrip() {
        let peers = vec![
            Ipv4Peer {
                host: "10.0.0.1".into(),
                port: 8080,
            },
            Ipv4Peer {
                host: "192.168.1.1".into(),
                port: 3000,
            },
        ];
        let mut state = State::new();
        preencode_ipv4_array(&mut state, &peers);
        state.alloc();
        encode_ipv4_array(&mut state, &peers).unwrap();

        let mut dec = State::from_buffer(&state.buffer);
        let decoded = decode_ipv4_array(&mut dec).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].host, "10.0.0.1");
        assert_eq!(decoded[0].port, 8080);
        assert_eq!(decoded[1].host, "192.168.1.1");
        assert_eq!(decoded[1].port, 3000);
    }

    #[test]
    fn command_values() {
        assert_eq!(Command::Ping as u64, 0);
        assert_eq!(Command::PingNat as u64, 1);
        assert_eq!(Command::FindNode as u64, 2);
        assert_eq!(Command::DownHint as u64, 3);
        assert_eq!(Command::DelayedPing as u64, 4);
    }
}
