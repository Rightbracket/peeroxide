//! blind-relay protocol messages — wire-compatible with Node.js `blind-relay@1.4.0`.
//!
//! The blind-relay protocol uses Protomux with protocol name `"blind-relay"`.

#![allow(dead_code)]

use crate::compact_encoding::{self as c, State};
use crate::protomux::{self, Channel, ChannelEvent, Mux};
use thiserror::Error;
use tracing::debug;

/// Pair message — requests relay pairing with a 32-byte token.
#[derive(Debug, Clone, PartialEq)]
pub struct PairMessage {
    /// Indicates whether this peer initiated the pair request.
    pub is_initiator: bool,
    /// Relay token used to match the pair and unpair messages.
    pub token: [u8; 32],
    /// Stream identifier assigned by the local peer.
    pub id: u64,
    /// Sequence number for the pair request.
    pub seq: u64,
}

/// Unpair message — cancels a relay pairing.
#[derive(Debug, Clone, PartialEq)]
pub struct UnpairMessage {
    /// Relay token used to cancel a pending pair request.
    pub token: [u8; 32],
}

/// Protocol name used over Protomux.
pub const PROTOCOL_NAME: &str = "blind-relay";

/// Protomux message type index for pair.
pub const MSG_TYPE_PAIR: u32 = 0;

/// Protomux message type index for unpair.
pub const MSG_TYPE_UNPAIR: u32 = 1;

/// Pre-encodes a [`PairMessage`], advancing the state cursor.
pub fn preencode_pair(state: &mut State, msg: &PairMessage) {
    state.end += 1; // bitfield(7) = 1 byte
    state.end += 32; // fixed32 token
    c::preencode_uint(state, msg.id);
    c::preencode_uint(state, msg.seq);
}

/// Encodes a [`PairMessage`] into the state buffer.
pub(crate) fn encode_pair(state: &mut State, msg: &PairMessage) {
    let flags: u8 = if msg.is_initiator { 1 } else { 0 };
    c::encode_uint8(state, flags);
    c::encode_fixed32(state, &msg.token);
    c::encode_uint(state, msg.id);
    c::encode_uint(state, msg.seq);
}

/// Decodes a [`PairMessage`] from the state buffer.
pub(crate) fn decode_pair(state: &mut State) -> c::Result<PairMessage> {
    let flags = c::decode_uint8(state)?;
    let is_initiator = flags & 1 != 0;
    let token = c::decode_fixed32(state)?;
    let id = c::decode_uint(state)?;
    let seq = c::decode_uint(state)?;
    Ok(PairMessage {
        is_initiator,
        token,
        id,
        seq,
    })
}

/// Pre-encodes a [`UnpairMessage`], advancing the state cursor.
pub fn preencode_unpair(state: &mut State, _msg: &UnpairMessage) {
    state.end += 1; // bitfield(7) = 1 byte
    state.end += 32; // fixed32 token
}

/// Encodes a [`UnpairMessage`] into the state buffer.
pub(crate) fn encode_unpair(state: &mut State, msg: &UnpairMessage) {
    c::encode_uint8(state, 0); // flags = 0
    c::encode_fixed32(state, &msg.token);
}

/// Decodes a [`UnpairMessage`] from the state buffer.
pub(crate) fn decode_unpair(state: &mut State) -> c::Result<UnpairMessage> {
    let _flags = c::decode_uint8(state)?;
    let token = c::decode_fixed32(state)?;
    Ok(UnpairMessage { token })
}

/// Encode a pair message to bytes (preencode + allocate + encode).
pub(crate) fn encode_pair_to_vec(msg: &PairMessage) -> Vec<u8> {
    let mut state = State::new();
    preencode_pair(&mut state, msg);
    state.alloc();
    encode_pair(&mut state, msg);
    state.buffer
}

/// Encode an unpair message to bytes.
pub(crate) fn encode_unpair_to_vec(msg: &UnpairMessage) -> Vec<u8> {
    let mut state = State::new();
    preencode_unpair(&mut state, msg);
    state.alloc();
    encode_unpair(&mut state, msg);
    state.buffer
}

/// Decode a pair message from bytes.
pub(crate) fn decode_pair_from_slice(data: &[u8]) -> c::Result<PairMessage> {
    let mut state = State::from_buffer(data);
    decode_pair(&mut state)
}

/// Decode an unpair message from bytes.
pub(crate) fn decode_unpair_from_slice(data: &[u8]) -> c::Result<UnpairMessage> {
    let mut state = State::from_buffer(data);
    decode_unpair(&mut state)
}

// ── Client ───────────────────────────────────────────────────────────────────

/// Errors that can occur while using the blind relay client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RelayError {
    /// A Protomux operation failed while opening, sending, or receiving.
    #[error("protomux error: {0}")]
    Protomux(#[from] protomux::ProtomuxError),

    /// Encoding or decoding of relay messages failed.
    #[error("encoding error: {0}")]
    Encoding(#[from] c::EncodingError),

    /// The channel closed before a matching pair response arrived.
    #[error("channel closed before pair response")]
    ChannelClosed,

    /// The client was destroyed before the operation could complete.
    #[error("relay client destroyed")]
    Destroyed,

    /// A pair request with this token is already in flight.
    #[error("already pairing with this token")]
    AlreadyPairing,
}

/// Response from a successful relay pairing.
#[derive(Debug, Clone)]
pub struct PairResponse {
    /// Relay-assigned remote stream identifier.
    pub remote_id: u64,
}

/// Client-side blind-relay over an existing Protomux connection.
///
/// Wraps a Protomux channel with protocol `"blind-relay"`. Sends pair/unpair
/// messages and waits for the relay server to match the token.
pub struct BlindRelayClient {
    channel: Channel,
}

impl BlindRelayClient {
    /// Open a blind-relay channel on the given Mux.
    ///
    /// `id` should be the local public key used when connecting to the relay.
    /// Both the relay server and the connecting peer must use the same `id`
    /// (the connecting peer's public key) so that Protomux can pair the
    /// channels correctly.
    ///
    /// Sends the Open frame immediately. Call [`Self::wait_opened`] before
    /// sending pair/unpair messages.
    pub async fn open(mux: &Mux, id: Option<Vec<u8>>) -> Result<Self, RelayError> {
        let channel = mux.create_channel(PROTOCOL_NAME, id, None).await?;
        Ok(Self { channel })
    }

    /// Wait for the remote side to open the channel.
    pub async fn wait_opened(&mut self) -> Result<(), RelayError> {
        self.channel.wait_opened().await?;
        Ok(())
    }

    /// Send a pair request and wait for the relay server's response.
    ///
    /// Returns the relay-assigned `remote_id` (UDX stream ID on the relay side).
    /// Blocks until the server sends a matching pair message back.
    pub async fn pair(
        &mut self,
        is_initiator: bool,
        token: &[u8; 32],
        stream_id: u64,
    ) -> Result<PairResponse, RelayError> {
        let msg = PairMessage {
            is_initiator,
            token: *token,
            id: stream_id,
            seq: 0,
        };
        self.channel
            .send(MSG_TYPE_PAIR, &encode_pair_to_vec(&msg))?;

        debug!(
            is_initiator,
            token = %format_args!("{:02x?}", &token[..4]),
            stream_id,
            "sent pair request"
        );

        loop {
            match self.channel.recv().await {
                Some(ChannelEvent::Message { message_type, data }) => {
                    if message_type == MSG_TYPE_PAIR {
                        let response = decode_pair_from_slice(&data)?;
                        if response.token == *token && response.is_initiator == is_initiator {
                            debug!(
                                remote_id = response.id,
                                "pair response received"
                            );
                            return Ok(PairResponse {
                                remote_id: response.id,
                            });
    }
}

#[cfg(test)]
mod golden_interop {
    use super::{
        decode_pair_from_slice, decode_unpair_from_slice, encode_pair_to_vec, encode_unpair_to_vec,
        PairMessage, UnpairMessage,
    };
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct GoldenFile {
        #[allow(dead_code)]
        generated_by: String,
        #[allow(dead_code)]
        blind_relay_version: String,
        fixtures: Vec<Fixture>,
    }

    #[derive(Deserialize)]
    struct Fixture {
        label: String,
        #[serde(rename = "type")]
        typ: String,
        hex: String,
        decoded: serde_json::Value,
    }

    fn load_fixtures() -> Vec<Fixture> {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/interop/blind-relay-fixtures.json"
        );
        let data = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!("Failed to read blind-relay fixtures at {path}: {e}. Run `node generate-blind-relay-golden.js` in tests/node/ first.")
        });
        let file: GoldenFile = serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("Failed to parse blind-relay fixtures: {e}"));
        file.fixtures
    }

    fn from_hex(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap_or_else(|e| panic!("Invalid hex '{s}': {e}"))
    }

    fn token_from_hex(s: &str) -> [u8; 32] {
        let v = from_hex(s);
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&v);
        arr
    }

    #[test]
    fn golden_blind_relay_decode_pair() {
        let fixtures = load_fixtures();
        let pairs: Vec<_> = fixtures.iter().filter(|f| f.typ == "pair").collect();
        assert!(!pairs.is_empty(), "no pair fixtures found");

        for fix in pairs {
            let raw = from_hex(&fix.hex);
            let decoded = decode_pair_from_slice(&raw)
                .unwrap_or_else(|e| panic!("[{}] decode failed: {e}", fix.label));

            let d = &fix.decoded;
            let expected = PairMessage {
                is_initiator: d["is_initiator"].as_bool().unwrap(),
                token: token_from_hex(d["token"].as_str().unwrap()),
                id: d["id"].as_u64().unwrap(),
                seq: d["seq"].as_u64().unwrap(),
            };

            assert_eq!(decoded, expected, "[{}] decode mismatch", fix.label);
        }
    }

    #[test]
    fn golden_blind_relay_decode_unpair() {
        let fixtures = load_fixtures();
        let unpairs: Vec<_> = fixtures.iter().filter(|f| f.typ == "unpair").collect();
        assert!(!unpairs.is_empty(), "no unpair fixtures found");

        for fix in unpairs {
            let raw = from_hex(&fix.hex);
            let decoded = decode_unpair_from_slice(&raw)
                .unwrap_or_else(|e| panic!("[{}] decode failed: {e}", fix.label));

            let expected = UnpairMessage {
                token: token_from_hex(fix.decoded["token"].as_str().unwrap()),
            };

            assert_eq!(decoded, expected, "[{}] decode mismatch", fix.label);
        }
    }

    #[test]
    fn golden_blind_relay_encode_roundtrip() {
        let fixtures = load_fixtures();

        for fix in &fixtures {
            let expected_bytes = from_hex(&fix.hex);
            let d = &fix.decoded;

            let encoded = match fix.typ.as_str() {
                "pair" => {
                    let msg = PairMessage {
                        is_initiator: d["is_initiator"].as_bool().unwrap(),
                        token: token_from_hex(d["token"].as_str().unwrap()),
                        id: d["id"].as_u64().unwrap(),
                        seq: d["seq"].as_u64().unwrap(),
                    };
                    encode_pair_to_vec(&msg)
                }
                "unpair" => {
                    let msg = UnpairMessage {
                        token: token_from_hex(d["token"].as_str().unwrap()),
                    };
                    encode_unpair_to_vec(&msg)
                }
                other => panic!("[{}] unknown fixture type: {other}", fix.label),
            };

            assert_eq!(
                encoded, expected_bytes,
                "[{}] encode roundtrip mismatch\n  encoded: {}\n  expected: {}",
                fix.label,
                hex::encode(&encoded),
                fix.hex,
            );
        }
    }
}
                }
                Some(ChannelEvent::Closed { .. }) | None => {
                    return Err(RelayError::ChannelClosed);
                }
                Some(ChannelEvent::Opened { .. }) => {}
            }
        }
    }

    /// Cancel a pending pair request.
    pub fn unpair(&self, token: &[u8; 32]) -> Result<(), RelayError> {
        let msg = UnpairMessage { token: *token };
        self.channel
            .send(MSG_TYPE_UNPAIR, &encode_unpair_to_vec(&msg))?;
        Ok(())
    }

    /// Close the blind-relay channel.
    pub fn close(&mut self) {
        self.channel.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protomux::{FramedStream, Mux};
    use tokio::sync::mpsc;

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

    fn mem_pair() -> (MemStream, MemStream) {
        let (tx_a, rx_b) = mpsc::unbounded_channel();
        let (tx_b, rx_a) = mpsc::unbounded_channel();
        (
            MemStream { rx: rx_a, tx: tx_a },
            MemStream { rx: rx_b, tx: tx_b },
        )
    }

    #[test]
    fn pair_roundtrip_initiator() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0xaa; 32],
            id: 42,
            seq: 7,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pair_roundtrip_responder() {
        let msg = PairMessage {
            is_initiator: false,
            token: [0xbb; 32],
            id: 0,
            seq: 0,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn unpair_roundtrip() {
        let msg = UnpairMessage {
            token: [0xcc; 32],
        };
        let encoded = encode_unpair_to_vec(&msg);
        let decoded = decode_unpair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pair_wire_format() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0x42; 32],
            id: 1,
            seq: 2,
        };
        let encoded = encode_pair_to_vec(&msg);

        assert_eq!(encoded[0], 0x01); // flags: bit0=1 (initiator)
        assert_eq!(&encoded[1..33], &[0x42; 32]); // token
        assert_eq!(encoded[33], 0x01); // id=1 (varint)
        assert_eq!(encoded[34], 0x02); // seq=2 (varint)
        assert_eq!(encoded.len(), 35);
    }

    #[test]
    fn pair_wire_format_responder() {
        let msg = PairMessage {
            is_initiator: false,
            token: [0x00; 32],
            id: 0,
            seq: 0,
        };
        let encoded = encode_pair_to_vec(&msg);

        assert_eq!(encoded[0], 0x00); // flags: bit0=0 (responder)
        assert_eq!(&encoded[1..33], &[0x00; 32]); // token
        assert_eq!(encoded[33], 0x00); // id=0
        assert_eq!(encoded[34], 0x00); // seq=0
    }

    #[test]
    fn unpair_wire_format() {
        let msg = UnpairMessage {
            token: [0xff; 32],
        };
        let encoded = encode_unpair_to_vec(&msg);

        assert_eq!(encoded[0], 0x00); // flags: all zero
        assert_eq!(&encoded[1..33], &[0xff; 32]); // token
        assert_eq!(encoded.len(), 33);
    }

    #[test]
    fn pair_large_ids() {
        let msg = PairMessage {
            is_initiator: true,
            token: [0xde; 32],
            id: 100_000,
            seq: 65_536,
        };
        let encoded = encode_pair_to_vec(&msg);
        let decoded = decode_pair_from_slice(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn protocol_name_constant() {
        assert_eq!(PROTOCOL_NAME, "blind-relay");
    }

    #[tokio::test]
    async fn client_pair_with_fake_relay() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let token = [0xaa; 32];

        let client_task = tokio::spawn(async move {
            let mut client = BlindRelayClient::open(&mux_a, None).await.unwrap();
            client.wait_opened().await.unwrap();
            let resp = client.pair(true, &token, 42).await.unwrap();
            client.close();
            resp
        });

        // Fake relay server: open matching channel, wait for pair, send response
        let mut server_ch = mux_b
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();
        server_ch.wait_opened().await.unwrap();

        let event = server_ch.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, MSG_TYPE_PAIR);
                let pair_msg = decode_pair_from_slice(&data).unwrap();
                assert!(pair_msg.is_initiator);
                assert_eq!(pair_msg.token, token);
                assert_eq!(pair_msg.id, 42);

                let reply = PairMessage {
                    is_initiator: true,
                    token,
                    id: 99,
                    seq: 0,
                };
                server_ch
                    .send(MSG_TYPE_PAIR, &encode_pair_to_vec(&reply))
                    .unwrap();
            }
            other => panic!("expected pair Message, got {other:?}"),
        }

        let resp = client_task.await.unwrap();
        assert_eq!(resp.remote_id, 99);
    }

    #[tokio::test]
    async fn client_unpair() {
        let (stream_a, stream_b) = mem_pair();

        let (mux_a, run_a) = Mux::new(stream_a);
        let (mux_b, run_b) = Mux::new(stream_b);

        tokio::spawn(run_a);
        tokio::spawn(run_b);

        let token = [0xbb; 32];

        let mut client = BlindRelayClient::open(&mux_a, None).await.unwrap();
        let mut server_ch = mux_b
            .create_channel(PROTOCOL_NAME, None, None)
            .await
            .unwrap();

        client.wait_opened().await.unwrap();
        server_ch.wait_opened().await.unwrap();

        client.unpair(&token).unwrap();

        let event = server_ch.recv().await.unwrap();
        match event {
            ChannelEvent::Message { message_type, data } => {
                assert_eq!(message_type, MSG_TYPE_UNPAIR);
                let unpair_msg = decode_unpair_from_slice(&data).unwrap();
                assert_eq!(unpair_msg.token, token);
            }
            other => panic!("expected unpair Message, got {other:?}"),
        }

        client.close();
    }
}
