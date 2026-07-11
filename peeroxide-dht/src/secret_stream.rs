//! Encrypted transport: Noise XX handshake + secretstream over `AsyncRead + AsyncWrite`.
//!
//! Wire protocol (matching `@hyperswarm/secret-stream`):
//!
//! ```text
//! FRAMING: Every message is prefixed with a 3-byte little-endian uint24 length.
//!
//! NOISE XX HANDSHAKE:
//!   [3B len=32 ][32B Noise M1]                    initiator → responder
//!   [3B len=96 ][96B Noise M2]                    responder → initiator
//!   [3B len=64 ][64B Noise M3]                    initiator → responder
//!
//! ID HEADER EXCHANGE (both sides, immediately after handshake):
//!   [3B len=56 ][32B stream_id][24B secretstream_header]
//!
//! APPLICATION MESSAGES:
//!   [3B len=L+17][1B enc_tag][LB ciphertext][16B MAC]
//! ```

use std::sync::LazyLock;

use blake2::digest::{KeyInit, Mac};
use blake2::Blake2bMac;
use blake2::digest::consts::U32;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tracing::debug;

use crate::crypto;
use crate::noise::{self, Keypair, NoiseError};
use crate::secretstream::{self, ABYTES, HEADERBYTES, KEYBYTES};

type Blake2bMac256 = Blake2bMac<U32>;

// ── Constants ────────────────────────────────────────────────────────────────

/// Size of the ID header body: 32-byte stream ID + 24-byte secretstream header.
const IDHEADERBYTES: usize = HEADERBYTES + 32;

/// Maximum message body that fits in a 3-byte LE length prefix.
const MAX_MESSAGE_LEN: usize = 0xFF_FF_FF; // 16,777,215

/// Namespace constants for `hyperswarm/secret-stream`.
/// Index 0 = initiator, 1 = responder, 2 = send (unordered, unused for now).
static NS_SECRET_STREAM: LazyLock<[[u8; 32]; 3]> = LazyLock::new(|| {
    let ns = crypto::namespace("hyperswarm/secret-stream", &[0, 1, 2]);
    [ns[0], ns[1], ns[2]]
});

fn ns_initiator() -> &'static [u8; 32] {
    &NS_SECRET_STREAM[0]
}
fn ns_responder() -> &'static [u8; 32] {
    &NS_SECRET_STREAM[1]
}

// ── Errors ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
/// Errors returned by `SecretStream` and its framing helpers.
#[non_exhaustive]
pub enum SecretStreamError {
    /// An underlying I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Noise handshake negotiation failed.
    #[error("noise handshake failed: {0}")]
    Noise(#[from] NoiseError),

    /// Secretstream decryption failed.
    #[error("secretstream decryption failed: {0}")]
    Decrypt(#[from] secretstream::SecretstreamError),

    /// The handshake ended before completion.
    #[error("handshake did not complete")]
    HandshakeIncomplete,

    /// EOF was encountered while the handshake was still in progress.
    #[error("unexpected EOF during handshake")]
    UnexpectedEof,

    /// The ID header length was invalid.
    #[error("invalid ID header: expected {expected} bytes, got {got}")]
    InvalidIdHeader {
        /// The expected header length in bytes.
        expected: usize,
        /// The actual header length received.
        got: usize,
    },

    /// The remote stream ID did not match the expected identity.
    #[error("stream ID mismatch: remote peer sent wrong identity")]
    StreamIdMismatch,

    /// The framed message exceeded the maximum allowed length.
    #[error("message too large: {len} bytes exceeds maximum {MAX_MESSAGE_LEN}")]
    MessageTooLarge {
        /// The length of the oversized message in bytes.
        len: usize,
    },
}

// ── SecretStream ─────────────────────────────────────────────────────────────

/// An encrypted bidirectional stream.
///
/// Wraps any `AsyncRead + AsyncWrite` transport with a Noise XX handshake
/// followed by libsodium-compatible secretstream encryption.
pub struct SecretStream<T> {
    raw: T,
    encrypt: secretstream::Push,
    decrypt: secretstream::Pull,
    remote_public_key: [u8; 32],
    handshake_hash: [u8; 64],
    is_initiator: bool,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> SecretStream<T> {
    /// Perform the Noise XX handshake + ID header exchange, returning a ready
    /// encrypted stream.
    ///
    /// - `is_initiator`: true for the connecting side, false for the accepting side.
    /// - `raw`: the underlying transport (e.g. `TcpStream`, UDX stream adapter).
    /// - `keypair`: the local Ed25519 static keypair.
    pub async fn new(
        is_initiator: bool,
        mut raw: T,
        keypair: Keypair,
    ) -> Result<Self, SecretStreamError> {
        // ── Phase 1: Noise XX handshake ──────────────────────────────────
        let mut handshake = noise::Handshake::new(is_initiator, keypair);

        if is_initiator {
            // → M1
            let m1 = handshake.send().map_err(SecretStreamError::Noise)?;
            write_frame(&mut raw, &m1).await?;

            // ← M2
            let m2 = read_frame(&mut raw)
                .await?
                .ok_or(SecretStreamError::UnexpectedEof)?;
            handshake.recv(&m2).map_err(SecretStreamError::Noise)?;

            // → M3
            let m3 = handshake.send().map_err(SecretStreamError::Noise)?;
            write_frame(&mut raw, &m3).await?;
        } else {
            // ← M1
            let m1 = read_frame(&mut raw)
                .await?
                .ok_or(SecretStreamError::UnexpectedEof)?;
            handshake.recv(&m1).map_err(SecretStreamError::Noise)?;

            // → M2
            let m2 = handshake.send().map_err(SecretStreamError::Noise)?;
            write_frame(&mut raw, &m2).await?;

            // ← M3
            let m3 = read_frame(&mut raw)
                .await?
                .ok_or(SecretStreamError::UnexpectedEof)?;
            handshake.recv(&m3).map_err(SecretStreamError::Noise)?;
        }

        let hr = handshake
            .result()
            .ok_or(SecretStreamError::HandshakeIncomplete)?
            .clone();

        debug!(
            is_initiator,
            "noise handshake complete"
        );

        // ── Phase 2: ID header exchange ──────────────────────────────────
        // Create encryptor → produces the 24-byte secretstream header.
        let tx_key: [u8; KEYBYTES] = hr.tx;
        let (encrypt, ss_header) = secretstream::Push::new(&tx_key);

        // Build local ID message: [32B stream_id][24B secretstream_header]
        let local_id = stream_id(&hr.handshake_hash, is_initiator);
        let mut id_msg = Vec::with_capacity(IDHEADERBYTES);
        id_msg.extend_from_slice(&local_id);
        id_msg.extend_from_slice(&ss_header);
        write_frame(&mut raw, &id_msg).await?;

        // Read remote ID message.
        let remote_msg = read_frame(&mut raw)
            .await?
            .ok_or(SecretStreamError::UnexpectedEof)?;
        if remote_msg.len() != IDHEADERBYTES {
            return Err(SecretStreamError::InvalidIdHeader {
                expected: IDHEADERBYTES,
                got: remote_msg.len(),
            });
        }

        // Validate remote stream ID.
        let remote_id = &remote_msg[..32];
        let expected_id = stream_id(&hr.handshake_hash, !is_initiator);
        if remote_id != expected_id {
            return Err(SecretStreamError::StreamIdMismatch);
        }

        // Initialize decryptor with remote's secretstream header.
        let remote_header: [u8; HEADERBYTES] = remote_msg[32..56]
            .try_into()
            // SAFETY: remote_msg.len() == IDHEADERBYTES (56) verified above; [32..56] is 24 bytes == HEADERBYTES.
            .expect("slice length verified above");
        let rx_key: [u8; KEYBYTES] = hr.rx;
        let decrypt = secretstream::Pull::new(&rx_key, &remote_header);

        debug!(is_initiator, "secret stream established");

        Ok(Self {
            raw,
            encrypt,
            decrypt,
            remote_public_key: hr.remote_public_key,
            handshake_hash: hr.handshake_hash,
            is_initiator,
        })
    }

    /// Create a SecretStream from pre-negotiated session keys (e.g. from Noise IK).
    ///
    /// Skips the Noise XX handshake but still performs the ID header exchange
    /// (stream identity + secretstream header). Used for relay connections and
    /// direct connections where Noise IK already established the session.
    pub async fn from_session(
        is_initiator: bool,
        mut raw: T,
        tx: [u8; 32],
        rx: [u8; 32],
        handshake_hash: [u8; 64],
        remote_public_key: [u8; 32],
    ) -> Result<Self, SecretStreamError> {
        let (encrypt, ss_header) = secretstream::Push::new(&tx);

        let local_id = stream_id(&handshake_hash, is_initiator);
        let mut id_msg = Vec::with_capacity(IDHEADERBYTES);
        id_msg.extend_from_slice(&local_id);
        id_msg.extend_from_slice(&ss_header);
        write_frame(&mut raw, &id_msg).await?;

        let remote_msg = read_frame(&mut raw)
            .await?
            .ok_or(SecretStreamError::UnexpectedEof)?;
        if remote_msg.len() != IDHEADERBYTES {
            return Err(SecretStreamError::InvalidIdHeader {
                expected: IDHEADERBYTES,
                got: remote_msg.len(),
            });
        }

        let remote_id = &remote_msg[..32];
        let expected_id = stream_id(&handshake_hash, !is_initiator);
        if remote_id != expected_id {
            return Err(SecretStreamError::StreamIdMismatch);
        }

        let remote_header: [u8; HEADERBYTES] = remote_msg[32..56]
            .try_into()
            // SAFETY: remote_msg.len() == IDHEADERBYTES (56) verified above; [32..56] is 24 bytes == HEADERBYTES.
            .expect("slice length verified above");
        let decrypt = secretstream::Pull::new(&rx, &remote_header);

        debug!(is_initiator, "secret stream established (from session)");

        Ok(Self {
            raw,
            encrypt,
            decrypt,
            remote_public_key,
            handshake_hash,
            is_initiator,
        })
    }

    /// Encrypt and send `data` as a single framed message.
    pub async fn write(&mut self, data: &[u8]) -> Result<(), SecretStreamError> {
        let encrypted = self.encrypt.next(data);
        // encrypted = [enc_tag(1)][ciphertext][mac(16)] = data.len() + ABYTES bytes
        debug_assert_eq!(encrypted.len(), data.len() + ABYTES);
        write_frame(&mut self.raw, &encrypted).await
    }

    /// Gracefully close the write half, sending a FIN to the remote peer.
    pub async fn shutdown(&mut self) -> Result<(), SecretStreamError> {
        use tokio::io::AsyncWriteExt;
        self.raw.shutdown().await.map_err(SecretStreamError::Io)
    }

    /// Read and decrypt the next framed message.
    ///
    /// Returns `Ok(None)` on clean EOF, `Ok(Some(plaintext))` for data messages.
    /// Empty messages (keepalives) are silently consumed and the next message
    /// is read.
    pub async fn read(&mut self) -> Result<Option<Vec<u8>>, SecretStreamError> {
        loop {
            let msg = match read_frame(&mut self.raw).await? {
                Some(m) => m,
                None => return Ok(None),
            };

            if msg.len() < ABYTES {
                return Err(SecretStreamError::Decrypt(
                    secretstream::SecretstreamError::CiphertextTooShort,
                ));
            }

            let (plaintext, _tag) = self.decrypt.next(&msg)?;

            // Drop empty keepalive messages (match Node.js behavior).
            if plaintext.is_empty() {
                continue;
            }

            return Ok(Some(plaintext));
        }
    }

    /// Remote peer's static Ed25519 public key.
    pub fn remote_public_key(&self) -> &[u8; 32] {
        &self.remote_public_key
    }

    /// BLAKE2b-512 hash of the Noise handshake transcript.
    pub fn handshake_hash(&self) -> &[u8; 64] {
        &self.handshake_hash
    }

    /// Whether this side initiated the connection.
    pub fn is_initiator(&self) -> bool {
        self.is_initiator
    }

    /// Consume the wrapper, returning the underlying transport.
    pub fn into_inner(self) -> T {
        self.raw
    }

    /// Split the stream into owned read and write halves.
    ///
    /// The underlying transport is split with [`tokio::io::split`]; the
    /// decryptor (`Pull`) moves into the [`SecretStreamReadHalf`] and the
    /// encryptor (`Push`) moves into the [`SecretStreamWriteHalf`]. Because the
    /// two halves own disjoint cipher state, they can be driven from separate
    /// tasks concurrently without either half cancelling the other's in-flight
    /// framed read/write. Wire framing and cipher behavior are identical to the
    /// unsplit [`SecretStream::read`]/[`SecretStream::write`].
    pub fn into_split(
        self,
    ) -> (
        SecretStreamReadHalf<ReadHalf<T>>,
        SecretStreamWriteHalf<WriteHalf<T>>,
    ) {
        let (read_raw, write_raw) = tokio::io::split(self.raw);
        (
            SecretStreamReadHalf {
                raw: read_raw,
                decrypt: self.decrypt,
            },
            SecretStreamWriteHalf {
                raw: write_raw,
                encrypt: self.encrypt,
            },
        )
    }
}

/// Owned read half of a [`SecretStream`], produced by [`SecretStream::into_split`].
///
/// Holds the decryptor and the read side of the underlying transport. Its
/// [`read`](SecretStreamReadHalf::read) mirrors [`SecretStream::read`] exactly
/// (same length-prefixed framing, same empty-keepalive handling, same AEAD
/// decryption).
pub struct SecretStreamReadHalf<R> {
    raw: R,
    decrypt: secretstream::Pull,
}

impl<R: AsyncRead + Unpin + Send> SecretStreamReadHalf<R> {
    /// Read and decrypt the next framed message.
    ///
    /// Returns `Ok(None)` on clean EOF, `Ok(Some(plaintext))` for data
    /// messages. Empty messages (keepalives) are silently consumed and the next
    /// message is read. Identical framing/cipher semantics to
    /// [`SecretStream::read`].
    pub async fn read(&mut self) -> Result<Option<Vec<u8>>, SecretStreamError> {
        loop {
            let msg = match read_frame(&mut self.raw).await? {
                Some(m) => m,
                None => return Ok(None),
            };

            if msg.len() < ABYTES {
                return Err(SecretStreamError::Decrypt(
                    secretstream::SecretstreamError::CiphertextTooShort,
                ));
            }

            let (plaintext, _tag) = self.decrypt.next(&msg)?;

            // Drop empty keepalive messages (match Node.js behavior).
            if plaintext.is_empty() {
                continue;
            }

            return Ok(Some(plaintext));
        }
    }
}

/// Owned write half of a [`SecretStream`], produced by [`SecretStream::into_split`].
///
/// Holds the encryptor and the write side of the underlying transport. Its
/// [`write`](SecretStreamWriteHalf::write) mirrors [`SecretStream::write`]
/// exactly.
pub struct SecretStreamWriteHalf<W> {
    raw: W,
    encrypt: secretstream::Push,
}

impl<W: AsyncWrite + Unpin + Send> SecretStreamWriteHalf<W> {
    /// Encrypt and send `data` as a single framed message.
    ///
    /// Identical framing/cipher semantics to [`SecretStream::write`].
    pub async fn write(&mut self, data: &[u8]) -> Result<(), SecretStreamError> {
        let encrypted = self.encrypt.next(data);
        // encrypted = [enc_tag(1)][ciphertext][mac(16)] = data.len() + ABYTES bytes
        debug_assert_eq!(encrypted.len(), data.len() + ABYTES);
        write_frame(&mut self.raw, &encrypted).await
    }

    /// Gracefully close the write half, sending a FIN to the remote peer.
    ///
    /// Identical to [`SecretStream::shutdown`].
    pub async fn shutdown(&mut self) -> Result<(), SecretStreamError> {
        self.raw.shutdown().await.map_err(SecretStreamError::Io)
    }
}

// ── FramedStream adapter ─────────────────────────────────────────────────────

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> crate::protomux::FramedStream
    for SecretStream<T>
{
    async fn read_frame(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.read().await.map_err(|e| match e {
            SecretStreamError::Io(io_err) => io_err,
            other => std::io::Error::other(other.to_string()),
        })
    }

    async fn write_frame(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.write(data).await.map_err(|e| match e {
            SecretStreamError::Io(io_err) => io_err,
            other => std::io::Error::other(other.to_string()),
        })
    }
}

// ── Framing helpers ──────────────────────────────────────────────────────────

/// Write a length-prefixed frame: `[3B LE uint24 length][body]`.
async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    data: &[u8],
) -> Result<(), SecretStreamError> {
    let len = data.len();
    if len > MAX_MESSAGE_LEN {
        return Err(SecretStreamError::MessageTooLarge { len });
    }
    let header = [len as u8, (len >> 8) as u8, (len >> 16) as u8];
    w.write_all(&header).await?;
    w.write_all(data).await?;
    w.flush().await?;
    Ok(())
}

/// Read a length-prefixed frame.
///
/// Returns `Ok(None)` on clean EOF (zero bytes read for the length header).
/// Returns `Err(UnexpectedEof)` if EOF occurs mid-message.
async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<Option<Vec<u8>>, SecretStreamError> {
    let mut header = [0u8; 3];
    match r.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(SecretStreamError::Io(e)),
    }

    let len = header[0] as usize | (header[1] as usize) << 8 | (header[2] as usize) << 16;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)
        .await
        .map_err(|_| SecretStreamError::UnexpectedEof)?;

    Ok(Some(buf))
}

// ── Stream ID derivation ─────────────────────────────────────────────────────

/// Compute the 32-byte stream identity for one side of the connection.
///
/// Equivalent to Node.js:
/// ```js
/// sodium.crypto_generichash(out, isInitiator ? NS_INITIATOR : NS_RESPONDER, handshakeHash)
/// ```
///
/// This is a keyed BLAKE2b-256: data = namespace constant, key = handshake hash.
fn stream_id(handshake_hash: &[u8; 64], is_initiator: bool) -> [u8; 32] {
    let ns = if is_initiator {
        ns_initiator()
    } else {
        ns_responder()
    };
    let mut mac: Blake2bMac256 =
        // SAFETY: BLAKE2b accepts keys from 1..=64 bytes; handshake_hash is always 64 bytes.
        KeyInit::new_from_slice(handshake_hash).expect("64-byte key is valid for BLAKE2b");
    mac.update(ns);
    let output = mac.finalize().into_bytes();
    let mut result = [0u8; 32];
    result.copy_from_slice(&output);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_constants_are_distinct() {
        let ns = &*NS_SECRET_STREAM;
        assert_ne!(ns[0], ns[1]);
        assert_ne!(ns[1], ns[2]);
        assert_ne!(ns[0], ns[2]);
    }

    #[test]
    fn stream_id_differs_by_role() {
        let hash = [0xABu8; 64];
        let id_i = stream_id(&hash, true);
        let id_r = stream_id(&hash, false);
        assert_ne!(id_i, id_r);
    }

    #[test]
    fn frame_roundtrip() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let data = b"hello framing";
            let mut buf = Vec::new();
            write_frame(&mut buf, data).await.unwrap();

            assert_eq!(buf.len(), 3 + data.len());
            assert_eq!(buf[0], data.len() as u8);
            assert_eq!(buf[1], 0);
            assert_eq!(buf[2], 0);

            let mut cursor = std::io::Cursor::new(buf);
            let result = read_frame(&mut cursor).await.unwrap();
            assert_eq!(result.as_deref(), Some(data.as_slice()));
        });
    }

    #[test]
    fn frame_eof_returns_none() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
            let result = read_frame(&mut cursor).await.unwrap();
            assert!(result.is_none());
        });
    }

    #[tokio::test]
    async fn handshake_and_exchange() {
        // Full in-process test using duplex streams (tokio::io::duplex).
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let kp_a = noise::generate_keypair();
        let kp_b = noise::generate_keypair();

        let (client_result, server_result) = tokio::join!(
            SecretStream::new(true, client_stream, kp_a),
            SecretStream::new(false, server_stream, kp_b),
        );

        let mut client = client_result.expect("client handshake failed");
        let mut server = server_result.expect("server handshake failed");

        // Send from client → server
        client.write(b"hello from client").await.unwrap();
        let msg = server.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"hello from client");

        // Send from server → client
        server.write(b"hello from server").await.unwrap();
        let msg = client.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"hello from server");

        // Multiple messages
        for i in 0..10 {
            let payload = format!("message {i}");
            client.write(payload.as_bytes()).await.unwrap();
            let msg = server.read().await.unwrap().expect("expected message");
            assert_eq!(msg, payload.as_bytes());
        }
    }

    #[tokio::test]
    async fn empty_message_roundtrip() {
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let (mut client, mut server) = tokio::try_join!(
            SecretStream::new(true, client_stream, noise::generate_keypair()),
            SecretStream::new(false, server_stream, noise::generate_keypair()),
        )
        .unwrap();

        // Empty messages are consumed as keepalives by read().
        // Send a real message after an empty one.
        client.write(b"").await.unwrap();
        client.write(b"after empty").await.unwrap();
        let msg = server.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"after empty");
    }

    #[tokio::test]
    async fn remote_public_key_matches() {
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let kp_a = noise::generate_keypair();
        let kp_b = noise::generate_keypair();
        let pk_a = kp_a.public_key;
        let pk_b = kp_b.public_key;

        let (client, server) = tokio::try_join!(
            SecretStream::new(true, client_stream, kp_a),
            SecretStream::new(false, server_stream, kp_b),
        )
        .unwrap();

        assert_eq!(*client.remote_public_key(), pk_b);
        assert_eq!(*server.remote_public_key(), pk_a);
        assert_eq!(client.handshake_hash(), server.handshake_hash());
    }

    #[tokio::test]
    async fn from_session_roundtrip() {
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let tx_key = [0x11u8; 32];
        let rx_key = [0x22u8; 32];
        let hash = [0xABu8; 64];
        let remote_pk = [0xCCu8; 32];

        let (client_result, server_result) = tokio::join!(
            SecretStream::from_session(true, client_stream, tx_key, rx_key, hash, remote_pk),
            SecretStream::from_session(false, server_stream, rx_key, tx_key, hash, remote_pk),
        );

        let mut client = client_result.expect("client from_session failed");
        let mut server = server_result.expect("server from_session failed");

        client.write(b"pre-keyed hello").await.unwrap();
        let msg = server.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"pre-keyed hello");

        server.write(b"pre-keyed reply").await.unwrap();
        let msg = client.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"pre-keyed reply");

        assert!(client.is_initiator());
        assert!(!server.is_initiator());
    }

    #[tokio::test]
    async fn into_split_roundtrip() {
        // Split both peers into read/write halves and exchange in both
        // directions concurrently. Proves the split preserves wire framing and
        // AEAD cipher behavior identically to the unsplit read()/write().
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let (client, server) = tokio::try_join!(
            SecretStream::new(true, client_stream, noise::generate_keypair()),
            SecretStream::new(false, server_stream, noise::generate_keypair()),
        )
        .unwrap();

        let (mut client_rd, mut client_wr) = client.into_split();
        let (mut server_rd, mut server_wr) = server.into_split();

        // client → server
        client_wr.write(b"hello from client").await.unwrap();
        let msg = server_rd.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"hello from client");

        // server → client
        server_wr.write(b"hello from server").await.unwrap();
        let msg = client_rd.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"hello from server");

        // Concurrent bidirectional bulk: writer and reader in separate tasks,
        // read_exact is never cancelled — the exact property the split provides.
        let writer = tokio::spawn(async move {
            for i in 0..100 {
                let payload = format!("bulk {i}");
                client_wr.write(payload.as_bytes()).await.unwrap();
            }
            client_wr.shutdown().await.unwrap();
        });
        let reader = tokio::spawn(async move {
            let mut count = 0;
            while let Some(msg) = server_rd.read().await.unwrap() {
                assert_eq!(msg, format!("bulk {count}").as_bytes());
                count += 1;
            }
            count
        });
        writer.await.unwrap();
        assert_eq!(reader.await.unwrap(), 100);
    }

    #[tokio::test]
    async fn into_split_empty_keepalive() {
        // Empty messages are consumed as keepalives by the split read half,
        // matching SecretStream::read.
        let (client_stream, server_stream) = tokio::io::duplex(8192);

        let (client, server) = tokio::try_join!(
            SecretStream::new(true, client_stream, noise::generate_keypair()),
            SecretStream::new(false, server_stream, noise::generate_keypair()),
        )
        .unwrap();

        let (_client_rd, mut client_wr) = client.into_split();
        let (mut server_rd, _server_wr) = server.into_split();

        client_wr.write(b"").await.unwrap();
        client_wr.write(b"after empty").await.unwrap();
        let msg = server_rd.read().await.unwrap().expect("expected message");
        assert_eq!(msg, b"after empty");
    }
}
