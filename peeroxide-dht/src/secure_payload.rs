//! Authenticated-encryption helper for short DHT-handshake payloads.
//!
//! `SecurePayload` wraps an XChaCha20-Poly1305 secretbox keyed off a
//! BLAKE2b namespace + remote-secret pairing, and is used by the swarm
//! and connect-handshake paths to attach encrypted application data
//! (peer addresses, holepunch instructions) to otherwise plaintext
//! `PEER_HANDSHAKE` / `PEER_HOLEPUNCH` messages.
//!
//! Errors are reported through `SecurePayloadError`. The encrypted output
//! also carries a short opaque token (see `SecurePayload::token`) that
//! callers use to bind a reply to the originating request.

#![allow(missing_docs)]

use blake2::digest::consts::U32;
use blake2::digest::{KeyInit, Mac};
use blake2::Blake2bMac;
use rand::RngCore;
use xsalsa20poly1305::aead::AeadInPlace;
use xsalsa20poly1305::{Nonce, Tag, XSalsa20Poly1305};

use crate::compact_encoding::EncodingError;
use crate::hyperdht_messages::{self, HolepunchPayload};

type Blake2bMac256 = Blake2bMac<U32>;

const NONCE_SIZE: usize = 24;
const TAG_SIZE: usize = 16;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SecurePayloadError {
    #[error("encoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("encryption failed")]
    EncryptFailed,
    #[error("decryption failed")]
    DecryptFailed,
    #[error("buffer too short for encrypted payload")]
    BufferTooShort,
}

pub struct SecurePayload {
    shared_secret: [u8; 32],
    local_secret: [u8; 32],
}

impl SecurePayload {
    pub fn new(holepunch_secret: [u8; 32]) -> Self {
        let mut local_secret = [0u8; 32];
        rand::rng().fill_bytes(&mut local_secret);
        Self {
            shared_secret: holepunch_secret,
            local_secret,
        }
    }

    pub fn with_local_secret(holepunch_secret: [u8; 32], local_secret: [u8; 32]) -> Self {
        Self {
            shared_secret: holepunch_secret,
            local_secret,
        }
    }

    /// Encrypt a HolepunchPayload.
    ///
    /// Wire format: `nonce(24) || tag(16) || ciphertext(n)`
    /// Compatible with libsodium's `crypto_secretbox_easy`.
    pub fn encrypt(&self, payload: &HolepunchPayload) -> Result<Vec<u8>, SecurePayloadError> {
        let encoded = hyperdht_messages::encode_holepunch_payload_to_bytes(payload)
            .map_err(SecurePayloadError::Encoding)?;

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        let cipher = XSalsa20Poly1305::new(&self.shared_secret.into());
        let mut ciphertext = encoded;
        let tag = cipher
            .encrypt_in_place_detached(&nonce, b"", &mut ciphertext)
            .map_err(|_| SecurePayloadError::EncryptFailed)?;

        let mut result = Vec::with_capacity(NONCE_SIZE + TAG_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(tag.as_slice());
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    pub fn encrypt_with_nonce(
        &self,
        payload: &HolepunchPayload,
        nonce_bytes: [u8; NONCE_SIZE],
    ) -> Result<Vec<u8>, SecurePayloadError> {
        let encoded = hyperdht_messages::encode_holepunch_payload_to_bytes(payload)
            .map_err(SecurePayloadError::Encoding)?;

        let nonce = Nonce::from(nonce_bytes);
        let cipher = XSalsa20Poly1305::new(&self.shared_secret.into());
        let mut ciphertext = encoded;
        let tag = cipher
            .encrypt_in_place_detached(&nonce, b"", &mut ciphertext)
            .map_err(|_| SecurePayloadError::EncryptFailed)?;

        let mut result = Vec::with_capacity(NONCE_SIZE + TAG_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(tag.as_slice());
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Decrypt an encrypted payload buffer.
    ///
    /// Expected wire format: `nonce(24) || tag(16) || ciphertext(n)`
    pub fn decrypt(&self, buffer: &[u8]) -> Result<HolepunchPayload, SecurePayloadError> {
        let min_len = NONCE_SIZE + TAG_SIZE + 1;
        if buffer.len() < min_len {
            return Err(SecurePayloadError::BufferTooShort);
        }

        let nonce = Nonce::from_slice(&buffer[..NONCE_SIZE]);
        let tag = Tag::from_slice(&buffer[NONCE_SIZE..NONCE_SIZE + TAG_SIZE]);
        let mut plaintext = buffer[NONCE_SIZE + TAG_SIZE..].to_vec();

        let cipher = XSalsa20Poly1305::new(&self.shared_secret.into());
        cipher
            .decrypt_in_place_detached(nonce, b"", &mut plaintext, tag)
            .map_err(|_| SecurePayloadError::DecryptFailed)?;

        hyperdht_messages::decode_holepunch_payload_from_bytes(&plaintext)
            .map_err(SecurePayloadError::Encoding)
    }

    /// Generate a 32-byte token for a peer address using keyed BLAKE2b.
    /// Mirrors `sodium.crypto_generichash(out, Buffer.from(addr.host), localSecret)`.
    pub fn token(&self, host: &str) -> [u8; 32] {
        let mut mac: Blake2bMac256 =
            // SAFETY: BLAKE2b accepts keys from 1..=64 bytes; local_secret is always 32 bytes.
            KeyInit::new_from_slice(&self.local_secret).expect("32-byte key valid for BLAKE2b");
        mac.update(host.as_bytes());
        let output = mac.finalize().into_bytes();
        let mut result = [0u8; 32];
        result.copy_from_slice(&output);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Ipv4Peer;

    fn make_payload() -> HolepunchPayload {
        HolepunchPayload {
            error: 0,
            firewall: 1,
            round: 3,
            connected: false,
            punching: true,
            addresses: Some(vec![Ipv4Peer {
                host: "1.2.3.4".into(),
                port: 1000,
            }]),
            remote_address: None,
            token: None,
            remote_token: None,
        }
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let secret = [0xaa; 32];
        let sp = SecurePayload::new(secret);
        let payload = make_payload();

        let encrypted = sp.encrypt(&payload).unwrap();
        assert!(encrypted.len() > NONCE_SIZE + TAG_SIZE);

        let decrypted = sp.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, payload);
    }

    #[test]
    fn decrypt_wrong_key_fails() {
        let sp1 = SecurePayload::new([0xaa; 32]);
        let sp2 = SecurePayload::new([0xbb; 32]);

        let encrypted = sp1.encrypt(&make_payload()).unwrap();
        assert!(sp2.decrypt(&encrypted).is_err());
    }

    #[test]
    fn decrypt_truncated_fails() {
        let sp = SecurePayload::new([0xaa; 32]);
        assert!(sp.decrypt(&[0u8; 40]).is_err());
    }

    #[test]
    fn decrypt_corrupted_ciphertext_fails() {
        let sp = SecurePayload::new([0xaa; 32]);
        let mut encrypted = sp.encrypt(&make_payload()).unwrap();
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0xff;
        assert!(sp.decrypt(&encrypted).is_err());
    }

    #[test]
    fn decrypt_corrupted_tag_fails() {
        let sp = SecurePayload::new([0xaa; 32]);
        let mut encrypted = sp.encrypt(&make_payload()).unwrap();
        encrypted[NONCE_SIZE] ^= 0xff;
        assert!(sp.decrypt(&encrypted).is_err());
    }

    #[test]
    fn deterministic_encrypt_with_fixed_nonce() {
        let secret = [0x42; 32];
        let sp = SecurePayload::new(secret);
        let payload = make_payload();
        let nonce = [0x01; NONCE_SIZE];

        let enc1 = sp.encrypt_with_nonce(&payload, nonce).unwrap();
        let enc2 = sp.encrypt_with_nonce(&payload, nonce).unwrap();
        assert_eq!(enc1, enc2);

        let decrypted = sp.decrypt(&enc1).unwrap();
        assert_eq!(decrypted, payload);
    }

    #[test]
    fn token_deterministic() {
        let sp = SecurePayload::with_local_secret([0xaa; 32], [0xbb; 32]);
        let t1 = sp.token("1.2.3.4");
        let t2 = sp.token("1.2.3.4");
        assert_eq!(t1, t2);
    }

    #[test]
    fn token_different_hosts() {
        let sp = SecurePayload::with_local_secret([0xaa; 32], [0xbb; 32]);
        let t1 = sp.token("1.2.3.4");
        let t2 = sp.token("5.6.7.8");
        assert_ne!(t1, t2);
    }

    #[test]
    fn token_different_secrets() {
        let sp1 = SecurePayload::with_local_secret([0xaa; 32], [0xbb; 32]);
        let sp2 = SecurePayload::with_local_secret([0xaa; 32], [0xcc; 32]);
        assert_ne!(sp1.token("1.2.3.4"), sp2.token("1.2.3.4"));
    }

    #[test]
    fn encrypt_minimal_payload() {
        let sp = SecurePayload::new([0xaa; 32]);
        let payload = HolepunchPayload {
            error: 0,
            firewall: 0,
            round: 0,
            connected: false,
            punching: false,
            addresses: None,
            remote_address: None,
            token: None,
            remote_token: None,
        };
        let encrypted = sp.encrypt(&payload).unwrap();
        let decrypted = sp.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, payload);
    }

    #[test]
    fn encrypt_all_fields_payload() {
        let sp = SecurePayload::new([0xaa; 32]);
        let payload = HolepunchPayload {
            error: 1,
            firewall: 2,
            round: 10,
            connected: true,
            punching: true,
            addresses: Some(vec![
                Ipv4Peer {
                    host: "192.168.1.1".into(),
                    port: 3000,
                },
                Ipv4Peer {
                    host: "10.0.0.1".into(),
                    port: 8080,
                },
            ]),
            remote_address: Some(Ipv4Peer {
                host: "5.6.7.8".into(),
                port: 4000,
            }),
            token: Some([0xcc; 32]),
            remote_token: Some([0xdd; 32]),
        };
        let encrypted = sp.encrypt(&payload).unwrap();
        let decrypted = sp.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, payload);
    }
}
