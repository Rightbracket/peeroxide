//! Pure-Rust implementation of libsodium's `crypto_secretstream_xchacha20poly1305`.
//!
//! Uses manual ChaCha20 + Poly1305 construction matching libsodium's internal
//! layout exactly:
//! - Counter=0: generate 64-byte block → first 32 bytes = Poly1305 key
//! - Counter=1: encrypt 64-byte tag block (only byte 0 holds the tag)
//! - Counter=2+: encrypt message bytes
//!
//! MAC input (matching libsodium's known-quirky construction):
//!   aad || pad16(aad) || encrypted_block\[64\] || encrypted_msg || pad_msg || le64(adlen) || le64(64+mlen)
//! where pad_msg = (mlen & 0xf) bytes of zeros (NOT standard AEAD alignment).

use chacha20::cipher::{KeyIvInit, StreamCipher};
use chacha20::hchacha;
use poly1305::universal_hash::KeyInit;
use poly1305::Poly1305;
use rand::{rngs::OsRng, TryRngCore};
use thiserror::Error;
use tracing::debug;

pub const KEYBYTES: usize = 32;
pub const HEADERBYTES: usize = 24;
/// 1 encrypted tag byte + 16-byte Poly1305 MAC.
pub const ABYTES: usize = 17;

pub const TAG_MESSAGE: u8 = 0x00;
pub const TAG_PUSH: u8 = 0x01;
pub const TAG_REKEY: u8 = 0x02;
pub const TAG_FINAL: u8 = 0x03;

/// Zero pad used for explicit Poly1305 padding (max 16 bytes needed).
static PAD0: [u8; 16] = [0u8; 16];

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SecretstreamError {
    #[error("ciphertext too short")]
    CiphertextTooShort,
    #[error("decryption failed")]
    DecryptionFailed,
}

fn hchacha20_subkey(key: &[u8; KEYBYTES], input: &[u8; 16]) -> [u8; KEYBYTES] {
    use chacha20::cipher::consts::{U10, U16, U32};
    use chacha20::cipher::generic_array::GenericArray;

    let out: GenericArray<u8, U32> = hchacha::<U10>(
        GenericArray::<u8, U32>::from_slice(key),
        GenericArray::<u8, U16>::from_slice(input),
    );
    let mut subkey = [0u8; 32];
    subkey.copy_from_slice(out.as_slice());
    subkey
}

fn evolve_nonce(nonce: &mut [u8; 12], mac: &[u8; 16]) {
    // XOR first 8 bytes of MAC into inonce (nonce[4..12])
    for i in 0..8 {
        nonce[4 + i] ^= mac[i];
    }
    // Increment counter (nonce[0..4]) as LE u32
    let counter = u32::from_le_bytes([nonce[0], nonce[1], nonce[2], nonce[3]]);
    nonce[0..4].copy_from_slice(&counter.wrapping_add(1).to_le_bytes());
}

/// Rekey: XOR [key || inonce] with keystream, then reset counter to 1.
///
/// Matches libsodium's `crypto_secretstream_xchacha20poly1305_rekey` exactly:
/// the input to the XOR is the concatenation of key and inonce, NOT zeros.
fn rekey_state(key: &mut [u8; KEYBYTES], nonce: &mut [u8; 12]) {
    let mut buf = [0u8; 40];
    buf[..32].copy_from_slice(key);
    buf[32..40].copy_from_slice(&nonce[4..12]);
    chacha20::ChaCha20::new(key.as_slice().into(), nonce.as_slice().into())
        .apply_keystream(&mut buf);
    key.copy_from_slice(&buf[..32]);
    nonce[4..12].copy_from_slice(&buf[32..40]);
    // Reset counter to 1 (libsodium's _counter_reset)
    nonce[0..4].copy_from_slice(&1u32.to_le_bytes());
}

/// Build the Poly1305 MAC input matching libsodium's secretstream construction.
///
/// Layout:
///   aad || pad_aad || encrypted_block[64] || encrypted_msg[mlen] || pad_msg || le64(adlen) || le64(64+mlen)
///
/// Padding formulas (from libsodium, including its known quirk):
///   pad_aad = (16 - adlen) & 0xf
///   pad_msg = (16 - 64 + mlen) & 0xf  =  mlen & 0xf
fn compute_mac(
    poly_key: &[u8; 32],
    aad: &[u8],
    tag_block: &[u8; 64],
    encrypted_msg: &[u8],
) -> [u8; 16] {
    let adlen = aad.len();
    let mlen = encrypted_msg.len();
    let pad_aad = (16usize.wrapping_sub(adlen)) & 0xf;
    let pad_msg = mlen & 0xf; // libsodium's quirky formula: (0x10 - sizeof(block) + mlen) & 0xf

    // Build the full MAC input with explicit padding
    let total = adlen + pad_aad + 64 + mlen + pad_msg + 16;
    let mut mac_input = Vec::with_capacity(total);
    mac_input.extend_from_slice(aad);
    mac_input.extend_from_slice(&PAD0[..pad_aad]);
    mac_input.extend_from_slice(tag_block);
    mac_input.extend_from_slice(encrypted_msg);
    mac_input.extend_from_slice(&PAD0[..pad_msg]);
    mac_input.extend_from_slice(&(adlen as u64).to_le_bytes());
    mac_input.extend_from_slice(&((64 + mlen) as u64).to_le_bytes());

    let mac = Poly1305::new(poly_key.into());
    let result = mac.compute_unpadded(&mac_input);
    let mut out = [0u8; 16];
    out.copy_from_slice(result.as_slice());
    out
}

/// Encrypt: manual ChaCha20 + Poly1305 matching libsodium secretstream.
///
/// Counter=0 → Poly1305 key (first 32 of 64-byte keystream).
/// Counter=1 → XOR 64-byte block (byte 0 = tag, rest = 0) → full block used for MAC.
/// Counter=2 → XOR message bytes.
/// Output: encrypted_tag[1] || encrypted_msg[mlen] || mac[16].
fn secretstream_encrypt(
    key: &[u8; KEYBYTES],
    nonce: &[u8; 12],
    aad: &[u8],
    tag: u8,
    message: &[u8],
) -> Vec<u8> {
    let mut cipher = chacha20::ChaCha20::new(key.as_slice().into(), nonce.as_slice().into());

    // Counter=0: 64 bytes of keystream → poly key from first 32
    let mut block0 = [0u8; 64];
    cipher.apply_keystream(&mut block0);
    let mut poly_key = [0u8; 32];
    poly_key.copy_from_slice(&block0[..32]);

    // Counter=1: encrypt full 64-byte tag block
    let mut tag_block = [0u8; 64];
    tag_block[0] = tag;
    cipher.apply_keystream(&mut tag_block);
    // tag_block is now the full encrypted block; tag_block[0] = encrypted tag
    let encrypted_tag = tag_block[0];

    // Counter=2+: encrypt message
    let mut encrypted_msg = message.to_vec();
    cipher.apply_keystream(&mut encrypted_msg);

    // Compute MAC over full 64-byte encrypted block + encrypted message
    let mac = compute_mac(&poly_key, aad, &tag_block, &encrypted_msg);

    // Output: enc_tag || enc_msg || mac
    let mut output = Vec::with_capacity(1 + message.len() + 16);
    output.push(encrypted_tag);
    output.extend_from_slice(&encrypted_msg);
    output.extend_from_slice(&mac);
    output
}

/// Decrypt: verify MAC then decrypt. Returns (plaintext, tag).
///
/// Reconstructs the 64-byte MAC block from the encrypted tag byte:
/// block = [enc_tag, 0, 0, ...] XOR'd with counter=1 keystream to get tag,
/// then block[0] restored to enc_tag for MAC verification (matching libsodium).
fn secretstream_decrypt(
    key: &[u8; KEYBYTES],
    nonce: &[u8; 12],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<(Vec<u8>, u8), SecretstreamError> {
    if ciphertext.len() < ABYTES {
        return Err(SecretstreamError::CiphertextTooShort);
    }

    let mac_start = ciphertext.len() - 16;
    let received_mac = &ciphertext[mac_start..];
    let encrypted_tag = ciphertext[0];
    let encrypted_msg = &ciphertext[1..mac_start];

    let mut cipher = chacha20::ChaCha20::new(key.as_slice().into(), nonce.as_slice().into());

    // Counter=0: poly key
    let mut block0 = [0u8; 64];
    cipher.apply_keystream(&mut block0);
    let mut poly_key = [0u8; 32];
    poly_key.copy_from_slice(&block0[..32]);

    // Counter=1: reconstruct the 64-byte MAC block and extract tag
    // Start with [enc_tag, 0, 0, ...], XOR with counter=1 keystream
    let mut tag_block = [0u8; 64];
    tag_block[0] = encrypted_tag;
    cipher.apply_keystream(&mut tag_block);
    let tag = tag_block[0]; // decrypted tag
    // Restore enc_tag for MAC (block = [enc_tag, ks1[1], ..., ks1[63]])
    tag_block[0] = encrypted_tag;

    // Verify MAC
    let computed_mac = compute_mac(&poly_key, aad, &tag_block, encrypted_msg);
    if computed_mac != received_mac {
        return Err(SecretstreamError::DecryptionFailed);
    }

    // Counter=2: decrypt message
    let mut plaintext = encrypted_msg.to_vec();
    cipher.apply_keystream(&mut plaintext);

    Ok((plaintext, tag))
}

fn init_from_header(key: &[u8; KEYBYTES], header: &[u8; HEADERBYTES]) -> ([u8; 32], [u8; 12]) {
    let mut input = [0u8; 16];
    input.copy_from_slice(&header[..16]);
    let subkey = hchacha20_subkey(key, &input);

    let mut nonce = [0u8; 12];
    nonce[0] = 1;
    nonce[4..12].copy_from_slice(&header[16..24]);

    (subkey, nonce)
}

fn should_rekey(tag: u8, nonce: &[u8; 12]) -> bool {
    let rekey_bit = tag & TAG_REKEY != 0;
    let counter_zero = nonce[0..4] == [0, 0, 0, 0];
    rekey_bit || counter_zero
}

pub struct Push {
    key: [u8; KEYBYTES],
    nonce: [u8; 12],
}

impl Push {
    pub fn new(key: &[u8; KEYBYTES]) -> (Self, [u8; HEADERBYTES]) {
        let mut header = [0u8; HEADERBYTES];
        // SAFETY: OsRng is the OS CSPRNG; failure indicates a fatal platform issue.
        OsRng.try_fill_bytes(&mut header).expect("OsRng should not fail");
        let state = Self::with_header(key, &header);
        (state, header)
    }

    pub fn with_header(key: &[u8; KEYBYTES], header: &[u8; HEADERBYTES]) -> Self {
        let (subkey, nonce) = init_from_header(key, header);
        debug!("Push::with_header – subkey derived");
        Self { key: subkey, nonce }
    }

    pub fn next(&mut self, plaintext: &[u8]) -> Vec<u8> {
        self.push(plaintext, None, TAG_MESSAGE)
    }

    pub fn finalize(&mut self, plaintext: &[u8]) -> Vec<u8> {
        self.push(plaintext, None, TAG_FINAL)
    }

    pub fn push(&mut self, plaintext: &[u8], ad: Option<&[u8]>, tag: u8) -> Vec<u8> {
        let aad = ad.unwrap_or(&[]);
        let ciphertext = secretstream_encrypt(&self.key, &self.nonce, aad, tag, plaintext);

        let mac_start = ciphertext.len() - 16;
        let mut mac = [0u8; 16];
        mac.copy_from_slice(&ciphertext[mac_start..]);

        evolve_nonce(&mut self.nonce, &mac);

        if should_rekey(tag, &self.nonce) {
            rekey_state(&mut self.key, &mut self.nonce);
        }

        debug!(tag, plaintext_len = plaintext.len(), ciphertext_len = ciphertext.len(), "Push::push");
        ciphertext
    }
}

pub struct Pull {
    key: [u8; KEYBYTES],
    nonce: [u8; 12],
}

impl Pull {
    pub fn new(key: &[u8; KEYBYTES], header: &[u8; HEADERBYTES]) -> Self {
        let (subkey, nonce) = init_from_header(key, header);
        debug!("Pull::new – subkey derived");
        Self { key: subkey, nonce }
    }

    pub fn next(&mut self, ciphertext: &[u8]) -> Result<(Vec<u8>, u8), SecretstreamError> {
        self.pull(ciphertext, None)
    }

    pub fn pull(
        &mut self,
        ciphertext: &[u8],
        ad: Option<&[u8]>,
    ) -> Result<(Vec<u8>, u8), SecretstreamError> {
        if ciphertext.len() < ABYTES {
            return Err(SecretstreamError::CiphertextTooShort);
        }

        let aad = ad.unwrap_or(&[]);

        let mac_start = ciphertext.len() - 16;
        let mut mac = [0u8; 16];
        mac.copy_from_slice(&ciphertext[mac_start..]);

        let (message, tag) = secretstream_decrypt(&self.key, &self.nonce, aad, ciphertext)?;

        evolve_nonce(&mut self.nonce, &mac);

        if should_rekey(tag, &self.nonce) {
            rekey_state(&mut self.key, &mut self.nonce);
        }

        debug!(tag, message_len = message.len(), "Pull::pull");
        Ok((message, tag))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key() -> [u8; KEYBYTES] {
        [0x42u8; KEYBYTES]
    }

    #[test]
    fn push_pull_roundtrip() {
        let key = make_key();
        let (mut push, header) = Push::new(&key);
        let mut pull = Pull::new(&key, &header);

        let ct1 = push.next(b"hello");
        let ct2 = push.next(b"world");
        let ct3 = push.next(b"");
        let ct4 = push.finalize(b"last");

        let (msg1, tag1) = pull.next(&ct1).unwrap();
        let (msg2, tag2) = pull.next(&ct2).unwrap();
        let (msg3, tag3) = pull.next(&ct3).unwrap();
        let (msg4, tag4) = pull.next(&ct4).unwrap();

        assert_eq!(msg1, b"hello");
        assert_eq!(msg2, b"world");
        assert_eq!(msg3, b"");
        assert_eq!(msg4, b"last");

        assert_eq!(tag1, TAG_MESSAGE);
        assert_eq!(tag2, TAG_MESSAGE);
        assert_eq!(tag3, TAG_MESSAGE);
        assert_eq!(tag4, TAG_FINAL);
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = make_key();
        let (mut push, header) = Push::new(&key);
        let mut pull = Pull::new(&key, &header);

        let mut ct = push.next(b"secret");
        let mid = ct.len() / 2;
        ct[mid] ^= 0xFF;

        let result = pull.next(&ct);
        assert!(result.is_err(), "tampered ciphertext should fail");
    }

    #[test]
    fn empty_plaintext_size() {
        let key = make_key();
        let (mut push, _header) = Push::new(&key);
        let ct = push.next(b"");
        assert_eq!(ct.len(), ABYTES);
    }

    #[test]
    fn tag_final_detected() {
        let key = make_key();
        let (mut push, header) = Push::new(&key);
        let mut pull = Pull::new(&key, &header);

        let ct = push.finalize(b"bye");
        let (_msg, tag) = pull.next(&ct).unwrap();
        assert_eq!(tag, TAG_FINAL);
    }
}
