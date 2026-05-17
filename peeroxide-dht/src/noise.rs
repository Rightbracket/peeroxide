//! Noise XX handshake — byte-compatible with `noise-handshake` + `noise-curve-ed`.
//!
//! Protocol string: `Noise_XX_Ed25519_ChaChaPoly_BLAKE2b`
//!
//! Reference implementations:
//! - `noise-handshake/noise.js`        — XX state machine
//! - `noise-handshake/symmetric-state.js` — SymmetricState
//! - `noise-handshake/cipher.js`       — CipherState (ChaCha20-Poly1305 IETF)
//! - `noise-handshake/hkdf.js`         — HKDF-BLAKE2b
//! - `noise-handshake/hmac.js`         — HMAC-BLAKE2b (standard ipad/opad)
//! - `noise-curve-ed/index.js`         — Ed25519 DH via SHA-512 scalar extraction

use blake2::digest::consts::U64;
use blake2::{Blake2b, Digest};
use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    ChaCha20Poly1305, Nonce,
};
use chacha20poly1305::aead::rand_core::RngCore;
use curve25519_dalek::{edwards::CompressedEdwardsY, Scalar};
use ed25519_dalek::SigningKey;
use sha2::Sha512;

type Blake2b512 = Blake2b<U64>;

/// `Noise_XX_Ed25519_ChaChaPoly_BLAKE2b` — 35 bytes, fits within HASHLEN=64.
const PROTOCOL_NAME: &[u8] = b"Noise_XX_Ed25519_ChaChaPoly_BLAKE2b";

/// `Noise_IK_Ed25519_ChaChaPoly_BLAKE2b` — 35 bytes, fits within HASHLEN=64.
const PROTOCOL_NAME_IK: &[u8] = b"Noise_IK_Ed25519_ChaChaPoly_BLAKE2b";

// ─── Error type ──────────────────────────────────────────────────────────────

/// Errors that can occur during a Noise protocol handshake.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NoiseError {
    /// The provided public key could not be decompressed to a valid curve point.
    #[error("invalid public key")]
    InvalidPublicKey,
    /// AEAD decryption or authentication failed.
    #[error("decryption failed")]
    DecryptionFailed,
    /// A send or receive was attempted after the handshake already completed.
    #[error("handshake already complete")]
    HandshakeComplete,
    /// The handshake state machine received a message out of order.
    #[error("unexpected handshake state")]
    UnexpectedState,
}

// ─── Primitive cryptography ───────────────────────────────────────────────────

/// BLAKE2b-512 (64-byte) hash of `data`.
fn blake2b_512(data: &[u8]) -> [u8; 64] {
    let output = Blake2b512::digest(data);
    let mut result = [0u8; 64];
    result.copy_from_slice(&output);
    result
}

/// BLAKE2b-512 over the concatenation of `parts`.
fn blake2b_512_multi(parts: &[&[u8]]) -> [u8; 64] {
    let mut h = Blake2b512::new();
    for part in parts {
        Digest::update(&mut h, part);
    }
    let output = h.finalize();
    let mut result = [0u8; 64];
    result.copy_from_slice(&output);
    result
}

/// Standard HMAC-BLAKE2b-512 (ipad/opad construction, BLOCKLEN=128).
///
/// Matches `noise-handshake/hmac.js` which uses `crypto_generichash_batch`
/// with explicit 128-byte pads — NOT BLAKE2b's native keyed MAC mode.
fn hmac_blake2b(key: &[u8], data: &[u8]) -> [u8; 64] {
    const BLOCKLEN: usize = 128;
    let mut hkey = [0u8; BLOCKLEN];
    if key.len() > BLOCKLEN {
        let h = blake2b_512(key);
        hkey[..64].copy_from_slice(&h);
    } else {
        hkey[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0u8; BLOCKLEN];
    let mut opad = [0u8; BLOCKLEN];
    for i in 0..BLOCKLEN {
        ipad[i] = hkey[i] ^ 0x36;
        opad[i] = hkey[i] ^ 0x5c;
    }
    let inner = blake2b_512_multi(&[&ipad, data]);
    blake2b_512_multi(&[&opad, &inner])
}

/// Noise-specific HKDF using HMAC-BLAKE2b-512 with empty `info`.
///
/// Algorithm (matches `noise-handshake/hkdf.js` with info=''):
/// ```text
/// temp_key = HMAC-BLAKE2b(key=chaining_key, data=input_key_material)
/// T1       = HMAC-BLAKE2b(key=temp_key, data=[0x01])
/// T2       = HMAC-BLAKE2b(key=temp_key, data=T1 || [0x02])
/// ```
fn hkdf(chaining_key: &[u8; 64], input_key_material: &[u8]) -> ([u8; 64], [u8; 64]) {
    let temp_key = hmac_blake2b(chaining_key, input_key_material);
    let output1 = hmac_blake2b(&temp_key, &[0x01]);
    // input for T2 = T1 (64 bytes) || 0x02
    let mut input2 = [0u8; 65];
    input2[..64].copy_from_slice(&output1);
    input2[64] = 0x02;
    let output2 = hmac_blake2b(&temp_key, &input2);
    (output1, output2)
}

/// Ed25519 DH — matches `noise-curve-ed/index.js`.
///
/// Algorithm:
/// 1. `hash = SHA-512(secret_key[0..32])` — expand the 32-byte seed
/// 2. Clamp: `hash[0] &= 248; hash[31] &= 127; hash[31] |= 64`
/// 3. Scalar multiplication on the Edwards curve
///
/// `secret_key` is in libsodium 64-byte format: `seed[32] || pubkey[32]`.
pub fn ed25519_dh(secret_key: &[u8; 64], public_key: &[u8; 32]) -> Result<[u8; 32], NoiseError> {
    // Expand the 32-byte seed via SHA-512 to obtain the scalar
    let mut hash = Sha512::digest(&secret_key[..32]);
    // Clamping — identical to libsodium's implementation
    hash[0] &= 248;
    hash[31] &= 127;
    hash[31] |= 64;

    let mut scalar_bytes = [0u8; 32];
    scalar_bytes.copy_from_slice(&hash[..32]);
    let scalar = Scalar::from_bytes_mod_order(scalar_bytes);

    let compressed = CompressedEdwardsY(*public_key);
    let point = compressed
        .decompress()
        .ok_or(NoiseError::InvalidPublicKey)?;

    Ok((point * scalar).compress().to_bytes())
}

// ─── CipherState ──────────────────────────────────────────────────────────────

/// ChaCha20-Poly1305 IETF cipher state.
///
/// Nonce layout (12 bytes): `[0x00; 4] || nonce.to_le_bytes()`.
/// When no key is set, encrypt/decrypt are identity operations.
struct CipherState {
    key: Option<[u8; 32]>,
    nonce: u64,
}

impl CipherState {
    fn new() -> Self {
        CipherState { key: None, nonce: 0 }
    }

    fn has_key(&self) -> bool {
        self.key.is_some()
    }

    fn set_key(&mut self, key: [u8; 32]) {
        self.key = Some(key);
        self.nonce = 0;
    }

    /// Build the 12-byte IETF nonce: `[0x00; 4] || nonce_u64_le`.
    /// Matches `cipher.js`: `view.setUint32(4, counter, true)` on a zero-
    /// initialised 12-byte buffer (equivalent for counters < 2^32).
    fn build_nonce(nonce: u64) -> [u8; 12] {
        let mut nonce_bytes = [0u8; 12];
        let le_bytes = nonce.to_le_bytes();
        nonce_bytes[4..12].copy_from_slice(&le_bytes);
        nonce_bytes
    }

    /// Encrypt `plaintext` with AEAD tag appended.  `ad` is the additional data.
    /// Returns plaintext unchanged when no key is set.
    fn encrypt(&mut self, plaintext: &[u8], ad: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let key = match self.key {
            None => return Ok(plaintext.to_vec()),
            Some(k) => k,
        };
        let nonce_bytes = Self::build_nonce(self.nonce);
        let cipher = ChaCha20Poly1305::new_from_slice(&key)
            // SAFETY: CipherState.key is Option<[u8; 32]>; ChaCha20Poly1305 requires 32 bytes.
            .expect("key is always 32 bytes");
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = cipher
            .encrypt(&nonce, Payload { msg: plaintext, aad: ad })
            .map_err(|_| NoiseError::DecryptionFailed)?;
        self.nonce += 1;
        Ok(ciphertext)
    }

    /// Decrypt `ciphertext` (includes 16-byte tag).  `ad` is the additional data.
    /// Returns ciphertext unchanged when no key is set.
    fn decrypt(&mut self, ciphertext: &[u8], ad: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let key = match self.key {
            None => return Ok(ciphertext.to_vec()),
            Some(k) => k,
        };
        let nonce_bytes = Self::build_nonce(self.nonce);
        let cipher = ChaCha20Poly1305::new_from_slice(&key)
            // SAFETY: CipherState.key is Option<[u8; 32]>; ChaCha20Poly1305 requires 32 bytes.
            .expect("key is always 32 bytes");
        let nonce = Nonce::from(nonce_bytes);
        let plaintext = cipher
            .decrypt(&nonce, Payload { msg: ciphertext, aad: ad })
            .map_err(|_| NoiseError::DecryptionFailed)?;
        self.nonce += 1;
        Ok(plaintext)
    }
}

// ─── SymmetricState ───────────────────────────────────────────────────────────

/// Noise SymmetricState: tracks `ck` (chaining key), `h` (running hash),
/// and an embedded CipherState.
struct SymmetricState {
    chaining_key: [u8; 64],
    digest: [u8; 64],
    cipher: CipherState,
}

impl SymmetricState {
    /// Initialise with `protocol_name`.
    ///
    /// If `len <= 64`: pad with zeros → `h`.  Else: `h = BLAKE2b-512(name)`.
    /// Then `ck = h`.  Cipher starts with no key.
    ///
    /// **Does not** mix the prologue — callers must call `mix_hash` explicitly.
    fn new(protocol_name: &[u8]) -> Self {
        let mut digest = [0u8; 64];
        if protocol_name.len() <= 64 {
            digest[..protocol_name.len()].copy_from_slice(protocol_name);
        } else {
            digest = blake2b_512(protocol_name);
        }
        SymmetricState {
            chaining_key: digest,
            digest,
            cipher: CipherState::new(),
        }
    }

    /// `h = BLAKE2b-512(h || data)`.
    fn mix_hash(&mut self, data: &[u8]) {
        self.digest = blake2b_512_multi(&[&self.digest, data]);
    }

    /// Run HKDF over `dh_output`; update `ck` and set the cipher key.
    fn mix_key(&mut self, dh_output: &[u8; 32]) {
        let (ck, temp_k) = hkdf(&self.chaining_key, dh_output);
        self.chaining_key = ck;
        let mut key = [0u8; 32];
        key.copy_from_slice(&temp_k[..32]);
        self.cipher.set_key(key);
    }

    /// `ciphertext = encrypt(plaintext, h)` then `mix_hash(ciphertext)`.
    fn encrypt_and_hash(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let ad = self.digest;
        let ciphertext = self.cipher.encrypt(plaintext, &ad)?;
        self.mix_hash(&ciphertext);
        Ok(ciphertext)
    }

    /// `plaintext = decrypt(ciphertext, h)` then `mix_hash(ciphertext)`.
    /// Note: the *ciphertext* (not plaintext) is mixed into `h`.
    fn decrypt_and_hash(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let ad = self.digest;
        let plaintext = self.cipher.decrypt(ciphertext, &ad)?;
        self.mix_hash(ciphertext);
        Ok(plaintext)
    }

    /// HKDF split: derive two 32-byte session keys from the chaining key.
    ///
    /// Returns `(k1, k2)` where both are the first 32 bytes of the respective
    /// 64-byte HKDF outputs.
    fn split(&self) -> ([u8; 32], [u8; 32]) {
        let (t1, t2) = hkdf(&self.chaining_key, &[]);
        let mut k1 = [0u8; 32];
        let mut k2 = [0u8; 32];
        k1.copy_from_slice(&t1[..32]);
        k2.copy_from_slice(&t2[..32]);
        (k1, k2)
    }

    fn get_handshake_hash(&self) -> [u8; 64] {
        self.digest
    }

    fn has_key(&self) -> bool {
        self.cipher.has_key()
    }
}

// ─── Public types ─────────────────────────────────────────────────────────────

/// Ed25519 keypair in libsodium 64-byte format: `seed[32] || pubkey[32]`.
#[derive(Clone)]
pub struct Keypair {
    /// Ed25519 public key (32-byte compressed Edwards Y point).
    pub public_key: [u8; 32],
    /// 64-byte libsodium format: first 32 bytes are the seed, last 32 are the
    /// compressed Edwards Y public key.
    pub secret_key: [u8; 64],
}

/// Session keys and metadata produced after a completed Noise XX handshake.
#[derive(Clone)]
#[non_exhaustive]
pub struct HandshakeResult {
    /// Session key for outbound messages (encrypt with this).
    pub tx: [u8; 32],
    /// Session key for inbound messages (decrypt with this).
    pub rx: [u8; 32],
    /// BLAKE2b-512 hash of the entire handshake transcript.
    pub handshake_hash: [u8; 64],
    /// Remote peer's static public key.
    pub remote_public_key: [u8; 32],
}

// ─── Handshake state machine ──────────────────────────────────────────────────

/// Noise XX handshake.
///
/// ```text
/// Initiator: send() → recv() → send()   (completes after 3rd call)
/// Responder: recv() → send() → recv()   (completes after 3rd call)
/// ```
pub struct Handshake {
    is_initiator: bool,
    /// Counts completed half-steps (0–2; 3 = complete).
    step: u8,
    symmetric: SymmetricState,
    /// Static keypair.
    s: Keypair,
    /// Ephemeral keypair (generated lazily on first send).
    e: Option<Keypair>,
    /// Remote ephemeral public key.
    re: Option<[u8; 32]>,
    /// Remote static public key (learned during handshake).
    rs: Option<[u8; 32]>,
    /// Populated when the handshake completes.
    result: Option<HandshakeResult>,
}

impl Handshake {
    /// Create a new Noise XX handshake state.
    ///
    /// - `is_initiator` — true for the initiating peer, false for the responder.
    /// - `keypair`      — the local static keypair.
    pub fn new(is_initiator: bool, keypair: Keypair) -> Self {
        // Initialise SymmetricState: h = padded(protocol_name), ck = h.
        let mut sym = SymmetricState::new(PROTOCOL_NAME);
        // Mix the empty prologue: h = BLAKE2b-512(h).
        // This matches `noise.js initialise(prologue=Buffer.alloc(0))`.
        sym.mix_hash(&[]);

        Handshake {
            is_initiator,
            step: 0,
            symmetric: sym,
            s: keypair,
            e: None,
            re: None,
            rs: None,
            result: None,
        }
    }

    /// Pre-set the ephemeral keypair (for deterministic testing only).
    ///
    /// Must be called before the first `send()`.
    pub fn set_ephemeral(&mut self, keypair: Keypair) {
        self.e = Some(keypair);
    }

    /// Returns true after the handshake has completed.
    pub fn complete(&self) -> bool {
        self.result.is_some()
    }

    /// Access the handshake result (available once `complete()` returns true).
    pub fn result(&self) -> Option<&HandshakeResult> {
        self.result.as_ref()
    }

    /// Send the next handshake message.
    ///
    /// | Call | Initiator         | Responder         |
    /// |------|-------------------|-------------------|
    /// | 1st  | sends M1 (32 B)   | error             |
    /// | 2nd  | sends M3 (64 B)   | sends M2 (96 B)   |
    pub fn send(&mut self) -> Result<Vec<u8>, NoiseError> {
        if self.complete() {
            return Err(NoiseError::HandshakeComplete);
        }
        match (self.is_initiator, self.step) {
            (true, 0) => self.initiator_send_m1(),
            (false, 1) => self.responder_send_m2(),
            (true, 2) => self.initiator_send_m3(),
            _ => Err(NoiseError::UnexpectedState),
        }
    }

    /// Receive the next handshake message.
    ///
    /// Returns `Some(HandshakeResult)` when the handshake completes
    /// (responder on M3), or `None` if more messages remain.
    pub fn recv(&mut self, message: &[u8]) -> Result<Option<HandshakeResult>, NoiseError> {
        if self.complete() {
            return Err(NoiseError::HandshakeComplete);
        }
        match (self.is_initiator, self.step) {
            (false, 0) => self.responder_recv_m1(message),
            (true, 1) => self.initiator_recv_m2(message),
            (false, 2) => self.responder_recv_m3(message),
            _ => Err(NoiseError::UnexpectedState),
        }
    }

    // ── M1: Initiator → Responder ─────────────────────────────────────────────

    /// Initiator sends M1: `e` (32 bytes).
    fn initiator_send_m1(&mut self) -> Result<Vec<u8>, NoiseError> {
        // TOK_E: use pre-set ephemeral or generate new one
        let e = self.e.take().unwrap_or_else(generate_keypair);
        self.symmetric.mix_hash(&e.public_key);
        let out = e.public_key.to_vec();
        self.e = Some(e);

        // Empty payload: encrypt_and_hash([]) — no key yet, so 0-byte output,
        // but mix_hash([]) still runs.
        let payload_enc = self.symmetric.encrypt_and_hash(&[])?;

        self.step = 1;
        let mut message = out;
        message.extend_from_slice(&payload_enc);
        Ok(message)
    }

    /// Responder receives M1: parse `re`, mix_hash, decrypt empty payload.
    fn responder_recv_m1(&mut self, message: &[u8]) -> Result<Option<HandshakeResult>, NoiseError> {
        if message.len() < 32 {
            return Err(NoiseError::UnexpectedState);
        }
        // TOK_E: re = first 32 bytes
        let mut re = [0u8; 32];
        re.copy_from_slice(&message[..32]);
        self.symmetric.mix_hash(&re);
        self.re = Some(re);

        // Empty payload (remaining bytes after the 32-byte pubkey)
        self.symmetric.decrypt_and_hash(&message[32..])?;

        self.step = 1;
        Ok(None)
    }

    // ── M2: Responder → Initiator ─────────────────────────────────────────────

    /// Responder sends M2: `e, ee, s, es` + empty payload.
    /// Output: e.pubkey(32) + enc_s(48) + enc_payload(16) = 96 bytes.
    fn responder_send_m2(&mut self) -> Result<Vec<u8>, NoiseError> {
        let re = self.re.ok_or(NoiseError::UnexpectedState)?;
        let mut message: Vec<u8> = Vec::with_capacity(96);

        // TOK_E: use pre-set ephemeral or generate new one
        let e = self.e.take().unwrap_or_else(generate_keypair);
        self.symmetric.mix_hash(&e.public_key);
        message.extend_from_slice(&e.public_key);

        // TOK_EE: dh(e.secret, re)  [local=e, remote=re]
        let dh_ee = ed25519_dh(&e.secret_key, &re)?;
        self.symmetric.mix_key(&dh_ee);

        // TOK_S: encrypt_and_hash(s.pubkey)  → 48 bytes
        let s_pub = self.s.public_key;
        let enc_s = self.symmetric.encrypt_and_hash(&s_pub)?;
        message.extend_from_slice(&enc_s);

        // TOK_ES (responder): dh(s.secret, re)  [local=s, remote=re]
        let dh_es = ed25519_dh(&self.s.secret_key, &re)?;
        self.symmetric.mix_key(&dh_es);

        self.e = Some(e);

        // Empty payload: encrypt_and_hash([])  → 16-byte MAC
        let enc_payload = self.symmetric.encrypt_and_hash(&[])?;
        message.extend_from_slice(&enc_payload);

        self.step = 2;
        Ok(message)
    }

    /// Initiator receives M2: parse `e, ee, s, es` + empty payload.
    fn initiator_recv_m2(&mut self, message: &[u8]) -> Result<Option<HandshakeResult>, NoiseError> {
        // Expected: 32 (re) + 48 (enc_s) + 16 (enc_payload) = 96 bytes
        if message.len() < 96 {
            return Err(NoiseError::UnexpectedState);
        }
        let e = self.e.as_ref().ok_or(NoiseError::UnexpectedState)?;
        let e_secret = e.secret_key;

        let mut offset = 0;

        // TOK_E: re = message[0..32]
        let mut re = [0u8; 32];
        re.copy_from_slice(&message[offset..offset + 32]);
        self.symmetric.mix_hash(&re);
        self.re = Some(re);
        offset += 32;

        // TOK_EE: dh(e.secret, re)  [local=e, remote=re]
        let dh_ee = ed25519_dh(&e_secret, &re)?;
        self.symmetric.mix_key(&dh_ee);

        // TOK_S: decrypt_and_hash(message[32..80])  → rs (32 bytes)
        let enc_s_len = if self.symmetric.has_key() { 32 + 16 } else { 32 };
        if message.len() < offset + enc_s_len {
            return Err(NoiseError::UnexpectedState);
        }
        let rs_bytes = self.symmetric.decrypt_and_hash(&message[offset..offset + enc_s_len])?;
        let mut rs = [0u8; 32];
        rs.copy_from_slice(&rs_bytes);
        self.rs = Some(rs);
        offset += enc_s_len;

        // TOK_ES (initiator): dh(e.secret, rs)  [local=e, remote=rs]
        let dh_es = ed25519_dh(&e_secret, &rs)?;
        self.symmetric.mix_key(&dh_es);

        // Empty payload
        self.symmetric.decrypt_and_hash(&message[offset..])?;

        self.step = 2;
        Ok(None)
    }

    // ── M3: Initiator → Responder ─────────────────────────────────────────────

    /// Initiator sends M3: `s, se` + empty payload.
    /// Output: enc_s(48) + enc_payload(16) = 64 bytes.
    fn initiator_send_m3(&mut self) -> Result<Vec<u8>, NoiseError> {
        let re = self.re.ok_or(NoiseError::UnexpectedState)?;
        let mut message: Vec<u8> = Vec::with_capacity(64);

        // TOK_S: encrypt_and_hash(s.pubkey)  → 48 bytes
        let s_pub = self.s.public_key;
        let enc_s = self.symmetric.encrypt_and_hash(&s_pub)?;
        message.extend_from_slice(&enc_s);

        // TOK_SE (initiator): dh(s.secret, re)  [local=s, remote=re]
        let dh_se = ed25519_dh(&self.s.secret_key, &re)?;
        self.symmetric.mix_key(&dh_se);

        // Empty payload: encrypt_and_hash([])  → 16-byte MAC
        let enc_payload = self.symmetric.encrypt_and_hash(&[])?;
        message.extend_from_slice(&enc_payload);

        // split() → session keys
        let (k1, k2) = self.symmetric.split();
        let result = HandshakeResult {
            tx: k1,
            rx: k2,
            handshake_hash: self.symmetric.get_handshake_hash(),
            remote_public_key: self.rs.unwrap_or([0u8; 32]),
        };
        self.result = Some(result);
        self.step = 3;
        Ok(message)
    }

    /// Responder receives M3: parse `s, se` + empty payload, then split.
    fn responder_recv_m3(&mut self, message: &[u8]) -> Result<Option<HandshakeResult>, NoiseError> {
        // Expected: 48 (enc_s) + 16 (enc_payload) = 64 bytes
        if message.len() < 64 {
            return Err(NoiseError::UnexpectedState);
        }
        let e = self.e.as_ref().ok_or(NoiseError::UnexpectedState)?;
        let e_secret = e.secret_key;

        // TOK_S: decrypt_and_hash(message[0..48])  → rs
        let enc_s_len = if self.symmetric.has_key() { 32 + 16 } else { 32 };
        let rs_bytes = self.symmetric.decrypt_and_hash(&message[..enc_s_len])?;
        let mut rs = [0u8; 32];
        rs.copy_from_slice(&rs_bytes);
        self.rs = Some(rs);

        // TOK_SE (responder): dh(e.secret, rs)  [local=e, remote=rs]
        let dh_se = ed25519_dh(&e_secret, &rs)?;
        self.symmetric.mix_key(&dh_se);

        // Empty payload
        self.symmetric.decrypt_and_hash(&message[enc_s_len..])?;

        // split() → session keys (responder: tx=k2, rx=k1)
        let (k1, k2) = self.symmetric.split();
        let result = HandshakeResult {
            tx: k2,
            rx: k1,
            handshake_hash: self.symmetric.get_handshake_hash(),
            remote_public_key: rs,
        };
        self.result = Some(result.clone());
        self.step = 3;
        Ok(Some(result))
    }
}

// ─── Noise IK handshake ──────────────────────────────────────────────────────

/// Noise IK handshake — `Noise_IK_Ed25519_ChaChaPoly_BLAKE2b`.
///
/// Pre-message: `<- s` (responder's static key is known to initiator).
///
/// ```text
/// Initiator: send(payload) → recv(payload)   (completes after recv)
/// Responder: recv(payload) → send(payload)   (completes after send)
/// ```
pub struct HandshakeIK {
    is_initiator: bool,
    step: u8,
    symmetric: SymmetricState,
    s: Keypair,
    e: Option<Keypair>,
    re: Option<[u8; 32]>,
    rs: Option<[u8; 32]>,
    result: Option<HandshakeResult>,
}

impl HandshakeIK {
    /// Create an initiator — knows the responder's static key upfront.
    pub fn new_initiator(keypair: Keypair, remote_static: [u8; 32], prologue: &[u8]) -> Self {
        let mut sym = SymmetricState::new(PROTOCOL_NAME_IK);
        sym.mix_hash(prologue);
        sym.mix_hash(&remote_static);

        HandshakeIK {
            is_initiator: true,
            step: 0,
            symmetric: sym,
            s: keypair,
            e: None,
            re: None,
            rs: Some(remote_static),
            result: None,
        }
    }

    /// Create a responder — learns the initiator's key from M1.
    pub fn new_responder(keypair: Keypair, prologue: &[u8]) -> Self {
        let mut sym = SymmetricState::new(PROTOCOL_NAME_IK);
        sym.mix_hash(prologue);
        sym.mix_hash(&keypair.public_key);

        HandshakeIK {
            is_initiator: false,
            step: 0,
            symmetric: sym,
            s: keypair,
            e: None,
            re: None,
            rs: None,
            result: None,
        }
    }

    /// Pre-set the ephemeral keypair (for deterministic testing only).
    pub fn set_ephemeral(&mut self, keypair: Keypair) {
        self.e = Some(keypair);
    }

    /// Returns true after the handshake has completed.
    pub fn complete(&self) -> bool {
        self.result.is_some()
    }

    /// Access the handshake result (available once [`complete`](Self::complete) returns true).
    pub fn result(&self) -> Option<&HandshakeResult> {
        self.result.as_ref()
    }

    /// Returns the remote peer's static public key, if known.
    pub fn remote_static_key(&self) -> Option<&[u8; 32]> {
        self.rs.as_ref()
    }

    /// Send the next handshake message carrying `payload`.
    pub fn send(&mut self, payload: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if self.complete() {
            return Err(NoiseError::HandshakeComplete);
        }
        match (self.is_initiator, self.step) {
            (true, 0) => self.initiator_send_m1(payload),
            (false, 1) => self.responder_send_m2(payload),
            _ => Err(NoiseError::UnexpectedState),
        }
    }

    /// Receive the next handshake message, returns the decrypted payload.
    pub fn recv(&mut self, message: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if self.complete() {
            return Err(NoiseError::HandshakeComplete);
        }
        match (self.is_initiator, self.step) {
            (false, 0) => self.responder_recv_m1(message),
            (true, 1) => self.initiator_recv_m2(message),
            _ => Err(NoiseError::UnexpectedState),
        }
    }

    // ── M1: Initiator → Responder — e, es, s, ss + payload ──────────────────

    fn initiator_send_m1(&mut self, payload: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let rs = self.rs.ok_or(NoiseError::UnexpectedState)?;
        let mut message = Vec::new();

        let e = self.e.take().unwrap_or_else(generate_keypair);
        self.symmetric.mix_hash(&e.public_key);
        message.extend_from_slice(&e.public_key);

        let dh_es = ed25519_dh(&e.secret_key, &rs)?;
        self.symmetric.mix_key(&dh_es);

        let enc_s = self.symmetric.encrypt_and_hash(&self.s.public_key)?;
        message.extend_from_slice(&enc_s);

        let dh_ss = ed25519_dh(&self.s.secret_key, &rs)?;
        self.symmetric.mix_key(&dh_ss);

        self.e = Some(e);

        let enc_payload = self.symmetric.encrypt_and_hash(payload)?;
        message.extend_from_slice(&enc_payload);

        self.step = 1;
        Ok(message)
    }

    fn responder_recv_m1(&mut self, message: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if message.len() < 96 {
            return Err(NoiseError::UnexpectedState);
        }

        let mut offset = 0;

        let mut re = [0u8; 32];
        re.copy_from_slice(&message[offset..offset + 32]);
        self.symmetric.mix_hash(&re);
        self.re = Some(re);
        offset += 32;

        let dh_es = ed25519_dh(&self.s.secret_key, &re)?;
        self.symmetric.mix_key(&dh_es);

        let enc_s_len = if self.symmetric.has_key() { 48 } else { 32 };
        if message.len() < offset + enc_s_len {
            return Err(NoiseError::UnexpectedState);
        }
        let rs_bytes = self.symmetric.decrypt_and_hash(&message[offset..offset + enc_s_len])?;
        let mut rs = [0u8; 32];
        rs.copy_from_slice(&rs_bytes);
        self.rs = Some(rs);
        offset += enc_s_len;

        let dh_ss = ed25519_dh(&self.s.secret_key, &rs)?;
        self.symmetric.mix_key(&dh_ss);

        let payload = self.symmetric.decrypt_and_hash(&message[offset..])?;

        self.step = 1;
        Ok(payload)
    }

    // ── M2: Responder → Initiator — e, ee, se + payload ─────────────────────

    fn responder_send_m2(&mut self, payload: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let re = self.re.ok_or(NoiseError::UnexpectedState)?;
        let rs = self.rs.ok_or(NoiseError::UnexpectedState)?;
        let mut message = Vec::new();

        let e = self.e.take().unwrap_or_else(generate_keypair);
        self.symmetric.mix_hash(&e.public_key);
        message.extend_from_slice(&e.public_key);

        // TOK_EE: DH(e, re) — local ephemeral × remote ephemeral
        let dh_ee = ed25519_dh(&e.secret_key, &re)?;
        self.symmetric.mix_key(&dh_ee);

        // TOK_SE (responder): DH(e, rs) — local ephemeral × remote static
        // Noise spec: responder computes DH(e, rs) for `se` token.
        let dh_se = ed25519_dh(&e.secret_key, &rs)?;
        self.symmetric.mix_key(&dh_se);

        self.e = Some(e);

        let enc_payload = self.symmetric.encrypt_and_hash(payload)?;
        message.extend_from_slice(&enc_payload);

        let (k1, k2) = self.symmetric.split();
        self.result = Some(HandshakeResult {
            tx: k2,
            rx: k1,
            handshake_hash: self.symmetric.get_handshake_hash(),
            remote_public_key: self.rs.unwrap_or([0u8; 32]),
        });
        self.step = 2;
        Ok(message)
    }

    fn initiator_recv_m2(&mut self, message: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if message.len() < 48 {
            return Err(NoiseError::UnexpectedState);
        }

        let e = self.e.as_ref().ok_or(NoiseError::UnexpectedState)?;
        let e_secret = e.secret_key;

        let mut offset = 0;

        let mut re = [0u8; 32];
        re.copy_from_slice(&message[offset..offset + 32]);
        self.symmetric.mix_hash(&re);
        self.re = Some(re);
        offset += 32;

        // TOK_EE: DH(e, re) — local ephemeral × remote ephemeral
        let dh_ee = ed25519_dh(&e_secret, &re)?;
        self.symmetric.mix_key(&dh_ee);

        // TOK_SE (initiator): DH(s, re) — local static × remote ephemeral
        // Noise spec: initiator computes DH(s, re) for `se` token.
        let dh_se = ed25519_dh(&self.s.secret_key, &re)?;
        self.symmetric.mix_key(&dh_se);

        let payload = self.symmetric.decrypt_and_hash(&message[offset..])?;

        let (k1, k2) = self.symmetric.split();
        self.result = Some(HandshakeResult {
            tx: k1,
            rx: k2,
            handshake_hash: self.symmetric.get_handshake_hash(),
            remote_public_key: self.rs.unwrap_or([0u8; 32]),
        });
        self.step = 2;
        Ok(payload)
    }
}

// ─── Key generation helper ────────────────────────────────────────────────────

/// Generate a fresh Ed25519 keypair in libsodium 64-byte format.
pub fn generate_keypair() -> Keypair {
    let mut seed = [0u8; 32];
    chacha20poly1305::aead::rand_core::OsRng.fill_bytes(&mut seed);
    keypair_from_seed(&seed)
}

/// Create an Ed25519 keypair from a 32-byte seed (deterministic).
///
/// Matches `noise-curve-ed` `generateKeyPair(seed)`.
pub fn keypair_from_seed(seed: &[u8; 32]) -> Keypair {
    let signing_key = SigningKey::from_bytes(seed);
    let pub_key = signing_key.verifying_key().to_bytes();
    let mut secret_key = [0u8; 64];
    secret_key[..32].copy_from_slice(seed);
    secret_key[32..].copy_from_slice(&pub_key);
    Keypair { public_key: pub_key, secret_key }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify Ed25519 DH is symmetric: `dh(sk_a, pk_b) == dh(sk_b, pk_a)`.
    #[test]
    fn ed25519_dh_symmetric() {
        let kp_a = generate_keypair();
        let kp_b = generate_keypair();

        let shared_ab = ed25519_dh(&kp_a.secret_key, &kp_b.public_key).unwrap();
        let shared_ba = ed25519_dh(&kp_b.secret_key, &kp_a.public_key).unwrap();

        assert_eq!(shared_ab, shared_ba);
    }

    /// Encrypt then decrypt produces the original plaintext.
    #[test]
    fn cipher_roundtrip() {
        let key = [0x42u8; 32];
        let mut enc = CipherState::new();
        enc.set_key(key);
        let mut dec = CipherState::new();
        dec.set_key(key);

        let plaintext = b"hello noise world";
        let ad = b"additional data";

        let ciphertext = enc.encrypt(plaintext, ad).unwrap();
        let recovered = dec.decrypt(&ciphertext, ad).unwrap();

        assert_eq!(recovered, plaintext);
    }

    /// With no key set, encrypt and decrypt are identity operations.
    #[test]
    fn cipher_no_key_passthrough() {
        let mut cs = CipherState::new();
        assert!(!cs.has_key());

        let data = b"passthrough data";
        let enc = cs.encrypt(data, &[]).unwrap();
        assert_eq!(enc, data);

        let dec = cs.decrypt(data, &[]).unwrap();
        assert_eq!(dec, data);
    }

    /// Same inputs always produce the same HMAC output.
    #[test]
    fn hmac_deterministic() {
        let key = b"test-key";
        let data = b"test-data";
        let out1 = hmac_blake2b(key, data);
        let out2 = hmac_blake2b(key, data);
        assert_eq!(out1, out2);
    }

    /// HKDF outputs are different from each other.
    #[test]
    fn hkdf_produces_different_outputs() {
        let ck = [0x11u8; 64];
        let ikm = [0xaau8; 32];
        let (out1, out2) = hkdf(&ck, &ikm);
        assert_ne!(out1, out2);
    }

    /// Full Noise XX handshake between initiator and responder.
    #[test]
    fn handshake_xx_completes() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let resp_static_pub = resp_kp.public_key;

        let mut initiator = Handshake::new(true, init_kp);
        let mut responder = Handshake::new(false, resp_kp);

        // M1: Initiator → Responder
        let m1 = initiator.send().expect("initiator send M1");
        assert!(!initiator.complete(), "initiator should not be done after M1");

        responder.recv(&m1).expect("responder recv M1");
        assert!(!responder.complete(), "responder should not be done after M1");

        // M2: Responder → Initiator
        let m2 = responder.send().expect("responder send M2");
        assert!(!responder.complete(), "responder should not be done after M2");

        initiator.recv(&m2).expect("initiator recv M2");
        assert!(!initiator.complete(), "initiator should not be done after M2");

        // M3: Initiator → Responder
        let m3 = initiator.send().expect("initiator send M3");
        assert!(initiator.complete(), "initiator should be done after M3");

        let resp_result = responder
            .recv(&m3)
            .expect("responder recv M3")
            .expect("responder should return HandshakeResult");
        assert!(responder.complete(), "responder should be done after M3");

        let init_result = initiator.result().expect("initiator result should be set");

        // Session keys must agree
        assert_eq!(init_result.tx, resp_result.rx, "initiator.tx == responder.rx");
        assert_eq!(init_result.rx, resp_result.tx, "initiator.rx == responder.tx");

        // Handshake hashes must agree
        assert_eq!(
            init_result.handshake_hash, resp_result.handshake_hash,
            "handshake hashes must match"
        );

        // Each side should know the other's static public key
        assert_eq!(
            init_result.remote_public_key, resp_static_pub,
            "initiator should learn responder's static pubkey"
        );
    }

    // ── Noise IK tests ──────────────────────────────────────────────────────

    #[test]
    fn handshake_ik_completes() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let resp_pub = resp_kp.public_key;
        let init_pub = init_kp.public_key;
        let prologue = b"test-prologue";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_pub, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let payload_1 = b"hello from initiator";
        let m1 = initiator.send(payload_1).expect("initiator send M1");
        assert!(!initiator.complete());

        let recv_payload_1 = responder.recv(&m1).expect("responder recv M1");
        assert!(!responder.complete());
        assert_eq!(recv_payload_1, payload_1);

        let payload_2 = b"hello from responder";
        let m2 = responder.send(payload_2).expect("responder send M2");
        assert!(responder.complete());

        let recv_payload_2 = initiator.recv(&m2).expect("initiator recv M2");
        assert!(initiator.complete());
        assert_eq!(recv_payload_2, payload_2);

        let init_result = initiator.result().expect("initiator result");
        let resp_result = responder.result().expect("responder result");

        assert_eq!(init_result.tx, resp_result.rx);
        assert_eq!(init_result.rx, resp_result.tx);
        assert_eq!(init_result.handshake_hash, resp_result.handshake_hash);
        assert_eq!(init_result.remote_public_key, resp_pub);
        assert_eq!(resp_result.remote_public_key, init_pub);
    }

    #[test]
    fn handshake_ik_empty_payloads() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let prologue = b"";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_kp.public_key, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let m1 = initiator.send(&[]).unwrap();
        let p1 = responder.recv(&m1).unwrap();
        assert!(p1.is_empty());

        let m2 = responder.send(&[]).unwrap();
        let p2 = initiator.recv(&m2).unwrap();
        assert!(p2.is_empty());

        assert!(initiator.complete());
        assert!(responder.complete());

        let ir = initiator.result().unwrap();
        let rr = responder.result().unwrap();
        assert_eq!(ir.tx, rr.rx);
        assert_eq!(ir.rx, rr.tx);
    }

    #[test]
    fn handshake_ik_different_prologue_fails() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_kp.public_key, b"prologue-A");
        let mut responder = HandshakeIK::new_responder(resp_kp, b"prologue-B");

        let m1 = initiator.send(b"test").unwrap();
        assert!(responder.recv(&m1).is_err());
    }

    #[test]
    fn handshake_ik_wrong_remote_key_fails() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let wrong_kp = generate_keypair();
        let prologue = b"test";

        let mut initiator = HandshakeIK::new_initiator(init_kp, wrong_kp.public_key, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let m1 = initiator.send(b"test").unwrap();
        assert!(responder.recv(&m1).is_err());
    }

    #[test]
    fn handshake_ik_m1_too_short() {
        let resp_kp = generate_keypair();
        let mut responder = HandshakeIK::new_responder(resp_kp, b"");
        assert!(responder.recv(&[0u8; 95]).is_err());
    }

    #[test]
    fn handshake_ik_m2_too_short() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let prologue = b"";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_kp.public_key, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let m1 = initiator.send(&[]).unwrap();
        let _ = responder.recv(&m1).unwrap();
        let _ = responder.send(&[]).unwrap();

        assert!(initiator.recv(&[0u8; 47]).is_err());
    }

    #[test]
    fn handshake_ik_remote_static_key_available() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let resp_pub = resp_kp.public_key;
        let init_pub = init_kp.public_key;
        let prologue = b"";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_pub, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        assert_eq!(initiator.remote_static_key(), Some(&resp_pub));
        assert_eq!(responder.remote_static_key(), None);

        let m1 = initiator.send(&[]).unwrap();
        let _ = responder.recv(&m1).unwrap();
        assert_eq!(responder.remote_static_key(), Some(&init_pub));

        let m2 = responder.send(&[]).unwrap();
        let _ = initiator.recv(&m2).unwrap();
        assert_eq!(initiator.remote_static_key(), Some(&resp_pub));
    }

    #[test]
    fn handshake_ik_large_payload() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let prologue = b"";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_kp.public_key, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let big_payload = vec![0xab; 4096];
        let m1 = initiator.send(&big_payload).unwrap();
        let recv_big = responder.recv(&m1).unwrap();
        assert_eq!(recv_big, big_payload);

        let m2 = responder.send(&big_payload).unwrap();
        let recv_big_2 = initiator.recv(&m2).unwrap();
        assert_eq!(recv_big_2, big_payload);
    }

    #[test]
    fn handshake_ik_send_after_complete_errors() {
        let init_kp = generate_keypair();
        let resp_kp = generate_keypair();
        let prologue = b"";

        let mut initiator = HandshakeIK::new_initiator(init_kp, resp_kp.public_key, prologue);
        let mut responder = HandshakeIK::new_responder(resp_kp, prologue);

        let m1 = initiator.send(&[]).unwrap();
        let _ = responder.recv(&m1).unwrap();
        let m2 = responder.send(&[]).unwrap();
        let _ = initiator.recv(&m2).unwrap();

        assert!(initiator.send(&[]).is_err());
        assert!(initiator.recv(&[0u8; 48]).is_err());
        assert!(responder.send(&[]).is_err());
        assert!(responder.recv(&[0u8; 96]).is_err());
    }
}
