//! v2 key derivation.
//!
//! Spec: see *Key Derivation* section of `DEADDROP_V2.md (and `docs/src/dd/`)`.
//!
//!   root_keypair      = KeyPair::from_seed(root_seed)
//!   `index_keypair[i]`  = KeyPair::from_seed(blake2b(root_seed || b"idx" || i_le))
//!                       where i is u32 little-endian
//!   salt              = `root_seed[0]`

#![allow(dead_code)]

use peeroxide::{discovery_key, KeyPair};

/// Per-deaddrop salt byte. Embedded in every data chunk header.
///
/// Currently forced to `0x00`: the original intent was DHT address-space
/// isolation between unrelated deaddrops with identical content, but in
/// practice this is unnecessary. The header byte is retained so the wire
/// format does not change.
pub fn salt(_root_seed: &[u8; 32]) -> u8 {
    0x00
}

/// Derive the keypair for non-root index chunk number `i`.
///
/// `i` is a sender-assigned linear number in `[0, 2^32 - 1]`. The order
/// in which the sender assigns numbers is unspecified by the protocol;
/// the reference sender uses bottom-up build order. Tree position is
/// not encoded in the keypair.
pub fn derive_index_keypair(root_seed: &[u8; 32], i: u32) -> KeyPair {
    let mut input = Vec::with_capacity(32 + 3 + 4);
    input.extend_from_slice(root_seed);
    input.extend_from_slice(b"idx");
    input.extend_from_slice(&i.to_le_bytes());
    let seed = discovery_key(&input);
    KeyPair::from_seed(seed)
}

/// Topic for need-list publishing: `discovery_key(root_pk || b"need")`.
pub fn need_topic(root_pk: &[u8; 32]) -> [u8; 32] {
    let mut input = Vec::with_capacity(32 + 4);
    input.extend_from_slice(root_pk);
    input.extend_from_slice(b"need");
    discovery_key(&input)
}

/// Topic for pickup acknowledgements: `discovery_key(root_pk || b"ack")`.
pub fn ack_topic(root_pk: &[u8; 32]) -> [u8; 32] {
    let mut input = Vec::with_capacity(32 + 3);
    input.extend_from_slice(root_pk);
    input.extend_from_slice(b"ack");
    discovery_key(&input)
}

/// Compute the DHT address (BLAKE2b-256 of the encoded chunk) for a data chunk.
///
/// Same as `discovery_key` of the encoded bytes, but named to make intent
/// clear at call sites.
pub fn data_chunk_address(encoded_chunk: &[u8]) -> [u8; 32] {
    discovery_key(encoded_chunk)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salt_is_zero() {
        let mut seed = [0u8; 32];
        seed[0] = 0xAB;
        seed[1] = 0xCD;
        assert_eq!(salt(&seed), 0x00);
    }

    #[test]
    fn derive_index_keypair_deterministic() {
        let seed = [42u8; 32];
        let kp_a = derive_index_keypair(&seed, 7);
        let kp_b = derive_index_keypair(&seed, 7);
        assert_eq!(kp_a.public_key, kp_b.public_key);
        assert_eq!(kp_a.secret_key, kp_b.secret_key);
    }

    #[test]
    fn derive_index_keypair_distinct_per_index() {
        let seed = [42u8; 32];
        let kp_0 = derive_index_keypair(&seed, 0);
        let kp_1 = derive_index_keypair(&seed, 1);
        let kp_2 = derive_index_keypair(&seed, 2);
        assert_ne!(kp_0.public_key, kp_1.public_key);
        assert_ne!(kp_1.public_key, kp_2.public_key);
        assert_ne!(kp_0.public_key, kp_2.public_key);
    }

    #[test]
    fn derive_index_keypair_distinct_per_seed() {
        let kp_a = derive_index_keypair(&[1u8; 32], 0);
        let kp_b = derive_index_keypair(&[2u8; 32], 0);
        assert_ne!(kp_a.public_key, kp_b.public_key);
    }

    #[test]
    fn derive_index_keypair_supports_high_indices() {
        // Sanity: u32 max should not panic.
        let _ = derive_index_keypair(&[0u8; 32], u32::MAX);
        let _ = derive_index_keypair(&[0u8; 32], 1_000_000);
    }

    #[test]
    fn need_topic_deterministic() {
        let pk = [99u8; 32];
        assert_eq!(need_topic(&pk), need_topic(&pk));
    }

    #[test]
    fn need_and_ack_topics_differ() {
        let pk = [42u8; 32];
        assert_ne!(need_topic(&pk), ack_topic(&pk));
    }

    #[test]
    fn data_chunk_address_changes_with_salt() {
        // Same payload, different salt → different address (the whole point).
        let payload = b"identical content";
        let mut chunk_a = vec![0x02, 0xAA];
        chunk_a.extend_from_slice(payload);
        let mut chunk_b = vec![0x02, 0xBB];
        chunk_b.extend_from_slice(payload);
        assert_ne!(data_chunk_address(&chunk_a), data_chunk_address(&chunk_b));
    }
}
