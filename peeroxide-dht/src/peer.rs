//! Peer identity: node ID type alias and peer-ID derivation utilities.
//!
//! TODO(Wave 9): add module documentation.

#![allow(missing_docs)]

use blake2::digest::consts::U32;
use blake2::digest::Digest;
use blake2::Blake2b;

use crate::compact_encoding::{self, State};

pub type NodeId = [u8; 32];

type Blake2b256 = Blake2b<U32>;

#[derive(Debug, Clone)]
pub struct PeerAddr {
    pub host: String,
    pub port: u16,
}

impl PeerAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn id(&self) -> NodeId {
        peer_id(&self.host, self.port)
    }
}

/// Compute a 32-byte node ID from a host:port pair.
/// Matches Node.js: `sodium.crypto_generichash(out, ipv4_encode(host, port))`
pub fn peer_id(host: &str, port: u16) -> NodeId {
    let mut enc = State::new();
    compact_encoding::preencode_ipv4_address(&mut enc, host, port);
    enc.alloc();
    compact_encoding::encode_ipv4_address(&mut enc, host, port)
        // SAFETY: caller provides host/port; encoding to buffer cannot fail for valid inputs.
        .expect("valid ipv4 address");

    let mut hasher = Blake2b256::new();
    hasher.update(&enc.buffer);
    let hash = hasher.finalize();

    let mut id = [0u8; 32];
    id.copy_from_slice(&hash);
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_id_deterministic() {
        let a = peer_id("127.0.0.1", 49737);
        let b = peer_id("127.0.0.1", 49737);
        assert_eq!(a, b);
    }

    #[test]
    fn peer_id_differs_by_port() {
        let a = peer_id("127.0.0.1", 49737);
        let b = peer_id("127.0.0.1", 49738);
        assert_ne!(a, b);
    }

    #[test]
    fn peer_id_differs_by_host() {
        let a = peer_id("10.0.0.1", 8080);
        let b = peer_id("10.0.0.2", 8080);
        assert_ne!(a, b);
    }

    #[test]
    fn peer_addr_id() {
        let addr = PeerAddr::new("192.168.1.1", 3000);
        let expected = peer_id("192.168.1.1", 3000);
        assert_eq!(addr.id(), expected);
    }

    #[test]
    fn peer_id_is_32_bytes() {
        let id = peer_id("0.0.0.0", 0);
        assert_eq!(id.len(), 32);
        assert_ne!(id, [0u8; 32]);
    }
}
