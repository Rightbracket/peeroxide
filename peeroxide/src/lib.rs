#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Rust port of [Hyperswarm](https://github.com/holepunchto/hyperswarm) —
//! topic-based P2P peer discovery and encrypted connections over the
//! public HyperDHT network.
//!
//! `peeroxide` is the high-level entry point for the Peeroxide stack.
//! It discovers peers by topic, establishes Noise-encrypted connections,
//! and manages the full connection lifecycle (deduplication, retry with
//! backoff, priority queuing). When configured, a swarm can also route
//! server-side connections through a blind-relay server instead of using
//! the direct or hole-punched path. Wire-compatible with the Node.js
//! Hyperswarm network.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use peeroxide::{spawn, discovery_key, JoinOpts, SwarmConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = SwarmConfig::with_public_bootstrap();
//!     let (_task, handle, mut conn_rx) = spawn(config).await?;
//!
//!     let topic = discovery_key(b"my-application");
//!     handle.join(topic, JoinOpts::default()).await?;
//!
//!     while let Some(conn) = conn_rx.recv().await {
//!         println!("connected to {}", hex::encode(conn.remote_public_key()));
//!         // conn.peer.stream: encrypted AsyncRead + AsyncWrite
//!     }
//!     Ok(())
//! }
//! ```
//!
//! # How it works
//!
//! 1. **Join a topic** — `handle.join(topic, opts)` announces your
//!    presence and/or looks up other peers on that 32-byte topic hash.
//! 2. **Peer discovery** — the DHT returns peers announcing the same
//!    topic. The swarm connects to them automatically, with retry
//!    backoff and priority scoring.
//! 3. **Noise-encrypted connection** — each connection completes a
//!    Noise IK handshake, then wraps the UDX stream in a SecretStream
//!    (ChaCha20-Poly1305 AEAD).
//! 4. **Receive connections** — incoming `SwarmConnection` values arrive
//!    on the `conn_rx` channel, each carrying the remote peer's public
//!    key and an encrypted bidirectional stream.
//!
//! # Crate stack
//!
//! ```text
//! peeroxide          ← you are here (topic discovery + connection management)
//! └── peeroxide-dht  ← Kademlia DHT, Noise handshakes, hole-punching, relay
//!     └── libudx     ← reliable UDP transport with BBR congestion control
//! ```
//!
//! # Re-exports
//!
//! This crate re-exports commonly used types so you don't need to depend
//! on `peeroxide-dht` directly:
//!
//! - [`discovery_key`] — BLAKE2b-256 hash for deriving topic keys
//! - [`KeyPair`] — Ed25519 key pair for peer identity
//! - [`HyperDhtHandle`] — low-level DHT handle for mutable/immutable storage
//! - [`MutablePutResult`], [`MutableGetResult`], [`ImmutablePutResult`] — DHT storage results
//! - [`DEFAULT_BOOTSTRAP`] — public HyperDHT bootstrap node addresses

#![deny(clippy::all)]

mod connection_set;
mod error;
mod peer_discovery;
mod peer_info;
mod swarm;

pub use error::SwarmError;
pub use peer_info::{PeerInfo, Priority};
pub use swarm::{spawn, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle};

// Re-export commonly used types from peeroxide-dht.
pub use peeroxide_dht::crypto::hash as discovery_key;
pub use peeroxide_dht::hyperdht::{
    HyperDhtHandle, ImmutablePutResult, KeyPair, MutableGetResult, MutablePutResult,
    DEFAULT_BOOTSTRAP,
};
