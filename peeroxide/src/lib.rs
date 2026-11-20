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
//! # Choosing limits
//!
//! Each connection's stream holds at most its read buffer limit for its
//! reader, and its write buffer limit written and not yet acknowledged.
//! The limits bound memory and, with it, speed: a stream has no more on its
//! way at one time than the sender's write limit and the receiver's read
//! limit allow, so a connection moves at most about **0.94 × limit ÷ round
//! trip** in each direction. A limit is a bound, not memory set aside: a
//! reader that keeps up holds little.
//!
//! | Preset | Each limit | Peers | Budget | 100 ms | 200 ms |
//! |---|---|---|---|---|---|
//! | [`SwarmConfig::constrained`] | 256 KiB | 16 | 8 MiB | 2.5 MB/s | 1.2 MB/s |
//! | [`SwarmConfig::mobile`] | 1 MiB | 32 | 64 MiB | 9.9 MB/s | 4.9 MB/s |
//! | [`SwarmConfig::balanced`] (default) | 8 MiB | 64 | none | 79 MB/s | 39.5 MB/s |
//! | [`SwarmConfig::throughput`] | 32 MiB | 64 | none | 311 MB/s | 153 MB/s |
//!
//! The rates were measured for one connection, without jitter or loss, and
//! need the remote to allow them too: a remote on Node.js advertises 4 MiB,
//! about 36 MB/s over 100 ms. Below a round trip of a few milliseconds the
//! machine, not the limits, sets the rate.
//!
//! [`SwarmConfig::memory_budget`] caps all connections together: each gets
//! the lower of its limits and `budget / (2 × max_peers)`, but never less
//! than [`MIN_STREAM_LIMIT`]. [`SwarmConfig::connection_limits`] tells what
//! a connection is given. Not covered by any of them: a connection
//! receiving a message holds it whole, up to 16 MiB.
//!
//! A preset is a starting point; any field can be changed after it:
//!
//! ```
//! use peeroxide::SwarmConfig;
//!
//! let mut config = SwarmConfig::throughput();
//! config.memory_budget = Some(2 << 30); // no more than 2 GiB in all
//! assert_eq!(config.connection_limits().read_buffer_limit, Some(16 << 20));
//! ```
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
//! - [`StreamLimits`] — a connection's read and write buffer limits

#![deny(clippy::all)]

mod connection_set;
mod error;
mod peer_discovery;
mod peer_info;
mod swarm;

pub use error::SwarmError;
pub use peer_info::{PeerInfo, Priority};
pub use swarm::{spawn, JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle, MIN_STREAM_LIMIT};

// Re-export commonly used types from peeroxide-dht.
pub use peeroxide_dht::crypto::hash as discovery_key;
pub use peeroxide_dht::hyperdht::{
    HyperDhtHandle, ImmutablePutResult, KeyPair, MutableGetResult, MutablePutResult,
    StreamLimits, DEFAULT_BOOTSTRAP,
};
