#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Rust port of [HyperDHT](https://github.com/holepunchto/hyperdht) — a
//! Kademlia distributed hash table with NAT hole-punching, Noise-encrypted
//! connections, and blind-relay fallback.
//!
//! This crate implements the full HyperDHT protocol stack, wire-compatible
//! with the Node.js implementation on the public Hyperswarm network.
//!
//! # Protocol layers
//!
//! From bottom to top:
//!
//! | Layer | Module | Reference |
//! |---|---|---|
//! | Wire encoding | `compact_encoding` (crate-internal) | [compact-encoding](https://github.com/holepunchto/compact-encoding) |
//! | DHT RPC | [`rpc`], [`io`], `query` (internal), `routing_table` (internal) | [dht-rpc](https://github.com/mafintosh/dht-rpc) |
//! | Peer operations | [`hyperdht`], [`hyperdht_messages`] | [hyperdht](https://github.com/holepunchto/hyperdht) |
//! | Noise XX handshake | [`noise`], [`noise_wrap`] | [noise-handshake](https://github.com/holepunchto/noise-handshake) |
//! | Encrypted streams | [`secret_stream`], [`secretstream`] | [@hyperswarm/secret-stream](https://github.com/holepunchto/hyperswarm-secret-stream) |
//! | NAT traversal | `nat` (internal), [`holepuncher`] | hyperdht/lib/holepuncher.js |
//! | Relay | [`blind_relay`], [`protomux`] | [blind-relay](https://github.com/holepunchto/blind-relay) |
//!
//! # Typical usage
//!
//! Most users should depend on the higher-level [`peeroxide`](https://docs.rs/peeroxide)
//! crate, which wraps this DHT layer with topic-based peer discovery and
//! connection management. Use `peeroxide-dht` directly when you need
//! low-level DHT operations (custom commands, mutable/immutable storage,
//! manual hole-punching).
//!
//! ```rust,no_run
//! use libudx::UdxRuntime;
//! use peeroxide_dht::hyperdht::{self, HyperDhtConfig, KeyPair};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = UdxRuntime::new()?;
//! let config = HyperDhtConfig::with_public_bootstrap();
//! let (_task, dht, _server_rx) = hyperdht::spawn(&runtime, config).await?;
//!
//! dht.bootstrapped().await?;
//!
//! let key_pair = KeyPair::generate();
//! let topic = peeroxide_dht::crypto::hash(b"my-app");
//! dht.announce(topic, &key_pair, &[]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Interoperability
//!
//! Every protocol layer is validated against the Node.js reference via
//! golden byte fixtures and live cross-language interop tests. The crate
//! connects to the public HyperDHT bootstrap nodes and participates in
//! the same network as Node.js peers.

#![deny(clippy::all)]

// ─── Always-public modules (documented) ──────────────────────────────────────

/// Blind relay for proxying encrypted traffic between peers behind restrictive NATs.
pub mod blind_relay;
/// BLAKE2b hashing, Ed25519 signing, and namespace derivation helpers.
pub mod crypto;
/// High-level HyperDHT node: peer discovery, announce/unannounce, mutable/immutable
/// storage, and Noise-encrypted connections.
pub mod hyperdht;
/// Wire-format message types for HyperDHT peer handshake, holepunch, and relay
/// operations.
pub mod hyperdht_messages;
/// DHT RPC request/response message encoding and decoding.
pub mod messages;
/// Noise IK handshake for establishing shared secrets between peers.
pub mod noise;
/// Noise handshake wrapper that adds framing and key splitting for stream encryption.
pub mod noise_wrap;
/// Lightweight multiplexer for running multiple channels over a single connection.
pub mod protomux;
/// DHT RPC transport layer: request dispatch, reply handling, and node communication.
pub mod rpc;
/// Noise-encrypted bidirectional byte stream over any `AsyncRead + AsyncWrite` transport.
pub mod secret_stream;

// ─── Promoted modules (were #[doc(hidden)], now fully documented public API) ──
// Doc comments below are stubs; Wave 9 replaces them with full module documentation.

/// NAT hole-punching state machine and birthday-attack socket pool management.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod holepuncher;
/// Wire counters, request parameters, and I/O event types for the DHT RPC layer.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod io;
/// Peer identity: node ID type alias and peer-ID derivation utilities.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod peer;
/// Persistent DHT node storage: bootstrap-cache configuration and lifecycle.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod persistent;
/// Secretstream encryption layer: ChaCha20-Poly1305 AEAD over Noise sessions.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod secretstream;
/// Secure payload encoding for DHT peer-handshake data exchange.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod secure_payload;
/// UDP socket pool for NAT hole-punching and birthday-attack probe management.
///
/// TODO(Wave 9): expand with full module documentation.
pub mod socket_pool;

// ─── Demoted modules (crate-internal; not part of the published API) ─────────

pub(crate) mod compact_encoding;
pub(crate) mod nat;
pub(crate) mod query;
pub(crate) mod router;
pub(crate) mod routing_table;

// ─── Crate-root re-exports for types in pub(crate) modules that surface ───────
// via public method return types / error variants.  Without these re-exports,
// `cargo doc --no-deps` would fail because public signatures reference types
// that are not otherwise reachable from the crate root.

/// Errors produced by compact-encoding operations.
///
/// Re-exported from the crate-internal `compact_encoding` module because it
/// appears as a variant of [`hyperdht::HyperDhtError`] and [`RouterError`].
pub use compact_encoding::{EncodingError, State};

/// A reply entry in the iterative-query closest-replies set.
///
/// Re-exported from the crate-internal `query` module because it is the element
/// type of `Vec<QueryReply>` returned by
/// [`rpc::DhtHandle::find_node`], [`rpc::DhtHandle::query`], and
/// [`hyperdht::HyperDhtHandle::query_find_peer`].
pub use query::QueryReply;

/// Re-exported router types from the crate-internal `router` module.
///
/// [`Router`] is returned by [`hyperdht::HyperDhtHandle::router`].  The
/// associated action / result / error types appear in [`Router`]'s public
/// method signatures and in the [`hyperdht::HyperDhtError::Router`] variant.
pub use router::{
    ForwardEntry,
    HandshakeAction,
    HandshakeResult,
    HolepunchAction,
    HolepunchResult,
    Router,
    RouterError,
};
