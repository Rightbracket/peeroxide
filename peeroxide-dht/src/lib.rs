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

/// NAT hole-punching coordination for peer-to-peer connections.
///
/// The `Holepuncher` drives the active-side hole-punching state machine used
/// by [`hyperdht::HyperDhtHandle::connect`] to traverse symmetric NATs and
/// reach peers that cannot be dialed directly. It owns the local
/// [`socket_pool::SocketPool`] used for birthday-attack probes, the NAT
/// classification analyzer, and the punch-message dispatch loop that
/// cooperates with the remote peer over a relayed control channel.
///
/// Most consumers reach this module indirectly through `connect()` and the
/// resulting `PeerConnection`; direct use is only needed when building
/// custom DHT clients that orchestrate the punch sequence themselves. See
/// `hyperdht/lib/holepuncher.js` in the Node.js reference implementation for
/// the protocol shape.
pub mod holepuncher;
/// IO layer for the DHT-RPC protocol.
///
/// A faithful Rust port of the Node.js `dht-rpc` IO layer. The `Io` struct
/// tracks in-flight requests, wire counters, and per-request parameters, and
/// is driven by the caller from a `tokio::select!` loop rather than owning
/// its own task. Most consumers reach it only indirectly through
/// [`rpc::DhtHandle`]; direct use is for building alternative RPC transports
/// or low-level protocol tooling.
pub mod io;
/// Peer-identity primitives shared across the DHT and swarm layers.
///
/// Defines `PeerAddr` (an Ed25519-keyed node identity paired with a UDP
/// socket address) and the `peer_id` helper that derives the 32-byte
/// Kademlia node ID from a peer's public key. These types are used by
/// lower-level routing-table and request-routing code; most callers reach
/// them only through cross-language interop tests and DHT-level integration
/// code rather than directly.
pub mod peer;
/// Persistent storage for DHT records published by server nodes.
///
/// Provides the `Persistent` handler that backs the four record-storing RPC
/// verbs (`ANNOUNCE`, `UNANNOUNCE`, `MUTABLE_PUT`/`GET`, `IMMUTABLE_PUT`/`GET`)
/// with bounded LRU caches sized via `PersistentConfig`. `PersistentStats`
/// exposes per-cache record counts for operators monitoring a running node.
/// `PersistentConfig` is part of the public API for callers that run their
/// own DHT node (e.g. the `peeroxide node` CLI); the handler itself is
/// plumbed inside [`rpc::DhtHandle`] and not constructed directly by typical
/// consumers.
pub mod persistent;
/// Pure-Rust implementation of libsodium's `crypto_secretstream_xchacha20poly1305`.
///
/// Uses a manual ChaCha20 + Poly1305 construction matching libsodium's
/// internal layout exactly (counter=0 generates the Poly1305 key, counter=1
/// encrypts a 64-byte tag block, counter=2+ encrypts message bytes), backing
/// the encrypted-stream layer used above the Noise handshake. Most
/// consumers reach this only through [`secret_stream`]; direct use is for
/// byte-exact interop with the Node.js `sodium-native` secretstream API.
pub mod secretstream;
/// Authenticated-encryption helper for short DHT-handshake payloads.
///
/// `SecurePayload` wraps an XChaCha20-Poly1305 secretbox keyed off a BLAKE2b
/// namespace + remote-secret pairing, and is used by the swarm and
/// connect-handshake paths to attach encrypted application data (peer
/// addresses, holepunch instructions) to otherwise plaintext
/// `PEER_HANDSHAKE` / `PEER_HOLEPUNCH` messages. Errors are reported through
/// `SecurePayloadError`; the encrypted output also carries a short opaque
/// token (`SecurePayload::token`) that callers use to bind a reply to the
/// originating request.
pub mod secure_payload;
/// Shared UDP socket pool used by NAT hole-punching.
///
/// `SocketPool` hands out `SocketRef` references to ephemeral UDP sockets
/// bound on local ports, multiplexing receive of incoming `PEER_HOLEPUNCH`
/// probes through a dedicated channel (`HolepunchEvent`). The
/// [`holepuncher::Holepuncher`] uses this pool to launch the birthday-attack
/// probe sequence required to traverse symmetric NATs. Most consumers use
/// the pool indirectly via [`hyperdht::HyperDhtHandle::connect`]; direct use
/// is only needed for custom DHT-server orchestration or low-level
/// hole-punch experiments.
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
