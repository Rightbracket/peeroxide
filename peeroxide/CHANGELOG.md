# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.3](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.7.2...peeroxide-v1.7.3) - 2026-07-12

### Other

- full documentation audit (rustdoc + mdBook) ([#28](https://github.com/Rightbracket/peeroxide/pull/28))

## [1.7.2](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.3.1...peeroxide-v1.7.2) - 2026-07-12

### Other

- NAT holepunch hardening + blind-relay server implementation ([#27](https://github.com/Rightbracket/peeroxide/pull/27))

### Fixed

- Fixed a cross-crate UDX stream-id collision in `swarm.rs`'s `create_server_relay_connection`: it reused a `local_stream_id` allocated from `peeroxide`'s own stream-id counter for the blind-relay data stream, but the control connection to the relay (established via `peeroxide-dht`'s `connect_to`) allocates from a separate counter — both starting at 1 independently, they could collide on the same socket and silently clobber the control channel's UDX demux registration. Now uses the new `peeroxide_dht::hyperdht::alloc_stream_id()` to allocate from the same counter `connect_to` uses.

## [1.3.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.2.0...peeroxide-v1.3.0) - 2026-05-14

### Other

- peeroxide-cli 0.2.0: chat subsystem, init command, dd v2 tree protocol, progress UX ([#15](https://github.com/Rightbracket/peeroxide/pull/15))

### Changed

- Bumped `peeroxide-dht` dependency from 1.2.0 to 1.3.0. This update adds new public wire-byte counter accessors to `HyperDhtHandle` and `DhtHandle`. See `peeroxide-dht/CHANGELOG.md` for the full list of new additive symbols.

## [1.2.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.1.0...peeroxide-v1.2.0) - 2026-04-30

### Added

- `SwarmHandle::dht()` — exposes the underlying `HyperDhtHandle` for low-level DHT operations such as mutable/immutable record storage and manual peer lookup.
- `SwarmHandle::key_pair()` — exposes the Ed25519 key pair identifying this swarm node, for use with DHT mutable records or other identity operations.
- Re-exported `HyperDhtHandle`, `MutablePutResult`, `MutableGetResult`, and `ImmutablePutResult` from the `peeroxide` crate root, so callers no longer need to depend on `peeroxide-dht` directly for common DHT storage types.

### Changed

- Swarm nodes now self-announce under `hash(publicKey)` during each discovery refresh. This populates `ForwardEntry` records on the nodes closest to the peer's public key, enabling `PEER_HANDSHAKE` routing to work correctly — matching the behaviour of the Node.js reference implementation.
- Server registrations are now cleaned up properly on `leave()` (when the last server topic is left), on `destroy()`, and when the swarm handle is dropped. Previously, `ForwardEntry` records for the local server could persist in the router until TTL expiry.
- Incoming server connections now reuse the DHT's bound listen socket for the UDX stream, rather than creating a new socket. This ensures streams arrive on the same port that remote peers have on record, fixing connection establishment in NAT environments.
- Handshake replies no longer echo the client's address back in `peer_address`; the field is now correctly set to `None` in the reply, matching the wire protocol.

## [1.1.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.0.1...peeroxide-v1.1.0) - 2026-04-28

### Other

- Add #[non_exhaustive] to public structs and enums ([#10](https://github.com/Rightbracket/peeroxide/pull/10))

## [1.0.1](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.0.0...peeroxide-v1.0.1) - 2026-04-26

### Other

- Add doc comments to all public API items and enforce deny(missing_docs) ([#2](https://github.com/Rightbracket/peeroxide/pull/2))

## [1.0.0] - 2025-04-25

Initial release. Pure Rust implementation of the Hyperswarm P2P networking
stack, wire-compatible with the existing Node.js network.

### Added

- Topic-based peer discovery and connection management (Hyperswarm port)
- `SwarmConfig` with configurable max peers, firewall callback, backoff, and jitter
- `spawn()` returns `(JoinHandle, SwarmHandle, Receiver<SwarmConnection>)`
- `SwarmHandle`: `join()`, `leave()`, `status()`, `destroy()`
- Connection deduplication by remote public key
- Peer state machine with priority scoring and exponential retry backoff
- `SwarmConfig::with_public_bootstrap()` for connecting to the live HyperDHT network
- `discovery_key()` helper for topic hashing

### Tested

- 24 unit tests passing
- 3 integration tests passing
- All tests verified against Node.js Hyperswarm reference behaviour

### Dependencies

- `peeroxide-dht` — HyperDHT implementation
- `libudx` — reliable UDP transport
- `tokio` — async runtime
- `tracing`, `thiserror` — logging and error handling
- `rand` — ephemeral keypair generation

### Compatibility

- Wire-compatible with Node.js Hyperswarm
- Rust edition 2024, MSRV 1.85
- Dual-licensed: MIT OR Apache-2.0

[1.0.0]: https://github.com/Rightbracket/peeroxide/releases/tag/v1.0.0
