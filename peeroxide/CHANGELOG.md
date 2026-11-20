# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SwarmConfig::read_buffer_limit`: the most each connection's stream holds for its reader. See Changed.
- **Memory presets for a swarm** (#50): `SwarmConfig::constrained()` (256 KiB each way, 16 peers, an 8 MiB budget) for embedded devices, `mobile()` (1 MiB, 32 peers, 64 MiB) for phones, `balanced()` (the default: 8 MiB, 64 peers) and `throughput()` (32 MiB, 64 peers) for servers. Each sets only those fields. A connection moves at most about 0.94 × the limit ÷ the round trip: over 100 ms, about 2.5, 9.9, 79 and 311 MB/s. See "Choosing limits" in the crate docs.
- `SwarmConfig::memory_budget`: the most all connections' stream buffers hold together. Each connection's stream gets, each way, the lower of its limit and `budget / (2 × max_peers)`, and never less than `MIN_STREAM_LIMIT` (64 KiB); `SwarmConfig::connection_limits` tells what. A blind relay's control connection gets `MIN_STREAM_LIMIT` under a budget. The share is fixed by `max_peers`; sharing it among the peers connected at the moment is #51.
- `SwarmConfig::write_buffer_limit`: the most each connection's stream holds written and not yet acknowledged. See Changed.
- Re-exports `StreamLimits` from `peeroxide-dht`.

### Changed

- **A swarm's connections are given a write buffer limit of 8 MiB by default** (#50), with the read buffer limit (see the next entry). Without one, a connection's stream held every packet it sent until it was acknowledged, as much as the remote's window allowed: a remote that advertised a large window decided how much memory the connection's writes took. Against a remote on Node.js, which advertises 4 MiB, or one on this crate with the default 8 MiB, it changes nothing. `SwarmConfig::write_buffer_limit = None` restores the old behaviour.
- **A swarm's connections are given a read buffer limit of 8 MiB by default** (#38), the ones it makes and the ones it accepts. Without one, a remote could have a connection hold whatever it sent until the reader took it: across the 64 connections a swarm takes by default, the limit bounds the streams' read buffers at 512 MiB. A message being received can take up to 16 MiB more on each connection, which the limit does not cover (see `peeroxide-dht`). A reader that keeps up is not slowed down, since the window 8 MiB leaves, about 8.4 MB, is wider than the 4 MiB a stream without a limit advertises. A remote on Node.js gives up on a connection whose reader stops reading for about thirteen seconds while it is full. `SwarmConfig::read_buffer_limit = None` restores the old behaviour.
- A write to a connection's stream returns once the message is queued for sending, where it returned once the remote had acknowledged it, two round trips later (see `peeroxide-dht` and `libudx`). `shutdown` on the stream returns once everything written has been acknowledged.

## [1.7.4](https://github.com/Rightbracket/peeroxide/compare/peeroxide-v1.7.3...peeroxide-v1.7.4) - 2026-11-02

### Fixed

- *(swarm)* let Node.js clients connect to a firewalled peeroxide server ([#31](https://github.com/Rightbracket/peeroxide/pull/31))

### Fixed

- Node.js Hyperswarm clients can now connect to a firewalled peeroxide server. Before, every attempt timed out (`passive holepunch timed out` here, `HOLEPUNCH_ABORTED` on the Node side). The server side of the handshake now matches Node hyperdht `lib/server.js` on three points:
  - **Duplicate PEER_HANDSHAKE.** A Node client sends one Noise message through up to two relays in parallel and acts on the first reply. Each copy now gets the same reply, cached by a hash of the Noise message for 20 s. Before, each copy got a fresh Noise responder, and the second replaced the first's holepunch session. The cache holds at most 1024 replies (oldest evicted first), expires them in insertion order and on a periodic sweep, and skips handshakes the server rejects (already connected, at `max_peers`, no UDX info), staging no passive holepunch for them when the swarm is full (and releasing one staged for an already-connected peer), so a flood of distinct handshakes cannot grow it or stall the swarm.
  - **Holepunch relays.** The reply's `holepunch.relays[].peer_address` is now our own address as each relay saw it (`HyperDhtHandle::current_relays()`). Before, it held the client's address, so the relay sent the client's PEER_HOLEPUNCH back to the client.
  - **Open or direct clients.** A client that says it is `FIREWALL_OPEN`, or that reached us without a relay, is now dialled by the server: its stream connects to the client's address, so the server sends the first packet and opens its own NAT mapping, as Node's `hs.onsocket(...)` does. A firewalled server still advertises `holepunch { id, relays }` to such a client, as Node does, so an open Node client waits for that dial instead of dialling the firewalled server itself. Before, the open client was given a passive holepunch it never performs.

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
