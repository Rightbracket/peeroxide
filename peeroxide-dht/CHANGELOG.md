# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `BlindRelayServer`/`BlindRelaySession` (`blind_relay` module): a real blind-relay server implementation. Previously peeroxide only implemented the blind-relay *client* side; this adds the pairing-table/session-limit/stats engine mirroring Node's `blind-relay` `Server`/`BlindRelaySession`/`BlindRelayPair`.
- `BlindRelayServerConfig`: configurable `max_sessions`, `max_pairings_per_session`, `pairing_timeout`, `idle_session_timeout`. Node's reference `blind-relay` has none of these; defaults are generous so a default relay behaves like the unthrottled Node reference in practice.
- `relay_service::run_relay_server`: standalone accept-and-bridge entry point wiring `BlindRelayServer` to real UDX transport (`UdxStream::relay_to` for blind, packet-level forwarding). Deliberately independent of `peeroxide::Swarm`.
- Idle-session-timeout enforcement and stream teardown on unpair/session-close for active pairings (peeroxide-specific hardening; the latter has 1:1 precedent in Node's `blind-relay` `_onclose`/`_onunpair`).
- `hyperdht::alloc_stream_id()`: exposes the same stream-id counter used internally by `connect_to`, for callers (e.g. `peeroxide::swarm`) that need to allocate a stream id on the same counter as an existing control connection.

### Fixed

- The relay service now self-announces its own identity (`hash(public_key)`) to the DHT at startup and on a periodic refresh, mirroring Node's `Server.listen()` (which internally starts an `Announcer` for the server's own target). Without this, `register_server` alone only made the relay answer inbound `PEER_HANDSHAKE` requests locally — nothing told the rest of the network the relay existed, so a Node.js `hyperdht` client's `dht.connect(pubkey)` (which resolves candidates via a LOOKUP-style `findPeer` query for an announced record, not a raw `FIND_NODE` walk) could never discover it and failed with `PEER_NOT_FOUND`.

## [1.4.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.3.1...peeroxide-dht-v1.4.0) - 2026-05-18

### Added

- `peeroxide_dht::State` re-export at the crate root (alongside the existing `EncodingError` re-export). The compact-encoding `State` type was used in many public encode/decode function signatures; the re-export makes it nameable from out-of-crate consumers without depending on the `compact_encoding` module path.
- `peeroxide_dht::QueryReply` and `peeroxide_dht::QueryResult` re-exports at the crate root. These types are returned by `DhtHandle::find_node` / `DhtHandle::query`; the re-exports preserve external reachability after the `query` module was demoted to `pub(crate)`.
- `peeroxide_dht::{Router, HandshakeAction, HolepunchAction, ForwardEntry, HandshakeResult, HolepunchResult, RouterError}` re-exports at the crate root. These types are returned by `HyperDhtHandle::router()` and related public methods; the re-exports preserve external reachability after the `router` module was demoted to `pub(crate)`.
- `WireCounters::from_counters(bytes_sent, bytes_received)` constructor. Allows building a `WireCounters` snapshot from externally-owned `Arc<AtomicU64>` counters (e.g. a CLI progress reporter that already holds atomic byte counters wants a `WireCounters` view sharing those atomics).
- Module-level documentation for `holepuncher`, `io`, `peer`, `persistent`, `secretstream`, `secure_payload`, `socket_pool`, which are now documented public API rather than `#[doc(hidden)]`.

### Changed

- `holepuncher`, `io`, `peer`, `persistent`, `secretstream`, `secure_payload`, `socket_pool` are now documented public modules. They were previously `#[doc(hidden)] pub mod` (publicly reachable but absent from rustdoc); the new state is fully public and intended as the advanced-use surface for consumers building custom DHT clients, server nodes, or hole-punch orchestration.
- `compact_encoding`, `nat`, `query`, `router`, `routing_table` are now `pub(crate)`. They were previously `pub` (some `#[doc(hidden)]`); none had cross-crate usage beyond the types now re-exported at the crate root (`EncodingError`, `State`, `QueryReply`, `QueryResult`, `Router` family) or the `peeroxide` integration test surface. Their items are accessible via the re-exports listed above where needed; direct `peeroxide_dht::<module>::*` paths are no longer reachable.
- `Holepuncher.nat: Nat` field demoted from `pub` to `pub(crate)`. The `nat` module is now `pub(crate)`; the field continues to be read by internal callers but is no longer reachable from outside the crate.
- `blind_relay::encode_pair`, `encode_unpair`, `decode_pair`, `decode_unpair`, `preencode_pair`, `preencode_unpair`, `encode_pair_to_vec`, `encode_unpair_to_vec`, `decode_pair_from_slice`, `decode_unpair_from_slice` demoted from `pub` to `pub(crate)`. These helpers are used internally by `BlindRelayClient` and have no documented external consumers.
- Applied `#[non_exhaustive]` to additional Config / Result / Event types whose role in the public API admits forward-compatible field/variant additions: `io::WireCounters`, `io::IoConfig`, `io::IoStats`, `io::IoEvent`, `io::TimeoutEvent`, `protomux::Channel`, `protomux::Mux`, `blind_relay::PairResponse`, `noise::HandshakeResult`, `socket_pool::HolepunchEvent`. Out-of-crate consumers can no longer use struct-literal construction or exhaustive enum matches on these types; readers and pattern-matching with a wildcard arm continue to work.

## [1.3.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.2.0...peeroxide-dht-v1.3.0) - 2026-05-13

### Added

- `WireCounters` struct — provides atomic, shareable counters for tracking total bytes sent and received. Includes `new()` for initialization and `snapshot()` for retrieving current totals.
- `Io::wire` field — public access to the IO layer's `WireCounters`.
- `Io::wire_counters()` — returns a handle to the IO layer's wire byte counters.
- `DhtHandle::wire_stats()` — returns a snapshot of cumulative wire bytes `(sent, received)` for the DHT node.
- `DhtHandle::wire_counters()` — returns a handle to the node-wide `WireCounters`.
- `HyperDhtHandle::wire_stats()` — returns a snapshot of total wire bytes processed by the DHT.
- `HyperDhtHandle::wire_counters()` — returns a handle to the shared wire byte counters for the running instance.

## [1.2.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.1.0...peeroxide-dht-v1.2.0) - 2026-04-30

### Added

- `DhtHandle::table_id()` — returns the node's current routing table ID; useful for server nodes that need to derive their own `NodeId` after bootstrapping.
- `DhtHandle::server_socket()` — returns a shared `Arc<UdxSocket>` for the primary socket, enabling UDX stream multiplexing by callers.
- `DhtHandle::listen_socket()` — returns a shared `Arc<UdxSocket>` for the socket bound to the advertised port, used for inbound UDX stream connections.
- `PersistentStats` struct with `records`, `record_topics`, `mutables`, `immutables`, and `router_entries` fields; returned by the new `stats()` method on node server handles.
- `RoutingTable::rebuild_with_id()` — rebuilds the routing table under a new node ID, mirroring the Node.js `_updateNetworkState` rebuild in `dht-rpc`.
- `SecretStream::shutdown()` — gracefully closes the write half of a secret stream, sending a FIN to the remote peer.
- `PingResponse` now includes `to` (reflexive address as seen by the remote) and `closer_nodes` (closer nodes returned by the remote's routing table).

### Changed

- Router forward-entry TTL corrected from 30 seconds to 20 minutes, matching the Node.js HyperDHT reference implementation. Entries for running servers (`has_server = true`) no longer expire via TTL or GC — they persist until the server is explicitly unregistered.
- Bootstrap ping now uses `CMD_FIND_NODE` with the local node's table ID as the target, instead of `CMD_PING` with no target. This causes bootstrap nodes to return closer nodes, accelerating routing table population.
- Non-ephemeral nodes with a known public address now derive a deterministic node ID from `hash(host, port)` at spawn time, matching Node.js DHT identity behaviour. Nodes bound to a wildcard address instead collect reflexive address samples during bootstrapping and update their ID once consensus is reached.
- Announce handler now populates a `ForwardEntry` in the router for newly-seen peers (when no server entry already exists), so inbound `PEER_HANDSHAKE` requests can be relayed to recently-announced peers even before they connect.

## [1.1.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.0.1...peeroxide-dht-v1.1.0) - 2026-04-28

### Other

- Add #[non_exhaustive] to public structs and enums ([#10](https://github.com/Rightbracket/peeroxide/pull/10))

## [1.0.1](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.0.0...peeroxide-dht-v1.0.1) - 2026-04-26

### Other

- Add doc comments to all public API items and enforce deny(missing_docs) ([#2](https://github.com/Rightbracket/peeroxide/pull/2))

## [1.0.0] - 2025-04-25

Initial release. Pure Rust implementation of HyperDHT, wire-compatible with
the existing Node.js network.

### Added

- Full HyperDHT implementation (Kademlia DHT, hole-punching, blind relay)
- Noise XX and Noise IK handshake patterns (Ed25519 DH, ChaChaPoly)
- SecretStream transport (pure-Rust libsodium `crypto_secretstream_xchacha20poly1305`)
- Protomux channel multiplexer (actor model, ~1460 lines)
- Blind relay client for NAT traversal
- Compact encoding (all types from the Node.js `compact-encoding` package)
- Server-side record storage with LRU+TTL eviction
- `HyperDhtHandle` client API: `lookup`, `announce`, `find_peer`, `unannounce`, `immutable_put/get`, `mutable_put/get`, `connect`, `register_server`
- NAT classification (OPEN/CONSISTENT/RANDOM/UNKNOWN)
- Socket pool with multi-socket management
- Holepunch strategy selection (direct, birthday paradox)
- Async DNS resolution for bootstrap nodes
- `HyperDhtConfig::with_public_bootstrap()` for live network use

### Tested

- 314 unit tests passing
- 66 integration tests passing (protocol handshakes, DHT queries, relay, holepunch)
- 6 live network tests (ignored by default — require public HyperDHT bootstrap connectivity)
- Golden byte fixtures verified against Node.js HyperDHT, dht-rpc, and hyperswarm-secret-stream reference implementations
- Live cross-language interop tests (Rust ↔ Node.js) at every protocol layer

### Dependencies

- `libudx` — reliable UDP transport
- `tokio` — async runtime
- `tracing`, `thiserror` — logging and error handling
- Pure Rust crypto stack (RustCrypto): `blake2`, `ed25519-dalek`, `curve25519-dalek`, `sha2`, `chacha20poly1305`, `chacha20`, `poly1305`, `xsalsa20poly1305`
- `rand` — key generation

### Compatibility

- Wire-compatible with Node.js HyperDHT and dht-rpc
- Rust edition 2024, MSRV 1.85
- Dual-licensed: MIT OR Apache-2.0

[1.0.0]: https://github.com/Rightbracket/peeroxide/releases/tag/v1.0.0
