# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Token-only `ANNOUNCE` refreshes were ignored** (#71). An `ANNOUNCE` carrying a `refresh` token but no peer or signature always got no reply, so it renewed nothing. As Node's `persistent.js` (`_onrefresh`), a signed announce with a `refresh` token now caches its target, self-announce flag and record. A later refresh presenting a preimage of that token renews the cached decision for the cached target only, never the request's `target`, and the cached entry is then stored under the presented token, so each token in the chain works once. The stock Node client and peeroxide's own `announce` don't send refresh tokens, so this is server-side parity with Node and changes nothing for them.

## [1.8.2](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.8.1...peeroxide-dht-v1.8.2) - 2026-11-23

### Fixed

- *(dht)* only a target's own key may take or drop its handshake routing ([#69](https://github.com/Rightbracket/peeroxide/pull/69)) ([#70](https://github.com/Rightbracket/peeroxide/pull/70))
- **Any key could make a server unreachable by announcing under its target** (#69). A node pointed its handshake forwarding for a target at whoever announced under it, so another key announcing under `hash(server_pk)` took the server's `PEER_HANDSHAKE`s on the nodes closest to it and dropped them, and clients, by `join_peer` or by topic, never connected. Likewise, another key's `UNANNOUNCE` under `hash(server_pk)` removed the server's record that `FIND_PEER` and `LOOKUP` return. As Node's `persistent.js` (`announceSelf`), a node now sets handshake forwarding on `ANNOUNCE`, and clears it and the record on `UNANNOUNCE`, only when `hash(peer.public_key) == target`; other announces stay plain records. Reported by @qknight.

## [1.8.1](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.8.0...peeroxide-dht-v1.8.1) - 2026-11-22

### Other

- Retry a failed discovery refresh soon, and recover a DHT node that started offline ([#56](https://github.com/Rightbracket/peeroxide/pull/56)) ([#66](https://github.com/Rightbracket/peeroxide/pull/66))
- Give the first wildcard-bound DHT node a valid node id ([#55](https://github.com/Rightbracket/peeroxide/pull/55)) ([#65](https://github.com/Rightbracket/peeroxide/pull/65))

### Added

- `DhtConfig::public_host` and `DhtConfig::bootstrapper(port, public_host)`, as Node.js `DHT.bootstrapper(port, host)` (#55). A persistent node derives its node id from `public_host` and its bound port, so the first node of a DHT can be bound to `0.0.0.0` (or behind 1:1 NAT) and still be used by other nodes. An unfirewalled one also reports it from `DhtHandle::remote_address` whenever the NAT analysis has none, as Node's bootstrapper does. An invalid `public_host` (not IPv4, or `0.0.0.0`) makes `spawn` fail with the new `DhtError::InvalidConfig`.
- `HyperDhtHandle::lookup_with_replies`, which returns `LookupReplies`: what `lookup` returns, and how many nodes with a valid node id answered, which tells a lookup that reached no node that can hold an announcement from one on a topic no one announces (#56).
- `AnnounceResult::acknowledged`: how many of the nodes an announce reached confirmed they took it. Zero means no node confirmed it (#56).

### Fixed

- **The first persistent node of a DHT, bound to a wildcard host with nothing to bootstrap from, never got a valid node id, and did not say so** (#55). It advertised a random id, which other nodes reject because it doesn't match its address, so they never added it to their routing tables or stored announcements on it: on a DHT of that node and ephemeral clients, `announce` reached no node and `lookup` found nothing. A persistent node bound to a wildcard host now runs as ephemeral (advertises no id, as Node.js `dht-rpc` does) until it knows its address, keeps learning it from the replies to its own requests after bootstrap instead of only during it, queries the bootstrap nodes again with its new id once it has one (as Node's bootstrap does) so that nodes that saw it without one keep it, reports the address from `remote_address` when it's on the bound port, and logs a warning when bootstrap ends without it. Set `DhtConfig::public_host` for such a node. It only takes its address from direct replies to its listening socket with a usable IPv4 address: before, a firewalled node, whose requests leave from another socket, took that socket's port for its id, which other nodes also reject.
- **A node that found no bootstrap node reachable at start fills its routing table once one is** (#56). A query, including the node's own refreshes, asked only the nodes in its routing table, so a node whose table was empty never asked anyone again, unless another node contacted it, and an announce or lookup on it reached no node and returned `Ok` with no result. A query on a table with fewer than k (20) nodes now also asks the bootstrap nodes, as Node's dht-rpc does. A node with an empty table refreshes every 40 s, so it recovers by itself within about that.
- A bootstrap node given by hostname that did not resolve at start, as when DNS was down, is resolved again, at most every 10 s, when a query needs the bootstrap nodes, and asked as soon as it resolves; a query that needs the bootstrap nodes meanwhile asks the nodes it has at once, and each of those hostnames as it resolves, and, if no node has answered it yet, does not finish until each has resolved or failed, as Node's waits for them. It was dropped for good (#56).
- `announce` no longer records a node that refused the announce, or did not answer, as one of the relays (`current_relays`) a self-announce advertises, as Node's `Announcer` does not (#56).

## [1.8.0](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.7.4...peeroxide-dht-v1.8.0) - 2026-11-22

### Other

- Fix four security advisories (remote DoS), and add join_peer ([#61](https://github.com/Rightbracket/peeroxide/pull/61))
- Memory/throughput presets: write buffer limit, StreamLimits, swarm presets and memory budget ([#50](https://github.com/Rightbracket/peeroxide/pull/50)) ([#52](https://github.com/Rightbracket/peeroxide/pull/52))
- Default 8 MiB read buffer limit for untrusted streams; fix mid-stream limit stall and Protomux deadlock ([#44](https://github.com/Rightbracket/peeroxide/pull/44))
- *(libudx)* make streams faster than Node.js (udx-native) ([#36](https://github.com/Rightbracket/peeroxide/pull/36))

### Added

- `examples/secret_transfer.rs` measures how fast a `SecretStream` over a UDX stream carries messages between two programs. Either end can be a Node.js program with `@hyperswarm/secret-stream` on a `udx-native` stream.
- `SecretStream::get_ref` returns the transport a `SecretStream` runs over, without giving the stream up as `into_inner` does. Over a UDX stream that is where the limit on the stream's read buffer is set (`UdxAsyncStream::set_read_buffer_limit`, see `libudx`): on a `PeerConnection`, `connection.stream.get_ref().set_read_buffer_limit(..)`.
- `ConnectOpts::read_buffer_limit` and `DEFAULT_READ_BUFFER_LIMIT` (8 MiB). See Changed.
- **`StreamLimits`, a connection's read and write buffer limits in one value, with presets** (#50): `constrained()` 256 KiB each way, `mobile()` 1 MiB, `balanced()` 8 MiB (the default) and `throughput()` 32 MiB, plus `unlimited()` and `new()`. A connection moves at most about 0.94 × the limit ÷ the round trip in each direction: measured over 100 and 200 ms round trips, 2.5 and 1.2 MB/s, 9.9 and 4.9, 79 and 39.5, and 311 and 153. `ConnectOpts::with_limits` applies one; `ConnectOpts::limits` reads them back.
- `ConnectOpts::write_buffer_limit` and `DEFAULT_WRITE_BUFFER_LIMIT` (8 MiB). See Changed.
- `ConnectOpts::relay_control_limits`: the buffer limits of the control connection to a blind relay, when a connection goes through one. By default it gets the connection's own.
- `Mux::pair`, as Node's `mux.pair()`: a `PairListener` told (`PairRequest`) of the channels the remote opens for a protocol, and optionally an id, that no local channel is waiting for, so that `Mux::accept` can create the channel for it, which fails if that open is no longer waiting, or with the new `ProtomuxError::NotNextOpen` if an earlier open of its protocol and id waits (the remote pairs answers with its opens in order) or the request is of another mux. A listener that does not keep up is not told of more than 64 KiB of them at once. Dropping the listener stops it, as `unpair` does.
- `FramedStream::into_framed_halves`, a provided method that returns a stream as halves that can be read and written at the same time (`FramedHalves`), or the stream itself if it cannot be split, which is the default. `SecretStream` implements it. See Fixed.

### Changed

- **A channel the remote opens that no local channel pairs with is rejected** (see Security). It gets a second for a local channel of its protocol and id to be created, or a `Mux::pair` listener to create one; Node waits for its `pair` callback, or rejects it at once when there is none. Past 64 opens waiting at once, an open is rejected at once, as Node rejects one nothing is registered for. A remote that leaves 64 KiB of the mux's rejects unread has the mux end. A `Mux` handles the commands of its handles before the frames it reads, so a channel created as soon as the mux starts is waiting when the remote's open arrives. The future `Mux::new` returns now needs a Tokio runtime with its timer enabled, and over a stream that cannot be split, `FramedStream::read_frame` must lose nothing when dropped part way, as a command already had it be.
- **A `Mux` frees channel ids as Node does.** A channel the remote closes frees its local id, where it kept it until it was closed on our side too; a `Channel` handle of a closed channel no longer reaches the next channel to get its id. A channel closed on our side before the remote answered its open keeps its id until the answer, open or reject, comes, so that the answer is not taken for the next channel's. A remote's close of a channel it opened that nothing had paired with yet is answered with a reject, as Node answers it, where it went unanswered and the remote's id for it stayed taken. While corked, a `Mux` now batches its opens and closes with its messages, and sends the batch in parts of about 8 MiB (`MAX_BATCH`), as Node does, where they went out ahead of the batch: a close no longer overtakes the channel's last messages, nor an open of a reused id the messages of the channel that had it.

- **The streams of the connections this crate makes are given a write buffer limit of 8 MiB by default** (#50), wherever they are given a read buffer limit (see the next entry), relay service included. Without one, a stream held every packet it sent until it was acknowledged, as much as its remote's window allowed, so a remote that advertised a large window decided how much memory the stream's writes took. Against a remote on Node.js or C libudx, which advertise 4 MiB, and one on this crate with the default 8 MiB, it changes nothing. `ConnectOpts::write_buffer_limit = None` restores the old behaviour, as does `connection.stream.get_ref().set_write_buffer_limit(None)`.
- **The streams of the connections this crate makes are given a read buffer limit of 8 MiB by default** (#38): `connect`, `connect_with_nodes`, `connect_to`, `connect_with_options`, `establish_stream` and `establish_stream_with_socket`, both the data stream and the control connection of a connection through a blind relay, and the control connections a relay server (`relay_service`) accepts. Without one, a remote could have a connection hold whatever it sent until the reader took it, on every connection at once. The limit covers the stream's read buffer only: a `SecretStream` still holds each message whole as it arrives, up to 16 MiB, and packets that carry no data still wait unbounded for the stream's task. The window 8 MiB leaves, about 8.4 MB, is wider than the 4 MiB a stream without a limit advertises, so a reader that keeps up is not slowed down. What changes is that a remote on Node.js gives up on a stream whose reader stops reading for about thirteen seconds while it is full, where before it went on filling memory. `ConnectOpts::read_buffer_limit = None` restores the old behaviour, as does `connection.stream.get_ref().set_read_buffer_limit(None)` on an established connection. No signature changes.
- `SecretStream::write` and `SecretStreamWriteHalf::write` return once their message has been handed to the transport. Over a UDX stream they returned when the remote had acknowledged it, which took two round trips for every message: one for the three bytes of the frame's length, which went out in a packet of their own, and one for the body. A frame's length and body now go to the transport in one write, and a UDX stream no longer waits for acknowledgements (see `libudx`). Through a path of 100 Mbit/s with a 40 ms round trip, a stream of 64 KB messages went from 0.8 MB/s to between 9 and 10 MB/s, and a stream of 1 KB messages from 12 messages a second to about 8 MB/s. Two Node.js programs reach 9.3 to 9.8 and 8.3 to 9.7 MB/s there. `shutdown` returns once the remote has acknowledged everything written: a program that exits after its last message should await it first.
- On Linux, DHT messages and holepunch probes leave marked "don't fragment", as every packet of a `libudx` socket does there now and as Node.js's `dht-rpc` sends them. A message larger than the path carries is dropped where it was fragmented. The largest request, a `mutable_put` of a 1000-byte value, is an IP packet of 1206 bytes.

### Fixed

- The relay service (`relay_service`) takes its stream ids from the crate's one counter (`alloc_stream_id`), where it had its own from 1, while its streams share the DHT server socket with the crate's other connections and a swarm's: two live streams could hold one id, which `libudx` now refuses.
- **A `Mux` over a `SecretStream` reads while it writes.** It wrote everything it had queued before it read anything, so two ends that both sent more than the transport between them holds waited on each other for good: with the read buffer limit that connections now have by default, more than about 8 MiB each way. It now reads and writes the two halves of the stream (`SecretStream::into_split`) on their own. This also stops it from cancelling a read part way through a frame whenever a channel sent a message, which lost the part already read and left the stream out of step with its remote. A `Mux` over a `FramedStream` that cannot be split behaves as before.

- The live test `rust_announce_rust_lookup` announces node A's own key before it checks `current_relay_addresses()`. Only an announce of a node's own key records its relays, since #27; the test announced only a topic, so the list stayed empty and the test failed on every run since.
- The test `pair_and_unpair_reset_idle_clock` no longer depends on a 30 ms sleep ending within 50 ms. It failed in three of five runs on GitHub's macOS runner, where a sleep ends later than that.

### Security

- **A connect no longer waits for good on a peer that answers and goes quiet.** Once the stream is connected, the remote has 20 s to send its half of the stream's setup (`establish_stream`, `establish_stream_with_socket`, and every `connect` method), past the time the transport takes to give up on a lost packet; and a connection through a relay has 15 s from the start of its connection to the relay to the relay's pairing, as Node's `relayTimeout`, a relay reached through another relay included; the relay's given addresses are tried first, and if they have not connected within 5 s, or fail, the relay is also dialled by its key, as Node dials it, and the first to connect is used. Past that the connect fails with `HyperDhtError::StreamEstablishment`, and the connection to the relay is destroyed so that the relay lets go of what it holds for it. A remote that acknowledged what it was sent and sent nothing more held the connect, and a swarm's dial of it, forever: three such peers on a topic stopped a swarm dialling anyone else. `run_server` now destroys the passive puncher of a holepunch round once it has done: a puncher still live when its punch returns, or of a round not asked to punch, answers the client's probes for up to 10 s more, as Node bounds one with `HANDSHAKE_INITIAL_TIMEOUT`. Each round's puncher was kept for good. A `Holepuncher` dropped without `destroy` (a connect given up on mid-punch) now ends its receive tasks, which held its sockets open. A relay service session whose channel never opens now gives its `max_sessions` slot back, when it is dropped or swept as idle (`BlindRelaySession::wait_opened` now ends then); it kept it for good. `BlindRelayServer::release_session` gives a session's slot back once, however often it is called. A connect whose stream setup fails or is given up on now destroys the stream, so that the remote lets go of it at once. The relay service now gives a client 20 s to set up its control stream, then destroys it, and counts a connection against `max_sessions` from its handshake, declining handshakes at capacity, where it set up any number at once; a client that never did held the stream and its task for good. A pairing whose session closes while the relay is still wiring its streams is now torn down (`BlindRelayServer::mark_active` signals the teardown at once); its streams were kept for good.
- **A LOOKUP reply can no longer abort the process** ([GHSA-4jh8-33j3-h73j](https://github.com/Rightbracket/peeroxide/security/advisories/GHSA-4jh8-33j3-h73j)). Its decoder allocated room for as many peers as the reply claimed before it checked that the reply held them, so any DHT node a lookup reached could claim 2^40 peers in a few bytes and abort the process, or panic it with a capacity overflow: every swarm client and `peeroxide lookup` was exposed. A reply that claims more than 1,048,576 peers, or more than its bytes can hold, is now an error (`EncodingError::ArrayTooLarge` or `OutOfBounds`). Every other array decoder (`decode_uint_array`, `decode_buffer_array`, `decode_string_array`, and the arrays of peers and relays in DHT messages) now also allocates no more items than its bytes can hold. Reported by Joachim Schiele (@qknight).
- **A `SecretStream` no longer allocates a frame's declared length before the peer is authenticated** ([GHSA-pchg-rmv6-56f4](https://github.com/Rightbracket/peeroxide/security/advisories/GHSA-pchg-rmv6-56f4)). Each frame of the handshake allocated the length its first three bytes declared, up to 16 MiB, before the peer had proven anything, and held it for as long as the peer took to send the body. A frame of the Noise handshake or the ID header that declares more than 4 KiB now fails the handshake with `SecretStreamError::HandshakeFrameTooLarge`; none is more than a few hundred bytes. After the handshake, a frame's buffer starts at 64 KiB at most and doubles as the body arrives, so a peer holds memory in proportion to what it has sent, not to what it declared. Reported by Joachim Schiele (@qknight).
- **A remote can no longer make a `Mux` allocate a channel list of any length with one frame** ([GHSA-m2w2-wg2f-5p57](https://github.com/Rightbracket/peeroxide/security/advisories/GHSA-m2w2-wg2f-5p57)). A remote that opened a channel at id `0xFFFF_FFFF` had the mux grow its list of remote channels to that length, about 240 GB, and the process aborted. One frame did it: a blind relay (`relay_service`) runs a `Mux` for every client, and a relay one for each client that goes through it. As in Node's `protomux`, a remote now opens each channel at an id it has used or at the next one, and any other id is an error that ends the mux. A reject for a channel that was never opened, id 0 included, is now ignored where it panicked the mux's task, and a batch item that declares a length near `usize::MAX` is an error where its bounds check overflowed. Control frames that a corked Node peer sends in a batch (opens, closes, rejects) are now handled, as Node handles them, where they were dropped. As in Node, a reject now closes only a channel whose open is still waiting for its pair, and a channel closed before its open is answered can no longer be paired with the next channel to use its id. A remote could also have a `Mux` hold a channel for every open it sent that nothing paired with, with its id and handshake, and every message sent on one, for good, and a slot for every channel id it had ever used. As in Node, an open nothing pairs with is now rejected (see Changed), and the mux stops reading while it holds more than 32 KiB for channels not yet paired, their opens and messages (Node's `MAX_BUFFERED`), part way through a batch if need be; over a stream that can be split, the frames read ahead of the mux and waiting for it are held to 1 MiB, or one larger frame, besides the frame being read, where they were 16 frames of any size. Ids the remote is done with take no room. A channel closed on our side now frees the remote's id for it at once, as Node does: the remote's next open at that id was taken for a duplicate and ended the mux, and its messages on the closed channel were held.

## [1.7.4](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.7.3...peeroxide-dht-v1.7.4) - 2026-11-02

### Fixed

- *(swarm)* let Node.js clients connect to a firewalled peeroxide server ([#31](https://github.com/Rightbracket/peeroxide/pull/31))

### Added

- `HyperDhtHandle::current_relays()`: the relays of the last self-announce, each paired with our own address as that relay saw it (`RelayInfo { relay_address, peer_address }`), mirroring Node hyperdht's `Announcer.relays`. A server advertises these in the `holepunch.relays` of its handshake reply; the relay forwards a client's PEER_HOLEPUNCH to `peer_address`. `current_relay_addresses()` is unchanged.
- `QueryReply::to`: our own address as the responding node saw it (the `to` field of its reply).

## [1.7.3](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.7.2...peeroxide-dht-v1.7.3) - 2026-07-12

### Other

- full documentation audit (rustdoc + mdBook) ([#28](https://github.com/Rightbracket/peeroxide/pull/28))

## [1.7.2](https://github.com/Rightbracket/peeroxide/compare/peeroxide-dht-v1.3.1...peeroxide-dht-v1.7.2) - 2026-07-12

### Added

- *(secret-stream)* add SecretStream::into_split() for owned read/write halves ([#26](https://github.com/Rightbracket/peeroxide/pull/26))

### Other

- NAT holepunch hardening + blind-relay server implementation ([#27](https://github.com/Rightbracket/peeroxide/pull/27))

### Added

- `BlindRelayServer`/`BlindRelaySession` (`blind_relay` module): a real blind-relay server implementation. Previously peeroxide only implemented the blind-relay *client* side; this adds the pairing-table/session-limit/stats engine mirroring Node's `blind-relay` `Server`/`BlindRelaySession`/`BlindRelayPair`.
- `BlindRelayServerConfig`: configurable `max_sessions`, `max_pairings_per_session`, `pairing_timeout`, `idle_session_timeout`. Node's reference `blind-relay` has none of these; defaults are generous so a default relay behaves like the unthrottled Node reference in practice.
- `relay_service::run_relay_server`: standalone accept-and-bridge entry point wiring `BlindRelayServer` to real UDX transport (`UdxStream::relay_to` for blind, packet-level forwarding). Deliberately independent of `peeroxide::Swarm`.
- Idle-session-timeout enforcement and stream teardown on unpair/session-close for active pairings (peeroxide-specific hardening; the latter has 1:1 precedent in Node's `blind-relay` `_onclose`/`_onunpair`).
- `hyperdht::alloc_stream_id()`: exposes the same stream-id counter used internally by `connect_to`, for callers (e.g. `peeroxide::swarm`) that need to allocate a stream id on the same counter as an existing control connection.

### Fixed

- The relay service now self-announces its own identity (`hash(public_key)`) to the DHT at startup and on a periodic refresh, mirroring Node's `Server.listen()` (which internally starts an `Announcer` for the server's own target). Without this, `register_server` alone only made the relay answer inbound `PEER_HANDSHAKE` requests locally — nothing told the rest of the network the relay existed, so a Node.js `hyperdht` client's `dht.connect(pubkey)` (which resolves candidates via a LOOKUP-style `findPeer` query for an announced record, not a raw `FIND_NODE` walk) could never discover it and failed with `PEER_NOT_FOUND`.
- `RelayStats` is now `#[non_exhaustive]`, matching its `RelayStatsSnapshot` counterpart and the crate's Event/Result visibility policy (`VISIBILITY_POLICY.md`), so future counter fields can be added without a breaking change.

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
