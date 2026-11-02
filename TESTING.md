# Peeroxide Testing

## Prerequisites

| Tool      | Min Version | Purpose                          |
|-----------|-------------|----------------------------------|
| Rust      | 1.85        | Workspace builds (edition 2024)  |
| Node.js   | 18+         | Golden fixture generation, live interop peers |
| npm       | 9+          | Install Node.js test dependencies |
| Docker    | 20+         | NAT simulation tests (hole-punching) |

### First-time setup

```bash
# Install Node.js test dependencies
cd peeroxide/tests/node && npm install && cd -
```

## Quick Reference

```bash
# Run all unit tests (300 tests: 279 peeroxide-dht + 21 peeroxide)
cargo test --workspace --lib

# Run all golden fixture tests (no Node.js runtime needed)
cargo test --workspace --test '*golden*'

# Run all live interop tests (requires Node.js)
cargo test --workspace --test '*interop*' -- --exclude '*golden*'

# Run everything
cargo test --workspace

# Run live network tests (requires internet; ignored by default)
cargo test -p peeroxide-dht --test live_bootstrap -- --ignored
cargo test -p peeroxide-dht --test live_announce_lookup -- --ignored

# Clippy (must pass clean)
cargo clippy --workspace --all-targets -- -D warnings
```

## Gate Scripts

Each test suite has a canonical gate script under `tests/`.
These scripts regenerate fixtures, run all tests for that suite, and exit
non-zero on any failure.

```bash
bash tests/run_compact_encoding.sh
bash tests/run_libudx.sh
bash tests/run_dht_rpc.sh
bash tests/run_hyperdht.sh
bash tests/run_encrypted_connections.sh
bash tests/run_hole_punching.sh
bash tests/run_relay.sh
bash tests/run_hyperswarm.sh
```

## Test Suites

### Foundation (compact-encoding)

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests | `cargo test -p peeroxide-dht --lib compact_encoding` | Unit | ~30 |
| Golden fixtures | `cargo test -p peeroxide-dht --test golden_interop` | Golden | 24 |

**Fixtures**: `tests/interop/golden-fixtures.json`
**Generator**: `node tests/node/generate-golden.js`

Passing result: all 24 golden tests pass, byte-identical encoding/decoding against Node.js `compact-encoding`.

### libudx

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests | `cargo test -p libudx --lib` | Unit | 48 |
| Live interop | `cargo test -p libudx --test udx_interop` | Live | 1 |

**Node helper**: `tests/node/udx-echo-server.js` (spawned automatically by test)

Passing result: Rust UDX stream sends data to a Node.js `udx-native` echo server and receives it back identically. Bidirectional.

### DHT-RPC

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests | `cargo test -p peeroxide-dht --lib` (rpc/messages/etc) | Unit | ~40 |
| Golden fixtures | `cargo test -p peeroxide-dht --test dht_golden_interop` | Golden | 3 |
| Live interop | `cargo test -p peeroxide-dht --test dht_interop` | Live | 1 |

**Fixtures**: `tests/interop/dht-rpc-fixtures.json`
**Generator**: `node tests/node/generate-dht-golden.js`
**Node helper**: `tests/node/dht-rpc-server.js` (spawned automatically)

Passing result: wire encoding matches Node.js `dht-rpc`, Rust node bootstraps against a JS node and successfully pings it.

### HyperDHT Operations

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Golden fixtures | `cargo test -p peeroxide-dht --test hyperdht_golden_interop` | Golden | 7 |
| Live interop | `cargo test -p peeroxide-dht --test hyperdht_interop` | Live | 1 |

**Fixtures**: `tests/interop/hyperdht-fixtures.json`
**Generator**: `node tests/node/generate-hyperdht-golden.js`
**Node helper**: `tests/node/hyperdht-peer.js` (spawned automatically)

Passing result: HyperDHT message encoding matches Node.js `hyperdht`, cross-language announce/lookup discovers peers.

### Encrypted Connections

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests | `cargo test -p peeroxide-dht --lib noise secretstream secret_stream` | Unit | ~17 |
| Golden fixtures | `cargo test -p peeroxide-dht --test noise_golden_interop` | Golden | 5 |
| Live interop | `cargo test -p peeroxide-dht --test secret_stream_interop` | Live | 1 |

**Fixtures**: `tests/interop/noise-fixtures.json`
**Generator**: `node tests/node/generate-noise-golden.js`
**Node helper**: `tests/node/secret-stream-server.js` (spawned automatically by live interop test)

Passing result: Ed25519 DH, Noise XX handshake, and secretstream encryption/decryption all produce byte-identical output to Node.js `noise-handshake` + `sodium-secretstream`. Live interop confirms Rust initiator can establish encrypted bidirectional communication with Node.js `@hyperswarm/secret-stream` responder.

### Hole-Punching

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests (nat) | `cargo test -p peeroxide-dht --lib nat` | Unit | 15 |
| Unit tests (socket_pool) | `cargo test -p peeroxide-dht --lib socket_pool` | Unit | 5 |
| Unit tests (router) | `cargo test -p peeroxide-dht --lib router` | Unit | 14 |
| Unit tests (holepuncher) | `cargo test -p peeroxide-dht --lib holepuncher` | Unit | 6 |
| Unit tests (noise_wrap) | `cargo test -p peeroxide-dht --lib noise_wrap` | Unit | 6 |
| Unit tests (secure_payload) | `cargo test -p peeroxide-dht --lib secure_payload` | Unit | 9 |
| Golden fixtures (IK) | `cargo test -p peeroxide-dht --test noise_ik_golden_interop` | Golden | 3 |
| Golden fixtures (secure_payload) | `cargo test -p peeroxide-dht --test secure_payload_golden_interop` | Golden | 3 |
| Integration (2-node handshake) | `cargo test -p peeroxide-dht --test hyperdht_connect_interop` | Live | 1 |
| Docker NAT simulation | `bash tests/docker/run-nat-test.sh` | Docker | (manual) |

**Fixtures**: `tests/interop/noise-ik-fixtures.json`, `tests/interop/secure-payload-fixtures.json`
**Generator**: `node tests/node/generate-noise-ik-golden.js` (secure-payload fixtures are pre-generated)

Passing result: Noise IK handshake, NoiseWrap, holepunch secret derivation, and secure payload encrypt/decrypt all produce byte-identical output to Node.js. Two Rust HyperDHT nodes complete a full Noise IK handshake via PEER_HANDSHAKE command. NAT simulation tests require Docker with Linux containers.

### Relay

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests (protomux) | `cargo test -p peeroxide-dht --lib protomux` | Unit | 12 |
| Unit tests (blind_relay) | `cargo test -p peeroxide-dht --lib blind_relay` | Unit | 8 |
| Unit tests (establish_stream)| `cargo test -p peeroxide-dht --lib establish_stream` | Unit | 4 |
| Golden fixtures (protomux) | `cargo test -p peeroxide-dht --test protomux_golden_interop` | Golden | 7 |
| Golden fixtures (blind-relay)| `cargo test -p peeroxide-dht --test blind_relay_golden_interop`| Golden | 3 |
| Live interop (protomux) | `cargo test -p peeroxide-dht --test protomux_interop` | Live | 1 |

**Fixtures**: `tests/interop/protomux-fixtures.json`, `tests/interop/blind-relay-fixtures.json`
**Generator**: `node tests/node/generate-protomux-golden.js`, `node tests/node/generate-blind-relay-golden.js`
**Node helper**: `tests/node/protomux-interop-peer.js` (spawned automatically)

Passing result: Protomux framing and blind-relay control messages are byte-identical to Node.js. Protomux actor correctly handles channel multiplexing, pairing, and unpairing. Live interop confirms Rust and Node.js can multiplex data over a SecretStream using Protomux.

### Hyperswarm

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Unit tests (peeroxide) | `cargo test -p peeroxide --lib` | Unit | 29 |
| Live interop | `cargo test -p peeroxide --test hyperswarm_interop` | Live | 4 |
| Server handshake | `cargo test -p peeroxide --test server_handshake_dedup` | Integration | 5 |

**Node helper**: `tests/node/hyperswarm-server.js` (spawned automatically by live interop test). Flags: `--remote-client` (the Node client skips the same-host LAN shortcut and reports `FIREWALL_UNKNOWN`, as a client behind a NAT on another network would; one more DHT node is started so it can classify its NAT) `--open-client` (the Node client's DHT node is reachable, like a peer with a public address, but ephemeral, so it reaches the server through relays) and `--direct-client` (persistent, reporting `FIREWALL_UNKNOWN` and skipping the LAN shortcut: on this small testnet it stores the server's announce and sends its handshake straight to the server; through a relay it would have to holepunch). A `stats` command returns the Node DHT's punch counters (`punches.open` / `consistent` / `random`), which the tests use to check which side dialled.

Passing result:
- `hyperswarm_cross_language_connect`: Rust Hyperswarm client connects to a Node.js `hyperswarm` server over the DHT. Full flow: DHT bootstrap → topic announce/lookup → PEER_HANDSHAKE → skip-holepunching (same-host) → UDX stream → SecretStream encrypted channel → receive "hello from node". Verifies end-to-end interoperability of the complete Hyperswarm stack.
- `hyperswarm_node_client_holepunches_to_rust_server`: a firewalled Node.js client connects to a firewalled Rust server. The test first waits for the server to classify itself as firewalled (`CONSISTENT`/`RANDOM`). The client sends one PEER_HANDSHAKE through two relays in parallel, then holepunches through the relays the server advertised (Node's punch counters confirm a holepunch and no direct dial); data flows both ways.
- `hyperswarm_open_node_client_connects_to_rust_server`: a reachable Node.js client reaches the firewalled Rust server through relays, sees an advertised holepunch, and waits for the server to dial it, as a Node server would. Node's punch counters confirm the client neither dialled nor holepunched; on loopback a client dial would also succeed, so this is what shows the server dialled.
- `hyperswarm_direct_node_client_connects_to_rust_server`: a persistent Node.js client that reports `FIREWALL_UNKNOWN` and skips the LAN shortcut sends its handshake straight to the Rust server (no relay); both sides dial and data flows both ways. Node's punch counters confirm no holepunch, which a relayed attempt would have needed.
- `server_handshake_dedup::duplicate_peer_handshake_gets_the_same_reply`: a Rust client sends the same PEER_HANDSHAKE twice straight to a swarm server and gets byte-identical replies.
- `server_handshake_dedup::duplicate_through_two_relays_keeps_the_staged_holepunch`: the same PEER_HANDSHAKE reaches a firewalled swarm server through two of its relays. Both copies get the same reply, and a PEER_HOLEPUNCH for the first reply's id is still answered, so the second copy did not replace the staged holepunch session.
- `server_handshake_dedup::handshake_at_max_peers_is_not_cached_or_staged`: a firewalled server with `max_peers = 0` answers each relayed copy with a fresh reply (nothing cached) and stages no holepunch.
- `server_handshake_dedup::handshake_from_connected_peer_releases_its_staged_holepunch`: a new relayed handshake from an already-connected peer is staged (the server only learns the peer's key from Noise), then released: its reply is not cached and a PEER_HOLEPUNCH for its id gets no answer.
- `server_handshake_dedup::rust_client_connects_directly_to_firewalled_server`: a Rust client sends its handshake straight to a firewalled swarm server; the server dials it and data flows both ways (guards Rust to Rust direct connections against the server-side dial).

### Live Network Validation

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Live bootstrap | `cargo test -p peeroxide-dht --test live_bootstrap -- --ignored` | Live | 1 |
| Live announce/lookup | `cargo test -p peeroxide-dht --test live_announce_lookup -- --ignored` | Live | 2 |

**Node helper**: `tests/node/hyperdht-live-peer.js` (spawned by cross-language test)

### Cross-Machine Connection (Manual Examples)

| Tool | Command | Description |
|------|---------|-------------|
| Relay soak | `RUST_LOG=info cargo run --example relay_soak -p peeroxide-dht` | Long-running DHT node logging relay traffic |
| Rust announce | `cargo run --example swarm_announce -p peeroxide [-- <topic>]` | Announce + echo server |
| Rust join | `cargo run --example swarm_join -p peeroxide -- <topic> [msg]` | Join + send + receive echo |
| Node announce | `node tests/node/hyperswarm-announce.js [topic]` | Node.js announce counterpart |
| Node join | `node tests/node/hyperswarm-join.js <topic> [msg]` | Node.js join counterpart |

**Cross-machine test procedure:**
1. Machine A: `cargo run --example swarm_announce -p peeroxide` — note the printed topic hex
2. Machine B: `cargo run --example swarm_join -p peeroxide -- <topic>` — should connect and echo
3. Or use Node.js on either side for cross-language testing

### UDX Relay + Encrypted Relay Tests

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| UDX relay bidirectional | `cargo test -p libudx --test stream_relay` | Integration | 1 |
| Encrypted relay (same-host) | `cargo test -p peeroxide-dht --test relay_encrypted` | Integration | 1 |

**UDX relay test** (`stream_relay`): Creates 4 streams on one UdxRuntime. Two relay streams forward packets between two peer streams using `UdxStream::relay_to()`. Verifies bidirectional data flow through the relay.

**Encrypted relay test** (`relay_encrypted`): Full end-to-end proof of the blind-relay mechanism on a single machine:
1. Noise IK handshake in-memory → derive session keys
2. UDX relay on localhost (3 separate UdxRuntimes: relay, peer_a, peer_b)
3. `SecretStream::from_session()` wraps relayed UDX streams with the Noise session keys
4. Bidirectional encrypted messages + 10-message burst test
5. Proves the relay never sees plaintext (blind relay principle)

This is the exact mechanism used by Hyperswarm's blind-relay in production — `udx_stream_relay_to()` for kernel-level packet forwarding + SecretStream for end-to-end encryption.

### libudx Native Rust Rewrite

Pure Rust UDX protocol implementation replacing C FFI bindings. All tests now run against the native backend (no feature flags needed).

| Suite | Command | Type | Count |
|-------|---------|------|-------|
| Wire format | `cargo test -p libudx --test wire_format` | Unit | 8 |
| Socket | `cargo test -p libudx --test socket` | Unit | 3 |
| Stream lifecycle | `cargo test -p libudx --test stream_lifecycle` | Integration | 9 |
| Stream interop | `cargo test -p libudx --test stream_interop` | Live | 2 |
| Async stream | `cargo test -p libudx --test async_stream` | Integration | 6 |
| Multiplexing | `cargo test -p libudx --test multiplexing` | Integration | 4 |
| Relay | `cargo test -p libudx --test relay` | Integration | 5 |
| Throughput | `cargo test -p libudx --test throughput` | Performance | 3 |
| Error cases | `cargo test -p libudx --test errors` | Unit | 6 |
| Wire compatibility | `cargo test -p libudx --test wire_compat` | Live | 6 |
| UDX relay bidir | `cargo test -p libudx --test stream_relay` | Integration | 1 |
| UDX interop | `cargo test -p libudx --test udx_interop` | Live | 1 |

**Total**: 102 tests (48 unit + 54 integration), 0 ignored

**Shared utilities**: `libudx/tests/common/mod.rs` — `create_runtime()`, `create_bound_socket()`, `create_connected_pair()`, `random_payload()`, `verify_payload()`, `with_timeout()`

**Node.js helpers**:
- `tests/node/udx-echo-server.js` — single-stream echo server
- `tests/node/udx-echo-server-multi.js` — multi-stream echo server
- `tests/node/udx-wire-capture.js` — raw UDP packet capture
- `tests/node/udx-large-transfer.js` — large transfer helper (echo, receive_and_hash, send_and_receive modes)

**Wire format reference**: `libudx/tests/fixtures/wire-format-reference.json` — captured DATA + END packets from Node.js udx-native

**Encrypted stream coverage**: The planned `async_stream_with_secret_stream` and `relay_with_encrypted_stream` tests were not implemented in the libudx crate because SecretStream lives in peeroxide-dht and cannot be a dev-dependency of libudx (circular dependency). Equivalent coverage exists in:
- `peeroxide-dht/tests/relay_encrypted.rs` — SecretStream over UDX relay (bidirectional encrypted messages + 10-message burst)
- `peeroxide-dht/tests/secret_stream_interop.rs` — SecretStream with Node.js over TCP (same AsyncRead/AsyncWrite interface)

**Sub-milestones**:
- Regression test suite against FFI baseline
- Wire format + UDP socket (native)
- Runtime + Stream + AsyncStream foundation
- Reliability (ACK, retransmit, SACK, RTO)
- BBR congestion control
- MTU constants + relay forwarding
- Error handling + cross-language verification
- Remove FFI, native-only backend

Passing result: all 102 tests pass against the pure Rust native UDX implementation. Wire compatibility verified against Node.js `udx-native` via 6 wire-compat tests + 3 interop tests. Full workspace regression: 468 tests pass, 6 ignored.
