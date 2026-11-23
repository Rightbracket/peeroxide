# peeroxide-dht

[![crates.io](https://img.shields.io/crates/v/peeroxide-dht.svg)](https://crates.io/crates/peeroxide-dht)
[![docs.rs](https://docs.rs/peeroxide-dht/badge.svg)](https://docs.rs/peeroxide-dht)
[![CI](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml/badge.svg)](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml)

Rust port of HyperDHT — Kademlia DHT with hole-punching, Noise-encrypted connections, and relay.

This crate implements the full HyperDHT protocol stack. It is wire-compatible with the Node.js implementation on the public Hyperswarm network.

Most users should use the higher-level `peeroxide` crate. Use `peeroxide-dht` directly for low-level DHT operations or building custom peer-to-peer applications.

### Protocol Layers

| Layer | Modules | Reference |
|---|---|---|
| Wire encoding | compact_encoding | compact-encoding |
| DHT RPC | rpc, io, query, routing_table | dht-rpc |
| Peer operations | hyperdht, hyperdht_messages | hyperdht |
| Noise XX handshake | noise, noise_wrap | noise-handshake |
| Encrypted streams | secret_stream, secretstream | @hyperswarm/secret-stream |
| NAT traversal | nat, holepuncher | hyperdht/lib/holepuncher.js |
| Relay | blind_relay, protomux | blind-relay |

### Quick Start

```rust
use libudx::UdxRuntime;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, KeyPair};

let runtime = UdxRuntime::new()?;
let config = HyperDhtConfig::with_public_bootstrap();
let (_task, dht, _server_rx) = hyperdht::spawn(&runtime, config).await?;
dht.bootstrapped().await?;

let key_pair = KeyPair::generate();
let topic = peeroxide_dht::crypto::hash(b"my-app");
dht.announce(topic, &key_pair, &[]).await?;
```

### Features

- Key operations: `announce`, `lookup`, `find_peer`, `connect`, and mutable/immutable `put`/`get`.
- Bounded connection memory: streams set up with `establish_stream` (and `connect_with_options`) get an 8 MiB read and an 8 MiB write buffer limit by default (`DEFAULT_READ_BUFFER_LIMIT`, `DEFAULT_WRITE_BUFFER_LIMIT`), so an untrusted remote can't make a stream buffer without bound. `StreamLimits` offers `constrained`, `mobile`, `balanced` (default), `throughput`, and `unlimited` presets; apply one with `ConnectOpts::with_limits`, or change an established stream with `UdxStream::set_read_buffer_limit` / `set_write_buffer_limit`. The limits also cap throughput, at about 0.94 × limit ÷ round trip.
- Pure Rust cryptography: Uses `ed25519-dalek`, `chacha20poly1305`, and `blake2`. No `libsodium` dependency.
- Interoperability: Validated through golden byte fixtures and live cross-language tests.

### Project

Part of the [peeroxide](https://github.com/Rightbracket/peeroxide) project.

### License

MIT OR Apache-2.0
