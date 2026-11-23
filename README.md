# peeroxide

[![CI](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml/badge.svg)](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)

Rust implementation of the Hyperswarm P2P networking stack, wire-compatible with the existing Node.js network.

This project is a faithful port targeting full interoperability with the existing Hyperswarm network. Node.js peers can discover and connect to Rust peers and vice versa.

## Architecture

```
peeroxide-cli          — command-line toolkit (init, node, relay, lookup, announce, ping, cp, dd, chat)
└── peeroxide          — topic-based peer discovery + connection management (Hyperswarm)
    └── peeroxide-dht  — Kademlia DHT, Noise handshakes, hole-punching, relay (HyperDHT)
        └── libudx     — reliable UDP transport with BBR congestion control (libudx)
```

## Install the CLI

The `peeroxide` CLI bundles several subcommands (`lookup`, `announce`, `ping`, `cp`, `dd`, `chat`, `node`, `relay`, `init`).
The CLI was built as an example of how to use the library, and also serves as a convenient toolkit for interacting with the network from the terminal to test connectivity, share files, or chat with peers.
No Rust toolchain is needed for the prebuilt CLI route.

**Homebrew (macOS / Linux):**

```bash
brew install rightbracket/peeroxide/peeroxide
```

Homebrew will auto-tap `rightbracket/peeroxide` on first use. Prebuilt binaries are published for macOS (universal Apple Silicon + Intel), Linux x86_64 (glibc), and Linux aarch64 (glibc).

**Cargo:**

```bash
cargo install peeroxide-cli
```

**Build from upstream `main`:**

```bash
brew install --HEAD rightbracket/peeroxide/peeroxide
```

After install:

```bash
peeroxide --help
peeroxide chat --help
```

Tap details and upgrade / uninstall instructions: <https://github.com/Rightbracket/homebrew-peeroxide>.

## Quick Start (library)

```rust
use peeroxide::{spawn, discovery_key, JoinOpts, SwarmConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = SwarmConfig::with_public_bootstrap();
    let (_task, handle, mut conn_rx) = spawn(config).await?;

    let topic = discovery_key(b"my-application");
    handle.join(topic, JoinOpts::default()).await?;

    while let Some(conn) = conn_rx.recv().await {
        println!("connected to {}", hex::encode(conn.remote_public_key()));
    }
    Ok(())
}
```

## Crates

- **peeroxide** [![Crates.io](https://img.shields.io/crates/v/peeroxide.svg)](https://crates.io/crates/peeroxide)  
  High-level swarm management and topic-based discovery.
- **peeroxide-dht** [![Crates.io](https://img.shields.io/crates/v/peeroxide-dht.svg)](https://crates.io/crates/peeroxide-dht)  
  HyperDHT implementation including Kademlia, hole-punching, and Noise handshakes.
- **libudx** [![Crates.io](https://img.shields.io/crates/v/libudx.svg)](https://crates.io/crates/libudx)  
  Pure Rust implementation of the UDX protocol with BBR congestion control.
- **peeroxide-cli** [![Crates.io](https://img.shields.io/crates/v/peeroxide-cli.svg)](https://crates.io/crates/peeroxide-cli)  
  Command-line toolkit (`peeroxide` binary): `init`, `node`, `relay`, `lookup`, `announce`, `ping`, `cp`, `dd`, `chat`.

## Interoperability

All layers are validated via golden byte fixtures and live cross-language interop tests against Node.js reference implementations. The stack uses pure Rust crypto (RustCrypto) and excludes C dependencies. It targets the Rust 2024 edition and uses the tokio async runtime.

## Reference Implementations

This work implements the protocols defined in the following reference projects:
- [hyperswarm](https://github.com/holepunchto/hyperswarm)
- [hyperdht](https://github.com/holepunchto/hyperdht)
- [dht-rpc](https://github.com/holepunchto/dht-rpc)
- [hyperswarm-secret-stream](https://github.com/holepunchto/hyperswarm-secret-stream)
- [libudx](https://github.com/holepunchto/libudx)

## Status

The full protocol stack is implemented and validated, including live network interoperability with the Node.js reference implementation. See each crate's CHANGELOG ([CHANGELOG.md](./CHANGELOG.md)) for current versions and release notes.

## License

Dual-licensed under MIT OR Apache-2.0. See THIRD_PARTY_NOTICES for upstream license details.
