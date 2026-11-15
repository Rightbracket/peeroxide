# libudx

[![crates.io](https://img.shields.io/crates/v/libudx.svg)](https://crates.io/crates/libudx)
[![docs.rs](https://docs.rs/libudx/badge.svg)](https://docs.rs/libudx)
[![CI](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml/badge.svg)](https://github.com/Rightbracket/peeroxide/actions/workflows/ci.yml)

Pure Rust implementation of the UDX reliable UDP transport protocol, wire-compatible with C libudx.

This crate provides ordered, reliable byte streams over UDP. It implements BBR congestion control, SACK-based loss recovery, and MTU path discovery. Built on tokio, it's designed for high-performance P2P networking and maintains wire compatibility with the [C libudx](https://github.com/holepunchto/libudx) implementation used by the Hyperswarm network.

## Quick Start

```rust
use libudx::{UdxRuntime, UdxSocket, UdxStream};

let runtime = UdxRuntime::new()?;
let socket = runtime.create_socket().await?;
socket.bind("0.0.0.0:0".parse().unwrap()).await?;

let stream = runtime.create_stream(1).await?;
stream.connect(&socket, 1, "127.0.0.1:9000".parse().unwrap()).await?;
stream.write(b"hello").await?;
```

## Key Types

- `UdxRuntime`: The central driver managing socket events and stream state.
- `UdxSocket`: A UDP socket wrapper for sending and receiving UDX packets.
- `UdxStream`: A handle to a reliable byte stream.
- `UdxAsyncStream`: An adapter implementing `AsyncRead` and `AsyncWrite` for tokio integration.

## Features

- BBR congestion control
- SACK and RACK-TLP loss recovery (RFC 8985)
- Stream multiplexing over a single socket
- Stream relaying (`relay_to`)
- MTU probing and path discovery
- Heartbeat keepalive mechanism

## Protocol

See [PROTOCOL.md](PROTOCOL.md) for the UDX wire format specification, BBR
congestion control details, and protocol constants.

## Implementation Details

- Pure Rust with no C dependencies
- tokio async runtime
- Part of the [peeroxide](https://github.com/Rightbracket/peeroxide) workspace

## License

MIT OR Apache-2.0
