# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.2](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.3.1...libudx-v1.7.2) - 2026-07-12

### Other

- NAT holepunch hardening + blind-relay server implementation ([#27](https://github.com/Rightbracket/peeroxide/pull/27))

### Fixed

- Fixed a `process_incoming` ordering bug: the relay fast-path (packet forwarding once `UdxStream::relay_to` is configured) was checked before the firewall-hook gate, permanently starving hook-based 4-tuple address adoption whenever `relay_to` was pre-wired on both sides of a pairing before either side's first packet arrived. The relay-forwarding check now runs after the firewall-hook gate.

## [1.3.1](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.3.0...libudx-v1.3.1) - 2026-05-14

### Other

- release ([#20](https://github.com/Rightbracket/peeroxide/pull/20))

## [1.3.0](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.2.0...libudx-v1.3.0) - 2026-05-14

### Other

- release ([#19](https://github.com/Rightbracket/peeroxide/pull/19))

## [1.2.0](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.1.0...libudx-v1.2.0) - 2026-05-01

### Changed

- `UdxSocket` is now a cheap-clone `Arc` handle (`UdxSocketInner` holds all state internally). All clones share the same underlying socket; the recv loop is only torn down when the last clone is dropped. `UdxSocket::close(self)` consuming signature is unchanged. ([#12](https://github.com/Rightbracket/peeroxide/pull/12))

## [1.1.0](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.0.1...libudx-v1.1.0) - 2026-04-28

### Changed

- `UdxError` marked `#[non_exhaustive]` to allow future variants without a breaking change. ([#10](https://github.com/Rightbracket/peeroxide/pull/10))

## [1.0.1](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.0.0...libudx-v1.0.1) - 2026-04-26

### Changed

- Added `#[forbid(unsafe_code)]` and `[package.metadata.docs.rs]` configuration.
- Fixed doc example to use `?` instead of `.unwrap()`.
- Increased network simulation test timeout from 120 s to 300 s for slower CI runners.
- Updated repository URL, badges, and README links after repo transfer to the `Rightbracket` org.

## [1.0.0](https://github.com/Rightbracket/peeroxide/releases/tag/v1.0.0) - 2026-04-25

Initial release. Pure Rust implementation of the UDX protocol with BBR
congestion control, wire-compatible with the existing Node.js network.

### Added

- Pure Rust UDX protocol implementation (replaced C FFI bindings)
- BBR congestion control (faithful port of C `udx_bbr.c`)
- Reliability: cumulative ACK, SACK, retransmission with RTO, fast retransmit
- RTT estimation (Jacobson/Karels per RFC 6298)
- Rate sampling for BBR bandwidth estimation
- Token bucket pacing
- MTU probing (base 1200, max 1500, step 32)
- Relay packet forwarding (header rewriting, DESTROY propagation)
- `UdxAsyncStream`: `AsyncRead + AsyncWrite + Unpin` adapter for tokio
- Multiplexing: multiple streams per socket with independent congestion state
- Heartbeat keepalive (1s interval)
- Graceful shutdown with buffered write drain

### Tested

- 53 unit tests passing
- 74 integration and network simulation tests (loss, delay, jitter, reorder, MTU clamping)
- Golden byte fixtures verified against Node.js libudx reference implementation

### Dependencies

- `tokio` — async runtime
- `tracing`, `thiserror` — logging and error handling

### Compatibility

- Wire-compatible with Node.js libudx
- Rust edition 2024, MSRV 1.85
- Dual-licensed: MIT OR Apache-2.0
