# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `examples/transfer.rs` measures how fast a stream carries data between two programs. Either end can be a Node.js program on `udx-native`. The sending end also reports the size of the largest packet the stream came to send.
- `UdxAsyncStream::effective_mtu`, the counterpart of `UdxStream::effective_mtu`.

### Changed

- The sending side of a stream now follows C libudx: its write queue, congestion control, pacing, loss detection and timers are ports of `udx_bbr.c`, `udx_rate.c` and the send path of `udx.c`. A 50 MB transfer between two programs on one machine went from 0.1 MB/s to about 250 MB/s on Windows, and from 0.9 MB/s to about 600 MB/s on Linux under WSL2. Through a path of 100 Mbit/s with a 40 ms round trip, 10 MB went from 0.4 MB/s to between 8 and 9 MB/s, which is what two Node.js programs on `udx-native` reach there. No signature of the public API changes. What a write through `UdxAsyncStream` waits for does: see the next entry.
- **`UdxAsyncStream` no longer waits for acknowledgements.** `poll_write` takes the bytes into the stream's write queue and returns, as a TCP socket takes them into its send buffer. It asks its writer to wait only while more than 262,144 bytes on top of the send window are written and unacknowledged, which is C libudx's high-water mark. `poll_flush` returns at once. Before, a write waited for the one before it to be acknowledged, and `poll_flush` for the last one, so a stream written in pieces moved one piece per round trip: through the path above, 10 MB written 64 KB at a time went from 1.4 MB/s to about 8 MB/s. `poll_shutdown` returns, as before, once the end of the stream has been acknowledged, and with it everything written: that is now how to know that everything arrived. `UdxStream::write` is unchanged and returns when its data has been acknowledged.
- **An acknowledgement names up to 144 ranges of what arrived out of order, where it named 31.** That is what fits a packet of the starting MTU (141 over IPv6). A sender learns of a gap from the ranges around it, and they are the lowest ones, so what it can repair in one round trip is what one acknowledgement names: a burst of a thousand losses, which the end of Startup produces on a path with a shallow queue, took some thirty round trips to repair and now takes seven. Through a path of 100 Mbit/s with a 40 ms round trip, 10 MB between two ends of this crate went from 7.8 to 9.9 MB/s, and with 1% of packets lost as well from 7.7 to 9.4, where the slowest of twelve runs went from 4.5 to 9.1. A `udx-native` sender into this crate went from 8.5 to 9.6 MB/s and from 8.8 to 9.4. Two `udx-native` ends, whose acknowledgements name 51 ranges, reach 8.6 and 8.7 there. A list of more than 31 ranges goes out in C libudx's form, which C libudx reads to any length and releases of this crate up to 1.7.4 do not read; a list of up to 31 is sent as before. The acknowledgements of a transfer take about as many bytes as before, since the repair they are needed for is over sooner.
- A `UdxAsyncStream` that is dropped goes on delivering what was written to it, in the background, until all of it has been acknowledged. It stops earlier if the remote destroys the stream or stops answering, or after 30 seconds without progress. Before, it sent each packet still queued once and stopped, so a packet lost after the drop was never sent again.
- A write goes into a write queue, and packets are cut from the queue as they are sent, as in C libudx. Before, a write was cut into packets, and those were numbered, when it was written. A packet is now as large as the path's MTU allows at the moment it goes out, so an MTU found larger during a long write applies to the rest of it. Writes smaller than a packet share one. The end of the stream rides on the last packet of data, where it took a packet of its own. A packet carries the acknowledgement number of the moment it is sent.
- A stream that gives up on its remote, or is closed, ends a read that is waiting. `UdxAsyncStream` reports why: a read fails with `TimedOut` when the remote stopped answering, as writes, `flush` and `shutdown` do from then on, and `flush` and `shutdown` fail with `BrokenPipe` on a stream that was closed before everything written to it was acknowledged. Before, such a read waited for good.
- The congestion window is no longer scaled when the path's MTU is found larger. It counts packets, and C libudx leaves it alone.
- A stream sends as soon as an acknowledgement, a write or a timer allows, for as long as the pacer and the congestion window let it. Before, the pacer never held more than two packets, and a stream with more to send waited on a 1 ms timer, which Windows fires every 15.6 ms.
- Loss is detected as in RFC 8985 (RACK-TLP): a packet is lost when one sent after it has been delivered and the reordering window has passed, and a tail loss probe finds the loss of a flight's last packets. Lost packets are sent again through the pacer and the congestion window, before new data. Before, every acknowledgement with a gap marked the packets below it lost again, retransmitted ones included, and each copy went out at once.
- The retransmission timer resends the packets that have timed out and is then set to twice the timeout, as in C libudx. A stream whose peer never answers fails after 13 seconds; before, it resent everything in flight, doubled the timeout each time, and failed after 61 seconds.
- One acknowledgement answers all the packets that arrived together, where each packet drew its own before.
- **What a packet carries for the reader is taken in by the task that receives it**, as in C libudx, whose `process_packet` runs in the callback that reads the socket. A socket's receive task hands a packet's data to the stream's reader and sends the acknowledgement it draws. The stream's own task still does all the sending, and is passed only what a packet says to the sending side: what it acknowledges, the ranges it names and the window it advertises. Before, every packet was handed to the stream's task first, and on a runtime with several threads nine in ten of those hand-overs went to another thread. Into this crate, a 200 MB transfer between two programs on one Windows machine went from 204 to 259 MB/s from a `udx-native` sender, where two `udx-native` ends reach 264, and from 209 to 250 MB/s from a sender of this crate. The receiving end used 1.1 seconds of processor time where it used 2.0. On Linux under WSL2, 500 MB went from 805 to 1,065 MB/s and from 826 to 895. The packets of a stream that is not connected yet, or that relays to another stream, go to the stream's own task as before.
- **On Windows, the acknowledgements a socket owes wait while packets keep arriving.** They wait one turn of the event loop, as in C libudx, and once a datagram has ended such a wait, up to 50 microseconds for each next one, until 64 have been read. Sending a datagram costs several times more there than on Linux, and each acknowledgement costs the remote, which is busy sending, the work of taking it in: a flow is now acknowledged once for about sixty packets, where it was once for four or five. A wait that no datagram ends puts the next one back to a single turn, so a path too slow to fill a wait is acknowledged as before. A packet that leaves a gap, a packet the stream had already, and the end of a stream are acknowledged without waiting. A turn that takes longer than 100 microseconds switches the waiting off for the next 64 times. Into this crate, a 200 MB transfer between two programs on one Windows machine went from 265 to 314 MB/s from a `udx-native` sender, and from 261 to 317 from a sender of this crate, where two `udx-native` ends reach 268. Through an encrypted stream it went from 210 to 244 and from 269 to 323, where they reach 233. On other systems acknowledgements go out as soon as the socket is empty: waiting made no difference that could be measured on Linux.
- A packet is allocated once, where it was allocated twice. What a stream keeps of a packet until it is acknowledged and what goes on the wire are the same bytes, and a packet that is sent again goes out from them without a copy. The task that sends is the one a transfer waits for: on Windows it spends four fifths of its time in the system's send call and has none to spare. Between two programs on one Windows machine, a 200 MB transfer from this crate into `udx-native` went from 252 to 260 MB/s, where two `udx-native` ends reach 263, and through an encrypted stream from 278 to 297, where they reach 226. On Linux under WSL2, 500 MB between two ends of this crate went from 896 to 953 MB/s.
- BBR, its delivery-rate sampling and its bandwidth filter are ported function by function. The send interval of a rate sample is measured from the sending of the packet acknowledged last, where it was always zero. The round-trip time BBR sees is the packet's own, where it was the time since the flight began. The window stops at its target in Startup, where it grew without bound. The pacing rate is never zero, which switched pacing off.
- Rate samples are measured in microseconds. C libudx counts whole milliseconds, which leaves a sample unusable or several times too low when a round trip takes less than one, as it does between two programs on one machine or one network.
- The tail loss probe waits two round trips on a path that answers in under a millisecond too. C libudx takes a smoothed round-trip time of 0 ms to mean that none was measured, and waits a second.
- A socket asks the system for the buffer sizes C libudx asks for: 212,992 bytes to send, and 1 MiB to receive, or the largest of 512, 256 and 208 KiB the system grants. New dependency: `socket2`.
- **A stream's MTU is the size of the whole IP packet**, as in C libudx and `udx-native`: the IP and UDP headers, the UDX header and the payload. It was the size of the UDP datagram. `effective_mtu` still starts at 1200 and ends at 1500 on a path that carries that much, and now means what `udx-native`'s `stream.mtu` means. A full packet carries 1152 to 1452 bytes of payload over IPv4 and 1132 to 1432 over IPv6, where it carried 1180 to 1480 over either.

### Fixed

- A socket no longer stops receiving on Windows after one of its datagrams goes undelivered. Windows reports the ICMP error that such a datagram draws as an error on the socket's next receive: `WSAECONNRESET` for "port unreachable", from a peer that has gone, and `WSAENETRESET` for "time to live exceeded", from a low-TTL holepunch probe. Before, the receive loop ended at the first receive error, so the socket heard nothing more, neither datagrams nor the packets of its streams, until another stream was registered on it. The loop now carries on after those two errors, as libuv's does. Other systems report no such error on an unconnected socket, and behave as before.
- **A stream's full packets no longer travel as two IP fragments each on a path that carries 1500 bytes.** A stream counted its MTU as the size of the UDP datagram, so the largest size it settled on, 1500 bytes of datagram, was an IP packet of 1528, and every full packet was split in two on any ordinary path. A stream's MTU now counts the whole IP packet, as in C libudx, so its largest packet is an IP packet of 1500 bytes. A socket's packets are still not marked "don't fragment", which C libudx does with a socket option that nothing safe offers. So on a path that carries less than 1500 bytes a stream still settles on 1500, and its full packets are fragmented there, as before.
- Selective acknowledgements from C libudx and Node.js peers are no longer ignored. C libudx sends the ranges as the payload of a packet with `data_offset` zero; this crate only read ranges whose length was in `data_offset`, so it learned of a gap only from its retransmission timer. Both forms are read now.
- An acknowledgement with more than 31 ranges no longer states a wrong length for them. It carried up to 50, whose length in bytes does not fit the header's one-byte `data_offset`. A list that long now leaves `data_offset` at zero, which is how C libudx sends every list.
- `UdxStream::write` can no longer leave a sequence number that no packet carries. It counted a write's packets, and reserved their numbers, before it cut them, and looked the packet size up a second time to cut them. If the path's MTU was found larger in between, fewer packets came out than numbers were reserved, the remote waited for the missing one, and neither that write nor any after it completed. Packets are now numbered as they are cut.
- A write to a `UdxAsyncStream` after `shutdown` is refused. It was sent after the end of the stream before. A second `shutdown` no longer sends a second end of stream.

## [1.7.4](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.7.3...libudx-v1.7.4) - 2026-11-02

### Fixed

- *(swarm)* let Node.js clients connect to a firewalled peeroxide server ([#31](https://github.com/Rightbracket/peeroxide/pull/31))

## [1.7.3](https://github.com/Rightbracket/peeroxide/compare/libudx-v1.7.2...libudx-v1.7.3) - 2026-07-12

### Other

- full documentation audit (rustdoc + mdBook) ([#28](https://github.com/Rightbracket/peeroxide/pull/28))

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
