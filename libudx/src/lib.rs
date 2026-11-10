// `unsafe` is confined to `native::sockopt`, which sets two socket options
// that nothing safe reaches.
#![deny(unsafe_code)]

//! Pure Rust implementation of the UDX reliable UDP transport protocol.
//!
//! `libudx` provides ordered, reliable byte streams over UDP with BBR
//! congestion control, SACK-based loss recovery, and MTU path discovery.
//! It is wire-compatible with the C [libudx](https://github.com/holepunchto/libudx)
//! library used by the [Hyperswarm](https://github.com/holepunchto/hyperswarm)
//! P2P network.
//!
//! # Architecture
//!
//! The crate is built on [tokio] and exposes four main types:
//!
//! - [`UdxRuntime`] — owns the UDP event loop; create one per application
//!   (or per logical network context).
//! - [`UdxSocket`] — a bound UDP socket that multiplexes many streams.
//! - [`UdxStream`] — a single reliable, ordered byte stream between two
//!   peers, identified by a `(socket, stream_id)` pair.
//! - [`UdxAsyncStream`] — an [`AsyncRead`](tokio::io::AsyncRead) +
//!   [`AsyncWrite`](tokio::io::AsyncWrite) adapter around `UdxStream`,
//!   suitable for use with `tokio::io::copy`, framed codecs, and
//!   higher-level protocols like [`SecretStream`](https://docs.rs/peeroxide-dht).
//!
//! # Quick start
//!
//! ```rust,no_run
//! use libudx::{UdxRuntime, UdxSocket, UdxStream};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = UdxRuntime::new()?;
//! let socket = runtime.create_socket().await?;
//! socket.bind("0.0.0.0:0".parse()?).await?;
//!
//! let stream = runtime.create_stream(1).await?;
//! stream.connect(&socket, 1, "127.0.0.1:9000".parse()?).await?;
//! stream.write(b"hello").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Feature highlights
//!
//! - **BBR congestion control** — faithful port of the C libudx BBR
//!   implementation, with pacing and bandwidth estimation.
//! - **SACK-based loss detection** — selective acknowledgements with fast
//!   retransmit for rapid recovery under packet loss.
//! - **Stream relay** — [`UdxStream::relay_to`] forwards packets between
//!   two streams at the UDP layer, enabling blind-relay topologies.
//! - **No C dependencies** — everything is pure Rust + tokio.

#![deny(clippy::all)]

// ── Shared ───────────────────────────────────────────────────────────

mod error;
pub use error::{Result, UdxError};

// ── Native backend ───────────────────────────────────────────────────

mod native;

pub use native::async_stream::UdxAsyncStream;
pub use native::runtime::{RuntimeHandle, UdxRuntime};
pub use native::socket::{Datagram, UdxSocket};
pub use native::stream::UdxStream;
