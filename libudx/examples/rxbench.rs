//! Measures what receiving costs a stream, for comparing two builds of this
//! crate: `.github/scripts/rxbench.py` runs it from both and compares the
//! receiving end's throughput and processor time.
//!
//! Start the receiving end, then the sending end:
//!
//! ```text
//! cargo run --release -p libudx --example rxbench -- recv 127.0.0.1:9101 127.0.0.1:9102 200
//! cargo run --release -p libudx --example rxbench -- send 127.0.0.1:9102 127.0.0.1:9101 200
//! ```
//!
//! Each end takes its own address, the other end's address, and the number of
//! megabytes to transfer. The receiving end reads a packet at a time with
//! [`UdxStream::read`], or through the `AsyncRead` adapter with `async`, and
//! takes a read buffer limit with `limit=<bytes>`. It prints the rate from the
//! first byte to the last. The sending end writes 64 KB at a time through the
//! adapter.
//!
//! It uses only API that has been in this crate since the read buffer limit
//! was added, so that it builds against an older revision too.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use libudx::{UdxRuntime, UdxStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const USAGE: &str =
    "usage: rxbench recv|send <own addr> <other addr> <megabytes> [async] [limit=<bytes>]";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let receiving = match args.get(1).map(String::as_str) {
        Some("recv") => true,
        Some("send") => false,
        _ => panic!("{USAGE}"),
    };
    let local: SocketAddr = args.get(2).and_then(|s| s.parse().ok()).expect(USAGE);
    let remote: SocketAddr = args.get(3).and_then(|s| s.parse().ok()).expect(USAGE);
    let megabytes: usize = args.get(4).and_then(|s| s.parse().ok()).expect(USAGE);
    let total = megabytes * 1024 * 1024;
    let through_adapter = args.iter().any(|arg| arg == "async");
    let limit = args
        .iter()
        .find_map(|arg| arg.strip_prefix("limit="))
        .map(|limit| limit.parse::<u32>().expect(USAGE));

    let runtime = UdxRuntime::new().expect("runtime");
    let socket = runtime.create_socket().await.expect("socket");
    socket.bind(local).await.expect("bind");

    if receiving {
        let stream = runtime.create_stream(2).await.expect("stream");
        stream.set_read_buffer_limit(limit);
        stream.connect(&socket, 1, remote).await.expect("connect");
        receive(stream, total, through_adapter).await;
        // Let the last acknowledgements out before the socket closes.
        tokio::time::sleep(Duration::from_millis(200)).await;
    } else {
        let stream = runtime.create_stream(1).await.expect("stream");
        stream.connect(&socket, 2, remote).await.expect("connect");
        let mut stream = stream.into_async_stream();
        let chunk = vec![7u8; 64 * 1024];
        let mut sent = 0;
        while sent < total {
            let n = chunk.len().min(total - sent);
            stream.write_all(&chunk[..n]).await.expect("write");
            sent += n;
        }
        stream.shutdown().await.expect("shutdown");
    }
}

/// Reads `total` bytes and prints the rate from the first of them to the last.
async fn receive(stream: UdxStream, total: usize, through_adapter: bool) {
    let mut received = 0;
    let mut started = None;
    if through_adapter {
        let mut stream = stream.into_async_stream();
        let mut buf = vec![0u8; 64 * 1024];
        while received < total {
            let n = stream.read(&mut buf).await.expect("read");
            assert!(n > 0, "the stream ended early");
            started.get_or_insert_with(Instant::now);
            received += n;
        }
    } else {
        let mut stream = stream;
        while received < total {
            let data = stream.read().await.expect("read").expect("the stream ended early");
            started.get_or_insert_with(Instant::now);
            received += data.len();
        }
    }
    let seconds = started.expect("data").elapsed().as_secs_f64();
    println!("RESULT mbps={:.1}", total as f64 / 1e6 / seconds);
}
