//! Measures how fast a stream carries data from one program to another.
//!
//! Start the receiving end, then the sending end:
//!
//! ```text
//! cargo run --release -p libudx --example transfer -- recv 127.0.0.1:9001 127.0.0.1:9002 50
//! cargo run --release -p libudx --example transfer -- send 127.0.0.1:9002 127.0.0.1:9001 50
//! ```
//!
//! Each end takes its own address, the other end's address, and the number of
//! megabytes to transfer. The sending end also takes the size of each write
//! in kilobytes (by default everything goes in one write), and `async` to
//! write through the `AsyncWrite` adapter instead of [`UdxStream::write`].
//!
//! The receiving end has stream id 2 and the sending end stream id 1, so
//! either end can be a Node program on `udx-native` that uses the same ids.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use libudx::{UdxRuntime, UdxStream};
use tokio::io::AsyncWriteExt;

const USAGE: &str = "usage: transfer recv|send <own addr> <other addr> <megabytes> [write KB] [async]";

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

    let runtime = UdxRuntime::new().expect("runtime");
    let socket = runtime.create_socket().await.expect("socket");
    socket.bind(local).await.expect("bind");

    if receiving {
        let mut stream = runtime.create_stream(2).await.expect("stream");
        stream.connect(&socket, 1, remote).await.expect("connect");
        receive(&mut stream, total).await;
        // Let the last acknowledgements out, the one for the end of the
        // stream included, before the socket closes.
        tokio::time::sleep(Duration::from_millis(200)).await;
    } else {
        let write_size = args
            .get(5)
            .and_then(|s| s.parse::<usize>().ok())
            .map_or(total, |kilobytes| kilobytes * 1024);
        let through_adapter = args.iter().any(|arg| arg == "async");

        let stream = runtime.create_stream(1).await.expect("stream");
        stream.connect(&socket, 2, remote).await.expect("connect");
        send(stream, total, write_size.max(1), through_adapter).await;
    }
}

/// Reads `total` bytes and prints the rate from the first of them to the last.
async fn receive(stream: &mut UdxStream, total: usize) {
    let mut received = 0;
    let mut started = None;
    while received < total {
        let data = stream.read().await.expect("read").expect("the stream ended early");
        started.get_or_insert_with(Instant::now);
        received += data.len();
    }

    let seconds = started.expect("data").elapsed().as_secs_f64();
    let megabytes = total as f64 / 1e6;
    println!("received {megabytes:.1} MB in {seconds:.2} s: {:.1} MB/s", megabytes / seconds);
}

/// Writes `total` bytes, `write_size` at a time, and prints the rate up to
/// the acknowledgement of the last of them. Then ends the stream, and
/// returns once the other end has acknowledged that.
async fn send(stream: UdxStream, total: usize, write_size: usize, through_adapter: bool) {
    let buf = vec![7u8; write_size.min(total)];
    let started = Instant::now();
    let mut sent = 0;

    if through_adapter {
        let mut stream = stream.into_async_stream();
        while sent < total {
            let n = buf.len().min(total - sent);
            stream.write_all(&buf[..n]).await.expect("write");
            sent += n;
        }
        stream.flush().await.expect("flush");
        report(total, started);
        stream.shutdown().await.expect("shutdown");
    } else {
        while sent < total {
            let n = buf.len().min(total - sent);
            stream.write(&buf[..n]).await.expect("write");
            sent += n;
        }
        report(total, started);
        stream.shutdown().await.expect("shutdown");
    }
}

fn report(total: usize, started: Instant) {
    let seconds = started.elapsed().as_secs_f64();
    let megabytes = total as f64 / 1e6;
    println!("sent {megabytes:.1} MB, acknowledged in {seconds:.2} s: {:.1} MB/s", megabytes / seconds);
}
