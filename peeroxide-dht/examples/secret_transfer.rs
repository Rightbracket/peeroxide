//! Measures how fast an encrypted stream carries messages from one program
//! to another: a [`SecretStream`] over a UDX stream, which is what a
//! connection made through the DHT is.
//!
//! Start the receiving end, then the sending end:
//!
//! ```text
//! cargo run --release -p peeroxide-dht --example secret_transfer -- recv 127.0.0.1:9001 127.0.0.1:9002 50
//! cargo run --release -p peeroxide-dht --example secret_transfer -- send 127.0.0.1:9002 127.0.0.1:9001 50
//! ```
//!
//! Each end takes its own address, the other end's address, and the number of
//! megabytes to transfer. The sending end also takes the size of each message
//! in kilobytes, 64 by default.
//!
//! The receiving end has stream id 2 and answers the handshake. The sending
//! end has stream id 1 and starts it. Either end can be a Node program that
//! puts `@hyperswarm/secret-stream` on a `udx-native` stream with the same
//! ids.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use libudx::{UdxAsyncStream, UdxRuntime};
use peeroxide_dht::noise;
use peeroxide_dht::secret_stream::SecretStream;

const USAGE: &str = "usage: secret_transfer recv|send <own addr> <other addr> <megabytes> [message KB]";

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

    let (own_id, other_id) = if receiving { (2, 1) } else { (1, 2) };
    let stream = runtime.create_stream(own_id).await.expect("stream");
    stream.connect(&socket, other_id, remote).await.expect("connect");

    let secret = SecretStream::new(!receiving, stream.into_async_stream(), noise::generate_keypair())
        .await
        .expect("handshake");

    if receiving {
        receive(secret, total).await;
    } else {
        let message_size = args
            .get(5)
            .and_then(|s| s.parse::<usize>().ok())
            .map_or(64 * 1024, |kilobytes| kilobytes * 1024);
        send(secret, total, message_size.max(1)).await;
    }

    // Let the last acknowledgements out before the socket closes.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Reads `total` bytes and prints the rate from the first of them to the
/// last. Then waits for the other end to end the stream, and ends its own.
async fn receive(mut secret: SecretStream<UdxAsyncStream>, total: usize) {
    let mut received = 0;
    let mut started = None;
    while received < total {
        let message = secret.read().await.expect("read").expect("the stream ended early");
        started.get_or_insert_with(Instant::now);
        received += message.len();
    }

    let seconds = started.expect("data").elapsed().as_secs_f64();
    let megabytes = total as f64 / 1e6;
    println!("received {megabytes:.1} MB in {seconds:.2} s: {:.1} MB/s", megabytes / seconds);

    let _ = tokio::time::timeout(Duration::from_secs(5), secret.read()).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), secret.shutdown()).await;
}

/// Writes `total` bytes in messages of `message_size`, ends the stream, and
/// prints the rate up to the acknowledgement of the last byte.
async fn send(mut secret: SecretStream<UdxAsyncStream>, total: usize, message_size: usize) {
    let buf = vec![7u8; message_size.min(total)];
    let started = Instant::now();
    let mut sent = 0;
    while sent < total {
        let n = buf.len().min(total - sent);
        secret.write(&buf[..n]).await.expect("write");
        sent += n;
    }
    // A write does not wait to be acknowledged. The end of the stream is
    // acknowledged after everything before it.
    secret.shutdown().await.expect("shutdown");

    let seconds = started.elapsed().as_secs_f64();
    let megabytes = total as f64 / 1e6;
    println!("sent {megabytes:.1} MB, acknowledged in {seconds:.2} s: {:.1} MB/s", megabytes / seconds);

    // The other end ends its stream in answer.
    let _ = tokio::time::timeout(Duration::from_secs(5), secret.read()).await;
}
