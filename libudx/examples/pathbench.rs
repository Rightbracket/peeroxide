//! Carries data one way for a fixed time and reports how much arrived when,
//! for charting how a path's round trip, jitter and loss shape a stream's
//! throughput. `.github/scripts/pathsweep.py` runs it through emulated paths.
//!
//! Start the receiving end, then the sending end:
//!
//! ```text
//! cargo run --release -p libudx --example pathbench -- recv 127.0.0.1:9201 127.0.0.1:9202 15
//! cargo run --release -p libudx --example pathbench -- send 127.0.0.1:9202 127.0.0.1:9201 15
//! ```
//!
//! Each end takes its own address, the other end's address, and the number of
//! seconds to measure for. The receiving end takes a read buffer limit with
//! `limit=<bytes>`, which sets the window it advertises. It reads through
//! the `AsyncRead` adapter and, from the first byte, prints `AT <seconds> <bytes>` every
//! 100 ms: the time since the first byte and the bytes received by then. It
//! exits once the seconds have passed. The sending end writes 64 KB at a time
//! through the adapter until it is stopped, or for a minute past the seconds.
//!
//! It uses only API that has been in this crate since `rxbench` was added, so
//! that it builds against an older revision too.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use libudx::UdxRuntime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const USAGE: &str = "usage: pathbench recv|send <own addr> <other addr> <seconds> [limit=<bytes>]";
const TICK: Duration = Duration::from_millis(100);

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
    let seconds: f64 = args.get(4).and_then(|s| s.parse().ok()).expect(USAGE);
    let measure = Duration::from_secs_f64(seconds);
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
        let mut stream = stream.into_async_stream();

        // Read on a task of its own, so that the timeline is printed on time
        // whatever the reads are doing.
        let received = Arc::new(AtomicU64::new(0));
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let counter = Arc::clone(&received);
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            let mut first = Some(first_tx);
            loop {
                let n = stream.read(&mut buf).await.expect("read");
                if n == 0 {
                    break;
                }
                if let Some(first) = first.take() {
                    let _ = first.send(Instant::now());
                }
                counter.fetch_add(n as u64, Ordering::Relaxed);
            }
        });

        let started = first_rx.await.expect("the stream ended before any data");
        let mut next = started;
        while next < started + measure {
            next += TICK;
            tokio::time::sleep_until(next.into()).await;
            let at = started.elapsed().as_secs_f64();
            println!("AT {at:.3} {}", received.load(Ordering::Relaxed));
        }
        std::process::exit(0);
    } else {
        let stream = runtime.create_stream(1).await.expect("stream");
        stream.connect(&socket, 2, remote).await.expect("connect");
        let mut stream = stream.into_async_stream();
        let chunk = vec![7u8; 64 * 1024];
        let until = Instant::now() + measure + Duration::from_secs(60);
        while Instant::now() < until {
            stream.write_all(&chunk).await.expect("write");
        }
    }
}
