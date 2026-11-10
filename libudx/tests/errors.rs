#![deny(clippy::all)]

mod common;

use common::{create_bound_socket, create_connected_pair, create_runtime, with_timeout};
use std::net::SocketAddr;
use std::time::Duration;

const LOCALHOST: &str = "127.0.0.1:0";

fn addr() -> SocketAddr {
    LOCALHOST.parse().expect("parse localhost")
}

#[tokio::test]
async fn double_bind() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(5), async {
        let rt = create_runtime();
        let socket = rt.create_socket().await.expect("create socket");
        socket.bind(addr()).await.expect("first bind");

        let bound_addr = socket.local_addr().await.expect("local_addr");
        let socket2 = rt.create_socket().await.expect("create socket2");
        let result = socket2.bind(bound_addr).await;
        assert!(result.is_err(), "second bind to same port should fail");
    })
    .await;
}

#[tokio::test]
async fn write_before_connect() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(5), async {
        let rt = create_runtime();
        let stream = rt.create_stream(1).await.expect("create stream");
        let result = stream.write(b"should fail").await;
        assert!(result.is_err(), "write before connect should fail");
    })
    .await;
}

#[tokio::test]
async fn destroy_during_write() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream_a, _stream_b, _socket_a, _socket_b) =
            create_connected_pair(&rt).await;

        stream_a.write(b"data before destroy").await.expect("write");
        stream_a.destroy().await.expect("destroy after write");
    })
    .await;
}

#[tokio::test]
async fn drop_runtime_with_active_streams() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(5), async {
        let rt = create_runtime();
        let (stream_a, stream_b, socket_a, socket_b) =
            create_connected_pair(&rt).await;

        stream_a.write(b"active data").await.expect("write");

        // Drop runtime FIRST — while streams and sockets are still alive.
        drop(rt);
        drop(socket_a);
        drop(socket_b);
        drop(stream_a);
        drop(stream_b);
    })
    .await;
}

#[tokio::test]
async fn create_destroy_loop() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        for _ in 0..100 {
            let rt = create_runtime();
            let socket = rt.create_socket().await.expect("create socket");
            socket.bind(addr()).await.expect("bind");

            let stream = rt.create_stream(1).await.expect("create stream");
            stream.destroy().await.expect("destroy");
            socket.close().await.expect("close");
            drop(rt);
        }
    })
    .await;
}

/// Connect to a port where no peer is listening. The write should eventually
/// fail with a timeout error after RTO retries are exhausted.
#[tokio::test]
async fn connect_unreachable() {
    let _ = tracing_subscriber::fmt::try_init();

    // The retransmission timer fires after 1 s and then every 2 s, and the
    // stream gives up when it fires for the seventh time: 13 s, as in C
    // libudx. Allow 60 s.
    with_timeout(Duration::from_secs(60), async {
        let rt = create_runtime();
        let (socket, _addr) = create_bound_socket(&rt).await;
        let stream = rt.create_stream(1).await.expect("create stream");

        // Connect to a port that nobody is listening on.
        let unreachable: SocketAddr = "127.0.0.1:1".parse().unwrap();
        stream
            .connect(&socket, 99, unreachable)
            .await
            .expect("connect sets state only, should not fail");

        // Write data — no ACKs will arrive, so RTO retries will exhaust.
        let result = stream.write(b"hello unreachable").await;
        assert!(
            result.is_err(),
            "write to unreachable addr should fail after RTO retries"
        );
    })
    .await;
}

/// Connect the same stream twice — second connect should fail because the
/// processor task has already been started and the incoming channel consumed.
#[tokio::test]
async fn double_connect() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(5), async {
        let rt = create_runtime();
        let (socket, _addr) = create_bound_socket(&rt).await;
        let stream = rt.create_stream(1).await.expect("create stream");

        let target: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        stream
            .connect(&socket, 10, target)
            .await
            .expect("first connect should succeed");

        let result = stream.connect(&socket, 20, target).await;
        assert!(
            result.is_err(),
            "second connect on same stream should fail"
        );
    })
    .await;
}

#[tokio::test]
async fn shared_runtime_outlives_owner() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let handle = {
            let owner = create_runtime();
            owner.handle()
        };
        // owner dropped — shared runtime from handle must still work
        let shared = libudx::UdxRuntime::shared(handle);

        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&shared).await;

        stream_a.write(b"from shared runtime").await.expect("write");
        let received = stream_b.read().await.expect("read").expect("EOF");
        assert_eq!(received, b"from shared runtime");
    })
    .await;
}
