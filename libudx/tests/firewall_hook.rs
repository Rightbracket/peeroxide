#![deny(clippy::all)]

mod common;

use common::{create_bound_socket, create_runtime, with_timeout};
use libudx::{UdxAsyncStream, UdxRuntime, UdxSocket, UdxStream};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn firewall_hook_accepts_first_packet() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();

        let (socket_a, addr_a) = create_bound_socket(&rt).await;
        let (socket_b, _addr_b) = create_bound_socket(&rt).await;

        let id_a = 10u32;
        let id_b = 20u32;

        let mut stream_a = rt.create_stream(id_a).await.expect("create stream_a");
        stream_a
            .set_firewall_hook(&socket_a, id_b, |_, _, _| true)
            .expect("set_firewall_hook");

        let stream_b = rt.create_stream(id_b).await.expect("create stream_b");
        stream_b
            .connect(&socket_b, id_a, addr_a)
            .await
            .expect("stream_b connect");

        let msg = b"hello from firewall hook test";
        stream_b.write(msg).await.expect("stream_b write");

        let data = stream_a
            .read()
            .await
            .expect("stream_a read")
            .expect("stream_a unexpected EOF");
        assert_eq!(&data[..], msg.as_ref());
    })
    .await;
}

// ── Written to, and shut down, before the hook fires ────────────────────────────
//
// A stream that waits on its firewall hook takes writes, and sends them once
// the hook has let a remote in. Until then nothing was delivered.

const LISTENER: u32 = 10;
const DIALER: u32 = 20;

/// A stream that waits on its firewall hook, and the socket it listens on.
/// `lets_in` is what the hook answers the first remote.
async fn listening(rt: &UdxRuntime, lets_in: bool) -> (UdxStream, UdxSocket, SocketAddr) {
    let (socket, addr) = create_bound_socket(rt).await;
    let stream = rt.create_stream(LISTENER).await.expect("create the listening stream");
    stream
        .set_firewall_hook(&socket, DIALER, move |_, _, _| lets_in)
        .expect("set_firewall_hook");
    (stream, socket, addr)
}

/// Such a stream with `hello` written to it through the async adapter.
async fn listening_with_hello_written(
    rt: &UdxRuntime,
    lets_in: bool,
) -> (UdxAsyncStream, UdxSocket, SocketAddr) {
    let (stream, socket, addr) = listening(rt, lets_in).await;
    let mut listener = stream.into_async_stream();
    listener.write_all(b"hello").await.expect("write before the hook fires");
    (listener, socket, addr)
}

/// A stream connected to the listening one. Its first packet is what fires
/// the hook.
async fn dial(rt: &UdxRuntime, listener: SocketAddr) -> (UdxAsyncStream, UdxSocket) {
    let (socket, _) = create_bound_socket(rt).await;
    let stream = rt.create_stream(DIALER).await.expect("create the dialing stream");
    stream.connect(&socket, LISTENER, listener).await.expect("connect");
    let mut dialer = stream.into_async_stream();
    dialer.write_all(b"knock").await.expect("write the first packet");
    (dialer, socket)
}

#[tokio::test]
async fn shutdown_before_the_firewall_hook_fires_waits_for_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, _addr) = listening_with_hello_written(&rt, true).await;

        // No remote has shown up, so nothing was delivered, and nothing is
        // acknowledged.
        let shutdown = tokio::time::timeout(Duration::from_millis(500), listener.shutdown()).await;
        assert!(shutdown.is_err(), "shutdown returned {shutdown:?} with nothing delivered");
    })
    .await;
}

#[tokio::test]
async fn shutdown_before_the_firewall_hook_fires_returns_at_once_when_nothing_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, _addr) = listening(&rt, true).await;
        let mut listener = stream.into_async_stream();

        tokio::time::timeout(Duration::from_millis(500), listener.shutdown())
            .await
            .expect("there is nothing to wait for")
            .expect("shutdown");
    })
    .await;
}

#[tokio::test]
async fn what_was_written_before_the_firewall_hook_fired_arrives_and_shutdown_then_returns() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, addr) = listening_with_hello_written(&rt, true).await;
        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned with nothing delivered");

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert_eq!(received, b"hello");
        shutdown
            .await
            .expect("shutdown task")
            .expect("shutdown, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test]
async fn shutdown_before_the_firewall_hook_fails_when_the_hook_turns_the_remote_away() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, addr) = listening_with_hello_written(&rt, false).await;
        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned with nothing delivered");

        let (_dialer, _dialer_socket) = dial(&rt, addr).await;
        let result = shutdown.await.expect("shutdown task");
        assert!(result.is_err(), "nothing was delivered, and shutdown returned {result:?}");
    })
    .await;
}

#[tokio::test]
async fn a_stream_dropped_before_its_firewall_hook_fires_still_delivers_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (listener, _socket, addr) = listening_with_hello_written(&rt, true).await;
        // Let go of without `shutdown`: what was written is still owed.
        drop(listener);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        let mut received = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(3), dialer.read_exact(&mut received))
            .await
            .expect("what was written before the drop arrives")
            .expect("read");
        assert_eq!(&received, b"hello");
    })
    .await;
}

#[tokio::test]
async fn shutdown_of_the_stream_itself_fails_at_once_before_the_firewall_hook_fires() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, _addr) = listening(&rt, true).await;

        // `UdxStream::shutdown` does not wait for a remote to be let in.
        let error = tokio::time::timeout(Duration::from_millis(500), stream.shutdown())
            .await
            .expect("it does not wait")
            .expect_err("the stream is not connected");
        assert!(error.to_string().contains("stream not connected"), "{error}");
    })
    .await;
}
