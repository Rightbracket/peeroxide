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

// ── While the hook is being asked ───────────────────────────────────────────────
//
// The hook is taken out of the stream before it is called, and the stream is
// connected only once it has answered. Until then the stream still waits on
// it.

/// A stream that waits on a hook which, once called, tells `called` and
/// answers `lets_in` only once `answer` is sent to.
async fn listening_on_a_slow_hook(
    rt: &UdxRuntime,
    lets_in: bool,
) -> (UdxStream, UdxSocket, SocketAddr, tokio::sync::oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (socket, addr) = create_bound_socket(rt).await;
    let stream = rt.create_stream(LISTENER).await.expect("create the listening stream");
    let (called_tx, called) = tokio::sync::oneshot::channel();
    let (answer, answer_rx) = std::sync::mpsc::channel::<()>();
    stream
        .set_firewall_hook(&socket, DIALER, move |_, _, _| {
            let _ = called_tx.send(());
            let _ = answer_rx.recv();
            lets_in
        })
        .expect("set_firewall_hook");
    (stream, socket, addr, called, answer)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_the_firewall_hook_is_being_asked_waits_for_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr, called, answer) = listening_on_a_slow_hook(&rt, true).await;
        let mut listener = stream.into_async_stream();
        listener.write_all(b"hello").await.expect("write before the hook fires");

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        called.await.expect("the hook is called");

        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown returned {:?} while the hook was being asked, with nothing delivered",
            shutdown.await
        );

        answer.send(()).expect("answer the hook");
        let mut received = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), dialer.read_to_end(&mut received))
            .await
            .expect("the end of the stream arrives")
            .expect("read to the end of the stream");
        assert_eq!(received, b"hello");
        shutdown
            .await
            .expect("shutdown task")
            .expect("shutdown, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_while_the_firewall_hook_is_being_asked_is_taken() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr, called, answer) = listening_on_a_slow_hook(&rt, true).await;
        let mut listener = stream.into_async_stream();

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        called.await.expect("the hook is called");

        listener
            .write_all(b"hello")
            .await
            .expect("a write while the hook is being asked");
        answer.send(()).expect("answer the hook");
        listener.shutdown().await.expect("shutdown");

        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert_eq!(received, b"hello");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_the_firewall_hook_is_being_asked_fails_when_it_turns_the_remote_away() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr, called, answer) = listening_on_a_slow_hook(&rt, false).await;
        let mut listener = stream.into_async_stream();
        listener.write_all(b"hello").await.expect("write before the hook fires");

        let (_dialer, _dialer_socket) = dial(&rt, addr).await;
        called.await.expect("the hook is called");

        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!shutdown.is_finished(), "shutdown returned while the hook was being asked");

        answer.send(()).expect("answer the hook");
        let result = shutdown.await.expect("shutdown task");
        assert!(result.is_err(), "nothing was delivered, and shutdown returned {result:?}");
    })
    .await;
}

// ── `UdxStream::shutdown_graceful` ──────────────────────────────────────────────
//
// Sends the end of the stream after what was written, on a stream that waits
// on its firewall hook too, once the hook lets a remote in.

#[tokio::test]
async fn graceful_shutdown_before_the_firewall_hook_fires_waits_for_a_remote() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, _addr) = listening(&rt, true).await;

        let shutdown = tokio::time::timeout(Duration::from_millis(500), stream.shutdown_graceful()).await;
        assert!(shutdown.is_err(), "shutdown_graceful returned {shutdown:?} with no remote let in");
    })
    .await;
}

#[tokio::test]
async fn graceful_shutdown_before_the_firewall_hook_fires_sends_the_end_after_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr) = listening(&rt, true).await;
        let stream = std::sync::Arc::new(stream);
        let write = tokio::spawn({
            let stream = stream.clone();
            async move { stream.write(b"hello").await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let shutdown = tokio::spawn({
            let stream = stream.clone();
            async move { stream.shutdown_graceful().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown_graceful returned with nothing delivered");

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert_eq!(received, b"hello");
        write.await.expect("write task").expect("write, once acknowledged");
        shutdown
            .await
            .expect("shutdown task")
            .expect("shutdown_graceful, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test]
async fn graceful_shutdown_with_nothing_written_ends_the_stream_once_a_remote_is_let_in() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr) = listening(&rt, true).await;
        let shutdown = tokio::spawn(async move { stream.shutdown_graceful().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown_graceful returned with no remote let in");

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("the remote reads the end of the stream");
        assert!(received.is_empty(), "{received:?}");
        shutdown
            .await
            .expect("shutdown task")
            .expect("shutdown_graceful, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test]
async fn graceful_shutdown_before_the_firewall_hook_fails_when_the_hook_turns_the_remote_away() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream, _socket, addr) = listening(&rt, false).await;
        let shutdown = tokio::spawn(async move { stream.shutdown_graceful().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown_graceful returned with no remote let in");

        let (_dialer, _dialer_socket) = dial(&rt, addr).await;
        let result = shutdown.await.expect("shutdown task");
        assert!(result.is_err(), "no remote was let in, and shutdown_graceful returned {result:?}");
    })
    .await;
}

#[tokio::test]
async fn graceful_shutdown_of_a_stream_neither_connected_nor_waiting_on_a_hook_fails() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let stream = rt.create_stream(LISTENER).await.expect("create a stream");

        let error = tokio::time::timeout(Duration::from_millis(500), stream.shutdown_graceful())
            .await
            .expect("it does not wait")
            .expect_err("the stream is not connected");
        assert!(error.to_string().contains("stream not connected"), "{error}");
    })
    .await;
}

#[tokio::test]
async fn graceful_shutdown_of_a_connected_stream_sends_the_end_after_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) = common::create_connected_pair(&rt).await;
        stream_a.write(b"hello").await.expect("write");
        stream_a.shutdown_graceful().await.expect("shutdown_graceful");

        let data = stream_b.read().await.expect("read").expect("data");
        assert_eq!(data, b"hello");
        assert_eq!(stream_b.read().await.expect("read"), None, "the end of the stream");
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
