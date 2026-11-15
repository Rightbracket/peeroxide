#![deny(clippy::all)]

mod common;

use common::{create_bound_socket, create_connected_pair, create_runtime, with_timeout};
use libudx::{UdxAsyncStream, UdxRuntime, UdxSocket, UdxStream};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
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

// ── While the firewall hook runs ────────────────────────────────────────────────
//
// The stream takes its hook out to call it, and is connected only once the
// hook has answered. Until then it is still waiting on the hook: what is
// written is queued, and a shutdown waits for it.

/// A hook held while it runs: it says that it has been called, and then
/// answers what it is told to.
struct HeldHook {
    called: Arc<AtomicBool>,
    answer: mpsc::Sender<bool>,
}

impl HeldHook {
    /// Wait until the hook has been called and waits for its answer.
    async fn running(&self) {
        while !self.called.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn answer(&self, lets_in: bool) {
        self.answer.send(lets_in).expect("the hook waits for its answer");
    }
}

/// A listening stream whose hook is held while it runs. The hook holds a
/// thread of the runtime, so a test needs a runtime with more than one.
async fn listening_with_a_held_hook(rt: &UdxRuntime) -> (UdxStream, UdxSocket, SocketAddr, HeldHook) {
    let (socket, addr) = create_bound_socket(rt).await;
    let stream = rt.create_stream(LISTENER).await.expect("create the listening stream");
    let called = Arc::new(AtomicBool::new(false));
    let hook_called = Arc::clone(&called);
    let (answer, answers) = mpsc::channel();
    stream
        .set_firewall_hook(&socket, DIALER, move |_, _, _| {
            // Told by a flag, not by waking a task: a task woken from here
            // could wait for this thread, which the hook holds.
            hook_called.store(true, Ordering::SeqCst);
            answers.recv().unwrap_or(false)
        })
        .expect("set_firewall_hook");
    (stream, socket, addr, HeldHook { called, answer })
}

/// Such a stream with `hello` written to it through the async adapter.
async fn held_with_hello_written(rt: &UdxRuntime) -> (UdxAsyncStream, UdxSocket, SocketAddr, HeldHook) {
    let (stream, socket, addr, hook) = listening_with_a_held_hook(rt).await;
    let mut listener = stream.into_async_stream();
    listener.write_all(b"hello").await.expect("write before the hook fires");
    (listener, socket, addr, hook)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_the_firewall_hook_runs_waits_for_what_was_written() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, addr, hook) = held_with_hello_written(&rt).await;
        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        hook.running().await;

        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned while the hook ran, with nothing delivered");

        hook.answer(true);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_while_the_firewall_hook_runs_is_delivered() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, addr, hook) = held_with_hello_written(&rt).await;
        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        hook.running().await;

        listener.write_all(b", world").await.expect("write while the hook runs");
        hook.answer(true);
        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert_eq!(received, b"hello, world");
        shutdown.await.expect("shutdown task").expect("shutdown");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_while_the_firewall_hook_runs_fails_when_the_hook_turns_the_remote_away() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (mut listener, _socket, addr, hook) = held_with_hello_written(&rt).await;
        let (_dialer, _dialer_socket) = dial(&rt, addr).await;
        hook.running().await;

        let shutdown = tokio::spawn(async move { listener.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned while the hook ran, with nothing delivered");

        hook.answer(false);
        let result = shutdown.await.expect("shutdown task");
        assert!(result.is_err(), "nothing was delivered, and shutdown returned {result:?}");
    })
    .await;
}

// ── Shut down once a remote is let in ──────────────────────────────────────────
//
// `UdxStream::shutdown_when_connected` is `shutdown` for a stream that may
// still wait on its firewall hook: the end of the stream goes out after what
// was written, once the hook lets a remote in.

#[tokio::test]
async fn shutdown_when_connected_ends_the_stream_after_what_was_written_once_a_remote_is_let_in() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (listener, _socket, addr) = listening(&rt, true).await;
        // The write is queued first: futures joined are first polled in
        // order, and each queues what it sends on its first poll.
        let both = tokio::spawn(async move {
            tokio::join!(listener.write(b"hello"), listener.shutdown_when_connected())
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!both.is_finished(), "the write or the shutdown returned with nothing delivered");

        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert_eq!(received, b"hello");
        let (written, shut_down) = both.await.expect("the task");
        written.expect("the write, once it is acknowledged");
        shut_down.expect("shutdown, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test]
async fn shutdown_when_connected_fails_when_the_hook_turns_the_remote_away() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (listener, _socket, addr) = listening(&rt, false).await;
        let shutdown = tokio::spawn(async move { listener.shutdown_when_connected().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned before a remote came");

        let (_dialer, _dialer_socket) = dial(&rt, addr).await;
        let result = shutdown.await.expect("shutdown task");
        assert!(result.is_err(), "the remote was turned away, and shutdown returned {result:?}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_when_connected_while_the_firewall_hook_runs_waits_for_the_remote() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (listener, _socket, addr, hook) = listening_with_a_held_hook(&rt).await;
        let (mut dialer, _dialer_socket) = dial(&rt, addr).await;
        hook.running().await;

        let shutdown = tokio::spawn(async move { listener.shutdown_when_connected().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown returned while the hook ran");

        hook.answer(true);
        let mut received = Vec::new();
        dialer.read_to_end(&mut received).await.expect("read to the end of the stream");
        assert!(received.is_empty(), "nothing was written, and {received:?} arrived");
        shutdown
            .await
            .expect("shutdown task")
            .expect("shutdown, once the end of the stream is acknowledged");
    })
    .await;
}

#[tokio::test]
async fn shutdown_when_connected_ends_a_connected_stream_as_shutdown_does() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) = create_connected_pair(&rt).await;
        stream_a.write(b"hello").await.expect("write");
        stream_a.shutdown_when_connected().await.expect("shutdown");
        assert_eq!(stream_b.read().await.expect("read").as_deref(), Some(&b"hello"[..]));
        assert_eq!(stream_b.read().await.expect("read the end"), None);
    })
    .await;
}

#[tokio::test]
async fn shutdown_when_connected_fails_at_once_on_a_stream_that_waits_for_nothing() {
    with_timeout(Duration::from_secs(10), async {
        let rt = create_runtime();
        let stream = rt.create_stream(LISTENER).await.expect("create a stream");

        // Neither connected nor listening: no remote can ever come.
        let error = tokio::time::timeout(Duration::from_millis(500), stream.shutdown_when_connected())
            .await
            .expect("it does not wait")
            .expect_err("the stream is not connected");
        assert!(error.to_string().contains("stream not connected"), "{error}");
    })
    .await;
}
