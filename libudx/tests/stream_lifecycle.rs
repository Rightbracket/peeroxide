#![deny(clippy::all)]

mod common;

use common::{
    create_connected_pair, create_runtime, random_payload, verify_payload, with_timeout,
    DEFAULT_TIMEOUT,
};
use std::time::Duration;

#[tokio::test]
async fn stream_connect_basic() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (_stream_a, _stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;
    })
    .await;
}

#[tokio::test]
async fn stream_write_read_small() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = random_payload(100);
        stream_a.write(&payload).await.expect("write failed");

        let received = stream_b.read().await.expect("read failed").expect("unexpected EOF");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn stream_write_read_1kb() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = random_payload(1024);
        stream_a.write(&payload).await.expect("write failed");

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn stream_write_read_64kb() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = random_payload(64 * 1024);
        stream_a.write(&payload).await.expect("write failed");

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn stream_write_read_1mb() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = random_payload(1024 * 1024);
        stream_a.write(&payload).await.expect("write failed");

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn stream_write_read_10mb() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(60), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = random_payload(10 * 1024 * 1024);
        stream_a.write(&payload).await.expect("write failed");

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn stream_multiple_writes() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let mut all_sent = Vec::new();
        for i in 0u32..100 {
            let size = ((i % 10) as usize + 1) * 100;
            let payload = random_payload(size);
            all_sent.extend_from_slice(&payload);
            stream_a.write(&payload).await.expect("write failed");
        }

        let mut all_received = Vec::new();
        while all_received.len() < all_sent.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            all_received.extend_from_slice(&chunk);
        }
        verify_payload(&all_sent, &all_received);
    })
    .await;
}

#[tokio::test]
async fn stream_bidirectional() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload_a = random_payload(10_000);
        let payload_b = random_payload(10_000);

        let payload_a_clone = payload_a.clone();
        let payload_b_clone = payload_b.clone();

        let write_a = tokio::spawn({
            let sa = stream_a;
            let pb = payload_a_clone;
            async move {
                sa.write(&pb).await.expect("write_a failed");
                sa
            }
        });

        let write_b = tokio::spawn({
            let sb = stream_b;
            let pa = payload_b_clone;
            async move {
                sb.write(&pa).await.expect("write_b failed");
                sb
            }
        });

        let mut stream_a = write_a.await.expect("join write_a");
        let mut stream_b = write_b.await.expect("join write_b");

        let mut recv_a = Vec::new();
        while recv_a.len() < payload_b.len() {
            let chunk = stream_a.read().await.expect("read_a failed").expect("unexpected EOF");
            recv_a.extend_from_slice(&chunk);
        }
        verify_payload(&payload_b, &recv_a);

        let mut recv_b = Vec::new();
        while recv_b.len() < payload_a.len() {
            let chunk = stream_b.read().await.expect("read_b failed").expect("unexpected EOF");
            recv_b.extend_from_slice(&chunk);
        }
        verify_payload(&payload_a, &recv_b);
    })
    .await;
}

#[tokio::test]
async fn stream_shutdown_graceful() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let payload = b"before shutdown";
        stream_a.write(payload).await.expect("write failed");

        let received = stream_b.read().await.expect("read failed").expect("unexpected EOF");
        assert_eq!(received, payload);

        stream_a.shutdown().await.expect("shutdown failed");

        let eof = stream_b.read().await.expect("read after shutdown failed");
        assert!(eof.is_none(), "expected EOF after remote shutdown");
    })
    .await;
}

#[tokio::test]
async fn stream_read_after_eof() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        stream_a.shutdown().await.expect("shutdown failed");

        let first = stream_b.read().await.expect("first read failed");
        assert!(first.is_none(), "expected None on first read after EOF");

        let second = stream_b.read().await;
        assert!(second.is_err() || second.unwrap().is_none(),
            "expected error or None on repeated read after EOF");
    })
    .await;
}

#[tokio::test]
async fn stream_destroy_immediate() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, _stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        stream_a.destroy().await.expect("destroy failed");
    })
    .await;
}

#[tokio::test]
async fn stream_write_after_shutdown() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, _stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        stream_a.shutdown().await.expect("shutdown failed");

        let result = stream_a.write(b"after shutdown").await;
        assert!(result.is_err(), "write after shutdown should fail");
    })
    .await;
}

// ── UdxStreamHandle ──────────────────────────────────────────────────────────

#[tokio::test]
async fn a_handle_destroys_its_stream_and_the_remote_sees_it() {
    use tokio::io::AsyncReadExt;
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let mut a = stream_a.into_async_stream();
        let mut b = stream_b.into_async_stream();
        let (handle_a, handle_b) = (a.handle(), b.handle());
        assert!(!handle_a.is_closed() && !handle_b.is_closed());

        handle_a.clone().destroy();
        handle_a.closed().await;
        let mut buf = [0u8; 8];
        let err = a.read(&mut buf).await.expect_err("a read on a destroyed stream");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);

        // The remote is told, and is over too.
        handle_b.closed().await;
        assert_eq!(b.read(&mut buf).await.unwrap_or(0), 0);

        // Again, and after it is over: nothing happens.
        handle_a.destroy();
        assert!(handle_a.is_closed());
    })
    .await;
}

#[tokio::test]
async fn a_handle_sees_its_stream_let_go_of() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;

        // An adapter let go of with nothing left to deliver.
        let a = stream_a.into_async_stream();
        let handle = a.handle();
        drop(a);
        handle.closed().await;
        assert!(handle.is_closed());

        // A stream dropped as it is.
        let handle = stream_b.handle();
        drop(stream_b);
        handle.closed().await;
        assert!(handle.is_closed());
    })
    .await;
}

#[tokio::test]
async fn a_handle_sees_its_stream_give_up_on_a_silent_remote() {
    use tokio::io::AsyncWriteExt;
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(120), async {
        let runtime = create_runtime();
        let (stream_a, stream_b, _socket_a, socket_b) = create_connected_pair(&runtime).await;
        let mut a = stream_a.into_async_stream();
        let handle = a.handle();
        // Nothing answers for b any more.
        drop(stream_b);
        drop(socket_b);

        a.write_all(b"anyone there?").await.expect("write");
        handle.closed().await;
        assert!(handle.is_closed());
    })
    .await;
}

#[tokio::test]
async fn a_handle_sees_its_stream_ended_both_ways_while_its_owner_holds_it() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let mut a = stream_a.into_async_stream();
        let mut b = stream_b.into_async_stream();
        let (handle_a, handle_b) = (a.handle(), b.handle());

        // One end ended: not over.
        a.shutdown().await.expect("a shutdown");
        let mut buf = [0u8; 8];
        assert_eq!(b.read(&mut buf).await.expect("b read"), 0);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!handle_a.is_closed() && !handle_b.is_closed());

        // Both: over, with both owners still holding their streams.
        b.shutdown().await.expect("b shutdown");
        assert_eq!(a.read(&mut buf).await.expect("a read"), 0);
        handle_a.closed().await;
        handle_b.closed().await;
        drop((a, b));
    })
    .await;
}

#[tokio::test]
async fn a_destroy_reaches_the_remote_however_the_owner_lets_go() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();

        // Destroyed, then the stream dropped at once.
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let remote = stream_b.handle();
        stream_a.handle().destroy();
        drop(stream_a);
        remote.closed().await;

        // The adapter let go of, then destroyed.
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let remote = stream_b.handle();
        let a = stream_a.into_async_stream();
        let handle = a.handle();
        drop(a);
        handle.destroy();
        remote.closed().await;
        drop(stream_b);
    })
    .await;
}

#[tokio::test]
async fn a_stream_destroyed_before_it_connects_stays_over() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket, addr) = common::create_bound_socket(&runtime).await;
        let stream = runtime.create_stream(7).await.expect("create");
        stream.handle().destroy();
        assert!(stream.connect(&socket, 8, addr).await.is_err());
        assert!(stream.set_firewall_hook(&socket, 8, |_, _, _| true).is_err());

        // Dropped before it connects, with nothing else to end it: over.
        let stream = runtime.create_stream(9).await.expect("create");
        let handle = stream.handle();
        drop(stream);
        handle.closed().await;
    })
    .await;
}

#[tokio::test]
async fn an_old_owner_leaves_the_stream_that_took_its_id_alone() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket_a, addr_a) = common::create_bound_socket(&runtime).await;
        let (socket_b, addr_b) = common::create_bound_socket(&runtime).await;

        let old = runtime.create_stream(1).await.expect("create");
        old.connect(&socket_a, 2, addr_b).await.expect("connect old");
        let handle = old.handle();
        handle.destroy();
        handle.closed().await;

        // Id 1 again, on the same socket, once the old stream has left it.
        let new = runtime.create_stream(1).await.expect("create");
        let mut tries = 0;
        while new.connect(&socket_a, 3, addr_b).await.is_err() {
            tries += 1;
            assert!(tries < 100, "id 1 never came free");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // A peer of another id: the DESTROY the old stream sent was for 2.
        let peer = runtime.create_stream(3).await.expect("create");
        peer.connect(&socket_b, 1, addr_a).await.expect("connect peer");

        drop(old);

        let mut new = new.into_async_stream();
        let mut peer = peer.into_async_stream();
        peer.write_all(b"still here").await.expect("write");
        let mut buf = [0u8; 10];
        new.read_exact(&mut buf).await.expect("the new stream lost its place");
        assert_eq!(&buf, b"still here");
    })
    .await;
}

#[tokio::test]
async fn a_destroyed_stream_says_so() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, _stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let mut a = stream_a.into_async_stream();
        a.handle().destroy();
        let mut buf = [0u8; 1];
        let err = a.read(&mut buf).await.expect_err("read");
        assert_eq!(err.to_string(), "stream destroyed");
        let err = a.write_all(b"x").await.expect_err("write");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted);
    })
    .await;
}

#[tokio::test]
async fn a_write_waiting_on_a_destroyed_stream_fails_as_aborted() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket, _) = common::create_bound_socket(&runtime).await;
        // A remote that never acknowledges anything.
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
        let stream = std::sync::Arc::new(runtime.create_stream(1).await.expect("create"));
        stream
            .connect(&socket, 2, silent.local_addr().expect("addr"))
            .await
            .expect("connect");

        let writer = tokio::spawn({
            let stream = stream.clone();
            async move { stream.write(b"waits for an ack").await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        stream.handle().destroy();
        match writer.await.expect("join") {
            Err(libudx::UdxError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionAborted),
            other => panic!("expected ConnectionAborted, got {other:?}"),
        }
    })
    .await;
}

#[tokio::test]
async fn a_socket_refuses_an_id_another_stream_holds() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        let (socket, _) = common::create_bound_socket(&runtime).await;
        let (_, remote) = common::create_bound_socket(&runtime).await;
        let first = runtime.create_stream(5).await.expect("create");
        first.connect(&socket, 6, remote).await.expect("connect");

        let second = runtime.create_stream(5).await.expect("create");
        let err = second.connect(&socket, 6, remote).await.expect_err("id 5 is taken");
        assert!(matches!(err, libudx::UdxError::Io(e) if e.kind() == std::io::ErrorKind::AddrInUse));
        assert!(second.set_firewall_hook(&socket, 6, |_, _, _| true).is_err());

        // Once the first has gone, the second, left as it was, takes it.
        drop(first);
        second.connect(&socket, 6, remote).await.expect("id 5 is free");
    })
    .await;
}

#[tokio::test]
async fn a_closed_stream_is_released_once_what_arrived_is_read_or_let_go_of() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(DEFAULT_TIMEOUT, async {
        let runtime = create_runtime();
        for read_it in [true, false] {
            let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
            let mut a = stream_a.into_async_stream();
            let mut b = stream_b.into_async_stream();
            let handle = b.handle();

            a.write_all(b"unread").await.expect("write");
            // Delivered and acknowledged before the stream is destroyed,
            // which drops what was written and not yet sent.
            a.shutdown().await.expect("shutdown");
            a.handle().destroy();
            handle.closed().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(200), handle.released()).await.is_err(),
                "released with data still unread"
            );

            if read_it {
                let mut buf = Vec::new();
                b.read_to_end(&mut buf).await.expect("read");
                assert_eq!(buf, b"unread");
            } else {
                drop(b);
            }
            handle.released().await;
        }

        // Closed by its remote, with data unread: destroying it here lets go
        // of the data, and the stream is released, held and unread.
        let (stream_a, stream_b, _socket_a, _socket_b) = create_connected_pair(&runtime).await;
        let mut a = stream_a.into_async_stream();
        let b = stream_b.into_async_stream();
        let handle = b.handle();
        a.write_all(b"unread").await.expect("write");
        a.shutdown().await.expect("shutdown");
        a.handle().destroy();
        handle.closed().await;
        handle.destroy();
        handle.released().await;
        drop(b);
    })
    .await;
}
