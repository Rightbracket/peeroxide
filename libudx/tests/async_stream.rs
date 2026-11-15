#![deny(clippy::all)]

mod common;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct VecWriter(Vec<u8>);

impl AsyncWrite for VecWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().0.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn async_stream_read_write_basic() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let payload = common::random_payload(100);
        let payload_clone = payload.clone();

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload_clone)
                .await
                .expect("write_all failed");
        });
        let read_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 100];
            async_b
                .read_exact(&mut buf)
                .await
                .expect("read_exact failed");
            buf
        });

        write_task.await.expect("write task panicked");
        let received = read_task.await.expect("read task panicked");
        common::verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn async_stream_partial_read() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let payload = common::random_payload(1000);
        let payload_clone = payload.clone();

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload_clone)
                .await
                .expect("write_all 1000 bytes failed");
            async_a
                .shutdown()
                .await
                .expect("shutdown after partial write failed");
        });
        let read_task = tokio::spawn(async move {
            let mut received = Vec::new();
            let mut buf = [0u8; 100];
            loop {
                let n = async_b.read(&mut buf).await.expect("read chunk failed");
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
            }
            received
        });

        write_task.await.expect("write task panicked");
        let received = read_task.await.expect("read task panicked");
        common::verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn async_stream_flush() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let payload = common::random_payload(32);

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload)
                .await
                .expect("write_all before flush failed");
            async_a.flush().await.expect("flush failed");
        });
        let mut buf = [0u8; 32];
        async_b
            .read_exact(&mut buf)
            .await
            .expect("read_exact in flush test failed");
        write_task.await.expect("write task panicked");
    })
    .await;
}

#[tokio::test]
async fn async_stream_shutdown() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let shutdown_task = tokio::spawn(async move {
            async_a.shutdown().await.expect("shutdown failed");
        });
        let mut buf = [0u8; 64];
        let n = async_b
            .read(&mut buf)
            .await
            .expect("read after shutdown failed");
        assert_eq!(n, 0, "expected EOF (0 bytes), got {n}");
        shutdown_task.await.expect("shutdown task panicked");
    })
    .await;
}

#[tokio::test]
async fn async_stream_eof() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let payload = common::random_payload(64);
        let payload_clone = payload.clone();

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload_clone)
                .await
                .expect("write_all for eof test failed");
            async_a
                .shutdown()
                .await
                .expect("shutdown for eof test failed");
        });

        let mut received = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = async_b
                .read(&mut buf)
                .await
                .expect("read in eof loop failed");
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
        }

        write_task.await.expect("write task panicked");
        common::verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn async_stream_tokio_copy() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(common::DEFAULT_TIMEOUT, async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        let payload = common::random_payload(1024 * 1024);
        let payload_clone = payload.clone();

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload_clone)
                .await
                .expect("write_all 1MB failed");
            async_a
                .shutdown()
                .await
                .expect("shutdown after 1MB write failed");
        });

        let mut sink = VecWriter(Vec::new());
        tokio::io::copy(&mut async_b, &mut sink)
            .await
            .expect("tokio::io::copy failed");
        let received = sink.0;

        write_task.await.expect("write task panicked");
        common::verify_payload(&payload, &received);
    })
    .await;
}

/// A reader with a limit on what its stream holds for it, and slower than
/// the writer: everything arrives, and the writer is held to its pace.
#[tokio::test]
async fn async_stream_read_buffer_limit() {
    let _ = tracing_subscriber::fmt::try_init();
    common::with_timeout(std::time::Duration::from_secs(30), async {
        let runtime = common::create_runtime();
        let (stream_a, stream_b, _socket_a, _socket_b) =
            common::create_connected_pair(&runtime).await;
        let mut async_a = stream_a.into_async_stream();
        let mut async_b = stream_b.into_async_stream();

        assert_eq!(async_b.read_buffer_limit(), None);
        async_b.set_read_buffer_limit(Some(65_536));
        assert_eq!(async_b.read_buffer_limit(), Some(65_536));

        let payload = common::random_payload(2 * 1024 * 1024);
        let payload_clone = payload.clone();

        let write_task = tokio::spawn(async move {
            async_a
                .write_all(&payload_clone)
                .await
                .expect("write_all 2MB failed");
            async_a
                .shutdown()
                .await
                .expect("shutdown after 2MB write failed");
        });

        let mut received = Vec::new();
        let mut buf = vec![0u8; 16 * 1024];
        let mut next_wait = 128 * 1024;
        loop {
            let n = async_b.read(&mut buf).await.expect("read failed");
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
            if received.len() >= next_wait {
                next_wait += 128 * 1024;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }

        write_task.await.expect("write task panicked");
        common::verify_payload(&payload, &received);
    })
    .await;
}

