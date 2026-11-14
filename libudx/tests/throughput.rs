#![deny(clippy::all)]

mod common;

use common::{create_connected_pair, create_runtime, random_payload, verify_payload, with_timeout};
use std::net::SocketAddr;
use std::time::Duration;

const LOCALHOST: &str = "127.0.0.1:0";

fn addr() -> SocketAddr {
    LOCALHOST.parse().expect("parse localhost")
}

#[tokio::test]
async fn throughput_sustained() {
    let _ = tracing_subscriber::fmt::try_init();

    // 50 MiB takes two to three seconds in a debug build. The limit is low
    // enough to fail if a stream falls back to a few megabytes per second.
    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        let total = 50 * 1024 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let write_handle = tokio::spawn(async move {
            let chunk_size = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk_size).min(payload_clone.len());
                stream_a
                    .write(&payload_clone[offset..end])
                    .await
                    .expect("write");
                offset = end;
            }
        });

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            let chunk = stream_b.read().await.expect("read").expect("EOF");
            received.extend_from_slice(&chunk);
        }

        write_handle.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn throughput_multiple_streams() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(120), async {
        let rt = create_runtime();

        let socket_a = rt.create_socket().await.expect("socket_a");
        socket_a.bind(addr()).await.expect("bind_a");
        let addr_a = socket_a.local_addr().await.expect("addr_a");

        let socket_b = rt.create_socket().await.expect("socket_b");
        socket_b.bind(addr()).await.expect("bind_b");
        let addr_b = socket_b.local_addr().await.expect("addr_b");

        let stream_count = 5;
        let per_stream = 10 * 1024 * 1024;

        let mut write_handles = Vec::new();
        let mut read_handles = Vec::new();

        for i in 0u32..stream_count {
            let id_a = 300 + i * 2;
            let id_b = 300 + i * 2 + 1;

            let sa = rt.create_stream(id_a).await.expect("create sa");
            let mut sb = rt.create_stream(id_b).await.expect("create sb");
            sa.connect(&socket_a, id_b, addr_b).await.expect("connect sa");
            sb.connect(&socket_b, id_a, addr_a).await.expect("connect sb");

            let payload = random_payload(per_stream + i as usize);

            let payload_for_verify = payload.clone();
            let payload_for_write = payload;

            write_handles.push(tokio::spawn(async move {
                let chunk_size = 64 * 1024;
                let mut offset = 0;
                while offset < payload_for_write.len() {
                    let end = (offset + chunk_size).min(payload_for_write.len());
                    sa.write(&payload_for_write[offset..end])
                        .await
                        .expect("write");
                    offset = end;
                }
            }));

            let expected_len = per_stream + i as usize;
            read_handles.push(tokio::spawn(async move {
                let mut received = Vec::with_capacity(expected_len);
                while received.len() < expected_len {
                    let chunk = sb.read().await.expect("read").expect("EOF");
                    received.extend_from_slice(&chunk);
                }
                verify_payload(&payload_for_verify, &received);
            }));
        }

        for h in write_handles {
            h.await.expect("writer panicked");
        }
        for h in read_handles {
            h.await.expect("reader panicked");
        }
    })
    .await;
}

#[tokio::test]
async fn heartbeat_keeps_alive() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(15), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _socket_a, _socket_b) =
            create_connected_pair(&runtime).await;

        stream_a.write(b"before idle").await.expect("write 1");
        let received = stream_b.read().await.expect("read 1").expect("EOF");
        assert_eq!(received, b"before idle");

        tokio::time::sleep(Duration::from_secs(5)).await;

        stream_a.write(b"after idle").await.expect("write 2");
        let received = stream_b.read().await.expect("read 2").expect("EOF");
        assert_eq!(received, b"after idle");
    })
    .await;
}
