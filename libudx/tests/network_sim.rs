#![deny(clippy::all)]

mod common;

use common::proxy::{DirectionConfig, ProxyConfig};
use common::{create_lossy_pair, create_runtime, random_payload, verify_payload, with_timeout};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

const LONG_TIMEOUT: Duration = Duration::from_secs(300);

/// What IPv4 and UDP add to a datagram. A stream's MTU is the size of the IP
/// packet, and the proxy measures the datagram inside it.
const IPV4_UDP_HEADERS: usize = 20 + 8;

#[tokio::test]
async fn proxy_passthrough_no_impairments() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let config = ProxyConfig::default();

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 256 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
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

        writer.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn retransmission_under_loss() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // Loss A→B only: data packets drop, ACKs flow cleanly.
        let config = ProxyConfig {
            a_to_b: DirectionConfig {
                loss_rate: 0.05,
                ..Default::default()
            },
            b_to_a: DirectionConfig::default(),
        };

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 256 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
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

        writer.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn rtt_under_delay() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let config = ProxyConfig::symmetric(DirectionConfig {
            delay: Duration::from_millis(50),
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let start = Instant::now();
        stream_a.write(b"ping").await.expect("write");
        let elapsed = start.elapsed();

        let data = stream_b.read().await.expect("read").expect("EOF");
        assert_eq!(data, b"ping");

        // A→proxy(50ms)→B + B→proxy(50ms)→A ACK ≈ 100ms round-trip
        assert!(
            elapsed >= Duration::from_millis(80),
            "write completed too fast ({elapsed:?}), expected ≥80ms with 50ms delay each direction"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "write took too long ({elapsed:?}), expected <5s"
        );
    })
    .await;
}

#[tokio::test]
async fn receive_window_respected() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // Proxy rewrites recv_window in B→A packets to 4096 bytes (~3 packets).
        // Sender A should respect this and limit inflight accordingly.
        let config = ProxyConfig {
            a_to_b: DirectionConfig::default(),
            b_to_a: DirectionConfig {
                recv_window_override: Some(4096),
                ..Default::default()
            },
        };

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 512 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
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

        writer.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn congestion_recovery_after_burst_loss() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // 10% loss A→B only: high enough to trigger congestion recovery,
        // but ACKs flow cleanly so the sender can detect losses via SACK.
        let config = ProxyConfig {
            a_to_b: DirectionConfig {
                loss_rate: 0.10,
                ..Default::default()
            },
            b_to_a: DirectionConfig::default(),
        };

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 256 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
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

        writer.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn reorder_handling() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        let config = ProxyConfig::symmetric(DirectionConfig {
            reorder_rate: 0.10,
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 1024 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
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

        writer.await.expect("writer panicked");
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn mtu_probing_discovers_path_mtu() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // A path whose MTU is 1400: an IP packet of 1400 bytes is a datagram
        // of 1372. The probes of 1232, 1264 and so on to 1392 arrive, and
        // the one of 1424 is dropped three times, so the search ends at 1392.
        let config = ProxyConfig::symmetric(DirectionConfig {
            max_packet_size: Some(1400 - IPV4_UDP_HEADERS),
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 512 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 64 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
                stream_a
                    .write(&payload_clone[offset..end])
                    .await
                    .expect("write");
                offset = end;
            }
            stream_a
        });

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            let chunk = stream_b.read().await.expect("read").expect("EOF");
            received.extend_from_slice(&chunk);
        }

        let stream_a = writer.await.expect("writer panicked");
        verify_payload(&payload, &received);

        assert_eq!(
            stream_a.effective_mtu(),
            1392,
            "the search should end at the largest size tried that the path carries"
        );
    })
    .await;
}

#[tokio::test]
async fn mtu_probing_stays_at_base_when_clamped() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // A path whose MTU is the starting size, 1200: the first probe, of
        // 1232, is dropped three times, and the search ends where it began.
        let config = ProxyConfig::symmetric(DirectionConfig {
            max_packet_size: Some(1200 - IPV4_UDP_HEADERS),
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 64 * 1024;
        let payload = random_payload(total);

        let payload_clone = payload.clone();
        let writer = tokio::spawn(async move {
            let chunk = 32 * 1024;
            let mut offset = 0;
            while offset < payload_clone.len() {
                let end = (offset + chunk).min(payload_clone.len());
                stream_a
                    .write(&payload_clone[offset..end])
                    .await
                    .expect("write");
                offset = end;
            }
            stream_a
        });

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            let chunk = stream_b.read().await.expect("read").expect("EOF");
            received.extend_from_slice(&chunk);
        }

        let stream_a = writer.await.expect("writer panicked");
        verify_payload(&payload, &received);

        assert_eq!(
            stream_a.effective_mtu(),
            1200,
            "MTU should stay at base when path MTU = 1200"
        );
    })
    .await;
}

#[tokio::test]
async fn destroy_during_pending_write() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();

        let config = ProxyConfig::symmetric(DirectionConfig {
            delay: Duration::from_millis(200),
            ..Default::default()
        });

        let (stream_a, stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let writer = tokio::spawn(async move {
            stream_a.write(b"hello from a").await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        stream_b.destroy().await.expect("destroy");

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            writer,
        )
        .await
        .expect("writer timed out — pending write was not drained")
        .expect("writer task panicked");

        assert!(
            result.is_err(),
            "write should fail when remote peer destroys during pending write"
        );
    })
    .await;
}

#[tokio::test]
async fn adapter_writes_do_not_wait_for_acknowledgements() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();

        // A quarter of a second each way: an acknowledgement takes half a
        // second to come back.
        let config = ProxyConfig::symmetric(DirectionConfig {
            delay: Duration::from_millis(250),
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;
        let mut writer = stream_a.into_async_stream();

        // Fifty messages, written and flushed one by one, are taken before
        // the first of them can have been acknowledged.
        let message = [7u8; 1000];
        let started = Instant::now();
        for _ in 0..50 {
            writer.write_all(&message).await.expect("write");
            writer.flush().await.expect("flush");
        }
        let taken = started.elapsed();
        assert!(
            taken < Duration::from_millis(250),
            "fifty writes took {taken:?}: they waited for acknowledgements"
        );

        let mut received = 0;
        while received < 50 * message.len() {
            received += stream_b.read().await.expect("read").expect("EOF").len();
        }

        // The end of the stream is acknowledged after all of them.
        writer.shutdown().await.expect("shutdown");
    })
    .await;
}

#[tokio::test]
async fn adapter_asks_its_writer_to_wait_at_the_high_water_mark() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let config = ProxyConfig::symmetric(DirectionConfig {
            delay: Duration::from_millis(250),
            ..Default::default()
        });

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;
        let mut writer = stream_a.into_async_stream();

        // More than the stream takes unacknowledged: 262,144 bytes on top
        // of a window of ten packets.
        let total = 600 * 1024;
        let payload = random_payload(total);
        let payload_clone = payload.clone();

        let started = Instant::now();
        let writing = tokio::spawn(async move {
            writer.write_all(&payload_clone).await.expect("write");
            writer.shutdown().await.expect("shutdown");
            started.elapsed()
        });

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            let chunk = stream_b.read().await.expect("read").expect("EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);

        // The writer had to wait for acknowledgements to make room.
        let taken = writing.await.expect("writer panicked");
        assert!(
            taken >= Duration::from_millis(500),
            "600 KiB were taken in {taken:?}, without waiting for room"
        );
    })
    .await;
}

#[tokio::test]
async fn what_was_written_arrives_after_the_adapter_is_dropped() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let runtime = create_runtime();
        // Loss A→B only: data packets drop, ACKs flow cleanly.
        let config = ProxyConfig {
            a_to_b: DirectionConfig {
                loss_rate: 0.05,
                ..Default::default()
            },
            b_to_a: DirectionConfig::default(),
        };

        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;

        let total = 256 * 1024;
        let payload = random_payload(total);
        {
            let mut writer = stream_a.into_async_stream();
            writer.write_all(&payload).await.expect("write");
            // Dropped with none of it acknowledged, and most of it not
            // sent yet. One packet in twenty will have to go out again.
        }

        let mut received = Vec::with_capacity(total);
        while received.len() < total {
            let chunk = stream_b.read().await.expect("read").expect("EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);
    })
    .await;
}

#[tokio::test]
async fn shutdown_of_an_adapter_whose_remote_is_gone_fails() {
    let _ = tracing_subscriber::fmt::try_init();

    // The stream gives up when its retransmission timer fires for the
    // seventh time: 13 s. Allow 60 s.
    with_timeout(Duration::from_secs(60), async {
        let runtime = create_runtime();
        // Nothing gets through.
        let config = ProxyConfig::symmetric(DirectionConfig {
            loss_rate: 1.0,
            ..Default::default()
        });

        let (stream_a, _stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;
        let mut writer = stream_a.into_async_stream();

        // The write is taken, as nothing says yet that it cannot arrive.
        writer.write_all(b"into the void").await.expect("write");
        writer.flush().await.expect("flush");

        // Ending the stream is how a writer learns whether it did.
        let error = writer.shutdown().await.expect_err("shutdown succeeded");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);

        // And from then on every write says so.
        let error = writer.write_all(b"more").await.expect_err("write succeeded");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        let error = writer.flush().await.expect_err("flush succeeded");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    })
    .await;
}

/// Sixteen MiB from one end to the other through the proxy, and how long
/// it took to arrive.
async fn time_to_carry_16_mib(config: ProxyConfig) -> Duration {
    let runtime = create_runtime();
    let (stream_a, mut stream_b, _sa, _sb, _proxy) = create_lossy_pair(&runtime, config).await;

    let total = 16 * 1024 * 1024;
    let payload = random_payload(total);
    let started = Instant::now();

    // Written through the adapter, which does not wait for each write to be
    // acknowledged: the path's length would set the speed.
    let mut writer = stream_a.into_async_stream();
    let payload_clone = payload.clone();
    let writer = tokio::spawn(async move {
        writer.write_all(&payload_clone).await.expect("write");
        writer.flush().await.expect("flush");
        writer
    });

    let mut received = Vec::with_capacity(total);
    while received.len() < total {
        let chunk = stream_b.read().await.expect("read").expect("EOF");
        received.extend_from_slice(&chunk);
    }
    let took = started.elapsed();

    writer.await.expect("writer panicked");
    verify_payload(&payload, &received);
    took
}

/// A path a quarter of a second long whose data packets arrive up to 12 ms
/// early or late, and so out of order, and of which one in 500 is lost.
/// Its data arrives at no less than half the speed of a path that does
/// neither (#47).
#[tokio::test]
async fn a_long_path_that_reorders_and_loses_a_few_packets_carries_data_at_speed() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(LONG_TIMEOUT, async {
        let one_way = DirectionConfig {
            delay: Duration::from_millis(125),
            ..Default::default()
        };
        let clean = time_to_carry_16_mib(ProxyConfig::symmetric(one_way.clone())).await;
        let jittery = time_to_carry_16_mib(ProxyConfig {
            a_to_b: DirectionConfig {
                jitter: Duration::from_millis(12),
                loss_rate: 0.002,
                ..one_way.clone()
            },
            b_to_a: one_way,
        })
        .await;

        assert!(
            jittery <= clean * 2,
            "{jittery:?} through the path that reorders, against {clean:?}"
        );
    })
    .await;
}

/// Writes that wait for their acknowledgements wait for room too, on a
/// stream with a write buffer limit. One that does not fit has taken
/// nothing while it waits: given up, none of it is sent.
#[tokio::test]
async fn a_write_that_does_not_fit_under_the_write_limit_waits_and_takes_nothing() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        // A round trip of 200 ms.
        let config = ProxyConfig::symmetric(DirectionConfig {
            delay: Duration::from_millis(100),
            ..Default::default()
        });
        let (stream_a, mut stream_b, _sa, _sb, _proxy) =
            create_lossy_pair(&runtime, config).await;
        stream_a.set_write_buffer_limit(Some(8_192));
        assert_eq!(stream_a.write_buffer_limit(), Some(8_192));

        // Two writes that are not both under the limit. The second waits
        // for the first to be acknowledged, and is given up before that.
        let data = [7u8; 6_000];
        let (first, second) = tokio::join!(
            stream_a.write(&data),
            tokio::time::timeout(Duration::from_millis(50), stream_a.write(&data)),
        );
        first.expect("first write");
        assert!(second.is_err(), "the second write did not wait for room");

        // Only the first arrives.
        let mut received = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while let Ok(read) = tokio::time::timeout_at(deadline, stream_b.read()).await {
            received += read.expect("read").expect("EOF").len();
        }
        assert_eq!(received, data.len());
    })
    .await;
}
