#![deny(clippy::all)]

mod common;

use common::{create_connected_pair, create_runtime, random_payload, verify_payload, with_timeout};
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(15);

// T1: Baseline — a write that fills one packet at the starting MTU
#[tokio::test]
async fn mtu_baseline_payload_transfer() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(TEST_TIMEOUT, async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _sa, _sb) = create_connected_pair(&runtime).await;

        // MTU_BASE (1200) less the IPv4 header (20), the UDP header (8) and
        // the UDX header (20).
        let payload = random_payload(1152);
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

// T2: Large write is chunked correctly
#[tokio::test]
async fn mtu_large_write_chunked() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _sa, _sb) = create_connected_pair(&runtime).await;

        let payload = random_payload(5000);
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

// T4: MTU probing increases payload size — C1b: MTU probing implemented
#[tokio::test]
async fn mtu_probing_increases_effective_mtu() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _sa, _sb) = create_connected_pair(&runtime).await;

        let payload = random_payload(64 * 1024);
        stream_a.write(&payload).await.expect("write failed");

        let mut received = Vec::new();
        while received.len() < payload.len() {
            let chunk = stream_b.read().await.expect("read failed").expect("unexpected EOF");
            received.extend_from_slice(&chunk);
        }
        verify_payload(&payload, &received);

        assert!(stream_a.effective_mtu() > 1200, "MTU should have increased above 1200 after probing");
    })
    .await;
}

// T9: Probing doesn't interfere with SACK
#[tokio::test]
async fn mtu_probing_does_not_interfere_with_sack() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _sa, _sb) = create_connected_pair(&runtime).await;

        let payload = random_payload(100 * 1024);
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

// T10: Data integrity across MTU change boundary
#[tokio::test]
async fn mtu_data_integrity_across_mtu_change() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        let runtime = create_runtime();
        let (stream_a, mut stream_b, _sa, _sb) = create_connected_pair(&runtime).await;

        let payload = random_payload(100 * 1024);
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
