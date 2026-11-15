#![deny(clippy::all)]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::Duration;

use common::{create_runtime, node_script_path, random_payload, with_timeout};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn stream_interop_large() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        run_large_transfer_test().await.expect("large transfer interop failed");
    })
    .await;
}

async fn run_large_transfer_test() -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("node")
        .arg(node_script_path("udx-large-transfer.js"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child.stdout.take().expect("no stdout");
    let mut stdin = child.stdin.take().expect("no stdin");

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let info: serde_json::Value = serde_json::from_str(line.trim())?;
    let node_port = info["port"].as_u64().expect("missing port") as u16;
    let node_local_id = info["localId"].as_u64().expect("missing localId") as u32;
    let node_remote_id = info["remoteId"].as_u64().expect("missing remoteId") as u32;

    let runtime = create_runtime();
    let socket = runtime.create_socket().await?;
    socket
        .bind("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let local_addr = socket.local_addr().await?;
    let rust_port = local_addr.port();

    let payload = random_payload(1024 * 1024);
    let expected_hash = format!("{:x}", Sha256::digest(&payload));

    let handshake = serde_json::json!({
        "port": rust_port,
        "mode": "receive_and_hash",
        "expectedSize": payload.len()
    });
    writeln!(stdin, "{handshake}")?;
    drop(stdin);

    let mut stream = runtime.create_stream(node_remote_id).await?;
    let node_addr: SocketAddr = format!("127.0.0.1:{node_port}").parse()?;
    stream.connect(&socket, node_local_id, node_addr).await?;

    let mut ready_line = String::new();
    reader.read_line(&mut ready_line)?;
    let ready: serde_json::Value = serde_json::from_str(ready_line.trim())?;
    assert!(ready["ready"].as_bool().unwrap_or(false), "node not ready");

    stream.write(&payload).await?;
    stream.shutdown().await?;

    let eof = stream.read().await?;
    assert!(eof.is_none(), "expected EOF after node ends");

    let mut result_line = String::new();
    reader.read_line(&mut result_line)?;
    let result: serde_json::Value = serde_json::from_str(result_line.trim())?;

    let received_size = result["received"].as_u64().expect("missing received");
    assert_eq!(received_size, payload.len() as u64, "Node received wrong size");

    let received_hash = result["sha256"].as_str().expect("missing sha256");
    assert_eq!(received_hash, expected_hash, "SHA256 mismatch on Node side");

    stream.destroy().await?;
    socket.close().await?;
    drop(runtime);

    let status = child.wait()?;
    assert!(status.success(), "node process exited with: {status}");
    Ok(())
}

#[tokio::test]
async fn stream_interop_bidirectional() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(30), async {
        run_bidirectional_interop_test().await.expect("bidirectional interop failed");
    })
    .await;
}

async fn run_bidirectional_interop_test() -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("node")
        .arg(node_script_path("udx-large-transfer.js"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child.stdout.take().expect("no stdout");
    let mut stdin = child.stdin.take().expect("no stdin");

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let info: serde_json::Value = serde_json::from_str(line.trim())?;
    let node_port = info["port"].as_u64().expect("missing port") as u16;
    let node_local_id = info["localId"].as_u64().expect("missing localId") as u32;
    let node_remote_id = info["remoteId"].as_u64().expect("missing remoteId") as u32;

    let runtime = create_runtime();
    let socket = runtime.create_socket().await?;
    socket
        .bind("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let local_addr = socket.local_addr().await?;
    let rust_port = local_addr.port();

    let rust_payload = random_payload(100 * 1024);
    let rust_hash = format!("{:x}", Sha256::digest(&rust_payload));

    let node_send_size: usize = 100 * 1024;

    let handshake = serde_json::json!({
        "port": rust_port,
        "mode": "send_and_receive",
        "sendSize": node_send_size
    });
    writeln!(stdin, "{handshake}")?;
    drop(stdin);

    let mut stream = runtime.create_stream(node_remote_id).await?;
    let node_addr: SocketAddr = format!("127.0.0.1:{node_port}").parse()?;
    stream.connect(&socket, node_local_id, node_addr).await?;

    let mut ready_line = String::new();
    reader.read_line(&mut ready_line)?;
    let ready: serde_json::Value = serde_json::from_str(ready_line.trim())?;
    assert!(ready["ready"].as_bool().unwrap_or(false), "node not ready");

    stream.write(&rust_payload).await?;
    stream.shutdown().await?;

    let mut received_from_node = Vec::new();
    while let Some(chunk) = stream.read().await? {
        received_from_node.extend_from_slice(&chunk);
    }

    assert_eq!(
        received_from_node.len(),
        node_send_size,
        "received {} bytes from Node, expected {}",
        received_from_node.len(),
        node_send_size
    );

    let mut result_line = String::new();
    reader.read_line(&mut result_line)?;
    let result: serde_json::Value = serde_json::from_str(result_line.trim())?;

    let node_received = result["received"].as_u64().expect("missing received") as usize;
    assert_eq!(node_received, rust_payload.len(), "Node received wrong size from Rust");

    let node_received_hash = result["receivedSha256"].as_str().expect("missing receivedSha256");
    assert_eq!(node_received_hash, rust_hash, "Node SHA256 mismatch for Rust data");

    let node_sent_hash = result["sentSha256"].as_str().expect("missing sentSha256");
    let our_received_hash = format!("{:x}", Sha256::digest(&received_from_node));
    assert_eq!(our_received_hash, node_sent_hash, "Rust SHA256 mismatch for Node data");

    stream.destroy().await?;
    socket.close().await?;
    drop(runtime);

    let status = child.wait()?;
    assert!(status.success(), "node process exited with: {status}");
    Ok(())
}

/// A stream with a limit on its read buffer, and a reader slower than Node
/// writes. Node is held to what the reader takes: it learns of the room
/// from the window this stream advertises, which it goes by only when the
/// packets that carry it are numbered as C libudx numbers its own.
#[tokio::test]
async fn stream_interop_slow_reader_with_a_limit() {
    let _ = tracing_subscriber::fmt::try_init();

    with_timeout(Duration::from_secs(60), async {
        run_slow_reader_interop_test().await.expect("slow reader interop failed");
    })
    .await;
}

async fn run_slow_reader_interop_test() -> Result<(), Box<dyn std::error::Error>> {
    const LIMIT: u32 = 131_072;
    const NODE_SENDS: usize = 2 * 1024 * 1024;

    let mut child = Command::new("node")
        .arg(node_script_path("udx-large-transfer.js"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let stdout = child.stdout.take().expect("no stdout");
    let mut stdin = child.stdin.take().expect("no stdin");

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let info: serde_json::Value = serde_json::from_str(line.trim())?;
    let node_port = info["port"].as_u64().expect("missing port") as u16;
    let node_local_id = info["localId"].as_u64().expect("missing localId") as u32;
    let node_remote_id = info["remoteId"].as_u64().expect("missing remoteId") as u32;

    let runtime = create_runtime();
    let socket = runtime.create_socket().await?;
    socket
        .bind("127.0.0.1:0".parse::<SocketAddr>()?)
        .await?;
    let rust_port = socket.local_addr().await?.port();

    let handshake = serde_json::json!({
        "port": rust_port,
        "mode": "send_and_receive",
        "sendSize": NODE_SENDS
    });
    writeln!(stdin, "{handshake}")?;
    drop(stdin);

    let mut stream = runtime.create_stream(node_remote_id).await?;
    stream.set_read_buffer_limit(Some(LIMIT));
    let node_addr: SocketAddr = format!("127.0.0.1:{node_port}").parse()?;
    stream.connect(&socket, node_local_id, node_addr).await?;

    let mut ready_line = String::new();
    reader.read_line(&mut ready_line)?;
    let ready: serde_json::Value = serde_json::from_str(ready_line.trim())?;
    assert!(ready["ready"].as_bool().unwrap_or(false), "node not ready");

    // This stream writes as well, so that its sequence numbers are past the
    // ones Node has seen by the time the window matters.
    let rust_payload = random_payload(20 * 1024);
    stream.write(&rust_payload).await?;

    // A reader that takes 64 KB at a time with a wait in between, and stops
    // for three seconds on the way: long enough for Node to find the stream
    // full and ask it for room more than once.
    let mut received_from_node = Vec::new();
    let mut next_wait = 64 * 1024;
    let mut paused = false;
    while received_from_node.len() < NODE_SENDS {
        let chunk = stream.read().await?.expect("Node ended before it had sent everything");
        received_from_node.extend_from_slice(&chunk);
        if received_from_node.len() >= next_wait {
            next_wait += 64 * 1024;
            let wait = if !paused && received_from_node.len() >= NODE_SENDS / 2 {
                paused = true;
                3_000
            } else {
                5
            };
            tokio::time::sleep(Duration::from_millis(wait)).await;
        }
    }

    // Node ends its side once this one has.
    stream.shutdown().await?;
    assert!(stream.read().await?.is_none(), "Node sent more than it said it would");

    let mut result_line = String::new();
    reader.read_line(&mut result_line)?;
    let result: serde_json::Value = serde_json::from_str(result_line.trim())?;

    let node_sent_hash = result["sentSha256"].as_str().expect("missing sentSha256");
    let our_received_hash = format!("{:x}", Sha256::digest(&received_from_node));
    assert_eq!(our_received_hash, node_sent_hash, "Rust SHA256 mismatch for Node data");

    let node_received_hash = result["receivedSha256"].as_str().expect("missing receivedSha256");
    let rust_hash = format!("{:x}", Sha256::digest(&rust_payload));
    assert_eq!(node_received_hash, rust_hash, "Node SHA256 mismatch for Rust data");

    // Node sent each packet once, or nearly: it kept to the window. A sender
    // that did not would have had most of what it sent turned away.
    let packets = result["packetsTransmitted"].as_u64().expect("missing packetsTransmitted");
    let again = result["retransmits"].as_u64().expect("missing retransmits");
    eprintln!("node sent {packets} packets, {again} of them again");
    assert!(again < 50, "Node sent {again} of its {packets} packets again");

    stream.destroy().await?;
    socket.close().await?;
    drop(runtime);

    let status = child.wait()?;
    assert!(status.success(), "node process exited with: {status}");
    Ok(())
}
