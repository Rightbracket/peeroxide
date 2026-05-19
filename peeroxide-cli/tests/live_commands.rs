//! Live integration tests — requires internet access to the public HyperDHT network.
//!
//! Run with: `cargo test -p peeroxide-cli --test live_commands -- --ignored`

#![deny(clippy::all)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn bin_path() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("peeroxide")
}

fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn unique_topic() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("peeroxide-live-test-{ts}")
}

#[tokio::test]
#[ignore = "requires internet — lookup on public HyperDHT"]
async fn test_live_lookup() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let topic = unique_topic();

        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "lookup", &topic, "--json",
                ])
                .output()
                .expect("failed to run lookup")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "live lookup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    })
    .await;

    assert!(result.is_ok(), "test_live_lookup timed out after 60s");
}

#[tokio::test]
#[ignore = "requires internet — announce+lookup on public HyperDHT"]
async fn test_live_announce_then_lookup() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let topic = unique_topic();

        let mut announce = Command::new(bin_path())
            .args([
                "--no-default-config", "--public",
                "announce", &topic, "--duration", "30",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn announce");

        tokio::time::sleep(Duration::from_secs(10)).await;

        let topic_clone = topic.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "lookup", &topic_clone, "--json",
                ])
                .output()
                .expect("failed to run lookup")
        })
        .await
        .unwrap();

        kill_child(&mut announce);

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            output.status.success(),
            "live lookup failed: {stderr}"
        );

        assert!(
            stdout.contains("\"peers_found\""),
            "expected peer data in output.\nstdout: {stdout}\nstderr: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_live_announce_then_lookup timed out after 60s");
}

#[tokio::test]
#[ignore = "requires internet — dd roundtrip on public HyperDHT"]
async fn test_live_dd_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let msg_path = dir.path().join("live-msg.txt");
        std::fs::write(&msg_path, b"live dd test message").unwrap();

        let msg_path_str = msg_path.to_str().unwrap().to_string();
        let mut leave_child = Command::new(bin_path())
            .args([
                "--no-default-config", "--public",
                "dd", "put", &msg_path_str, "--ttl", "45",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put");

        let stdout = leave_child.stdout.take().unwrap();
        let pickup_key = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let pickup_key = pickup_key.expect("dd put did not output pickup key");

        tokio::time::sleep(Duration::from_secs(3)).await;

        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "dd", "get", &pickup_key,
                    "--timeout", "30",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get")
        })
        .await
        .unwrap();

        kill_child(&mut leave_child);

        let pickup_stdout = String::from_utf8_lossy(&pickup_output.stdout);
        let pickup_stderr = String::from_utf8_lossy(&pickup_output.stderr);

        assert!(
            pickup_output.status.success(),
            "live dd get failed: {pickup_stderr}"
        );

        assert_eq!(
            pickup_stdout.as_ref(), "live dd test message",
            "get content mismatch.\nstdout: {pickup_stdout}\nstderr: {pickup_stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_live_dd_roundtrip timed out after 60s");
}

#[tokio::test]
#[ignore = "requires internet — cp file transfer on public HyperDHT"]
async fn test_live_cp_send_recv() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let send_path = dir.path().join("testfile.dat");
        let content = b"peeroxide cp live integration test content";
        std::fs::write(&send_path, content).unwrap();

        let recv_path = dir.path().join("received.dat");
        let send_path_str = send_path.to_str().unwrap().to_string();

        let mut send_child = Command::new(bin_path())
            .args([
                "--no-default-config", "--public",
                "cp", "send", &send_path_str,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn cp send");

        let stdout = send_child.stdout.take().unwrap();
        let topic = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let topic = topic.expect("cp send did not output topic");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let recv_path_str = recv_path.to_str().unwrap().to_string();
        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "cp", "recv", &topic, &recv_path_str,
                    "--yes",
                    "--timeout", "30",
                ])
                .output()
                .expect("failed to run cp recv")
        })
        .await
        .unwrap();

        kill_child(&mut send_child);

        let recv_stderr = String::from_utf8_lossy(&recv_output.stderr);
        assert!(
            recv_output.status.success(),
            "live cp recv failed: {recv_stderr}"
        );

        let received = std::fs::read(&recv_path).expect("received file not found");
        assert_eq!(
            received, content,
            "received file content doesn't match.\nstderr: {recv_stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_live_cp_send_recv timed out after 60s");
}

/// Honest non-LAN gate: forces `local_connection=false` so the receiver
/// cannot fall back to the same-host loopback shortcut. With Phase 3 hole-
/// punching landed, two peers on the same host must complete the transfer
/// via the holepunched path over the receiver's puncher socket. This test
/// verifies the toggle engaged AND the transfer succeeded AND the received
/// bytes match the sent bytes — proving the Phase 3 holepunch flow works
/// end-to-end on the public DHT.
#[tokio::test]
#[ignore = "requires internet — non-LAN holepunch gate, Phase 3"]
async fn test_live_cp_send_recv_no_lan() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let send_path = dir.path().join("testfile.dat");
        let content = b"peeroxide cp live no-LAN gate content";
        std::fs::write(&send_path, content).unwrap();

        let recv_path = dir.path().join("received.dat");
        let send_path_str = send_path.to_str().unwrap().to_string();

        let mut send_child = Command::new(bin_path())
            .args([
                "--no-default-config", "--public",
                "cp", "send", &send_path_str,
            ])
            .env("PEEROXIDE_LOCAL_CONNECTION", "false")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn cp send");

        let stdout = send_child.stdout.take().unwrap();
        let topic = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let topic = topic.expect("cp send did not output topic");

        // Wait longer than the LAN-shortcut variant because the no-LAN
        // path can't fast-resolve via loopback; we need the sender's
        // announce to fully propagate before recv attempts lookup.
        tokio::time::sleep(Duration::from_secs(15)).await;

        let recv_path_str = recv_path.to_str().unwrap().to_string();
        let recv_path_for_assert = recv_path.clone();
        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "cp", "recv", &topic, &recv_path_str,
                    "--yes",
                    "--timeout", "45",
                ])
                .env("PEEROXIDE_LOCAL_CONNECTION", "false")
                .env("RUST_LOG", "peeroxide_dht=debug")
                .env("NO_COLOR", "1")
                .output()
                .expect("failed to run cp recv")
        })
        .await
        .unwrap();

        kill_child(&mut send_child);

        let recv_stderr = String::from_utf8_lossy(&recv_output.stderr);

        // The recv must have rejected the loopback shortcut. Look for
        // either `same_host=false` (LAN-shortcut explicitly disabled) or
        // a non-loopback dial target. NOT seeing these means the toggle
        // didn't take effect.
        let disabled_lan_shortcut = recv_stderr.contains("same_host=false");
        let attempted_non_loopback = recv_stderr.contains("connect_addr=")
            && !recv_stderr.contains("connect_addr=127.0.0.1:");

        assert!(
            disabled_lan_shortcut || attempted_non_loopback,
            "local_connection=false toggle did not engage; expected `same_host=false` \
             or non-loopback `connect_addr` in stderr.\n--- stderr ---\n{recv_stderr}"
        );

        // Phase 3 holepunch landed — recv MUST succeed.
        assert!(
            recv_output.status.success(),
            "live cp recv (no-LAN, Phase 3 holepunch) failed.\n--- stderr ---\n{recv_stderr}"
        );

        let received = std::fs::read(&recv_path_for_assert)
            .expect("recv output file not found despite recv exiting 0");
        assert_eq!(
            received, content,
            "received file content does not match sent content.\n--- stderr ---\n{recv_stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_live_cp_send_recv_no_lan timed out after 90s");
}

/// Force the data path through a blind-relay by advertising `relay_through` from the
/// send (server) side via the `PEEROXIDE_FORCE_RELAY` env var. The placeholder uses a
/// zero pubkey + `8.8.8.8:49737`, which is intentionally unreachable as a blind-relay
/// endpoint — the test asserts that:
///
///   1. The recv (client) honors the advertised `relay_through` (does NOT successfully
///      fall through to direct/holepunch when relay is mandated).
///   2. The recv exits non-zero — failure mode is the relay attempt, not topic lookup
///      or some unrelated path.
///
/// This wires the env var end-to-end. A real public relay pubkey + addr (once we find
/// one via the hunt script) can be substituted to flip this from "fail cleanly" to
/// "succeed via relay."
#[tokio::test]
#[ignore = "requires internet — force-relay wiring against placeholder, expected fail"]
async fn test_live_cp_send_recv_force_relay() {
    let placeholder_pk = "0".repeat(64);
    let placeholder_relay = format!("{placeholder_pk}@8.8.8.8:49737");

    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let dir = tempfile::tempdir().unwrap();
        let send_path = dir.path().join("testfile.dat");
        let content = b"peeroxide cp force-relay placeholder content";
        std::fs::write(&send_path, content).unwrap();

        let recv_path = dir.path().join("received.dat");
        let send_path_str = send_path.to_str().unwrap().to_string();

        let mut send_child = Command::new(bin_path())
            .args([
                "--no-default-config", "--public",
                "cp", "send", &send_path_str,
            ])
            .env("PEEROXIDE_FORCE_RELAY", &placeholder_relay)
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn cp send");

        let stdout = send_child.stdout.take().unwrap();
        let topic = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let topic = topic.expect("cp send did not output topic");

        tokio::time::sleep(Duration::from_secs(15)).await;

        let recv_path_str = recv_path.to_str().unwrap().to_string();
        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--public",
                    "cp", "recv", &topic, &recv_path_str,
                    "--yes",
                    "--timeout", "30",
                ])
                .env("RUST_LOG", "peeroxide_dht=debug,peeroxide=debug")
                .env("NO_COLOR", "1")
                .output()
                .expect("failed to run cp recv")
        })
        .await
        .unwrap();

        kill_child(&mut send_child);

        let recv_stderr = String::from_utf8_lossy(&recv_output.stderr);

        assert!(
            !recv_output.status.success(),
            "recv unexpectedly succeeded against 8.8.8.8 placeholder relay; \
             either the relay path was bypassed (a regression) or 8.8.8.8 ran a \
             blind-relay (extremely unlikely).\n--- stderr ---\n{recv_stderr}"
        );

        let relay_path_engaged = recv_stderr.contains("relay_through")
            || recv_stderr.contains("relay_connection")
            || recv_stderr.contains("BlindRelayClient")
            || recv_stderr.contains("8.8.8.8");

        assert!(
            relay_path_engaged,
            "recv stderr shows no evidence of relay-path engagement against the \
             placeholder; PEEROXIDE_FORCE_RELAY may not be plumbed through to the \
             handshake.\n--- stderr ---\n{recv_stderr}"
        );
    })
    .await;

    assert!(
        result.is_ok(),
        "test_live_cp_send_recv_force_relay timed out after 90s"
    );
}
