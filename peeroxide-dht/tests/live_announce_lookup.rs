//! M9.3: Live announce & lookup on the public HyperDHT network.
//!
//! Run with: `cargo test -p peeroxide-dht --test live_announce_lookup -- --ignored`

#![deny(clippy::all)]

use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, KeyPair};
use serde_json::Value;

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ── Rust-only: two in-process HyperDHT nodes ─────────────────────────────────

#[tokio::test]
#[ignore = "requires internet — two Rust nodes announce+lookup on public HyperDHT"]
async fn rust_announce_rust_lookup() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(60), run_rust_only()).await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("live announce/lookup test failed: {e}"),
        Err(_) => panic!("live announce/lookup test timed out after 60s"),
    }
}

async fn run_rust_only() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = UdxRuntime::new()?;

    let config_a = HyperDhtConfig::with_public_bootstrap();
    let config_b = HyperDhtConfig::with_public_bootstrap();

    let (task_a, handle_a, _rx_a) = hyperdht::spawn(&runtime, config_a).await?;
    let (task_b, handle_b, _rx_b) = hyperdht::spawn(&runtime, config_b).await?;

    handle_a.bootstrapped().await?;
    handle_b.bootstrapped().await?;
    tracing::info!("both Rust nodes bootstrapped");

    let topic = hash(b"peeroxide-live-test-rust-only");
    let topic_hex = to_hex(&topic);
    tracing::info!("topic: {topic_hex}");

    let kp = KeyPair::generate();
    tracing::info!("node A announcing...");
    handle_a.announce(topic, &kp, &[]).await?;
    tracing::info!("announce complete");

    let relays = handle_a.current_relay_addresses();
    tracing::info!("node A current_relay_addresses() returned {} entries", relays.len());
    assert!(
        !relays.is_empty(),
        "current_relay_addresses() should be populated after a successful announce"
    );
    assert!(
        relays.len() <= 3,
        "current_relay_addresses() should be capped at 3 (got {})",
        relays.len()
    );
    for relay in &relays {
        assert_ne!(
            relay.host, "127.0.0.1",
            "relay address should NOT be loopback (Bug A)"
        );
        assert!(!relay.host.is_empty(), "relay host should be non-empty");
        assert_ne!(relay.port, 0, "relay port should be non-zero");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!("node B looking up...");
    let results = handle_b.lookup(topic).await?;
    tracing::info!("lookup returned {} node replies", results.len());

    let found_any = results.iter().any(|r| !r.peers.is_empty());
    assert!(found_any, "node B found no peers after node A announced");
    tracing::info!("Rust-only announce/lookup passed");

    handle_a.destroy().await?;
    handle_b.destroy().await?;
    let _ = task_a.await;
    let _ = task_b.await;
    drop(runtime);

    Ok(())
}

// ── Cross-language: Node.js ↔ Rust on public DHT ─────────────────────────────

fn node_live_peer_path() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    format!("{manifest}/../tests/node/hyperdht-live-peer.js")
}

struct JsLivePeer {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    lines: Arc<Mutex<Vec<String>>>,
    _reader_thread: std::thread::JoinHandle<()>,
    next_id: u32,
}

impl JsLivePeer {
    fn spawn() -> Self {
        let mut child = Command::new("node")
            .arg(node_live_peer_path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn node hyperdht-live-peer.js");

        let stdout = child.stdout.take().expect("no stdout");
        let stdin = child.stdin.take().expect("no stdin");
        let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let lines_clone = lines.clone();

        let reader_thread = std::thread::spawn(move || {
            let reader = std::io::BufReader::new(stdout);
            for line in reader.lines().map_while(Result::ok) {
                lines_clone.lock().unwrap().push(line);
            }
        });

        Self {
            child,
            stdin,
            lines,
            _reader_thread: reader_thread,
            next_id: 1,
        }
    }

    fn wait_ready(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                if let Some(line) = lines.first() {
                    let v: Value = serde_json::from_str(line).expect("invalid JSON from JS live peer");
                    assert!(v["ready"].as_bool().unwrap_or(false), "JS live peer not ready");
                    return;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS live peer ready"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn send_cmd(&mut self, cmd: serde_json::Value) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        let mut msg = cmd;
        msg["id"] = serde_json::json!(id);
        let line = serde_json::to_string(&msg).unwrap() + "\n";
        self.stdin.write_all(line.as_bytes()).expect("write to JS stdin");
        self.stdin.flush().expect("flush JS stdin");
        id
    }

    fn wait_reply(&self, id: u32) -> Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                for line in lines.iter() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        if v["id"].as_u64() == Some(id as u64) {
                            return v;
                        }
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS reply id={id}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn announce(&mut self, topic_hex: &str) -> Value {
        let id = self.send_cmd(serde_json::json!({ "cmd": "announce", "topic": topic_hex }));
        let reply = self.wait_reply(id);
        assert!(reply["ok"].as_bool().unwrap_or(false), "JS announce failed: {reply}");
        reply
    }

    fn lookup(&mut self, topic_hex: &str) -> Vec<String> {
        let id = self.send_cmd(serde_json::json!({ "cmd": "lookup", "topic": topic_hex }));
        let reply = self.wait_reply(id);
        assert!(reply["ok"].as_bool().unwrap_or(false), "JS lookup failed: {reply}");
        reply["peers"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect()
    }
}

impl Drop for JsLivePeer {
    fn drop(&mut self) {
        let _ = self.stdin.write_all(b"{\"cmd\":\"shutdown\",\"id\":0}\n");
        let _ = self.stdin.flush();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
#[ignore = "requires internet + Node.js — cross-language announce/lookup on public HyperDHT"]
async fn cross_language_live_announce_lookup() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(120), run_cross_language()).await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("cross-language live test failed: {e}"),
        Err(_) => panic!("cross-language live test timed out after 120s"),
    }
}

async fn run_cross_language() -> Result<(), Box<dyn std::error::Error>> {
    let mut js = JsLivePeer::spawn();
    js.wait_ready();
    tracing::info!("JS live peer ready on public DHT");

    let runtime = UdxRuntime::new()?;
    let config = HyperDhtConfig::with_public_bootstrap();
    let (task, handle, _server_rx) = hyperdht::spawn(&runtime, config).await?;
    handle.bootstrapped().await?;
    tracing::info!("Rust node bootstrapped on public DHT");

    // ── Test 1: Node.js announces, Rust looks up ──────────────────────────
    let topic1 = hash(b"peeroxide-live-cross-js-announces");
    let topic1_hex = to_hex(&topic1);

    tracing::info!("JS announcing topic {topic1_hex}");
    js.announce(&topic1_hex);
    tracing::info!("JS announce complete");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    tracing::info!("Rust looking up topic {topic1_hex}");
    let results = handle.lookup(topic1).await?;
    let found_any = results.iter().any(|r| !r.peers.is_empty());
    assert!(found_any, "Rust found no peers after JS announce on live DHT");
    tracing::info!("test 1 passed: Rust discovered JS peer on live DHT");

    // ── Test 2: Rust announces, Node.js looks up ──────────────────────────
    let topic2 = hash(b"peeroxide-live-cross-rust-announces");
    let topic2_hex = to_hex(&topic2);

    let kp = KeyPair::generate();
    tracing::info!("Rust announcing topic {topic2_hex}");
    handle.announce(topic2, &kp, &[]).await?;
    tracing::info!("Rust announce complete");

    tokio::time::sleep(Duration::from_millis(1000)).await;

    tracing::info!("JS looking up topic {topic2_hex}");
    let js_peers = js.lookup(&topic2_hex);
    assert!(!js_peers.is_empty(), "JS found no peers after Rust announce on live DHT");
    tracing::info!("test 2 passed: JS discovered Rust peer on live DHT");

    handle.destroy().await?;
    let _ = task.await;
    drop(js);
    drop(runtime);

    Ok(())
}
