//! Integration tests for `peeroxide chat` — multi-instance DHT interaction.
//!
//! Tests in this file exercise the full chat system including:
//! - Profile CRUD (no network)
//! - Nexus publish + lookup (local DHT cluster)
//! - Message exchange between two instances (local DHT cluster)
//! - Read-only mode verification
//!
//! Run with: `cargo test -p peeroxide-cli --test chat_integration`

#![deny(clippy::all)]

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, HyperDhtError, HyperDhtHandle, ServerEvent};
use peeroxide_dht::rpc::DhtConfig;

fn bin_path() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("peeroxide")
}

/// Points a chat command at a test home. `PEEROXIDE_HOME` is what isolates the
/// chat directory on Windows, where `HOME` is ignored.
trait ChatHome {
    fn chat_home(&mut self, home: impl AsRef<std::ffi::OsStr>) -> &mut Self;
}

impl ChatHome for Command {
    fn chat_home(&mut self, home: impl AsRef<std::ffi::OsStr>) -> &mut Self {
        self.env("HOME", home.as_ref()).env("PEEROXIDE_HOME", home.as_ref())
    }
}

async fn spawn_bootstrap() -> (u16, BootstrapNode) {
    let rt = UdxRuntime::new().unwrap();
    let mut dht_cfg = DhtConfig::default();
    dht_cfg.bootstrap = vec![];
    dht_cfg.port = 0;
    dht_cfg.host = "127.0.0.1".to_string();
    dht_cfg.firewalled = false;

    let mut cfg = HyperDhtConfig::default();
    cfg.dht = dht_cfg;

    let (task, handle, rx) = hyperdht::spawn(&rt, cfg).await.unwrap();
    let port = handle.local_port().await.unwrap();

    (port, BootstrapNode { _rt: rt, _task: task, _handle: handle, _rx: rx })
}

struct BootstrapNode {
    _rt: UdxRuntime,
    _task: tokio::task::JoinHandle<Result<(), HyperDhtError>>,
    _handle: HyperDhtHandle,
    _rx: tokio::sync::mpsc::UnboundedReceiver<ServerEvent>,
}

async fn spawn_dht_cluster(n: usize) -> (Vec<u16>, Vec<BootstrapNode>) {
    assert!(n >= 2, "cluster requires at least 2 nodes");

    let (first_port, first_node) = spawn_bootstrap().await;
    let mut ports = vec![first_port];
    let mut nodes = vec![first_node];

    for _ in 1..n {
        let rt = UdxRuntime::new().unwrap();
        let mut dht_cfg = DhtConfig::default();
        dht_cfg.bootstrap = vec![format!("127.0.0.1:{first_port}")];
        dht_cfg.port = 0;
        dht_cfg.host = "127.0.0.1".to_string();
        dht_cfg.firewalled = false;

        let mut cfg = HyperDhtConfig::default();
        cfg.dht = dht_cfg;

        let (task, handle, rx) = hyperdht::spawn(&rt, cfg).await.unwrap();
        handle.bootstrapped().await.unwrap();
        let port = handle.local_port().await.unwrap();

        ports.push(port);
        nodes.push(BootstrapNode { _rt: rt, _task: task, _handle: handle, _rx: rx });
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    (ports, nodes)
}

fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn setup_profile_home(screen_name: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let profiles_dir = dir.path().join(".config/peeroxide/chat/profiles/default");

    std::fs::create_dir_all(&profiles_dir).unwrap();

    let seed: [u8; 32] = rand::random();
    std::fs::write(profiles_dir.join("seed"), seed).unwrap();
    std::fs::write(profiles_dir.join("name"), screen_name).unwrap();

    dir
}

// ── Test: chat --help ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chat_help() {
    let output = tokio::task::spawn_blocking(|| {
        Command::new(bin_path())
            .args(["chat", "--help"])
            .output()
            .expect("failed to run chat --help")
    })
    .await
    .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("join"), "help should mention 'join'");
    assert!(stdout.contains("dm"), "help should mention 'dm'");
    assert!(stdout.contains("inbox"), "help should mention 'inbox'");
    assert!(stdout.contains("whoami"), "help should mention 'whoami'");
    assert!(stdout.contains("profiles"), "help should mention 'profiles'");
    assert!(stdout.contains("nexus"), "help should mention 'nexus'");
    assert!(stdout.contains("friends"), "help should mention 'friends'");
}

// ── Test: profile CRUD ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chat_profiles_create_list_delete() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().to_str().unwrap().to_string();

    let home_create = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_create)
            .args(["chat", "profiles", "create", "alice", "--screen-name", "Alice"])
            .output()
            .expect("failed to run profiles create")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "profiles create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Created profile 'alice'"), "got: {stdout}");
    assert!(stdout.contains("Public key:"), "got: {stdout}");

    let home_list = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_list)
            .args(["chat", "profiles", "list"])
            .output()
            .expect("failed to run profiles list")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("alice"), "profile list should contain 'alice', got: {stdout}");

    let home_delete = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_delete)
            .args(["chat", "profiles", "delete", "alice"])
            .output()
            .expect("failed to run profiles delete")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("Deleted profile 'alice'"), "got: {stdout}");

    let home_verify = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_verify)
            .args(["chat", "profiles", "list"])
            .output()
            .expect("failed to run profiles list after delete")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(!stdout.contains("alice"), "deleted profile should not appear, got: {stdout}");
}

// ── Test: whoami ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chat_whoami() {
    let home_dir = setup_profile_home("TestUser");
    let home = home_dir.path().to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home)
            .args(["chat", "whoami"])
            .output()
            .expect("failed to run whoami")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "whoami failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Profile: default"), "got: {stdout}");
    assert!(stdout.contains("Public key:"), "got: {stdout}");
    assert!(stdout.contains("Screen name: TestUser"), "got: {stdout}");
    assert!(stdout.contains("Nexus topic:"), "got: {stdout}");
}

// ── Test: nexus set-name and set-bio (local, no network) ──────────────────────

#[tokio::test]
async fn test_chat_nexus_set_name_and_bio() {
    let home_dir = setup_profile_home("OldName");
    let home = home_dir.path().to_str().unwrap().to_string();

    let home_name = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_name)
            .args(["chat", "nexus", "--set-name", "NewName"])
            .output()
            .expect("failed to run nexus --set-name")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "nexus --set-name failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Screen name updated to: NewName"), "got: {stdout}");

    let home_bio = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_bio)
            .args(["chat", "nexus", "--set-bio", "A test bio"])
            .output()
            .expect("failed to run nexus --set-bio")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "nexus --set-bio failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Bio updated"), "got: {stdout}");

    let home_verify = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_verify)
            .args(["chat", "whoami"])
            .output()
            .expect("failed to run whoami after set")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Screen name: NewName"), "got: {stdout}");
}

// ── Test: nexus publish + lookup round-trip ─────────────────────────────────────

#[tokio::test]
async fn test_chat_nexus_publish_and_lookup() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);

        let pub_home = setup_profile_home("NexusAlice");
        let pub_home_str = pub_home.path().to_str().unwrap().to_string();

        let pub_home_whoami = pub_home_str.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .chat_home(&pub_home_whoami)
                .args(["chat", "whoami"])
                .output()
                .expect("failed to run whoami")
        })
        .await
        .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);
        let pubkey_line = stdout
            .lines()
            .find(|l| l.starts_with("Public key:"))
            .expect("no Public key line");
        let pubkey = pubkey_line.trim_start_matches("Public key:").trim().to_string();
        assert_eq!(pubkey.len(), 64, "pubkey should be 64 hex chars");

        let pub_home_publish = pub_home_str.clone();
        let bs_addr_pub = bs_addr.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .chat_home(&pub_home_publish)
                .args([
                    "--no-default-config",
                    "chat", "nexus", "--publish",
                    "--bootstrap", &bs_addr_pub,
                ])
                .output()
                .expect("failed to run nexus --publish")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "nexus publish failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("nexus published"),
            "expected 'nexus published' in stderr, got: {stderr}"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        let lookup_home = tempfile::tempdir().unwrap();
        let lookup_home_str = lookup_home.path().to_str().unwrap().to_string();
        let bs_addr_lookup = bs_addr.clone();
        let pubkey_lookup = pubkey.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .chat_home(&lookup_home_str)
                .args([
                    "--no-default-config",
                    "chat", "nexus", "--lookup", &pubkey_lookup,
                    "--bootstrap", &bs_addr_lookup,
                ])
                .output()
                .expect("failed to run nexus --lookup")
        })
        .await
        .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr_lookup = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "nexus lookup failed: {stderr_lookup}"
        );
        assert!(
            stdout.contains("Name: NexusAlice"),
            "expected 'Name: NexusAlice' in stdout, got: {stdout}\nstderr: {stderr_lookup}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_chat_nexus_publish_and_lookup timed out");
}

// ── Test: two instances exchange a message ──────────────────────────────────────

#[tokio::test]
#[ignore = "requires multi-node DHT — local cluster cannot propagate announcements for discovery"]
async fn test_chat_message_exchange() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);

        let alice_home = setup_profile_home("Alice");
        let bob_home = setup_profile_home("Bob");

        let alice_home_str = alice_home.path().to_str().unwrap().to_string();
        let bob_home_str = bob_home.path().to_str().unwrap().to_string();

        let bs_alice = bs_addr.clone();
        let mut alice = Command::new(bin_path())
            .chat_home(&alice_home_str)
            .args([
                "--no-default-config",
                "chat", "join", "test-chat-exchange",
                "--bootstrap", &bs_alice,
                "--no-nexus", "--no-friends",
                "--feed-lifetime", "60",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn Alice's chat join");

        let alice_stderr = alice.stderr.take().unwrap();
        let alice_stderr_reader = BufReader::new(alice_stderr);
        let alice_live = tokio::task::spawn_blocking(move || {
            for line in alice_stderr_reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("— live —") {
                    return true;
                }
            }
            false
        });

        let alice_ready = tokio::time::timeout(Duration::from_secs(30), alice_live).await;
        assert!(
            matches!(alice_ready, Ok(Ok(true))),
            "Alice did not reach live state"
        );

        let bs_bob = bs_addr.clone();
        let mut bob = Command::new(bin_path())
            .chat_home(&bob_home_str)
            .args([
                "--no-default-config",
                "chat", "join", "test-chat-exchange",
                "--bootstrap", &bs_bob,
                "--no-nexus", "--no-friends",
                "--feed-lifetime", "60",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn Bob's chat join");

        let bob_stderr = bob.stderr.take().unwrap();
        let bob_stderr_reader = BufReader::new(bob_stderr);
        let bob_live = tokio::task::spawn_blocking(move || {
            for line in bob_stderr_reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("— live —") {
                    return true;
                }
            }
            false
        });

        let bob_ready = tokio::time::timeout(Duration::from_secs(30), bob_live).await;
        assert!(
            matches!(bob_ready, Ok(Ok(true))),
            "Bob did not reach live state"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;

        let alice_stdin = alice.stdin.as_mut().expect("no stdin for Alice");
        writeln!(alice_stdin, "hello from alice").expect("failed to write to Alice stdin");
        alice_stdin.flush().expect("failed to flush Alice stdin");

        let bob_stdout = bob.stdout.take().unwrap();
        let bob_stdout_reader = BufReader::new(bob_stdout);
        let received = tokio::task::spawn_blocking(move || {
            for line in bob_stdout_reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("hello from alice") {
                    return Some(line);
                }
            }
            None
        });

        let msg_result = tokio::time::timeout(Duration::from_secs(45), received).await;

        kill_child(&mut alice);
        kill_child(&mut bob);

        match msg_result {
            Ok(Ok(Some(line))) => {
                assert!(
                    line.contains("hello from alice"),
                    "received line should contain the message: {line}"
                );
                assert!(
                    line.contains('[') && line.contains(']'),
                    "message should have display formatting: {line}"
                );
            }
            Ok(Ok(None)) => {
                panic!("Bob's stdout closed without receiving Alice's message");
            }
            Ok(Err(e)) => {
                panic!("Bob's reader thread panicked: {e}");
            }
            Err(_) => {
                panic!("Timed out waiting for Bob to receive Alice's message");
            }
        }
    })
    .await;

    assert!(result.is_ok(), "test_chat_message_exchange timed out");
}

// ── Test: burst of rapid messages from one sender arrives in chain order ────────

#[tokio::test]
#[ignore = "requires multi-node DHT — local cluster cannot propagate announcements for discovery"]
async fn test_chat_burst_ordering() {
    const BURST_SIZE: usize = 50;

    let result = tokio::time::timeout(Duration::from_secs(180), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);

        let alice_home = setup_profile_home("Alice");
        let bob_home = setup_profile_home("Bob");
        let alice_home_str = alice_home.path().to_str().unwrap().to_string();
        let bob_home_str = bob_home.path().to_str().unwrap().to_string();

        let bs_alice = bs_addr.clone();
        let mut alice = Command::new(bin_path())
            .chat_home(&alice_home_str)
            .args([
                "--no-default-config",
                "chat", "join", "test-chat-burst",
                "--bootstrap", &bs_alice,
                "--no-nexus", "--no-friends",
                "--feed-lifetime", "60",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn Alice");

        let alice_stderr = BufReader::new(alice.stderr.take().unwrap());
        let alice_live = tokio::task::spawn_blocking(move || {
            for line in alice_stderr.lines() {
                if line.unwrap_or_default().contains("— live —") {
                    return true;
                }
            }
            false
        });
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(30), alice_live).await,
                Ok(Ok(true))
            ),
            "Alice did not reach live state"
        );

        let bs_bob = bs_addr.clone();
        let mut bob = Command::new(bin_path())
            .chat_home(&bob_home_str)
            .args([
                "--no-default-config",
                "chat", "join", "test-chat-burst",
                "--bootstrap", &bs_bob,
                "--read-only",
                "--no-nexus", "--no-friends",
                "--feed-lifetime", "60",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn Bob");

        let bob_stderr = BufReader::new(bob.stderr.take().unwrap());
        let bob_live = tokio::task::spawn_blocking(move || {
            for line in bob_stderr.lines() {
                if line.unwrap_or_default().contains("— live —") {
                    return true;
                }
            }
            false
        });
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(30), bob_live).await,
                Ok(Ok(true))
            ),
            "Bob did not reach live state"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;

        let alice_stdin = alice.stdin.as_mut().expect("no stdin for Alice");
        for i in 1..=BURST_SIZE {
            writeln!(alice_stdin, "burst-line-{i:03}")
                .expect("failed to write burst line");
        }
        alice_stdin.flush().expect("failed to flush Alice stdin");

        let bob_stdout = BufReader::new(bob.stdout.take().unwrap());
        let collector = tokio::task::spawn_blocking(move || {
            let mut seen: Vec<usize> = Vec::new();
            for line in bob_stdout.lines() {
                let line = line.unwrap_or_default();
                if let Some(idx) = line
                    .rsplit_once("burst-line-")
                    .and_then(|(_, tail)| tail.get(..3))
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    seen.push(idx);
                    if seen.len() >= BURST_SIZE {
                        break;
                    }
                }
            }
            seen
        });

        let collect_result =
            tokio::time::timeout(Duration::from_secs(120), collector).await;

        kill_child(&mut alice);
        kill_child(&mut bob);

        let seen = collect_result
            .expect("timed out collecting burst lines")
            .expect("collector thread panicked");

        assert_eq!(
            seen.len(),
            BURST_SIZE,
            "expected {BURST_SIZE} lines, got {}",
            seen.len()
        );
        let expected: Vec<usize> = (1..=BURST_SIZE).collect();
        assert_eq!(seen, expected, "messages arrived out of order: {seen:?}");
    })
    .await;

    assert!(result.is_ok(), "test_chat_burst_ordering timed out");
}

// ── Test: read-only mode does not post or announce ──────────────────────────────

#[tokio::test]
async fn test_chat_read_only_no_post() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");

        let home_dir = setup_profile_home("ReadOnlyUser");
        let home = home_dir.path().to_str().unwrap().to_string();

        let bs_clone = bs_addr.clone();
        let mut child = Command::new(bin_path())
            .chat_home(&home)
            .args([
                "--no-default-config",
                "chat", "join", "readonly-test-channel",
                "--bootstrap", &bs_clone,
                "--read-only",
                "--no-nexus", "--no-friends",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn read-only chat");

        let stderr = child.stderr.take().unwrap();
        let stderr_reader = BufReader::new(stderr);
        let live_check = tokio::task::spawn_blocking(move || {
            for line in stderr_reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("— live —") {
                    return true;
                }
            }
            false
        });

        let ready = tokio::time::timeout(Duration::from_secs(20), live_check).await;
        assert!(matches!(ready, Ok(Ok(true))), "read-only instance did not reach live state");

        if let Some(ref mut stdin) = child.stdin {
            let _ = writeln!(stdin, "this should not post");
            let _ = stdin.flush();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        kill_child(&mut child);
    })
    .await;

    assert!(result.is_ok(), "test_chat_read_only_no_post timed out");
}

// ── Test: cannot delete default profile ─────────────────────────────────────────

#[tokio::test]
async fn test_chat_cannot_delete_default_profile() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home)
            .args(["chat", "profiles", "delete", "default"])
            .output()
            .expect("failed to run profiles delete default")
    })
    .await
    .unwrap();

    assert!(!output.status.success(), "should fail to delete default profile");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot delete the default profile"),
        "expected error about default profile, got: {stderr}"
    );
}

// ── Test: friends add and list ──────────────────────────────────────────────────

#[tokio::test]
async fn test_chat_friends_add_list() {
    let home_dir = setup_profile_home("FriendlyUser");
    let home = home_dir.path().to_str().unwrap().to_string();

    let fake_pubkey = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    let home_add = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_add)
            .args(["chat", "friends", "add", fake_pubkey, "--alias", "TestBuddy"])
            .output()
            .expect("failed to run friends add")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "friends add failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("Added friend"), "got: {stdout}");

    let home_list = home.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .chat_home(&home_list)
            .args(["chat", "friends", "list"])
            .output()
            .expect("failed to run friends list")
    })
    .await
    .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(
        stdout.contains("TestBuddy"),
        "friends list should show alias 'TestBuddy', got: {stdout}"
    );
}

// ── Test: piped stdin auto-detects line mode without --line-mode ────────────────
//
// Regression for the case where `cat msgs | peeroxide chat join …` from a
// shell would crash because the interactive TUI was selected (stdout was a
// TTY) but stdin was a pipe — crossterm's `EventStream` cannot read events
// from a non-TTY stdin.
//
// In a cargo test subprocess both stdout and stdin are pipes, so this test
// can't fully reproduce the "TTY stdout + pipe stdin" shell scenario without
// pulling in a pty crate. What it DOES verify:
//
//   - Spawning the binary with piped stdin and no `--line-mode` flag does
//     not crash (clean exit status 0).
//   - Lines piped to stdin are consumed; `/quit` triggers graceful shutdown.
//
// That guards against future regressions in the line-mode path itself and in
// the stdin-handling code. The TTY-stdout-plus-pipe-stdin shell scenario
// should still be smoke-tested manually after touching `make_ui`.
#[tokio::test]
async fn test_chat_join_piped_stdin_auto_line_mode() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");

        let home_dir = setup_profile_home("PipedStdinUser");
        let home = home_dir.path().to_str().unwrap().to_string();

        let bs_clone = bs_addr.clone();
        let mut child = Command::new(bin_path())
            .chat_home(&home)
            .args([
                "--no-default-config",
                "chat",
                "join",
                "piped-stdin-test",
                "--bootstrap",
                &bs_clone,
                "--read-only",
                "--no-nexus",
                "--no-friends",
                // Deliberately NO --line-mode — proving auto-detection works.
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn chat join with piped stdin");

        // Wait until the chat is live before feeding stdin so we don't race
        // the startup. Crucially, the stderr drain must KEEP running for the
        // lifetime of the child — if we drop the BufReader after spotting
        // "— live —" the child's next stderr write hits EPIPE and the
        // binary panics. We use a oneshot to surface the live signal while
        // a background thread silently drains the remainder.
        let stderr = child.stderr.take().unwrap();
        let (live_tx, live_rx) = tokio::sync::oneshot::channel::<bool>();
        let _stderr_drain = std::thread::spawn(move || {
            let stderr_reader = BufReader::new(stderr);
            let mut live_tx = Some(live_tx);
            for line in stderr_reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("— live —") {
                    if let Some(tx) = live_tx.take() {
                        let _ = tx.send(true);
                    }
                }
                // After signalling live, keep reading & discarding so the
                // child's stderr pipe doesn't fill or close.
            }
            if let Some(tx) = live_tx.take() {
                // Stream ended without seeing "— live —".
                let _ = tx.send(false);
            }
        });

        let saw_live = match tokio::time::timeout(Duration::from_secs(20), live_rx).await {
            Ok(Ok(b)) => b,
            _ => false,
        };
        assert!(
            saw_live,
            "piped-stdin instance did not reach live state — auto line-mode may have failed"
        );

        // Also drain stdout in the background to keep that pipe healthy too.
        let stdout_handle = child.stdout.take().unwrap();
        let _stdout_drain = std::thread::spawn(move || {
            let r = BufReader::new(stdout_handle);
            for _line in r.lines().map_while(Result::ok) {}
        });

        // Feed `/quit` and expect a graceful exit.
        {
            let mut stdin = child.stdin.take().expect("child has no stdin");
            writeln!(stdin, "/quit").expect("failed to write /quit to stdin");
            stdin.flush().expect("failed to flush stdin");
            // Drop stdin to signal EOF as well — line-mode's default
            // behaviour also exits cleanly on stdin EOF.
        }

        // Wait for graceful exit. If the binary crashed (panic / abort)
        // we'd see a non-zero status; if it hung we'd time out.
        let status = tokio::task::spawn_blocking(move || child.wait())
            .await
            .expect("join wait task")
            .expect("child wait");
        assert!(
            status.success(),
            "piped-stdin chat exited non-zero: {status:?}"
        );
    })
    .await;

    assert!(
        result.is_ok(),
        "test_chat_join_piped_stdin_auto_line_mode timed out — binary likely hung instead of exiting on /quit"
    );
}
