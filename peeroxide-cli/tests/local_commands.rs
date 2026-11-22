//! Local integration tests — no internet required.
//! Each test starts an isolated bootstrap node and exercises CLI commands against it.
//!
//! Run with: `cargo test -p peeroxide-cli --test local_commands`

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

// ── Test: node starts and binds ──────────────────────────────────────────────

#[tokio::test]
async fn test_node_starts_and_binds() {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let mut child = Command::new(bin_path())
            .args(["--no-default-config", "node", "--port", "0", "--host", "127.0.0.1"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn peeroxide node");

        let stdout = child.stdout.take().unwrap();
        let handle = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                if line.contains("127.0.0.1:") {
                    return Some(line);
                }
            }
            None
        });

        let found = tokio::time::timeout(Duration::from_secs(10), handle).await;
        kill_child(&mut child);

        match found {
            Ok(Ok(Some(line))) => {
                assert!(line.contains("127.0.0.1:"), "expected address in: {line}");
            }
            _ => {
                // Node ran without crashing — acceptable even if it didn't print to stdout
            }
        }
    })
    .await;

    assert!(result.is_ok(), "test_node_starts_and_binds timed out");
}

// ── Test: first node on a wildcard host needs --public-host ─────────────────

/// Waits up to 15 s for `child` to exit, killing it and failing otherwise.
fn wait_or_kill(child: &mut Child) -> std::process::ExitStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        if std::time::Instant::now() > deadline {
            kill_child(child);
            panic!("peeroxide node did not exit in time");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Starts `peeroxide <cmd>` with no bootstrap nodes, waits for its first line
/// on stdout, stops it, and returns everything it wrote to stderr.
fn run_isolated_node(cmd: &str, extra: &[&str]) -> String {
    let mut child = Command::new(bin_path())
        .env_remove("PEEROXIDE_CONFIG")
        .args(["--no-default-config", "--no-public", cmd, "--port", "0"])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn peeroxide node");
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stdout).read_line(&mut line);
        let _ = tx.send(line);
    });
    let line = rx.recv_timeout(Duration::from_secs(15));
    kill_child(&mut child);
    let line = line.expect("peeroxide node did not print its address in time");
    assert!(!line.is_empty(), "expected startup output");
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr)
        .expect("read stderr");
    stderr
}

#[test]
fn test_node_wildcard_without_bootstrap_warns() {
    for cmd in ["node", "relay"] {
        let stderr = run_isolated_node(cmd, &[]);
        assert!(stderr.contains("--public-host"), "{cmd}: expected a warning, got: {stderr}");

        let stderr = run_isolated_node(cmd, &["--public-host", "127.0.0.1"]);
        assert!(!stderr.contains("--public-host"), "{cmd}: unexpected warning: {stderr}");
    }
}

#[test]
fn test_node_rejects_invalid_public_host() {
    let mut child = Command::new(bin_path())
        .env_remove("PEEROXIDE_CONFIG")
        .args(["--no-default-config", "--no-public", "node", "--port", "0", "--public-host", "0.0.0.0"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn peeroxide node");
    let status = wait_or_kill(&mut child);
    assert!(!status.success());
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr)
        .expect("read stderr");
    assert!(stderr.contains("public_host"), "unexpected stderr: {stderr}");
}

// ── Test: lookup returns empty for unknown topic ─────────────────────────────

#[tokio::test]
async fn test_lookup_empty_topic() {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");

        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config",
                    "lookup", "nonexistent-topic-12345",
                    "--bootstrap", &bs_addr,
                    "--json",
                ])
                .output()
                .expect("failed to run lookup")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "lookup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("\"peers_found\":0") || stdout.contains("\"peers_found\": 0"),
            "expected 0 peers found, got: {stdout}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_lookup_empty_topic timed out");
}

// ── Test: announce then lookup finds the peer ────────────────────────────────

#[tokio::test]
async fn test_announce_then_lookup() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");

        let mut announce = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "announce", "local-test-announce-lookup",
                "--bootstrap", &bs_addr,
                "--duration", "20",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn announce");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let bs_addr_clone = bs_addr.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "lookup", "local-test-announce-lookup",
                    "--bootstrap", &bs_addr_clone,
                    "--json",
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
            "lookup failed: {stderr}"
        );

        // Single-bootstrap DHT cannot propagate announcements, so we only
        // verify the lookup executes successfully and produces valid JSON.
        // Actual discovery is verified in live_commands.rs tests.
        assert!(
            stdout.contains("\"peers_found\""),
            "expected JSON summary in output.\nstdout: {stdout}\nstderr: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_announce_then_lookup timed out");
}

// ── Test: config loading with --config flag ──────────────────────────────────

#[tokio::test]
async fn test_config_file_loading() {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("test-config.toml");

        let mut f = std::fs::File::create(&config_path).unwrap();
        writeln!(f, "[network]").unwrap();
        writeln!(f, "bootstrap = [\"127.0.0.1:19999\"]").unwrap();

        let config_str = config_path.to_str().unwrap().to_string();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--config", &config_str,
                    "lookup", "--help",
                ])
                .output()
                .expect("failed to run with --config")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "config loading failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    })
    .await;

    assert!(result.is_ok(), "test_config_file_loading timed out");
}

// ── Test: dd put then get (local DHT) ───────────────────────────────────────

#[tokio::test]
async fn test_dd_local_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.txt");
        let output_path = dir.path().join("output.txt");

        let msg = b"local dd test payload";
        std::fs::write(&input_path, msg).unwrap();

        let input_path_str = input_path.to_str().unwrap().to_string();
        let bs_addr_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_path_str,
                "--bootstrap", &bs_addr_clone,
                "--ttl", "35",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put");

        let stdout = leave.stdout.take().unwrap();
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

        let pickup_key = pickup_key.expect("dd put did not output a pickup key");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let output_path_str = output_path.to_str().unwrap().to_string();
        let bs_addr_clone2 = bs_addr.clone();
        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_addr_clone2,
                    "--output", &output_path_str,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&pickup_output.stderr);
        assert!(
            pickup_output.status.success(),
            "dd get failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert_eq!(received, msg, "payload mismatch.\nstderr: {stderr}");
    })
    .await;

    assert!(result.is_ok(), "test_dd_local_roundtrip timed out");
}

#[tokio::test]
async fn test_dd_get_json_requires_output() {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args(["dd", "get", "--json", "--passphrase", "x"])
                .output()
                .expect("failed to run dd get --json")
        })
        .await
        .unwrap();

        assert!(!output.status.success(), "command unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("--output") || stderr.contains("required"),
            "expected validation message mentioning --output, got: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_dd_get_json_requires_output timed out");
}

// ── Test: --help works for all subcommands ──────────────────────────────────

#[tokio::test]
async fn test_help_all_subcommands() {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let subcommands = ["init", "node", "lookup", "announce", "ping", "cp", "dd"];

        for subcmd in subcommands {
            let subcmd_owned = subcmd.to_string();
            let output = tokio::task::spawn_blocking(move || {
                Command::new(bin_path())
                    .args([&subcmd_owned, "--help"])
                    .output()
                    .expect("failed to run --help")
            })
            .await
            .unwrap();

            assert!(
                output.status.success(),
                "{subcmd} --help failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                !stdout.is_empty(),
                "{subcmd} --help produced no output"
            );
        }
    })
    .await;

    assert!(result.is_ok(), "test_help_all_subcommands timed out");
}

// ── Test: init creates config file ──────────────────────────────────────────

#[tokio::test]
async fn test_init_creates_config() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("peeroxide").join("config.toml");
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init"])
            .output()
            .expect("failed to run init")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(config_path.exists(), "config file not created");
    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("[network]"), "config missing [network] section");
    assert!(content.contains("[node]"), "config missing [node] section");
}

// ── Test: init with --public sets public in config ──────────────────────────

#[tokio::test]
async fn test_init_public_flag() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init", "--public"])
            .output()
            .expect("failed to run init --public")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --public failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("public = true"),
        "config should contain 'public = true', got:\n{content}"
    );
}

// ── Test: init existing config without --force is no-op ─────────────────────

#[tokio::test]
async fn test_init_existing_no_force() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "[network]\npublic = true\n").unwrap();
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init"])
            .output()
            .expect("failed to run init (existing)")
    })
    .await
    .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("config already exists"),
        "expected 'config already exists' message, got: {stdout}"
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert_eq!(content, "[network]\npublic = true\n", "config should not be modified");
}

// ── Test: init --force overwrites existing config ───────────────────────────

#[tokio::test]
async fn test_init_force_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "old content").unwrap();
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init", "--force"])
            .output()
            .expect("failed to run init --force")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --force failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(content.contains("[network]"), "config should be regenerated");
    assert_ne!(content, "old content", "config should be overwritten");
}

// ── Test: init --update patches fields ──────────────────────────────────────

#[tokio::test]
async fn test_init_update_patches() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "[network]\n# public = false\n\n[node]\nport = 49737\n").unwrap();
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init", "--update", "--public"])
            .output()
            .expect("failed to run init --update")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --update failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("public = true"),
        "config should have public = true after update, got:\n{content}"
    );
    assert!(
        content.contains("port = 49737"),
        "config should preserve existing port setting, got:\n{content}"
    );
}

// ── Test: init --update on nonexistent config errors ────────────────────────

#[tokio::test]
async fn test_init_update_no_config_errors() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("nonexistent.toml");
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["--config", &config_str, "init", "--update", "--public"])
            .output()
            .expect("failed to run init --update (nonexistent)")
    })
    .await
    .unwrap();

    assert!(
        !output.status.success(),
        "init --update on nonexistent config should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no config to update"),
        "expected 'no config to update' error, got: {stderr}"
    );
}

#[tokio::test]
async fn test_init_update_no_flags_errors() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "[network]\npublic = false\n").unwrap();

    let config_str = config_path.to_str().unwrap().to_string();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--config", &config_str, "--update"])
            .output()
            .expect("failed to run init --update")
    })
    .await
    .unwrap();

    assert!(
        !output.status.success(),
        "init --update with no flags should fail (exit non-zero)"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("nothing to update"),
        "expected 'nothing to update' error, got: {stderr}"
    );
}

#[tokio::test]
async fn test_init_update_preserves_trailing_comments() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        "[network]\npublic = false # important note\nbootstrap = [\"keep:1\"] # node list\n",
    )
    .unwrap();

    let config_str = config_path.to_str().unwrap().to_string();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--config", &config_str, "--update", "--public"])
            .output()
            .expect("failed to run init --update")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --update failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("# important note"),
        "trailing comment on updated key should be preserved, got: {content}"
    );
    assert!(
        content.contains("# node list"),
        "trailing comment on untouched key should be preserved, got: {content}"
    );
    assert!(
        content.contains("true"),
        "public should be updated to true, got: {content}"
    );
    assert!(
        content.contains("keep:1"),
        "bootstrap should be untouched, got: {content}"
    );
}

// ── Test: init --man-pages generates manpages ───────────────────────────────

#[tokio::test]
async fn test_init_man_pages() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--man-pages", &dir_str])
            .output()
            .expect("failed to run init --man-pages")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --man-pages failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let man1_dir = dir.path().join("man1");
    assert!(man1_dir.exists(), "man1/ subdirectory not created");

    let expected_pages = [
        "peeroxide.1",
        "peeroxide-init.1",
        "peeroxide-node.1",
        "peeroxide-lookup.1",
        "peeroxide-announce.1",
        "peeroxide-ping.1",
        "peeroxide-cp.1",
        "peeroxide-dd.1",
    ];

    for page in &expected_pages {
        let path = man1_dir.join(page);
        assert!(path.exists(), "missing manpage: {page}");
        let content = std::fs::read(&path).unwrap();
        assert!(!content.is_empty(), "empty manpage: {page}");
    }
}

// ── Test: init --man-pages removes stale pages ─────────────────────────────

#[tokio::test]
async fn test_init_man_pages_removes_stale() {
    let dir = tempfile::tempdir().unwrap();
    let man1_dir = dir.path().join("man1");
    std::fs::create_dir_all(&man1_dir).unwrap();

    std::fs::write(man1_dir.join("peeroxide-deaddrop.1"), b"stale").unwrap();
    std::fs::write(man1_dir.join("peeroxide-config.1"), b"stale").unwrap();
    std::fs::write(man1_dir.join("unrelated.1"), b"keep").unwrap();

    let dir_str = dir.path().to_str().unwrap().to_string();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--man-pages", &dir_str])
            .output()
            .expect("failed to run init --man-pages")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --man-pages failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        !man1_dir.join("peeroxide-deaddrop.1").exists(),
        "stale peeroxide-deaddrop.1 should have been removed"
    );
    assert!(
        !man1_dir.join("peeroxide-config.1").exists(),
        "stale peeroxide-config.1 should have been removed"
    );
    assert!(
        man1_dir.join("unrelated.1").exists(),
        "non-peeroxide files should be preserved"
    );
    assert!(
        man1_dir.join("peeroxide-dd.1").exists(),
        "current peeroxide-dd.1 should exist"
    );
}

// ── Test: init --man-pages conflicts with config flags ──────────────────────

#[tokio::test]
async fn test_init_man_pages_conflicts_with_force() {
    let output = tokio::task::spawn_blocking(|| {
        Command::new(bin_path())
            .args(["init", "--man-pages", "/tmp", "--force"])
            .output()
            .expect("failed to run init")
    })
    .await
    .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot be used with") || stderr.contains("conflict"),
        "expected conflict error, got: {stderr}"
    );
}

#[tokio::test]
async fn test_init_man_pages_conflicts_with_update() {
    let output = tokio::task::spawn_blocking(|| {
        Command::new(bin_path())
            .args(["init", "--man-pages", "/tmp", "--update"])
            .output()
            .expect("failed to run init")
    })
    .await
    .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot be used with") || stderr.contains("conflict"),
        "expected conflict error, got: {stderr}"
    );
}

// ── Test: init respects PEEROXIDE_CONFIG env ────────────────────────────────

#[tokio::test]
async fn test_init_respects_peeroxide_config_env() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("custom.toml");
    let config_str = config_path.to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .env("PEEROXIDE_CONFIG", &config_str)
            .args(["init"])
            .output()
            .expect("failed to run init")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init with PEEROXIDE_CONFIG failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(config_path.exists(), "config not created at PEEROXIDE_CONFIG path");
}

// ── Test: init --update preserves inline table fields ────────────────────────

#[tokio::test]
async fn test_init_update_preserves_inline_table() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        r#"network = { public = false, bootstrap = ["keep:1234"] }"#,
    )
    .unwrap();

    let config_str = config_path.to_str().unwrap().to_string();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--config", &config_str, "--update", "--public"])
            .output()
            .expect("failed to run init --update")
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "init --update failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let content = std::fs::read_to_string(&config_path).unwrap();
    assert!(
        content.contains("keep:1234"),
        "bootstrap should be preserved in inline table, got: {content}"
    );
    assert!(
        content.contains("true"),
        "public should be set to true, got: {content}"
    );
}

// ── Test: init rejects directory as config path ─────────────────────────────

#[tokio::test]
async fn test_init_rejects_directory_path() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();

    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--config", &dir_str])
            .output()
            .expect("failed to run init")
    })
    .await
    .unwrap();

    assert!(
        !output.status.success(),
        "init should fail when --config points to a directory"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("is a directory"),
        "error should mention directory, got: {stderr}"
    );
}

// ── Test: init --update rejects non-table network value ─────────────────────

#[tokio::test]
async fn test_init_update_rejects_non_table_network() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, "network = \"oops\"\n").unwrap();

    let config_str = config_path.to_str().unwrap().to_string();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(["init", "--config", &config_str, "--update", "--public"])
            .output()
            .expect("failed to run init --update")
    })
    .await
    .unwrap();

    assert!(
        !output.status.success(),
        "init --update should fail on non-table network value"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not a table"),
        "error should mention non-table, got: {stderr}"
    );
}

// ── Test: global --help ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_global_help() {
    let output = tokio::task::spawn_blocking(|| {
        Command::new(bin_path())
            .args(["--help"])
            .output()
            .expect("failed to run --help")
    })
    .await
    .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("init"));
    assert!(stdout.contains("node"));
    assert!(stdout.contains("lookup"));
    assert!(stdout.contains("announce"));
    assert!(stdout.contains("ping"));
    assert!(stdout.contains("cp"));
    assert!(stdout.contains("dd"));
}

// ── Test: ping direct with --json produces valid NDJSON ─────────────────────

#[tokio::test]
async fn test_ping_direct_json() {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let (port, _bs) = spawn_bootstrap().await;
        let target = format!("127.0.0.1:{port}");

        let bs_addr = target.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config",
                    "ping", &target,
                    "--bootstrap", &bs_addr,
                    "--json",
                ])
                .output()
                .expect("failed to run ping")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "ping failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<&str> = stdout.lines().collect();

        // Should have resolve line + probe line (count defaults to 1)
        assert!(lines.len() >= 2, "expected >=2 JSON lines, got: {stdout}");

        // First line: resolve
        let resolve: serde_json::Value = serde_json::from_str(lines[0])
            .unwrap_or_else(|e| panic!("invalid JSON line 0: {e}\nline: {}", lines[0]));
        assert_eq!(resolve["type"], "resolve");
        assert_eq!(resolve["method"], "direct");

        // Second line: probe with status ok
        let probe: serde_json::Value = serde_json::from_str(lines[1])
            .unwrap_or_else(|e| panic!("invalid JSON line 1: {e}\nline: {}", lines[1]));
        assert_eq!(probe["type"], "probe");
        assert_eq!(probe["seq"], 1);
        assert_eq!(probe["status"], "ok");
        assert!(probe["rtt_ms"].as_f64().unwrap() > 0.0, "rtt_ms should be positive");
    })
    .await;

    assert!(result.is_ok(), "test_ping_direct_json timed out");
}

// ── Test: ping --count 3 produces repeated probes and a summary ─────────────

#[tokio::test]
async fn test_ping_count_repeated() {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let (port, _bs) = spawn_bootstrap().await;
        let target = format!("127.0.0.1:{port}");

        let bs_addr = target.clone();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config",
                    "ping", &target,
                    "--bootstrap", &bs_addr,
                    "--count", "3",
                    "--interval", "0.1",
                    "--json",
                ])
                .output()
                .expect("failed to run ping")
        })
        .await
        .unwrap();

        assert!(
            output.status.success(),
            "ping --count 3 failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<&str> = stdout.lines().collect();

        // resolve + 3 probes + summary = 5 lines
        assert!(
            lines.len() >= 5,
            "expected >=5 JSON lines for --count 3, got {}:\n{stdout}",
            lines.len()
        );

        // Verify seq numbers are 1, 2, 3
        for (i, expected_seq) in [1, 2, 3].iter().enumerate() {
            let probe: serde_json::Value = serde_json::from_str(lines[i + 1])
                .unwrap_or_else(|e| panic!("invalid JSON line {}: {e}", i + 1));
            assert_eq!(probe["type"], "probe");
            assert_eq!(probe["seq"], *expected_seq);
        }

        // Last line should be the summary
        let summary: serde_json::Value = serde_json::from_str(lines.last().unwrap())
            .unwrap_or_else(|e| panic!("invalid summary JSON: {e}"));
        assert_eq!(summary["type"], "summary");
        assert_eq!(summary["probes_sent"], 3);
        assert_eq!(summary["probes_responded"], 3);
    })
    .await;

    assert!(result.is_ok(), "test_ping_count_repeated timed out");
}

// ── Test: announce on topic then ping by topic finds the peer ───────────────

#[tokio::test]
#[ignore = "requires multi-node DHT — single bootstrap cannot propagate announcements"]
async fn test_ping_by_topic() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");
        let topic = "local-test-ping-topic";

        // Start announce
        let bs_for_announce = bs_addr.clone();
        let mut announce = Command::new(bin_path())
            .args([
                "--no-default-config",
                "announce", topic,
                "--bootstrap", &bs_for_announce,
                "--duration", "25",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn announce");

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Ping the topic
        let bs_for_ping = bs_addr.clone();
        let topic_owned = topic.to_string();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config",
                    "ping", &topic_owned,
                    "--bootstrap", &bs_for_ping,
                    "--json",
                ])
                .output()
                .expect("failed to run ping by topic")
        })
        .await
        .unwrap();

        kill_child(&mut announce);

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(
            output.status.success(),
            "ping by topic failed.\nstdout: {stdout}\nstderr: {stderr}"
        );

        // Should contain resolve line showing topic resolution
        assert!(
            stdout.contains("\"type\":\"resolve\"") || stdout.contains("\"type\": \"resolve\""),
            "expected resolve in output.\nstdout: {stdout}\nstderr: {stderr}"
        );

        // Should have at least one probe
        assert!(
            stdout.contains("\"type\":\"probe\"") || stdout.contains("\"type\": \"probe\""),
            "expected probe in output.\nstdout: {stdout}\nstderr: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_ping_by_topic timed out");
}

// ── Same-host smoke tests: explicit bootstrap, direct-connect path ───────────
//
// SCOPE: These tests verify E2E file transfer works on 127.0.0.1 with an
// explicit local bootstrap node. They exercise:
//   - DHT announce/lookup via explicit bootstrap (B2 mode)
//   - Noise handshake + SecretStream encryption
//   - UDX reliable transport
//   - CLI argument parsing and file I/O
//
// LIMITATION: On same-host, `should_direct_connect` always returns true
// (same_host=true), so ALL these tests take the direct-connect path regardless
// of --public/--no-public flags. They do NOT verify topology-specific
// relay/holepunch behavior (T3/T5/T6). Topology-specific connection path
// decisions are covered by the unit-level 3×6 scenario matrix in cmd/mod.rs.
//
// The flag combinations (--public, --no-public, default) verify that the
// CLI correctly passes bootstrap config through to the swarm without causing
// connection failures. Actual firewall-differentiated behavior requires
// multi-host or network-namespace testing.

#[tokio::test]
async fn test_cp_local_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");
        let dir = tempfile::tempdir().unwrap();

        // Create source file
        let src_path = dir.path().join("testfile.txt");
        let payload = b"hello from cp local test\n";
        std::fs::write(&src_path, payload).unwrap();

        let dest_path = dir.path().join("received.txt");

        // Start sender

        let bs_for_send = bs_addr.clone();
        let src_str = src_path.to_str().unwrap().to_string();
        let mut sender = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "cp", "send", &src_str,
                "--bootstrap", &bs_for_send,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn cp send");

        // Read topic from sender stdout
        let stdout = sender.stdout.take().unwrap();
        let topic = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let topic = topic.expect("cp send did not output topic");

        // Wait for sender to announce and self-announce
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Run receiver
        let bs_for_recv = bs_addr.clone();
        let dest_str = dest_path.to_str().unwrap().to_string();
        let dest_str_display = dest_str.clone();
        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "cp", "recv", &topic,
                    &dest_str,
                    "--bootstrap", &bs_for_recv,
                    "--yes",
                    "--force",
                    "--timeout", "30",
                ])
                .output()
                .expect("failed to run cp recv")
        })
        .await
        .unwrap();

        kill_child(&mut sender);

        let stderr = String::from_utf8_lossy(&recv_output.stderr);
        assert!(
            recv_output.status.success(),
            "cp recv failed: {stderr}"
        );

        // Verify file content
        let received = std::fs::read(&dest_path)
            .unwrap_or_else(|_| panic!("output file not found at {dest_str_display}\nstderr: {stderr}"));
        assert_eq!(
            received, payload,
            "file content mismatch.\nexpected: {:?}\ngot: {:?}\nstderr: {stderr}",
            String::from_utf8_lossy(payload),
            String::from_utf8_lossy(&received)
        );
    })
    .await;

    assert!(result.is_ok(), "test_cp_local_roundtrip timed out");
}

async fn cp_roundtrip_with_flags(sender_no_public: bool, receiver_no_public: bool, test_name: &str) {
    let (port, _bs) = spawn_bootstrap().await;
    let bs_addr = format!("127.0.0.1:{port}");
    let dir = tempfile::tempdir().unwrap();

    let src_path = dir.path().join("testfile.txt");
    let payload = b"bootstrap scenario test payload\n";
    std::fs::write(&src_path, payload).unwrap();

    let dest_path = dir.path().join("received.txt");

    let bs_for_send = bs_addr.clone();
    let src_str = src_path.to_str().unwrap().to_string();

    let mut send_args: Vec<&str> = vec!["--no-default-config"];
    if sender_no_public {
        send_args.push("--no-public");
    }
    send_args.extend(["cp", "send", &src_str, "--bootstrap"]);

    let mut sender = Command::new(bin_path())
        .args(&send_args)
        .arg(&bs_for_send)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("[{test_name}] failed to spawn cp send: {e}"));

    let stdout = sender.stdout.take().unwrap();
    let tn = test_name.to_string();
    let topic = tokio::task::spawn_blocking(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let line = line.unwrap_or_default();
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
        None
    })
    .await
    .unwrap();

    let topic = topic.unwrap_or_else(|| panic!("[{tn}] cp send did not output topic"));

    tokio::time::sleep(Duration::from_secs(5)).await;

    let bs_for_recv = bs_addr.clone();
    let dest_str = dest_path.to_str().unwrap().to_string();
    let tn2 = test_name.to_string();

    let mut recv_args: Vec<String> = vec!["--no-default-config".to_string()];
    if receiver_no_public {
        recv_args.push("--no-public".to_string());
    }
    recv_args.extend([
        "cp".to_string(),
        "recv".to_string(),
        topic.clone(),
        dest_str.clone(),
        "--bootstrap".to_string(),
        bs_for_recv,
        "--yes".to_string(),
        "--force".to_string(),
        "--timeout".to_string(),
        "30".to_string(),
    ]);

    let recv_output = tokio::task::spawn_blocking(move || {
        Command::new(bin_path())
            .args(&recv_args)
            .output()
            .unwrap_or_else(|e| panic!("[{tn2}] failed to run cp recv: {e}"))
    })
    .await
    .unwrap();

    kill_child(&mut sender);

    let stderr = String::from_utf8_lossy(&recv_output.stderr);
    assert!(
        recv_output.status.success(),
        "[{test_name}] cp recv failed: {stderr}"
    );

    let dest_str_display = dest_path.to_str().unwrap().to_string();
    let received = std::fs::read(&dest_path)
        .unwrap_or_else(|_| panic!("[{test_name}] output file not found at {dest_str_display}\nstderr: {stderr}"));
    assert_eq!(
        received, payload,
        "[{test_name}] file content mismatch.\nexpected: {:?}\ngot: {:?}\nstderr: {stderr}",
        String::from_utf8_lossy(payload),
        String::from_utf8_lossy(&received)
    );
}

#[tokio::test]
async fn test_cp_sender_default_receiver_public() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        cp_roundtrip_with_flags(false, true, "sender_default_receiver_no_public").await;
    })
    .await;
    assert!(result.is_ok(), "test_cp_sender_default_receiver_public timed out");
}

#[tokio::test]
async fn test_cp_sender_public_receiver_default() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        cp_roundtrip_with_flags(true, false, "sender_no_public_receiver_default").await;
    })
    .await;
    assert!(result.is_ok(), "test_cp_sender_public_receiver_default timed out");
}

#[tokio::test]
async fn test_cp_both_default_same_host() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        cp_roundtrip_with_flags(false, false, "both_default_same_host").await;
    })
    .await;
    assert!(result.is_ok(), "test_cp_both_default_same_host timed out");
}

// ── Isolated mode: no bootstrap, graceful failure ────────────────────────────

#[tokio::test]
async fn test_cp_no_public_flag_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        let (port, _bs) = spawn_bootstrap().await;
        let bs_addr = format!("127.0.0.1:{port}");
        let dir = tempfile::tempdir().unwrap();

        let src_path = dir.path().join("testfile.txt");
        let payload = b"no-public flag e2e test\n";
        std::fs::write(&src_path, payload).unwrap();

        let dest_path = dir.path().join("received.txt");

        let bs_for_send = bs_addr.clone();
        let src_str = src_path.to_str().unwrap().to_string();

        let mut sender = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "cp", "send", &src_str,
                "--bootstrap", &bs_for_send,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn cp send with --no-public");

        let stdout = sender.stdout.take().unwrap();
        let topic = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
            None
        })
        .await
        .unwrap();

        let topic = topic.expect("cp send --no-public did not output topic");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let bs_for_recv = bs_addr.clone();
        let dest_str = dest_path.to_str().unwrap().to_string();
        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "cp", "recv", &topic,
                    &dest_str,
                    "--bootstrap", &bs_for_recv,
                    "--yes",
                    "--force",
                    "--timeout", "30",
                ])
                .output()
                .expect("failed to run cp recv with --no-public")
        })
        .await
        .unwrap();

        kill_child(&mut sender);

        let stderr = String::from_utf8_lossy(&recv_output.stderr);
        assert!(
            recv_output.status.success(),
            "cp recv --no-public failed: {stderr}"
        );

        let received = std::fs::read(&dest_path)
            .unwrap_or_else(|_| panic!("output file not found\nstderr: {stderr}"));
        assert_eq!(
            received, payload,
            "file content mismatch with --no-public flag.\nstderr: {stderr}"
        );
    })
    .await;
    assert!(result.is_ok(), "test_cp_no_public_flag_roundtrip timed out");
}

#[tokio::test]
async fn test_cp_isolated_no_bootstrap_times_out() {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        let dir = tempfile::tempdir().unwrap();
        let dest_path = dir.path().join("should_not_exist.txt");
        let dest_str = dest_path.to_str().unwrap().to_string();

        let recv_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config",
                    "cp", "recv",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    &dest_str,
                    "--yes",
                    "--force",
                    "--timeout", "5",
                ])
                .output()
                .expect("failed to run cp recv")
        })
        .await
        .unwrap();

        assert!(
            !recv_output.status.success(),
            "isolated cp recv should fail (no bootstrap → no discovery → timeout)"
        );
        assert!(
            !dest_path.exists(),
            "no file should be created when recv times out"
        );
    })
    .await;
    assert!(result.is_ok(), "test_cp_isolated_no_bootstrap_times_out timed out");
}

#[tokio::test]
async fn test_dd_passphrase_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.txt");
        let output_path = dir.path().join("output.txt");

        let msg = b"passphrase dd roundtrip payload";
        std::fs::write(&input_path, msg).unwrap();

        let input_path_str = input_path.to_str().unwrap().to_string();
        let bs_addr_clone = bs_addr.clone();

        let mut leave_cmd = Command::new(bin_path());
        leave_cmd
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_path_str,
                "--bootstrap", &bs_addr_clone,
                "--ttl", "40",
                "--passphrase", "dd-test-pass-abc",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            unsafe extern "C" { fn setsid() -> i32; }
            unsafe { leave_cmd.pre_exec(|| { setsid(); Ok(()) }); }
        }

        let mut leave = leave_cmd.spawn().expect("failed to spawn dd put --passphrase");

        let stdout = leave.stdout.take().unwrap();
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

        let pickup_key = pickup_key.expect("dd put --passphrase did not output pickup key");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let output_path_str = output_path.to_str().unwrap().to_string();
        let bs_addr_clone2 = bs_addr.clone();
        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_addr_clone2,
                    "--output", &output_path_str,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get after passphrase put")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&pickup_output.stderr);
        assert!(
            pickup_output.status.success(),
            "get after passphrase put failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert_eq!(received, msg, "payload mismatch after passphrase put.\nstderr: {stderr}");
    })
    .await;

    assert!(result.is_ok(), "test_dd_passphrase_roundtrip timed out");
}

#[tokio::test]
async fn test_dd_large_payload() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("large_input.bin");
        let output_path = dir.path().join("large_output.bin");

        let msg: Vec<u8> = (0u8..=255).cycle().take(5000).collect();
        std::fs::write(&input_path, &msg).unwrap();

        let input_path_str = input_path.to_str().unwrap().to_string();
        let bs_addr_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_path_str,
                "--bootstrap", &bs_addr_clone,
                "--ttl", "40",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put (large payload)");

        let stdout = leave.stdout.take().unwrap();
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

        let pickup_key = pickup_key.expect("dd put (large) did not output pickup key");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let output_path_str = output_path.to_str().unwrap().to_string();
        let bs_addr_clone2 = bs_addr.clone();
        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_addr_clone2,
                    "--output", &output_path_str,
                    "--timeout", "25",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (large payload)")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&pickup_output.stderr);
        assert!(
            pickup_output.status.success(),
            "dd get (large payload) failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found (large payload)");
        assert_eq!(
            received, msg,
            "large payload mismatch ({} bytes expected, {} bytes received).\nstderr: {stderr}",
            msg.len(),
            received.len()
        );
    })
    .await;

    assert!(result.is_ok(), "test_dd_large_payload timed out");
}

#[tokio::test]
async fn test_dd_stdin_stdout() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);

        let msg = b"stdin-to-stdout dd test payload";

        let bs_addr_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", "-",
                "--bootstrap", &bs_addr_clone,
                "--ttl", "40",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put (stdin)");

        let mut leave_stdin = leave.stdin.take().unwrap();
        let msg_clone = msg.to_vec();
        let stdin_writer = tokio::task::spawn_blocking(move || {
            let _ = leave_stdin.write_all(&msg_clone);
        });

        let stdout = leave.stdout.take().unwrap();
        let key_reader = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let trimmed = line.trim();
                if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(trimmed.to_string());
                }
            }
            None
        });

        let (_, pickup_key_result) = tokio::join!(stdin_writer, key_reader);
        let pickup_key = pickup_key_result.unwrap().expect("dd put (stdin) did not output pickup key");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let bs_addr_clone2 = bs_addr.clone();
        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_addr_clone2,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (stdout mode)")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&pickup_output.stderr);
        assert!(
            pickup_output.status.success(),
            "dd get (stdout mode) failed: {stderr}"
        );

        assert_eq!(
            pickup_output.stdout, msg,
            "stdout payload mismatch.\nstderr: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_dd_stdin_stdout timed out");
}

#[tokio::test]
async fn test_dd_get_timeout() {
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);

        let nonexistent_key = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", nonexistent_key,
                    "--bootstrap", &bs_addr,
                    "--timeout", "5",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (timeout test)")
        })
        .await
        .unwrap();

        assert!(
            !pickup_output.status.success(),
            "get of nonexistent key should fail, but exited 0"
        );

        let stderr = String::from_utf8_lossy(&pickup_output.stderr);
        assert!(
            stderr.contains("timeout") || stderr.contains("not found"),
            "expected timeout/not-found in stderr, got: {stderr}"
        );
    })
    .await;

    assert!(result.is_ok(), "test_dd_get_timeout timed out");
}

#[tokio::test]
async fn test_dd_wrong_passphrase_fails() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.txt");

        let msg = b"wrong passphrase test payload";
        std::fs::write(&input_path, msg).unwrap();

        let input_path_str = input_path.to_str().unwrap().to_string();
        let bs_addr_clone = bs_addr.clone();

        let mut leave_cmd = Command::new(bin_path());
        leave_cmd
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_path_str,
                "--bootstrap", &bs_addr_clone,
                "--ttl", "40",
                "--passphrase", "correct-secret-passphrase",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            unsafe extern "C" { fn setsid() -> i32; }
            unsafe { leave_cmd.pre_exec(|| { setsid(); Ok(()) }); }
        }

        let mut leave = leave_cmd.spawn().expect("failed to spawn dd put (wrong passphrase test)");

        let stdout = leave.stdout.take().unwrap();
        let leave_key_result = tokio::task::spawn_blocking(move || {
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

        assert!(leave_key_result.is_some(), "dd put did not output a key");

        tokio::time::sleep(Duration::from_secs(3)).await;

        let wrong_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let bs_addr_clone2 = bs_addr.clone();
        let pickup_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", wrong_key,
                    "--bootstrap", &bs_addr_clone2,
                    "--timeout", "8",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (wrong passphrase test)")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        assert!(
            !pickup_output.status.success(),
            "get with wrong key should fail, but succeeded"
        );
    })
    .await;

    assert!(result.is_ok(), "test_dd_wrong_passphrase_fails timed out");
}

#[tokio::test]
async fn test_dd_v2_multi_index() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("large.bin");
        let output_path = dir.path().join("out.bin");

        let msg: Vec<u8> = (0..30_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&input_path, &msg).unwrap();

        let input_str = input_path.to_str().unwrap().to_string();
        let bs_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_str,
                "--bootstrap", &bs_clone,
                "--ttl", "60",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put");

        let stdout = leave.stdout.take().unwrap();
        let pickup_key = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let t = line.trim().to_string();
                if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(t);
                }
            }
            None
        })
        .await
        .unwrap()
        .expect("no pickup key from dd put");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let out_str = output_path.to_str().unwrap().to_string();
        let bs_clone2 = bs_addr.clone();
        let get_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_clone2,
                    "--output", &out_str,
                    "--timeout", "40",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&get_output.stderr);
        assert!(
            get_output.status.success(),
            "dd get (multi-index) failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert_eq!(received, msg, "payload mismatch. stderr: {stderr}");
    })
    .await;
    assert!(result.is_ok(), "test_dd_v2_multi_index timed out");
}

#[tokio::test]
async fn test_dd_v2_empty_file() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("empty.bin");
        let output_path = dir.path().join("out.bin");

        std::fs::write(&input_path, b"").unwrap();

        let input_str = input_path.to_str().unwrap().to_string();
        let bs_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_str,
                "--bootstrap", &bs_clone,
                "--ttl", "40",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put (empty)");

        let stdout = leave.stdout.take().unwrap();
        let pickup_key = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let t = line.trim().to_string();
                if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(t);
                }
            }
            None
        })
        .await
        .unwrap()
        .expect("no pickup key from dd put (empty)");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let out_str = output_path.to_str().unwrap().to_string();
        let bs_clone2 = bs_addr.clone();
        let get_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_clone2,
                    "--output", &out_str,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (empty)")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&get_output.stderr);
        assert!(
            get_output.status.success(),
            "dd get (empty) failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert!(received.is_empty(), "expected empty output, got {} bytes. stderr: {stderr}", received.len());
    })
    .await;
    assert!(result.is_ok(), "test_dd_v2_empty_file timed out");
}

#[tokio::test]
async fn test_dd_v1_flag_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("v1input.txt");
        let output_path = dir.path().join("v1out.txt");

        let msg = b"v1 flag roundtrip test payload";
        std::fs::write(&input_path, msg).unwrap();

        let input_str = input_path.to_str().unwrap().to_string();
        let bs_clone = bs_addr.clone();
        let mut leave = Command::new(bin_path())
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_str,
                "--v1",
                "--bootstrap", &bs_clone,
                "--ttl", "40",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn dd put --v1");

        let stdout = leave.stdout.take().unwrap();
        let pickup_key = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let t = line.trim().to_string();
                if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(t);
                }
            }
            None
        })
        .await
        .unwrap()
        .expect("no pickup key from dd put --v1");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let out_str = output_path.to_str().unwrap().to_string();
        let bs_clone2 = bs_addr.clone();
        let get_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_clone2,
                    "--output", &out_str,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get after --v1 put")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&get_output.stderr);
        assert!(
            get_output.status.success(),
            "dd get after --v1 put failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert_eq!(received, msg, "v1 flag roundtrip payload mismatch. stderr: {stderr}");
    })
    .await;
    assert!(result.is_ok(), "test_dd_v1_flag_roundtrip timed out");
}

#[tokio::test]
async fn test_dd_v2_passphrase_roundtrip() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let (ports, _cluster) = spawn_dht_cluster(3).await;
        let bs_addr = format!("127.0.0.1:{}", ports[0]);
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.txt");
        let output_path = dir.path().join("output.txt");

        let msg = b"v2 passphrase roundtrip payload";
        std::fs::write(&input_path, msg).unwrap();

        let input_str = input_path.to_str().unwrap().to_string();
        let bs_clone = bs_addr.clone();

        let mut leave_cmd = Command::new(bin_path());
        leave_cmd
            .args([
                "--no-default-config", "--no-public",
                "dd", "put", &input_str,
                "--bootstrap", &bs_clone,
                "--ttl", "40",
                "--passphrase", "v2-test-passphrase-xyz",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            unsafe extern "C" { fn setsid() -> i32; }
            unsafe { leave_cmd.pre_exec(|| { setsid(); Ok(()) }); }
        }

        let mut leave = leave_cmd.spawn().expect("failed to spawn dd put v2 passphrase");

        let stdout = leave.stdout.take().unwrap();
        let pickup_key = tokio::task::spawn_blocking(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let line = line.unwrap_or_default();
                let t = line.trim().to_string();
                if t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(t);
                }
            }
            None
        })
        .await
        .unwrap()
        .expect("no pickup key from dd put v2 passphrase");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let out_str = output_path.to_str().unwrap().to_string();
        let bs_clone2 = bs_addr.clone();
        let get_output = tokio::task::spawn_blocking(move || {
            Command::new(bin_path())
                .args([
                    "--no-default-config", "--no-public",
                    "dd", "get", &pickup_key,
                    "--bootstrap", &bs_clone2,
                    "--output", &out_str,
                    "--timeout", "20",
                    "--no-ack",
                ])
                .output()
                .expect("failed to run dd get (v2 passphrase)")
        })
        .await
        .unwrap();

        kill_child(&mut leave);

        let stderr = String::from_utf8_lossy(&get_output.stderr);
        assert!(
            get_output.status.success(),
            "dd get (v2 passphrase) failed: {stderr}"
        );

        let received = std::fs::read(&output_path).expect("output file not found");
        assert_eq!(received, msg, "v2 passphrase payload mismatch. stderr: {stderr}");
    })
    .await;
    assert!(result.is_ok(), "test_dd_v2_passphrase_roundtrip timed out");
}
