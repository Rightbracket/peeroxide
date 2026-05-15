use clap::{Args, Subcommand};
use peeroxide::{spawn, JoinOpts, SwarmConfig};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::io::AsyncWriteExt;

use crate::config::ResolvedConfig;
use super::{build_dht_config, parse_topic, to_hex};

const CHUNK_SIZE: usize = 65536;

#[derive(Subcommand)]
pub enum CpCommands {
    /// Send a file to a peer
    Send(SendArgs),
    /// Receive a file from a peer
    Recv(RecvArgs),
}

#[derive(Args)]
pub struct SendArgs {
    /// File path or - for stdin
    file: String,

    /// Topic (64-char hex or plaintext); random if omitted
    topic: Option<String>,

    /// Override filename in metadata header
    #[arg(long)]
    name: Option<String>,

    /// Stay alive for multiple transfers (sequential)
    #[arg(long)]
    keep_alive: bool,

    /// Show transfer progress bar
    #[arg(long)]
    progress: bool,
}

#[derive(Args)]
pub struct RecvArgs {
    /// Topic from the sender
    topic: String,

    /// Destination path, directory, or - for stdout
    dest: Option<String>,

    /// Skip confirmation prompt
    #[arg(long)]
    yes: bool,

    /// Allow overwriting existing files
    #[arg(long)]
    force: bool,

    /// Timeout in seconds waiting for sender (default: 60)
    #[arg(long, default_value_t = 60)]
    timeout: u64,

    /// Show transfer progress bar
    #[arg(long)]
    progress: bool,
}

pub async fn run(cmd: CpCommands, cfg: &ResolvedConfig) -> i32 {
    match cmd {
        CpCommands::Send(args) => run_send(args, cfg).await,
        CpCommands::Recv(args) => run_recv(args, cfg).await,
    }
}

async fn run_send(args: SendArgs, cfg: &ResolvedConfig) -> i32 {
    if args.file == "-" && args.keep_alive {
        eprintln!("error: --keep-alive is incompatible with stdin (-)");
        return 1;
    }

    let (file_size, file_name) = if args.file == "-" {
        (None, args.name.clone().unwrap_or_else(|| "stdin".to_string()))
    } else {
        let path = PathBuf::from(&args.file);
        if !path.exists() {
            eprintln!("error: file not found: {}", args.file);
            return 1;
        }
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("error: cannot read file: {e}");
                return 1;
            }
        };
        let name = args.name.clone().unwrap_or_else(|| {
            path.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string()
        });
        (Some(metadata.len()), name)
    };

    let topic = match &args.topic {
        Some(t) => parse_topic(t),
        None => {
            let mut bytes = [0u8; 32];
            use rand::RngCore;
            rand::rng().fill_bytes(&mut bytes);
            bytes
        }
    };

    let topic_hex = to_hex(&topic);
    let topic_display = match &args.topic {
        Some(t) if t.len() == 64 && hex::decode(t).is_ok() => topic_hex.clone(),
        Some(t) => t.clone(),
        None => topic_hex.clone(),
    };

    let dht_config = build_dht_config(cfg);
    let mut swarm_config = SwarmConfig::default();
    swarm_config.dht = dht_config;
    // PEEROXIDE_LOCAL_CONNECTION=false disables the same-NAT LAN-shortcut.
    // Used by `test_live_cp_send_recv_no_lan` to force the real network path.
    if std::env::var("PEEROXIDE_LOCAL_CONNECTION").as_deref() == Ok("false") {
        swarm_config.local_connection = false;
    }

    let (task, handle, mut conn_rx) = match spawn(swarm_config).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: failed to start swarm: {e}");
            return 1;
        }
    };

    let mut join_opts = JoinOpts::default();
    join_opts.client = false;
    if let Err(e) = handle.join(topic, join_opts).await {
        eprintln!("error: failed to join topic: {e}");
        return 1;
    }

    if let Err(e) = handle.flush().await {
        eprintln!("error: flush failed: {e}");
        return 1;
    }

    println!("{topic_display}");

    let size_display = file_size
        .map(format_size)
        .unwrap_or_else(|| "unknown size".to_string());
    eprintln!("CP SEND {file_name} ({size_display})");
    eprintln!("  topic: {topic_hex}");
    eprintln!("  waiting for receiver...");

    let mut transfer_count = 0u64;
    let mut interrupted = false;

    loop {
        let conn = tokio::select! {
            c = conn_rx.recv() => {
                match c {
                    Some(c) => c,
                    None => break,
                }
            }
            _ = signal::ctrl_c() => {
                interrupted = true;
                break;
            }
        };

        let remote_pk = to_hex(conn.remote_public_key());
        eprintln!("  connected from @{}", &remote_pk[..8]);

        let start = Instant::now();
        let bytes_sent = stream_send_file(&args.file, file_size, &file_name, conn, args.progress).await;

        match bytes_sent {
            Ok(sent) => {
                let elapsed = start.elapsed();
                let speed = if elapsed.as_secs_f64() > 0.0 {
                    format_size((sent as f64 / elapsed.as_secs_f64()) as u64)
                } else {
                    "∞".to_string()
                };
                eprintln!("  done: {} in {:.1}s ({speed}/s)", format_size(sent), elapsed.as_secs_f64());
                transfer_count += 1;
            }
            Err(ref e) if e == "interrupted" => {
                eprintln!("  interrupted");
                interrupted = true;
                break;
            }
            Err(e) => {
                eprintln!("  error during transfer: {e}");
            }
        }

        if !args.keep_alive {
            break;
        }
    }

    let _ = handle.destroy().await;
    let _ = task.await;

    if interrupted {
        130
    } else if transfer_count > 0 {
        0
    } else {
        1
    }
}

async fn stream_send_file(
    file_path: &str,
    file_size: Option<u64>,
    file_name: &str,
    mut conn: peeroxide::SwarmConnection,
    show_progress: bool,
) -> Result<u64, String> {
    use indicatif::{ProgressBar, ProgressStyle};

    let metadata = serde_json::json!({
        "filename": file_name,
        "size": file_size,
        "version": 1,
    });
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();

    tokio::select! {
        result = conn.peer.stream.write(&metadata_bytes) => {
            result.map_err(|e| format!("failed to send metadata: {e}"))?;
        }
        _ = signal::ctrl_c() => {
            return Err("interrupted".to_string());
        }
    };

    let pb = if show_progress {
        match file_size {
            Some(size) => {
                let pb = ProgressBar::new(size);
                pb.set_style(ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                    .unwrap()
                    .progress_chars("#>-"));
                Some(pb)
            }
            None => {
                let pb = ProgressBar::new_spinner();
                pb.set_style(ProgressStyle::default_spinner()
                    .template("{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})")
                    .unwrap());
                Some(pb)
            }
        }
    } else {
        None
    };

    let mut total_sent: u64 = 0;

    if file_path == "-" {
        use tokio::io::AsyncReadExt;
        let mut stdin = tokio::io::stdin();
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let n = tokio::select! {
                result = stdin.read(&mut buf) => {
                    result.map_err(|e| format!("stdin read error: {e}"))?
                }
                _ = signal::ctrl_c() => {
                    return Err("interrupted".to_string());
                }
            };
            if n == 0 {
                break;
            }
            tokio::select! {
                result = conn.peer.stream.write(&buf[..n]) => {
                    result.map_err(|e| format!("write failed: {e}"))?;
                }
                _ = signal::ctrl_c() => {
                    return Err("interrupted".to_string());
                }
            };
            total_sent += n as u64;
            if let Some(ref pb) = pb {
                pb.set_position(total_sent);
            }
        }
    } else {
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| format!("failed to open file: {e}"))?;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let n = tokio::select! {
                result = file.read(&mut buf) => {
                    result.map_err(|e| format!("file read error: {e}"))?
                }
                _ = signal::ctrl_c() => {
                    return Err("interrupted".to_string());
                }
            };
            if n == 0 {
                break;
            }
            tokio::select! {
                result = conn.peer.stream.write(&buf[..n]) => {
                    result.map_err(|e| format!("write failed: {e}"))?;
                }
                _ = signal::ctrl_c() => {
                    return Err("interrupted".to_string());
                }
            };
            total_sent += n as u64;
            if let Some(ref pb) = pb {
                pb.set_position(total_sent);
            }
        }
    }

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    tokio::select! {
        result = conn.peer.stream.shutdown() => {
            result.map_err(|e| format!("shutdown failed: {e}"))?;
        }
        _ = signal::ctrl_c() => {
            return Err("interrupted".to_string());
        }
    };
    drop(conn);
    Ok(total_sent)
}

async fn run_recv(args: RecvArgs, cfg: &ResolvedConfig) -> i32 {
    let topic = parse_topic(&args.topic);
    let topic_hex = to_hex(&topic);

    let stdout_mode = args.dest.as_deref() == Some("-");

    let dht_config = build_dht_config(cfg);
    let mut swarm_config = SwarmConfig::default();
    swarm_config.dht = dht_config;
    // PEEROXIDE_LOCAL_CONNECTION=false disables the same-NAT LAN-shortcut.
    // Used by `test_live_cp_send_recv_no_lan` to force the real network path.
    if std::env::var("PEEROXIDE_LOCAL_CONNECTION").as_deref() == Ok("false") {
        swarm_config.local_connection = false;
    }

    let (task, handle, mut conn_rx) = match spawn(swarm_config).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: failed to start swarm: {e}");
            return 1;
        }
    };

    let mut join_opts = JoinOpts::default();
    join_opts.server = false;
    if let Err(e) = handle.join(topic, join_opts).await {
        eprintln!("error: failed to join topic: {e}");
        return 1;
    }

    eprintln!("CP RECV topic: {topic_hex}");
    eprintln!("  looking up sender...");

    let timeout_dur = Duration::from_secs(args.timeout);
    let retry_interval = Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + timeout_dur;
    let mut conn = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            eprintln!("error: sender not found within {}s timeout", args.timeout);
            let _ = handle.destroy().await;
            let _ = task.await;
            return 1;
        }
        let wait_time = remaining.min(retry_interval);

        tokio::select! {
            c = conn_rx.recv() => {
                match c {
                    Some(c) => break c,
                    None => {
                        eprintln!("error: connection channel closed");
                        let _ = handle.destroy().await;
                        let _ = task.await;
                        return 1;
                    }
                }
            }
            _ = tokio::time::sleep(wait_time) => {
                let _ = handle.leave(topic).await;
                let mut retry_opts = JoinOpts::default();
                retry_opts.server = false;
                if let Err(e) = handle.join(topic, retry_opts).await {
                    eprintln!("error: retry join failed: {e}");
                    let _ = handle.destroy().await;
                    let _ = task.await;
                    return 1;
                }
                eprintln!("  retrying lookup...");
            }
            _ = signal::ctrl_c() => {
                let _ = handle.destroy().await;
                let _ = task.await;
                return 130;
            }
        }
    };

    let remote_pk = to_hex(conn.remote_public_key());
    eprintln!("  connected to @{}", &remote_pk[..8]);

    let metadata_msg = match tokio::time::timeout(
        Duration::from_secs(args.timeout),
        conn.peer.stream.read(),
    )
    .await
    {
        Ok(Ok(Some(data))) => data,
        _ => {
            eprintln!("error: failed to receive metadata from sender");
            let _ = handle.destroy().await;
            let _ = task.await;
            return 1;
        }
    };

    let metadata: serde_json::Value = match serde_json::from_slice(&metadata_msg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: invalid metadata from sender: {e}");
            return 1;
        }
    };

    let version = metadata["version"].as_u64().unwrap_or(0);
    if version != 1 {
        eprintln!("error: unsupported protocol version: {version}");
        return 1;
    }

    let filename = match metadata.get("filename").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            eprintln!("error: sender metadata missing or invalid 'filename' field");
            let _ = handle.destroy().await;
            let _ = task.await;
            return 1;
        }
    };

    if filename.contains('/') || filename.contains('\\') || filename == "." || filename == ".." {
        eprintln!("error: sender filename contains path separators or is invalid: \"{filename}\"");
        let _ = handle.destroy().await;
        let _ = task.await;
        return 1;
    }

    let expected_size = match metadata.get("size") {
        Some(serde_json::Value::Null) | None => None,
        Some(v) => match v.as_u64() {
            Some(n) => Some(n),
            None => {
                eprintln!("error: sender metadata 'size' is not a valid non-negative integer");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        },
    };

    let sanitized_name = sanitize_filename(&filename);
    if sanitized_name != filename {
        eprintln!("  warning: filename sanitized: \"{}\" → \"{}\"", filename, sanitized_name);
    }

    let dest_path = if stdout_mode {
        None
    } else {
        Some(resolve_dest_path(&args.dest, &sanitized_name))
    };

    if !stdout_mode {
        let size_display = expected_size
            .map(format_size)
            .unwrap_or_else(|| "unknown size".to_string());

        let dest_display = dest_path.as_ref().unwrap().display();

        if !args.yes {
            eprintln!("  Incoming file: {filename} ({size_display})");
            eprintln!("  Save to: {dest_display}");

            if let Some(ref p) = dest_path {
                if p.exists() && !args.force {
                    eprintln!("  (file exists — will overwrite)");
                }
            }

            eprint!("  Accept? [y/N] ");
            std::io::stderr().flush().ok();

            let accepted = prompt_tty_yes();
            if !accepted {
                eprintln!("  rejected");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        } else if let Some(ref p) = dest_path {
            if p.exists() && !args.force {
                eprintln!("error: destination exists and --force not specified");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        }
    }

    let start = Instant::now();
    let mut total_received: u64 = 0;

    if stdout_mode {
        use indicatif::{ProgressBar, ProgressStyle};
        let pb = if args.progress {
            match expected_size {
                Some(size) => {
                    let pb = ProgressBar::new(size);
                    pb.set_style(ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                        .unwrap()
                        .progress_chars("#>-"));
                    Some(pb)
                }
                None => {
                    let pb = ProgressBar::new_spinner();
                    pb.set_style(ProgressStyle::default_spinner()
                        .template("{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})")
                        .unwrap());
                    Some(pb)
                }
            }
        } else {
            None
        };

        let mut stdout = tokio::io::stdout();
        loop {
            let chunk = tokio::select! {
                result = tokio::time::timeout(
                    Duration::from_secs(args.timeout),
                    conn.peer.stream.read(),
                ) => {
                    match result {
                        Ok(Ok(Some(data))) => data,
                        Ok(Ok(None)) => break,
                        Ok(Err(e)) => {
                            if let Some(ref pb) = pb { pb.abandon(); }
                            eprintln!("error: read error during transfer: {e}");
                            let _ = handle.destroy().await;
                            let _ = task.await;
                            return 1;
                        }
                        Err(_) => {
                            if let Some(ref pb) = pb { pb.abandon(); }
                            eprintln!("error: transfer stalled (no data for {}s)", args.timeout);
                            let _ = handle.destroy().await;
                            let _ = task.await;
                            return 1;
                        }
                    }
                }
                _ = signal::ctrl_c() => {
                    if let Some(ref pb) = pb { pb.abandon(); }
                    let _ = handle.destroy().await;
                    let _ = task.await;
                    return 130;
                }
            };

            total_received += chunk.len() as u64;
            if let Some(expected) = expected_size {
                if total_received > expected {
                    if let Some(ref pb) = pb { pb.abandon(); }
                    eprintln!("error: received more data than expected size");
                    let _ = handle.destroy().await;
                    let _ = task.await;
                    return 1;
                }
            }
            if let Some(ref pb) = pb {
                pb.set_position(total_received);
            }

            if let Err(e) = stdout.write_all(&chunk).await {
                if let Some(ref pb) = pb { pb.abandon(); }
                eprintln!("error: failed to write to stdout: {e}");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        }
        let _ = stdout.flush().await;
        if let Some(pb) = pb { pb.finish_and_clear(); }

        if let Some(expected) = expected_size {
            if total_received != expected {
                eprintln!("error: size mismatch (expected {expected}, got {total_received})");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        }
    } else {
        let dest = dest_path.unwrap();
        let dir = dest.parent().unwrap_or(std::path::Path::new("."));
        let temp_path = dir.join(format!(".peeroxide-recv-{}-{}", std::process::id(), rand_suffix()));

        let mut temp_file = match tokio::fs::File::create(&temp_path).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("error: failed to create temp file: {e}");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        };

        use indicatif::{ProgressBar, ProgressStyle};
        let pb = if args.progress {
            match expected_size {
                Some(size) => {
                    let pb = ProgressBar::new(size);
                    pb.set_style(ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                        .unwrap()
                        .progress_chars("#>-"));
                    Some(pb)
                }
                None => {
                    let pb = ProgressBar::new_spinner();
                    pb.set_style(ProgressStyle::default_spinner()
                        .template("{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})")
                        .unwrap());
                    Some(pb)
                }
            }
        } else {
            None
        };

        loop {
            let chunk = tokio::select! {
                result = tokio::time::timeout(
                    Duration::from_secs(args.timeout),
                    conn.peer.stream.read(),
                ) => {
                    match result {
                        Ok(Ok(Some(data))) => data,
                        Ok(Ok(None)) => break,
                        Ok(Err(e)) => {
                            if let Some(ref pb) = pb { pb.abandon(); }
                            let _ = tokio::fs::remove_file(&temp_path).await;
                            eprintln!("error: read error during transfer: {e}");
                            let _ = handle.destroy().await;
                            let _ = task.await;
                            return 1;
                        }
                        Err(_) => {
                            if let Some(ref pb) = pb { pb.abandon(); }
                            let _ = tokio::fs::remove_file(&temp_path).await;
                            eprintln!("error: transfer stalled (no data for {}s)", args.timeout);
                            let _ = handle.destroy().await;
                            let _ = task.await;
                            return 1;
                        }
                    }
                }
                _ = signal::ctrl_c() => {
                    if let Some(ref pb) = pb { pb.abandon(); }
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    let _ = handle.destroy().await;
                    let _ = task.await;
                    return 130;
                }
            };

            total_received += chunk.len() as u64;
            if let Some(expected) = expected_size {
                if total_received > expected {
                    if let Some(ref pb) = pb { pb.abandon(); }
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    eprintln!("error: received more data than expected size");
                    let _ = handle.destroy().await;
                    let _ = task.await;
                    return 1;
                }
            }
            if let Some(ref pb) = pb {
                pb.set_position(total_received);
            }

            if let Err(e) = temp_file.write_all(&chunk).await {
                if let Some(ref pb) = pb { pb.abandon(); }
                let _ = tokio::fs::remove_file(&temp_path).await;
                eprintln!("error: failed to write chunk: {e}");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        }

        if let Err(e) = temp_file.flush().await {
            if let Some(ref pb) = pb { pb.abandon(); }
            let _ = tokio::fs::remove_file(&temp_path).await;
            eprintln!("error: failed to flush file: {e}");
            let _ = handle.destroy().await;
            let _ = task.await;
            return 1;
        }
        drop(temp_file);

        if let Some(expected) = expected_size {
            if total_received != expected {
                if let Some(ref pb) = pb { pb.abandon(); }
                let _ = tokio::fs::remove_file(&temp_path).await;
                eprintln!("error: size mismatch (expected {expected}, got {total_received})");
                let _ = handle.destroy().await;
                let _ = task.await;
                return 1;
            }
        }
        if let Some(pb) = pb { pb.finish_and_clear(); }

        if let Err(e) = tokio::fs::rename(&temp_path, &dest).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            eprintln!("error: failed to rename temp file: {e}");
            let _ = handle.destroy().await;
            let _ = task.await;
            return 1;
        }

        let elapsed = start.elapsed();
        let size_display = format_size(total_received);
        let speed = if elapsed.as_secs_f64() > 0.0 {
            format_size((total_received as f64 / elapsed.as_secs_f64()) as u64)
        } else {
            "∞".to_string()
        };
        eprintln!("  done: {size_display} in {:.1}s ({speed}/s)", elapsed.as_secs_f64());
        eprintln!("  saved to {}", dest.display());
    }

    let _ = handle.destroy().await;
    let _ = task.await;
    0
}

fn prompt_tty_yes() -> bool {
    use std::io::Read;
    if let Ok(mut tty) = std::fs::File::open("/dev/tty") {
        let mut buf = [0u8; 64];
        if let Ok(n) = tty.read(&mut buf) {
            let answer = String::from_utf8_lossy(&buf[..n]);
            return answer.trim().eq_ignore_ascii_case("y");
        }
    }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).ok();
    line.trim().eq_ignore_ascii_case("y")
}

fn rand_suffix() -> u32 {
    use rand::RngCore;
    rand::rng().next_u32()
}

fn sanitize_filename(name: &str) -> String {
    let base = name
        .replace(['/', '\\', '\0'], "_")
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>();
    let trimmed = base.trim().to_string();
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "download".to_string()
    } else {
        trimmed
    }
}

fn resolve_dest_path(dest: &Option<String>, filename: &str) -> PathBuf {
    match dest {
        Some(d) if d != "-" => {
            let p = PathBuf::from(d);
            if p.is_dir() {
                p.join(filename)
            } else {
                p
            }
        }
        _ => PathBuf::from(filename),
    }
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
