use clap::Args;
use peeroxide::{spawn, JoinOpts, KeyPair, SwarmConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::signal;
use tokio::sync::Semaphore;

use crate::config::ResolvedConfig;
use super::{build_dht_config, parse_topic, to_hex};

const PING_MAGIC: &[u8; 4] = b"PING";
const PONG_MAGIC: &[u8; 4] = b"PONG";
const MAX_ECHO_SESSIONS: usize = 64;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const ECHO_MSG_LEN: usize = 16;

#[derive(Args)]
pub struct AnnounceArgs {
    /// Target topic (64-char hex = raw hash, plaintext = BLAKE2b hashed)
    topic: String,

    /// Seed string to derive deterministic keypair
    #[arg(long)]
    seed: Option<String>,

    /// Metadata to store on the DHT (max 1000 UTF-8 bytes)
    #[arg(long)]
    data: Option<String>,

    /// How long to stay announced in seconds (default: indefinite)
    #[arg(long)]
    duration: Option<u64>,

    /// Accept incoming connections and run echo protocol
    #[arg(long)]
    ping: bool,
}

pub async fn run(args: AnnounceArgs, cfg: &ResolvedConfig) -> i32 {
    let topic = parse_topic(&args.topic);
    let topic_hex = to_hex(&topic);

    if let Some(ref data) = args.data {
        if data.len() > 1000 {
            eprintln!("error: --data exceeds 1000 byte limit ({} bytes)", data.len());
            return 1;
        }
    }

    if args.duration == Some(0) {
        eprintln!("error: --duration must be greater than 0");
        return 1;
    }

    let key_pair = match &args.seed {
        Some(seed) => {
            let hash = peeroxide::discovery_key(seed.as_bytes());
            KeyPair::from_seed(hash)
        }
        None => KeyPair::generate(),
    };

    let pk_hex = to_hex(&key_pair.public_key);
    let is_ephemeral = args.seed.is_none();

    let dht_config = build_dht_config(cfg);
    let mut swarm_config = SwarmConfig::default();
    swarm_config.key_pair = Some(key_pair.clone());
    swarm_config.dht = dht_config;

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

    if is_ephemeral {
        eprintln!("ANNOUNCE blake2b(\"{}\") as @{pk_hex} (ephemeral)", args.topic);
    } else {
        eprintln!("ANNOUNCE blake2b(\"{}\") as @{pk_hex}", args.topic);
    }
    eprintln!("  announced to closest nodes");

    if let Some(ref data) = args.data {
        let seq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        match handle.dht().mutable_put(&key_pair, data.as_bytes(), seq).await {
            Ok(_) => {
                eprintln!("  metadata: \"{data}\" ({} bytes, seq={seq})", data.len());
            }
            Err(e) => {
                eprintln!("  warning: mutable_put failed: {e}");
            }
        }
    }

    if args.ping {
        eprintln!("  listening for echo connections...");
    }

    let echo_sem = Arc::new(Semaphore::new(MAX_ECHO_SESSIONS));
    let active_sessions = Arc::new(AtomicUsize::new(0));

    let duration_deadline = args.duration.map(|d| tokio::time::Instant::now() + Duration::from_secs(d));

    let data_for_refresh = args.data.clone();
    let key_pair_for_refresh = key_pair.clone();
    let handle_for_refresh = handle.clone();

    let refresh_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(600));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Some(ref data) = data_for_refresh {
                let seq = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Err(e) = handle_for_refresh
                    .dht()
                    .mutable_put(&key_pair_for_refresh, data.as_bytes(), seq)
                    .await
                {
                    eprintln!("  warning: metadata refresh failed: {e}");
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                break;
            }
            _ = super::sigterm_recv() => {
                break;
            }
            _ = async {
                if let Some(deadline) = duration_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                break;
            }
            conn = conn_rx.recv() => {
                match conn {
                    Some(mut swarm_conn) => {
                        if !args.ping {
                            drop(swarm_conn);
                            continue;
                        }

                        let remote_pk = to_hex(swarm_conn.remote_public_key());
                        let permit = match echo_sem.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                drop(swarm_conn);
                                continue;
                            }
                        };

                        let active = active_sessions.clone();
                        active.fetch_add(1, Ordering::Relaxed);
                        eprintln!("  [connected] @{remote_pk} (echo mode)");

                        tokio::spawn(async move {
                            let mut probes_echoed: u64 = 0;
                            let stream = &mut swarm_conn.peer.stream;

                            let first_msg = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read()).await;
                            match first_msg {
                                Ok(Ok(Some(msg))) if msg == PING_MAGIC => {
                                    if stream.write(PONG_MAGIC).await.is_err() {
                                        eprintln!("  [disconnected] @{remote_pk} (write error)");
                                        active.fetch_sub(1, Ordering::Relaxed);
                                        drop(permit);
                                        return;
                                    }
                                }
                                _ => {
                                    eprintln!("  [disconnected] @{remote_pk} (bad handshake)");
                                    active.fetch_sub(1, Ordering::Relaxed);
                                    drop(permit);
                                    return;
                                }
                            }

                            loop {
                                let msg = tokio::time::timeout(IDLE_TIMEOUT, stream.read()).await;
                                match msg {
                                    Ok(Ok(Some(data))) => {
                                        if data.len() != ECHO_MSG_LEN {
                                            break;
                                        }
                                        if stream.write(&data).await.is_err() {
                                            break;
                                        }
                                        probes_echoed += 1;
                                    }
                                    _ => break,
                                }
                            }

                            eprintln!("  [disconnected] @{remote_pk} ({probes_echoed} probes echoed)");
                            active.fetch_sub(1, Ordering::Relaxed);
                            drop(permit);
                        });
                    }
                    None => break,
                }
            }
        }
    }

    if args.topic.len() == 64 && hex::decode(&args.topic).is_ok() {
        eprintln!("UNANNOUNCE {topic_hex}");
    } else {
        eprintln!("UNANNOUNCE blake2b(\"{}\")", args.topic);
    }

    refresh_task.abort();
    let _ = handle.leave(topic).await;
    let _ = handle.destroy().await;
    let _ = task.await;

    eprintln!("  done");
    0
}
