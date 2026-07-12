use clap::Args;
use libudx::UdxRuntime;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig, KeyPair};
use peeroxide_dht::relay_service::{self, RelayServiceConfig};
use peeroxide_dht::blind_relay::BlindRelayServerConfig;
use peeroxide_dht::rpc::DhtConfig;
use std::time::Duration;
use tokio::signal;

use crate::config::ResolvedConfig;
use super::{resolve_bootstrap, to_hex};

#[derive(Args)]
pub struct RelayArgs {
    /// Bind port (default: 49737)
    #[arg(long)]
    port: Option<u16>,

    /// Bind address (default: 0.0.0.0)
    #[arg(long)]
    host: Option<String>,

    /// Hex-encoded 32-byte seed for a deterministic identity key pair
    /// (default: a fresh random key pair each run)
    #[arg(long)]
    key_seed: Option<String>,

    /// How often to log relay/routing stats in seconds (default: 60)
    #[arg(long)]
    stats_interval: Option<u64>,

    /// Maximum concurrently accepted relay sessions (default: 10000)
    #[arg(long)]
    max_sessions: Option<usize>,

    /// Maximum concurrent pending+active pairings per session (default: 256)
    #[arg(long)]
    max_pairings_per_session: Option<usize>,

    /// Drop an unmatched pairing after this many seconds (default: 300)
    #[arg(long)]
    pairing_timeout: Option<u64>,

    /// Close a session with no pair/unpair activity for this many seconds
    /// (default: 600)
    #[arg(long)]
    idle_session_timeout: Option<u64>,
}

pub async fn run(args: RelayArgs, cfg: &ResolvedConfig) -> i32 {
    let port = args.port.or(cfg.node.port).unwrap_or(49737);
    let host = args
        .host
        .or_else(|| cfg.node.host.clone())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let stats_interval = args.stats_interval.unwrap_or(60);

    if stats_interval == 0 {
        eprintln!("error: --stats-interval must be greater than 0");
        return 1;
    }

    let key_pair = match parse_key_seed(args.key_seed.as_deref()) {
        Ok(kp) => kp,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let mut relay_config = BlindRelayServerConfig::default();
    if let Some(v) = args.max_sessions {
        relay_config.max_sessions = v;
    }
    if let Some(v) = args.max_pairings_per_session {
        relay_config.max_pairings_per_session = v;
    }
    if let Some(v) = args.pairing_timeout {
        relay_config.pairing_timeout = Duration::from_secs(v);
    }
    if let Some(v) = args.idle_session_timeout {
        relay_config.idle_session_timeout = Duration::from_secs(v);
    }

    let bootstrap = resolve_bootstrap(cfg);

    let mut dht_cfg = DhtConfig::default();
    dht_cfg.bootstrap = bootstrap;
    dht_cfg.port = port;
    dht_cfg.host = host.clone();
    dht_cfg.ephemeral = Some(false);
    dht_cfg.firewalled = false;
    let mut dht_config = HyperDhtConfig::default();
    dht_config.dht = dht_cfg;

    let runtime = match UdxRuntime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: failed to create UDP runtime: {e}");
            return 1;
        }
    };

    let (task, handle, server_rx) = match hyperdht::spawn(&runtime, dht_config).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: failed to start DHT node: {e}");
            return 1;
        }
    };

    let listen_port = match handle.local_port().await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: failed to get local port: {e}");
            return 1;
        }
    };

    println!("relay public key: {}", to_hex(&key_pair.public_key));
    println!("{host}:{listen_port}");

    if let Err(e) = handle.bootstrapped().await {
        eprintln!("error: bootstrap failed: {e}");
        return 1;
    }

    tracing::info!(
        pubkey = %to_hex(&key_pair.public_key),
        "Blind-relay server ready"
    );

    let mut relay_service_config = RelayServiceConfig::default();
    relay_service_config.relay = relay_config;

    let (relay, _relay_task) = relay_service::run_relay_server(
        runtime.handle(),
        handle.clone(),
        key_pair,
        server_rx,
        relay_service_config,
    );

    let mut stats_timer = tokio::time::interval(Duration::from_secs(stats_interval));
    stats_timer.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                tracing::info!("Shutdown signal received");
                break;
            }
            _ = super::sigterm_recv() => {
                tracing::info!("SIGTERM received");
                break;
            }
            _ = stats_timer.tick() => {
                let table_size = handle.table_size().await.unwrap_or(0);
                let stats = relay.stats();
                tracing::info!(
                    "Routing table: {table_size} peers | sessions: {}/active {} | \
                     pairings: {} requested, {} matched, {} pending, {} active, {} cancelled | \
                     streams: {} opened, {} closed, {} errors",
                    stats.sessions_accepted,
                    stats.sessions_active,
                    stats.pairings_requested,
                    stats.pairings_matched,
                    stats.pairings_pending,
                    stats.pairings_active,
                    stats.pairings_cancelled,
                    stats.streams_opened,
                    stats.streams_closed,
                    stats.streams_errors,
                );
            }
        }
    }

    let _ = handle.destroy().await;
    let _ = task.await;

    0
}

fn parse_key_seed(seed_hex: Option<&str>) -> Result<KeyPair, String> {
    match seed_hex {
        None => Ok(KeyPair::generate()),
        Some(hex_str) => {
            let bytes = hex::decode(hex_str).map_err(|e| format!("invalid --key-seed hex: {e}"))?;
            let seed: [u8; 32] = bytes
                .try_into()
                .map_err(|_| "--key-seed must be exactly 32 bytes (64 hex chars)".to_string())?;
            Ok(KeyPair::from_seed(seed))
        }
    }
}
