use clap::Args;
use libudx::UdxRuntime;
use peeroxide_dht::hyperdht::{self, HyperDhtConfig};
use peeroxide_dht::persistent::PersistentConfig;
use peeroxide_dht::rpc::DhtConfig;
use tokio::signal;
use std::time::Duration;

use crate::config::ResolvedConfig;
use super::resolve_bootstrap;

#[derive(Args)]
pub struct NodeArgs {
    /// Bind port (default: 49737)
    #[arg(long)]
    port: Option<u16>,

    /// Bind address (default: 0.0.0.0)
    #[arg(long)]
    host: Option<String>,

    /// How often to log routing table size in seconds (default: 60)
    #[arg(long)]
    stats_interval: Option<u64>,

    /// Max announcement records stored
    #[arg(long)]
    max_records: Option<usize>,

    /// Max entries per LRU cache
    #[arg(long)]
    max_lru_size: Option<usize>,

    /// Max peer announcements per topic
    #[arg(long)]
    max_per_key: Option<usize>,

    /// TTL for announcement records in seconds
    #[arg(long)]
    max_record_age: Option<u64>,

    /// TTL for LRU cache entries in seconds
    #[arg(long)]
    max_lru_age: Option<u64>,
}

pub async fn run(args: NodeArgs, cfg: &ResolvedConfig) -> i32 {
    let port = args.port.or(cfg.node.port).unwrap_or(49737);
    let host = args.host.or_else(|| cfg.node.host.clone()).unwrap_or_else(|| "0.0.0.0".to_string());
    let stats_interval = args.stats_interval.or(cfg.node.stats_interval).unwrap_or(60);

    if stats_interval == 0 {
        eprintln!("error: --stats-interval must be greater than 0");
        return 1;
    }

    let mut persistent = PersistentConfig::default();
    if let Some(v) = args.max_records.or(cfg.node.max_records) {
        persistent.max_records = v;
    }
    if let Some(v) = args.max_lru_size.or(cfg.node.max_lru_size) {
        persistent.max_lru_size = v;
    }
    if let Some(v) = args.max_per_key.or(cfg.node.max_per_key) {
        persistent.max_per_key = v;
    }
    if let Some(v) = args.max_record_age.or(cfg.node.max_record_age) {
        persistent.max_record_age = Duration::from_secs(v);
    }
    if let Some(v) = args.max_lru_age.or(cfg.node.max_lru_age) {
        persistent.max_lru_age = Duration::from_secs(v);
    }

    let bootstrap = resolve_bootstrap(cfg);

    let is_networked = cfg.public == Some(true) || !bootstrap.is_empty();

    let mut dht_cfg = DhtConfig::default();
    dht_cfg.bootstrap = bootstrap;
    dht_cfg.port = port;
    dht_cfg.host = host.clone();
    dht_cfg.ephemeral = Some(false);
    dht_cfg.firewalled = false;
    let mut dht_config = HyperDhtConfig::default();
    dht_config.dht = dht_cfg;
    dht_config.persistent = persistent;

    let runtime = match UdxRuntime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: failed to create UDP runtime: {e}");
            return 1;
        }
    };

    let (task, handle, _server_rx) = match hyperdht::spawn(&runtime, dht_config).await {
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

    println!("{host}:{listen_port}");

    if let Err(e) = handle.bootstrapped().await {
        eprintln!("error: bootstrap failed: {e}");
        return 1;
    }

    let table_size = handle.table_size().await.unwrap_or(0);

    if is_networked {
        tracing::info!("Bootstrap complete — routing table: {table_size} peers");
    } else {
        tracing::info!("Node ready (isolated mode) — listening for incoming peers");
    }

    let mut stats_timer = tokio::time::interval(Duration::from_secs(stats_interval));
    stats_timer.tick().await; // skip first immediate tick

    let mut ticks_since_bootstrap: u64 = 0;

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
                ticks_since_bootstrap += 1;
                let size = handle.table_size().await.unwrap_or(0);
                let pstats = handle.persistent_stats().await.unwrap_or_default();
                tracing::debug!(
                    "Routing table: {size} peers | Records: {} ({} topics) | Mutables: {} | Immutables: {} | Router: {}",
                    pstats.records, pstats.record_topics, pstats.mutables, pstats.immutables, pstats.router_entries
                );

                if is_networked && size == 0 {
                    if ticks_since_bootstrap == 1 {
                        let elapsed = stats_interval;
                        tracing::warn!(
                            "Routing table empty {elapsed}s after bootstrap — \
                             this node may be unreachable. Check that UDP port {listen_port} \
                             is open and not firewalled."
                        );
                    } else if ticks_since_bootstrap == 2 {
                        let elapsed_min = (stats_interval * 2) / 60;
                        tracing::warn!(
                            "Routing table still empty after {elapsed_min}m — \
                             node is likely unreachable from the network. \
                             Verify UDP port {listen_port} is reachable from external hosts."
                        );
                    }
                }
            }
        }
    }

    let _ = handle.destroy().await;
    let _ = task.await;

    0
}
