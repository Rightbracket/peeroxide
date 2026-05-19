pub mod announce;
pub mod chat;
pub mod cp;
pub mod deaddrop;
pub mod init;
pub mod lookup;
pub mod node;
pub mod ping;

use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;

use crate::config::ResolvedConfig;

pub fn parse_topic(input: &str) -> [u8; 32] {
    if input.len() == 64 {
        if let Ok(bytes) = hex::decode(input) {
            if bytes.len() == 32 {
                let mut topic = [0u8; 32];
                topic.copy_from_slice(&bytes);
                return topic;
            }
        }
    }
    peeroxide::discovery_key(input.as_bytes())
}

/// Resolve the bootstrap list using additive semantics:
///
/// 1. Start with bootstrap addresses from ResolvedConfig (CLI --bootstrap or config file).
/// 2. If `public` is Some(true), add DEFAULT_BOOTSTRAP.
/// 3. If the list is still empty, add DEFAULT_BOOTSTRAP (auto-public default).
/// 4. If `public` is Some(false) (--no-public or config public=false), remove
///    DEFAULT_BOOTSTRAP entries by value.
pub fn resolve_bootstrap(cfg: &ResolvedConfig) -> Vec<String> {
    let default_bootstrap: Vec<String> = peeroxide::DEFAULT_BOOTSTRAP
        .iter()
        .map(|s| (*s).to_string())
        .collect();

    let mut bootstrap = cfg.bootstrap.clone();

    if cfg.public == Some(true) {
        tracing::debug!("--public: adding default bootstrap nodes");
        for addr in &default_bootstrap {
            if !bootstrap.contains(addr) {
                bootstrap.push(addr.clone());
            }
        }
    }

    if bootstrap.is_empty() {
        tracing::debug!("no bootstrap configured, using public defaults (auto-public)");
        bootstrap = default_bootstrap.clone();
    }

    if cfg.public == Some(false) {
        tracing::debug!("--no-public: removing default bootstrap nodes");
        bootstrap.retain(|addr| !default_bootstrap.contains(addr));
    }

    tracing::info!(nodes = %bootstrap.join(", "), count = bootstrap.len(), "bootstrap resolved");

    bootstrap
}

pub fn build_dht_config(cfg: &ResolvedConfig) -> HyperDhtConfig {
    let bootstrap = resolve_bootstrap(cfg);

    let mut dht_cfg = DhtConfig::default();
    dht_cfg.bootstrap = bootstrap;
    let mut hyper_cfg = HyperDhtConfig::default();
    hyper_cfg.dht = dht_cfg;
    hyper_cfg
}

pub fn to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Parse the `PEEROXIDE_FORCE_RELAY` env var value as `<pubkey_hex>@<host:port>`
/// and return the resulting `(pubkey, addr)` pair.
///
/// Returns `Ok(None)` if the env var is unset or empty. Returns `Err(message)` if
/// the var is set but malformed — callers should surface this to the user and exit.
pub fn parse_force_relay_env() -> Result<Option<([u8; 32], std::net::SocketAddr)>, String> {
    let raw = match std::env::var("PEEROXIDE_FORCE_RELAY") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };

    let (pk_hex, addr_str) = raw.split_once('@').ok_or_else(|| {
        format!(
            "PEEROXIDE_FORCE_RELAY: expected <pubkey_hex>@<host:port>, got {raw:?}"
        )
    })?;

    if pk_hex.len() != 64 {
        return Err(format!(
            "PEEROXIDE_FORCE_RELAY: pubkey must be 64 hex chars (got {} chars)",
            pk_hex.len()
        ));
    }

    let pk_bytes = hex::decode(pk_hex).map_err(|e| {
        format!("PEEROXIDE_FORCE_RELAY: invalid hex in pubkey: {e}")
    })?;
    if pk_bytes.len() != 32 {
        return Err(format!(
            "PEEROXIDE_FORCE_RELAY: decoded pubkey is {} bytes, expected 32",
            pk_bytes.len()
        ));
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&pk_bytes);

    let addr: std::net::SocketAddr = addr_str
        .parse()
        .map_err(|e| format!("PEEROXIDE_FORCE_RELAY: invalid socket addr {addr_str:?}: {e}"))?;

    Ok(Some((pk, addr)))
}

/// Apply `PEEROXIDE_FORCE_RELAY` to a `SwarmConfig`. On malformed values, prints to
/// stderr and returns `false` — callers should treat this as fatal.
///
/// Protocol effect: only the side acting as a noise-handshake server (e.g. `cp send`,
/// which announces a topic and accepts connections) advertises `relay_through` to
/// its peer. Applying it on the client side (`cp recv`) is currently a no-op on the
/// wire, but the helper accepts it uniformly so the env var can be set symmetrically
/// on both processes for operator clarity.
pub fn apply_force_relay_env(swarm_config: &mut peeroxide::SwarmConfig) -> bool {
    match parse_force_relay_env() {
        Ok(None) => true,
        Ok(Some((pk, addr))) => {
            tracing::info!(
                pubkey = %hex::encode(&pk[..8]),
                addr = %addr,
                "PEEROXIDE_FORCE_RELAY: advertising relay_through",
            );
            swarm_config.relay_through = Some(pk);
            swarm_config.relay_address = Some(addr);
            true
        }
        Err(msg) => {
            eprintln!("error: {msg}");
            false
        }
    }
}

#[cfg(unix)]
pub async fn sigterm_recv() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut s = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
    s.recv().await;
}

#[cfg(not(unix))]
pub async fn sigterm_recv() {
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use peeroxide_dht::hyperdht::should_direct_connect;
    use peeroxide_dht::hyperdht_messages::{
        FIREWALL_CONSISTENT, FIREWALL_OPEN, FIREWALL_RANDOM, FIREWALL_UNKNOWN,
    };

    #[test]
    fn parse_topic_hex_64_chars() {
        let hex_str = "a".repeat(64);
        let topic = parse_topic(&hex_str);
        assert_eq!(topic, [0xaa; 32]);
    }

    #[test]
    fn parse_topic_short_string_hashes() {
        let topic = parse_topic("hello");
        let expected = peeroxide::discovery_key(b"hello");
        assert_eq!(topic, expected);
    }

    #[test]
    fn parse_topic_63_char_hex_hashes() {
        let input = "a".repeat(63);
        let topic = parse_topic(&input);
        let expected = peeroxide::discovery_key(input.as_bytes());
        assert_eq!(topic, expected);
    }

    #[test]
    fn parse_topic_invalid_hex_64_chars_hashes() {
        let input = "g".repeat(64);
        let topic = parse_topic(&input);
        let expected = peeroxide::discovery_key(input.as_bytes());
        assert_eq!(topic, expected);
    }

    #[test]
    fn to_hex_roundtrip() {
        let bytes = [0xde, 0xad, 0xbe, 0xef];
        assert_eq!(to_hex(&bytes), "deadbeef");
    }

    #[test]
    fn build_dht_config_uses_defaults_when_public_no_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(true),
            bootstrap: vec![],
            node: Default::default(),
        };
        let dht_cfg = build_dht_config(&cfg);
        assert!(!dht_cfg.dht.bootstrap.is_empty());
    }

    #[test]
    fn build_dht_config_uses_provided_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(true),
            bootstrap: vec!["1.2.3.4:49737".to_string()],
            node: Default::default(),
        };
        let dht_cfg = build_dht_config(&cfg);
        assert!(dht_cfg.dht.bootstrap.contains(&"1.2.3.4:49737".to_string()));
    }

    #[test]
    fn no_public_with_no_bootstrap_produces_empty() {
        let cfg = ResolvedConfig {
            public: Some(false),
            bootstrap: vec![],
            node: Default::default(),
        };
        let dht_cfg = build_dht_config(&cfg);
        assert!(
            dht_cfg.dht.bootstrap.is_empty(),
            "--no-public with no custom bootstrap should produce empty list"
        );
    }

    // ── Additive bootstrap resolution scenarios ────────────────────────────

    #[test]
    fn bare_command_no_flags_auto_public() {
        let cfg = ResolvedConfig {
            public: None,
            bootstrap: vec![],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        let default: Vec<String> = peeroxide::DEFAULT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        assert_eq!(bootstrap, default, "bare command with no config should auto-public");
    }

    #[test]
    fn explicit_public_adds_defaults() {
        let cfg = ResolvedConfig {
            public: Some(true),
            bootstrap: vec![],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        let default: Vec<String> = peeroxide::DEFAULT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        assert_eq!(bootstrap, default);
    }

    #[test]
    fn no_public_removes_defaults() {
        let cfg = ResolvedConfig {
            public: Some(false),
            bootstrap: vec![],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert!(bootstrap.is_empty(), "--no-public with no custom bootstrap → empty");
    }

    #[test]
    fn custom_bootstrap_only() {
        let cfg = ResolvedConfig {
            public: None,
            bootstrap: vec!["x:1234".to_string()],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert_eq!(bootstrap, vec!["x:1234"]);
    }

    #[test]
    fn public_with_custom_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(true),
            bootstrap: vec!["x:1234".to_string()],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert!(bootstrap.contains(&"x:1234".to_string()));
        let default: Vec<String> = peeroxide::DEFAULT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        for addr in &default {
            assert!(bootstrap.contains(addr), "public should add default bootstrap");
        }
    }

    #[test]
    fn no_public_with_custom_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(false),
            bootstrap: vec!["x:1234".to_string()],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert_eq!(bootstrap, vec!["x:1234"], "--no-public keeps custom, removes defaults");
    }

    #[test]
    fn config_public_true_with_custom_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(true),
            bootstrap: vec!["y:5678".to_string()],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert!(bootstrap.contains(&"y:5678".to_string()));
        let default: Vec<String> = peeroxide::DEFAULT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        for addr in &default {
            assert!(bootstrap.contains(addr));
        }
    }

    #[test]
    fn config_public_false_with_custom_bootstrap() {
        let cfg = ResolvedConfig {
            public: Some(false),
            bootstrap: vec!["y:5678".to_string()],
            node: Default::default(),
        };
        let bootstrap = resolve_bootstrap(&cfg);
        assert_eq!(bootstrap, vec!["y:5678"]);
    }

    // ── 3×6 Scenario Matrix: Bootstrap Type × Network Topology ────────────
    //
    // This test enumerates every combination of:
    //   Bootstrap types (B1-B3):
    //     B1: Public default (empty bootstrap + public=Some(true) → DEFAULT_BOOTSTRAP)
    //     B2: Explicit/custom (user-provided bootstrap addresses + public=Some(true))
    //     B3: Isolated (empty bootstrap + public=Some(false) → empty)
    //
    //   Network topologies (T1-T6):
    //     T1: Both open
    //     T2: Sender firewalled, receiver open
    //     T3: Sender open, receiver firewalled
    //     T4: Both firewalled, same host (same_host bypass)
    //     T5: Both firewalled, different networks (holepunch — same decision as T3)
    //     T6: One behind CGNAT (FIREWALL_RANDOM — distinct firewall type)
    //
    // For each cell we assert:
    //   1. Bootstrap config output (bootstrap list presence)
    //   2. Connection-path decision (should_direct_connect result)
    //   3. Combined expected behavior (discovery feasible + connection path)

    fn b1_config() -> ResolvedConfig {
        ResolvedConfig {
            public: Some(true),
            bootstrap: vec![],
            node: Default::default(),
        }
    }

    fn b2_config() -> ResolvedConfig {
        ResolvedConfig {
            public: Some(true),
            bootstrap: vec!["10.0.0.1:49737".to_string()],
            node: Default::default(),
        }
    }

    fn b3_config() -> ResolvedConfig {
        ResolvedConfig {
            public: Some(false),
            bootstrap: vec![],
            node: Default::default(),
        }
    }

    struct TopologyParams {
        relayed: bool,
        firewall: u64,
        holepunchable: bool,
        same_host: bool,
    }

    struct MatrixExpectation {
        has_bootstrap: bool,
        direct_connect: bool,
        behavior: &'static str,
    }

    #[test]
    fn scenario_matrix_3x6_cross_product() {
        let topologies: [(&str, TopologyParams); 6] = [
            (
                "T1: both open",
                TopologyParams {
                    relayed: false,
                    firewall: FIREWALL_OPEN,
                    holepunchable: true,
                    same_host: false,
                },
            ),
            (
                "T2: sender firewalled, receiver open",
                TopologyParams {
                    relayed: true,
                    firewall: FIREWALL_OPEN,
                    holepunchable: true,
                    same_host: false,
                },
            ),
            (
                "T3: sender open, receiver firewalled",
                TopologyParams {
                    relayed: true,
                    firewall: FIREWALL_CONSISTENT,
                    holepunchable: true,
                    same_host: false,
                },
            ),
            (
                "T4: both firewalled, same host",
                TopologyParams {
                    relayed: true,
                    firewall: FIREWALL_CONSISTENT,
                    holepunchable: true,
                    same_host: true,
                },
            ),
            (
                "T5: both firewalled, different networks",
                TopologyParams {
                    relayed: true,
                    firewall: FIREWALL_CONSISTENT,
                    holepunchable: true,
                    same_host: false,
                },
            ),
            (
                "T6: one behind CGNAT (symmetric/random NAT)",
                TopologyParams {
                    relayed: true,
                    firewall: FIREWALL_RANDOM,
                    holepunchable: true,
                    same_host: false,
                },
            ),
        ];

        type MatrixRow = (&'static str, fn() -> ResolvedConfig, [MatrixExpectation; 6]);
        let matrix: [MatrixRow; 3] = [
            (
                "B1: public default",
                b1_config as fn() -> ResolvedConfig,
                [
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect via public DHT",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect (receiver is open)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (receiver firewalled, holepunchable)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect (same host bypass)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (both firewalled, receiver holepunchable)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (CGNAT/symmetric NAT, FIREWALL_RANDOM)",
                    },
                ],
            ),
            (
                "B2: explicit/custom",
                b2_config as fn() -> ResolvedConfig,
                [
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect via custom bootstrap",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect (receiver is open)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (receiver firewalled, holepunchable)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: true,
                        behavior: "direct connect (same host bypass)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (both firewalled, receiver holepunchable)",
                    },
                    MatrixExpectation {
                        has_bootstrap: true,
                        direct_connect: false,
                        behavior: "holepunch (CGNAT/symmetric NAT, FIREWALL_RANDOM)",
                    },
                ],
            ),
            (
                "B3: isolated",
                b3_config as fn() -> ResolvedConfig,
                [
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: true,
                        behavior: "no discovery (isolated); direct if manually connected",
                    },
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: true,
                        behavior: "no discovery (isolated); direct if manually connected",
                    },
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: false,
                        behavior: "no discovery (isolated); would holepunch if connected",
                    },
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: true,
                        behavior: "no discovery (isolated); same host bypass",
                    },
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: false,
                        behavior: "no discovery (isolated); would holepunch if connected",
                    },
                    MatrixExpectation {
                        has_bootstrap: false,
                        direct_connect: false,
                        behavior: "no discovery (isolated); would holepunch if connected",
                    },
                ],
            ),
        ];

        for (b_name, config_fn, expectations) in &matrix {
            let cfg = config_fn();
            let dht_cfg = build_dht_config(&cfg);

            for (i, (t_name, topo)) in topologies.iter().enumerate() {
                let exp = &expectations[i];

                assert_eq!(
                    !dht_cfg.dht.bootstrap.is_empty(),
                    exp.has_bootstrap,
                    "[{b_name} × {t_name}] bootstrap presence mismatch: expected has_bootstrap={}, got bootstrap={:?}",
                    exp.has_bootstrap,
                    dht_cfg.dht.bootstrap,
                );

                let direct = should_direct_connect(
                    topo.relayed,
                    topo.firewall,
                    topo.holepunchable,
                    topo.same_host,
                );
                assert_eq!(
                    direct, exp.direct_connect,
                    "[{b_name} × {t_name}] connection path mismatch: expected direct_connect={} ({}), got {}",
                    exp.direct_connect, exp.behavior, direct,
                );
            }
        }
    }

    #[test]
    fn isolated_mode_no_discovery_semantics() {
        let cfg = b3_config();
        let dht_cfg = build_dht_config(&cfg);

        assert!(
            dht_cfg.dht.bootstrap.is_empty(),
            "isolated mode must have empty bootstrap"
        );
    }

    #[test]
    fn cgnat_represented_as_firewall_random() {
        assert_eq!(FIREWALL_RANDOM, 3);
        assert_eq!(FIREWALL_UNKNOWN, 0);
        assert_eq!(FIREWALL_OPEN, 1);
        assert_eq!(FIREWALL_CONSISTENT, 2);

        assert!(!should_direct_connect(true, FIREWALL_RANDOM, true, false));
    }

    fn with_force_relay_env<F: FnOnce()>(value: Option<&str>, f: F) {
        use std::sync::Mutex;
        static LOCK: Mutex<()> = Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("PEEROXIDE_FORCE_RELAY").ok();
        unsafe {
            match value {
                Some(v) => std::env::set_var("PEEROXIDE_FORCE_RELAY", v),
                None => std::env::remove_var("PEEROXIDE_FORCE_RELAY"),
            }
        }
        f();
        unsafe {
            match prev {
                Some(v) => std::env::set_var("PEEROXIDE_FORCE_RELAY", v),
                None => std::env::remove_var("PEEROXIDE_FORCE_RELAY"),
            }
        }
    }

    #[test]
    fn force_relay_env_unset_returns_none() {
        with_force_relay_env(None, || {
            assert!(matches!(parse_force_relay_env(), Ok(None)));
        });
    }

    #[test]
    fn force_relay_env_empty_returns_none() {
        with_force_relay_env(Some(""), || {
            assert!(matches!(parse_force_relay_env(), Ok(None)));
        });
    }

    #[test]
    fn force_relay_env_packed_parses() {
        let pk_hex = "0123456789abcdef".repeat(4);
        let value = format!("{pk_hex}@1.2.3.4:49737");
        with_force_relay_env(Some(&value), || {
            let (pk, addr) = parse_force_relay_env()
                .expect("ok")
                .expect("some");
            assert_eq!(hex::encode(pk), pk_hex);
            assert_eq!(addr.to_string(), "1.2.3.4:49737");
        });
    }

    #[test]
    fn force_relay_env_placeholder_8888_parses() {
        let pk_hex = "0".repeat(64);
        let value = format!("{pk_hex}@8.8.8.8:49737");
        with_force_relay_env(Some(&value), || {
            let (pk, addr) = parse_force_relay_env()
                .expect("ok")
                .expect("some");
            assert_eq!(pk, [0u8; 32]);
            assert_eq!(addr.to_string(), "8.8.8.8:49737");
        });
    }

    #[test]
    fn force_relay_env_missing_at_errors() {
        with_force_relay_env(Some("abc"), || {
            assert!(parse_force_relay_env().is_err());
        });
    }

    #[test]
    fn force_relay_env_short_pubkey_errors() {
        with_force_relay_env(Some("dead@1.2.3.4:1234"), || {
            assert!(parse_force_relay_env().is_err());
        });
    }

    #[test]
    fn force_relay_env_non_hex_pubkey_errors() {
        let value = format!("{}@1.2.3.4:1234", "z".repeat(64));
        with_force_relay_env(Some(&value), || {
            assert!(parse_force_relay_env().is_err());
        });
    }

    #[test]
    fn force_relay_env_bad_addr_errors() {
        let value = format!("{}@not-an-addr", "0".repeat(64));
        with_force_relay_env(Some(&value), || {
            assert!(parse_force_relay_env().is_err());
        });
    }
}
