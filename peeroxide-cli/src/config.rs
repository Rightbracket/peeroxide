use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct GlobalFlags {
    pub config_path: Option<String>,
    pub no_default_config: bool,
    pub public: Option<bool>,
    pub bootstrap: Option<Vec<String>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub announce: AnnounceConfig,
    #[serde(default)]
    pub cp: CpConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NetworkConfig {
    pub public: Option<bool>,
    pub bootstrap: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NodeConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
    pub public_host: Option<String>,
    pub stats_interval: Option<u64>,
    pub max_records: Option<usize>,
    pub max_lru_size: Option<usize>,
    pub max_per_key: Option<usize>,
    pub max_record_age: Option<u64>,
    pub max_lru_age: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AnnounceConfig {}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CpConfig {}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub public: Option<bool>,
    pub bootstrap: Vec<String>,
    pub node: NodeConfig,
}

pub fn load_config(flags: &GlobalFlags) -> Result<ResolvedConfig, String> {
    let file_config = if let Some(ref path) = flags.config_path {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read config file {path}: {e}"))?;
        tracing::info!(path, "config loaded");
        Some(
            toml::from_str::<ConfigFile>(&contents)
                .map_err(|e| format!("invalid config file {path}: {e}"))?,
        )
    } else if let Some(path) = env_config_path() {
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read config file {}: {e}", path.display()))?;
        tracing::info!(path = %path.display(), "config loaded via $PEEROXIDE_CONFIG");
        Some(
            toml::from_str::<ConfigFile>(&contents)
                .map_err(|e| format!("invalid config file {}: {e}", path.display()))?,
        )
    } else if !flags.no_default_config {
        match default_config_path() {
            Some(p) => {
                let contents = std::fs::read_to_string(&p).ok();
                if let Some(ref contents) = contents {
                    tracing::info!(path = %p.display(), "config loaded");
                    Some(
                        toml::from_str::<ConfigFile>(contents)
                            .map_err(|e| format!("invalid config file {}: {e}", p.display()))?,
                    )
                } else {
                    tracing::debug!("no config file found at default location");
                    None
                }
            }
            None => {
                tracing::debug!("no default config path available");
                None
            }
        }
    } else {
        tracing::debug!("config file loading skipped (--no-default-config)");
        None
    };

    let file_config = file_config.unwrap_or_default();

    let public = flags.public.or(file_config.network.public);

    let bootstrap = flags
        .bootstrap
        .clone()
        .or(file_config.network.bootstrap)
        .unwrap_or_default();

    Ok(ResolvedConfig {
        public,
        bootstrap,
        node: file_config.node,
    })
}

fn env_config_path() -> Option<PathBuf> {
    std::env::var("PEEROXIDE_CONFIG").ok().map(PathBuf::from)
}

/// Returns a footer string for help output showing the active or expected config path.
pub fn config_path_footer() -> String {
    if let Some(env_path) = env_config_path() {
        return if env_path.exists() {
            format!("Config: {} (via $PEEROXIDE_CONFIG)", env_path.display())
        } else {
            format!(
                "Config: {} (via $PEEROXIDE_CONFIG, not found)",
                env_path.display()
            )
        };
    }

    if let Some(path) = default_config_path() {
        return format!("Config: {}", path.display());
    }

    match expected_default_path() {
        Some(path) => format!(
            "Config: {} (not found; create with 'peeroxide init')",
            path.display()
        ),
        None => "Config: not found (create with 'peeroxide init')".to_string(),
    }
}

/// Returns the default config path without checking if the file exists.
fn expected_default_path() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(xdg).join("peeroxide").join("config.toml"));
    }
    if let Some(config_dir) = dirs::config_dir() {
        return Some(config_dir.join("peeroxide").join("config.toml"));
    }
    if let Some(home) = dirs::home_dir() {
        return Some(home.join(".config").join("peeroxide").join("config.toml"));
    }
    None
}

fn default_config_path() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        let p = PathBuf::from(xdg).join("peeroxide").join("config.toml");
        if p.exists() {
            return Some(p);
        }
    }

    if let Some(config_dir) = dirs::config_dir() {
        let p = config_dir.join("peeroxide").join("config.toml");
        if p.exists() {
            return Some(p);
        }
    }

    if let Some(home) = dirs::home_dir() {
        let p = home.join(".config").join("peeroxide").join("config.toml");
        if p.exists() {
            return Some(p);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_parses() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.network.public.is_none());
        assert!(cfg.network.bootstrap.is_none());
    }

    #[test]
    fn full_config_parses() {
        let toml_str = r#"
[network]
public = true
bootstrap = ["10.0.1.5:49737", "10.0.1.6:49737"]

[node]
port = 49737
host = "0.0.0.0"
public_host = "203.0.113.7"
stats_interval = 60
max_records = 65536
max_lru_size = 65536
max_per_key = 20
max_record_age = 1200
max_lru_age = 1200
"#;
        let cfg: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.network.public, Some(true));
        assert_eq!(
            cfg.network.bootstrap,
            Some(vec!["10.0.1.5:49737".to_string(), "10.0.1.6:49737".to_string()])
        );
        assert_eq!(cfg.node.port, Some(49737));
        assert_eq!(cfg.node.host, Some("0.0.0.0".to_string()));
        assert_eq!(cfg.node.public_host, Some("203.0.113.7".to_string()));
        assert_eq!(cfg.node.stats_interval, Some(60));
        assert_eq!(cfg.node.max_records, Some(65536));
    }

    #[test]
    fn cli_flags_override_config() {
        let flags = GlobalFlags {
            config_path: None,
            no_default_config: true,
            public: Some(true),
            bootstrap: Some(vec!["1.2.3.4:49737".to_string()]),
        };
        let cfg = load_config(&flags).unwrap();
        assert_eq!(cfg.public, Some(true));
        assert_eq!(cfg.bootstrap, vec!["1.2.3.4:49737"]);
    }

    #[test]
    fn no_default_config_produces_empty() {
        let flags = GlobalFlags {
            config_path: None,
            no_default_config: true,
            public: None,
            bootstrap: None,
        };
        let cfg = load_config(&flags).unwrap();
        assert_eq!(cfg.public, None);
        assert!(cfg.bootstrap.is_empty());
    }
}
