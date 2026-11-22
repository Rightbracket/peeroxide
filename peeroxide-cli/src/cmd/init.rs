use std::path::{Path, PathBuf};

use clap::Args;

use crate::manpage;

/// Context from global CLI flags needed by the init command.
pub struct InitContext {
    /// Global --config path override
    pub config_path: Option<String>,
}

#[derive(Args)]
pub struct InitArgs {
    /// Overwrite existing config file
    #[arg(long, conflicts_with = "update")]
    force: bool,

    /// Update specific fields in existing config without overwriting other settings
    #[arg(long, conflicts_with = "force")]
    update: bool,

    /// Set network.public = true in the generated config (adds default public HyperDHT bootstrap nodes at runtime)
    #[arg(long)]
    public: bool,

    /// Bootstrap node addresses to set in config (repeatable)
    #[arg(long, action = clap::ArgAction::Append)]
    bootstrap: Vec<String>,

    /// Generate and install man pages instead of config.
    /// If PATH is omitted, defaults to /usr/local/share/man/.
    #[arg(long, value_name = "PATH", num_args = 0..=1, default_missing_value = "/usr/local/share/man/", conflicts_with_all = ["force", "update", "public", "bootstrap"])]
    man_pages: Option<PathBuf>,
}

pub fn run(args: InitArgs, ctx: InitContext) -> i32 {
    if let Some(man_path) = args.man_pages {
        return run_man_pages(&man_path);
    }
    run_config(args, ctx)
}

// ── Mode 2: Man pages ────────────────────────────────────────────────────────

fn run_man_pages(base_path: &Path) -> i32 {
    let man1_dir = base_path.join("man1");
    if let Err(e) = std::fs::create_dir_all(&man1_dir) {
        eprintln!(
            "error: cannot create directory {}: {e}\n\n\
             Try: sudo peeroxide init --man-pages {}\n\
             Or specify a writable path: peeroxide init --man-pages ~/.local/share/man/",
            man1_dir.display(),
            base_path.display()
        );
        return 1;
    }

    let pages = manpage::generate_all();
    let mut generated_filenames: std::collections::HashSet<std::ffi::OsString> =
        std::collections::HashSet::new();

    for (name, content) in &pages {
        let filename = format!("{name}.1");
        generated_filenames.insert(std::ffi::OsString::from(&filename));
        let path = man1_dir.join(&filename);
        if let Err(e) = std::fs::write(&path, content) {
            eprintln!(
                "error: failed to write {}: {e}\n\n\
                 Try: sudo peeroxide init --man-pages {}\n\
                 Or specify a writable path: peeroxide init --man-pages ~/.local/share/man/",
                path.display(),
                base_path.display()
            );
            return 1;
        }
        eprintln!("{}", path.display());
    }

    // Clean up stale peeroxide-*.1 pages from previous installations (e.g. renamed commands).
    if let Ok(entries) = std::fs::read_dir(&man1_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("peeroxide")
                && name_str.ends_with(".1")
                && !generated_filenames.contains(&name)
                && std::fs::remove_file(entry.path()).is_ok()
            {
                eprintln!("removed stale: {}", entry.path().display());
            }
        }
    }

    eprintln!(
        "Generated {} man page(s) in {}",
        pages.len(),
        man1_dir.display()
    );
    0
}

// ── Mode 1: Config initialization ───────────────────────────────────────────

fn run_config(args: InitArgs, ctx: InitContext) -> i32 {
    let config_path = resolve_config_path(&ctx.config_path);

    if config_path.is_dir() {
        eprintln!(
            "error: {} is a directory, not a file\n\
             Specify a file path: --config {}/config.toml",
            config_path.display(),
            config_path.display()
        );
        return 1;
    }

    if args.update {
        return run_update(&config_path, &args);
    }

    // Fresh creation mode
    if config_path.exists() && !args.force {
        println!("config already exists at {}", config_path.display());
        return 0;
    }

    // Ensure parent directory exists
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                eprintln!("error: cannot create directory: {e}");
                return 1;
            }
        }
    }

    let content = generate_config_content(args.public, &args.bootstrap);
    if let Err(e) = std::fs::write(&config_path, &content) {
        eprintln!("error: failed to write config: {e}");
        return 1;
    }
    eprintln!("Config written to {}", config_path.display());
    0
}

fn run_update(config_path: &Path, args: &InitArgs) -> i32 {
    if !config_path.exists() {
        eprintln!(
            "error: no config to update at {}\n\
             Run `peeroxide init` first to create a config file.",
            config_path.display()
        );
        return 1;
    }

    let has_public = args.public;
    let has_bootstrap = !args.bootstrap.is_empty();

    if !has_public && !has_bootstrap {
        eprintln!("error: nothing to update; specify --public or --bootstrap");
        return 1;
    }

    let content = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: cannot read config file {}: {e}", config_path.display());
            return 1;
        }
    };

    let mut doc = match content.parse::<toml_edit::DocumentMut>() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: invalid TOML in {}: {e}", config_path.display());
            return 1;
        }
    };

    if let Some(item) = doc.get("network") {
        if !item.is_table() && !item.is_inline_table() && !item.is_none() {
            eprintln!(
                "error: 'network' in {} is not a table; cannot update fields",
                config_path.display()
            );
            return 1;
        }
    }

    // Ensure [network] exists as a standard table (not inline) before inserting keys
    if doc.get("network").is_none() {
        doc["network"] = toml_edit::Item::Table(toml_edit::Table::new());
    }

    if has_public {
        let old_decor = doc
            .get("network")
            .and_then(|n| n.get("public"))
            .and_then(|item| item.as_value())
            .map(|v| (v.decor().prefix().cloned(), v.decor().suffix().cloned()));

        doc["network"]["public"] = toml_edit::value(true);

        if let Some((prefix, suffix)) = old_decor {
            if let Some(val) = doc["network"]["public"].as_value_mut() {
                if let Some(p) = prefix {
                    val.decor_mut().set_prefix(p);
                }
                if let Some(s) = suffix {
                    val.decor_mut().set_suffix(s);
                }
            }
        }
    }

    if has_bootstrap {
        let old_decor = doc
            .get("network")
            .and_then(|n| n.get("bootstrap"))
            .and_then(|item| item.as_value())
            .map(|v| (v.decor().prefix().cloned(), v.decor().suffix().cloned()));

        let arr: toml_edit::Array = args.bootstrap.iter().collect();
        doc["network"]["bootstrap"] = toml_edit::value(arr);

        if let Some((prefix, suffix)) = old_decor {
            if let Some(val) = doc["network"]["bootstrap"].as_value_mut() {
                if let Some(p) = prefix {
                    val.decor_mut().set_prefix(p);
                }
                if let Some(s) = suffix {
                    val.decor_mut().set_suffix(s);
                }
            }
        }
    }

    if let Err(e) = std::fs::write(config_path, doc.to_string()) {
        eprintln!("error: failed to write config: {e}");
        return 1;
    }
    eprintln!("Config updated at {}", config_path.display());
    0
}

fn resolve_config_path(cli_config: &Option<String>) -> PathBuf {
    if let Some(path) = cli_config {
        return PathBuf::from(path);
    }

    if let Ok(env_path) = std::env::var("PEEROXIDE_CONFIG") {
        return PathBuf::from(env_path);
    }

    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(xdg).join("peeroxide").join("config.toml");
    }

    if let Some(home) = dirs::home_dir() {
        return home.join(".config").join("peeroxide").join("config.toml");
    }

    PathBuf::from(".config/peeroxide/config.toml")
}

fn generate_config_content(public: bool, bootstrap: &[String]) -> String {
    let mut content = String::from(
        "# Peeroxide configuration file\n\
         # Place at ~/.config/peeroxide/config.toml or set PEEROXIDE_CONFIG env var\n\
         \n\
         [network]\n\
         # public = true tells runtime subcommands to add the default public HyperDHT bootstrap nodes.\n\
         # When public is unset, runtime subcommands auto-fill the default public bootstrap nodes anyway\n\
         # if the resolved bootstrap list would otherwise be empty.\n",
    );

    if public {
        content.push_str("public = true\n");
    } else {
        content.push_str("# public = false\n");
    }

    content.push_str("\n# Bootstrap node addresses (host:port). CLI --bootstrap overrides this list at runtime.\n# An empty list auto-fills with the default public bootstrap nodes unless --no-public is set.\n");

    if bootstrap.is_empty() {
        content.push_str("# bootstrap = [\"bootstrap1.example.com:49737\"]\n");
    } else {
        let entries: Vec<String> = bootstrap.iter().map(|b| format!("\"{b}\"")).collect();
        content.push_str(&format!("bootstrap = [{}]\n", entries.join(", ")));
    }

    content.push_str(
        "\n[node]\n\
         # Bind port for the DHT node (default: 49737)\n\
         # port = 49737\n\
         \n\
         # Bind address (default: 0.0.0.0)\n\
         # host = \"0.0.0.0\"\n\
         \n\
         # Public IPv4 address other nodes reach this node on. Required for the first\n\
         # node of a DHT (no bootstrap nodes) bound to a wildcard host (default: unset)\n\
         # public_host = \"203.0.113.7\"\n\
         \n\
         # How often to log stats in seconds (default: 60)\n\
         # stats_interval = 60\n\
         \n\
         # Max announcement records stored (default: 65536)\n\
         # max_records = 65536\n\
         \n\
         # Max entries per LRU cache (default: 65536)\n\
         # max_lru_size = 65536\n\
         \n\
         # Max peer announcements per topic (default: 20)\n\
         # max_per_key = 20\n\
         \n\
         # TTL for announcement records in seconds (default: 1200)\n\
         # max_record_age = 1200\n\
         \n\
         # TTL for LRU cache entries in seconds (default: 1200)\n\
         # max_lru_age = 1200\n\
         \n\
         [announce]\n\
         # (No configurable options currently)\n\
         \n\
         [cp]\n\
         # (No configurable options currently)\n",
    );

    content
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;

    #[test]
    fn man_pages_without_path_uses_default_dir() {
        use clap::Parser as _;
        let cli = crate::Cli::try_parse_from(["peeroxide", "init", "--man-pages"]).unwrap();
        let Some(crate::Commands::Init(args)) = cli.command else {
            panic!("expected init subcommand");
        };
        assert_eq!(args.man_pages, Some(PathBuf::from("/usr/local/share/man/")));
    }

    #[test]
    fn generated_config_default_is_valid_toml() {
        let content = generate_config_content(false, &[]);
        let parsed: ConfigFile = toml::from_str(&content).unwrap();
        assert!(parsed.network.public.is_none());
        assert!(parsed.network.bootstrap.is_none());
        assert!(parsed.node.port.is_none());
    }

    #[test]
    fn generated_config_with_public_sets_field() {
        let content = generate_config_content(true, &[]);
        let parsed: ConfigFile = toml::from_str(&content).unwrap();
        assert_eq!(parsed.network.public, Some(true));
    }

    #[test]
    fn generated_config_with_bootstrap_sets_field() {
        let content = generate_config_content(false, &["10.0.0.1:49737".to_string()]);
        let parsed: ConfigFile = toml::from_str(&content).unwrap();
        assert_eq!(
            parsed.network.bootstrap,
            Some(vec!["10.0.0.1:49737".to_string()])
        );
    }

    #[test]
    fn resolve_config_path_uses_cli_override() {
        let path = resolve_config_path(&Some("/tmp/custom.toml".to_string()));
        assert_eq!(path, PathBuf::from("/tmp/custom.toml"));
    }

    #[test]
    fn update_preserves_inline_table_siblings() {
        let src = r#"network = { public = false, bootstrap = ["old:1234"] }"#;
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        doc["network"]["public"] = toml_edit::value(true);
        let result = doc.to_string();

        assert!(result.contains("true"), "public should be set to true");
        assert!(result.contains("old:1234"), "bootstrap should be preserved, got: {result}");
    }

    #[test]
    fn update_auto_creates_network_table() {
        let src = "[node]\nport = 49737\n";
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        if doc.get("network").is_none() {
            doc["network"] = toml_edit::Item::Table(toml_edit::Table::new());
        }
        doc["network"]["public"] = toml_edit::value(true);
        let result = doc.to_string();

        assert!(
            result.contains("[network]"),
            "should create standard [network] table, got: {result}"
        );
        assert!(result.contains("public = true"), "public should be set, got: {result}");
        assert!(result.contains("port = 49737"), "existing content preserved, got: {result}");
    }

    #[test]
    fn update_preserves_leading_comments() {
        let src = "[network]\n# Whether public\npublic = false\n# Bootstrap nodes\nbootstrap = [\"old:1\"]\n";
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        doc["network"]["public"] = toml_edit::value(true);
        let result = doc.to_string();

        assert!(result.contains("# Whether public"), "leading comment should be preserved, got: {result}");
        assert!(result.contains("# Bootstrap nodes"), "other comments preserved, got: {result}");
        assert!(result.contains("old:1"), "bootstrap preserved, got: {result}");
    }

    #[test]
    fn update_preserves_trailing_comment() {
        let src = "[network]\npublic = false # keep this\nbootstrap = [\"old:1\"]\n";
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        let old_decor = doc["network"]["public"]
            .as_value()
            .map(|v| (v.decor().prefix().cloned(), v.decor().suffix().cloned()));

        doc["network"]["public"] = toml_edit::value(true);

        if let Some((prefix, suffix)) = old_decor {
            if let Some(val) = doc["network"]["public"].as_value_mut() {
                if let Some(p) = prefix {
                    val.decor_mut().set_prefix(p);
                }
                if let Some(s) = suffix {
                    val.decor_mut().set_suffix(s);
                }
            }
        }

        let result = doc.to_string();
        assert!(result.contains("# keep this"), "trailing comment should be preserved, got: {result}");
    }

    #[test]
    fn update_creates_standard_table_when_network_missing() {
        let src = "[node]\nport = 49737\n";
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        if doc.get("network").is_none() {
            doc["network"] = toml_edit::Item::Table(toml_edit::Table::new());
        }
        doc["network"]["public"] = toml_edit::value(true);
        let result = doc.to_string();

        assert!(
            result.contains("[network]"),
            "should create [network] table header, got: {result}"
        );
        assert!(
            !result.contains("network = {"),
            "should NOT create inline table, got: {result}"
        );
        assert!(result.contains("port = 49737"), "existing content preserved, got: {result}");
    }

    #[test]
    fn update_preserves_value_prefix_spacing() {
        let src = "[network]\nbootstrap    =    [\"a:1\", \"b:2\"]   # keep\n";
        let mut doc: toml_edit::DocumentMut = src.parse().unwrap();

        let old_decor = doc["network"]["bootstrap"]
            .as_value()
            .map(|v| (v.decor().prefix().cloned(), v.decor().suffix().cloned()));

        let arr: toml_edit::Array = ["x:9", "y:10"].iter().copied().collect();
        doc["network"]["bootstrap"] = toml_edit::value(arr);

        if let Some((prefix, suffix)) = old_decor {
            if let Some(val) = doc["network"]["bootstrap"].as_value_mut() {
                if let Some(p) = prefix {
                    val.decor_mut().set_prefix(p);
                }
                if let Some(s) = suffix {
                    val.decor_mut().set_suffix(s);
                }
            }
        }

        let result = doc.to_string();
        assert!(
            result.contains("=    ["),
            "prefix spacing between = and value should be preserved, got: {result}"
        );
        assert!(
            result.contains("# keep"),
            "trailing comment should be preserved, got: {result}"
        );
    }
}
