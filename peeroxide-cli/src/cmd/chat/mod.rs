#![allow(dead_code)]

pub mod crypto;
pub mod debug;
pub mod display;
pub mod dm;
pub mod dm_cmd;
pub mod feed;
pub mod inbox;
pub mod inbox_cmd;
pub mod inbox_monitor;
pub mod join;
pub mod known_users;
pub mod name_resolver;
pub mod names;
pub mod nexus;
pub mod ordering;
pub mod post;
pub mod probe;
pub mod profile;
pub mod publisher;
pub mod reader;
pub mod session;
pub mod tui;
pub mod wire;

use clap::{Parser, Subcommand};

use crate::config::ResolvedConfig;

#[derive(Parser)]
pub struct ChatArgs {
    #[command(subcommand)]
    pub command: ChatCommands,

    /// Enable debug event logging to stderr
    #[arg(long, global = true)]
    pub debug: bool,

    /// Enable message-flow probes (stdin/post/fetch_batch/release) to stderr.
    /// Diagnostic only; useful for tracing ordering and duplication bugs.
    #[arg(long, global = true)]
    pub probe: bool,

    /// Disable the interactive TTY mode and use line-oriented stdin/stdout
    /// even when stdout is a terminal. Auto-enabled when stdout is not a TTY
    /// (e.g. piped or redirected). The env var PEEROXIDE_LINE_MODE=1 has the
    /// same effect.
    #[arg(long, global = true)]
    pub line_mode: bool,
}

#[derive(Subcommand)]
pub enum ChatCommands {
    /// Join a channel and participate interactively
    Join(join::JoinArgs),
    /// Start or resume a DM conversation
    Dm(dm_cmd::DmArgs),
    /// Monitor the invite inbox
    Inbox(inbox_cmd::InboxArgs),
    /// Display the current profile's identity
    Whoami(WhoamiArgs),
    /// Manage profiles
    Profiles {
        #[command(subcommand)]
        command: ProfilesCommands,
    },
    /// Manage friends list
    Friends {
        #[command(subcommand)]
        command: Option<FriendsCommands>,
    },
    /// Manage the personal nexus record
    Nexus(nexus::NexusArgs),
}

#[derive(Parser)]
pub struct WhoamiArgs {
    /// Profile to display
    #[arg(long, default_value = "default")]
    pub profile: String,
}

#[derive(Subcommand)]
pub enum ProfilesCommands {
    /// List all profiles
    List,
    /// Create a new profile
    Create {
        /// Profile name
        name: String,
        /// Optional screen name
        #[arg(long)]
        screen_name: Option<String>,
    },
    /// Delete a profile
    Delete {
        /// Profile name to delete
        name: String,
    },
}

#[derive(Subcommand)]
pub enum FriendsCommands {
    /// List all friends
    List {
        /// Identity profile to use
        #[arg(long, default_value = "default")]
        profile: String,
    },
    /// Add a friend
    Add {
        /// Recipient: alias, pubkey hex (64 chars), @shortkey, name@shortkey, or screen name
        key: String,
        /// Local alias for this friend
        #[arg(long)]
        alias: Option<String>,
        /// Identity profile to use
        #[arg(long, default_value = "default")]
        profile: String,
    },
    /// Remove a friend
    Remove {
        /// Recipient: alias, pubkey hex (64 chars), @shortkey, name@shortkey, or screen name
        key: String,
        /// Identity profile to use
        #[arg(long, default_value = "default")]
        profile: String,
    },
    /// One-shot refresh all friend nexus records
    Refresh,
}

pub async fn run(args: ChatArgs, cfg: &ResolvedConfig) -> i32 {
    if args.debug {
        debug::enable();
    }
    if args.probe {
        probe::enable();
    }
    let line_mode = args.line_mode
        || std::env::var("PEEROXIDE_LINE_MODE")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false);
    match args.command {
        ChatCommands::Join(join_args) => join::run(join_args, cfg, line_mode).await,
        ChatCommands::Dm(dm_args) => dm_cmd::run(dm_args, cfg, line_mode).await,
        ChatCommands::Inbox(inbox_args) => inbox_cmd::run(inbox_args, cfg).await,
        ChatCommands::Whoami(args) => run_whoami(args),
        ChatCommands::Profiles { command } => run_profiles(command),
        ChatCommands::Friends { command } => {
            let command = command.unwrap_or(FriendsCommands::List {
                profile: "default".to_string(),
            });
            match command {
                FriendsCommands::Refresh => run_friends_refresh(cfg).await,
                other => run_friends_sync(other),
            }
        }
        ChatCommands::Nexus(nexus_args) => nexus::run(nexus_args, cfg).await,
    }
}

fn run_whoami(args: WhoamiArgs) -> i32 {
    let prof = match profile::load_or_create_profile(&args.profile) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: failed to load profile '{}': {e}", args.profile);
            return 1;
        }
    };

    let kp = peeroxide_dht::hyperdht::KeyPair::from_seed(prof.seed);
    let pubkey_hex = hex::encode(kp.public_key);
    let nexus_topic = hex::encode(peeroxide_dht::crypto::hash(&kp.public_key));

    println!("Profile: {}", prof.name);
    println!("Public key: {pubkey_hex}");
    if let Some(ref name) = prof.screen_name {
        println!("Screen name: {name}");
    } else {
        println!("Screen name: (not set)");
    }
    println!("Nexus topic: {nexus_topic}");
    0
}

fn run_profiles(command: ProfilesCommands) -> i32 {
    match command {
        ProfilesCommands::List => {
            let profiles = match profile::list_profiles() {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };
            if profiles.is_empty() {
                println!("No profiles found. Create one with: peeroxide chat profiles create <name>");
                return 0;
            }
            for name in profiles {
                match profile::load_profile(&name) {
                    Ok(prof) => {
                        let kp = peeroxide_dht::hyperdht::KeyPair::from_seed(prof.seed);
                        let short = &hex::encode(kp.public_key)[..8];
                        let screen = prof
                            .screen_name
                            .as_deref()
                            .map(|s| format!("({s})"))
                            .unwrap_or_else(|| "(no screen name)".to_string());
                        println!("  {name:16} {short}...  {screen}");
                    }
                    Err(e) => {
                        println!("  {name:16} (error: {e})");
                    }
                }
            }
            0
        }
        ProfilesCommands::Create { name, screen_name } => {
            match profile::create_profile(&name, screen_name.as_deref()) {
                Ok(prof) => {
                    let kp = peeroxide_dht::hyperdht::KeyPair::from_seed(prof.seed);
                    let pubkey_hex = hex::encode(kp.public_key);
                    println!("Created profile '{name}'");
                    println!("Name:       {name}");
                    println!("Public key: {pubkey_hex}");
                    0
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }
        ProfilesCommands::Delete { name } => {
            if name == "default" {
                eprintln!("error: cannot delete the default profile");
                return 1;
            }
            match profile::delete_profile(&name) {
                Ok(()) => {
                    println!("Deleted profile '{name}'");
                    0
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    1
                }
            }
        }
    }
}

fn run_friends_sync(command: FriendsCommands) -> i32 {
    match command {
        FriendsCommands::List { profile } => {
            let friends = match profile::load_friends(&profile) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };
            if friends.is_empty() {
                println!("No friends. Add one with: peeroxide chat friends add <pubkey>");
                return 0;
            }
            for f in &friends {
                let pk_hex = hex::encode(f.pubkey);
                let short = &pk_hex[..8];
                let alias_str = f.alias.as_deref().unwrap_or("");
                let name_str = f
                    .cached_name
                    .clone()
                    .unwrap_or_else(|| names::generate_name_from_seed(&f.pubkey));
                if alias_str.is_empty() {
                    println!("  {short}  {name_str}");
                } else {
                    println!("  {short}  {alias_str} ({name_str})");
                }
            }
            0
        }
        FriendsCommands::Add { key, alias, profile } => {
            // Resolve key: could be full 64-char hex, 8-char shortkey, or name@shortkey
            let pubkey = match resolve_recipient(&profile, &key) {
                Ok(pk) => pk,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };
            if let Err(e) = std::fs::create_dir_all(profile::profile_dir(&profile)) {
                eprintln!("error: {e}");
                return 1;
            }
            let alias = match alias {
                Some(alias) => Some(alias),
                None => known_users::load_shared_users()
                    .ok()
                    .and_then(|users| {
                        users
                            .into_iter()
                            .find(|user| user.pubkey == pubkey)
                            .map(|user| user.screen_name)
                            .filter(|name| !name.is_empty())
                    })
                    .or_else(|| Some(names::generate_name_from_seed(&pubkey))),
            };
            let friend = profile::Friend {
                pubkey,
                alias,
                cached_name: None,
                cached_bio_line: None,
            };
            if let Err(e) = profile::save_friend(&profile, &friend) {
                eprintln!("error: {e}");
                return 1;
            }
            println!("Added friend {}", hex::encode(pubkey));
            0
        }
        FriendsCommands::Remove { key, profile } => {
            let pubkey = match resolve_recipient(&profile, &key) {
                Ok(pk) => pk,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 1;
                }
            };
            if let Err(e) = profile::remove_friend(&profile, &pubkey) {
                eprintln!("error: {e}");
                return 1;
            }
            println!("Removed friend {}", &hex::encode(pubkey)[..8]);
            0
        }
        FriendsCommands::Refresh => unreachable!(),
    }
}

async fn run_friends_refresh(cfg: &ResolvedConfig) -> i32 {
    use libudx::UdxRuntime;
    use peeroxide_dht::hyperdht;

    let dht_config = crate::cmd::build_dht_config(cfg);
    let runtime = match UdxRuntime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let (task, handle, _) = match hyperdht::spawn(&runtime, dht_config).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: failed to start DHT: {e}");
            return 1;
        }
    };

    if let Err(e) = handle.bootstrapped().await {
        eprintln!("error: bootstrap failed: {e}");
        return 1;
    }

    eprintln!("*** refreshing friend nexus records...");
    nexus::refresh_friends(&handle, "default").await;
    eprintln!("*** done");

    let _ = handle.destroy().await;
    let _ = task.await;
    0
}

/// Resolve a recipient identifier to a 32-byte Ed25519 public key.
pub fn resolve_recipient(profile_name: &str, input: &str) -> Result<[u8; 32], String> {
    let resolved = if input.len() == 64 {
        match hex::decode(input) {
            Ok(bytes) if bytes.len() == 32 => {
                let mut pk = [0u8; 32];
                pk.copy_from_slice(&bytes);
                Ok(pk)
            }
            _ => Err(format!("invalid 64-char hex pubkey: '{input}'")),
        }
    } else if let Some(shortkey) = input.strip_prefix('@') {
        resolve_shortkey_input(shortkey)
    } else if let Some(pos) = input.rfind('@') {
        let name_part = &input[..pos];
        let shortkey_part = &input[pos + 1..];
        let pk = resolve_shortkey_input(shortkey_part)?;

        let users = known_users::load_shared_users()
            .map_err(|e| format!("failed to load known users: {e}"))?;
        if let Some(user) = users.iter().find(|u| u.pubkey == pk) {
            if user.screen_name == name_part {
                Ok(pk)
            } else {
                Err("name mismatch".to_string())
            }
        } else {
            Ok(pk)
        }
    } else if input.len() == 8 && input.chars().all(|c| c.is_ascii_hexdigit()) {
        resolve_shortkey_input(input)
    } else {
        let friends = profile::load_friends(profile_name).unwrap_or_default();
        let mut matched_pubkeys: Vec<[u8; 32]> = Vec::new();
        for f in &friends {
            if f.alias.as_deref() == Some(input) {
                matched_pubkeys.push(f.pubkey);
            }
        }

        if matched_pubkeys.is_empty() {
            let users = known_users::load_shared_users().unwrap_or_default();
            for u in &users {
                if u.screen_name == input {
                    matched_pubkeys.push(u.pubkey);
                }
            }
        }

        matched_pubkeys.sort();
        matched_pubkeys.dedup();
        match matched_pubkeys.len() {
            1 => Ok(matched_pubkeys[0]),
            0 => Err(format!("recipient '{input}' not found")),
            n => Err(format!("recipient '{input}' is ambiguous ({n} matches)")),
        }
    };

    let resolved = resolved?;
    if let Ok(own_prof) = profile::load_profile(profile_name) {
        let own_kp = peeroxide_dht::hyperdht::KeyPair::from_seed(own_prof.seed);
        if resolved == own_kp.public_key {
            return Err("cannot send a DM to yourself".to_string());
        }
    }
    Ok(resolved)
}

fn resolve_shortkey_input(shortkey: &str) -> Result<[u8; 32], String> {
    let mut cache = known_users::SharedKnownUsers::load_from_shared();
    match cache.resolve_shortkey(shortkey) {
        Ok(Some(pk)) => Ok(pk),
        Ok(None) => Err(format!("shortkey '{shortkey}' not found in known users")),
        Err(e) => Err(format!("failed to search known users: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{self, Write};
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;

    fn pk(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn profile_root(home: &Path) -> std::path::PathBuf {
        home.join(".config/peeroxide/chat/profiles")
    }

    fn write_profile(home: &Path, name: &str, seed: [u8; 32]) -> io::Result<()> {
        let dir = profile_root(home).join(name);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("seed"), seed)
    }

    fn write_known_users(home: &Path, rows: &[([u8; 32], &str)]) -> io::Result<()> {
        let dir = home.join(".config").join("peeroxide").join("chat");
        fs::create_dir_all(&dir)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("known_users"))?;
        for (pubkey, name) in rows {
            writeln!(file, "{}\t{}", hex::encode(pubkey), name)?;
        }
        Ok(())
    }

    fn write_friends(home: &Path, profile_name: &str, rows: &[([u8; 32], Option<&str>)]) -> io::Result<()> {
        let dir = profile_root(home).join(profile_name);
        fs::create_dir_all(&dir)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("friends"))?;
        for (pubkey, alias) in rows {
            writeln!(file, "{}\t{}\t\t", hex::encode(pubkey), alias.unwrap_or(""))?;
        }
        Ok(())
    }

    fn prepare_profile(home: &Path, profile_name: &str) -> io::Result<()> {
        fs::create_dir_all(profile_root(home).join(profile_name))
    }

    fn friend_output(friend: &profile::Friend) -> String {
        let pk_hex = hex::encode(friend.pubkey);
        let short = &pk_hex[..8];
        let alias_str = friend.alias.as_deref().unwrap_or("");
        let name_str = friend
            .cached_name
            .clone()
            .unwrap_or_else(|| names::generate_name_from_seed(&friend.pubkey));
        if alias_str.is_empty() {
            format!("  {short}  {name_str}")
        } else {
            format!("  {short}  {alias_str} ({name_str})")
        }
    }

    fn current_test_binary() -> std::path::PathBuf {
        std::env::current_exe().unwrap()
    }

    /// Runs the sandbox test `name` from this module in a child test process,
    /// with `PEEROXIDE_HOME` pointing at `home`, so the chat code under test
    /// resolves its paths there without mutating this process's environment.
    fn run_sandbox(name: &str, home: &Path, envs: &[(&str, &str)]) {
        // libtest matches `--exact` against the path without the crate name.
        let module = module_path!();
        let module = module.split_once("::").map_or(module, |(_, rest)| rest);
        let output = Command::new(current_test_binary())
            .args(["--exact", &format!("{module}::{name}"), "--nocapture", "--test-threads=1"])
            .env(profile::HOME_ENV, home)
            .envs(envs.iter().copied())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "stdout: {stdout}\nstderr: {stderr}");
        // A filter that matches nothing also exits successfully.
        assert!(
            stdout.contains("1 passed"),
            "sandbox test {name} did not run\nstdout: {stdout}\nstderr: {stderr}"
        );
    }

    fn run_child_case(home: &Path, case: &str, profile_name: &str, input: &str) {
        run_sandbox(
            "resolve_recipient_sandbox",
            home,
            &[("RESOLVE_CASE", case), ("RESOLVE_PROFILE", profile_name), ("RESOLVE_INPUT", input)],
        );
    }

    fn run_friends_child_case(home: &Path, case: &str) {
        run_sandbox("friends_sandbox", home, &[("FRIENDS_CASE", case)]);
    }

    #[test]
    fn test_resolve_64char_valid_hex() {
        let tmp = TempDir::new().unwrap();
        let input = hex::encode([0x11u8; 32]);
        run_child_case(tmp.path(), "valid_hex", "default", &input);
    }

    #[test]
    fn test_resolve_64char_invalid_hex() {
        let tmp = TempDir::new().unwrap();
        let input = "g".repeat(64);
        run_child_case(tmp.path(), "invalid_hex", "default", &input);
    }

    #[test]
    fn test_resolve_at_shortkey() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(1), "Alice")]).unwrap();
        let shortkey = &hex::encode(pk(1))[..8];
        run_child_case(tmp.path(), "at_shortkey", "default", &format!("@{shortkey}"));
    }

    #[test]
    fn test_resolve_name_at_shortkey() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(2), "alice")]).unwrap();
        let shortkey = &hex::encode(pk(2))[..8];
        run_child_case(tmp.path(), "name_at_shortkey", "default", &format!("alice@{shortkey}"));
    }

    #[test]
    fn test_resolve_bare_shortkey() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(3), "Bob")]).unwrap();
        let shortkey = &hex::encode(pk(3))[..8];
        run_child_case(tmp.path(), "bare_shortkey", "default", shortkey);
    }

    #[test]
    fn test_resolve_friend_alias() {
        let tmp = TempDir::new().unwrap();
        write_friends(tmp.path(), "default", &[(pk(4), Some("carol"))]).unwrap();
        run_child_case(tmp.path(), "friend_alias", "default", "carol");
    }

    #[test]
    fn test_resolve_known_user_screen_name() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(5), "dave")]).unwrap();
        run_child_case(tmp.path(), "known_user", "default", "dave");
    }

    #[test]
    fn test_resolve_friend_alias_priority() {
        let tmp = TempDir::new().unwrap();
        write_friends(tmp.path(), "default", &[(pk(6), Some("erin"))]).unwrap();
        write_known_users(tmp.path(), &[(pk(7), "erin")]).unwrap();
        run_child_case(tmp.path(), "friend_priority", "default", "erin");
    }

    #[test]
    fn test_resolve_ambiguous() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(8), "frank"), (pk(9), "frank")]).unwrap();
        run_child_case(tmp.path(), "ambiguous", "default", "frank");
    }

    #[test]
    fn test_resolve_not_found() {
        let tmp = TempDir::new().unwrap();
        run_child_case(tmp.path(), "not_found", "default", "missing");
    }

    #[test]
    fn test_resolve_name_mismatch() {
        let tmp = TempDir::new().unwrap();
        write_known_users(tmp.path(), &[(pk(10), "grace")]).unwrap();
        let shortkey = &hex::encode(pk(10))[..8];
        run_child_case(tmp.path(), "name_mismatch", "default", &format!("wrong@{shortkey}"));
    }

    #[test]
    fn test_friends_add_auto_alias_vendor() {
        let tmp = TempDir::new().unwrap();
        run_friends_child_case(tmp.path(), "vendor");
    }

    #[test]
    fn test_friends_add_auto_alias_explicit_preserved() {
        let tmp = TempDir::new().unwrap();
        run_friends_child_case(tmp.path(), "explicit");
    }

    #[test]
    fn test_friends_list_vendor_fallback() {
        let tmp = TempDir::new().unwrap();
        run_friends_child_case(tmp.path(), "vendor_fallback");
    }

    #[test]
    fn test_friends_list_cached_name_preserved() {
        let friend = profile::Friend {
            pubkey: pk(14),
            alias: Some("pal".to_string()),
            cached_name: Some("Alice".to_string()),
            cached_bio_line: None,
        };
        let line = friend_output(&friend);
        assert!(line.contains("Alice"));
        assert!(!line.contains("(unknown)"));
    }

    #[test]
    fn test_resolve_self_guard() {
        let tmp = TempDir::new().unwrap();
        let seed = [0x42u8; 32];
        write_profile(tmp.path(), "default", seed).unwrap();
        let own_pk = peeroxide_dht::hyperdht::KeyPair::from_seed(seed).public_key;
        run_child_case(tmp.path(), "self_guard", "default", &hex::encode(own_pk));
    }

    #[test]
    fn resolve_recipient_sandbox() {
        let case = match std::env::var("RESOLVE_CASE") {
            Ok(v) => v,
            Err(_) => return,
        };
        let profile_name = std::env::var("RESOLVE_PROFILE").unwrap();
        let input = std::env::var("RESOLVE_INPUT").unwrap();
        match case.as_str() {
            "valid_hex" => {
                let pk = resolve_recipient(&profile_name, &input).unwrap();
                assert_eq!(pk, [0x11u8; 32]);
            }
            "invalid_hex" => {
                let err = resolve_recipient(&profile_name, &input).unwrap_err();
                assert_eq!(err, format!("invalid 64-char hex pubkey: '{input}'"));
            }
            "at_shortkey" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(1));
            }
            "name_at_shortkey" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(2));
            }
            "bare_shortkey" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(3));
            }
            "friend_alias" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(4));
            }
            "known_user" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(5));
            }
            "friend_priority" => {
                assert_eq!(resolve_recipient(&profile_name, &input).unwrap(), pk(6));
            }
            "ambiguous" => {
                let err = resolve_recipient(&profile_name, &input).unwrap_err();
                assert!(err.contains("ambiguous"));
            }
            "not_found" => {
                let err = resolve_recipient(&profile_name, &input).unwrap_err();
                assert!(err.contains("not found"));
            }
            "name_mismatch" => {
                let err = resolve_recipient(&profile_name, &input).unwrap_err();
                assert_eq!(err, "name mismatch");
            }
            "self_guard" => {
                let err = resolve_recipient(&profile_name, &input).unwrap_err();
                assert_eq!(err, "cannot send a DM to yourself");
            }
            other => panic!("unknown case: {other}"),
        }
    }

    #[test]
    fn friends_sandbox() {
        let case = match std::env::var("FRIENDS_CASE") {
            Ok(v) => v,
            Err(_) => return,
        };
        let home = std::path::PathBuf::from(std::env::var_os(profile::HOME_ENV).unwrap());
        match case.as_str() {
            "vendor" => {
                prepare_profile(&home, "default").unwrap();
                let pubkey = pk(11);
                let expected = names::generate_name_from_seed(&pubkey);
                let friend = profile::Friend {
                    pubkey,
                    alias: Some(expected.clone()),
                    cached_name: None,
                    cached_bio_line: None,
                };
                profile::save_friend("default", &friend).unwrap();
                let loaded = profile::load_friends("default").unwrap();
                assert_eq!(loaded.len(), 1);
                assert_eq!(loaded[0].alias.as_deref(), Some(expected.as_str()));
            }
            "explicit" => {
                prepare_profile(&home, "default").unwrap();
                let friend = profile::Friend {
                    pubkey: pk(12),
                    alias: Some("buddy".to_string()),
                    cached_name: None,
                    cached_bio_line: None,
                };
                profile::save_friend("default", &friend).unwrap();
                let loaded = profile::load_friends("default").unwrap();
                assert_eq!(loaded.len(), 1);
                assert_eq!(loaded[0].alias.as_deref(), Some("buddy"));
            }
            "vendor_fallback" => {
                prepare_profile(&home, "default").unwrap();
                let pubkey = pk(13);
                let friend = profile::Friend {
                    pubkey,
                    alias: None,
                    cached_name: None,
                    cached_bio_line: None,
                };
                profile::save_friend("default", &friend).unwrap();
                let loaded = profile::load_friends("default").unwrap();
                let line = friend_output(&loaded[0]);
                let expected = names::generate_name_from_seed(&pubkey);
                assert!(line.contains(&expected));
                assert!(!line.contains("(unknown)"));
            }
            other => panic!("unknown case: {other}"),
        }
    }
}
