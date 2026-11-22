//! Profile directory management for the peeroxide chat system.
//!
//! ## Directory Layout
//!
//! ```text
//! ~/.config/peeroxide/chat/profiles/<name>/
//! ├── seed         # 32 raw bytes (Ed25519 seed)
//! ├── name         # UTF-8 screen name (optional)
//! ├── bio          # UTF-8 bio text (optional)
//! └── friends      # tab-separated: pubkey\talias\tcached_name\tcached_bio_line
//! ```
//!
//! The shared known-users cache lives one level up at
//! `~/.config/peeroxide/chat/known_users` and is process-wide, not per
//! profile — see `known_users::shared_known_users_path`.
//!
//! `$PEEROXIDE_HOME`, when set and non-empty, takes the place of `~` in both
//! paths — see [`chat_home_dir`].

use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use peeroxide_dht::hyperdht::KeyPair;

use super::names;

/// A local chat identity stored on disk.
#[derive(Debug, Clone)]
pub struct Profile {
    /// Directory name used to identify this profile on disk.
    pub name: String,
    /// Raw Ed25519 seed (32 bytes).
    pub seed: [u8; 32],
    /// Optional human-readable screen name.
    pub screen_name: Option<String>,
    /// Optional biography text.
    pub bio: Option<String>,
}

/// A trusted contact stored in the `friends` file.
#[derive(Debug, Clone)]
pub struct Friend {
    /// The friend's Ed25519 public key (32 bytes).
    pub pubkey: [u8; 32],
    /// Local alias chosen by the profile owner.
    pub alias: Option<String>,
    /// Most recently cached screen name announced by the friend.
    pub cached_name: Option<String>,
    /// Most recently cached first line of bio announced by the friend.
    pub cached_bio_line: Option<String>,
}

/// Environment variable that, when set and non-empty, replaces the home
/// directory as the base of `.config/peeroxide/chat/`.
pub const HOME_ENV: &str = "PEEROXIDE_HOME";

/// Returns the directory under which chat state lives in `.config/peeroxide/chat/`:
/// `$PEEROXIDE_HOME` if set and non-empty, otherwise the user's home directory.
///
/// On Windows the home directory is the user's profile folder whatever `HOME`
/// says, so `$PEEROXIDE_HOME` is the only way to move chat state there.
pub fn chat_home_dir() -> Option<PathBuf> {
    match std::env::var_os(HOME_ENV) {
        Some(dir) if !dir.is_empty() => Some(PathBuf::from(dir)),
        _ => dirs::home_dir(),
    }
}

/// Returns `~/.config/peeroxide/chat/profiles/` (see [`chat_home_dir`]).
pub fn profiles_dir() -> PathBuf {
    chat_home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join("peeroxide")
        .join("chat")
        .join("profiles")
}

/// Returns the directory for a specific named profile.
pub fn profile_dir(name: &str) -> PathBuf {
    profiles_dir().join(name)
}

/// Creates a new profile on disk.
///
/// Generates a fresh random 32-byte seed, creates the profile directory, and
/// writes the seed (and optional screen name) to disk.  Fails if the profile
/// already exists.
pub fn create_profile(name: &str, screen_name: Option<&str>) -> io::Result<Profile> {
    let dir = profile_dir(name);
    if dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("profile '{}' already exists at {}", name, dir.display()),
        ));
    }
    fs::create_dir_all(&dir)?;

    let mut seed = [0u8; 32];
    {
        use rand::RngCore;
        rand::rng().fill_bytes(&mut seed);
    }

    fs::write(dir.join("seed"), seed)?;

    let effective_screen_name = match screen_name {
        Some(sn) => sn.to_owned(),
        None => names::generate_name_from_seed(&KeyPair::from_seed(seed).public_key),
    };
    fs::write(dir.join("name"), &effective_screen_name)?;

    Ok(Profile {
        name: name.to_owned(),
        seed,
        screen_name: Some(effective_screen_name),
        bio: None,
    })
}

/// Loads an existing profile from disk.
pub fn load_profile(name: &str) -> io::Result<Profile> {
    let dir = profile_dir(name);

    let seed_bytes = fs::read(dir.join("seed"))?;
    if seed_bytes.len() != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "profile '{}': seed file must be exactly 32 bytes, got {}",
                name,
                seed_bytes.len()
            ),
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);

    let screen_name = match read_optional_text(&dir.join("name"))? {
        Some(name) => Some(name),
        None => Some(names::generate_name_from_seed(&KeyPair::from_seed(seed).public_key)),
    };
    let bio = read_optional_text(&dir.join("bio"))?;

    Ok(Profile {
        name: name.to_owned(),
        seed,
        screen_name,
        bio,
    })
}

pub fn load_or_create_profile(name: &str) -> io::Result<Profile> {
    match load_profile(name) {
        Ok(p) => Ok(p),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            eprintln!("*** creating new profile '{name}'");
            create_profile(name, None)
        }
        Err(e) => Err(e),
    }
}

/// Deletes a profile and all its files from disk.
pub fn delete_profile(name: &str) -> io::Result<()> {
    let dir = profile_dir(name);
    if !dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("profile '{}' does not exist", name),
        ));
    }
    fs::remove_dir_all(dir)
}

/// Lists all profile names (subdirectory names inside `profiles_dir()`).
pub fn list_profiles() -> io::Result<Vec<String>> {
    let dir = profiles_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(n) = entry.file_name().to_str() {
                names.push(n.to_owned());
            }
        }
    }
    names.sort();
    Ok(names)
}

/// Loads the `friends` file for the given profile.
///
/// Lines are tab-separated: `<64-hex-pubkey>\t<alias>\t<cached_name>\t<cached_bio_line>`.
/// Lines starting with `#` are comments and are skipped.  When the same
/// public key appears more than once, the **last** entry wins.
pub fn load_friends(profile_name: &str) -> io::Result<Vec<Friend>> {
    let path = profile_dir(profile_name).join("friends");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;

    let mut map: HashMap<[u8; 32], (usize, Friend)> = HashMap::new();
    let mut order: Vec<[u8; 32]> = Vec::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.splitn(4, '\t').collect();
        let pubkey = match decode_pubkey(parts[0]) {
            Ok(k) => k,
            Err(_) => continue,
        };
        let alias = optional_field(parts.get(1).copied());
        let cached_name = optional_field(parts.get(2).copied());
        let cached_bio_line = optional_field(parts.get(3).copied());

        let friend = Friend {
            pubkey,
            alias,
            cached_name,
            cached_bio_line,
        };

        if let Some(existing) = map.get_mut(&pubkey) {
            existing.1 = friend;
        } else {
            let idx = order.len();
            order.push(pubkey);
            map.insert(pubkey, (idx, friend));
        }
    }

    let mut result: Vec<(usize, Friend)> = map.into_values().collect();
    result.sort_by_key(|(idx, _)| *idx);
    Ok(result.into_iter().map(|(_, f)| f).collect())
}

/// Appends or updates a friend entry in the `friends` file.
///
/// The entry is always appended; deduplication happens at read time (latest
/// entry wins).
pub fn save_friend(profile_name: &str, friend: &Friend) -> io::Result<()> {
    let path = profile_dir(profile_name).join("friends");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;

    let line = format!(
        "{}\t{}\t{}\t{}\n",
        hex::encode(friend.pubkey),
        friend.alias.as_deref().unwrap_or(""),
        friend.cached_name.as_deref().unwrap_or(""),
        friend.cached_bio_line.as_deref().unwrap_or(""),
    );
    file.write_all(line.as_bytes())
}

/// Removes a friend from the `friends` file by rewriting the file without
/// any entries for the given public key.
pub fn remove_friend(profile_name: &str, pubkey: &[u8; 32]) -> io::Result<()> {
    let path = profile_dir(profile_name).join("friends");
    if !path.exists() {
        return Ok(());
    }
    let content = fs::read_to_string(&path)?;
    let target_hex = hex::encode(pubkey);

    let filtered: String = content
        .lines()
        .filter(|line| {
            let l = line.trim();
            if l.is_empty() || l.starts_with('#') {
                return true;
            }
            let first_field = l.split('\t').next().unwrap_or("");
            first_field != target_hex
        })
        .map(|l| format!("{}\n", l))
        .collect();

    fs::write(&path, filtered)
}

fn read_optional_text(path: &std::path::Path) -> io::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(s) => {
            let trimmed = s.trim().to_owned();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn decode_pubkey(s: &str) -> Result<[u8; 32], hex::FromHexError> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        // `hex::FromHexError` has no wrong-length variant; `InvalidStringLength`
        // is the closest available error for a well-formed but wrong-sized decode.
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn optional_field(s: Option<&str>) -> Option<String> {
    match s {
        Some(v) if !v.is_empty() => Some(v.to_owned()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn do_create_profile(
        profiles_root: &std::path::Path,
        name: &str,
        screen_name: Option<&str>,
    ) -> io::Result<Profile> {
        let dir = profiles_root.join(name);
        if dir.exists() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "already exists"));
        }
        fs::create_dir_all(&dir)?;

        let mut seed = [0u8; 32];
        {
            use rand::RngCore;
            rand::rng().fill_bytes(&mut seed);
        }
        fs::write(dir.join("seed"), seed)?;
        let effective_screen_name = match screen_name {
            Some(sn) => sn.to_owned(),
            None => crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(seed).public_key),
        };
        fs::write(dir.join("name"), &effective_screen_name)?;
        Ok(Profile {
            name: name.to_owned(),
            seed,
            screen_name: Some(effective_screen_name),
            bio: None,
        })
    }

    fn do_load_profile(profiles_root: &std::path::Path, name: &str) -> io::Result<Profile> {
        let dir = profiles_root.join(name);
        let seed_bytes = fs::read(dir.join("seed"))?;
        if seed_bytes.len() != 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "seed must be 32 bytes",
            ));
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&seed_bytes);
        let screen_name = match read_optional_text(&dir.join("name"))? {
            Some(name) => Some(name),
            None => Some(crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(seed).public_key)),
        };
        let bio = read_optional_text(&dir.join("bio"))?;
        Ok(Profile {
            name: name.to_owned(),
            seed,
            screen_name,
            bio,
        })
    }

    #[test]
    fn profile_create_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        let created = do_create_profile(root, "alice", Some("Alice Liddell")).unwrap();
        assert_eq!(created.name, "alice");
        assert_eq!(created.screen_name.as_deref(), Some("Alice Liddell"));
        assert!(created.bio.is_none());

        let loaded = do_load_profile(root, "alice").unwrap();
        assert_eq!(loaded.name, "alice");
        assert_eq!(loaded.seed, created.seed);
        assert_eq!(loaded.screen_name, created.screen_name);
    }

    #[test]
    fn profile_create_no_screen_name() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "bob", None).unwrap();
        let expected = crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(created.seed).public_key);
        assert_eq!(created.screen_name.as_deref(), Some(expected.as_str()));
        let loaded = do_load_profile(tmp.path(), "bob").unwrap();
        assert_eq!(loaded.screen_name.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn profile_seed_is_32_bytes() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "carol", None).unwrap();
        let raw = fs::read(tmp.path().join("carol").join("seed")).unwrap();
        assert_eq!(raw.len(), 32);
        assert_eq!(raw.as_slice(), created.seed.as_slice());
    }

    fn write_friends_file(dir: &std::path::Path, content: &str) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        fs::write(dir.join("friends"), content)
    }

    fn parse_friends_from_dir(dir: &std::path::Path) -> io::Result<Vec<Friend>> {
        let path = dir.join("friends");
        if !path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(&path)?;
        let mut map: HashMap<[u8; 32], (usize, Friend)> = HashMap::new();
        let mut order: Vec<[u8; 32]> = Vec::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.splitn(4, '\t').collect();
            let pubkey = match decode_pubkey(parts[0]) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let alias = optional_field(parts.get(1).copied());
            let cached_name = optional_field(parts.get(2).copied());
            let cached_bio_line = optional_field(parts.get(3).copied());
            let friend = Friend { pubkey, alias, cached_name, cached_bio_line };
            if let Some(existing) = map.get_mut(&pubkey) {
                existing.1 = friend;
            } else {
                let idx = order.len();
                order.push(pubkey);
                map.insert(pubkey, (idx, friend));
            }
        }
        let mut result: Vec<(usize, Friend)> = map.into_values().collect();
        result.sort_by_key(|(idx, _)| *idx);
        Ok(result.into_iter().map(|(_, f)| f).collect())
    }

    fn pubkey_from_u8(n: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = n;
        k
    }

    #[test]
    fn friends_parse_basic() {
        let tmp = TempDir::new().unwrap();
        let key_a = pubkey_from_u8(1);
        let key_b = pubkey_from_u8(2);
        let content = format!(
            "# comment\n{}\talias_a\tCached A\tBio A\n{}\t\t\t\n",
            hex::encode(key_a),
            hex::encode(key_b),
        );
        write_friends_file(tmp.path(), &content).unwrap();

        let friends = parse_friends_from_dir(tmp.path()).unwrap();
        assert_eq!(friends.len(), 2);

        assert_eq!(friends[0].pubkey, key_a);
        assert_eq!(friends[0].alias.as_deref(), Some("alias_a"));
        assert_eq!(friends[0].cached_name.as_deref(), Some("Cached A"));
        assert_eq!(friends[0].cached_bio_line.as_deref(), Some("Bio A"));

        assert_eq!(friends[1].pubkey, key_b);
        assert!(friends[1].alias.is_none());
        assert!(friends[1].cached_name.is_none());
        assert!(friends[1].cached_bio_line.is_none());
    }

    #[test]
    fn friends_dedup_last_wins() {
        let tmp = TempDir::new().unwrap();
        let key = pubkey_from_u8(42);
        let content = format!(
            "{}\told_alias\told_name\told_bio\n{}\tnew_alias\tnew_name\tnew_bio\n",
            hex::encode(key),
            hex::encode(key),
        );
        write_friends_file(tmp.path(), &content).unwrap();

        let friends = parse_friends_from_dir(tmp.path()).unwrap();
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].alias.as_deref(), Some("new_alias"));
        assert_eq!(friends[0].cached_name.as_deref(), Some("new_name"));
    }

    #[test]
    fn friends_skips_malformed_lines() {
        let tmp = TempDir::new().unwrap();
        let key = pubkey_from_u8(5);
        let content = format!(
            "not-hex\talias\tname\tbio\n{}\tvalid\t\t\n",
            hex::encode(key),
        );
        write_friends_file(tmp.path(), &content).unwrap();
        let friends = parse_friends_from_dir(tmp.path()).unwrap();
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].pubkey, key);
    }

    #[test]
    fn create_profile_without_name_gets_generated_name() {
        let seed = [99u8; 32];
        let name = crate::cmd::chat::names::generate_name_from_seed(&seed);
        assert!(name.contains('_'), "generated name must contain underscore: {name}");
        let parts: Vec<&str> = name.splitn(3, '_').collect();
        assert_eq!(parts.len(), 3);
        assert!(parts[2].chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn create_profile_user_name_preserved() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "named", Some("MyCustomName")).unwrap();
        assert_eq!(created.screen_name.as_deref(), Some("MyCustomName"));
        let loaded = do_load_profile(tmp.path(), "named").unwrap();
        assert_eq!(loaded.screen_name.as_deref(), Some("MyCustomName"));
    }

    #[test]
    fn load_profile_derives_name_when_file_missing() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "missing-name", Some("Custom")).unwrap();
        fs::remove_file(tmp.path().join("missing-name").join("name")).unwrap();

        let loaded = do_load_profile(tmp.path(), "missing-name").unwrap();
        let expected = crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(created.seed).public_key);
        assert_eq!(loaded.screen_name.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn profile_create_no_screen_name_uses_pubkey() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "pubkey-create", None).unwrap();
        let expected = crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(created.seed).public_key);
        assert_eq!(created.screen_name.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn profile_load_missing_name_uses_pubkey() {
        let tmp = TempDir::new().unwrap();
        let created = do_create_profile(tmp.path(), "pubkey-load", Some("Shown")).unwrap();
        fs::remove_file(tmp.path().join("pubkey-load").join("name")).unwrap();

        let loaded = do_load_profile(tmp.path(), "pubkey-load").unwrap();
        let expected = crate::cmd::chat::names::generate_name_from_seed(&KeyPair::from_seed(created.seed).public_key);
        assert_eq!(loaded.screen_name.as_deref(), Some(expected.as_str()));
    }
}
