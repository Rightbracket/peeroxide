//! Shared known-users file with atomic I/O and mtime-based cache invalidation.
//!
//! File format: one entry per line, tab-separated:
//! `<64-hex-pubkey>\t<screen_name>`

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use fs2::FileExt;

/// A single entry in the known-users file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownUser {
    pub pubkey: [u8; 32],
    pub screen_name: String,
}

/// Returns `~/.config/peeroxide/chat/known_users`.
pub fn shared_known_users_path() -> PathBuf {
    let home = dirs::home_dir().expect("home directory not found");
    home.join(".config")
        .join("peeroxide")
        .join("chat")
        .join("known_users")
}

/// In-memory view of the shared known-users file.
///
/// Caches entries in memory with mtime-based invalidation (5-second debounce).
/// Writer coordination uses `fs2` advisory exclusive locks.  No Arc/Mutex —
/// this struct is single-owner.
pub struct SharedKnownUsers {
    path: PathBuf,
    entries: Vec<KnownUser>,
    index: HashMap<[u8; 32], usize>,
    last_mtime: Option<SystemTime>,
    last_checked: Instant,
}

impl SharedKnownUsers {
    /// Creates a new instance from the given path and immediately loads it.
    ///
    /// A missing file is treated as empty (no error).
    pub fn new(path: PathBuf) -> Self {
        let mut s = Self {
            path,
            entries: Vec::new(),
            index: HashMap::new(),
            last_mtime: None,
            last_checked: Instant::now(),
        };
        s.load();
        s
    }

    /// Convenience constructor using [`shared_known_users_path()`].
    pub fn load_from_shared() -> Self {
        Self::new(shared_known_users_path())
    }

    /// Reads and parses the file, updating `entries`, `index`, and `last_mtime`.
    ///
    /// A missing file silently results in an empty list.  Any unreadable file
    /// is also silently ignored to avoid crashing long-running callers.
    pub fn load(&mut self) {
        self.entries.clear();
        self.index.clear();
        self.last_mtime = None;

        let content = match fs::read_to_string(&self.path) {
            Ok(c) => c,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return,
            Err(_) => return,
        };

        // Record mtime *after* a successful read.
        if let Ok(meta) = fs::metadata(&self.path) {
            self.last_mtime = meta.modified().ok();
        }

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.splitn(2, '\t');
            let hex_key = parts.next().unwrap_or("").trim();
            let screen_name = parts.next().unwrap_or("").trim().to_owned();

            let pubkey = match decode_pubkey(hex_key) {
                Ok(k) => k,
                Err(_) => continue,
            };

            if let Some(&idx) = self.index.get(&pubkey) {
                // Last-wins: update the existing entry in place.
                self.entries[idx].screen_name = screen_name;
            } else {
                let idx = self.entries.len();
                self.entries.push(KnownUser { pubkey, screen_name });
                self.index.insert(pubkey, idx);
            }
        }
    }

    /// Reloads the file if the mtime changed and at least 5 seconds have
    /// elapsed since the last check.  Skips entirely when called too soon.
    pub fn maybe_reload(&mut self) {
        const CHECK_INTERVAL: Duration = Duration::from_secs(5);
        if self.last_checked.elapsed() < CHECK_INTERVAL {
            return;
        }
        self.last_checked = Instant::now();

        let current_mtime = fs::metadata(&self.path).ok().and_then(|m| m.modified().ok());
        if current_mtime != self.last_mtime {
            self.load();
        }
    }

    /// Returns the screen name for `pubkey`, calling `maybe_reload` first.
    pub fn get(&mut self, pubkey: &[u8; 32]) -> Option<&str> {
        self.maybe_reload();
        self.index
            .get(pubkey)
            .map(|&idx| self.entries[idx].screen_name.as_str())
    }

    /// Returns all entries as a slice, calling `maybe_reload` first.
    pub fn all_users(&mut self) -> &[KnownUser] {
        self.maybe_reload();
        &self.entries
    }

    /// Resolves a hex prefix to a pubkey.
    ///
    /// Returns `Ok(None)` when nothing matches, `Ok(Some(key))` for a unique
    /// match, and an `InvalidInput` error when the prefix is ambiguous.
    pub fn resolve_shortkey(&mut self, prefix: &str) -> io::Result<Option<[u8; 32]>> {
        if prefix.len() > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shortkey must not exceed 64 hex characters",
            ));
        }
        self.maybe_reload();
        let lower = prefix.to_lowercase();
        let matches: Vec<[u8; 32]> = self
            .entries
            .iter()
            .filter(|u| hex::encode(u.pubkey).starts_with(&lower))
            .map(|u| u.pubkey)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches[0])),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "shortkey '{}' is ambiguous: {} matches found",
                    prefix, n
                ),
            )),
        }
    }

    /// Inserts or updates `pubkey → screen_name`, writing atomically on change.
    ///
    /// Skips silently when `screen_name` is empty or sanitises to empty.
    /// Skips silently when the stored name is already `screen_name`.
    /// Enforces a 1 000-entry FIFO cap; oldest entry is evicted on overflow.
    pub fn update(&mut self, pubkey: &[u8; 32], screen_name: &str) -> io::Result<()> {
        if screen_name.is_empty() {
            return Ok(());
        }
        let sanitized = sanitize_screen_name(screen_name);
        if sanitized.is_empty() {
            return Ok(());
        }

        if let Some(&idx) = self.index.get(pubkey) {
            if self.entries[idx].screen_name == sanitized {
                return Ok(()); // skip-if-unchanged
            }
            self.entries[idx].screen_name = sanitized;
        } else {
            let idx = self.entries.len();
            self.entries.push(KnownUser {
                pubkey: *pubkey,
                screen_name: sanitized,
            });
            self.index.insert(*pubkey, idx);
        }

        // FIFO eviction when over cap.
        if self.entries.len() > 1000 {
            self.entries.remove(0);
            // Rebuild index after the removal shifted all slots.
            self.index.clear();
            for (i, entry) in self.entries.iter().enumerate() {
                self.index.insert(entry.pubkey, i);
            }
        }

        self.write_atomic()?;

        // Track the mtime of our own write to avoid re-reading it.
        if let Ok(meta) = fs::metadata(&self.path) {
            self.last_mtime = meta.modified().ok();
        }
        self.last_checked = Instant::now();

        Ok(())
    }

    /// Writes all entries atomically via a tmp-file + rename.
    ///
    /// Uses an `fs2` advisory exclusive lock on `.known_users.lock` to
    /// coordinate concurrent writers.
    pub fn write_atomic(&self) -> io::Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "known_users path has no parent directory",
            )
        })?;
        fs::create_dir_all(parent)?;

        let lock_path = parent.join(".known_users.lock");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)?;
        lock_file.lock_exclusive()?;

        let tmp_path = parent.join(".known_users.tmp");
        let mut content = String::new();
        for entry in &self.entries {
            content.push_str(&hex::encode(entry.pubkey));
            content.push('\t');
            content.push_str(&entry.screen_name);
            content.push('\n');
        }
        fs::write(&tmp_path, content.as_bytes())?;
        fs::rename(&tmp_path, &self.path)?;

        drop(lock_file);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn force_stale(&mut self) {
        self.last_checked = Instant::now() - Duration::from_secs(10);
    }
}

/// Sanitises a screen name for storage: strips `\r`/`\n`, replaces `\t` with
/// space, trims surrounding whitespace.  Emoji and other Unicode are preserved.
pub fn sanitize_screen_name(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .filter(|&c| c != '\r' && c != '\n')
        .map(|c| if c == '\t' { ' ' } else { c })
        .collect();
    cleaned.trim().to_owned()
}

/// Write-only helper for call sites that do not keep a long-lived cache.
///
/// Loads the shared file fresh, applies the update (with skip-if-unchanged
/// guard), and writes back atomically.
pub fn update_shared(pubkey: &[u8; 32], screen_name: &str) -> io::Result<()> {
    let mut cache = SharedKnownUsers::load_from_shared();
    cache.update(pubkey, screen_name)
}

/// One-shot load for short-lived CLI commands.
///
/// Returns a cloned snapshot of all entries.
pub fn load_shared_users() -> io::Result<Vec<KnownUser>> {
    let cache = SharedKnownUsers::load_from_shared();
    Ok(cache.entries.clone())
}

fn decode_pubkey(s: &str) -> Result<[u8; 32], hex::FromHexError> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_pubkey(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn test_load_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");
        let mut cache = SharedKnownUsers::new(path);
        assert!(cache.entries.is_empty());
        assert!(cache.get(&make_pubkey(1)).is_none());
    }

    #[test]
    fn test_write_and_read_back() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let pk1 = make_pubkey(1);
        let pk2 = make_pubkey(2);
        let pk3 = make_pubkey(3);

        {
            let mut cache = SharedKnownUsers::new(path.clone());
            cache.update(&pk1, "alice").unwrap();
            cache.update(&pk2, "bob").unwrap();
            cache.update(&pk3, "carol").unwrap();
        }

        let mut cache2 = SharedKnownUsers::new(path);
        assert_eq!(cache2.get(&pk1), Some("alice"));
        assert_eq!(cache2.get(&pk2), Some("bob"));
        assert_eq!(cache2.get(&pk3), Some("carol"));
    }

    #[test]
    fn test_dedup_last_wins() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");
        let pk = make_pubkey(10);

        let mut cache = SharedKnownUsers::new(path);
        cache.update(&pk, "alice").unwrap();
        cache.update(&pk, "alice2").unwrap();

        assert_eq!(cache.get(&pk), Some("alice2"));
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn test_skip_if_unchanged() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let pk = make_pubkey(11);
        let mut cache = SharedKnownUsers::new(path.clone());
        cache.update(&pk, "alice").unwrap();

        let mtime_before = fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(10));

        cache.update(&pk, "alice").unwrap();

        let mtime_after = fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(mtime_before, mtime_after);
    }

    #[test]
    fn test_skip_empty_name() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let pk = make_pubkey(12);
        let mut cache = SharedKnownUsers::new(path.clone());
        cache.update(&pk, "").unwrap();

        assert!(cache.get(&pk).is_none());
        assert!(!path.exists(), "file should not be created for empty name");
    }

    #[test]
    fn test_fifo_eviction() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let mut cache = SharedKnownUsers::new(path);

        for i in 0u32..=1000 {
            let mut pk = [0u8; 32];
            pk[0..4].copy_from_slice(&i.to_le_bytes());
            cache.update(&pk, &format!("user{}", i)).unwrap();
        }

        let mut pk0 = [0u8; 32];
        pk0[0..4].copy_from_slice(&0u32.to_le_bytes());
        assert!(cache.get(&pk0).is_none(), "pk0 should be evicted");

        let mut pk1 = [0u8; 32];
        pk1[0..4].copy_from_slice(&1u32.to_le_bytes());
        assert!(cache.get(&pk1).is_some(), "pk1 should be present");

        let mut pk1000 = [0u8; 32];
        pk1000[0..4].copy_from_slice(&1000u32.to_le_bytes());
        assert!(cache.get(&pk1000).is_some(), "pk1000 should be present");

        assert_eq!(cache.entries.len(), 1000);
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize_screen_name("hello\tworld\r\n"), "hello world");
        assert_eq!(sanitize_screen_name("alice 🎉"), "alice 🎉");
        assert_eq!(sanitize_screen_name("  spaces  "), "spaces");
        assert_eq!(sanitize_screen_name("\r\n"), "");
    }

    #[test]
    fn test_resolve_shortkey_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let pk = [0xabu8; 32];
        let mut cache = SharedKnownUsers::new(path);
        cache.update(&pk, "testuser").unwrap();

        let prefix = &hex::encode(pk)[..8];
        let result = cache.resolve_shortkey(prefix).unwrap();
        assert_eq!(result, Some(pk));
    }

    #[test]
    fn test_resolve_shortkey_ambiguous() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let mut pk1 = [0u8; 32];
        pk1[0] = 0xab;
        pk1[1] = 0x01;
        let mut pk2 = [0u8; 32];
        pk2[0] = 0xab;
        pk2[1] = 0x02;

        let mut cache = SharedKnownUsers::new(path);
        cache.update(&pk1, "user1").unwrap();
        cache.update(&pk2, "user2").unwrap();

        let result = cache.resolve_shortkey("ab");
        assert!(result.is_err(), "should be ambiguous");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("ambiguous"),
            "error message should mention ambiguous"
        );
    }

    #[test]
    fn test_resolve_shortkey_not_found() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");
        let mut cache = SharedKnownUsers::new(path);
        let result = cache.resolve_shortkey("deadbeef").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mtime_reload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("known_users");

        let pk1 = make_pubkey(1);
        let pk2 = make_pubkey(2);

        let mut cache1 = SharedKnownUsers::new(path.clone());
        cache1.update(&pk1, "alice").unwrap();

        // The reload goes by the file's modification time, and a file system
        // stamps a file from a clock that ticks every few milliseconds: up to
        // 10 ms apart on Linux and 15.6 ms on Windows. Two writes within one
        // tick carry the same time, so wait out a tick between them.
        std::thread::sleep(Duration::from_millis(20));

        let mut cache2 = SharedKnownUsers::new(path.clone());
        cache2.update(&pk2, "bob").unwrap();

        cache1.force_stale();

        assert_eq!(
            cache1.get(&pk2),
            Some("bob"),
            "cache1 should reload and find pk2 written by cache2"
        );
    }
}
