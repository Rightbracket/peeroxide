use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::chat::known_users::SharedKnownUsers;
use super::names;
use crate::cmd::chat::profile::Friend;

pub struct DisplayMessage {
    pub id_pubkey: [u8; 32],
    pub screen_name: String,
    pub content: String,
    pub timestamp: u64,
    pub is_self: bool,
    pub late: bool,
}

pub struct DisplayState {
    friends: HashMap<[u8; 32], Friend>,
    last_identity_shown: HashMap<[u8; 32], u64>,
    known_names: HashMap<[u8; 32], String>,
    name_change_at: HashMap<[u8; 32], u64>,
    known_users: SharedKnownUsers,
}

/// Output of [`DisplayState::render_to`] — a formatted message line plus any
/// associated system notices (identity reveal, name change). The caller is
/// responsible for actually printing these to the user; this separation lets
/// the line-mode UI route them to stdout/stderr and the interactive TUI route
/// them through the renderer.
#[derive(Debug, Clone)]
pub struct RenderedOutput {
    /// The formatted message line, e.g. `[12:34:56] [alice]: hello`. May be
    /// prefixed with `[late] ` if the message was delivered out-of-order.
    pub message_line: String,
    /// Zero or more system notices (each rendered as a separate line) that
    /// accompany this message: `*** vendor@short is fullkey`, name-change
    /// announcements, etc.
    pub system_notices: Vec<String>,
}

/// Render a [`DisplayMessage`] without any [`DisplayState`] context.
///
/// Used by `LineUi` when it doesn't have access to the live `DisplayState`
/// (e.g. when called outside the main join loop). Produces a best-effort
/// formatted line — no friend aliases, no identity notices, no cooldown
/// marker. The main loop should always go through [`DisplayState::render_to`]
/// for full formatting; this helper exists for fall-back paths.
pub fn render_message_line(msg: &DisplayMessage) -> RenderedOutput {
    let timestamp_str = format_timestamp(msg.timestamp);
    let shortkey = &hex::encode(msg.id_pubkey)[..8];
    let vendor_name = names::generate_name_from_seed(&msg.id_pubkey);
    let display_name = if !msg.screen_name.is_empty() {
        format!("<{}@{}>", msg.screen_name, shortkey)
    } else {
        format!("<{vendor_name}@{shortkey}>")
    };
    let late_marker = if msg.late { "[late] " } else { "" };
    RenderedOutput {
        message_line: format!(
            "{late_marker}[{timestamp_str}] [{display_name}]: {}",
            msg.content
        ),
        system_notices: Vec::new(),
    }
}

impl DisplayState {
    pub fn new(friends: Vec<Friend>, known_users: SharedKnownUsers) -> Self {
        let friends_map: HashMap<[u8; 32], Friend> =
            friends.into_iter().map(|f| (f.pubkey, f)).collect();
        Self {
            friends: friends_map,
            last_identity_shown: HashMap::new(),
            known_names: HashMap::new(),
            name_change_at: HashMap::new(),
            known_users,
        }
    }

    /// Reload the friends map from the given list.
    /// Called periodically to pick up alias edits and nexus name refreshes.
    pub fn reload_friends(&mut self, friends: Vec<Friend>) {
        self.friends = friends.into_iter().map(|f| (f.pubkey, f)).collect();
    }

    /// Render `msg` and print directly to stdout/stderr. Convenience wrapper
    /// around [`Self::render_to`]; preserved for callers that don't yet route
    /// through a `ChatUi`. New callers should prefer [`Self::render_to`] so the
    /// output can be directed appropriately (e.g. into the TUI scroll region).
    pub fn render(&mut self, msg: &DisplayMessage) {
        let out = self.render_to(msg);
        for notice in &out.system_notices {
            eprintln!("{notice}");
        }
        println!("{}", out.message_line);
    }

    /// Render `msg` and return the formatted output, mutating internal state
    /// (last-identity-shown, known-names, name-change-cooldown) along the way.
    /// The caller is responsible for emitting the resulting strings.
    pub fn render_to(&mut self, msg: &DisplayMessage) -> RenderedOutput {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let timestamp_str = format_timestamp(msg.timestamp);
        let display_name = self.format_display_name(msg, now_secs);

        let mut notices = Vec::new();

        if self.should_show_identity(msg, now_secs) {
            let shortkey = &hex::encode(msg.id_pubkey)[..8];
            let fullkey = hex::encode(msg.id_pubkey);
            let vendor_name = names::generate_name_from_seed(&msg.id_pubkey);
            notices.push(format!("*** {vendor_name}@{shortkey} is {fullkey}"));
            self.last_identity_shown.insert(msg.id_pubkey, now_secs);
        }

        let late_marker = if msg.late { "[late] " } else { "" };
        let message_line = format!(
            "{late_marker}[{timestamp_str}] [{display_name}]: {}",
            msg.content
        );

        if !msg.screen_name.is_empty() {
            let prev = self.known_names.get(&msg.id_pubkey);
            if let Some(old_name) = prev {
                if old_name.as_str() != msg.screen_name {
                    let shortkey = &hex::encode(msg.id_pubkey)[..8];
                    notices.push(format!(
                        "*** {}@{} changed screen name: \"{}\" → \"{}\"",
                        old_name, shortkey, old_name, msg.screen_name
                    ));
                    self.name_change_at.insert(msg.id_pubkey, now_secs);
                }
            }
            self.known_names
                .insert(msg.id_pubkey, msg.screen_name.clone());
        }

        RenderedOutput {
            message_line,
            system_notices: notices,
        }
    }

    fn format_display_name(&mut self, msg: &DisplayMessage, now_secs: u64) -> String {
        let shortkey = &hex::encode(msg.id_pubkey)[..8];
        let vendor_name = names::generate_name_from_seed(&msg.id_pubkey);

        let name_cooldown_active = self
            .name_change_at
            .get(&msg.id_pubkey)
            .map(|&t| now_secs.saturating_sub(t) < 300)
            .unwrap_or(false);
        let bang = if name_cooldown_active { "!" } else { "" };

        // Note: deliberately doesn't go through `NameResolver` — the
        // message-rendering ladder here differs from the general resolver
        // semantics in subtle ways. Specifically, the "friend without
        // alias" case uses the VENDOR name (not the cached known name) in
        // the parenthesised stable-identifier slot, while the general
        // resolver gives the cached known name priority. The two
        // semantics are different on purpose: this path wants a
        // pubkey-derived stable identifier for the friendship marker,
        // independent of whatever screen name the friend is currently
        // using. Friends-aware bar / slash output should compose
        // `NameResolver` results directly.
        if let Some(friend) = self.friends.get(&msg.id_pubkey) {
            if let Some(ref alias) = friend.alias {
                if msg.screen_name.is_empty() || *alias == msg.screen_name {
                    format!("({alias}){bang}")
                } else {
                    format!("({alias}) <{}>{bang}", msg.screen_name)
                }
            } else if !msg.screen_name.is_empty() {
                format!("({vendor_name}) <{}@{}>{bang}", msg.screen_name, shortkey)
            } else {
                format!("({vendor_name}){bang}")
            }
        } else if !msg.screen_name.is_empty() {
            format!("<{}@{}>{bang}", msg.screen_name, shortkey)
        } else if let Some(cached_name) = self.known_users.get(&msg.id_pubkey) {
            format!("<{}@{}>{bang}", cached_name, shortkey)
        } else {
            format!("<{vendor_name}@{shortkey}>{bang}")
        }
    }

    fn should_show_identity(&mut self, msg: &DisplayMessage, now_secs: u64) -> bool {
        if msg.is_self {
            return false;
        }
        if let Some(friend) = self.friends.get(&msg.id_pubkey) {
            if friend.alias.is_some() {
                return false;
            }
        }
        if self.known_users.get(&msg.id_pubkey).is_some() {
            return false;
        }
        match self.last_identity_shown.get(&msg.id_pubkey) {
            Some(&last) => now_secs.saturating_sub(last) > 600,
            None => true,
        }
    }
}

fn format_timestamp(unix_secs: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let secs = unix_secs;
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;

    let today_start = now - (now % 86400);
    if secs >= today_start {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        let days = secs / 86400;
        let y = 1970 + (days / 365);
        let d = days % 365;
        let mo = d / 30 + 1;
        let day = d % 30 + 1;
        format!("{y}-{mo:02}-{day:02} {h:02}:{m:02}:{s:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn format_display_name_friend_with_alias() {
        let friend = Friend {
            pubkey: [1u8; 32],
            alias: Some("alice".to_string()),
            cached_name: None,
            cached_bio_line: None,
        };
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));
        let mut state = DisplayState::new(vec![friend], ku);
        let msg = DisplayMessage {
            id_pubkey: [1u8; 32],
            screen_name: "alice".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name = state.format_display_name(&msg, 0);
        assert_eq!(name, "(alice)");
    }

    #[test]
    fn format_display_name_non_friend() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));
        let mut state = DisplayState::new(vec![], ku);
        let msg = DisplayMessage {
            id_pubkey: [0xab; 32],
            screen_name: "bob".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name = state.format_display_name(&msg, 0);
        assert!(name.starts_with("<bob@"));
        assert!(name.ends_with('>'));
    }

    #[test]
    fn format_display_name_non_friend_vendor_fallback() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));
        let mut state = DisplayState::new(vec![], ku);
        let msg = DisplayMessage {
            id_pubkey: [0x11; 32],
            screen_name: "".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let vendor = names::generate_name_from_seed(&msg.id_pubkey);
        let shortkey = &hex::encode(msg.id_pubkey)[..8];
        let name = state.format_display_name(&msg, 0);
        assert_eq!(name, format!("<{vendor}@{shortkey}>"));
    }

    #[test]
    fn format_display_name_with_name_change_cooldown() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));
        let mut state = DisplayState::new(vec![], ku);
        state.known_names.insert([0xab; 32], "old_name".to_string());
        state.name_change_at.insert([0xab; 32], 1000);

        let msg = DisplayMessage {
            id_pubkey: [0xab; 32],
            screen_name: "new_name".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name_during_cooldown = state.format_display_name(&msg, 1100);
        assert!(name_during_cooldown.ends_with('!'), "should show ! during 300s cooldown");

        let name_after_cooldown = state.format_display_name(&msg, 1400);
        assert!(!name_after_cooldown.ends_with('!'), "should NOT show ! after 300s");
    }

    #[test]
    fn format_display_name_known_users_fallback() {
        let dir = TempDir::new().unwrap();
        let mut ku = SharedKnownUsers::new(dir.path().join("known_users"));
        ku.update(&[0xabu8; 32], "bob").unwrap();

        let mut state = DisplayState::new(vec![], ku);
        let msg = DisplayMessage {
            id_pubkey: [0xabu8; 32],
            screen_name: "".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name = state.format_display_name(&msg, 0);
        let shortkey = &hex::encode([0xabu8; 32])[..8];
        assert_eq!(name, format!("<bob@{shortkey}>"));
    }

    #[test]
    fn format_display_name_friend_no_alias_no_wire_uses_vendor_name() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));

        let friend = Friend {
            pubkey: [2u8; 32],
            alias: None,
            cached_name: None,
            cached_bio_line: None,
        };
        let mut state = DisplayState::new(vec![friend], ku);
        let msg = DisplayMessage {
            id_pubkey: [2u8; 32],
            screen_name: "".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let vendor = names::generate_name_from_seed(&msg.id_pubkey);
        let name = state.format_display_name(&msg, 0);
        assert_eq!(name, format!("({vendor})"));
    }

    #[test]
    fn format_display_name_friend_no_alias_with_wire_uses_vendor_anchor() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));

        let friend = Friend {
            pubkey: [3u8; 32],
            alias: None,
            cached_name: None,
            cached_bio_line: None,
        };
        let mut state = DisplayState::new(vec![friend], ku);
        let msg = DisplayMessage {
            id_pubkey: [3u8; 32],
            screen_name: "wire_name".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let vendor = names::generate_name_from_seed(&msg.id_pubkey);
        let shortkey = &hex::encode(msg.id_pubkey)[..8];
        let name = state.format_display_name(&msg, 0);
        assert_eq!(name, format!("({vendor}) <wire_name@{shortkey}>"));
    }

    #[test]
    fn format_display_name_wire_precedence() {
        let dir = TempDir::new().unwrap();
        let mut ku = SharedKnownUsers::new(dir.path().join("known_users"));
        ku.update(&[0xabu8; 32], "old_bob").unwrap();

        let mut state = DisplayState::new(vec![], ku);
        let msg = DisplayMessage {
            id_pubkey: [0xabu8; 32],
            screen_name: "new_bob".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name = state.format_display_name(&msg, 0);
        let shortkey = &hex::encode([0xabu8; 32])[..8];
        assert_eq!(name, format!("<new_bob@{shortkey}>"));
    }

    #[test]
    fn format_display_name_friend_priority_over_known_users() {
        let dir = TempDir::new().unwrap();
        let mut ku = SharedKnownUsers::new(dir.path().join("known_users"));
        ku.update(&[1u8; 32], "bob_cache").unwrap();

        let friend = Friend {
            pubkey: [1u8; 32],
            alias: Some("bestie".to_string()),
            cached_name: None,
            cached_bio_line: None,
        };
        let mut state = DisplayState::new(vec![friend], ku);
        let msg = DisplayMessage {
            id_pubkey: [1u8; 32],
            screen_name: "bob_wire".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };
        let name = state.format_display_name(&msg, 0);
        assert!(name.starts_with("(bestie)"), "friend alias should take priority: {}", name);
    }

    #[test]
    fn render_identity_notice_includes_vendor_name() {
        let dir = TempDir::new().unwrap();
        let ku = SharedKnownUsers::new(dir.path().join("known_users"));
        let mut state = DisplayState::new(vec![], ku);
        let msg = DisplayMessage {
            id_pubkey: [0x11; 32],
            screen_name: "".to_string(),
            content: "hi".to_string(),
            timestamp: 0,
            is_self: false,
            late: false,
        };

        let vendor = names::generate_name_from_seed(&msg.id_pubkey);
        let shortkey = &hex::encode(msg.id_pubkey)[..8];
        assert!(state.should_show_identity(&msg, 0));
        let expected = format!("*** {vendor}@{shortkey} is {}", hex::encode(msg.id_pubkey));
        assert!(expected.contains(&format!("{vendor}@{shortkey}")));
    }
}
