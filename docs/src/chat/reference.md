# Chat Reference

Technical reference tables for constants, flags, and filesystem layouts in the Peeroxide chat subsystem.

## Constants

| Constant | Value | Description |
|---|---|---|
| `MAX_RECORD_SIZE` | 1000 bytes | Maximum size of any single DHT record. |
| `MSG_FIXED_OVERHEAD`| 180 bytes | Combined size of envelope fields (excluding name/content). |
| `MAX_SCREEN_NAME_CONTENT`| 820 bytes | Max sum of screen name + content lengths. |
| `NONCE_SIZE` | 24 bytes | XSalsa20 nonce size. |
| `TAG_SIZE` | 16 bytes | Poly1305 tag size. |
| `CONTENT_TYPE_TEXT` | `0x01` | Record content type for text messages. |
| `INVITE_TYPE_DM` | `0x01` | Inbox invite type for direct messages. |
| `INVITE_TYPE_PRIVATE` | `0x02` | Inbox invite type for private channels. |
| `SUMMARY_EVICT_TRIGGER`| 20 | Messages in `FeedRecord` before summary eviction. |
| `SUMMARY_EVICT_COUNT` | 15 | Number of messages moved to summary on eviction. |
| `MUTABLE_PUT_RETRY_MS` | `[200, 500, 1000]`| Retry intervals for mutable DHT updates. |
| `ROTATION_CHECK_INTERVAL`| 30s | How often the publisher checks for feed rotation. |
| `MAX_SUMMARY_DEPTH` | 100 | Maximum number of summary blocks to walk back. |
| `FEED_EXPIRY_SECS` | 1200 | Time (20 min) after which a feed is considered stale. |
| `DISCOVERY_INTERVAL_SECS`| 8 | Frequency of reader discovery lookups. |
| `HISTORY_CAP` | 500 | TUI scrollback history limit (in memory). |
| `CTRL_C_ARM_WINDOW` | 2s | Double-press window for force-exit. |
| `DEDUP_RING_CAPACITY` | 1000 | Max hashes stored in the deduplication set. |
| `GAP_TIMEOUT` | 5s | Time before ChainGate force-releases out-of-order msgs. |
| `REFETCH_SCHEDULE_MS` | `[0, 500, 1500, 3000]`| Backoff intervals for missing hash refetching. |

## CLI Flags

### Global Flags
- `--debug`: Enable stderr debug logs.
- `--probe`: Enable stderr trace probes.
- `--line-mode`: Force line-based I/O.

### Subcommand: join
- `--profile <name>`: Profile to use (default: `default`).
- `--group <salt>`: Private channel salt (conflicts with `--keyfile`).
- `--keyfile <path>`: Private salt from file (conflicts with `--group`).
- `--no-nexus`: Skip nexus refresh/publish.
- `--no-friends`: Skip friend refresh.
- `--read-only`: Listen only mode.
- `--stealth`: Shorthand for `--no-nexus --read-only --no-friends`. Note this does **not** suppress inbox polling; see [Stealth Mode](./user-guide.md#stealth-mode) in the user guide for the full threat-model breakdown.
- `--feed-lifetime <min>`: Feed rotation interval (default: `60`).
- `--batch-size <n>`: Max messages per batch (default: `16`). Values below `1` are clamped to `1`.
- `--batch-wait-ms <ms>`: Batch window (default: `50`).
- `--stay-after-eof`: Enter listener mode on EOF.
- `--no-inbox`: Disable inbox monitor.
- `--inbox-poll-interval <s>`: Inbox scan frequency (default: `15`). Values below `1` are clamped to `1`.

### Subcommand: dm
Same session-flag surface as `join`, **except** `--group` and `--keyfile` are not accepted (the DM channel key is derived deterministically from the two participants' identity public keys). DM also adds:
- `--message <text>`: Initial inbox-invite lure text. Ignored in stealth/read-only mode.

### Subcommand: inbox
- `--profile <name>`: Profile to use (default: `default`).
- `--poll-interval <secs>`: Polling interval (default: `15`). Values below `1` are clamped to `1`.
- `--no-nexus`, `--no-friends`: Accepted for flag-surface parity with `chat join` / `chat dm` but are no-ops here (the inbox CLI does not run nexus / friend background tasks).

### Subcommand: whoami
- `--profile <name>`: Profile to inspect (default: `default`).

### Subcommand: profiles
- `profiles list`: no flags.
- `profiles create <name> [--screen-name <name>]`: optional initial screen name; otherwise a deterministic vendor name is generated.
- `profiles delete <name>`: rejects `default`.

### Subcommand: friends
- `friends list [--profile <name>]`: also the implicit default if no subcommand is given.
- `friends add <key> [--alias <name>] [--profile <name>]`: alias auto-fills from the known-users cache (or vendor name) when omitted.
- `friends remove <key> [--profile <name>]`.
- `friends refresh`: one-shot DHT refresh; does **not** accept `--profile` and operates on the `default` profile only.

### Subcommand: nexus
- `--profile <name>`, `--set-name <name>`, `--set-bio <text>`, `--publish`, `--lookup <pubkey-hex>`, `--daemon` (publish every 480 s, refresh **all** friends every 600 s).
- `--lookup` short-circuits to lookup mode.
- When at least one of `--set-name` / `--set-bio` is supplied and neither `--publish` nor `--daemon` is set, the setters are written to the profile and the command exits without publishing.
- When no flags (or only `--profile`) are supplied, the command still performs a single Nexus publish.

## Profile Directory Layout

Profiles are stored under `~/.config/peeroxide/chat/profiles/` (the chat subsystem uses the XDG-style `~/.config/peeroxide/chat/` root regardless of the platform-specific config dir used by `peeroxide`'s top-level config file).

`~` is the home directory: `$HOME` on Linux and macOS, and the user's profile folder (`%USERPROFILE%`) on Windows, where `HOME` is not consulted. Set `PEEROXIDE_HOME` to a directory to use it in place of `~` for both the profiles and the shared known-users file below; an empty value is ignored.

```text
~/.config/peeroxide/chat/profiles/<profile_name>/
├── seed           # 32-byte raw Ed25519 secret seed
├── name           # Optional UTF-8 screen name
├── bio            # Optional UTF-8 biography
└── friends        # Friend list (TSV)
```

### Friends File Schema
The `friends` file is a Tab-Separated Values (TSV) file:
`<64-hex-pubkey>\t<alias>\t<cached_name>\t<cached_bio_line>`

### Shared Known Users
Located at `~/.config/peeroxide/chat/known_users`.
- **Format**: TSV `<64-hex-pubkey>\t<screen_name>`
- **Capacity**: 1000 entries (FIFO).
- **Reloading**: 5s mtime-debounced reload.

## Name Resolution Precedence

`NameResolver` (`peeroxide-cli/src/cmd/chat/name_resolver.rs`) resolves a peer's identity public key in the following order:

1. **Friend Alias**: the friend's locally assigned alias, if non-empty.
2. **Known Screen Name**: the latest screen name for that pubkey in the shared `~/.config/peeroxide/chat/known_users` cache, if non-empty.
3. **Vendor Fallback**: a deterministic auto-generated name derived from the pubkey seed.

Note: the friends file's per-friend `cached_name` and `cached_bio_line` columns are populated by the nexus refresh task for display in the friends-list and friend nexus prints, but `NameResolver` itself does not consult them — it goes straight from friend alias to the shared known_users cache.

The two output formats:

- **`bar_label()`** — compact label used in the status bar:
  - friend alias source → bare alias (e.g. `bob`).
  - any other source → `name@shortkey` (e.g. `alice@a1b2c3d4`), where `shortkey` is the first 8 hex characters of the pubkey.
- **`formal()`** — uniform fully-qualified label: `name (shortkey)` (e.g. `alice (a1b2c3d4)`), regardless of source.
