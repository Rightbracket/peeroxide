# Chat User Guide

The Peeroxide chat subsystem provides a set of CLI tools for managing identities, communicating in channels, and sending direct messages.

## Global Flags

These flags apply to all `peeroxide chat` subcommands.

| Flag | Description |
|---|---|
| `--debug` | Enable stderr debug event logs. |
| `--probe` | Enable internal trace probes (stdin, post, fetch_batch, etc) to stderr. |
| `--line-mode` | Force line-based I/O even when running on a TTY. |

In addition, every chat subcommand inherits the top-level `peeroxide` global flags documented in [init](../init/overview.md#global-cli-flags): `--config <FILE>`, `--no-default-config`, `--public`, `--no-public`, `--bootstrap <ADDR>` (repeatable), and `-v` / `--verbose`. These control config file loading, DHT bootstrap node selection, and tracing verbosity.

## Subcommand: join

Join a public or private channel for real-time conversation.

```bash
peeroxide chat join <channel> [flags]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--profile <name>` | `default` | Use a specific identity profile. |
| `--group <salt>` | | Set a private channel salt. Conflicts with `--keyfile`. |
| `--keyfile <path>` | | Read private channel salt from a file. Conflicts with `--group`. |
| `--no-nexus` | | Skip personal nexus (profile page) refresh and publication. |
| `--no-friends` | | Skip background friend nexus refresh. |
| `--read-only` | | Listen only; do not post messages or announce feeds. |
| `--stealth` | | Shorthand for `--no-nexus --read-only --no-friends`. |
| `--feed-lifetime <min>` | `60` | Rotation lifetime for your feed keypair. |
| `--batch-size <n>` | `16` | Maximum messages per publish batch. Values below `1` are clamped to `1`. |
| `--batch-wait-ms <ms>` | `50` | Maximum time to wait for a batch to fill before publishing. |
| `--stay-after-eof` | | Enter read-only mode on stdin EOF instead of exiting. |
| `--no-inbox` | | Disable background inbox monitoring. |
| `--inbox-poll-interval <s>` | `15` | How often to poll the inbox for new invites. Values below `1` are clamped to `1`. |

### Examples

Join a public channel:
```bash
peeroxide chat join general
```

Join a private channel with a secret group name:
```bash
peeroxide chat join development --group "super-secret-salt-2026"
```

## Subcommand: dm

Start an encrypted direct message session with another user.

```bash
peeroxide chat dm <recipient> [flags]
```

The `recipient` can be resolved using several formats (see Recipient Resolution below).

### Flags

`chat dm` supports most of the session flags from `join` (`--profile`, `--no-nexus`, `--no-friends`, `--read-only`, `--stealth`, `--feed-lifetime`, `--batch-size`, `--batch-wait-ms`, `--stay-after-eof`, `--no-inbox`, `--inbox-poll-interval`), plus a DM-only flag:

| Flag | Description |
|---|---|
| `--message <text>` | Initial lure text sent with the inbox invite. Ignored in stealth/read-only mode. |

`chat dm` does **not** accept `--group` / `--keyfile`; the channel key for a DM is derived deterministically from the two participants' identity public keys via `dm_channel_key`.

### Recipient Resolution

The recipient argument is resolved in the following order:
1. 64-character hex public key.
2. `@shortkey` (e.g., `@a1b2c3d4`).
3. `name@shortkey` (e.g., `alice@a1b2c3d4`).
4. 8-character shortkey (e.g., `a1b2c3d4`).
5. Friend alias (defined in your friends list).
6. Screen name from the `known_users` cache.

## Subcommand: inbox

Monitor your inbox for new invites without entering an interactive UI.

```bash
peeroxide chat inbox [flags]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--profile <name>` | `default` | Use a specific profile. |
| `--poll-interval <secs>` | `15` | Interval between inbox scans. Values below `1` are clamped to `1`. |
| `--no-nexus` | | Accepted for flag-surface parity with `chat join` / `chat dm`, but has no effect on `chat inbox` (which does not run a nexus publisher). |
| `--no-friends` | | Accepted for flag-surface parity with `chat join` / `chat dm`, but has no effect on `chat inbox` (which does not run a friend refresh task). |

## Profile Management: whoami and profiles

### whoami

Prints information about your current profile, including your public key, screen name, and nexus topic.

```bash
peeroxide chat whoami [--profile <name>]
```

| Flag | Default | Description |
|---|---|---|
| `--profile <name>` | `default` | Profile to inspect. |

### profiles

Manage multiple identities. Subcommands:

```bash
peeroxide chat profiles list
peeroxide chat profiles create <name> [--screen-name <name>]
peeroxide chat profiles delete <name>
```

| Subcommand | Args / flags | Description |
|---|---|---|
| `list` | — | List all available profiles. |
| `create <name>` | `--screen-name <name>` (optional) | Create a new profile. If `--screen-name` is omitted, a deterministic vendor name is generated and stored. |
| `delete <name>` | — | Delete a profile. The `default` profile cannot be deleted. |

## Friend Management: friends

Manage your list of trusted peers.

```bash
peeroxide chat friends [subcommand] [flags]
```

If no subcommand is given, `friends list` runs.

### Subcommands and flags

| Subcommand | Flags | Description |
|---|---|---|
| `list` | `--profile <name>` (default `default`) | Show all friends in the profile. |
| `add <key>` | `--alias <name>` (optional), `--profile <name>` (default `default`) | Add a new friend. Key resolution follows the same rules as DM recipients. If `--alias` is omitted, the alias auto-fills from the known-users cache or a vendor name. |
| `remove <key>` | `--profile <name>` (default `default`) | Remove a friend from the profile's list. |
| `refresh` | — | One-shot DHT update for friends' profile information. Does **not** accept a `--profile` flag — operates on the `default` profile only. |

## Personal Page: nexus

Manage your public profile information (Nexus) published on the DHT.

```bash
peeroxide chat nexus [flags]
```

If `--lookup` is supplied, the command short-circuits to lookup mode. Otherwise, `--set-name` and `--set-bio` are written to the profile first (both are applied in one run). After the setters, behavior is:

- `--publish`: perform a one-shot Nexus publish and exit.
- `--daemon`: enter the background loop (publish every 480 s, refresh **all** friends every 600 s).
- No `--publish` / `--daemon`, but at least one setter was supplied: exit without publishing.
- No flags at all (or only `--profile`): perform a one-shot Nexus publish and exit.

### Flags

| Flag | Default | Description |
|---|---|---|
| `--profile <name>` | `default` | Profile to publish from / inspect. |
| `--set-name <name>` | | Update your screen name (writes the profile's `name` file before publishing). |
| `--set-bio <text>` | | Update your biography (writes the profile's `bio` file before publishing). |
| `--publish` | | Publish your Nexus record to the DHT once. |
| `--daemon` | | Enter a background loop: publish your Nexus every 480s and refresh **all** friends every 600s. |
| `--lookup <pubkey>` | | Lookup and print the Nexus information for a specific public key. Short-circuits the rest. |

### Screen Name and Bio Files

A profile's screen name and bio live as plain UTF-8 text files inside the profile directory:

```text
~/.config/peeroxide/chat/profiles/<profile>/name
~/.config/peeroxide/chat/profiles/<profile>/bio
```

`~` is your home directory, or `$PEEROXIDE_HOME` when it is set; see [Profile Directory Layout](./reference.md#profile-directory-layout).

Both files are optional. If `name` is missing, a deterministic vendor name is generated from the profile's identity public key whenever a screen name is needed. If `bio` is missing or empty, the published Nexus record carries an empty bio.

You can populate them two ways:

- **`peeroxide chat nexus --set-name <text>`** / **`--set-bio <text>`** — writes the file with the supplied text (after trimming leading and trailing whitespace), then optionally publishes if `--publish` / `--daemon` is also given. Both setters can be supplied in one command.
- **Edit the file directly** with any editor. Multi-line bios are supported; the entire file content (after UTF-8 decoding) becomes the bio. The first line is treated specially by friends' clients — the [friends file](./reference.md#friends-file-schema) caches only the first line of each friend's bio for the `friends list` display, but the full bio is shown when a friend explicitly looks the identity up via `chat nexus --lookup`.

### Size Limit

The screen name and bio are serialized together into a single `NexusRecord` published to the DHT as a `mutable_put` value. The full record (3 framing bytes + `name` UTF-8 bytes + `bio` UTF-8 bytes) must fit within 1000 bytes, which is the `MAX_RECORD_SIZE` constant for chat records.

In practice: with a typical 10–40 byte screen name, the bio budget is roughly **950–990 UTF-8 bytes** (note: bytes, not characters — many non-ASCII characters take 2–4 bytes each).

If the combined size is too large, the publish step fails with:

```text
warning: nexus serialize failed: record too large: N bytes exceeds 1000 byte limit
```

The bio file is **still saved on disk** in this case — only the DHT publish is skipped. Shorten the bio (or screen name) and re-run with `--publish` to recover.

## Stealth Mode

The `--stealth` flag is supported by both `chat join` and `chat dm`. It is a shorthand for `--no-nexus --read-only --no-friends`, but the behavioral and threat-model implications are easier to reason about as a single concept.

### What `--stealth` suppresses

Passing `--stealth` is equivalent to enabling all three of:

- **`--read-only`** — your publisher is disabled entirely. No feed keypair is created, no message records are written via `immutable_put`, no `FeedRecord` is published via `mutable_put`, and no `announce` is sent on the channel or DM rendezvous topics. You become a pure observer of the channel.
- **`--no-nexus`** — your profile's Nexus record (screen name + bio) is not published. Other peers cannot resolve your identity public key to your screen name via the DHT, and you do not consume a `mutable_put` slot at your identity public key.
- **`--no-friends`** — the background friend-Nexus refresh task does not run. Your DHT does not issue periodic `mutable_get`s on each friend's identity public key, which would otherwise be observable to DHT nodes near those keys.

### What `--stealth` does NOT suppress

`--stealth` stops the publishing side of the protocol. Several other observable activities continue:

- **Channel discovery is still active.** Reading any channel requires `lookup`s on its discovery topics, followed by `mutable_get`s on each announcer's feed public key. Both operations remain visible to the DHT nodes serving them.
- **Inbox monitoring is independent of `--stealth`.** A stealth session still polls your profile's inbox topics every `--inbox-poll-interval` seconds (8 lookups per cycle by default — current + previous epoch, 4 buckets each). The wire-level lookup carries only the derived inbox topic, not your public key, so a passive DHT participant who does not already know your identity cannot recover it from these queries alone. However, an observer who **already knows your public key** can independently derive the same inbox topics and recognize the polling pattern, which lets them correlate the polling source IP with your identity. If that matches your threat model, also pass `--no-inbox`.
- **DM under stealth is receive-only.** The DM channel key is symmetric between the two parties, so you can decrypt incoming messages. But you never `announce` your DM feed, never publish a message, and never send the per-epoch nudge. Your DM peer has no way to know you are listening.
- **Network-level metadata is unchanged.** Every DHT operation goes out over UDP to peers who see your IP address. The Hyperswarm DHT has no traffic-mixing or onion-routing layer. If IP-level identifiability matters in your threat model — and especially if your public key is already known to an adversary — route peeroxide's traffic through a transport you trust to provide that property: typically a VPN that gives you a different egress IP, mixes your traffic with other clients, and does not retain per-flow logs.

### When `--stealth` is enough

It is sufficient when your only goal is to read a channel without contributing to its announce set or signaling your presence to other channel participants — for example, when you are using a fresh profile whose public key no observer has associated with you, and you want to listen first before deciding whether to post.

### When `--stealth` is not enough

It is **not** sufficient when your public key is already known to an adversary and IP-level correlation matters. In that case the chain `your public key → derived inbox / Nexus / announce topics → DHT lookups from your IP` is exploitable by a sufficiently positioned observer. Combine `--stealth --no-inbox` with a trustworthy anonymizing transport in front of the binary.

### Recipes

- Lurk on a channel without joining its announce set:

  ```bash
  peeroxide chat join general --stealth
  ```

- Same, plus suppress inbox polling:

  ```bash
  peeroxide chat join general --stealth --no-inbox
  ```

- Lurk under a burner profile so the activity is not tied to your main identity:

  ```bash
  peeroxide chat profiles create burner
  peeroxide chat join general --stealth --profile burner
  ```

## Interactive Usage

When running in a TTY, `join` and `dm` enter an interactive mode with a status bar and slash commands. See [Interactive TUI](./interactive-tui.md) for details.

In line mode (or when stdin is redirected), Peeroxide prints messages to stdout and notices to stderr. This is useful for piping chat into other tools.

### Message Display

Messages are formatted as:
`[TIMESTAMP] [DISPLAY_NAME]: CONTENT`

If a message arrives significantly after its timestamp, it is prefixed with `[late]`.

Display names are resolved with the following precedence:
1. Friend alias (e.g., `(Bob)`).
2. Friend's vendor name + screen name (e.g., `(Vendor) <Alice@a1b2c3d4>`).
3. Non-friend with a wire `screen_name` on the message (e.g., `<Alice@a1b2c3d4>`).
4. Non-friend without a wire `screen_name` but present in the shared `known_users` cache (e.g., `<Cached-Name@a1b2c3d4>`).
5. Vendor fallback (e.g., `<Fancy-Tiger@a1b2c3d4>`).

A `!` suffix on a name indicates the user is currently in a 300-second cooldown period after a name change.
