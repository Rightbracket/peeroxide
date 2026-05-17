# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Updated `peeroxide_dht::io::WireCounters` construction sites in the deaddrop progress reporter to use the new `WireCounters::from_counters(bytes_sent, bytes_received)` constructor, replacing struct-literal construction after `WireCounters` gained `#[non_exhaustive]` in `peeroxide-dht`.

## [0.2.1](https://github.com/Rightbracket/peeroxide/compare/peeroxide-cli-v0.2.0...peeroxide-cli-v0.2.1) - 2026-05-14

### Other

- CI release automation fix

## [0.2.0] - 2026-05-13

### Added

- `peeroxide chat` — pseudonymous end-to-end-encrypted P2P chat over the DHT. Subcommands: `join`, `dm`, `inbox`, `whoami`, `profiles {list, create, delete}`, `friends {list, add, remove, refresh}`, `nexus`. Public channels by name, private channels via `--group <salt>` or `--keyfile`; DMs derived from both participants' identity pubkeys plus an ECDH-augmented message key. Interactive TUI with a pinned status bar, multi-line input, slash commands, and a background inbox monitor; line mode is selected automatically when either stdio side is piped. Full reference and protocol spec: `docs/src/chat/`.
- `peeroxide init` — config bootstrap (default mode) and man-page installation (`--man-pages [PATH]`). New flags: `--force`, `--update`, `--public`, `--bootstrap <ADDR>` (repeatable), `--man-pages [PATH]`.
- Tree-indexed `dd` protocol v2 shipped under wire byte `0x02`. Receiver fetches the index tree breadth-first in parallel. Soft depth cap of 4 supports up to ~27 GB at the default 998-byte chunk size.
- `dd put` and `dd get` now display a progress bar by default when stderr is a TTY (indicatif-driven). New flags:
  - `--no-progress` — suppress the progress bar
  - `--json` — emit structured `start`/`progress`/`result`/`ack`/`done` events as JSON Lines on stdout (schema documented in `docs/src/dd/operations.md`)

  `dd get --json` requires `--output FILE`; without it, flag parsing fails with a clear error (stdout would otherwise conflict with the JSON event stream).
- `dd` progress display includes cumulative DHT wire bytes (sent / received) via the new `peeroxide-dht` 1.3.0 `HyperDhtHandle::wire_stats()` / `wire_counters()` API (additive — see `peeroxide-dht/CHANGELOG.md` for the full new symbol set). Shown in the bar, periodic log, and JSON events.
- New global `-v` / `--verbose` count flag (warn / info / debug; `RUST_LOG` overrides).
- New global `--no-public` flag that excludes the default public HyperDHT bootstrap nodes.
- Per-`mutable_put` timeout of 30 seconds in the `dd` v2 sender. Stall watchdog kicks AIMD concurrency off the floor if no put resolves for 30 seconds.
- `peeroxide-init(1)` and `peeroxide-chat(1)` man pages.
- New mdBook chapters: `docs/src/chat/` (overview, user-guide, interactive-tui, wire-format, protocol, reference), `docs/src/init/overview.md`, `docs/src/concepts/dht-primitives.md` (covers `immutable_put`/`mutable_put`/`announce`/`lookup`, rendezvous pattern, TTL, and 1002-byte size budget).
- `docs/ascii_art.txt` banner asset embedded into `peeroxide --version` via clap `long_version`, into the crate README, and into the mdBook introduction. `-V` continues to print the bare semver for scripts.
- Prebuilt `peeroxide` binaries distributed via the [`rightbracket/peeroxide` Homebrew tap](https://github.com/Rightbracket/homebrew-peeroxide) for macOS (universal Apple Silicon + Intel), Linux x86_64 (glibc), and Linux aarch64 (glibc). No Rust toolchain required; `brew install rightbracket/peeroxide/peeroxide` auto-taps and installs.

### Changed

- Renamed `deaddrop` command to `dd` (short for "Dead Drop").
- Renamed `deaddrop leave` subcommand to `dd put`.
- Renamed `deaddrop pickup` subcommand to `dd get`.
- `dd put` defaults to v2 protocol; pass `--v1` to force the legacy single-chain protocol.
- `dd get` auto-dispatches between v1 (`0x01`) and v2 (`0x02`) based on the root record's first byte.
- Bootstrap resolution: CLI `--bootstrap` overrides the config file's `network.bootstrap` (not additive). After base-list selection, `--public` adds defaults, an empty list auto-fills with defaults, and `--no-public` removes defaults.
- The legacy per-chunk status output emitted to stderr during the initial publish/fetch phase (`published chunk N/M`, `fetched data N/M`, `reassembled X bytes`, etc.) is replaced by the new progress UI (bar, periodic log, or JSON events). Scripts that parsed this output should migrate to `--json` mode.
  **Preserved:** Refresh, ack (`[ack] pickup #N detected`), "ack sent", "done", "written to PATH", and other lifecycle messages on stderr are not affected and continue to print as before.
- In `--json` mode, all structured events (including the pickup key for `dd put`) go to stdout (per `docs/AGENTS.md` convention). The pickup key is delivered as `{"type":"result","pickup_key":"..."}` rather than a bare stdout line. JSON consumers should parse `{"type":"result"}` events.
- Consolidated `peeroxide chat` man pages into a single `peeroxide-chat(1)` covering every subcommand and group. Total man-page count is 9 (one per top-level command).
- All man pages have refreshed long-about prose, examples, exit status, and see-also entries.
- Rewritten `docs/src/dd/` chapters covering both v1 and v2.

### Fixed

- Shared sticky `Shutdown` primitive across `dd put`. First SIGINT/SIGTERM cancels gracefully; second exits with code 130.
- `dd` v2 need-list watcher now publishes only attempted-and-failed chunk ranges, not all missing positions.

### Removed

- `peeroxide config init` — replaced by `peeroxide init`.
- The legacy `--generate-man <DIR>` flag — replaced by `peeroxide init --man-pages [PATH]`.
- The legacy `--firewalled` global flag — replaced by `--no-public`.

## [0.1.0] - 2026-04-29

### Added

- `peeroxide node` — run a long-lived DHT bootstrap node with configurable port, host, stats interval, and record limits
- `peeroxide lookup` — query the DHT for peers on a topic with `--json` NDJSON output and `--with-data` support
- `peeroxide announce` — announce presence on a topic with `--ping` echo responder, `--data` attachment, `--duration` limit, and `--seed` keypair
- `peeroxide ping` — diagnose reachability by address, public key, or topic with `--connect` full handshake, `--count`/`--interval` probes, and `--json` output
- `peeroxide cp send` / `peeroxide cp recv` — streaming file transfer over encrypted P2P connections with atomic writes, progress reporting, `--keep-alive`, `--force`, and stdin/stdout support
- `peeroxide deaddrop leave` / `peeroxide deaddrop pickup` — anonymous store-and-forward messaging via DHT mutable records with passphrase encryption, CRC32c integrity, and chunked payloads
- `peeroxide config init` — generate a default TOML configuration file
- TOML configuration system with `~/.config/peeroxide/config.toml`, `$PEEROXIDE_CONFIG` env var, and CLI flag overrides
- `--generate-man <DIR>` flag to produce roff man pages with rich descriptions, examples, exit status, and cross-references
- AIMD adaptive concurrency for deaddrop publish (responds to commit timeouts)
- SIGINT/SIGTERM graceful shutdown across all long-running commands
- Comprehensive test suite: 24 unit tests, 9 local integration tests, 4 live network tests

### Known Limitations

- Performance has not been optimized yet, particularly deaddrop throughput. This is an initial release to get something tangible out for people to play with — expect significant speed improvements in future versions.
