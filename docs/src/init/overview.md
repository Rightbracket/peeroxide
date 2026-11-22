# init

The `peeroxide init` command handles environment setup by generating configuration files or installing man pages. It provides a non-interactive way to bootstrap your local environment before running other peeroxide subcommands.

## Command Modes

The `init` command operates in two mutually exclusive modes.

### Config Mode (Default)

In its default mode, `init` writes a fresh `config.toml` file to your configuration directory. It includes a `[network]` table and commented examples of available fields.

- **First run**: Creates parent directories and writes the file.
- **Rerun without flags**: Prints a message stating the config already exists and exits with code 0.
- **Rerun with `--force`**: Overwrites the existing file entirely.
- **Rerun with `--update`**: Merges new `network.public` or `network.bootstrap` values into the existing file while preserving comments and formatting.

### Man-page Mode

When invoked with `--man-pages`, the command skips configuration and instead generates and installs system man pages.

## CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--force` | `bool` | `false` | Overwrites an existing config file. Conflicts with `--update`. |
| `--update` | `bool` | `false` | Updates specific fields in an existing config. Requires `--public` or `--bootstrap`. Conflicts with `--force`. |
| `--public` | `bool` | `false` | Sets `network.public = true`. Adds default public HyperDHT bootstrap nodes. |
| `--bootstrap <ADDR>` | `Vec<String>` | `[]` | Sets `network.bootstrap`. Repeatable. In update mode, this replaces the entire bootstrap list. |
| `--man-pages [PATH]` | `PathBuf` | `/usr/local/share/man/` | Installs generated man pages. Writes to `PATH/man1/`. |

### Flag Conflicts

- `--man-pages` cannot be used with `--force`, `--update`, `--public`, or `--bootstrap`.
- `--force` and `--update` are mutually exclusive.
- `--update` requires at least one field to change (`--public` or `--bootstrap`).

## Global CLI Flags

The `peeroxide` binary defines several global flags that apply to most subcommands. `peeroxide init` itself only consumes two of them:

- `--config <FILE>` — used as the target write path (and as the source path for `--update`).
- `-v` / `--verbose` — controls tracing verbosity.

The remaining global flags listed below are accepted by the parser but **do not affect** `init` (which has its own local `--public` and `--bootstrap` flags applied to the generated/updated config). They take effect on subcommands that do DHT work (lookup, announce, ping, cp, dd, chat, node).

| Flag | Type | Description |
|---|---|---|
| `-v`, `--verbose` | `u8` count | Increases logging level. `-v` for info, `-vv` for debug. `RUST_LOG` overrides this. (Used by init.) |
| `--config <FILE>` | `String` | Specifies a custom path for the config file. For `init`, this is the write target. |
| `--no-default-config` | `bool` | Skips loading the default configuration file. (Not consumed by `init`.) |
| `--public` | `bool` | Includes default public HyperDHT bootstrap nodes. (Not consumed by `init`; `init` has its own local `--public` for the generated config.) |
| `--no-public` | `bool` | Excludes default public HyperDHT bootstrap nodes. Conflicts with `--public`. (Not consumed by `init`.) |
| `--bootstrap <ADDR>` | `Vec<String>` | Adds a bootstrap node address (`host:port`). Repeatable. (Not consumed by `init`; `init` has its own local `--bootstrap` for the generated config.) |

## Config File Locations

### Target Path Precedence (init)

When `init` determines where to write the config file, it follows this order:

1. Path provided via `--config <FILE>`
2. Environment variable `$PEEROXIDE_CONFIG`
3. `$XDG_CONFIG_HOME/peeroxide/config.toml`
4. `~/.config/peeroxide/config.toml`
5. Default fallback `.config/peeroxide/config.toml`

### Runtime Load Precedence

When running commands, peeroxide loads configuration in this order:

1. Path provided via `--config <FILE>`
2. Environment variable `$PEEROXIDE_CONFIG`
3. `$XDG_CONFIG_HOME/peeroxide/config.toml`
4. Platform-specific config directory (e.g., `Library/Application Support` on macOS)
5. `~/.config/peeroxide/config.toml`

## Config Schema

The config file uses the TOML format.

### [network]

| Field | Type | Default | Description |
|---|---|---|---|
| `public` | `bool` | `None` | If `true`, adds public bootstrap nodes. If `false`, removes them. |
| `bootstrap` | `Vec<String>` | `None` | List of `host:port` or `ip:port` bootstrap addresses. |

### [node]

| Field | Type | Default | Description |
|---|---|---|---|
| `port` | `u16` | `49737` | The local port to bind for DHT operations. |
| `host` | `String` | `"0.0.0.0"` | The local address to bind. |
| `public_host` | `String` | unset | Public IPv4 address other nodes reach this node on. Required for the first node of a DHT bound to a wildcard address. See [node](../node/overview.md#running-the-first-node-of-a-dht). |
| `stats_interval` | `u64` | `60` | Interval in seconds for logging node statistics. |
| `max_records` | `usize` | `65536` | Maximum number of DHT records to store. |
| `max_lru_size` | `usize` | `65536` | Maximum size of the LRU cache for routing. |
| `max_per_key` | `usize` | `20` | Maximum records allowed per key. |
| `max_record_age` | `u64` | `1200` | Maximum age in seconds for DHT records. |
| `max_lru_age` | `u64` | `1200` | Maximum age in seconds for LRU entries. |

### [announce] and [cp]

These tables are currently empty and reserved for future use.

## Bootstrap Resolution

Peeroxide resolves the bootstrap-node list in two stages: a base-list selection from CLI/config (CLI overrides), then a public-default adjustment.

**Stage 1 — pick the base list (in `peeroxide-cli/src/config.rs`):**

- If `--bootstrap <ADDR>` was supplied (one or more times) on the command line, use **only** those CLI bootstraps for the base list. The config file's `network.bootstrap` is **ignored** in this case.
- Otherwise, use the `network.bootstrap` list from the config file (if any).
- If neither source supplied bootstraps, the base list starts empty.

**Stage 2 — apply the public-default adjustment (in `peeroxide-cli/src/cmd/mod.rs::resolve_bootstrap`):**

1. If `public=true` (via flag or config), add the default public HyperDHT bootstrap nodes to the base list.
2. If the list is still empty after step 1, automatically add the default public HyperDHT bootstrap nodes (so a fresh install with no config and no flags still connects).
3. If `public=false` (via `--no-public` or config), remove all default public bootstrap nodes from the list.

This ensures the node is never isolated unless specifically requested by combining `--no-public` with an empty bootstrap list. The `--no-public` flag replaces the legacy `--firewalled` flag behavior.

Note: this resolution happens at runtime in subcommands that do DHT work (lookup, announce, ping, cp, dd, chat, node). `peeroxide init` uses its own local `--public` and `--bootstrap` flags to populate the generated/updated config file; the base-list selection and public-default adjustment do not run during `init`.

## Man-page Installation

When running `peeroxide init --man-pages`, the tool:

1. Identifies the target directory (default `/usr/local/share/man/`).
2. Ensures the `man1/` subdirectory exists.
3. Cleans up any existing `peeroxide*.1` files in that directory.
4. Writes fresh man pages for the main command and all subcommands.

## Exit Codes

- `0`: Success.
- `1`: Runtime error, file system error, or TOML parsing error.
- `2`: Usage error or invalid arguments provided to the CLI.
