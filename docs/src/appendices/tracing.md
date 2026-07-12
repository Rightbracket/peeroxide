# Tracing and Logging

Peeroxide uses the [`tracing`](https://docs.rs/tracing) crate for all
diagnostic output across its four crates (`libudx`, `peeroxide-dht`,
`peeroxide`, `peeroxide-cli`). This appendix documents the conventions
operators and developers can rely on when filtering, capturing, or
extending log output.

## Target conventions

Every tracing call has a *target* — a string used by the
[`EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
to decide whether to emit the event. Peeroxide uses two kinds of
targets:

**1. Module-path targets (default).** Most calls inherit their target
from the Rust module path: `peeroxide_dht::holepuncher`,
`peeroxide_dht::hyperdht`, `libudx::native::stream`, and so on. These
are the natural granularity for developers debugging a specific
subsystem. EnvFilter directives prefix-match, so
`peeroxide_dht::holepuncher=trace` enables that one module and its
children.

**2. Reserved `peeroxide::_events::*` lifecycle targets.** A curated set
of high-signal operator-facing events use a stable, hand-picked target
under `peeroxide::_events::`. These are not Rust modules — they are
fixed labels that survive refactors. Examples:

```text
peeroxide::_events::swarm::started
peeroxide::_events::dht::bootstrapped
peeroxide::_events::peer::connected
peeroxide::_events::peer::connect_failed
peeroxide::_events::holepunch::probe_received
peeroxide::_events::holepunch::passive_reflected
peeroxide::_events::holepunch::nat_settled
peeroxide::_events::holepunch::final_punch_sent
peeroxide::_events::holepunch::connected
peeroxide::_events::holepunch::aborted
peeroxide::_events::holepunch::failed_no_verified_addr
```

Operators tail these to get a clean lifecycle stream without developer
noise:

```sh
RUST_LOG=peeroxide::_events=info peeroxide cp send ./file
```

## Level discipline

| Level | Used for | Default visibility |
|---|---|---|
| `error` | Fatal conditions; the operation cannot proceed | always |
| `warn` | Recoverable anomalies, validation failures, retries | always |
| `info` | Lifecycle events (`peeroxide::_events::*`) + startup | `-v` and above |
| `debug` | Per-connection / per-round state transitions | `-vv` and above |
| `trace` | Per-packet, per-loop iteration | only with explicit `RUST_LOG` |

Anything that fires more than once per significant operation lives at
`debug` or below. Per-packet paths live at `trace`. The `info` level is
reserved for the `_events::*` subtree plus a small handful of true
startup events.

## CLI verbosity

The `peeroxide` CLI exposes three verbosity levels via the `-v` flag,
each composing a default `EnvFilter`:

| Flag | Default filter | What you see |
|---|---|---|
| _(none)_ | `warn,peeroxide::_events=info` | Warnings + lifecycle events |
| `-v` | `peeroxide=info,peeroxide_dht=info,peeroxide::_events=info,warn` | Info-level developer events across both swarm and DHT crates |
| `-vv` | `peeroxide=debug,peeroxide_dht=debug,libudx=debug,peeroxide::_events=info,info` | Full debug stream across all peeroxide crates |

The `RUST_LOG` environment variable always overrides the default. Any
EnvFilter directive syntax is supported:

```sh
RUST_LOG=peeroxide_dht::holepuncher=trace peeroxide cp send ./file
RUST_LOG=peeroxide::_events=info,libudx=warn peeroxide cp recv <topic> -
RUST_LOG=peeroxide_dht::hyperdht=trace,peeroxide_dht::io=debug peeroxide node
```

## Subsystem map

The 8 natural subsystems and the targets that feed them:

| Subsystem | Module path | Event subtree |
|---|---|---|
| holepunch | `peeroxide_dht::holepuncher` | `peeroxide::_events::holepunch::*` |
| nat | `peeroxide_dht::nat` | _(none currently)_ |
| socket_pool | `peeroxide_dht::socket_pool` | _(none currently)_ |
| relay | `peeroxide_dht::blind_relay` | _(none currently)_ |
| discovery | `peeroxide_dht::query`, `peeroxide::peer_discovery` | _(none currently)_ |
| swarm | `peeroxide::swarm` | `peeroxide::_events::swarm::*`, `peeroxide::_events::peer::*` |
| dht_rpc | `peeroxide_dht::rpc`, `peeroxide_dht::io` | `peeroxide::_events::dht::*` |
| udx | `libudx::native::*` | _(none currently)_ |

New `_events::*` labels should be added sparingly, only when an event
represents an operator-visible lifecycle transition (something a
production operator would want in a clean default-level log).

## Adding a new lifecycle event

When wiring a new high-signal event, use an explicit target:

```rust
tracing::info!(
    target: "peeroxide::_events::holepunch::nat_settled",
    round,
    "NAT settled + verified remote, transitioning to final punch round"
);
```

When the same site also wants developer-level detail at debug level,
emit two separate calls or include enough structured fields in the
single `info!` so it serves both audiences (the latter is preferred).

## Anti-patterns

- **`eprintln!` for telemetry.** Bypasses level filtering and structured
  fields. Always use a tracing macro.
- **Emitting at `info` from a per-packet path.** Demote to `debug` or
  `trace`; the `info` level is reserved for lifecycle events.
- **Inventing many ad-hoc targets.** Stick to module-path defaults
  unless the call belongs to a curated `_events::*` lifecycle category.
- **Putting expensive computation outside the tracing macro.** The
  `tracing` macros short-circuit on the level filter before evaluating
  field expressions, so inline `format!()` / `.collect()` calls inside
  the macro are gated. The same code as a `let` outside the macro
  always runs.

## Reference

- [`tracing` crate](https://docs.rs/tracing)
- [`tracing-subscriber::EnvFilter`](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
- Inspired by [iroh's `iroh::_events::*` pattern](https://github.com/n0-computer/iroh)
