# Contributing to Peeroxide
This project is a Rust implementation of the Hyperswarm stack, focusing on wire compatibility with the Node.js reference network.

## Crate Stack

| Crate | Description | Published |
|---|---|---|
| `libudx` | Reliable UDP transport with BBR congestion control | crates.io |
| `peeroxide-dht` | Kademlia DHT, Noise handshakes, hole-punching, relay | crates.io |
| `peeroxide` | High-level swarm and topic-based discovery | crates.io |
| `peeroxide-cli` | CLI toolkit: lookup, announce, ping, cp, dd | crates.io (binary) |

## Development Requirements

- **Rust 2024 edition** (MSRV 1.85)
- **Node.js 20+** and **npm** (required for interop tests)

## Building

```bash
cargo build --workspace
```

## Testing

Two suites must pass before merging:

```bash
# Unit tests, integration tests, Node.js local interop
cargo test --workspace

# Live public HyperDHT network tests (requires internet)
cargo test -p peeroxide-cli --test live_commands -- --ignored
```

Before running tests, install the Node.js dependencies once:

```bash
npm ci --prefix tests/node
```

## Code Style

```bash
# Must pass with zero warnings
cargo clippy --workspace --all-targets -- -D warnings

# Docs must build clean
cargo doc --workspace --no-deps
```

## API Stability

These three library crates are published at `>=1.0` and have external users. 

**Any change to an existing public API signature is a breaking change and requires explicit maintainer approval before implementation.** 

When in doubt, add a new function rather than changing an existing one.

For *new* public types/functions/modules, see [`VISIBILITY_POLICY.md`](./VISIBILITY_POLICY.md) for the visibility (`pub` vs `pub(crate)`) and `#[non_exhaustive]` rubric — it exists precisely so future additive changes (new fields, new variants) don't become breaking changes later.

## PR Checklist

### Before opening a PR

- [ ] Both test suites pass locally (see Testing above)
- [ ] Clippy clean
- [ ] No public API breaking changes without maintainer approval
- [ ] New/changed public API in `libudx`, `peeroxide-dht`, or `peeroxide` reviewed against `VISIBILITY_POLICY.md` (visibility + `#[non_exhaustive]` per the type-role taxonomy)
- [ ] CHANGELOG updated for each affected crate
- [ ] Version bumps are correct per semver (patch / minor / major)
- [ ] New public API items have `///` doc comments
- [ ] No task-artifact files committed (planning docs, prompt files, checklists — see AGENTS.md)

### Before merging

- [ ] GitHub Actions CI is green (all checks pass)
- [ ] PR has been reviewed and approved

## Release Process

Releases are managed by [release-plz](https://release-plz.eplr.dev/). On merge to `main`, release-plz opens a release PR with version bumps and changelog entries. Merging that PR publishes to crates.io automatically. Do not run `cargo publish` manually.

For `peeroxide-cli` specifically, each `peeroxide-cli-v*` tag pushed by release-plz also triggers `.github/workflows/binary-release.yml`, which builds the `peeroxide` binary for four targets (macOS arm64, macOS x86_64, Linux x86_64, Linux aarch64) and attaches `.tar.gz` archives plus `.sha256` sidecars to the GitHub Release. The [Homebrew tap](https://github.com/Rightbracket/homebrew-peeroxide) polls those Release assets on its own schedule and opens same-repo bump PRs — no cross-repo automation runs from this side. Cutting a new `peeroxide-cli-v*` tag is the only step required to ship a new Homebrew release.

## Licensing

Peeroxide is dual-licensed under MIT and Apache 2.0. By contributing, you agree your contributions will be licensed under the same terms.
