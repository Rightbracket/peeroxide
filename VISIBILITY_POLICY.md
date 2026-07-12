# Visibility Policy

How to decide whether a new type, function, or module in `libudx`, `peeroxide-dht`, or `peeroxide` should be `pub`, `pub(crate)`, or `#[non_exhaustive]`.

`peeroxide-cli` is a binary consumer of these crates; it adapts to the library API, not the other way around.

## Why this exists

Routine bug fixes and feature gap-fills should not trigger SemVer-breaking diffs. They do whenever something is `pub` that shouldn't be, or whenever a public Config/Result/Event lacks `#[non_exhaustive]` and we want to add a field. This policy is the rubric we apply to keep the published API surface intentional and additively extensible.

## Goals

1. Identify the complete public API surface — every reachable item.
2. Keep non-surface entities `pub(crate)` so internal changes don't trigger SemVer churn.
3. Enable additive evolution of the surviving surface (new fields, new variants, new methods) without breaking the SemVer contract.
4. Stay compatible with the broader Hyperswarm / Hypercore / Holepunch ecosystem (see "Reference ecosystem pins" below).

## Non-goals

- Preserving struct-literal constructability for downstream users.
- Backward compatibility for items we explicitly demote.
- Wire-protocol changes (envelopes' Rust shape is downstream of the wire format, not the API contract).

## The two-axis decision

For every public-ish item, make two independent decisions.

### Axis 1 — visibility (`pub` vs `pub(crate)`)

In order of precedence:

1. Is it reached from outside its defining crate today (workspace-internal cross-crate use counts)? → `pub`.
2. Is it on the **pin list** below? → `pub`.
3. Is it returned by, or required as a parameter of, a public function in the same crate? → `pub` (cascade reachability).
4. Is it a wire-format envelope used cross-crate within the workspace? → `pub`.
5. Otherwise → `pub(crate)`.

### Axis 2 — `#[non_exhaustive]` (only if Axis 1 = `pub`)

1. The role default from the **type-role taxonomy** below applies unless explicitly carved out.
2. Default is **apply**. The burden of proof is on exemptions.

**Constructor rule**: every `#[non_exhaustive]` type that users construct (Configs, Options, Params, HandlerReply-shaped types) must ship a `::new(...)` / builder / `Default` impl in the same commit. Otherwise consumers can't construct it.

## Type-role taxonomy

Classify every public-ish item into exactly one role. The role drives both defaults.

| Role | Visibility default | `#[non_exhaustive]` default | Examples |
|---|---|---|---|
| **Handle** — opaque internal-state-with-methods, factory-constructed, never struct-literal'd | `pub` | no (adds nothing; consumers can't struct-literal a private-field type anyway) | `HyperDhtHandle`, `SwarmHandle`, `DhtHandle`, `Io`, `CongestionWindow`, `Holepuncher`, `SecretStream<T>`, `Push`, `Pull`, `SecurePayload`, `SocketPool`, `NoiseWrap`, `BlindRelayClient`, `Persistent`, `Router`, `UdxRuntime`, `UdxStream`, `UdxSocket`, `UdxAsyncStream` |
| **Config / Options / Params** — user-constructed for input | `pub` | **yes** (forward-compat for new knobs) | `SwarmConfig`, `JoinOpts`, `HyperDhtConfig`, `ServerConfig`, `ConnectOpts`, `IoConfig`, `PersistentConfig`, `WireCounters` |
| **Event / Result / Reply** — produced by the library, matched/destructured by the user | `pub` | **yes** (forward-compat for new fields/variants) | `LookupResult`, `AnnounceResult`, `ConnectResult`, `ImmutablePutResult`, `MutablePutResult`, `MutableGetResult`, `ServerEvent`, `IoEvent`, `TimeoutEvent`, `HolepunchEvent`, `ChannelEvent`, `PairResponse`, `HandshakeResult`, `PeerConnection`, `SwarmConnection`, `PeerInfo` |
| **Error** — matched by the user | `pub` | **yes** (forward-compat for new variants) | `HyperDhtError`, `SwarmError`, `UdxError`, `SecretStreamError`, `SecretstreamError`, `RelayError`, `ProtomuxError`, `IoError`, `NoiseError`, `EncodingError` |
| **Primitive / value type** — small, widely-constructed, semantics stable | `pub` | no (would block legitimate value construction) | `KeyPair`, `PeerAddr`, `noise::Keypair`, `Datagram`, `Priority` |
| **Wire-format envelope** — Rust shape mirrors a serialized protocol message | `pub` if cross-crate, else `pub(crate)` | **no** (struct-literal construction IS the protocol implementation) | All of `messages::*`, `hyperdht_messages::*`, `protomux::{ControlFrame, BatchItem, DecodedFrame}`, `blind_relay::{PairMessage, UnpairMessage}`, `noise::{Handshake, HandshakeIK}`, `compact_encoding::State` |
| **Public free function** | `pub` only if part of public contract; else `pub(crate)` | n/a (functions don't take `#[non_exhaustive]` — use signature discipline) | `discovery_key`, `crypto::{hash, hash_batch, sign_detached, verify_detached, namespace}` |
| **Trait** | case-by-case; use sealed-trait pattern (private supertrait or `Sealed` marker) if no out-of-crate impls are intended | n/a | — |
| **Internal helper** — never reached from outside the defining crate | `pub(crate)` | n/a | NAT state machine, routing-table internals, encoder primitives in `compact_encoding`, demoted `blind_relay::encode_*`/`decode_*` helpers |

## Reference ecosystem pins

These types MUST remain `pub`. They are the surface a future Rust port of any Hyperswarm-family project (hyperbeam, `@hyperswarm/rpc`, hypercore replication, etc.) would need to import. Verified against ~150 holepunchto-org projects.

**peeroxide**:
- `spawn()`, `discovery_key()`
- `SwarmConfig`, `JoinOpts`, `SwarmHandle`, `SwarmConnection`
- `SwarmHandle::{join, leave, dht, key_pair}`

**peeroxide-dht**:
- `spawn` (in `hyperdht` and `rpc`)
- `HyperDhtConfig`, `ServerConfig`, `ConnectOpts`
- `KeyPair` (with `from_seed` / `generate`)
- `HyperDhtHandle`, `PeerConnection`, `Holepuncher`
- `HyperDhtHandle::{lookup, announce, connect, connect_with_options, connect_with_nodes, connect_to, find_peer, mutable_put, mutable_get, immutable_put, immutable_get}`
- `LookupResult`, `AnnounceResult`, `ConnectResult`, `ImmutablePutResult`, `MutablePutResult`, `MutableGetResult`, `ServerEvent`
- `secret_stream::SecretStream`, `noise_wrap::NoiseWrap`, `protomux::*`
- `crypto::{discovery_key, hash, hash_batch, sign_detached, verify_detached, namespace}`

**libudx**:
- `UdxRuntime`, `UdxSocket`, `UdxStream`, `UdxAsyncStream`
- `UdxRuntime::{create_socket, create_stream}`
- `UdxSocket::{bind, send_to, recv_start, close}`
- `Header` and the wire flag constants (`FLAG_DATA`, `FLAG_END`, etc.)

## When the rubric is ambiguous

- **Adding a new public type?** Apply the role taxonomy. Default to `#[non_exhaustive]` for Config / Result / Event / Error roles. Skip for Handles, Primitives, and wire-format envelopes.
- **Adding a new module?** Default `pub(crate) mod`. Promote to `pub mod` only if it's a documented advanced-use surface — and only if it gets real module-level docs in the same commit.
- **Tempted to use `#[doc(hidden)] pub mod`?** Don't. It's a footgun: the module is publicly reachable but absent from rustdoc, so reviewers miss leaks. Either it's `pub mod` (with real docs) or it's `pub(crate) mod`.
- **Removing an item?** Breaking change. Requires explicit human approval per AGENTS.md HARD STOP.
- **Adding a field to a non-`#[non_exhaustive]` struct?** Breaking change. Requires approval. (Reconsider whether the struct should have been `#[non_exhaustive]` in the first place.)
- **Changing an existing public signature?** Breaking change. Requires approval.

## How to extend this policy

If a new use case doesn't fit cleanly into an existing role, prefer either:

1. **Carve-out**: prove it's an exception to a clear role and document the carve-out inline in the relevant role row above.
2. **New role**: introduce a new row in the type-role taxonomy with explicit defaults.

Don't ad-hoc decide visibility per-type without updating this document.

## Auditing the current state

When the question "is the public surface still consistent with this policy?" comes up:

1. Generate the public-surface inventory:
   ```bash
   RUSTC_BOOTSTRAP=1 cargo rustdoc -p <crate> -- \
       -Z unstable-options --output-format json --document-hidden-items
   ```
   (The `--document-hidden-items` flag is required to surface `#[doc(hidden)] pub mod` content if any has crept back in.)
2. For each public `struct` / `enum`, confirm it's covered by the role taxonomy above (either `#[non_exhaustive]` is applied per its role's default, or its role exempts it).
3. For each `pub mod`, confirm it has real module-level documentation. No `#[doc(hidden)] pub mod`.
4. For cross-crate reachability questions, use qualified-path grep (`use peeroxide_dht::<mod>`, `peeroxide_dht::<mod>::`). Do NOT rely on name-only graph queries — common method names like `destroy`, `add`, `update` create false positives.

## Hard constraints (from AGENTS.md, restated here for convenience)

- **No `git push`** without explicit user direction.
- **API breaking change HARD STOP**: visibility demotions (`pub` → `pub(crate)`) and removal of items are breaking changes. They require explicit human approval — they are not part of "routine maintenance."
- **MSRV**: Rust 1.85 (2024 edition).
