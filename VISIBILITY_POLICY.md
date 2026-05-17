# Visibility Policy & API Surface Audit

**Status**: 🟡 DESIGN IN PROGRESS

**Supersedes**: `VISIBILITY_REFORM_PLAN.md` (the prior, narrower attempt — kept on disk for history but not the source of truth going forward). Builds on the reconnaissance captured in `HANDOFF_VISIBILITY_AUDIT.md`.

**Scope**: All three library crates — `libudx`, `peeroxide-dht`, `peeroxide`. The `peeroxide-cli` binary is a consumer; it constrains but does not contribute to the published API surface.

---

## 0. Design decisions log (locked)

Three design questions were resolved with the user (2026-05-17):

| # | Question | Resolution | Affected sections |
|---|---|---|---|
| Q1 | Reference-ecosystem scope | **Comprehensive holepunchto sweep** — every end-user/middleware app in https://github.com/orgs/holepunchto that transitively depends on `hyperswarm`/`hyperdht`/`hyperswarm-secret-stream`/`dht-rpc`/`protomux`/`udx-native`. (Broader than the initial "core three" proposal.) | §6, §11 |
| Q2 | Constructor wave scope | **Same wave**: every `#[non_exhaustive]` add on a user-constructed type includes a `::new()` / builder in the same commit. | §12 Phase 2 |
| Q3 | Data pipeline | **rustdoc-JSON with `RUSTC_BOOTSTRAP=1`** as primary; semantic reachability via `vogon_poetry` graph queries. (Confirmed working in `audit_reachability_*.tsv`.) | §9, §11 |

Pending follow-up identified during execution: rustdoc-JSON pass needs the `--document-hidden-items` flag to cover the 8 `#[doc(hidden)] pub mod` modules; this is captured in `VISIBILITY_REFORM_PROMPT.md` §5.4 as a required pre-Phase-1 step.

---

## 1. Problem Statement

PR #10 (`ba7ad8a`) applied `#[non_exhaustive]` based on a name-pattern heuristic (`*Config` / `*Result` / `*Event` / `*Error`). The heuristic was incomplete in two directions:

- **Missed items inside `#[doc(hidden)] pub mod`** — `doc(hidden)` only suppresses rustdoc; the modules and the types within them remain publicly reachable. PR #10 mentally treated those modules as internal and skipped them. Five types were retroactively patched in `dea08a5` and `fd9f95d`.
- **Did not address visibility itself** — types that should never have been `pub` to begin with were left `pub`, so unrelated bug fixes have repeatedly produced SemVer-breaking diffs.

The downstream symptom: routine bug fixes and feature gap-fills keep tripping breaking-change tripwires because the published API surface is larger and less curated than intended.

## 2. Goals

1. **Identify the complete public API surface** of each library crate — every type, function, trait, module, field, and variant currently reachable from outside the crate.
2. **Demote non-surface entities** to `pub(crate)` (or private) so internal changes can land without producing SemVer churn.
3. **Enable additive evolution** of the surviving surface — new fields on structs, new variants on enums, new methods on traits — without breaking the SemVer contract.
4. **Pressure-test the surface against the Hyperswarm/Hypercore reference ecosystem** so we don't accidentally lock out legitimate downstream Rust ports (e.g. a future `hyperbeam-rs`, `hyperswarm-rpc-rs`, `hypercore-rs`).
5. **Produce a written rubric** that future PRs and reviewers can apply consistently, replacing the implicit name-pattern heuristic from PR #10.

## 3. Non-Goals

- **Preserving struct-literal constructability for downstream users.** The user has accepted this cost: legitimate constructors / builders will replace direct construction where needed. The rubric defaults to applying `#[non_exhaustive]`; the burden of proof sits on *exemptions*, not applications.
- **Backward compatibility for entities we demote.** We have no external users today (per user statement). A minor version bump suffices.
- **Wire-protocol changes.** Wire-format envelopes (`*Message`, `*Payload`, `*Info` in `hyperdht_messages.rs`, `messages.rs`, `protomux::*`, `blind_relay::*`, `libudx::native::header::*`) have their bytes-on-the-wire as the contract. Their Rust-side struct shape is downstream of the protocol, not the API.
- **A 2.0 release.** Per user direction: minor bump only.

## 4. Constraints

- **MSRV 1.85** (Rust 2024 edition).
- **No `git push`** — all work is local.
- **No breaking changes to `libudx`/`peeroxide-dht`/`peeroxide` public APIs without explicit human approval.** (AGENTS.md "HARD STOP" rule.) The visibility *demotions* in this audit ARE breaking changes by definition and are pre-approved as the explicit purpose of the work; new additive constructors / methods are not.
- **Wave gating** — each commit must pass `cargo build --workspace`, `cargo test --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings`. Final tip must additionally pass `cargo test -p peeroxide-cli --test live_commands -- --ignored`.
- **`peeroxide-cli` is a binary consumer** — pressures from it (e.g. needing shared sockets) are solved inside the CLI or via additive library API, never by mutating existing library signatures.

## 5. Design Decisions

### Decision: Scope of this document
**Choice**: Methodology + policy rubric + populated per-entity classification table + reference-ecosystem consumer mapping (option C from design discussion).
**Rationale**: PR #10's failure mode was lack of grounding. Building the reference-ecosystem consumer model into the same document we use for classification ensures the rubric stays connected to concrete downstream needs and prevents a future "we made it `pub(crate)` and then needed it back" cycle.
**Alternatives considered**: (A) rubric only and (B) rubric + classification. Rejected because both still leave the "would a Rust hyperbeam need this?" question implicit.

### Decision: Document location and git tracking
**Choice**: `VISIBILITY_POLICY.md` at workspace root, *committed* to git (explicit override of the AGENTS.md task-artifact rule).
**Rationale**: This is policy documentation, not a one-off plan. It needs to survive across sessions and be referenced from PR reviews and CHANGELOG entries. The previous `VISIBILITY_REFORM_PLAN.md` / `HANDOFF_*.md` style of uncommitted scratch did not produce durable shared understanding.

### Decision: Treatment of prior decisions in HANDOFF_VISIBILITY_AUDIT.md
**Choice**: Re-audit fresh under the new rubric. Where a prior decision flips, investigate the flip to determine whether the prior decision was an error or the new rubric is wrong.
**Rationale**: The prior reconnaissance was thorough on what it covered, but it operated under PR #10's narrower philosophy. Mechanically re-applying it would propagate any unstated assumptions. Flip cases are the most informative — they're where the new rubric earns its keep or reveals a flaw.

### Decision: Default for `#[non_exhaustive]` on public types
**Choice**: Default is `#[non_exhaustive]` for all public types EXCEPT explicit role-based carve-outs (see §7).
**Rationale**: User explicitly stated the primary concern is "fully identify the surface area and ensure additive changes don't break the API contract," not preserving user constructability. Inverting the default — apply by default, document exemptions — makes the policy easier to enforce in review.
**Alternatives considered**: Per-entity judgment (PR #10's approach — failed). Apply only to name-pattern matches (PR #10's heuristic — failed).

## 6. Reference Ecosystem Mapping

*(Source: `audit_ecosystem_mapping.md` (scratch, uncommitted). Comprehensive holepunchto org sweep completed 2026-05-17 — 150 consumer projects scanned, 102 mapped (Tier 1 + Tier 2), 48 catalogued only (Tier 3).)*

### Scope

The full `holepunchto` GitHub organization was enumerated and filtered to consumers of the networking layer:

| Tier | Definition | Count |
|---|---|---:|
| 1 | Direct protocol consumers (imports `hyperswarm` / `hyperdht` / `hyperswarm-secret-stream` / `dht-rpc` / `protomux` / `udx-native` / `@hyperswarm/*` directly) | 52 |
| 2 | Middleware (imports `hypercore` / `hyperdrive` / `corestore` / `autobase` and exposes networking through them) | 50 |
| 3 | Apps / tools (use Tier 2 packages or higher-level Holepunch products like Pear/Keet) | 48 |
| **Total in scope** | | **150** |

### Headline findings

1. **`peeroxide-dht` is a first-class consumer-facing layer, not an internal helper.** Many Tier 1 consumers (hyperbeam, autobase-discovery, autopass, blind-pairing, blind-peer, etc.) sit on raw `hyperdht` / `hyperswarm` primitives. This validates the "promote, not hide" stance for the `#[doc(hidden)] pub mod` declarations in peeroxide-dht.
2. **`protomux` is required by multiple ecosystem layers**, not just `@hyperswarm/rpc` and `hypercore`. The promotion call from the narrow mapping is reinforced.
3. **`hyperbeam` uses `hyperdht` directly, not `hyperswarm`** (confirmed earlier; broader sweep adds many more such direct-DHT consumers).
4. **`hypercore`-family consumers continue to treat `secret_stream`, `noise_wrap`, and `protomux` as public contract points** — not internal details.

### Must-stay-public pins (deduplicated across all 102 Tier 1 + Tier 2 projects)

**peeroxide** (8 pins):
- `spawn()`
- `SwarmConfig`
- `SwarmHandle`
- `JoinOpts`
- `SwarmConnection`
- `discovery_key()`
- `SwarmHandle::join()`
- `SwarmHandle::leave()`

**peeroxide-dht** (22 pins):
- `HyperDhtConfig`
- `HyperDhtHandle`
- `KeyPair` (+ `from_seed`, `generate`)
- `ServerConfig` (firewall callback)
- `LookupResult`
- `AnnounceResult`
- `ImmutablePutResult`
- `MutablePutResult`
- `MutableGetResult`
- `ConnectOpts`
- `ConnectResult`
- `ServerEvent`
- `PeerConnection`
- `Holepuncher`
- `HyperDhtHandle::lookup()`
- `HyperDhtHandle::announce()`
- `HyperDhtHandle::connect*()` family
- `secret_stream::SecretStream`
- `noise_wrap::NoiseWrap`
- `protomux::*` (Channel, Mux, frame types, control surface)
- `crypto::discovery_key()` (+ `hash`, `sign_detached`, `verify_detached`)
- Stream keepalive parameter (currently a capability gap)

**libudx** (11 pins):
- `UdxRuntime`
- `UdxSocket`
- `UdxStream`
- `UdxAsyncStream`
- `Header`
- `UdxRuntime::create_socket()` / `create_stream()`
- `UdxSocket::bind()` / `send_to()` / `recv_start()` / `close()`
- (Stream keepalive — capability gap)

### Capability gaps (not visibility issues — captured for future work)

- Public `protomux` surface (in scope for visibility — `protomux::*` is being promoted).
- `@hyperswarm/rpc`-style framing layer.
- Client/server RPC request-response semantics.
- Muxer attachment via stream `userData`.
- Publicly-controllable stream keepalive (`setKeepAlive(ms)`).
- `AnnounceResult` shape (some consumers iterate, some destructure — needs concrete shape decision in implementation).

### Flips vs prior reconnaissance (§9.3 categorization)

Three flips identified in the narrow mapping STAND after the broader sweep; **no new flips emerged**:

- **`protomux`**: PROMOTE (drop `#[doc(hidden)]`, add docs). Category (b) — new information from ecosystem mapping. Reinforced by multiple Tier 1/2 consumers across the broader sweep.
- **`noise_wrap`**: KEEP PUBLIC. Category (b) — newly identified pin. Reinforced.
- **`secret_stream`**: KEEP PUBLIC. Confirmed by hypercore replication + reinforced by broader sweep.

Existing demotion candidates (`query`, `router`, `nat`, `routing_table`) are NOT contradicted by the broader sweep — none of the 102 mapped consumers reach into them.

### Headline findings

1. **`hyperbeam` uses `hyperdht` directly, not `hyperswarm`.** Implication: `peeroxide-dht` is more user-facing than the original "internal layer" framing suggested. Direct DHT consumers exist in the reference ecosystem.
2. **`protomux` is a capability cornerstone for `@hyperswarm/rpc` and `hypercore`.** Currently it's a `#[doc(hidden)] pub mod` in peeroxide-dht. The mapping requires us to **promote, not hide** it.
3. **`hypercore` treats `setKeepAlive(5000)` as part of the replication contract.** This means `libudx`/`peeroxide-dht` need a publicly-controllable connection keepalive parameter. Not a visibility issue per se, but a feature gap the audit surfaces.

### Must-stay-public pins (deduplicated)

**peeroxide** (1 pin):
- `swarm::discovery_key()`

**peeroxide-dht** (6 pin groups):
- `KeyPair::from_seed` + `KeyPair::generate`
- `HyperDhtConfig` + top-level `spawn`
- `ServerConfig` (firewall callback support)
- `HyperDhtHandle::connect` / `connect_to`
- `PeerConnection`
- `secret_stream::SecretStream` + `noise_wrap::NoiseWrap`
- `protomux::*` (NEW pin — not on prior reconnaissance list)

**libudx** (3 pins):
- `UdxRuntime`
- `UdxSocket`
- `UdxAsyncStream` (duplex semantics + keepalive)

### Capability gaps revealed (out of audit scope — captured for future work)

- Public `protomux` surface (in scope for *visibility*; the *feature gaps* below are not).
- `@hyperswarm/rpc`-style framing layer.
- Client/server RPC request-response semantics.
- Muxer attachment via stream `userData`.
- Publicly-controllable stream keepalive.

### Impact on prior reconnaissance (flip cases per §9.3)

- **`protomux`**: Prior reconnaissance left it as `#[doc(hidden)] pub mod` (silently public). New mapping says **promote** (drop doc-hidden, add docs). **FLIP — category (b)**: new information from the reference-ecosystem mapping.
- **`noise_wrap`**: Prior reconnaissance did not call it out. New mapping says **keep public**. Flip is category (b) — newly identified pin.
- **`query` / `router`**: Prior recommendation to demote with `pub use` re-exports stands. Ecosystem mapping does not contradict it. No flip.
- **`nat` / `routing_table`**: Prior recommendation to demote stands. Ecosystem mapping does not contradict it. No flip.

## 7. Type-Role Taxonomy

*(DRAFT — see §10 Open Questions before finalizing.)*

Every public-ish entity gets classified into exactly one role. The role determines the default for both axes (visibility, `#[non_exhaustive]`). Per-entity decisions document any departure from the role default.

| Role | Default visibility | Default `#[non_exhaustive]` | Examples |
|---|---|---|---|
| **Handle** — opaque, factory-constructed, never struct-literal'd | `pub` | no (adds nothing; never constructed by users) | `HyperDhtHandle`, `SwarmHandle`, `DhtHandle`, `UdxRuntime` |
| **Config / Options / Params** — user-constructed for input | `pub` | **yes** (forward-compat for new knobs) | `SwarmConfig`, `JoinOpts`, `RequestParams` |
| **Event / Result / Reply** — produced by the library, matched by the user | `pub` | **yes** (forward-compat for new fields/variants) | `HolepunchEvent`, `QueryReply` |
| **Error** — produced by the library, matched by the user | `pub` | **yes** (forward-compat for new variants) | `SecretstreamError` |
| **Primitive / value type** — small, widely constructed, semantics stable | `pub` | no (would break legitimate value construction) | `KeyPair`, `Topic` (if present) |
| **Wire-format envelope** — Rust shape mirrors a serialized protocol message | `pub` (if cross-crate-used) else `pub(crate)` | **no** (struct-literal construction is part of the protocol implementation) | `HolepunchInfo`, `NoisePayload`, `Ipv4Peer`, anything in `messages.rs`/`hyperdht_messages.rs`/`protomux::*`/`libudx::native::header::*` |
| **Trait** — extension point | case-by-case | n/a (use sealed-trait pattern if no out-of-crate impls intended) | TBD per trait |
| **Internal helper** — never reached from outside the crate | `pub(crate)` | n/a | NAT internals, routing table, internal state machines |
| **Public free function** — bare `fn`, not a method | `pub` only if part of the public API contract; otherwise `pub(crate)` | n/a (functions don't take `#[non_exhaustive]`) | `compact_encoding::encode_uint32`, `crypto::hash`, `crypto::discovery_key` |

**Carve-out justification rule**: any entity that does NOT take the role default must have a one-sentence explanation recorded in the per-entity classification table.

## 8. The Two-Axis Rubric

For every entity, the audit produces two independent decisions:

### Axis 1 — Visibility (`pub` vs `pub(crate)` vs `pub(super)` vs private)

Decision criteria, in order of precedence:

1. **Is it reached from outside its defining crate today?** (Workspace-internal cross-crate use counts.) If yes → `pub`.
2. **Is it explicitly listed as a reference-ecosystem consumer-facing entity** (§6)? If yes → `pub`.
3. **Is it returned by, or required as a parameter to, a public function in the same crate?** If yes → `pub` (cascade reachability).
4. **Is it a wire-format envelope used cross-crate within our workspace?** If yes → `pub`. (Same outcome as 1, called out explicitly to prevent regressions on the 76-attribute failure mode.)
5. Otherwise → `pub(crate)` (or narrower).

### Axis 2 — `#[non_exhaustive]` (on the entity, *if* it survives Axis 1 as `pub`)

Decision criteria:

1. **Role default applies** (see §7) unless explicitly carved out.
2. **Carve-out: Wire-format envelope** — never `#[non_exhaustive]`. Adding it breaks our own struct-literal construction in protocol-implementation code.
3. **Carve-out: Handle** — `#[non_exhaustive]` adds nothing because users never struct-literal these. Leave off to keep documentation cleaner.
4. **Carve-out: Primitive value type** — `#[non_exhaustive]` would break legitimate user value construction without a real forward-compat benefit (these types are tightly defined).
5. Otherwise → apply `#[non_exhaustive]`.

**Note on traits**: Traits don't take `#[non_exhaustive]`. Their forward-compat tool is the sealed-trait pattern (private supertrait or `Sealed` marker). Trait policy is a §10 open question.

**Note on public free functions**: Functions don't have a `#[non_exhaustive]` analogue. Forward-compat for free functions comes from signature discipline:

- Avoid exhaustive-enum parameters (use `#[non_exhaustive]` enums instead).
- Avoid struct-literal-constructible parameter types (use `#[non_exhaustive]` structs with builders).
- Avoid returning concrete enum types that consumers might exhaustively match.

If a free function takes only primitives / byte slices and returns primitives / byte slices, it's signature-stable by construction. The `compact_encoding::encode_*` / `decode_*` family is exactly this shape, which is part of why so many were marked `pub` historically without obvious harm — though most are still candidates for `pub(crate)` demotion because they're not part of any consumer-facing contract.

## 8.5 Inventory snapshot (2026-05-17)

Initial machine inventory completed (see §9 for methodology limitations). Headline numbers across the three library crates:

| Crate | Public items | `#[non_exhaustive]` | `#[doc(hidden)] pub mod` |
|---|---|---|---|
| libudx | 57 | 2 | 0 |
| peeroxide-dht | 585 | 45 | 11 |
| peeroxide | 33 | 6 | 0 |
| **Total** | **675** | **53 (~8%)** | **11** |

Implications for this design:

- **peeroxide-dht dominates** at 87% of items. The audit and Phase 1/2 implementation work is overwhelmingly inside peeroxide-dht.
- **`compact_encoding` alone contributes ~120 free functions.** Most are demotion candidates — they implement encoding primitives that the workspace uses internally but downstream consumers should not depend on.
- **`#[non_exhaustive]` coverage is ~8% today**, ~80% under the §7 default. Phase 2 will be a large mechanical pass touching dozens of types.
- **11 `#[doc(hidden)] pub mod` declarations confirmed in peeroxide-dht.** Matches the prior reconnaissance in `HANDOFF_VISIBILITY_AUDIT.md`.

Caveats with this snapshot:

- The fallback enumeration used regex on source files (nightly rustdoc-JSON install failed in the audit sandbox). Counts are within ~10% of truth but the **cross-crate reachability data is unreliable** — it used name-matching, not semantic resolution, and conflates intra-crate uses with cross-crate uses.
- Per-axis decisions in §11 must therefore use a *reliable* reachability pass (see §9 Open Question on data pipeline) before being treated as binding.

## 9. Audit Methodology

### 9.1 Inventory pipeline

1. Generate machine-readable inventory for each library crate:
   ```bash
   cargo +nightly rustdoc -p libudx -- -Z unstable-options --output-format json
   cargo +nightly rustdoc -p peeroxide-dht -- -Z unstable-options --output-format json
   cargo +nightly rustdoc -p peeroxide -- -Z unstable-options --output-format json
   ```
   Output lives at `target/doc/<crate>.json`. Schema: `rustdoc_json_types::Crate`.

2. Extract every item with effective visibility `Public` plus its `Item.attrs` (to detect existing `#[non_exhaustive]`).

3. Cross-reference each public item against:
   - Workspace cross-crate use (ripgrep + `vogon_poetry_impact`).
   - Reference-ecosystem consumer mapping (§6).
   - Public function signatures in the same crate (cascade reachability).

4. Produce a single classification table (§11): one row per entity, columns for current visibility, current `#[non_exhaustive]`, proposed visibility, proposed `#[non_exhaustive]`, role, justification.

### 9.2 Format of the classification table

```
| crate | path | kind | role | curr.vis | curr.NE | prop.vis | prop.NE | rationale |
```

`prop.vis` ≠ `curr.vis` or `prop.NE` ≠ `curr.NE` rows drive the implementation work. Rows where everything matches are "already correct."

### 9.3 Flip-case investigation

For every row whose proposed decision differs from a decision recorded in the prior reconnaissance (`HANDOFF_VISIBILITY_AUDIT.md` §3a or `VISIBILITY_REFORM_PLAN.md`), record:

- Prior decision and the agent / session that produced it.
- New decision under this rubric.
- Whether the flip is (a) a prior error corrected, (b) new information from the reference-ecosystem mapping, or (c) a rubric mismatch that needs adjudication.

Category (c) flips MUST be brought back to the user / oracle before being applied — they signal the rubric itself may be wrong.

## 10. Open Questions

*(In priority order; each blocks subsequent design.)*

1. **Type-role taxonomy refinements** — is the 8-role taxonomy in §7 complete? Specifically:
   - Where do *callback / handler types* (the `*HandlerReply` family) sit — Result-shaped or their own role?
   - Are there any "internal-but-cross-crate" types that need a 9th role (workspace-internal but not user-internal)?
2. **Trait policy** — sealed traits via private supertrait, public unconditionally, or per-trait? Need to enumerate the current public trait surface before answering.
3. **Reference-ecosystem consumer selection** — which projects do we actually read for the mapping in §6? Recommendation: hyperbeam (smallest), `@hyperswarm/rpc` (already on our roadmap conceptually), hypercore replication protocol. Skip Keet (closed source, too large) and Pear (platform-level, indirect).
4. **Audit pipeline implementation** — bash + jq, Python script, or a small Rust binary in a new `xtask` workspace member? Recommendation: bash + jq for v1; promote to Rust if we keep using it.
5. **What constitutes a "public trait"?** Crate-level `pub trait` vs traits that only appear in associated-type bounds — both count, but only the first needs a sealed-pattern decision.

## 11. Per-Entity Classification

*(Initial population from `audit_reachability_*.tsv` + `audit_ecosystem_mapping.md`, 2026-05-17. Status: **partial** — see Data Coverage subsection below for what's missing.)*

### 11.1 Data Coverage

The reachability pass (semantic, via `vogon_poetry` graph queries — confirmed not regex) covered:

| Source category | Coverage | Disposition reliability |
|---|---|---|
| Crate-root re-exports + top-level public items | ✅ Full | High |
| `compact_encoding` free functions (~120 items) | ✅ Full | High |
| `blind_relay::encode_*` / `decode_*` family | ✅ Full | High |
| `crypto::*` helpers | ✅ Full | High |
| `noise_wrap`, `protomux`, `secret_stream` modules | ✅ Full (rustdoc-JSON ran with `--document-hidden-items`) | High |
| Other `#[doc(hidden)] pub mod` modules (`holepuncher`, `io`, `nat`, `peer`, `persistent`, `secretstream`, `secure_payload`, `socket_pool`) | ✅ Full (Phase 0C verification 2026-05-17 via qualified-path grep) | **VERIFIED** — 0 flips identified |

**Phase 0C verification (2026-05-17) closed the data gap.** All 8 doc-hidden module dispositions in §11.3 are now VERIFIED with reliable qualified-path reachability data. See `audit_phase0c_verification.md` (scratch).

### 11.2 Dispositions — HIGH CONFIDENCE (act on these)

#### `peeroxide-dht::compact_encoding::*` — ~120 functions

- **Visibility**: DEMOTE `pub mod compact_encoding` → `pub(crate) mod compact_encoding`.
- **Rationale**: Every function in the module shows `reachable_from = none` or `peeroxide-dht/tests` only. No cross-crate use, no ecosystem mapping pin.
- **`#[non_exhaustive]`**: N/A (functions).
- **Cascade**: `EncodingError` type also demotes; verify no public signature exposes it.
- **Flip vs prior recon**: No flip — prior recon flagged these as demotion candidates too.

#### `peeroxide-dht::blind_relay::encode_*` / `decode_*` family

- **Visibility**: DEMOTE → `pub(crate)`. Test-only reach.
- Keep `BlindRelayClient`, `pair`, `open`, `wait_opened`, `close` PUBLIC — used by `peeroxide::swarm` per inventory cross-references.
- **`#[non_exhaustive]`** on `PairResponse`, `RelayError`: yes (Reply + Error roles).
- **`PairMessage`, `UnpairMessage`**: wire-format envelopes — keep public, NO `#[non_exhaustive]`.

#### `peeroxide-dht::crypto::*`

- **Visibility**: KEEP PUBLIC. All confirmed cross-crate use:
  - `hash` — peeroxide + peeroxide-cli (reach=18)
  - `discovery_key` — peeroxide + peeroxide-cli (reach=16)
  - `hash_batch`, `sign_detached`, `verify_detached` — peeroxide-cli (reach=2 each)
  - `namespace` — peeroxide-cli
- **`#[non_exhaustive]`**: N/A (all free functions with primitive signatures — signature-stable by construction).
- **Pinned by ecosystem mapping**: `discovery_key` (hypercore replication).

#### `peeroxide-dht` ecosystem-pinned items (must stay public, apply `#[non_exhaustive]` per role)

- `HyperDhtConfig` → keep `pub` + `#[non_exhaustive]` (Config role).
- `HyperDhtHandle` → keep `pub`, no `#[non_exhaustive]` (Handle role).
- `KeyPair` → keep `pub`, no `#[non_exhaustive]` (Primitive value-type role; constructors `from_seed`/`generate`).
- `ServerConfig` → keep `pub` + `#[non_exhaustive]` (Config role).
- `ConnectOpts` → keep `pub` + `#[non_exhaustive]` (Options role).
- `PeerConnection` → keep `pub`, no `#[non_exhaustive]` (Handle role).
- `Holepuncher` → keep `pub`, no `#[non_exhaustive]` (Handle role).
- `ServerEvent`, `LookupResult`, `AnnounceResult`, `ConnectResult`, `ImmutablePutResult`, `MutablePutResult`, `MutableGetResult` → keep `pub` + `#[non_exhaustive]` (Event/Result roles).
- `HyperDhtError` → keep `pub` + `#[non_exhaustive]` (Error role).
- **From broader sweep (added 2026-05-17)**: `AnnounceResult` and `ConnectResult` are reachable from the ecosystem (Tier 1 consumers iterate/destructure). Need explicit shape decisions during Phase 2; treat as `#[non_exhaustive]` Result-role with accessors.

#### `libudx` ecosystem-pinned items

- `UdxRuntime` → keep `pub`, no `#[non_exhaustive]` (Handle).
- `UdxSocket` → keep `pub`, no `#[non_exhaustive]` (Handle).
- `UdxAsyncStream` → keep `pub`, no `#[non_exhaustive]` (Handle).
- `UdxStream` → keep `pub`, no `#[non_exhaustive]` (Handle).
- `UdxError` → keep `pub` + `#[non_exhaustive]` (Error).
- `Header`, `SackRange`, header flag constants → keep `pub`, NO `#[non_exhaustive]` (wire-format envelope).
- **From broader sweep**: `UdxRuntime::{create_socket, create_stream}`, `UdxSocket::{bind, send_to, recv_start, close}` confirmed as direct consumer surface — no signature changes needed; method visibility already public.

#### `peeroxide` top-level

- `spawn`, `discovery_key`, `SwarmConfig`, `JoinOpts`, `SwarmHandle`, `SwarmConnection`, `SwarmError` → all keep `pub`. Apply `#[non_exhaustive]` to `SwarmConfig`, `JoinOpts` (Config/Options), `SwarmError` (Error). `SwarmHandle`, `SwarmConnection` are Handles (no `#[non_exhaustive]`).
- `peer_info::Priority`, `peer_info::PeerInfo` → keep `pub` (reach via cli). Apply `#[non_exhaustive]` if matched by users, else value-type role.

### 11.3 Dispositions — VERIFIED (was FROM_PRIOR_RECON; verified 2026-05-17 in Phase 0C)

All 8 hidden-public modules had their dispositions re-verified with reliable qualified-path reachability (grep-based, not name-based graph). **0 flips identified**; all prior dispositions confirmed. Source: `audit_phase0c_verification.md` (scratch).

| Module | Disposition | External refs (count, files) | Source |
|---|---|---|---|
| `holepuncher` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs | 1 — `peeroxide/src/swarm.rs` | VERIFIED |
| `io` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs | 3 — `peeroxide-cli/src/cmd/deaddrop/progress/{bar,state,reporter}.rs` | VERIFIED |
| `nat` | DEMOTE `pub(crate) mod` | 0 — no external use | VERIFIED |
| `peer` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs | 2 — `peeroxide-dht/tests/dht_{golden_interop,interop}.rs` | VERIFIED |
| `persistent` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs (cli uses `PersistentConfig`) | 1 — `peeroxide-cli/src/cmd/node.rs` | VERIFIED |
| `secretstream` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs. Apply `#[non_exhaustive]` to `SecretstreamError` | 1 — `peeroxide-dht/tests/noise_golden_interop.rs` | VERIFIED |
| `secure_payload` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs (swarm uses `SecurePayload`) | 2 — `peeroxide-dht/tests/secure_payload_golden_interop.rs`, `peeroxide/src/swarm.rs` | VERIFIED |
| `socket_pool` | Keep `pub mod`, drop `#[doc(hidden)]`, add docs (swarm uses `SocketPool`) | 1 — `peeroxide/src/swarm.rs` | VERIFIED |

#### peeroxide-dht modules — additional dispositions (from ecosystem mapping / not in the 8-module list)

These are dispositions from §6 ecosystem mapping for modules NOT in the doc-hidden 8 above. They are NOT in the §11.3 verification scope; they remain HIGH-confidence per §11.2.

- `query` → DEMOTE `pub(crate) mod` + `pub use query::{QueryReply, QueryResult};` (already public mod, not doc-hidden).
- `router` → DEMOTE `pub(crate) mod` + `pub use router::{Router, HandshakeAction, HolepunchAction, ForwardEntry, HandshakeResult, HolepunchResult, RouterError};` (already public mod, not doc-hidden).
- `routing_table` → DEMOTE `pub(crate) mod` (already public mod, not doc-hidden).
- `protomux` → **PROMOTE** — drop `#[doc(hidden)]`, add docs, apply `#[non_exhaustive]` to `Channel`/`Mux`/`ChannelEvent`/`ProtomuxError`. KEEP `BatchItem`/`ControlFrame`/`DecodedFrame` public-NO-non_exhaustive (wire-format envelopes). Required by hypercore + @hyperswarm/rpc per §6.
- `noise_wrap` → KEEP PUBLIC, drop `#[doc(hidden)]` if applicable. Required by hypercore replication per §6.
- `secret_stream` → KEEP PUBLIC, drop `#[doc(hidden)]` if applicable, apply `#[non_exhaustive]` to `SecretStreamError`. Required by hypercore replication per §6.

---

## 12. Phasing & Implementation Plan

This plan branches on three pending open questions (§10 and prior design discussion). Wave numbering is *conditional* — actual waves get locked once those questions are answered.

### Phase 0 — Design lock (this document)

- 0a ✓ Skeleton + git stage
- 0b ✓ Initial inventory (with reliability caveats)
- 0c [pending Q] Lock reference-ecosystem consumer scope
- 0d [pending Q] Lock constructor-wave scope
- 0e [pending Q] Lock audit data pipeline approach (rustdoc-JSON retry vs targeted vogon_poetry)
- 0f Reference-ecosystem mapping populated into §6
- 0g Per-entity classification table populated in §11 from reliable reachability data
- 0h Trait policy decision (need full trait inventory first — only 1 public trait detected in inventory, but verify)

**Exit gate for Phase 0**: §6 mapping done, §11 table complete with proposed dispositions, flip cases (per §9.3) flagged for adjudication. Oracle skeptical review of the populated rubric + table before Phase 1.

### Phase 1 — Visibility reform (`pub` → `pub(crate)` demotions)

Cascade-safe demotions, one module/group per commit. Commit gates: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`. Wave structure depends on Phase 0 output, but the prior reconnaissance is a reasonable lower-bound preview:

- The 11 `#[doc(hidden)] pub mod` declarations in peeroxide-dht: ~3 demote, ~7 promote-and-document, ~1 mixed.
- The ~120 `compact_encoding` functions: likely a single demotion commit (with audit pass for any genuine consumer use).
- Internal helpers detected in libudx and peeroxide-dht: rolling per-module commits.

Tip of Phase 1 must pass the full live network suite (`cargo test -p peeroxide-cli --test live_commands -- --ignored`).

### Phase 2 — `#[non_exhaustive]` second sweep

For every surviving `pub` type, apply the rubric in §8.

- Per-crate commits (3 commits) OR per-role commits (~5-8 commits) — TBD by reviewer preference.
- **Constructor scope depends on Q2 answer**:
  - If Q2=(a): same commit applies `#[non_exhaustive]` AND adds `::new()` / builder for every newly-non_exhaustive user-constructed type.
  - If Q2=(b): `#[non_exhaustive]` commits first, constructor wave second (Phase 2.5).
  - If Q2=(c): constructors added only where workspace use requires them; rest deferred.

### Phase 3 — Verification

- 3a Full workspace build + test + clippy clean.
- 3b Live network suite green (`peeroxide-cli` 5/5 ignored tests).
- 3c Manual `peeroxide cp send/recv` with `PEEROXIDE_LOCAL_CONNECTION=false`.
- 3d `cargo doc --no-deps --workspace` clean — verifies no public signature references a `pub(crate)` type.
- 3e Oracle skeptical review of complete Phase 1 + Phase 2 diff.

### Phase 4 — Documentation

- Rustdoc module-level docs for any module that was promoted from `#[doc(hidden)]`.
- CHANGELOG entries listing every demotion + every newly `#[non_exhaustive]` type.
- `VISIBILITY_POLICY.md` (this file) updated with "Done" section and any rubric refinements that emerged during implementation.

### Phase 5 — Release prep (LOCAL only)

- `peeroxide-dht` 1.3.1 → 1.4.0 (minor — per resolved Gate D in prior reconnaissance).
- `libudx`, `peeroxide` patch or minor depending on whether their re-exports shifted.
- `peeroxide-cli` patch (binary; no SemVer surface).
- **NO `git push`.** Commit locally; user controls publication.

### Effort estimate

| Phase | Wall-clock (parallelized agents where possible) |
|---|---|
| Phase 0 | 4-8 hours (depends on Q1 scope and reachability pass approach) |
| Phase 1 | 2-4 hours |
| Phase 2 | 3-6 hours (depends on Q2 scope) |
| Phase 3 | 30-60 min (mostly compute time) |
| Phase 4 | 1-2 hours |
| Phase 5 | 30 min |
| **Total** | **11-22 hours** of agent work spread across multiple sessions |

This is a meaningful chunk of work. The implementation handoff (PROMPT file, to be produced when Phase 0 closes) will break it into ralph-loop-able chunks per phase.

---

## 12. Risks

- **Cascade demotion churn** — demoting an internal type may force cascading demotions of fields/methods that reference it. Inventory pipeline must surface these in the same pass, not as a follow-up.
- **Hidden re-exports** — a `pub use foo::Bar` can keep a type public even after the module is demoted. The rustdoc-JSON-driven inventory catches these because it walks the effective public namespace, but the human reviewer must verify per crate.
- **Doctest fallout** — module-level doctests that construct types we mark `#[non_exhaustive]` will fail. The build/test gate catches this; doctests in promoted modules likely need updates.
- **CHANGELOG drift** — every visibility/non_exhaustive change is, in principle, a documented breakage even if there are no current external users. We should still record them precisely for a future external-user audience.
