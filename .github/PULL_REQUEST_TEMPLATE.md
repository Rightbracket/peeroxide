## Summary

<!-- 1-5 bullets: what changed and why. For a PR covering several issues, lead each bullet with it,
     e.g. "- **#41**: `shutdown()` could return Ok without sending END. ..." -->
-

<!-- Link related issues, one per line: Closes #N / Fixes #N. Use "Part of #N" for partial work. -->

## Motivation

<!-- Why is this change needed? The root cause, with `file:line` references, and how it was reproduced
     (command, setup, before/after). Link decisions recorded on the issue (e.g. D1-D5).
     Delete for trivial fixes. -->

## Changes

<!-- For small PRs: prose is fine. For larger PRs: one row per change, including CHANGELOG,
     PROTOCOL.md, docs, CI and tooling. -->

| Crate | Change |
|-------|--------|
|       |        |

## Public API Changes

<!-- If no public API surface in libudx, peeroxide-dht or peeroxide is affected, replace this section's
     content with "No public API changes." (mention any new `pub(crate)` items if helpful).
     Otherwise tick one box and fill in the table. Breaking changes need maintainer approval BEFORE
     implementation (AGENTS.md). Classify each new item per VISIBILITY_POLICY.md: role (Handle / Config /
     Event-Result / Error / Primitive / Wire-envelope), `#[non_exhaustive]`, and a constructor for
     non-exhaustive Config types. -->

- [ ] Additive only (semver-compatible)
- [ ] Breaking (maintainer approval: <!-- link -->)

| Crate | Symbol | Change | VISIBILITY_POLICY role / `#[non_exhaustive]` |
|-------|--------|--------|----------------------------------------------|
|       |        |        |                                              |

## Behaviour and wire changes

<!-- Delete if none. Changed defaults, changed behaviour of existing APIs, and anything on the wire:
     new or changed frames/fields, and how Node.js Hyperswarm / udx-native / C libudx peers handle it. -->

## Testing

<!-- Record the result and platform for each command, e.g. "passed, 1,289 tests (Linux container, Node interop included)".
     Explain any failure or skip (unrelated, flaky, environment). -->

- [ ] `cargo test --workspace`:
- [ ] `cargo test -p peeroxide-cli --test live_commands -- --ignored`:
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`:
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`:

<!-- Optional, as relevant:
     - Failing tests written first: test name, result before the fix, result after.
     - Measurements: tool and setup (e.g. `.github/scripts/pathsweep.py`, netem settings, runs per row),
       before/after table, and how noisy it is.
     - For libudx receive-path changes, add the `benchmark` label to run the cross-OS receive benchmark. -->

## Checklist

- [ ] CHANGELOG updated for each affected crate
- [ ] New public items have `///` doc comments
- [ ] No task-artifact files added (check `git diff --name-only --diff-filter=A main`; see AGENTS.md)

## Notes

<!-- Anything reviewers should pay attention to: known limitations, migration steps, unrelated failures
     seen while testing, and follow-up work with issue links. Delete if empty. -->
