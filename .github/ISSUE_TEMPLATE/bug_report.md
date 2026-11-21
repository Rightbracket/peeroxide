---
name: Bug report
about: Something fails, regresses, or behaves differently from Node.js / udx-native
title: "<crate>: <what fails, in plain words>"
labels: bug
---

<!--
Title: name the area, usually with a crate prefix, and say what fails.
  e.g. "libudx: UdxAsyncStream::shutdown() can return Ok without sending END while the firewall hook is running"

Security issues (including remote denial of service) must NOT be filed here.
Report them privately: https://github.com/Rightbracket/peeroxide/security/advisories/new

Delete any optional section that doesn't apply.
-->

<!-- Where this was found, e.g. "Found while benchmarking #N", "Possible regression from #N (`abc1234`)",
     "Not reproduced yet; flagged during review". -->

## What fails

<!-- The symptom, with exact error messages, test names, or measured numbers. Tables are welcome.
     Include how to reproduce it: the command, setup (OS, network namespace, netem settings, ...), and
     what you expected instead. -->

## Environment

- Crate and version or commit:
- OS / arch:
- Rust version (`rustc -V`):
- Remote peer (peeroxide / Node.js Hyperswarm / udx-native / n/a):

## Cause

<!-- Optional. What is known or suspected, with `file:line` references or permalinks, and how sure you are
     ("checked in the code", "probably", "not confirmed"). Note if C libudx / udx-native behaves the same. -->

## Not yet known

<!-- Optional. Open questions and what would answer them. -->

## Options

<!-- Optional. Possible fixes and their trade-offs. Any change to a public API in libudx, peeroxide-dht or
     peeroxide must be additive (see AGENTS.md). -->

## Done when

- [ ] <!-- an observable outcome, e.g. "`cargo test -p peeroxide-cli` passes on `windows-latest`" -->
