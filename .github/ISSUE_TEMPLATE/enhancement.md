---
name: Enhancement
about: A new feature, capability, or improvement
title: "<crate>: <what it adds or improves>"
labels: enhancement
---

<!--
Title: name the area, usually with a crate prefix, and say what changes.
  e.g. "libudx: add a UdxStream shutdown variant that waits for the firewall hook"

Delete any optional section that doesn't apply.
-->

<!-- Optional: where this came from, e.g. "Follow-up to #N (decision D3)". -->

## Goal

<!-- What should be possible or better, and for whom. If this builds on earlier work, say what exists
     today and what it leaves out (a "Background" section works too). -->

## What we measured

<!-- Optional. Numbers that motivate the change, with the tool and setup used
     (e.g. `.github/scripts/pathsweep.py`, netem settings, runs per row). -->

## Proposal

<!-- What to add or change. If there are several gaps, list them. Code sketches and formulas are welcome. -->

## Things to settle

<!-- Optional. Open design questions, alternatives, and their trade-offs. -->

## Constraints

- Public API in `libudx`, `peeroxide-dht` and `peeroxide` changes additively only (AGENTS.md); new surface follows `VISIBILITY_POLICY.md`.
- Stays wire-compatible with Node.js Hyperswarm / udx-native / C libudx peers.
<!-- Add constraints specific to this change. -->

## First step

<!-- Optional. e.g. "A design doc for review, covering the API and per-crate surface. No code yet." -->

## Acceptance

- [ ] <!-- an observable outcome -->
