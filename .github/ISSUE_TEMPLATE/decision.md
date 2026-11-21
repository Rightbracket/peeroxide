---
name: Decision / question
about: A design or policy question that needs a maintainer decision before work starts
title: "Decide <what>"
labels: question
---

<!--
Title: state what needs deciding.
  e.g. "Decide on a default read buffer limit for streams from untrusted peers"

For general usage questions, open a blank issue instead.
-->

<!-- Optional: where this came from, e.g. "Follow-up from the review of #N". -->

## Background

<!-- What exists today and why it raises a question. Link the relevant PRs, issues and code (`file:line`). -->

## Question

<!-- One explicit question. -->

## Considerations

<!-- What the answer affects, for example:
     - Throughput / memory (with measurements, if any)
     - Interop with Node.js Hyperswarm / udx-native / C libudx
     - Public API: additive only (AGENTS.md); new surface per VISIBILITY_POLICY.md
     - Impact on peeroxide-cli
     - Prerequisites (issues that must be fixed first) -->

## Options

<!-- Optional. Candidate answers and their trade-offs. -->

## Decision

<!-- Filled in once decided: the choice, who made it, and the issue or PR that carries it out. -->
