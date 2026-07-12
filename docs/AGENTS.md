# AGENTS.md — docs/

This directory contains the peeroxide-cli mdBook documentation source.

## Structure

```
docs/
├── book.toml          — mdBook configuration (includes mdbook-mermaid preprocessor)
└── src/
    ├── SUMMARY.md     — Chapter outline and navigation tree
    ├── introduction.md
    ├── init/          — peeroxide init (config + man-page install) documentation
    ├── concepts/      — Shared conceptual background
    ├── lookup/        — lookup command documentation
    ├── announce/      — announce command documentation (echo protocol defined here)
    ├── ping/          — ping command documentation (cross-refs echo protocol)
    ├── node/          — node command documentation
    ├── cp/            — cp command documentation
    ├── dd/            — dd (Dead Drop) command documentation (v1 + v2 protocols)
    ├── chat/          — chat subsystem (user guide, TUI, wire format, protocol, reference)
    └── appendices/    — Security model, limits & performance
```

## Building

```bash
# Install prerequisites (once)
cargo install mdbook mdbook-mermaid

# Build
mdbook build docs/

# Serve locally with live reload
mdbook serve docs/
```

Output goes to `docs/book/` (gitignored).

## Conventions

- All Mermaid diagrams use ` ```mermaid ``` ` fences — rendered by `mdbook-mermaid`.
- Command chapters live under `src/<command>/` with an `overview.md` entry in `SUMMARY.md`; follow the same pattern for new commands such as `relay/`.
- Cross-references use relative `[text](../path/to/file.md)` links (mdBook requirement).
- Human output examples go on **stderr**; structured JSON output goes on **stdout**.
- The Echo Protocol is defined exactly once in `src/announce/echo-protocol.md`. All other chapters that reference it must link there rather than re-documenting it.
- Both `dd` v1 (`0x01`, single linked chain) and v2 (`0x02`, tree-indexed) protocols are shipped; `dd/future-direction.md` is a short pointer noting that there is no current speculative roadmap.

## Deployment

Docs are deployed to GitHub Pages automatically on push to `main` via `.github/workflows/docs-site.yml`.
