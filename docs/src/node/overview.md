# Node Overview

The `node` command runs a long-lived DHT routing node. It joins the HyperDHT-compatible network, maintains a routing table, answers DHT queries, and stores the short-lived records that power `lookup`, `announce`, and higher-level commands built on top of them.

For background on how routing works, see [DHT and Routing](../concepts/dht-and-routing.md) and [DHT Primitives](../concepts/dht-primitives.md).

## Why run a node?

Running `peeroxide node` is useful when you want to:

- contribute stable routing capacity to the network,
- self-host one or more bootstrap addresses for your own deployment,
- build a private or test mesh with explicitly chosen bootstrap peers,
- keep DHT infrastructure running alongside your own `announce`, `cp`, `dd`, or `chat` usage.

A bootstrap node is not a separate protocol role. It is just a node with a stable address that other peers use as an entry point.

## Usage

```bash
peeroxide node [FLAGS]
```

## Common examples

Run a node with the default network settings:

```bash
peeroxide node
```

Run a public-facing node on the standard HyperDHT port:

```bash
peeroxide node --public --host 0.0.0.0 --port 49737
```

Run a node in a private mesh using only explicit bootstrap peers:

```bash
peeroxide node \
  --no-public \
  --bootstrap 10.0.0.11:49737 \
  --bootstrap 10.0.0.12:49737 \
  --host 0.0.0.0 \
  --port 49737
```

Run a routing node that also offers courtesy blind-relay service:

```bash
peeroxide node --public --relay -v
```

## Bootstrap and network selection

`node` uses the same bootstrap-resolution rules as the other DHT commands. The inherited global flags are documented in [init](../init/overview.md#global-cli-flags).

In practice:

- `--bootstrap <ADDR>` supplies explicit bootstrap addresses. Repeat it for multiple peers.
- `--public` adds the default public HyperDHT bootstrap nodes.
- `--no-public` removes the default public bootstrap nodes.
- If you provide neither custom bootstraps nor `--no-public`, peeroxide auto-fills the public bootstrap set.

That means `peeroxide node --no-public` with no `--bootstrap` starts in isolated mode: it listens for inbound peers, but it has no built-in way to discover the wider network.

## Flags

### Node-specific flags

| Flag | Description |
|---|---|
| `--port <PORT>` | UDP bind port. Default: `49737`. |
| `--host <HOST>` | UDP bind address. Default: `0.0.0.0`. |
| `--stats-interval <SECONDS>` | Interval for periodic stats logging. Default: `60`. Must be greater than `0`. |
| `--max-records <N>` | Maximum number of announcement records the node stores. |
| `--max-lru-size <N>` | Maximum number of mutable/immutable cache entries. |
| `--max-per-key <N>` | Maximum peer announcements stored for a single topic. |
| `--max-record-age <SECONDS>` | TTL for announcement records. |
| `--max-lru-age <SECONDS>` | TTL for mutable/immutable cache entries. |

These storage and TTL knobs affect how much DHT state your node retains while serving the network.

### Relay-related flags

| Flag | Description |
|---|---|
| `--relay` | Also run a courtesy blind-relay server inside this node process. |
| `--relay-key-seed <64-hex>` | Deterministic 32-byte seed for the relay identity keypair. Without it, a fresh relay keypair is generated on each run. |
| `--relay-max-sessions <N>` | Maximum concurrently accepted relay sessions. Default: `10000`. |
| `--relay-max-pairings-per-session <N>` | Maximum pending + active pairings per relay session. Default: `256`. |
| `--relay-pairing-timeout <SECONDS>` | Drop an unmatched relay pairing after this many seconds. Default: `300`. |
| `--relay-idle-session-timeout <SECONDS>` | Close a relay session with no pair/unpair activity for this many seconds. Default: `600`. |

### Inherited global flags

`node` also accepts the shared top-level flags:

- `--config <FILE>`
- `--no-default-config`
- `--public`
- `--no-public`
- `--bootstrap <ADDR>`
- `-v` / `-vv`

## Relay mode

When you add `--relay`, the process still behaves as a normal DHT node. It simply adds a blind-relay service alongside its routing duties.

This is useful when you want one always-on process to do both jobs: participate in routing and offer a courtesy relay endpoint for peers that cannot connect directly. For a dedicated relay-only process, see the [relay command](../relay/overview.md).

If relay mode is enabled, startup prints the relay public key to stdout before the bound `host:port` line.

## Operational notes

- **Long-running process**: `node` is meant to stay up. It exits cleanly on Ctrl-C or SIGTERM.
- **Output streams**: the bound address is printed to stdout. Tracing and operational logs go to stderr.
- **Verbosity**: `-v` enables info-level lifecycle logs such as bootstrap completion and relay counters. `-vv` additionally enables periodic routing-table and cache statistics.
- **Private meshes**: for test or lab networks, combine `--no-public` with explicit `--bootstrap` peers. The repo's Docker NAT test uses a multi-node private mesh rather than a single bootstrap address so routing information converges faster.
- **Multiple nodes**: if you are building your own bootstrap set, run more than one stable node when possible. A small cluster is more robust than a single entry point.

## See Also

- [DHT and Routing](../concepts/dht-and-routing.md)
- [DHT Primitives](../concepts/dht-primitives.md)
- [Ping Overview](../ping/overview.md)
- [Announce Overview](../announce/overview.md)
