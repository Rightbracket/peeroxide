# Relay Overview

A blind relay helps two peers connect when a direct UDX path is not possible. In the normal case, peeroxide prefers a direct connection or DHT-assisted holepunching. A relay is the fallback when NAT or firewall behavior prevents that direct path from coming up.

For background on peer discovery and routing, see [DHT and Routing](../concepts/dht-and-routing.md) and [Topics and Discovery](../concepts/topics-and-discovery.md).

## What “blind” means

The relay coordinates the connection, but it does not terminate the application session. The two real peers still authenticate each other and speak over their own end-to-end encrypted transport. The relay only forwards packets between them.

That makes a blind relay different from an application proxy: it helps with reachability, not application-layer trust.

## Two ways to run a relay

### Dedicated relay process

Use `peeroxide relay` when you want a process whose only job is to serve blind-relay requests.

```bash
peeroxide relay --public --host 0.0.0.0 --port 49737
```

This command prints two useful startup lines on stdout:

```text
relay public key: <64-hex>
0.0.0.0:49737
```

The public key is the relay's identity. Clients need that public key plus a reachable UDP socket address for the relay.

### Courtesy relay on a routing node

Use [`peeroxide node`](../node/overview.md) with `--relay` when you want one long-running process to do both jobs: participate in DHT routing and offer relay service.

```bash
peeroxide node --public --relay
```

This is convenient for private meshes, bootstrap nodes, or small deployments where one operator wants to contribute both routing capacity and relay capacity.

## Common flags

`peeroxide relay --help` currently exposes these relay-specific flags:

| Flag | Meaning |
|---|---|
| `--port <PORT>` | UDP bind port. Default: `49737`. |
| `--host <HOST>` | UDP bind address. Default: `0.0.0.0`. |
| `--key-seed <KEY_SEED>` | Deterministic 32-byte relay identity seed in hex. |
| `--stats-interval <SECONDS>` | Periodic relay/routing stats log interval. Default: `60`. |
| `--max-sessions <N>` | Maximum concurrent relay sessions. Default: `10000`. |
| `--max-pairings-per-session <N>` | Maximum pending + active pairings per session. Default: `256`. |
| `--pairing-timeout <SECONDS>` | Drop an unmatched pairing after this many seconds. Default: `300`. |
| `--idle-session-timeout <SECONDS>` | Close a session with no pair/unpair activity for this many seconds. Default: `600`. |

The courtesy relay flags on `peeroxide node --relay` are the same knobs with `relay-` prefixes:
`--relay-key-seed`, `--relay-max-sessions`, `--relay-max-pairings-per-session`, `--relay-pairing-timeout`, and `--relay-idle-session-timeout`.

Like the other DHT-facing commands, both `relay` and `node --relay` also accept the shared bootstrap-selection flags documented in [init](../init/overview.md#global-cli-flags): `--public`, `--no-public`, `--bootstrap`, `--config`, and `--no-default-config`.

## Choosing between `relay` and `node --relay`

Use `peeroxide relay` when:

- you want a dedicated relay-only service,
- you do not want the process storing general DHT records,
- you want to scale relays separately from bootstrap or routing nodes.

Use `peeroxide node --relay` when:

- you already run a stable node,
- you want to offer relay service as a courtesy,
- a single always-on process is simpler operationally.

In either mode, the relay should usually run at a stable, reachable UDP address. The relay helps other peers traverse NATs; it is not designed to depend on its own holepunching sequence.

## How clients opt in

Blind relay is a `Swarm` feature. At the library level, a server-side swarm can advertise a relay by setting `SwarmConfig::relay_through` to the relay's public key and, optionally, `SwarmConfig::relay_address` to a known socket address.

```rust
use peeroxide::SwarmConfig;

let mut cfg = SwarmConfig::with_public_bootstrap();
cfg.relay_through = Some(relay_public_key);
cfg.relay_address = Some("198.51.100.10:49737".parse()?);
```

When that is set, inbound swarm clients are told to connect through the relay instead of trying the normal direct or holepunched path.

### CLI example: forcing relay for `cp`

In the CLI today, the practical opt-in surface is the `PEEROXIDE_FORCE_RELAY` environment variable used by swarm-backed commands such as `cp`.

```bash
export PEEROXIDE_FORCE_RELAY="<relay_pubkey_hex>@198.51.100.10:49737"
peeroxide cp send ./photo.jpg my-topic
peeroxide cp recv my-topic ./downloads/
```

The value format is:

```text
<relay_pubkey_hex>@<host:port>
```

A few details matter:

- The public key must be 64 hex characters.
- The address must be the relay's reachable UDP address, not necessarily the bind address it printed locally.
- The setting is most important on the side acting as the swarm server, such as `cp send`, because that side advertises `relay_through` in its handshake reply.
- Scripts may still export it on both sides for symmetry and operator clarity.

### What about `dd`?

`dd` does not use blind relay. It stores and retrieves data through DHT records rather than opening a long-lived swarm connection, so `PEEROXIDE_FORCE_RELAY` has no effect there.

## Practical workflow

A common setup looks like this:

1. Start a relay on a stable public address.
2. Note its printed relay public key.
3. Publish or otherwise share that public key and UDP address with the peers that should use it.
4. Configure the sender/server side to advertise that relay.
5. Let the receiving side connect normally; the handshake will direct it through the relay.

## Security notes

A blind relay improves reachability, not confidentiality. The application stream remains protected by the same end-to-end Noise + SecretStream security model described in [Security Model](../appendices/security-model.md).

The relay can observe metadata such as:

- which clients opened relay control sessions,
- how many pairings were attempted,
- the relay-visible source addresses of those sessions,
- traffic volume and stream lifetime.

It does **not** need to decrypt relayed application payloads.

## See Also

- [Architecture](architecture.md)
- [peeroxide node](../node/overview.md)
- [cp Overview](../cp/overview.md)
- [DHT and Routing](../concepts/dht-and-routing.md)
- [Security Model](../appendices/security-model.md)
