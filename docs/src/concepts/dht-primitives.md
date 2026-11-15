# DHT Primitives

This page is a reference for the four core operations that `peeroxide-dht` exposes and that every higher-level subsystem (`announce`, `lookup`, `cp`, `dd`, `chat`) is built on top of. Once you understand [DHT and Routing](./dht-and-routing.md) at the conceptual level, this is the next layer down: the actual operations you can perform against the network.

## `immutable_put` / `immutable_get` — Content-Addressed Storage

Stores arbitrary bytes on DHT nodes, addressed by the BLAKE2b-256 hash of the value itself. Content-addressed: you can only retrieve a value if you already know its hash.

- **`immutable_put(value: &[u8])`** — computes `target = hash(value)`, queries the K closest nodes to that target, commits the raw bytes. Returns the 32-byte hash.
- **`immutable_get(target: [u8; 32])`** — queries nodes closest to `target`; any node that has the value returns it. The client verifies `hash(returned_value) == target`.

| Property | Detail |
|----------|--------|
| Data stored | Raw `Vec<u8>` — arbitrary bytes, no signing, no keys, no seq |
| Addressing | `hash(value)` — immutable; changing the value yields a different address |
| Max payload | ~900–1000 bytes (UDP framing; no explicit code constant) |
| Wire commands | `IMMUTABLE_PUT = 8`, `IMMUTABLE_GET = 9` |
| Discoverability | The reader must already know the hash (given out-of-band or via a mutable pointer) |

## `mutable_put` / `mutable_get` — Signed, Updateable Storage

Stores arbitrary bytes signed by an Ed25519 keypair, addressed by `hash(public_key)`. The owner can update the value by incrementing a sequence number.

- **`mutable_put(key_pair, value: &[u8], seq: u64)`** — computes `target = hash(public_key)`, signs `(seq, value)` with the secret key, and sends `MutablePutRequest { public_key, seq, value, signature }` to the closest nodes.
- **`mutable_get(public_key: &[u8; 32], seq: u64)`** — queries with `target = hash(public_key)` and a requested minimum `seq`. Nodes return the stored value only if `stored.seq >= requested_seq`. The client verifies the signature.

| Property | Detail |
|----------|--------|
| Data stored | `{ public_key: [u8;32], seq: u64, value: Vec<u8>, signature: [u8;64] }` |
| Addressing | `hash(public_key)` — one mutable slot per keypair |
| Max payload (value) | **~1002 bytes** (token present, `seq ≤ 252`; derived in [Size Budget for `mutable_put`](#size-budget-for-mutable_put) below) |
| Seq semantics | Strictly monotonic. `SEQ_REUSED (16)` error if equal; `SEQ_TOO_LOW (17)` if lower |
| Salt support | Not implemented — there is no salt field; one record per keypair |
| Wire commands | `MUTABLE_PUT = 6`, `MUTABLE_GET = 7` |

## `announce` / `lookup` — Peer Discovery and Rendezvous

Originally designed as peer-discovery primitives — store structured peer records (public key + relay addresses) under a topic hash. **In this workspace they are also used as a general-purpose rendezvous mechanism**: the announcer's public key acts as a pointer to a further `mutable_put` slot containing the actual record. See [The Rendezvous Pattern](#the-rendezvous-pattern) below.

- **`announce(target: [u8;32], key_pair, relay_addresses)`** — queries the closest nodes for the topic and sends a signed `AnnounceMessage` containing `HyperPeer { public_key, relay_addresses }`. Multiple peers can announce under the same topic simultaneously.
- **`lookup(target: [u8;32])`** — queries the closest nodes; they return `LookupRawReply { peers: Vec<HyperPeer>, bump }` — all peers that have announced on that topic (up to 20 per node).

| Property | Detail |
|----------|--------|
| Data stored | `HyperPeer { public_key: [u8;32], relay_addresses: Vec<Ipv4Peer> }` |
| Multi-writer | Yes — up to 20 announcers per topic per node |
| IP in stored record | No — the source IP is not stored in `HyperPeer`; only the pubkey + relay addresses |
| Announce with no addresses | Allowed — `relay_addresses = []` is valid |
| `MAX_RECORDS_PER_LOOKUP` | 20 per node (per-node cap; the total across all queried nodes can exceed 20) |
| `MAX_RELAY_ADDRESSES` | 3 (truncated on store) |
| Wire commands | `LOOKUP = 3`, `ANNOUNCE = 4`, `FIND_PEER = 2` |

**Key differences from put/get:**

- `lookup` / `announce` is multi-writer — many peers announce under one topic.
- `put` / `get` is single-writer — one value per address.
- `announce` stores a small structured record; `put` stores opaque bytes (up to ~1000 B per record).

### The Rendezvous Pattern

An announce "topic" is just a 32-byte address. There's no constraint that it correspond to a real peer-discoverable resource — it can be:

- a hash of an application-meaningful key,
- a deterministic derivation from any shared input (a string, a public key, a secret, a counter, …) via BLAKE2b-256 or any other 32-byte hash,
- or even a random value, if both writer and reader can agree on it out-of-band.

This makes `announce` / `lookup` a generic rendezvous primitive: anyone who can independently arrive at the same 32-byte topic can find every other announcer at it. And because the only structured field the protocol actually requires the announcer to publish is a 32-byte `public_key`, that pubkey can be treated as a **pointer** — typically to a `mutable_put` slot owned by that same ephemeral keypair — rather than as a literal peer identity.

The generic three-step pattern is:

1. **Derive a topic** — any agreed-upon function `f(...) -> [u8;32]`. Writer and reader must arrive at the same value.
2. **Writer** — generate an ephemeral keypair `k`, publish the actual record at `mutable_put(k, value, seq)`, and `announce` `k.public_key` on the topic. The `relay_addresses` field carries no meaning and is typically left empty.
3. **Reader** — `lookup` the topic, then `mutable_get` each returned pubkey to retrieve the actual records in parallel.

This sidesteps `mutable_put`'s one-record-per-pubkey limitation: a single topic can host many simultaneous writers, each with its own independent `mutable_put` slot. The TTLs on the rendezvous record and on the payload record are also independent, so a writer can refresh them on different cadences.

### Increasing Footprint with Epochs and Buckets

The per-node cap is **20 announcers per topic per node**. Two complementary techniques extend that footprint by embedding deterministic salt into the topic derivation:

- **Epochs** — incorporate a quantized timestamp (for example, `floor(unix_secs / 60)`). The topic rotates over time automatically; writer and reader both use the current epoch when deriving the topic, and readers typically scan a small backward window so announcers near a boundary aren't missed. Epoch rotation also bounds how long an observer can correlate a single topic.
- **Buckets** — incorporate a small integer `0..N`. Writers hash to one of N possible topics (deterministically or at random); readers scan all N in parallel to find every announcer.

Combining the two yields `epoch_window × bucket_count` distinct topics for the same logical rendezvous — e.g. a 2-epoch window with 4 buckets gives an effective capacity of `8 × 20 = 160` announcers per node, all discoverable in 8 parallel lookups.

### Concrete uses in this workspace

The chat and dd v2 subsystems each lean on this pattern. Their exact topic-derivation rules, epoch/bucket counts, and write/read flows are documented alongside the rest of their wire formats and protocols:

- chat — see [Wire Format](../chat/wire-format.md) and [Protocol](../chat/protocol.md).
- dead drop v2 — see [Architecture](../dd/architecture.md) and [Wire Format](../dd/format.md).

## TTL (Time-To-Live)

All stored values are ephemeral — they expire from node storage.

| Storage type | TTL (default) |
|---|---|
| Announcement records (`RecordCache`) | 20 minutes (`max_record_age`) |
| Mutable / immutable LRU cache | 20 minutes (`max_lru_age`) |
| Router forward entries | 20 minutes (`DEFAULT_FORWARD_TTL`) |

Clients must periodically re-announce / re-put to keep data alive. The 20-minute default matches the Node.js reference implementation. Both `cp` and `dd` issue periodic refreshes during long-running operations for exactly this reason.

## Size Budget for `mutable_put`

The most common protocol-design question is "how many bytes can I put inside one `mutable_put` value?" Starting from the 1180 bytes these limits were sized for (a 1200-byte datagram less the 20 bytes of a UDX header) and subtracting the wire overhead for a `mutable_put` request with the routing token present and `seq ≤ 252`:

```text
1180  budget for the request
 - 75  outer RPC Request fixed fields (type, flags, tid, to, token, command, target)
 -  3  outer compact-encoding length prefix for put_bytes
 - 32  public_key field
 -  1  seq compact-encoding (1 byte for seq ≤ 252)
 -  3  inner compact-encoding length prefix for value
 - 64  signature
─────
1002  bytes available for the message value payload
```

In practice the higher-level subsystems reserve a small margin and call this `MAX_RECORD_SIZE = 1000` (see `chat::wire` and `deaddrop::v2::wire`). Subtract per-record framing — author pubkey, timestamp, content type, signature, length-prefix bytes — to derive the payload budget for your own protocol. The chat subsystem's [Reference](../chat/reference.md) and dead drop's [Wire Format](../dd/format.md) chapters carry the exact per-record overhead and the resulting content budgets.
