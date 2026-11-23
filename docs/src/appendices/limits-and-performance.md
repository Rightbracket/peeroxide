# Limits and Performance

This appendix documents hard limits, configurable bounds, and observed performance characteristics of the `peeroxide-cli` tools.

## Hard Limits

| Constant | Value | Context |
|---|---|---|
| `MAX_ECHO_SESSIONS` | 64 | Concurrent echo sessions per `announce` process |
| `HANDSHAKE_TIMEOUT` | 5 s | Echo protocol handshake timeout |
| `IDLE_TIMEOUT` | 30 s | Echo session idle timeout |
| `ECHO_MSG_LEN` | 16 bytes | Echo probe frame size (fixed) |
| `ECHO_TIMEOUT` (ping) | 5 s | Per-probe timeout in `ping --connect` mode |
| `MAX_CHUNKS` | 65 535 | Maximum chunks in a single `dd` message |
| `MAX_PAYLOAD` | 1 000 bytes | Maximum payload per `dd` chunk |
| `ROOT_HEADER_SIZE` | 39 bytes | `dd` root chunk header size |
| `NON_ROOT_HEADER_SIZE` | 33 bytes | `dd` non-root chunk header size |
| `CHUNK_SIZE` (cp) | 65 536 bytes | `cp` file chunk size |
| `--data` max (announce) | 1 000 bytes | Maximum `--data` payload for `announce` |
| lookup `--with-data` concurrency | 16 | `buffer_unordered(16)` for mutable DHT gets |
| ping topic mode peer cap | 20 | Maximum peers probed per topic lookup |

## Derived Limits

**Maximum `dd` message size:**

```
MAX_CHUNKS × MAX_PAYLOAD = 65 535 × 1 000 = ~65.5 MB
```

Practical limit is lower due to DHT value size constraints and network latency.

**Maximum `cp` file size:**

Limited by available DHT storage and client memory. Each 65 536-byte chunk is stored as a separate immutable DHT value. There is no hard-coded upper bound in the CLI, but very large files will require many round-trips.

## Connection Buffers and Memory

Commands that connect to peers use the library's default limits: `announce` and `cp` through a swarm with the default ("balanced") settings, and `ping --connect` through a direct DHT connection. Each connection's stream has:

| Setting | Default | Notes |
|---|---|---|
| Read buffer limit | 8 MiB (`DEFAULT_READ_BUFFER_LIMIT`) | Most a stream holds for its reader. A remote can't make it hold more however slowly the reader reads, e.g. when `cp` writes to a slow disk |
| Write buffer limit | 8 MiB (`DEFAULT_WRITE_BUFFER_LIMIT`) | Most a stream holds written and not yet acknowledged |
| Peers per swarm (`max_peers`) | 64 | `announce` and `cp` only. With the defaults, read buffers across all connections are bounded at 512 MiB |
| Message being received | up to 16 MiB | The encrypted stream holds each message whole as it arrives; this is on top of the buffer limits |

The limits also bound speed: a connection moves at most about **0.94 × limit ÷ round trip** in each direction, about 79 MB/s at 8 MiB over a 100 ms round trip. A remote on Node.js advertises a 4 MiB window, about 36 MB/s over 100 ms.

For library users, `peeroxide`'s `SwarmConfig` offers `constrained` (256 KiB, 16 peers, 8 MiB budget), `mobile` (1 MiB, 32 peers, 64 MiB budget), `balanced` (default), and `throughput` (32 MiB) presets, plus a swarm-wide `memory_budget` that gives each connection `budget / (2 × max_peers)` per direction, never less than 64 KiB (`MIN_STREAM_LIMIT`). A bare `libudx` stream has no buffer limit until one is set. Without one it advertises a 4 MiB window (`DEFAULT_RWND`) and holds no more than that ahead of a gap. The lowest limit a `libudx` stream takes is 8 KiB. See the [`peeroxide` crate docs](https://docs.rs/peeroxide) ("Choosing limits") and [`StreamLimits`](https://docs.rs/peeroxide-dht/latest/peeroxide_dht/hyperdht/struct.StreamLimits.html).

## Timing

| Parameter | Value | Notes |
|---|---|---|
| `announce` refresh interval | 600 s | Background mutable put to keep slot alive |
| `announce` seq | Unix epoch seconds | Two refreshes in the same second produce identical seq |
| `ping --interval` default | 1.0 s | Configurable |
| `ping --count` default | 1 | 0 = infinite |

## Concurrency

- `lookup --with-data` fetches peer data in parallel with a concurrency window of 16 (`buffer_unordered(16)`).
- `announce` handles incoming connections concurrently; echo sessions are bounded by `MAX_ECHO_SESSIONS = 64`.
- `cp` uploads/downloads chunks sequentially per file (parallelism may be added in future releases).

## Exit Codes

| Code | Meaning | Tools |
|---|---|---|
| 0 | Success / clean shutdown | all tools |
| 1 | Fatal error | all tools |
| 130 | SIGINT received | `lookup`, `ping` |
| 0 | SIGINT/SIGTERM received | `announce` (intentional — clean shutdown is success) |

Note: `announce` returns 0 on SIGINT/SIGTERM because interactive shutdown is the normal workflow. `lookup` and `ping` return 130 to allow callers to distinguish interruption from success.

## Chat

| Parameter | Value | Description |
|---|---|---|
| Max record size | 1000 bytes | Maximum size for a single DHT record |
| Message overhead | 180 bytes | Fixed overhead (screen name + content combined ≤ 820 bytes) |
| Encryption | XSalsa20-Poly1305 | Security parameters: nonce 24 bytes, tag 16 bytes |
| Epoch length | 60 s | Time window for message bucketing |
| Buckets per epoch | 4 | Sub-divisions within an epoch for message distribution |
| DHT lookups per cycle | 8 | Checks current and previous epoch across 4 buckets |
| Discovery interval | 8 s | Cadence for looking up new peers |
| Feed expiry | 1200 s | Time before a peer feed is considered stale |
| Summary eviction trigger | 20 messages | Number of messages before clearing old history |
| Summary eviction count | 15 messages | Number of messages removed during eviction |
| Mutable put retries | 3 | Retries at 200 ms, 500 ms, and 1000 ms intervals |
| Rotation check interval | 30 s | Frequency of checking for epoch/bucket rotation |
| Dedup ring capacity | 1000 hashes | Number of message hashes stored to prevent duplicates |
| Gap timeout | 5 s | Maximum wait time for out-of-order messages |
| TUI history cap | 500 lines | Scrollback buffer limit in the interactive interface |

### Chat Performance

The inbox polling mechanism uses parallel lookups and mutable gets. A full inbox cycle typically completes in 2-4 seconds of wall-clock time. This is a significant improvement over earlier nested-serial designs which required 10-20 seconds for the same operation.

## Dead Drop (v2)

| Parameter | Value | Description |
|---|---|---|
| Max chunk size | 1000 bytes | Total size including headers |
| Data payload | 998 bytes | Actual data bytes per non-root chunk |
| Root index slots | 30 | Pointers to child chunks in the root node |
| Non-root index slots | 31 | Pointers to child chunks in intermediate nodes |
| Need-list entries | 124 | 8-byte entries published in each DHT record |
| Parallel fetch cap | 64 | Maximum concurrent DHT requests |
| Soft depth cap | 4 | Maximum tree depth (~27 GB capacity) |
| Per-put timeout | 30 s | Maximum duration for a single chunk upload |
| Stall watchdog check | 5 s | Frequency of progress monitoring |
| Stall watchdog trigger | 30 s | Time with no progress before triggering a restart |
| Need-list publish | 20 s | Frequency of publishing the local need-list |
| Need-list announce | 60 s | Keepalive interval for the need-list topic |
| Refresh interval | 600 s | Default cadence for re-announcing data availability |
| Initial concurrency | 128 | Starting sender concurrency for AIMD |
| Fetch backoff | 500 ms to 15 s | Progressive delay for failed mutable or immutable gets |

### Tree Capacity by Depth

The implementation enforces `SOFT_DEPTH_CAP = 4`. Depths beyond that are theoretical only and are rejected at PUT time.

| Depth | Max Data Chunks | Approx. Capacity |
|---|---|---|
| 0 | 30 | 29 KB |
| 1 | 930 | 928 KB |
| 2 | 28,830 | 28 MB |
| 3 | 893,730 | 891 MB |
| 4 | 27,705,630 | 27 GB |

### AIMD Algorithms

**v2 (Current):**
- Uses Exponentially Weighted Moving Average (EWMA) with alpha 0.1.
- Decision interval of 20 samples.
- Fast-trip threshold of 10.
- Shrink factor: 0.75×.
- Growth factor: +2.

**v1 (Legacy):**
- Uses a tumbling window of 10 samples.
- Halves concurrency if degradation exceeds 30%.
- Increases concurrency by 1 if 0% degradation is detected.

