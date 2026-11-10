# UDX Protocol Reference

UDX is a lightweight reliable transport protocol built on UDP. There is no formal
specification document — the C source code at
[holepunchto/libudx](https://github.com/holepunchto/libudx) is the reference.
This document describes the wire format and protocol behavior as implemented by
this crate.

## Packet Header (20 bytes)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Magic (0xFF) |  Version (1)  |  Type/Flags   |  Data Offset  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Remote Stream ID (LE)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Receive Window (LE)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     Sequence Number (LE)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     ACK Number (LE)                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | magic | Always `0xFF` |
| 1 | 1 | version | Always `1` |
| 2 | 1 | type | Packet type flags (bitfield) |
| 3 | 1 | data_offset | Bytes of extension data between header and payload (SACK, MTU padding) |
| 4-7 | 4 | remote_id | Remote stream identifier (little-endian u32) |
| 8-11 | 4 | recv_window | Receiver's advertised window in bytes (LE u32) |
| 12-15 | 4 | seq | Sequence number (LE u32) |
| 16-19 | 4 | ack | Acknowledgment number (LE u32) |

All multi-byte fields are little-endian.

## Packet Type Flags (byte 2)

Flags are a bitfield. Multiple can be set simultaneously (e.g., `DATA | END`
for the last data packet, or `DATA | SACK`).

| Flag | Value | Description |
|------|-------|-------------|
| `DATA` | `0x01` | Data payload present |
| `END` | `0x02` | End of stream (write-side FIN) |
| `SACK` | `0x04` | Selective acknowledgment data follows header |
| `MESSAGE` | `0x08` | Unreliable datagram (no retransmission) |
| `DESTROY` | `0x10` | Immediate stream teardown |
| `HEARTBEAT` | `0x20` | Keepalive or MTU probe |

## SACK Format

SACK entries are pairs of `(start, end)` sequence number ranges encoded as LE
u32 values (8 bytes per range). A range covers `start` up to, but not
including, `end`.

The ranges follow the 20-byte header. There are two ways of saying how long
they are, and this crate reads both:

- This crate sets `data_offset` to their length in bytes. `data_offset` is one
  byte, so an acknowledgement carries at most 31 ranges.
- C libudx leaves `data_offset` at zero and sends the ranges as the whole
  payload of a packet that carries nothing else, up to 51 of them.

C libudx reads both forms as well.

## Stream Multiplexing

Multiple streams share a single UDP socket. Incoming packets are demultiplexed
by the `remote_id` field (bytes 4-7), which maps to the receiver's local stream ID.

## Writing

A write goes into the stream's write queue. Packets are cut from the queue as
they are sent, as in C libudx:

- A packet takes as many bytes as the path's MTU allows at that moment, from
  as many writes as that takes. Writes smaller than a packet share one
- A packet gets its sequence number, and the acknowledgement number and
  remote address it carries, when it is sent
- The end of the stream is queued like a write. It rides on the last packet of
  data (`DATA | END`), or goes in a packet of its own when there is none left
- A write is complete when the packet that carries its last byte has been
  acknowledged

`UdxStream::write` returns when its write is complete. The `AsyncWrite`
adapter does not wait: `poll_write` takes the bytes into the queue and
returns, the way a TCP socket takes them into its send buffer. It asks its
writer to wait only while the bytes written and not yet acknowledged exceed
`HIGH_WATERMARK` plus the send window (the smaller of the congestion window
and the remote's receive window), which is C's rule for `udx_stream_write`
and its drain callback. `poll_flush` has nothing to wait for. `poll_shutdown`
queues the end of the stream and returns once it has been acknowledged, and
with it everything written.

An adapter that is dropped without `shutdown` lets go of the stream, and the
stream goes on in the background until everything written has been
acknowledged. It stops earlier if the remote destroys the stream, stops
answering (the retransmission timer gives up), or makes no progress for
`LINGER_TIMEOUT`.

## Congestion Control: BBR

UDX uses BBR (Bottleneck Bandwidth and RTT) congestion control.

### State Machine

```
STARTUP --[bw plateau 3 rounds]--> DRAIN --[inflight <= BDP]--> PROBE_BW
                                                                    |
                                                     every 10s ----+
                                                                    v
                                                               PROBE_RTT
                                                     (200ms, cwnd=4) |
                                                                    |
                                                               back to
                                                               PROBE_BW
```

| State | Pacing Gain | cwnd Gain | Exit Condition |
|-------|-------------|-----------|----------------|
| STARTUP | 2.885 (2/ln2) | 2.885 | BW hasn't grown 25% for 3 rounds |
| DRAIN | 0.347 (1/2.885) | 2.885 | Inflight <= BDP |
| PROBE_BW | cycles [1.25, 0.75, 1.0x6] | 2.0 | Cycles each RTT; PROBE_RTT every 10s |
| PROBE_RTT | 1.0 | -- (cwnd=4 pkts) | After 200ms + 1 RTT with inflight <= 4 |

The window is `cwnd gain * BDP`, where the BDP is the bandwidth estimate times
the lowest round-trip time of the last ten seconds, counted as at least one
millisecond. Once the path's bandwidth is known, the window also grows by how
far acknowledgements have been seen to run ahead of that estimate, up to what
the bandwidth delivers in 100 ms.

### Loss Recovery (CA states)

| State | Value | Description |
|-------|-------|-------------|
| CA_OPEN | 1 | Normal operation |
| CA_RECOVERY | 2 | Repairing losses found from acknowledgements |
| CA_LOSS | 3 | Repairing losses after the retransmission timer fired |

Initial values: `cwnd = 10 packets`, `ssthresh = 0xFFFF`.

Recovery lasts until everything sent before it began has been acknowledged.
While it lasts, lost packets are sent again before new data, and BBR holds the
window to what is in flight plus what each acknowledgement frees.

## Loss Detection (RACK-TLP, RFC 8985)

A packet is lost when a packet sent after it has been delivered, and the
reordering window has passed since then. The reordering window is a quarter of
the lowest round-trip time. It is zero once three packets have been
acknowledged selectively, or during recovery, unless the path has been seen to
reorder packets. A packet sent again counts as sent at that moment, so it is
not declared lost a second time by acknowledgements of packets sent before it.

The loss of the last packets of a flight shows in no later acknowledgement.
After two round trips without one, a tail loss probe goes out: the next packet
waiting to be sent, or else the newest packet again. One probe is in flight at
a time, and none is armed during recovery or while selective acknowledgements
are outstanding.

The retransmission timer is the last resort. It is set again whenever an
acknowledgement delivers something. When it fires, the oldest packet and every
packet whose own round trip has run out are lost, the stream is in CA_LOSS,
and the timer is set to twice the timeout. A stream whose peer never answers
therefore fails 13 seconds after its first packet: 1 s, then 2 s for each of
the six timeouts a packet may have.

## RTT Estimation (Jacobson/Karels)

```
First sample:
  srtt = rtt
  rttvar = rtt / 2

Subsequent samples:
  delta = |srtt - rtt|
  rttvar = (3 * rttvar + delta) / 4
  srtt = (7 * srtt + rtt) / 8

RTO = max(srtt + 4 * rttvar, 1000ms)
RTO capped at 30,000ms
```

Round-trip times are in whole milliseconds. An acknowledgement of a packet
that was sent more than once is not a sample.

A packet may time out 6 times. The stream fails when the timer fires for it
once more.

## MTU Path Discovery

UDX probes the path MTU by sending progressively larger packets.

| Constant | Value | Description |
|----------|-------|-------------|
| MTU_BASE | 1200 | Starting MTU (bytes) |
| MTU_MAX | 1500 | Maximum MTU (bytes) |
| MTU_STEP | 32 | Probe size increment |
| MTU_MAX_PROBES | 3 | Failed probes before settling |

State machine: `BASE --> SEARCH --> SEARCH_COMPLETE`.

Probes reuse the `data_offset` byte to indicate padding size. The payload
scales from 1180 bytes (MTU_BASE - header) up to 1480 bytes (MTU_MAX - header).
Packets are cut when they are sent, so a larger MTU applies from the next
packet on, to what was written before it was found too. The congestion window
counts packets and is left as it is.

## Rate Sampling and Pacing

BBR uses rate sampling to estimate bottleneck bandwidth:

- Each sent packet records the `delivered` count, when delivery last advanced
  (`delivered_ts`), and when the newest acknowledged packet was sent
  (`first_sent_ts`)
- On ACK, delivery rate = `(delivered_now - delivered_at_send) / interval`,
  where the interval is the longer of the time the packets took to send and
  the time they took to be acknowledged
- A sample over an interval shorter than the lowest round-trip time is not used
- A sample taken while the application had nothing to send only counts if it
  raises the estimate
- Windowed max filter tracks peak bandwidth over the last ten round trips
- Pacing rate: `bw * mss * pacing_gain * 0.99`, in bytes per millisecond

The pacer refills on each millisecond of the stream's clock, with the pacing
rate times the milliseconds since the last refill, and a stream sends while
the pacer has anything left. A stream that empties it waits for the next
millisecond.

The intervals of a rate sample are measured in microseconds, where C libudx
measures them in whole milliseconds. Between two programs on one machine or
one network a round trip takes a fraction of a millisecond, and a sample
there, in whole milliseconds, is either unusable or several times too low.

## Relay (Packet Forwarding)

`relay_to` creates a fast-path packet relay between two streams on the same runtime:

1. When a packet arrives on the source stream, the relay rewrites only the
   `remote_id` field (bytes 4-7) to the destination's remote ID
2. The packet is forwarded to the destination's remote address via its UDP socket
3. If the DESTROY flag is set on the relayed packet, the source stream is closed
4. The relay does not participate in congestion control or acknowledgment

Both streams must be on the same `UdxRuntime`.

## Protocol Constants

| Constant | Value | Description |
|----------|-------|-------------|
| HEADER_SIZE | 20 | Fixed header size (bytes) |
| MAGIC | 0xFF | Magic byte |
| VERSION | 1 | Protocol version |
| DEFAULT_RWND_MAX | 4,194,304 | Default max receive window (~4MB) |
| RTO_MAX_MS | 30,000 | Maximum RTO |
| MAX_RTO_TIMEOUTS | 6 | Timeouts of one packet before giving up |
| MAX_SACK_RANGES | 31 | Ranges in one acknowledgement |
| HIGH_WATERMARK | 262,144 | Bytes written and unacknowledged, beyond the send window, before a writer waits |
| LINGER_TIMEOUT | 30 s | How long a dropped stream goes without progress before it stops delivering |

## Known Limitations

This implementation has the following known simplifications relative to the
C reference:

- The buffers of sent and received packets are ordered by raw u32 sequence
  number (`BTreeMap::split_off(&ack)`, `range`), which is not wrap-safe
  across sequence number wraparound in very long-lived streams
- A stream runs on a task of the tokio runtime. What was written is sent when
  that task runs: a program that blocks its runtime's thread, or exits, right
  after a write through the adapter leaves the write unsent. `shutdown`
  returns once everything has arrived
- An acknowledgement lists the gaps nearest the cumulative ACK, up to 31
  ranges here and 51 in C libudx. When a burst of loss leaves more gaps than
  that, the sender learns late of what arrived beyond them. It may send
  again what had arrived, and it counts those deliveries in a later interval,
  which overstates its bandwidth estimate for some round trips. C libudx
  behaves the same way
- `UdxError::Uv(i32)` exists for API compatibility but is never emitted
  by the native backend (only `Io` and `StreamClosed` are used)

## Reference

- C libudx source: <https://github.com/holepunchto/libudx>
- Node.js udx-native: <https://github.com/holepunchto/udx-native>
- BBR paper: <https://dl.acm.org/doi/10.1145/3009824>
- RFC 6298 (TCP RTO): <https://tools.ietf.org/html/rfc6298>
- RFC 2018 (TCP SACK): <https://tools.ietf.org/html/rfc2018>
- RFC 8985 (RACK-TLP loss detection): <https://tools.ietf.org/html/rfc8985>
