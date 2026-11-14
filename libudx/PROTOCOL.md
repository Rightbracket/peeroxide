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

Sequence and acknowledgment numbers wrap: zero follows `u32::MAX`, and a
stream carries on across the wrap in both directions. Two numbers are compared
as C libudx compares them (`seq_compare`): the later one is the one that is
less than 2^31 ahead of the other. The buffers of sent and of received packets
are keyed by number, and are read in sequence order where what they hold runs
across the wrap.

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

The ranges follow the 20-byte header in sequence order, the one nearest the
ACK number first. There are two ways of saying how long they are:

- C libudx leaves `data_offset` at zero and sends the ranges as the whole
  payload of a packet that carries nothing else. It sends up to 51 of them,
  and reads as many as a packet holds.
- This crate, up to 1.7.4, set `data_offset` to their length in bytes, and
  read only ranges announced that way. `data_offset` is one byte, which is
  room for 31 ranges.

This crate and C libudx read both. An acknowledgement from this crate carries
as many ranges as fit a packet of the starting MTU: 144 over IPv4, 141 over
IPv6. With up to 31 ranges it also says their length in `data_offset`, for
its older releases. With more it leaves `data_offset` at zero, as C libudx
does.

The number of ranges sets how fast a burst of losses is repaired. A sender
learns of a gap from the ranges around it, and the ranges are the lowest
ones, so what it can repair in one round trip is what one acknowledgement
names. Through a path of 100 Mbit/s with a 40 ms round trip, where the end of
Startup overflows the queue by a thousand packets or more, 10 MB between two
ends of this crate went from 7.8 MB/s with 31 ranges to 9.9 with 144, and
with 1% of packets lost as well, from 7.7 to 9.4. A `udx-native` sender into
this crate went from 8.5 to 9.6 MB/s and from 8.8 to 9.4, and with the 1%
loss it sent 86 packets again where it had sent 741. Two `udx-native` ends
reach 8.6 and 8.7 MB/s there. The acknowledgements of a transfer came to
between 1.3 and 2.1 MB before and after: they are longer, and the repair
they are needed for is over sooner.

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
STARTUP --[path counts as full]--> DRAIN --[inflight <= BDP]--> PROBE_BW
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
| STARTUP | 2.885 (2/ln2) | 2.885 | BW hasn't grown 25% for 3 rounds, or a round shows a queue and loss |
| DRAIN | 0.347 (1/2.885) | 2.885 | Inflight <= BDP |
| PROBE_BW | cycles [1.25, 0.75, 1.0x6] | 2.0 | Cycles each RTT; PROBE_RTT every 10s |
| PROBE_RTT | 1.0 | -- (cwnd=4 pkts) | After 200ms + 1 RTT with inflight <= 4 |

The window is `cwnd gain * BDP`, where the BDP is the bandwidth estimate times
the lowest round-trip time of the last ten seconds, counted as at least one
millisecond. Once the path's bandwidth is known, the window also grows by how
far acknowledgements have been seen to run ahead of that estimate, up to what
the bandwidth delivers in 100 ms. Only packets that were delivered in time
count as running ahead: see Rate Sampling and Pacing.

Startup takes the path for full in one of two ways. The first is C libudx's:
the bandwidth estimate has gone three round trips without growing by a
quarter. The second is not in C libudx: a round trip has shown both a queue
at the bottleneck and loss.

- A queue: the lowest round-trip time of that round trip is above the
  path's lowest by an eighth of it, and by no less than 4 ms and no more
  than 16. This is how HyStart++ reads a queue (RFC 9406).
- Loss: in loss recovery, more than 2% of what was in flight was lost, with
  at least eight acknowledgements revealing loss. This is how BBRv2 reads
  loss in Startup.

Startup keeps up to 2.89 times what the path holds in flight, so a path
that queues less than the rest drops it, and the first rule takes at least
three more round trips to notice, during which the queue goes on
overflowing. The second rule takes one.

BBRv2 ends Startup on the loss alone. Loss at random above 2% then ends it
long before the path is full, and BBR finds the rest of the bandwidth a
quarter at a time: through a path with a round trip of 100 ms that lost 5%
of its packets, a transfer between two ends of this crate was 29% slower
for it. Loss at random builds no queue, and a queue that lasts a round trip
says that the bottleneck was busy throughout, so the estimate holds what
the path carries. The round trips of the second rule are counted apart from
BBR's own, which start afresh whenever recovery begins.

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

An MTU is the size of a whole IP packet, as in C libudx: the IP header (20
bytes for IPv4, 40 for IPv6), the UDP header (8), the UDX header (20) and the
payload. The payload of a full packet therefore goes from 1152 bytes at
MTU_BASE to 1452 at MTU_MAX over IPv4, and from 1132 to 1432 over IPv6. A
remote reached at an IPv4 address mapped into IPv6 is reckoned as IPv6.

State machine: `BASE --> SEARCH --> SEARCH_COMPLETE`.

A probe is a data packet padded up to the size being tried. Its `data_offset`
byte says how much padding precedes the payload. A probe that is acknowledged
raises the MTU to its size, and the next size is tried. A probe that is lost
goes out again without its padding, and after MTU_MAX_PROBES lost probes the
search ends.

The search needs a packet that is too large to be dropped, not split. On
Linux a socket's packets are therefore marked "don't fragment" when it is
bound, as C libudx marks them: `IP_MTU_DISCOVER` is set to
`IP_PMTUDISC_PROBE`, and `IPV6_MTU_DISCOVER` to `IPV6_PMTUDISC_PROBE`. The
mark covers all of the socket's packets, datagrams sent with
`UdxSocket::send_to` included. On connecting, a stream there also asks the
system for the MTU of the link its remote is reached over, and searches no
further than that: a larger packet would not leave the machine. If that MTU
is MTU_BASE or less, there is no search.

C libudx marks the packets on every system, and asks for the link's MTU on
Linux and Windows. This crate contains no `unsafe` code, and only for Linux
is there a safe call for either, in rustix: neither the standard library,
tokio nor socket2 has one for any system. On the other systems a probe that
is too large for the path therefore still arrives, in fragments, and the
search ends at MTU_MAX whatever the path. A full packet is then an IP packet
of 1500 bytes: whole on a path that carries 1500, and in two fragments on a
narrower one.

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

A rate sample counts the packets that were delivered in its interval, as far
as their acknowledgement can tell, where C libudx counts every packet the
acknowledgement names. An acknowledgement names a limited number of ranges,
the lowest ones (see SACK Format). After a burst of losses the remote
therefore reports, as the gaps below them close, hundreds of packets at once
that had arrived many round trips before. Counted as deliveries of that
moment, such reports were seen to put the bandwidth estimate at two and a
half times what the path carried, and the estimate holds for ten round
trips, in which the stream overfills the queue again. So:

- A packet that was sent before one that an earlier acknowledgement had
  already acknowledged was reported late. It counts as delivered, for the
  window and for the round trips, and is left out of rate samples and of the
  count of acknowledgements that run ahead of the estimate
- The packets of one acknowledgement do not make each other late
- An acknowledgement that comes sooner after a packet was sent again than
  the lowest round-trip time answers the earlier sending. The packet is
  placed by when it was first sent, and no sample starts from it

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
| SACK ranges | 144 over IPv4, 141 over IPv6 | Ranges in one acknowledgement: what fits a packet of MTU_BASE |
| HIGH_WATERMARK | 262,144 | Bytes written and unacknowledged, beyond the send window, before a writer waits |
| LINGER_TIMEOUT | 30 s | How long a dropped stream goes without progress before it stops delivering |

## Known Limitations

This implementation has the following known simplifications relative to the
C reference:

- A stream runs on a task of the tokio runtime. What was written is sent when
  that task runs: a program that blocks its runtime's thread, or exits, right
  after a write through the adapter leaves the write unsent. `shutdown`
  returns once everything has arrived
- An acknowledgement lists the gaps nearest the cumulative ACK, up to 144
  ranges here and 51 in C libudx. When a burst of loss leaves more gaps than
  that, the sender learns late of what arrived beyond them, and may send
  again what had arrived. It leaves those late reports out of its rate
  samples (see Rate Sampling and Pacing). C libudx counts them, which
  overstates its bandwidth estimate for some round trips
- A loss recovery in Startup that does not end Startup leaves the
  congestion window at 65,535 packets until the next loss, so only the
  pacer holds the stream back for that while. C libudx does the same.
  Capping the window there, or saving and restoring it as Linux's BBR does,
  was measured and made transfers no faster
- The retransmission timeout is never under a second, as in C libudx. A
  flight whose last packets were all lost during recovery is repaired only
  when it fires, since no tail loss probe is armed then
- There is no zero-window probe. A remote that advertised a receive window
  of zero would hold a sender here until that remote's next packet. C
  libudx probes. No remote was seen to advertise one: `udx-native` took
  31.5 MB with nothing reading it
- A socket's packets are marked "don't fragment" on Linux only, for want of
  a safe call on the other systems (see MTU Path Discovery). There a stream
  on a path that carries less than 1500 bytes settles on 1500, and its full
  packets travel as two fragments each. C libudx marks them on every system
- `UdxError::Uv(i32)` exists for API compatibility but is never emitted
  by the native backend (only `Io` and `StreamClosed` are used)

## Reference

- C libudx source: <https://github.com/holepunchto/libudx>
- Node.js udx-native: <https://github.com/holepunchto/udx-native>
- BBR paper: <https://dl.acm.org/doi/10.1145/3009824>
- RFC 6298 (TCP RTO): <https://tools.ietf.org/html/rfc6298>
- RFC 2018 (TCP SACK): <https://tools.ietf.org/html/rfc2018>
- RFC 8985 (RACK-TLP loss detection): <https://tools.ietf.org/html/rfc8985>
