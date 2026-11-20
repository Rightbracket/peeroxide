#!/usr/bin/env python3
"""Chart how a path's round trip, jitter and loss shape a stream's throughput.

    pathsweep.py [--rtt 0:5:500] [--jitter 0] [--loss 0] [--limit none]
                 [--runs 5] [--seconds 15] [--rate MBIT] [--queue PACKETS]
                 [--impl head] [--impl origin/main] [--impl udx-native>head] ...
                 [--out sweep.csv]

Each of --rtt and --jitter (milliseconds) and --loss (percent) takes one value,
a comma-separated list, or start:step:end, and --limit a comma-separated list of the receiver's read
buffer limits (`none`, or bytes with an optional K or M suffix, of 1024), which
set the window it advertises: about the limit, less what it holds. The sweep
runs every combination of them, --runs times
each, the implementations taking turns so that whatever else the machine is
doing weighs on all of them alike. The defaults sweep the round trip from 0
to 500 ms in steps of 5, without jitter or loss, five runs each: about two
hours for one implementation.

An implementation is `head` (this checkout), `udx-native`, or any git ref,
which is built with this checkout's `libudx/examples/pathbench.rs` copied in.
A local branch such as `main` may be behind its remote: `origin/main` is not.
`A>B` sends from A into B; `A` alone is `A>A`.

Each run carries data one way, from a sender in one network namespace to a
receiver in another, over a veth pair. netem delays each direction by half
the round trip. Jitter, loss and --rate apply to the data direction only;
jitter uses netem's default distribution, and reorders packets once it is
larger than the time between them. The receiver reports how many bytes have
arrived every 100 ms from the first, for --seconds. Startup, which takes some
round trips to find what the path carries, falls in the first half, and the
steady rate is that of the second half: give --seconds at least 30 round
trips. A row:

  sender, receiver     the implementations
  rtt_ms, jitter_ms, loss_pct, rate_mbit
  read_limit           the receiver's read buffer limit in bytes, or empty for
                       none. A udx-native receiver has none, and ignores it
  run                  1 to --runs
  first_half_MBps      10^6 bytes a second, over the first half
  steady_MBps          over the second half
  wire_per_delivered   bytes the sender put on the wire, including those netem
                       dropped, for each byte of data delivered, over the
                       second half. Headers alone make it about 1.03; the rest
                       is what was sent more than once
  status               ok, no-data, short or timeout; the rates are empty
                       unless it is ok

Progress goes to stderr, the CSV to stdout or --out.

It needs Linux, root, iproute2, cargo, and for `udx-native`, node with
`tests/node/node_modules` installed (`cd tests/node && npm ci`, on Linux).
Elsewhere, run it in a container, with a cap on its memory:

    docker build -t peeroxide-pathsweep -f .github/scripts/pathsweep.Dockerfile .github/scripts
    GIT=$(git rev-parse --path-format=absolute --git-common-dir)
    docker run --rm --privileged --memory=4g --memory-swap=4g --cpus=4 \\
        -v "$PWD":/src -v "$GIT":"$GIT":ro \\
        -v peeroxide-pathsweep-nm:/src/tests/node/node_modules \\
        -v peeroxide-pathsweep-work:/work -w /src peeroxide-pathsweep \\
        sh -c 'cd tests/node && npm ci --silent && cd /src &&
               python3 .github/scripts/pathsweep.py --work /work --out /src/sweep.csv'

Builds go to --work, outside the checkout, so a container's builds do not
overwrite the host's. The git directory is mounted where the checkout expects
it, which a worktree needs for building a git ref. Only the Python standard library is used.
"""

import argparse
import csv
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXAMPLE = os.path.join(REPO, "libudx", "examples", "pathbench.rs")
NODE_BENCH = os.path.join(REPO, ".github", "scripts", "pathbench.js")
NS_RECV, NS_SEND = "pathsweep-r", "pathsweep-s"
DEV_RECV, DEV_SEND = "pswr", "psws"
IP_RECV, IP_SEND = "10.77.0.1", "10.77.0.2"
COLUMNS = [
    "sender", "receiver", "rtt_ms", "jitter_ms", "loss_pct", "rate_mbit", "read_limit", "run",
    "first_half_MBps", "steady_MBps", "wire_per_delivered", "status",
]


def log(message):
    print(message, file=sys.stderr, flush=True)


def run(*cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def values(spec):
    """`a`, `a,b,...` or `start:step:end`, inclusive, as floats."""
    if "," in spec:
        return [v for part in spec.split(",") for v in values(part)]
    parts = [float(p) for p in spec.split(":")]
    if len(parts) == 1:
        return parts
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"{spec!r}: one value or start:step:end")
    start, step, end = parts
    if step <= 0:
        return [start]
    count = int(round((end - start) / step)) + 1
    return [round(start + i * step, 6) for i in range(count) if start + i * step <= end + 1e-9]


def limits(spec):
    """`none`, or bytes with an optional K or M suffix, comma-separated."""
    out = []
    for part in spec.split(","):
        part = part.strip().upper()
        if part in ("", "NONE"):
            out.append(None)
            continue
        scale = {"K": 1024, "M": 1024 * 1024}.get(part[-1], 1)
        out.append(int(float(part.rstrip("KM")) * scale))
    return out


def number(x):
    return f"{x:g}"


def build(name, work):
    """The command that runs implementation `name`."""
    if name == "udx-native":
        modules = os.path.join(REPO, "tests", "node", "node_modules")
        if not os.path.isdir(os.path.join(modules, "udx-native")):
            sys.exit(f"udx-native is not installed in {modules}: cd tests/node && npm ci")
        os.environ["NODE_PATH"] = modules
        return ["node", NODE_BENCH]

    if name == "head":
        src = REPO
    else:
        git = ["git", "-c", "safe.directory=*", "-C", REPO]
        resolved = subprocess.run(
            [*git, "rev-parse", "--verify", "--quiet", name + "^{commit}"],
            capture_output=True, text=True,
        )
        if resolved.returncode != 0:
            sys.exit(f"{name!r} is not a commit in {REPO}, or its git directory is not reachable")
        log(f"{name} is {resolved.stdout.strip()[:12]}")
        src = os.path.join(work, "src-" + re.sub(r"[^A-Za-z0-9._-]", "_", name))
        shutil.rmtree(src, ignore_errors=True)
        os.makedirs(src)
        archive = subprocess.Popen([*git, "archive", name], stdout=subprocess.PIPE)
        run("tar", "-x", "-C", src, stdin=archive.stdout)
        if archive.wait() != 0:
            sys.exit(f"git archive {name} failed")
        shutil.copy(EXAMPLE, os.path.join(src, "libudx", "examples", "pathbench.rs"))

    target = os.path.join(work, "target-" + re.sub(r"[^A-Za-z0-9._-]", "_", name))
    log(f"building {name}")
    run(
        "cargo", "build", "-q", "--release", "--locked", "-p", "libudx",
        "--example", "pathbench", cwd=src, env={**os.environ, "CARGO_TARGET_DIR": target},
    )
    return [os.path.join(target, "release", "examples", "pathbench")]


def setup_namespaces():
    teardown_namespaces()
    run("ip", "netns", "add", NS_RECV)
    run("ip", "netns", "add", NS_SEND)
    run("ip", "link", "add", DEV_RECV, "netns", NS_RECV, "type", "veth",
        "peer", "name", DEV_SEND, "netns", NS_SEND)
    run("ip", "-n", NS_RECV, "addr", "add", f"{IP_RECV}/24", "dev", DEV_RECV)
    run("ip", "-n", NS_SEND, "addr", "add", f"{IP_SEND}/24", "dev", DEV_SEND)
    run("ip", "-n", NS_RECV, "link", "set", DEV_RECV, "up")
    run("ip", "-n", NS_SEND, "link", "set", DEV_SEND, "up")


def teardown_namespaces():
    for ns in (NS_RECV, NS_SEND):
        subprocess.run(["ip", "netns", "del", ns], stderr=subprocess.DEVNULL)


def shape(rtt_ms, jitter_ms, loss_pct, rate_mbit, queue_packets):
    half_us = int(round(rtt_ms * 500))
    data = ["limit", str(queue_packets), "delay", f"{half_us}us"]
    if jitter_ms > 0:
        data.append(f"{int(round(jitter_ms * 1000))}us")
    if loss_pct > 0:
        data += ["loss", f"{loss_pct}%"]
    if rate_mbit:
        data += ["rate", f"{rate_mbit}mbit"]
    acks = ["limit", str(queue_packets), "delay", f"{half_us}us"]
    run("ip", "netns", "exec", NS_SEND, "tc", "qdisc", "replace", "dev", DEV_SEND, "root", "netem", *data)
    run("ip", "netns", "exec", NS_RECV, "tc", "qdisc", "replace", "dev", DEV_RECV, "root", "netem", *acks)


def offered_bytes():
    """Bytes the sender handed its interface: what netem sent, and an
    estimate of what it dropped, at the average size of what it sent."""
    out = subprocess.run(
        ["ip", "netns", "exec", NS_SEND, "tc", "-s", "qdisc", "show", "dev", DEV_SEND],
        capture_output=True, text=True, check=True,
    ).stdout
    m = re.search(r"Sent (\d+) bytes (\d+) pkt \(dropped (\d+)", out)
    sent, packets, dropped = (int(g) for g in m.groups())
    return sent + (dropped * sent / packets if packets else 0)


def transfer(sender, receiver, port, seconds, rtt_ms, limit):
    """One run. Returns (first half, steady, wire per delivered, status)."""
    recv_cmd = ["ip", "netns", "exec", NS_RECV, *receiver, "recv",
                f"{IP_RECV}:{port}", f"{IP_SEND}:{port + 1}", str(seconds)]
    if limit is not None:
        recv_cmd.append(f"limit={limit}")
    send_cmd = ["ip", "netns", "exec", NS_SEND, *sender, "send",
                f"{IP_SEND}:{port + 1}", f"{IP_RECV}:{port}", str(seconds)]
    recv = subprocess.Popen(recv_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    lines = queue.Queue()
    threading.Thread(target=lambda: [lines.put(l) for l in recv.stdout] + [lines.put(None)], daemon=True).start()
    time.sleep(0.3)
    send = subprocess.Popen(send_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    samples = []
    half = None
    status = "ok"
    first_deadline = time.monotonic() + 30 + 4 * rtt_ms / 1000
    deadline = None
    try:
        while True:
            limit = deadline if deadline else first_deadline
            try:
                line = lines.get(timeout=max(0.0, limit - time.monotonic()))
            except queue.Empty:
                status = "timeout" if samples else "no-data"
                break
            if line is None:
                break
            parts = line.split()
            if len(parts) != 3 or parts[0] != "AT":
                continue
            at, received = float(parts[1]), int(parts[2])
            if not samples:
                deadline = time.monotonic() + seconds + 10
            samples.append((at, received))
            if half is None and at >= seconds / 2:
                half = (at, received, offered_bytes())
        end_offered = offered_bytes()
    finally:
        for proc in (recv, send):
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    if status == "ok" and (not samples or samples[-1][0] < seconds - 0.05 or half is None):
        status = "short" if samples else "no-data"
    if status != "ok":
        return None, None, None, status
    end_at, end_received = samples[-1]
    half_at, half_received, half_offered = half
    first = half_received / half_at / 1e6
    delivered = end_received - half_received
    steady = delivered / (end_at - half_at) / 1e6
    wire = (end_offered - half_offered) / delivered if delivered else None
    return first, steady, wire, status


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rtt", type=values, default=values("0:5:500"), help="ms: a, a,b,... or start:step:end")
    parser.add_argument("--jitter", type=values, default=values("0"), help="ms, or start:step:end")
    parser.add_argument("--loss", type=values, default=values("0"), help="percent, or start:step:end")
    parser.add_argument("--limit", type=limits, default=[None], help="read buffer limits, e.g. none,256K,8M")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seconds", type=float, default=15.0)
    parser.add_argument("--rate", type=float, default=0, help="Mbit/s of the data direction; 0 for none")
    parser.add_argument("--queue", type=int, default=100000, help="netem's queue, in packets")
    parser.add_argument("--impl", action="append", dest="impls", help="name or SENDER>RECEIVER; repeatable")
    parser.add_argument("--work", default=os.path.join(tempfile.gettempdir(), "pathsweep"))
    parser.add_argument("--out")
    args = parser.parse_args()

    if not sys.platform.startswith("linux") or os.geteuid() != 0:
        sys.exit("pathsweep needs Linux and root: see the docstring for running it in a container")

    pairs = []
    for spec in args.impls or ["head"]:
        sender, _, receiver = spec.partition(">")
        pairs.append((sender, receiver or sender))
    os.makedirs(args.work, exist_ok=True)
    commands = {name: build(name, args.work) for name in sorted({n for pair in pairs for n in pair})}

    out = open(args.out, "w", newline="") if args.out else sys.stdout
    writer = csv.writer(out)
    writer.writerow(COLUMNS)
    out.flush()

    setups = [(r, j, l, b) for r in args.rtt for j in args.jitter for l in args.loss for b in args.limit]
    total = len(setups) * args.runs * len(pairs)
    done = 0
    port = 20000
    setup_namespaces()
    try:
        shaped = None
        for rtt_ms, jitter_ms, loss_pct, limit in setups:
            if shaped != (rtt_ms, jitter_ms, loss_pct):
                shape(rtt_ms, jitter_ms, loss_pct, args.rate, args.queue)
                shaped = (rtt_ms, jitter_ms, loss_pct)
            for run_no in range(1, args.runs + 1):
                for sender, receiver in pairs:
                    port = 20000 + (port - 20000 + 2) % 40000
                    first, steady, wire, status = transfer(
                        commands[sender], commands[receiver], port, args.seconds, rtt_ms, limit
                    )
                    fmt = lambda x, d: "" if x is None else f"{x:.{d}f}"
                    writer.writerow([
                        sender, receiver, number(rtt_ms), number(jitter_ms), number(loss_pct),
                        number(args.rate), "" if limit is None else limit, run_no, fmt(first, 2), fmt(steady, 2), fmt(wire, 3), status,
                    ])
                    out.flush()
                    done += 1
                    log(f"[{done}/{total}] {sender}>{receiver} rtt={number(rtt_ms)} "
                        f"jitter={number(jitter_ms)} loss={number(loss_pct)} limit={limit} run {run_no}: "
                        f"{fmt(steady, 2) or status} MB/s steady")
    finally:
        teardown_namespaces()
        if args.out:
            out.close()


if __name__ == "__main__":
    main()
