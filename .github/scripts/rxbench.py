#!/usr/bin/env python3
"""Compare the receive path of two builds of libudx's `rxbench` example.

    rxbench.py <base binary> <head binary> [--runs N] [--megabytes M] [--out results.csv]

For each configuration, the two builds take turns, so that whatever else the
machine is doing weighs on both alike. Each run starts a receiver and a sender
on the loopback interface, and records the receiver's throughput and the
processor time its process used. Over loopback the sender mostly sets the
pace, so processor time per megabyte is the better measure of what receiving
costs. Prints a Markdown table of medians, with the change and its 95%
bootstrap confidence interval.

Only the Python standard library is used. Processor time comes from `wait4`
on Unix and `GetProcessTimes` on Windows.
"""

import argparse
import csv
import os
import random
import statistics
import subprocess
import sys
import time

CONFIGS = [
    ("read, no limit", []),
    ("read, 8 MiB limit", ["limit=8388608"]),
    ("async, no limit", ["async"]),
    ("async, 8 MiB limit", ["async", "limit=8388608"]),
]
RECV = "127.0.0.1:9101"
SEND = "127.0.0.1:9102"
TIMEOUT = 120


def cpu_seconds_of(proc):
    """Wait for `proc` and return (exit code, processor seconds it used)."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        try:
            proc.wait(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return None, None
        times = [wintypes.FILETIME() for _ in range(4)]
        ok = ctypes.windll.kernel32.GetProcessTimes(
            wintypes.HANDLE(int(proc._handle)), *[ctypes.byref(t) for t in times]
        )
        if not ok:
            return proc.returncode, None

        def seconds(t):
            return ((t.dwHighDateTime << 32) | t.dwLowDateTime) / 1e7

        return proc.returncode, seconds(times[2]) + seconds(times[3])

    deadline = time.monotonic() + TIMEOUT
    while True:
        pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
        if pid:
            proc.returncode = os.waitstatus_to_exitcode(status)
            return proc.returncode, usage.ru_utime + usage.ru_stime
        if time.monotonic() > deadline:
            proc.kill()
            os.wait4(proc.pid, 0)
            proc.returncode = -9
            return None, None
        time.sleep(0.02)


def one_run(binary, megabytes, extra):
    """Throughput (MB/s) and receiver processor seconds, or (None, None)."""
    receiver = subprocess.Popen(
        [binary, "recv", RECV, SEND, str(megabytes), *extra],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(0.3)
    try:
        subprocess.run(
            [binary, "send", SEND, RECV, str(megabytes)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        pass
    code, cpu = cpu_seconds_of(receiver)
    output = receiver.stdout.read()
    receiver.stdout.close()
    mbps = next(
        (float(line.split("mbps=")[1]) for line in output.splitlines() if line.startswith("RESULT")),
        None,
    )
    if code != 0 or mbps is None:
        print(f"run failed (exit {code}): {output.strip()[-300:]}", file=sys.stderr)
        return None, None
    return mbps, cpu


def change(before, after):
    return (statistics.median(after) - statistics.median(before)) / statistics.median(before) * 100


def interval(before, after):
    """Median change in percent, and its 95% bootstrap confidence interval."""
    rng = random.Random(1)
    samples = sorted(
        change([rng.choice(before) for _ in before], [rng.choice(after) for _ in after])
        for _ in range(5000)
    )
    return change(before, after), samples[125], samples[4874]


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("base")
    parser.add_argument("head")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--megabytes", type=int, default=200)
    parser.add_argument("--out", default="rxbench-results.csv")
    args = parser.parse_args()

    rows = []
    for run in range(1, args.runs + 1):
        for name, extra in CONFIGS:
            for build, binary in (("base", args.base), ("head", args.head)):
                mbps, cpu = one_run(binary, args.megabytes, extra)
                rows.append((name, build, run, mbps, cpu))
                print(f"{name},{build},{run},{mbps},{cpu}", flush=True)

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["config", "build", "run", "mbps", "cpu_seconds"])
        writer.writerows(rows)

    megabytes = args.megabytes * 1.048576
    print()
    print(
        "| Receiver | MB/s base | MB/s head | Change [95% CI] "
        "| CPU ms/MB base | CPU ms/MB head | Change [95% CI] | Failed runs |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for name, _ in CONFIGS:
        ok = [r for r in rows if r[0] == name and r[3] is not None and r[4] is not None]
        failed = sum(1 for r in rows if r[0] == name) - len(ok)
        tb = [r[3] for r in ok if r[1] == "base"]
        th = [r[3] for r in ok if r[1] == "head"]
        cb = [r[4] / megabytes * 1000 for r in ok if r[1] == "base"]
        ch = [r[4] / megabytes * 1000 for r in ok if r[1] == "head"]
        if len(tb) < 2 or len(th) < 2:
            print(f"| {name} | – | – | – | – | – | – | {failed} |")
            continue
        t, tlo, thi = interval(tb, th)
        c, clo, chi = interval(cb, ch)
        print(
            f"| {name} | {statistics.median(tb):.0f} | {statistics.median(th):.0f} "
            f"| {t:+.1f}% [{tlo:+.1f}, {thi:+.1f}] "
            f"| {statistics.median(cb):.2f} | {statistics.median(ch):.2f} "
            f"| {c:+.1f}% [{clo:+.1f}, {chi:+.1f}] | {failed} |"
        )


if __name__ == "__main__":
    main()
