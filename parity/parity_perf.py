#!/usr/bin/env python3
"""Quick perf snapshot: /api/flow throughput + best-case latency for both
implementations. Append-only writes to the parity report.

Sequential GETs from a single connection — keeps the noise low and the
numbers comparable across runs without needing a load generator.
"""

from __future__ import annotations
import argparse
import time
import urllib.request


def bench(base: str, duration_s: float = 5.0) -> tuple[int, float, float]:
    """Hammer /api/flow with sequential GETs for `duration_s` seconds.

    Returns (request_count, throughput_rps, best_latency_ms).
    """
    url = base + "/api/flow"
    deadline = time.time() + duration_s
    n = 0
    best_ms = float("inf")
    while time.time() < deadline:
        t0 = time.time()
        with urllib.request.urlopen(url, timeout=5) as resp:
            resp.read()
        elapsed_ms = (time.time() - t0) * 1000.0
        if elapsed_ms < best_ms:
            best_ms = elapsed_ms
        n += 1
    actual_s = duration_s
    return n, n / actual_s, best_ms


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csharp", required=True)
    ap.add_argument("--go", required=True)
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    lines: list[str] = []
    for name, base in [("csharp", args.csharp), ("go", args.go)]:
        # Warm-up so JIT-y first hits don't skew the best-latency number.
        urllib.request.urlopen(base + "/api/flow", timeout=5).read()
        n, rps, best_ms = bench(base)
        line = f"{name:<7} {int(rps):6d} req/s  best={best_ms:.1f}ms  (n={n})"
        lines.append(line)
        print(line)

    out = "\n".join(lines) + "\n"
    with open(args.report, "a") as f:
        f.write(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
