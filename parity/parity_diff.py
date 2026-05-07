#!/usr/bin/env python3
"""Compare zinc-flow-csharp vs zinc-flow-go API responses by structural shape.

We don't compare values — those legitimately diverge (timestamps, FlowFile
ids, processed counts, etc). What MUST match for parity is response shape:
the set of keys, nested types, and the per-element type of any list. A
divergence here means the dashboard would see a missing/renamed field
when pointed at one impl vs the other.
"""

from __future__ import annotations
import argparse
import json
import sys
import urllib.request
from typing import Any


# Read-only endpoints worth diffing. Path-templated routes (`{name}` etc.)
# are skipped here — they're covered by the mutation harness which seeds
# state first.
ENDPOINTS = [
    "/readyz",
    "/api/flow",
    "/api/flow/status",
    "/api/processors",
    "/api/registry",
    "/api/stats",
    "/api/processor-stats",
    "/api/edge-stats",
    "/api/connections",
    "/api/sources",
    "/api/providers",
    "/api/provenance",
    "/api/provenance/failures",
    "/api/vc/status",
    "/api/overlays",
    "/api/layout",
]


def fetch(base: str, path: str) -> tuple[int, Any]:
    req = urllib.request.Request(base + path, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read()
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, body.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body.decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return -1, f"<error: {e}>"


def shape(value: Any) -> Any:
    """Reduce a JSON value to its structural shape.

    Dicts → {key: shape(value)} with sorted keys.
    Lists → ["empty"] for empty lists, [shape(first)] for non-empty
            (assumes uniform element type — the case in every API
            response we ship).
    Primitives → their type name. None → "null".
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        if not value:
            return ["empty"]
        return [shape(value[0])]
    if isinstance(value, dict):
        return {k: shape(value[k]) for k in sorted(value.keys())}
    return type(value).__name__


def diff_shapes(csharp: Any, go: Any, path: str = "") -> list[str]:
    """Return a list of human-readable mismatch lines, [] when equal."""
    if type(csharp) is not type(go):
        return [f"{path or '<root>'}: type {type(csharp).__name__} vs {type(go).__name__}"]
    if isinstance(csharp, dict):
        diffs: list[str] = []
        cs_keys = set(csharp.keys())
        go_keys = set(go.keys())
        for missing in sorted(cs_keys - go_keys):
            diffs.append(f"{path}.{missing}: in csharp, missing in go")
        for extra in sorted(go_keys - cs_keys):
            diffs.append(f"{path}.{extra}: in go, missing in csharp")
        for k in sorted(cs_keys & go_keys):
            diffs.extend(diff_shapes(csharp[k], go[k], f"{path}.{k}"))
        return diffs
    if isinstance(csharp, list):
        if not csharp and not go:
            return []
        if not csharp or not go:
            return [f"{path or '<root>'}[]: csharp={csharp} go={go}"]
        return diff_shapes(csharp[0], go[0], f"{path}[0]")
    if csharp != go:
        return [f"{path or '<root>'}: csharp={csharp!r} go={go!r}"]
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csharp", required=True)
    ap.add_argument("--go", required=True)
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    lines: list[str] = []
    failures = 0
    for ep in ENDPOINTS:
        cs_code, cs_body = fetch(args.csharp, ep)
        go_code, go_body = fetch(args.go, ep)

        if cs_code != go_code:
            failures += 1
            lines.append(f"[FAIL] {ep}: status csharp={cs_code} go={go_code}")
            lines.append(f"       csharp: {repr(cs_body)[:200]}")
            lines.append(f"       go:     {repr(go_body)[:200]}")
            continue

        cs_shape = shape(cs_body)
        go_shape = shape(go_body)
        diffs = diff_shapes(cs_shape, go_shape)
        if diffs:
            failures += 1
            lines.append(f"[FAIL] {ep}: shape divergence ({cs_code})")
            for d in diffs:
                lines.append(f"       {d}")
        else:
            lines.append(f"[ OK ] {ep}: {cs_code}")

    summary = f"\n{len(ENDPOINTS) - failures}/{len(ENDPOINTS)} endpoints match"
    lines.append(summary)
    out = "\n".join(lines) + "\n"
    print(out)
    with open(args.report, "w") as f:
        f.write(out)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
