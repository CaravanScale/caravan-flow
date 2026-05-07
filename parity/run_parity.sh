#!/usr/bin/env bash
# parity harness — boots zinc-flow-csharp and zinc-flow-go side by side
# against the same flow config and diffs every read-only endpoint's
# response shape. Mutation endpoints are exercised separately by
# parity_mutations.py (also in this dir) which runs them in lock-step
# and verifies state convergence.
#
# Usage:
#   bash run_parity.sh
#
# Exits 0 when shapes match across every endpoint, 1 on any divergence.
# Writes a per-endpoint report to ./out/report.txt.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CSHARP_BIN="$ROOT/zinc-flow-csharp/build/ZincFlow"
GO_SRC="$ROOT/zinc-flow-go"
HARNESS_DIR="$ROOT/parity"
WORK="$HARNESS_DIR/out"
CSHARP_PORT=19091
GO_PORT=19092

mkdir -p "$WORK"
rm -rf "$WORK"/*.log "$WORK"/csharp.cwd "$WORK"/go.cwd "$WORK"/report.txt

# --- Build go binary -----------------------------------------------------------
# Stale-bin detection by mtime is unreliable across all .zn files in the
# tree; just rebuild every run. zinc build + go build are both fast enough
# (< 5s combined on a warm cache) that the simplicity is worth it.
GO_BIN="$WORK/zinc-flow-go.bin"
echo "[parity] building zinc-flow-go..."
(cd "$GO_SRC" && /tmp/zinc build >/dev/null)
(cd "$GO_SRC/zinc-out" && go build -o "$GO_BIN" .)
if [[ ! -x "$CSHARP_BIN" ]]; then
    echo "[parity] csharp binary missing at $CSHARP_BIN — build with"
    echo "         dotnet publish -c Release -r linux-x64 --self-contained true"
    echo "         under zinc-flow-csharp/ZincFlow/ first"
    exit 1
fi

# --- Per-impl working dirs with port-rewritten config.yaml -------------------
prepare_cwd() {
    local target="$1" port="$2"
    rm -rf "$target" && mkdir -p "$target"
    sed "s/^  port:.*/  port: $port/" "$HARNESS_DIR/parity_config.yaml" > "$target/config.yaml"
}
prepare_cwd "$WORK/csharp.cwd" "$CSHARP_PORT"
prepare_cwd "$WORK/go.cwd"     "$GO_PORT"

# --- Boot both -----------------------------------------------------------------
echo "[parity] starting csharp on :$CSHARP_PORT"
(cd "$WORK/csharp.cwd" && "$CSHARP_BIN" --mode=headless > "$WORK/csharp.log" 2>&1) &
CSHARP_PID=$!

echo "[parity] starting go on :$GO_PORT"
(cd "$WORK/go.cwd" && "$GO_BIN" > "$WORK/go.log" 2>&1) &
GO_PID=$!

cleanup() {
    kill "$CSHARP_PID" "$GO_PID" 2>/dev/null
    # Don't wait — children can hang on SIGTERM (Go panic stack, csharp
    # graceful drain) and we don't need their exit codes; let the OS reap.
}
trap cleanup EXIT

# Wait for both to bind
wait_ready() {
    local port="$1" name="$2" tries=0
    until curl -sf "http://127.0.0.1:$port/readyz" >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [[ $tries -gt 60 ]]; then
            echo "[parity] $name didn't come up on :$port after 60s"
            return 1
        fi
        sleep 1
    done
}
wait_ready "$CSHARP_PORT" csharp || { echo "[parity] csharp logs:"; tail -40 "$WORK/csharp.log"; exit 1; }
wait_ready "$GO_PORT"     go     || { echo "[parity] go logs:";     tail -40 "$WORK/go.log";     exit 1; }

# --- Diff endpoint shapes ------------------------------------------------------
DIFF_RC=0
python3 "$HARNESS_DIR/parity_diff.py" \
    --csharp "http://127.0.0.1:$CSHARP_PORT" \
    --go     "http://127.0.0.1:$GO_PORT" \
    --report "$WORK/report.txt" || DIFF_RC=$?

# --- Binary size + perf measurement -------------------------------------------
# Reported alongside the shape diff so the report doubles as an "is the Go
# track actually meeting its AOT pitch?" snapshot. csharp ships a
# self-contained linux-x64 build (.NET runtime bundled); Go ships a single
# stripped ELF. Boot time is ready-when /readyz returns 200; we time it from
# spawn to first 200, but since both are already running here we measure
# latency-to-first-flow plus a simple req/sec on /api/flow.
{
    echo ""
    echo "=== Binary size ==="
    CS_SIZE=$(stat -c '%s' "$CSHARP_BIN")
    GO_SIZE=$(stat -c '%s' "$GO_BIN")
    CS_MB=$(awk "BEGIN { printf \"%.1f\", $CS_SIZE/1048576 }")
    GO_MB=$(awk "BEGIN { printf \"%.1f\", $GO_SIZE/1048576 }")
    printf "csharp  %10d bytes (%5s MB)\n" "$CS_SIZE" "$CS_MB"
    printf "go      %10d bytes (%5s MB)\n" "$GO_SIZE" "$GO_MB"
    echo ""
    echo "=== /api/flow throughput (5s sequential GETs) ==="
} >> "$WORK/report.txt"

python3 "$HARNESS_DIR/parity_perf.py" \
    --csharp "http://127.0.0.1:$CSHARP_PORT" \
    --go     "http://127.0.0.1:$GO_PORT" \
    --report "$WORK/report.txt" || true

cat "$WORK/report.txt"
exit "$DIFF_RC"
