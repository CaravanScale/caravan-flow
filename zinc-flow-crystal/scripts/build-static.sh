#!/usr/bin/env bash
# Build the Crystal worker inside the Alpine+Crystal container, then
# extract the static binary into ./bin/ so it can run directly on the
# host. Output is fully portable — no brew / glibc dance required.
#
# Runs from the repo root so the sibling `crystal-avro` path: dep is
# reachable. The Dockerfile stays under zinc-flow-crystal/ for
# locality.

set -euo pipefail

# Locate repo root (parent of the script's parent).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRYSTAL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$CRYSTAL_DIR/.." && pwd)"

IMAGE_TAG="zinc-flow-crystal:build"

echo "→ docker build from $REPO_ROOT (context spans zinc-flow-crystal + crystal-avro)"
docker build --target build -t "$IMAGE_TAG" -f "$CRYSTAL_DIR/Dockerfile" "$REPO_ROOT"

echo "→ extracting static binary..."
mkdir -p "$CRYSTAL_DIR/bin"
container_id="$(docker create "$IMAGE_TAG")"
trap 'docker rm "$container_id" >/dev/null 2>&1 || true' EXIT
docker cp "$container_id:/out/zinc-flow" "$CRYSTAL_DIR/bin/zinc-flow"
chmod +x "$CRYSTAL_DIR/bin/zinc-flow"

echo
echo "→ verify:"
file "$CRYSTAL_DIR/bin/zinc-flow"
ls -lh "$CRYSTAL_DIR/bin/zinc-flow"
echo
echo "→ run with: ./bin/zinc-flow config.yml"
