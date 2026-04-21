#!/usr/bin/env bash
# Build the Crystal worker inside the Alpine+Crystal container, then
# extract the static binary into ./bin/ so it can run directly on the
# host. Output is fully portable — no brew / glibc dance required.

set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE_TAG="caravan-flow-crystal:build"

echo "→ docker build (Alpine + Crystal 1.20 + --static)..."
docker build --target build -t "$IMAGE_TAG" -f Dockerfile .

echo "→ extracting static binary..."
mkdir -p bin
container_id="$(docker create "$IMAGE_TAG")"
trap 'docker rm "$container_id" >/dev/null 2>&1 || true' EXIT
docker cp "$container_id:/out/caravan-flow" bin/caravan-flow
chmod +x bin/caravan-flow

echo
echo "→ verify:"
file bin/caravan-flow
ls -lh bin/caravan-flow
echo
echo "→ run with: ./bin/caravan-flow config.yml"
