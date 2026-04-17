#!/usr/bin/env bash
# build-ui.sh — two-step build: publish CaravanFlow.UI Blazor bundle,
# copy into the worker's content root, then build the worker AOT binary.
#
# Why two steps: caravan-csharp only knows how to build one project
# (the worker). CaravanFlow.UI is a separate WASM project whose
# output needs to land at CaravanFlow/wwwroot/ before the worker is
# built. The worker's Program.cs calls app.MapStaticAssets(), which
# reads the UI's endpoint manifest copied alongside.

set -euo pipefail

cd "$(dirname "$0")"

WORKER_DIR="CaravanFlow"
UI_DIR="CaravanFlow.UI"
UI_PUBLISH_DIR="$(mktemp -d)"
trap 'rm -rf "$UI_PUBLISH_DIR"' EXIT

export PATH="$HOME/.dotnet:$PATH"

echo "→ publishing $UI_DIR Blazor WASM bundle..."
dotnet publish "$UI_DIR" -c Release -o "$UI_PUBLISH_DIR" | tail -5

echo "→ copying bundle into $WORKER_DIR/wwwroot/..."
/usr/bin/rm -rf "$WORKER_DIR/wwwroot"
mkdir -p "$WORKER_DIR/wwwroot"
cp -r "$UI_PUBLISH_DIR/wwwroot/." "$WORKER_DIR/wwwroot/"

# MapStaticAssets auto-discovers the manifest at the worker's content
# root using the executing-assembly name. The UI's published manifest
# is named CaravanFlow.UI.staticwebassets.endpoints.json; we rename
# it so the worker (CaravanFlow.dll) picks it up.
UI_MANIFEST="$UI_PUBLISH_DIR/CaravanFlow.UI.staticwebassets.endpoints.json"
if [[ -f "$UI_MANIFEST" ]]; then
    cp "$UI_MANIFEST" "$WORKER_DIR/CaravanFlow.staticwebassets.endpoints.json"
    echo "→ endpoint manifest copied"
else
    echo "  (no static-webassets manifest produced — MapStaticAssets will still serve raw files)"
fi

echo "→ building worker (AOT via caravan-csharp)..."
if command -v caravan-csharp &>/dev/null; then
    caravan-csharp build "$@"
elif [[ -x ../../caravan/caravan-csharp/build-tool/caravan-csharp ]]; then
    ../../caravan/caravan-csharp/build-tool/caravan-csharp build "$@"
else
    echo "  caravan-csharp not found — falling back to dotnet build"
    dotnet build "$WORKER_DIR" -c Release
fi

echo "✓ done. Start the worker from this directory so config.yaml is picked up."
