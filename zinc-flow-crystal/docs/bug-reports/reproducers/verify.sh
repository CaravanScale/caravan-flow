#!/usr/bin/env bash
# Re-run each reproducer with the current Crystal and print whether the
# bug still fires. If any of these starts compiling cleanly, the bug is
# fixed in the current Crystal build — update the bug report to note
# the fixing version and move the file to resolved/.
#
# Usage:
#   ./verify.sh                 # uses crystal on PATH
#   CRYSTAL=/path/to/crystal ./verify.sh

set -uo pipefail

CRYSTAL="${CRYSTAL:-crystal}"
cd "$(dirname "$0")"

"$CRYSTAL" --version
echo

for f in *.cr; do
    printf "%-40s " "$f"
    if out=$("$CRYSTAL" build --no-codegen "$f" 2>&1); then
        echo "COMPILES — bug may be fixed! Review."
        echo "$out" | sed 's/^/    /'
    else
        first=$(echo "$out" | grep -E 'Error:' | head -1)
        echo "still fails: $first"
    fi
done
