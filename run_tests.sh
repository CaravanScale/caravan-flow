#!/bin/bash
# run_tests.sh — compile and run zinc-flow test suite
set -e

ZINC="/home/vrjoshi/proj/zinc/zinc-go/zinc"

# Swap main.zn with test entry point, copy helpers into src/
cp src/main.zn src/main.zn.bak
cp test/test_main.zn src/main.zn
cp test/test_helpers.zn src/test_helpers.zn

cleanup() {
    mv src/main.zn.bak src/main.zn
    rm -f src/test_helpers.zn
}
trap cleanup EXIT

# Run
$ZINC run .
