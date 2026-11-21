#!/bin/bash
# Single-thread parser microbenchmark + differential check on the 20M set.
BENCH=1 BENCH_MB=${1:-200} DATA_DIR=~/d20 OUTPUT_DIR=/tmp/pb NCPU=1 ~/ss/target/release/sessions 2>&1 | grep -v '^\['
