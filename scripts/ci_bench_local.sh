#!/bin/bash
# Smoke test of ci_bench.sh on the 1M dev set (checks the script, not speed).
S=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts
OUT_BASE=/tmp/cb_base OUT_HEAD=/tmp/cb_head bash $S/ci_bench.sh ~/bins/it6 ~/ss/target/release/sessions ~/dev 2 /tmp/cb.md && cat /tmp/cb.md
