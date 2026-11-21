#!/bin/bash
# Run the cycle-counter build (feature prof) on the warm 20M set (SEQ=1). Build with: prof.sh build
source /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/wsl_env.sh
if [ "$1" = build ]; then cd ~/ss && CARGO_TARGET_DIR=~/ss/target-prof cargo build --release --features prof 2>&1 | grep -E '^error|Finished'; fi
cat ~/d20/*/*.csv > /dev/null
for i in 1 2 3; do rm -rf /tmp/po; mkdir /tmp/po
  SEQ=1 DATA_DIR=~/d20 OUTPUT_DIR=/tmp/po NCPU=8 ~/ss/target-prof/release/sessions 2>&1 | grep -E 'prof|events parsed|done' | tr '\n' ' '; echo
done
