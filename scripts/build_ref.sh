#!/bin/bash
# Build reference binaries of earlier iterations into ~/bins/ (same deps as ~/ss).
source /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/wsl_env.sh
SP=/mnt/c/Users/majed/AppData/Local/Temp/claude/902d1e74ccdaf141/2d95dc10-9be0-4e25-8b11-4a2ec7a2a1c7/scratchpad/it2
mkdir -p ~/bins ~/ssref && cp -r ~/ss/Cargo.toml ~/ss/Cargo.lock ~/ss/.cargo ~/ssref/ && mkdir -p ~/ssref/src
for it in 1 2; do
  f=$SP/main.rs; [ $it = 1 ] && f=$SP/main_it1.rs
  cp $f ~/ssref/src/main.rs && (cd ~/ssref && cargo build --release 2>&1 | grep -E '^error|Finished') && cp ~/ssref/target/release/sessions ~/bins/it$it
done
ls -la ~/bins
