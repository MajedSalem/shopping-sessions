#!/bin/bash
# Copy sources into WSL native fs and build.
source /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/wsl_env.sh
S=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution
mkdir -p ~/ss && cp -r $S/Cargo.toml $S/src $S/.cargo ~/ss/
[ -d ~/fx ] || cp -r $S/fixtures ~/fx
cd ~/ss && cargo build --release 2>&1 | grep -E '^(error|warning: unused)|-->|Finished' | head -40
