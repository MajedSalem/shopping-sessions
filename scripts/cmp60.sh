#!/bin/bash
# 60M correctness: iteration 6 vs current build, outputs copied for an exact comparison; peak RSS.
[ -d ~/d60 ] || ~/gen/target/release/generate --seed 11 --events 60000000 --clicks 15000000 --output ~/d60 >/dev/null 2>&1
rm -rf ~/d10 ~/d30 ~/d20s11
for b in it6 cur; do B=$HOME/bins/it6; [ $b = cur ] && B=$HOME/ss/target/release/sessions
  rm -rf ~/o60$b; mkdir ~/o60$b
  /usr/bin/time -f "$b: exit %x, peak RSS %M KB" env DATA_DIR=~/d60 OUTPUT_DIR=~/o60$b NCPU=8 $B 2>&1 >/dev/null | grep -E "exit|sessions built"
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_60$b; rm -rf $W; cp -r ~/o60$b $W; rm -rf ~/o60$b
done
