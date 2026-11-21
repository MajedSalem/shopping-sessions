#!/bin/bash
# Boundary stress: run fixtures and the dev set with tiny pieces; copy outputs for comparison.
for P in 7 37 200; do  # fixtures only (tiny)
  outs=~/fxp$P; rm -rf $outs; mkdir -p $outs
  for d in ~/fx/*/; do c=$(basename "$d"); mkdir -p "$outs/$c"
    PIECE_BYTES=$P DATA_DIR="$d/input" OUTPUT_DIR="$outs/$c" NCPU=8 ~/ss/target/release/sessions 2>/dev/null || echo "CRASH $P $c"
  done
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_fxp$P; rm -rf $W; cp -r $outs $W
done
for P in 512 4096; do rm -rf ~/devp; mkdir ~/devp
  PIECE_BYTES=$P DATA_DIR=~/dev OUTPUT_DIR=~/devp NCPU=8 ~/ss/target/release/sessions 2>/dev/null || echo "CRASH dev $P"
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_devp$P; rm -rf $W; cp -r ~/devp $W
done
