#!/bin/bash
for P in 512 4096; do rm -rf ~/devp; mkdir ~/devp
  PIECE_BYTES=$P DATA_DIR=~/dev OUTPUT_DIR=~/devp NCPU=8 ~/ss/target/release/sessions 2>/dev/null || echo "CRASH dev $P"
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_devp$P; rm -rf $W; cp -r ~/devp $W
done
