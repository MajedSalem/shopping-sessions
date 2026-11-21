#!/bin/bash
# Run the 20M set N times and copy each output for comparison with the iteration-1 reference.
for i in $(seq ${1:-4}); do rm -rf ~/det$i; mkdir ~/det$i
  DATA_DIR=~/d20 OUTPUT_DIR=~/det$i NCPU=8 ~/ss/target/release/sessions 2>/dev/null || echo "CRASH $i"
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_det$i; rm -rf $W; cp -r ~/det$i $W
done
