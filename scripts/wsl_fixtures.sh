#!/bin/bash
# Run the binary on every fixture (native fs), then copy outputs to the Windows side.
outs=~/fxout; rm -rf $outs; mkdir -p $outs
for d in ~/fx/*/; do c=$(basename "$d"); mkdir -p "$outs/$c"
  DATA_DIR="$d/input" OUTPUT_DIR="$outs/$c" NCPU=8 ~/ss/target/release/sessions 2>"$outs/$c.log" || echo "CRASH $c"
done
W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_fx; rm -rf $W; cp -r $outs $W
