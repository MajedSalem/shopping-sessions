#!/bin/bash
# Save current binary's 20M output as a regression reference.
rm -rf ~/ref20 && mkdir -p ~/ref20 && DATA_DIR=~/d20 OUTPUT_DIR=~/ref20 NCPU=8 ~/ss/target/release/sessions 2>/dev/null && echo saved
W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_ref20; rm -rf $W; cp -r ~/ref20 $W
