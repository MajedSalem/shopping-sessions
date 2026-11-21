#!/bin/bash
# Run on a WSL-native dataset: wsl_dev.sh DATASET_DIR [copy]
D=${1:-~/dev}; O=~/devout; rm -rf $O; mkdir -p $O
/usr/bin/time -v env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 ~/ss/target/release/sessions 2>&1 | grep -E '^\[|Elapsed|Maximum resident|panick'
if [ "$2" = copy ]; then W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_dev; rm -rf $W; cp -r $O $W; fi
