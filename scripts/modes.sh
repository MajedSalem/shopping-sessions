#!/bin/bash
D=${1:-~/d20}; O=~/devout
for m in mmap populate read read mmap; do rm -rf $O; mkdir -p $O
echo "== $m"; READ_MODE=$m /usr/bin/time -v env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 ~/ss/target/release/sessions 2>&1 | grep -E 'setup|events parsed|done|Elapsed|Maximum resident'
done
