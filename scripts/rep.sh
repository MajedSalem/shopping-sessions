#!/bin/bash
D=${1:-~/d20}; N=${2:-4}; O=~/devout
for i in $(seq $N); do rm -rf $O; mkdir -p $O
env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 ~/ss/target/release/sessions 2>&1 | tr '\n' ' ' | sed 's/sessions built[^[]*//' ; echo
done
