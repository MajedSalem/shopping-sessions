#!/bin/bash
# Interleaved A/B benchmark: bench.sh ROUNDS BIN... (each a path); prints min and median wall time per binary.
R=$1; shift; D=~/d20; O=/tmp/bo
cat $D/*/*.csv > /dev/null   # warm the page cache
declare -A T
for r in $(seq $R); do
  for b in "$@"; do rm -rf $O; mkdir -p $O
    s=$(date +%s.%N); DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 $b 2>/dev/null; e=$(date +%s.%N)
    T[$b]="${T[$b]} $(echo "$e - $s" | bc)"
  done
done
for b in "$@"; do echo "$b: $(echo ${T[$b]} | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END {printf "min %.3f  median %.3f  (n=%d)", a[1], a[int((NR+1)/2)], NR}')"; done
