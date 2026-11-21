#!/bin/bash
# Per-stage durations (deltas between log lines), interleaved runs: stages.sh ROUNDS BIN...
R=$1; shift; D=~/d20; O=/tmp/bo
for r in $(seq $R); do for b in "$@"; do rm -rf $O; mkdir -p $O
  echo -n "$(basename $b): "
  DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 $b 2>&1 >/dev/null | sed -E 's/^\[ *([0-9.]+)s\] ([a-z ]+).*/\1 \2/' | awk '{t=$1; $1=""; if (NR==1) printf "map=%.2f ", t; if (NR>1) printf "%s=%.2f ", name, t-p; p=t; name=$0} END {printf "\n"}' | sed 's/  */ /g'
done; done
