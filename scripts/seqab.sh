#!/bin/bash
# Interleaved sequential-stage timings for two binaries: seqab.sh BIN_A BIN_B ROUNDS
for r in $(seq ${3:-3}); do for b in "$1" "$2"; do
  bash /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts/seq.sh "$b" 1 | sed "s|seqbin|$(basename $b)|"
done; done
