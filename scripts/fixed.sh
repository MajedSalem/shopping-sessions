#!/bin/bash
# Fixed-cost probe: wall time, last log line ("done"), exit gap, page faults, peak RSS. fixed.sh ROUNDS BIN... [ENV=VAL...]
R=$1; shift; D=~/d20; O=/tmp/bo
cat $D/*/*.csv > /dev/null
for r in $(seq $R); do for b in "$@"; do rm -rf $O; mkdir -p $O
  /usr/bin/time -f "%e %R %M" -o /tmp/tv env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 $b 2>/tmp/log >/dev/null
  read wall minf rss < /tmp/tv; done_t=$(grep done /tmp/log | sed -E 's/^\[ *([0-9.]+)s.*/\1/'); map_t=$(grep setup /tmp/log | sed -E 's/^\[ *([0-9.]+)s.*/\1/')
  printf "%-28s wall %.2f  map %.2f  done %.2f  exit-gap %.2f  minor-faults %7d  rss %5d MB\n" "$(basename $b)" $wall $map_t $done_t $(echo "$wall - $done_t" | bc) $minf $((rss/1024))
done; done
