#!/bin/bash
# 60M safety test: wall time, peak RSS, exit code and stage log per run.
B=${1:-$HOME/bins/it6}; D=~/d60; O=/tmp/o60
for i in 1 2 3; do rm -rf $O; mkdir -p $O
  /usr/bin/time -f "%e %M %x" -o /tmp/t60 env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 $B 2>/tmp/l60 >/dev/null
  read wall rss rc < /tmp/t60
  echo "run $i: wall ${wall}s  peak RSS $((rss/1024)) MB  exit $rc"
  sed -E 's/^\[ *([0-9.]+)s\] ([a-z ]+).*/\1 \2/' /tmp/l60 | awk '{t=$1; $1=""; if (NR>1) printf "%s=%.2f ", name, t-p; p=t; name=$0} END {printf "\n"}' | sed 's/  */ /g'
  grep -E "sessions built" /tmp/l60
done
du -sh $O/user_daily $O/campaign_daily; ls $O/user_daily | wc -l
