#!/bin/bash
# Scaling test: same generator seed family, 10M / 20M / 30M events; per-stage times (SEQ=1), best of 3.
rm -rf ~/d60   # free 5.3 GB of page cache / disk pressure
for M in 10 30; do [ -d ~/d$M ] || ~/gen/target/release/generate --seed 11 --events ${M}000000 --clicks $((M*250))000 --output ~/d$M >/dev/null 2>&1; done
[ -d ~/d20s11 ] || ~/gen/target/release/generate --seed 11 --events 20000000 --clicks 5000000 --output ~/d20s11 >/dev/null 2>&1
printf '#!/bin/sh\nSEQ=1 exec %s\n' $HOME/bins/it6 > /tmp/seq6; chmod +x /tmp/seq6
for D in d10 d20s11 d30; do
  cat ~/$D/*/*.csv > /dev/null
  echo "== $D ($(du -sh ~/$D | cut -f1))"
  for i in 1 2 3; do rm -rf /tmp/so; mkdir /tmp/so
    s=$(date +%s.%N); DATA_DIR=~/$D OUTPUT_DIR=/tmp/so NCPU=8 /tmp/seq6 2>/tmp/sl >/dev/null; e=$(date +%s.%N)
    printf "wall %.2f  " $(echo "$e - $s" | bc)
    sed -E 's/^\[ *([0-9.]+)s\] ([a-z ]+).*/\1 \2/' /tmp/sl | awk '{t=$1; $1=""; if (NR>1) printf "%s=%.2f ", name, t-p; p=t; name=$0} END {printf "\n"}' | sed 's/  */ /g'
  done
done
