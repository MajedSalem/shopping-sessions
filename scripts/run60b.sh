#!/bin/bash
# One 60M run with major faults, swap activity and memory snapshot.
B=${1:-$HOME/bins/it6}; D=~/d60; O=/tmp/o60; rm -rf $O; mkdir -p $O
free -m | head -3; cat /proc/swaps
grep -E '^(pswpin|pswpout)' /proc/vmstat > /tmp/vm0
/usr/bin/time -f "wall %e s, peak RSS %M KB, major faults %F, minor %R" env DATA_DIR=$D OUTPUT_DIR=$O NCPU=8 $B 2>&1 >/dev/null | grep -E 'wall|done'
grep -E '^(pswpin|pswpout)' /proc/vmstat > /tmp/vm1
paste /tmp/vm0 /tmp/vm1 | awk '{print $1, "pages:", $4-$2}'
