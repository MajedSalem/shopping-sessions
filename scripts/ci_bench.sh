#!/bin/bash
# Interleaved benchmark of two binaries on one dataset, with per-stage times.
#   ci_bench.sh BASE_BIN HEAD_BIN DATA_DIR ROUNDS REPORT_MD
# Every round runs base then head (order alternates each round), on a warm page cache.
# Stage times come from the pipeline's stderr log lines "[  1.234s] label ...": a stage's
# time is the gap from the previous log line. Reports min and median wall time and the
# minimum of each stage, as Markdown.
set -eu
BASE=$1; HEAD=$2; DATA=$3; ROUNDS=${4:-5}; REPORT=${5:-/dev/stdout}
NCPU=${NCPU:-$(nproc)}
WORK=$(mktemp -d)
cat "$DATA"/*/*.csv "$DATA"/*.csv > /dev/null 2>&1 || true   # warm the page cache

run_one() { # name bin round
  local name=$1 bin=$2 r=$3 out="$WORK/out_$1"
  rm -rf "$out"; mkdir -p "$out"
  local s e
  s=$(date +%s.%N)
  DATA_DIR="$DATA" OUTPUT_DIR="$out" NCPU="$NCPU" "$bin" 2> "$WORK/log_${name}_$r" > /dev/null
  e=$(date +%s.%N)
  echo "$e - $s" | bc >> "$WORK/wall_$name"
  # stage deltas: "label<TAB>seconds"
  sed -E -n 's/^\[ *([0-9.]+)s\] ([a-z][a-z ]*[a-z]).*/\1\t\2/p' "$WORK/log_${name}_$r" |
    awk -F'\t' 'NR==1 {printf "startup to %s\t%.4f\n", $2, $1} NR>1 {printf "%s\t%.4f\n", $2, $1 - p} {p=$1}' >> "$WORK/stages_$name"
}

for r in $(seq "$ROUNDS"); do
  if [ $((r % 2)) -eq 1 ]; then run_one base "$BASE" "$r"; run_one head "$HEAD" "$r"
  else run_one head "$HEAD" "$r"; run_one base "$BASE" "$r"; fi
done

stat() { sort -n | awk '{a[NR]=$1} END {if (NR==0) {print "-\t-"; exit} printf "%.3f\t%.3f\n", a[1], a[int((NR+1)/2)]}'; }
read bmin bmed < <(stat < "$WORK/wall_base" | tr '\t' ' ')
read hmin hmed < <(stat < "$WORK/wall_head" | tr '\t' ' ')
pct() { echo "scale=2; ($2 - $1) * 100 / $1" | bc; }

{
  echo "### Wall time (s), $ROUNDS interleaved rounds, NCPU=$NCPU"
  echo
  echo "| | base | head | change |"
  echo "|---|---|---|---|"
  echo "| min | $bmin | $hmin | $(pct "$bmin" "$hmin")% |"
  echo "| median | $bmed | $hmed | $(pct "$bmed" "$hmed")% |"
  echo
  echo "All runs (s): base $(tr '\n' ' ' < "$WORK/wall_base"); head $(tr '\n' ' ' < "$WORK/wall_head")"
  echo
  for name in base head; do
    echo "### Stages, $name (minimum over rounds, s)"
    echo
    echo "| stage | min | median |"
    echo "|---|---|---|"
    # keep first-seen order of labels
    cut -f1 "$WORK/stages_$name" | awk '!seen[$0]++' | while IFS= read -r label; do
      read smin smed < <(awk -F'\t' -v l="$label" '$1==l {print $2}' "$WORK/stages_$name" | stat | tr '\t' ' ')
      echo "| $label | $smin | $smed |"
    done
    echo
  done
} > "$REPORT"
cp -r "$WORK/out_base" "${OUT_BASE:-$WORK/keep_base}" 2>/dev/null || true
cp -r "$WORK/out_head" "${OUT_HEAD:-$WORK/keep_head}" 2>/dev/null || true
cp "$WORK"/log_* "${LOG_DIR:-$WORK}" 2>/dev/null || true
