#!/bin/bash
# Long-ID fallback check: rename u1 -> a 22-byte ID in L1 and L4 (links, events, clicks); output must match with the new name.
L=u1_a_very_long_user_id
for c in L1 L4 A5; do
  rm -rf /tmp/lf/$c; mkdir -p /tmp/lf/$c; cp -r ~/fx/$c/input /tmp/lf/$c/
  find /tmp/lf/$c/input -name '*.csv' -exec sed -i -E "s/(^|,|\")u1(,|\"|$)/\1$L\2/g" {} +
  rm -rf /tmp/lo; mkdir /tmp/lo; DATA_DIR=/tmp/lf/$c/input OUTPUT_DIR=/tmp/lo ~/ss/target/release/sessions 2>/dev/null
  W=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_long/$c; rm -rf $W; mkdir -p $W; cp -r /tmp/lo/* $W/
  sed "s/^u1,/$L,/" ~/fx/$c/expected/user_daily.csv > $W/user_daily.csv.exp; cp ~/fx/$c/expected/campaign_daily.csv $W/campaign_daily.csv.exp
done
