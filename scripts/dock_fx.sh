#!/bin/bash
# Run the image on every fixture with scorer flags, outputs to ../out_dfx/<case>
cd "$(dirname "$0")/.."; rm -rf out_dfx; mkdir -p out_dfx
for d in fixtures/*/; do c=$(basename $d); mkdir -p out_dfx/$c; chmod 777 out_dfx/$c
MSYS_NO_PATHCONV=1 docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m --cap-drop ALL --security-opt no-new-privileges --cpus 8 --memory 6g --user 1000:1000 \
  -v "$(pwd -W)/$d/input":/data:ro -v "$(pwd -W)/out_dfx/$c":/output -e NCPU=8 shopping-sessions:latest 2>/dev/null || echo "CRASH $c"
done
