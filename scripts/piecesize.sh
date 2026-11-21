#!/bin/bash
# Same binary, different piece sizes, interleaved.
B=${1:-$HOME/ss/target/release/sessions}
for p in 262144 1048576 4194304 16777216; do printf '#!/bin/sh\nPIECE_BYTES=%s exec %s\n' $p $B > /tmp/p$p; chmod +x /tmp/p$p; done
bash /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts/bench.sh ${2:-7} /tmp/p262144 /tmp/p1048576 /tmp/p4194304 /tmp/p16777216
