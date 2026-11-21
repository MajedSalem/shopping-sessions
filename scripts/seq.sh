#!/bin/bash
# Stage times with links built first (SEQ=1), so links / events / clicks are timed separately.
printf '#!/bin/sh\nSEQ=1 exec %s\n' "${1:-$HOME/ss/target/release/sessions}" > /tmp/seqbin; chmod +x /tmp/seqbin
bash /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts/stages.sh ${2:-3} /tmp/seqbin
