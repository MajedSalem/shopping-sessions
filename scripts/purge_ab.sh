#!/bin/bash
# it5 with default purging vs no purging: stage times and exit gaps.
printf '#!/bin/sh\nMIMALLOC_PURGE_DELAY=10 exec /home/majed/bins/it5\n' > /tmp/it5_purge10; chmod +x /tmp/it5_purge10
printf '#!/bin/sh\nMIMALLOC_PURGE_DELAY=-1 exec /home/majed/bins/it5\n' > /tmp/it5_nopurge; chmod +x /tmp/it5_nopurge
S=/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts
bash $S/fixed.sh 5 /tmp/it5_purge10 /tmp/it5_nopurge
bash $S/stages.sh 3 /tmp/it5_purge10 /tmp/it5_nopurge
