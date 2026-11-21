#!/bin/bash
# Same binary, different mimalloc settings, interleaved.
B=${1:-/home/majed/bins/it4}
cat > /tmp/m0 <<X
#!/bin/sh
exec $B
X
cat > /tmp/m1 <<X
#!/bin/sh
MIMALLOC_ALLOW_LARGE_OS_PAGES=1 exec $B
X
cat > /tmp/m2 <<X
#!/bin/sh
MIMALLOC_PURGE_DELAY=-1 exec $B
X
cat > /tmp/m3 <<X
#!/bin/sh
MIMALLOC_ALLOW_LARGE_OS_PAGES=1 MIMALLOC_PURGE_DELAY=-1 exec $B
X
chmod +x /tmp/m0 /tmp/m1 /tmp/m2 /tmp/m3
bash /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts/fixed.sh 3 /tmp/m0 /tmp/m1 /tmp/m2 /tmp/m3
