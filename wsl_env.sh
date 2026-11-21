# Local (WSL, no gcc) toolchain shim: zig as the C compiler and linker.
mkdir -p ~/bin
cat > ~/bin/cc <<'X'
#!/bin/bash
args=()
for a in "$@"; do
  case "$a" in
    --target=*) ;;
    *) args+=("$a") ;;
  esac
done
exec ~/zig/zig cc -target x86_64-linux-gnu.2.36 "${args[@]}"
X
chmod +x ~/bin/cc
ln -sf ~/bin/cc ~/bin/gcc
cat > ~/bin/c++ <<'X'
#!/bin/sh
exec ~/zig/zig c++ -target x86_64-linux-gnu.2.36 "$@"
X
chmod +x ~/bin/c++
cat > ~/bin/ar <<'X'
#!/bin/sh
exec ~/zig/zig ar "$@"
X
chmod +x ~/bin/ar
export PATH=~/bin:~/.cargo/bin:$PATH
