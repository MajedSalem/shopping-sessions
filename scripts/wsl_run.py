#!/usr/bin/env python3
"""Local helper: run the WSL-built binary on Windows paths. Usage: wsl_run.py DATA OUT"""
import os, subprocess, sys

def wsl(p):
    p = os.path.abspath(p).replace("\\", "/")
    return "/mnt/" + p[0].lower() + p[2:]

data, out = sys.argv[1], sys.argv[2]
sys.exit(subprocess.run(["wsl", "-d", "Ubuntu", "--", "env", f"DATA_DIR={wsl(data)}", f"OUTPUT_DIR={wsl(out)}",
                         "NCPU=8", "/home/majed/ss/target/release/sessions"]).returncode)
