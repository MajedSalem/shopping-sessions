# Local loop: build in WSL, run fixtures + dev check, time on the 20M set.
param([switch]$NoBig)
$S = "/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts"
wsl -d Ubuntu -- bash "$S/sync_build.sh"
wsl -d Ubuntu -- bash "$S/wsl_fixtures.sh"
Push-Location C:\Users\majed\Desktop\MeshHassoAwy\solution\scripts
python check_fixtures.py ../fixtures ../out_fx | Select-Object -Last 8
wsl -d Ubuntu -- bash "$S/wsl_dev.sh" /home/majed/dev copy
python compare.py ../out_dev ../../dev-answer/dev-answer | Select-Object -Last 3
Pop-Location
if (-not $NoBig) { wsl -d Ubuntu -- bash "$S/wsl_dev.sh" /home/majed/d20 }
