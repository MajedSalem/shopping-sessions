# Full check for an iteration: build, fixtures, dev answer, 20M diff vs iteration-1 reference, 4 timed runs.
$S = "/mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/scripts"
& C:\Users\majed\Desktop\MeshHassoAwy\solution\scripts\all.ps1 -NoBig
wsl -d Ubuntu -- bash -c "rm -rf ~/new20 && mkdir ~/new20 && DATA_DIR=~/d20 OUTPUT_DIR=~/new20 NCPU=8 ~/ss/target/release/sessions 2>/dev/null; rm -rf /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_new20; cp -r ~/new20 /mnt/c/Users/majed/Desktop/MeshHassoAwy/solution/out_new20"
Push-Location C:\Users\majed\Desktop\MeshHassoAwy\solution\scripts
"--- 20M regression vs iteration 1"
python compare.py ../out_ref20 ../out_new20 | Select-Object -Last 3
Pop-Location
wsl -d Ubuntu -- bash "$S/rep.sh" /home/majed/d20 4
