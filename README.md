# Shopping Sessions

Entry for the Shopping Sessions track. `rules.md` is the specification.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
scripts/docker_run.sh <data_dir> <output_dir>
```

## Check

```sh
python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh
python3 scripts/compare.py <output_dir> <dev_answer_dir>
```
