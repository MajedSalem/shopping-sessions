# Shopping Sessions: Rust pipeline

A single Rust binary that implements `rules.md` exactly. It has no query engine: it uses mmap, a hand-written CSV parser, and hash partitioning across 8 threads with rayon.

## Pipeline

1. **Read.** Every CSV is mmapped with `MAP_POPULATE` in parallel and cut into ~4 MB chunks at line boundaries. Only each file's first line is skipped as its header.
2. **Parse (in parallel with the links build).**
   - The parser implements the rules' CSV dialect itself: `\n` is the only line terminator, a quote is allowed only around a whole field, and the field count must be exact.
   - Timestamps F1, F2 and F3 are validated strictly with ASCII digits only. Calendar, offset and overflow checks are done by hand.
   - Each event copy becomes a 48-byte record: a 128-bit event_id hash, the user hash, ingest seconds, ts relative to the window start (or INVALID), and price|category. Records are partitioned into 256 buckets by event_id hash.
3. **Links.**
   - IDs are hashed and deduplicated per bucket.
   - A lock-free parallel union-find runs over all edges. It always links the larger root under the smaller, so no cycles can form.
   - The smallest ID of each component is chosen with a CAS loop. The result is 256 per-bucket maps from id hash to person key, where the person key is the hash of the person's name.
4. **Dedup.** For each event_id bucket, a hash map keeps the copy with the smallest `ingest_ts`. That copy is then judged valid or not, so dedup happens before validation.
5. **Resolve.** Surviving events and valid clicks are resolved to a person key with a lookup local to their user-hash bucket. They are then re-bucketed by person.
6. **Sessions.** For each person bucket, events are sorted by (person, ts) and clicks by (person, ts, click_id).
   - One pass builds the sessions.
   - A sliding 7-day click window keeps per-campaign counts plus an active set. It gives the credited click (the latest, with ties going to the smallest click_id) and the assisting campaigns.
   - The pass also accumulates the per-day user rows and per-thread campaign × day counters.
7. **Write.** Parquet is written with arrow-rs: 8 files for `user_daily` (written in parallel) and 1 file for `campaign_daily`.

## Local testing

`scripts/run_fixtures.py fixtures -- <cmd>` and `scripts/compare.py out dev-answer` come from the starter. `scripts/all.ps1` is a WSL loop for Windows machines without Docker. It builds with zig as the C toolchain, runs all 36 fixtures, checks the dev set, and times a 20M-event set.
