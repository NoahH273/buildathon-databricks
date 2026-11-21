# Shopping Sessions: native Rust pipeline

The root `Dockerfile` builds this crate. It implements `rules.md` with no engine: memory-mapped CSV,
AVX2 field splitting, and a hand-written Parquet writer.

## Data flow

1. **Links.** Parse `links/` and run a partitioned union-find on a side thread while phase 1 runs.
   Each ID hash maps to a component, named by its byte-wise smallest ID.
2. **Phase 1, parse.** Read events and clicks in 4 MB chunks across all threads. Every event row
   emits a 20-byte dedup key `(event_id, ingest, loc)` by event_id hash, invalid rows included,
   since dedup comes before validation. Valid events and clicks go out by user-hash partition.
3. **Phase 2a, dedup.** Per event_id partition, keep the copy with the smallest `ingest_ts`, and
   send every other copy's `loc` to its user partition as a kill.
4. **Phase 2b, resolve.** Per user partition, drop killed rows and resolve each user to its person.
   User partitions use the same top hash bits as the link table, so lookups stay in cache.
   Unlinked users are already in the right person partition; only linked ones get scattered.
5. **Phase 3, sessions.** Per person partition:
   - Run a counting sort on the person bits, then sort small buckets by `ts`.
   - Build sessions and per-day rows.
   - Credit each session to the last click within 7 days, breaking ties on the smallest `click_id`.
   - Count assists from merged `[click, click + 7d]` intervals for each campaign.
   - Each thread writes its own `user_daily` Parquet file.

Hashes are 64-bit. A collision between two distinct IDs is about 1e-4 likely at 60M events.

## Local loop

```sh
cd rust-pipeline && cargo build --release
python ../starter-duckdb/scripts/run_fixtures.py ../starter-duckdb/fixtures -- target/release/shopping-sessions
S=/path/to/scratch bash check.sh   # fixtures + dev-answer + seed-3 cross-check + timed 20M run
```

`BENCH=<events csv>` runs parse micro-benchmarks. `LOAD_MEM=1` reads inputs into RAM instead of using mmap.
