# Shopping Sessions: native Rust pipeline

The root `Dockerfile` builds this crate. It implements `rules.md` with no engine: memory-mapped CSV,
AVX2 field splitting, and a hand-written Parquet writer.

## Data flow

1. **Phase 0, links.** Parse `links/` in parallel, then run a partitioned union-find. Each ID hash maps
   to a component, and each component is named by its byte-wise smallest ID.
2. **Phase 1, parse.** Read events and clicks in 4 MB chunks across all threads. Each user ID is
   resolved to its person on the spot, using a prefetched lookup ring that hides the table's DRAM
   latency. Events are scattered by `event_id` hash, keeping invalid rows, since dedup comes before
   validation. Clicks are scattered by person.
3. **Phase 2, dedup.** Per event_id partition, keep the copy with the smallest `ingest_ts`, drop it if
   invalid, and scatter the survivors by person.
4. **Phase 3, sessions.** Per person partition:
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
