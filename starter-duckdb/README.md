# Shopping Sessions: Rust–DuckDB hybrid

This implementation follows `duckdb-rust-hybrid-spec.md` and the authoritative `rules.md`. Rust handles ingestion and attribution; DuckDB handles global event deduplication, sessions, user totals, and final Parquet output. The image targets `linux/amd64` and runs without network access.

If this starter and `rules.md` ever disagree, `rules.md` wins. Ask the organizers if `rules.md` leaves you unsure.

## Implementation

`rust/` builds `shopping-hybrid`, with independent `ingest` and `attribute` subcommands. Ingestion uses a shared exact CSV parser, byte-preserving linked-ID resolution, checked integer timestamps/prices, and parallel file scheduling with the largest files first. It writes Snappy Parquet batches bounded by 32,768 rows or about 8 MiB, flushing the row group at each batch. Invalid event candidates retain their ID, ingest time, and validity flag, so they can still win deduplication.

`pipeline/pipeline.py` orchestrates separate Rust and DuckDB phases under per-run scratch. `pipeline/pipeline.sql` materializes session and user stages in a file-backed DuckDB database. The default event winner uses a non-null packed struct with `arg_min`; `HYBRID_DEDUP=row_number` selects the reference plan. Session windows share the exact `(event_ms, event_id)` ordering.

DuckDB exports numeric-person session/click buckets once per relation and closes before Rust attribution starts. Rust uses compact sortable records and a click-ID byte arena. A single sweep computes last-click credit and campaign interval unions; binary searches and occupied-day ranges count assists without materializing session–campaign pairs. Bucket admission uses footer estimates and bounded worker reservations, then checks vector/arena growth and per-person metric workspace before publishing a shard. Oversized buckets are deferred, spill-sorted by DuckDB, and processed one at a time using bounded readers and current-person session state. Atomic per-bucket metric files prevent duplicate contributions on retry.

Build and test against all 36 fixtures and the public dev answer before submitting. Local timings are not scorer timings. The previous verified baseline remains available in Git history and can be built as a separate image for differential comparisons.

See [measured results and next improvements](HYBRID_RESULTS.md) for native/container acceptance, repeated baseline comparisons, and the profiling breakdown.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with `polars` for the test scripts. Install it with `pip install polars`.

Build the image and run all 36 fixtures:

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. `make dev` needs `generate` on your PATH: build it from the track page's generator.zip (Rust 1.85 or newer; its README has the two commands).

```sh
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** Build and run the amd64 image natively.
- **Apple-silicon Macs.** Emulating amd64 is slow. For fast local loops, build a native image with `make build run-dev PLATFORM=linux/arm64`. The Dockerfile pins no platform, so it builds either way. Check the amd64 image before you submit.

To iterate without Docker:

```sh
pip install -r requirements.txt polars
cargo test --locked --manifest-path rust/Cargo.toml
cargo build --release --locked --manifest-path rust/Cargo.toml
export HYBRID_BIN="$PWD/rust/target/release/shopping-hybrid"
# If CARGO_TARGET_DIR is configured, use its actual release binary instead.
export SCRATCH_DIR="$(mktemp -d)"
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py
python3 -m unittest discover -s tests -v
```

The Rust tests include exact CSV/timestamp cases, long unordered link chains, and 150 seeded attribution timelines checked against brute force. Python regressions exercise byte-preserving Parquet staging, invalid earliest candidates, alternative deduplication plans, partition counts, multi-file buckets, absent clicks, forced sorted fallback, and typed empty outputs. The fixture checker needs `polars`; the pipeline uses only the pinned DuckDB Python dependency plus the compiled Rust binary. Arrow/Parquet, Rayon, and JSON dependencies are pinned in Cargo and `Cargo.lock`.

For repeated measurements and retained artifacts, use the language-independent [testing harness](../testing/README.md). From the repository root:

```sh
HYBRID_BIN=/absolute/path/to/shopping-hybrid python3 testing/harness.py \
  --name rust-duckdb-hybrid --benchmark dev-1m /path/to/dev dev-answer \
  --repetitions 5 --profile-runs 2 --cpus 4 --mem-gb 8 \
  -- python3 starter-duckdb/pipeline/pipeline.py
```

Checkpoints and diagnostic queries run only in separate profiling invocations. The official measured path has no checkpoint writes or diagnostic counting queries. Profile metrics include candidate counts, ingest subphase durations, handoff bytes, largest in-memory bucket/person group, merged interval count, deferred buckets, and scratch sizes observed at phase boundaries. These scratch observations are a lower bound on the true instantaneous peak. Container adapter profiles additionally retain sampled Docker memory/CPU statistics; samples can miss short peaks and never run during official measurements.

| Environment variable | Default / behavior |
| --- | --- |
| `HYBRID_BIN` | `/app/bin/shopping-hybrid`; set explicitly for native runs |
| `HYBRID_BUCKETS` | One for source data below 8 MiB; otherwise up to `min(NCPU, 16)` |
| `HYBRID_MEMORY_BYTES` | 65% of `MEM_GB`, for ordinary attribution admission and allocations |
| `HYBRID_DEDUP` | `arg_min`; optional `row_number` reference |
| `HYBRID_KEEP_WORK=1` | Keep the scratch database and interchange files for inspection |

The attribution reservation is conservative, not an OS heap cap. It includes vector/arena capacities, decoded batches, pending interval state, and an allowance for metrics and sorting. The sorted path avoids whole-bucket allocations but still needs Arrow decoder buffers, aggregate counters, and the current person's session starts; artificially tiny budgets used to force that path are not a hard RSS bound. Container memory limits remain the enforcement mechanism. All bucket counts and deduplication options preserve the same output contract.

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **DuckDB's CSV reader differs from `rules.md`.** It reads a quoted empty field `""` as an empty string, not null. Its malformed-line handling, including `ignore_errors`, `strict_mode`, and `null_padding`, does not match `rules.md`. Test against the fixtures before relying on it.
- **Memory.** `pipeline.py` sets `memory_limit` to 80% of `MEM_GB` and `temp_directory` under `/scratch`, so big joins and sorts spill to disk instead of getting the container killed.
- **No extension downloads.** The container has no network at run time. Parquet and CSV are built into the wheel; anything else must be installed at build time.
- **`\d` is not `[0-9]`.** In several regex engines, including Rust's used by Polars, `\d` matches any Unicode digit. The rules mean ASCII digits only.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files in this starter

- `pipeline/pipeline.py` orchestrates Rust, DuckDB, fallback, and final output.
- `pipeline/pipeline.sql` implements DuckDB sessions and user totals.
- `rust/` contains parsing, typed Parquet exchange, attribution, and focused tests.
- `tests/` contains additional end-to-end regressions.
- `Dockerfile` builds a `linux/amd64` image for the Intel Xeon scorers.
- `fixtures/` holds the 36 edge cases, each an `input/` folder and the `expected/` tables. In this monorepo they live in `../../fixtures`; copy them in when publishing the starter as its own repo.
- `scripts/` runs your image the way the scorer does and diffs the output.
