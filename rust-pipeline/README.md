# Rust Shopping Sessions

The repository-root Dockerfile builds this Rust pipeline. The v5 generator, public answers and rules come from the organizer’s update. Identifiers are opaque, variable-width UTF-8 strings. Equality, linked-person minima and click ties use the complete decoded bytes.

Requires Rust 1.90.0 for native development, Python 3.10+ with `polars==2.1.0` for the existing harness, and Docker/buildx for scorer tests. Cargo.lock pins Arrow/Parquet for typed DATE output and Snappy compression, serde_json for the manifest, Rayon for bounded parallel execution, memchr for byte scanning, and ahash for lookup tables. Python and DuckDB are not runtime dependencies. Docker base images are pinned by digest.

From this directory:

```sh
cargo fmt --check
cargo test --locked
cargo test --locked --features profiling
cargo build --release --locked --no-default-features
python3 -m pip install polars==2.1.0
make native-fixtures
make build                       # linux/amd64, shopping-sessions-rust:latest
make fixtures                    # existing scorer wrapper; all 36 fixtures
make dev                         # seed 1, 1M events + 250K clicks, generator build stays here
make run-dev
make check-dev                   # exact comparison with ../dev-answer
```

The binary supports zero arguments using `DATA_DIR=/data`, `OUTPUT_DIR=/output`. Optional positional `DATA_DIR OUTPUT_DIR` override these for the fixture runner. `SCRATCH_DIR`, `NCPU`, and `MEM_GB` follow the scorer environment; the in-memory implementation uses a Rayon pool capped by both `NCPU` and the available CPU quota. It uses no scratch files. The container runs as UID/GID 1000, needs no network, and supports read-only root/input. Output is one schema-bearing Parquet file per table, with DATE32 days, required non-null fields and nullable `top_category`, including empty datasets. Output directories should be empty at invocation, as in the scorer and harness.

## Algorithm

- Catalogs and links use the strict UTF-8 parser. Event and click files are read into separate, phase-local byte buffers. Workers get complete LF-delimited rows, including a final unterminated row. CR remains field data.
- For events, an atomic bitset records which ID hash slots have been seen twice. The prepass normalizes whole-field quoting on the first token. A unique occupied slot proves its row has no competing copy. Workers project every structurally valid row. A repeated slot sends its projected rows, including hash collisions, to an exact string-keyed table. Each stored ingest time owns a valid projection or an invalid marker. An earlier invalid row suppresses later valid copies.
- The union-find computes a byte-minimum name for each component. Event workers look up linked IDs through immutable data and assign local handles to new singletons. Those handles are translated to shared IDs before sessionization. Product categories are looked up only for purchases.
- Events are ordered by canonical person and timestamp. A forward scan builds sessions and ordered user-day rows. It attributes all events to the session-start day and reuses category revenue counters. Integer sums and byte-wise category ties are exact.
- Click IDs borrow bytes from the input until attribution finishes. Workers validate clicks for known people. Every alias of a person with a session is already known from links or events. Sorting by person, time and exact ID makes identical click copies adjacent for deduplication. A per-person sliding window credits the latest eligible time and smallest ID on ties. Each active campaign assists once.
- Arrow/Parquet writes batches of at most 65,536 rows with Snappy compression. Output columns carry explicit nullability and a logical DATE. Both directories receive a schema-bearing file for empty results.

Input buffers, compact records and output aggregates must fit in memory. Highly duplicated event data increases the share of rows processed in the exact fallback. See the [performance report](PERFORMANCE.md) for the measured working sets and the [experiment log](OPTIMIZATION_LOG.md) for alternatives.

## Existing benchmark harness

Default and Docker builds exclude profiling completely. To collect development checkpoints, explicitly build a separate binary with the `profiling` Cargo feature. From repository root (all artifacts stay under this directory):

```sh
cargo build --release --locked --no-default-features --manifest-path rust-pipeline/Cargo.toml
cargo build --release --locked --features profiling --manifest-path rust-pipeline/Cargo.toml \
  --target-dir rust-pipeline/target/profiling
python3 testing/harness.py --name rust-native \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 1 --cpus 8 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts \
  --profile-command ./rust-pipeline/target/profiling/release/shopping-sessions \
  -- ./rust-pipeline/target/release/shopping-sessions
```

The harness checks schemas, exact rows, duplicate keys, nullability and conservation invariants on every run. It records wall time, CPU and per-process peak RSS for native runs; its container mode uses wall-only measurements. `CHECKPOINTS_ENABLED=1` plus `CHECKPOINTS_FILE` enables its JSONL phase profiling. `DIAGNOSTICS=1` prints phase durations and detailed event metrics to stderr for manual runs. Measured harness runs disable checkpoint recording. `/usr/bin/time` is not required.

### Detailed event checkpoints

The `events_project_candidates` and `events_resolve_repeats` checkpoints include exclusive operation times and call counts in `metrics.subphases`. The filter has its own coarse checkpoint. Detailed profiles use a single worker so operation durations can be compared to one wall-clock timeline. Set `EVENT_PROFILE_DETAIL=0` on the profiling executable for a normal-width parallel phase breakdown.

In that optional development binary, detailed timers turn on automatically for the harness's profiling runs or `DIAGNOSTICS=1`. Set `EVENT_PROFILE_DETAIL=0` for coarse checkpoints only. Explicit `CHECKPOINTS_ENABLED=0` overrides detailed diagnostics, including an inherited `DIAGNOSTICS=1`. Normal measured runs select a separate const specialization with per-operation clocks and counter updates compiled out. The shared harness is unchanged: nested subphase metrics are retained in `results.json`, while its existing report continues to show parent phases.

```sh
# After the explicit profiling build above, use three profiles to compare subphase medians:
python3 testing/harness.py --name rust-details \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 3 --cpus 5 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts \
  --profile-command ./rust-pipeline/target/profiling/release/shopping-sessions \
  -- ./rust-pipeline/target/release/shopping-sessions
python3 rust-pipeline/scripts/summarize_event_profiles.py \
  rust-pipeline/bench-artifacts/rust-details-*/results.json
```

Detailed profiling performs clock reads for every measured operation on every row. Its timings include clock-read cost and perturb fast operations. The residual contains untimed control flow and timer bookkeeping; it is not a pure overhead estimate. Compare ordinary/coarse/detailed runs separately, and do not treat a detailed run as normal runtime. The original baseline profile is in `EVENT_PROFILE.md`. Current comparisons are in `OPTIMIZATION_LOG.md`.

## Submission packaging

The repository now has a root-level Dockerfile and `.dockerignore`. Build the optimized commit from the repository root with the following command.

```sh
# From repository root:
docker buildx build --platform linux/amd64 --load \
  -t shopping-sessions-rust:submission .
```

Both the root Dockerfile and this directory's Dockerfile compile with `--no-default-features`. Checkpointing is excluded at compile time: the submission binary cannot read checkpoint/diagnostics environment switches, create checkpoint files or execute per-operation timers. All diagnostic flags are ineffective even if forced on by the caller. `profiling` is an explicit development-only Cargo feature and is off by default.

The root build caches locked registry downloads. A build host behind a TLS-inspecting proxy can pass its trusted CA bundle with `--secret id=cargo_ca,src=/path/to/ca-bundle.pem`. BuildKit mounts it only during dependency retrieval and compilation.

The root build context includes only `rust-pipeline/Cargo.toml`, `Cargo.lock` and `src/`. Inputs, outputs, archives and targets are excluded. The dedicated Dockerfile can still be built with `rust-pipeline` as its context and produces the same production configuration. The scorer wrapper and fixtures remain unchanged. See [the optimization log](OPTIMIZATION_LOG.md) for the experimental comparisons and the [performance report](PERFORMANCE.md) for the image and validation results. [SUBMISSION.md](SUBMISSION.md) records the baseline image.
