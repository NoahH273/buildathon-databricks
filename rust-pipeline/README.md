# Rust Shopping Sessions

Complete native Rust pipeline for the live repository `rules.md`. Implementation is in this directory; repository-root Docker packaging selects it for submission. The shared test scripts, fixtures, generator source, and expected answers are unchanged.

Requires Rust 1.90.0 for native development, Python 3.10+ with `polars==2.1.0` for the existing harness, and Docker/buildx for scorer tests. Production dependencies are Arrow/Parquet (typed DATE output and Snappy compression) and serde_json (manifest/checkpoints), pinned by `Cargo.lock`. Python and DuckDB are not runtime dependencies. Docker base images are pinned by digest.

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

The binary supports zero arguments using `DATA_DIR=/data`, `OUTPUT_DIR=/output`. Optional positional `DATA_DIR OUTPUT_DIR` override these for the fixture runner. `SCRATCH_DIR`, `NCPU`, and `MEM_GB` follow the scorer environment; the current serial in-memory algorithm uses no scratch and creates no threads or caches. The container runs as UID/GID 1000, needs no network, and supports read-only root/input. Output is one schema-bearing Parquet file per table, with DATE32 days, required non-null fields and nullable `top_category`, including empty datasets. Output directories should be empty at invocation, as in the scorer and harness.

## Algorithm

- `csv.rs`: buffered LF-only scanning and strict field structure/UTF-8 validation; preserves CR, case and spaces. Every matching part is read.
- `timestamp.rs`: strict Gregorian calendar and offset parsing, checked ASCII decimal parsing, integer milliseconds and half-open manifest windows.
- `ids.rs`: byte-exact string interning, union by size/path compression and a separate component minimum, including unused linked IDs.
- `events.rs`: global ID index retaining the earliest-ingested structurally parsed raw row; only winners undergo semantic validation and lateness rejection. Winning raw rows are freed while compact event records are produced.
- `sessions.rs` / `aggregate.rs`: sort compact events by person/time, scan gaps strictly over 30 minutes, and accumulate user/day/category cents using session-start dates. Release integer overflow checks prevent silent corruption.
- `clicks.rs`: global click dedup, validation, sort by person/time/byte ID, then a per-person two-pointer seven-day sweep. Campaign counts and a dense active set give one assist per eligible campaign/session. The first ID in the latest timestamp group wins ties; clicks are reusable across sessions.
- `parquet.rs`: internal row-key ordering, bounded 65,536-row Arrow batches/Parquet row groups, Snappy compression, explicit required/optional schema.

Runtime complexity is O(E log E + C log C + L alpha(V) + S + A), where A counts distinct campaign/session assists. There is no click/session range-join expansion. Memory is O(unique raw event bytes + E + C + V + aggregate keys), not a disk-spilling implementation. Unbounded datasets or extremely long IDs can exceed the available memory; the tested sizes and measured RSS are in `VALIDATION.md`. No full-scale scorer dataset is available here. Parallel sorting, sharded disk fallback and interval-based assist optimization remain possible future work; no semantics are approximated.

## Existing benchmark harness

Default and Docker builds exclude profiling completely. To collect development checkpoints, explicitly build a separate binary with the `profiling` Cargo feature. From repository root (all artifacts stay under this directory):

```sh
cargo build --release --locked --features profiling --manifest-path rust-pipeline/Cargo.toml \
  --target-dir rust-pipeline/target/profiling
python3 testing/harness.py --name rust-native \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 1 --cpus 8 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts \
  -- ./rust-pipeline/target/profiling/release/shopping-sessions
```

The harness checks schemas, exact rows, duplicate keys, nullability and conservation invariants on every run. It records wall time, CPU and per-process peak RSS for native runs; its container mode uses wall-only measurements. `CHECKPOINTS_ENABLED=1` plus `CHECKPOINTS_FILE` enables its JSONL phase profiling. `DIAGNOSTICS=1` prints phase durations and detailed event metrics to stderr for manual runs. Measured harness runs disable checkpoint recording. `/usr/bin/time` is not required.

### Detailed event checkpoints

With `--features profiling`, checkpoints subdivide `events_parse_dedup` and `events_validate_resolve`. Their existing JSONL records include `metrics.subphases` entries with exclusive `seconds` and operation `calls`, plus phase wall time and an unattributed residual. It times file discovery/open/header, buffered LF reads, UTF-8/CSV parsing, ingest timestamps, hash lookup/comparison, winner allocation/copy, index insert/replace/growth, winner iteration/reparse, required IDs, event timestamps/window, type/price, lateness, identity resolution, product lookup, compact-event append and raw/index cleanup.

In that optional development binary, detailed timers turn on automatically for the harness's profiling runs or `DIAGNOSTICS=1`. Set `EVENT_PROFILE_DETAIL=0` for coarse checkpoints only. Explicit `CHECKPOINTS_ENABLED=0` overrides detailed diagnostics, including an inherited `DIAGNOSTICS=1`. Normal measured runs select a separate const specialization with per-operation clocks and counter updates compiled out. The shared harness is unchanged: nested subphase metrics are retained in `results.json`, while its existing report continues to show parent phases.

```sh
# After the explicit profiling build above, use three profiles to compare subphase medians:
python3 testing/harness.py --name rust-details \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 3 --cpus 5 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts \
  -- ./rust-pipeline/target/profiling/release/shopping-sessions
python3 rust-pipeline/scripts/summarize_event_profiles.py \
  rust-pipeline/bench-artifacts/rust-details-*/results.json
```

Detailed profiling performs clock reads for every measured operation on every row. Its timings include clock-read cost and perturb fast operations. The residual contains untimed control flow and timer bookkeeping; it is not a pure overhead estimate. Compare ordinary/coarse/detailed runs separately, and do not treat a detailed run as normal runtime. Results are in `EVENT_PROFILE.md`.

## Submission packaging

The repository now has a root-level Dockerfile and `.dockerignore`. The public repository can be submitted directly, using the validated `rust-pipeline` commit SHA:

```sh
# From repository root:
docker buildx build --platform linux/amd64 --load \
  -t shopping-sessions-rust:submission .
```

Both the root Dockerfile and this directory's Dockerfile compile with `--no-default-features`. Checkpointing is excluded at compile time: the submission binary cannot read checkpoint/diagnostics environment switches, create checkpoint files or execute per-operation timers. All diagnostic flags are ineffective even if forced on by the caller. `profiling` is an explicit development-only Cargo feature and is off by default.

The root build context includes only `rust-pipeline/Cargo.toml`, `Cargo.lock` and `src/`. Inputs, outputs, archives and targets are excluded. The dedicated Dockerfile can still be built with `rust-pipeline` as its context and produces the same production configuration. The scorer wrapper and fixtures remain unchanged. See [`SUBMISSION.md`](SUBMISSION.md) for the final image, commands and results.
