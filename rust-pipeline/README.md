# Rust Shopping Sessions

Complete native Rust pipeline for the live repository `rules.md`. All implementation and packaging changes are confined to this directory. The shared test scripts, fixtures, generator source, and expected answers are unchanged.

Requires Rust 1.90.0 for native development, Python 3.10+ with `polars==2.1.0` for the existing harness, and Docker/buildx for scorer tests. Production dependencies are Arrow/Parquet (typed DATE output and Snappy compression) and serde_json (manifest/checkpoints), pinned by `Cargo.lock`. Python and DuckDB are not runtime dependencies. Docker base images are pinned by digest.

From this directory:

```sh
cargo fmt --check
cargo test --locked
cargo build --release --locked
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

From repository root (all artifacts stay under this directory):

```sh
python3 testing/harness.py --name rust-native \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 1 --cpus 8 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts \
  -- ./rust-pipeline/target/release/shopping-sessions
```

The harness checks schemas, exact rows, duplicate keys, nullability and conservation invariants on every run. It records wall time, CPU and per-process peak RSS for native runs; its container mode uses wall-only measurements. `CHECKPOINTS_ENABLED=1` plus `CHECKPOINTS_FILE` enables its JSONL phase profiling. `DIAGNOSTICS=1` prints phase durations to stderr for manual runs. Measured harness runs disable checkpoint recording. `/usr/bin/time` is not required.

## Submission packaging

Build from repository root with:

```sh
docker buildx build --platform linux/amd64 --load \
  -t shopping-sessions-rust:latest rust-pipeline
```

The build context contains only source/configuration, not generated inputs, outputs, or Cargo targets. For submission to the competition, `description.txt` requires a root-level Dockerfile in a public repository <=50 MB. This monorepo branch deliberately keeps ownership isolated: **publish this directory as the root of a separate submission repository, or coordinate a root-Dockerfile integration before submitting this monorepo.** Its Makefile uses the parent repository's unchanged harness and fixtures; a standalone repository must copy those unchanged (and set `HARNESS`, `FIXTURES`, `ANSWER` as appropriate). The production Docker build is self-contained already. No new repository or competition submission is created by this task.
