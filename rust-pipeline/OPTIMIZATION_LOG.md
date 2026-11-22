# Rust pipeline performance experiments

The baseline is `95bc77597993d366e6051d65a9575ad3725d4901` on `rust-pipeline`. Work is on `optimize/rust-measured`. The local host is a Xeon Platinum 8573C with a four-CPU cgroup quota, five visible CPUs, and 16 GiB of memory. All native measurements use Rust 1.90.0, the locked production release profile, warm input caches, no runtime diagnostics and fresh output directories. Measurement includes process startup, Parquet writes and teardown. Validation runs afterward.

The following baseline and experiment tables use the pre-v5 generator and matching pre-v5 oracle. Final v5 results are in [the performance report](PERFORMANCE.md). The repository harness checked seed 1 (1M events / 250K clicks) against the shipped CSVs. Seeds 2 (2M / 500K) and 42 (3.5M / 875K) use separate DuckDB outputs from the same generator inputs. Both Rust tables must match every row, key and type and pass the harness invariants. Per-experiment comparisons alternate old/new order after one warmup per executable. The experiment table gives medians of three measurements except where a larger confirmation set is named. RSS is the median of per-process high-water marks in MiB.

## Baseline on the previous generator

| Seed | Median seconds | Min–max seconds | Maximum RSS, MiB |
|---|---:|---:|---:|
| 1 | 3.154 | 3.123–3.307 | 247.1 |
| 2 | 7.457 | 6.165–7.738 | 487.9 |
| 42 | 12.399 | 11.674–13.463 | 721.5 |

All seven Rust tests, 36 fixtures and all 15 seeded runs passed. A separate coarse profile on seed 1 measured 1.537 s for event parsing/dedup, 1.135 s for validating winners, 0.467 s for building sessions and user rows, and 0.261 s for click ingestion. These four phases took 91% of the profiled process time. The concurrently developed `cameron/rust-fast` at `3f0b29e` was inspected. Its hash-only equality and fixed category/window bounds preclude adopting it intact.

## Experiments on the previous generator

| Experiment and Git reference | Hypothesis and change | Correctness | Median seconds, old → new (1M / 2M) | Peak RSS, old → new (1M / 2M) | Decision |
|---|---|---|---|---|---|
| Project event winners, `05aa438` | Removing the second CSV parse and raw-line allocations from the event phases could save 25–40% overall. Store a compact, optional event for each provisional winner. An invalid earlier projection still suppresses every later copy. Only purchases need a product-category lookup. | 7 tests in each feature configuration, 36 fixtures, both seeded oracles on all paired runs | 2.867 → 1.836 / 6.683 → 4.059 | 247 → 162 / 488 → 316 | Keep |
| Repeat filter, `5eba35d` | The fused event pass still took 1.67 s on seed 1. Four workers and bypassing the full map for provably unique IDs could remove 30–50% overall. A two-pass bit filter routes all collisions and repeats to one byte-exact winner table. New singleton IDs are remapped after parallel parsing. | 9 Rust tests in both configurations, 36 fixtures, seed 1 and 2 exact on every paired run | 1.894 → 1.040 / 4.795 → 2.199 | 162 → 166 / 316 → 352 | Keep |
| Ordered user days, `391e697` | Sessions arrive in person/day order. Replacing a hash update per event, a map per purchasing day and a later output sort could save 5–15%. Append ordered user rows and clear only touched category totals at each boundary. | 9 tests in both configurations, 36 fixtures, both seed oracles. Initial three-pair 1M result overlapped. Seven-pair confirmation shown. | 1.346 → 1.199 / 2.880 → 2.779 | 170 → 177 / 352 → 352 | Keep |
| Borrowed click IDs, `ce8a6d2` | Click ingestion used 0.317 s in the 1M coarse profile. Borrowing IDs from read buffers, reusing linked-person lookups and using the existing sort for duplicate elimination could save 10–20% overall. | 9 tests per feature mode, 36 fixtures, paired seed 1 and 2, plus independent 8M and duplicate-heavy oracles | 1.757 → 1.005 / 2.445 → 2.013 | 177 → 167 / 352 → 352 | Keep |
| Parallel projection of repeats, `0bd44bb` | The serial repeat/merge phase took 0.260 s on 1M. Project every structurally valid copy on its worker, including an invalid marker, then compare original IDs and ingest times. Expected 5–15% overall. On five additional 3.5M pairs all five improved, median 3.827 → 3.133 s and RSS 590 → 606 MiB. | 9 tests per feature mode, 36 fixtures, exact seed 1, 2 and 42 comparisons | 0.938 → 0.879 / 2.024 → 1.964 | 177 → 184 / 352 → 340 | Keep |
| All-ID sharding, `bba642f` | Removing the repeat prepass and partitioning every projected record for parallel exact dedup could save 10–20%. | 9 tests per feature mode, 36 fixtures, seeds 1, 2 and 17, and skew exact | 0.892 → 0.789 / 1.745 → 1.568 | 174 → 240 / 366 → 454 | Reject for submission. On 8M, 7.487 → 7.367 s with overlapping ranges while median RSS rose from 1,329 to 1,748 MiB (+32%). On duplicate-heavy skew, 0.098 → 0.103 s. The full candidate scatter costs memory for every unique row. |
| Strict digit pairs, `ea5abb5` | Event projection took 0.390 s on 1M. Fixed-width ASCII decoding avoids repeated UTF-8 and overflow checks on two-digit ISO fields, and a single decimal pass handles F3. Expected 5–10% overall. | 10 tests per feature mode including every digit position, 36 fixtures, five alternating pairs on both seeded oracles | 0.931 → 0.813 / 1.817 → 1.734 | 178 → 185 / 365 → 366 | Keep |
| Direct alias lookup, `69fdcba` | A read-only exact alias-to-minimum map could remove 5–10% by avoiding a second indexed load and repeated DSU scans. Reuse the interner map with ahash and release the union forest. Fix the repeat-routing seed for reproducible row order. Five-pair 1M median increased 5.7%, but all three 8M pairs improved, 8.158 → 7.246 s and 1,435 → 1,423 MiB. | 10 tests per feature mode, 36 fixtures, exact output on seeds 1, 2 and 17 | 0.735 → 0.776 / 1.803 → 1.664 | 185 → 172 / 349 → 336 | Keep |

## v5 integration

The organizer-supplied generator, public answers and rules were imported at `dbb6c33`. Decoded ID bytes control event equality, identity linking and click ties. The keyed mapping and the length of a suffix have no role in the pipeline. The v5 regression at `dd2910f` exercises identifiers with forty identical leading bytes, including a quoted duplicate with a later ingest time. The final benchmark compares the original release directly with the combined retained changes on new v5 inputs.

One further candidate, local experiment `21fbfa5`, raises the maximum repeat-filter size from 64 MiB to 256 MiB per bitmap. The hypothesis was that fewer possible repeats would reach the exact map on inputs larger than 2 GiB. Its unit suite and image build passed. It received no comparative timing before the optimization search closed. The selected release retains the measured 64 MiB ceiling. For full-size timings, memory and exact output counts, see [PERFORMANCE.md](PERFORMANCE.md).

## Reproduce

Run from the repository root on Linux AMD64. Set `LAB` to an empty writable directory outside the checkout. The setup below keeps the baseline, optimized production binary and instrumented binary separate.

```sh
mkdir -p "$LAB"
git worktree add --detach "$LAB/baseline" 95bc77597993d366e6051d65a9575ad3725d4901
cargo build --release --locked --no-default-features --manifest-path "$LAB/baseline/rust-pipeline/Cargo.toml"
cp "$LAB/baseline/rust-pipeline/target/release/shopping-sessions" "$LAB/base"
cargo fmt --check --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --features profiling --manifest-path rust-pipeline/Cargo.toml
cargo clippy --locked --all-targets --manifest-path rust-pipeline/Cargo.toml -- -D warnings
cargo clippy --locked --all-targets --features profiling --manifest-path rust-pipeline/Cargo.toml -- -D warnings
cargo build --release --locked --no-default-features --manifest-path rust-pipeline/Cargo.toml
cp rust-pipeline/target/release/shopping-sessions "$LAB/final"
make -C rust-pipeline native-fixtures
cargo build --release --locked --features profiling --manifest-path rust-pipeline/Cargo.toml --target-dir "$LAB/profiling"
cargo build --release --locked --manifest-path generator/Cargo.toml --target-dir "$LAB/gen"
python3 -m unittest discover -s testing/tests -q
```

The dataset sizes in the native comparison table are generator arguments. Generate each row with `"$LAB/gen/release/generate" --seed N --events E --clicks C --threads 4 --output "$LAB/NAME"`. For the public input, use seed 1, 1,000,000 events and 250,000 clicks. For each new seed, generate a separate DuckDB reference with the identical input bytes. The following commands illustrate the seed 17 and public comparisons. This session ran all workloads in the table.

```sh
mkdir -p "$LAB/scratch"
DATA_DIR="$LAB/seed17" OUTPUT_DIR="$LAB/answer-seed17" SCRATCH_DIR="$LAB/scratch" \
  NCPU=4 MEM_GB=9 python3 starter-duckdb/pipeline/pipeline.py
POLARS_MAX_THREADS=4 python3 rust-pipeline/scripts/compare_releases.py "$LAB/base" "$LAB/final" \
  --benchmark dev "$LAB/dev" dev-answer --benchmark seed17 "$LAB/seed17" "$LAB/answer-seed17" \
  --cpus 4 --mem-gb 16 --repetitions 3
EVENT_PROFILE_DETAIL=0 python3 testing/harness.py --name phases --benchmark dev "$LAB/dev" dev-answer \
  --warmups 1 --repetitions 3 --profile-runs 2 --profile-command "$LAB/profiling/release/shopping-sessions" \
  --artifacts "$LAB/reports" --cpus 4 --mem-gb 16 -- "$LAB/final"
docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:submission .
IMAGE=shopping-sessions-rust:submission CPUS=4 MEM=15g \
  python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- sh starter-duckdb/scripts/docker_run.sh
IMAGE=shopping-sessions-rust:submission CPUS=4 MEM=15g \
  sh starter-duckdb/scripts/docker_run.sh "$LAB/finals-971" "$LAB/out-971"
python3 starter-duckdb/scripts/compare.py "$LAB/out-971" "$LAB/answer-finals-971"
```

The skew input copies the v5 seed 2 catalogs and manifest. Read the CSV parts in byte-lexicographic path order, skipping each header. Take the first 45,000 seven-field event lines and 18,000 four-field click lines, splitting on literal commas. Replace each user with `skew` followed by the unsigned CRC32 of the first field with outer quotes stripped, modulo 1,200. Put every row in one part and repeat each row whose zero-based index modulo five is nonzero in a second part. Add a header-only part. Chain the links from `skew0,skew1` through `skew1199,skew1200`. The result tests a single linked person, duplicate-heavy data and uneven part sizes. DuckDB supplies the expected output.
