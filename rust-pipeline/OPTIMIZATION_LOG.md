# Rust pipeline performance experiments

The baseline is `95bc77597993d366e6051d65a9575ad3725d4901` on `rust-pipeline`. Work is on `optimize/rust-measured`. The local host is a Xeon Platinum 8573C with a four-CPU cgroup quota, five visible CPUs, and 16 GiB of memory. All native measurements use Rust 1.90.0, the locked production release profile, warm input caches, no runtime diagnostics and fresh output directories. Measurement includes process startup, Parquet writes and teardown. Validation runs afterward.

The repository harness checked seed 1 (1M events / 250K clicks) against the shipped CSVs. Seeds 2 (2M / 500K) and 42 (3.5M / 875K) use separate DuckDB outputs from the same generator inputs. Both Rust tables must match every row, key and type and pass the harness invariants. Per-experiment comparisons alternate old/new order after one warmup per executable. Times in the experiment table are medians of three measurements. RSS is the median of per-process high-water marks in MiB.

## Baseline

| Seed | Median seconds | Min–max seconds | Maximum RSS, MiB |
|---|---:|---:|---:|
| 1 | 3.154 | 3.123–3.307 | 247.1 |
| 2 | 7.457 | 6.165–7.738 | 487.9 |
| 42 | 12.399 | 11.674–13.463 | 721.5 |

All seven Rust tests, 36 fixtures and all 15 seeded runs passed. A separate coarse profile on seed 1 measured 1.537 s for event parsing/dedup, 1.135 s for validating winners, 0.467 s for building sessions and user rows, and 0.261 s for click ingestion. These four phases took 91% of the profiled process time. The concurrently developed `cameron/rust-fast` at `3f0b29e` was inspected. Its hash-only equality and fixed category/window bounds preclude adopting it intact.

## Experiments

| Experiment and Git reference | Hypothesis and change | Correctness | Median seconds, old → new (1M / 2M) | Peak RSS, old → new (1M / 2M) | Decision |
|---|---|---|---|---|---|
| Project event winners, `opt-exp/projected` | Removing the second CSV parse and raw-line allocations from the event phases could save 25–40% overall. Store a compact, optional event for each provisional winner. An invalid earlier projection still suppresses every later copy. Only purchases need a product-category lookup. | 7 tests in each feature configuration, 36 fixtures, both seeded oracles on all paired runs | 2.867 → 1.836 / 6.683 → 4.059 | 247 → 162 / 488 → 316 | Keep |
| `opt-exp/repeat` | The fused event pass still took 1.67 s on seed 1. Four workers and bypassing the full map for provably unique IDs could remove 30–50% overall. A two-pass bit filter routes all collisions and repeats to one byte-exact winner table. New singleton IDs are remapped after parallel parsing. | 9 Rust tests in both configurations, 36 fixtures, seed 1 and 2 exact on every paired run | 1.894 → 1.040 / 4.795 → 2.199 | 162 → 166 / 316 → 352 | Keep |
| `opt-exp/daily` | Sessions arrive in person/day order. Replacing a hash update per event, a map per purchasing day and a later output sort could save 5–15%. Append ordered user rows and clear only touched category totals at each boundary. | 9 tests in both configurations, 36 fixtures, both seed oracles. Initial three-pair 1M result overlapped. Seven-pair confirmation shown. | 1.346 → 1.199 / 2.880 → 2.779 | 170 → 177 / 352 → 352 | Keep |
| `opt-exp/clicks` | Click ingestion used 0.317 s in the 1M coarse profile. Borrowing IDs from read buffers, reusing linked-person lookups and using the existing sort for duplicate elimination could save 10–20% overall. | 9 tests per feature mode, 36 fixtures, paired seed 1 and 2, plus independent 8M and duplicate-heavy oracles | 1.757 → 1.005 / 2.445 → 2.013 | 177 → 167 / 352 → 352 | Keep |
| `opt-exp/pending` | The serial repeat/merge phase took 0.260 s on 1M. Project every structurally valid copy on its worker, including an invalid marker, then compare original IDs and ingest times. Expected 5–15% overall. On five additional 3.5M pairs all five improved, median 3.827 → 3.133 s and RSS 590 → 606 MiB. | 9 tests per feature mode, 36 fixtures, exact seed 1, 2 and 42 comparisons | 0.938 → 0.879 / 2.024 → 1.964 | 177 → 184 / 352 → 340 | Keep |
| All-ID sharding, `opt-exp/all-id-shards` | Removing the repeat prepass and partitioning every projected record for parallel exact dedup could save 10–20%. | 9 tests per feature mode, 36 fixtures, seeds 1, 2 and 17, and skew exact | 0.892 → 0.789 / 1.745 → 1.568 | 174 → 240 / 366 → 454 | Reject for submission. On 8M, 7.487 → 7.367 s with overlapping ranges while median RSS rose from 1,329 to 1,748 MiB (+32%). On duplicate-heavy skew, 0.098 → 0.103 s. The full candidate scatter costs memory for every unique row. |
