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
