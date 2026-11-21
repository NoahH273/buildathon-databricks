# Rust–DuckDB hybrid: implementation and measured results

Measured November 20, 2026 (America/Phoenix); artifact directory timestamps are UTC. The hybrid follows `../duckdb-rust-hybrid-spec.md`; `../rules.md` remains authoritative.

## Overall time

Both images ran serially on the same generated inputs, with four worker threads and Docker flags requesting four CPUs, 8 GiB RAM, no swap, no network, a read-only root/input mount, and UID 1000. Each benchmark used one warmup and three measurements; only correct measured runs enter the median. These are local warm-cache wall times including container startup/cleanup and final output writes. They exclude validation and input hashing.

| Dataset | Baseline median | Hybrid median | Hybrid min–max | Speedup | Time reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Seed 1: 1M events / 250k clicks | 7.04 s | 4.05 s | 3.21–4.18 s | 1.74× | 42.4% |
| Seed 7: 10M events / 2.5M clicks | 42.03 s | 17.47 s | 15.97–17.78 s | 2.41× | 58.4% |

A native acceptance pass also matched the published answer for seed 1 and the original baseline for seed 19 (200k events / 50k clicks) and seed 7 (10M / 2.5M). Its single 10M-event run took 17.18 s, with a largest-process RSS high-water mark of approximately 1,435.5 MiB. That RSS is not summed process-tree memory and is not the container peak.

## Largest work points

The following are medians from two separate container profiling runs on seed 7. Checkpoints, extra counts, scratch scans, and container monitoring are disabled in all official measurements. Percentages refer to recorded stage time, not total Docker wall time.

| Stage | Profile median | Share of recorded stages |
| --- | ---: | ---: |
| rust_ingest | 4.40 s | 32.9% |
| duckdb_sessionize | 3.71 s | 27.7% |
| duckdb_deduplicate | 3.48 s | 26.0% |
| duckdb_export_handoff | 0.51 s | 3.8% |
| duckdb_user_totals | 0.48 s | 3.6% |
| rust_attribute | 0.36 s | 2.7% |
| duckdb_person_keys | 0.20 s | 1.5% |
| release_ingest_and_duckdb | 0.13 s | 1.0% |
| duckdb_write_outputs | 0.10 s | 0.8% |

The Rust phase consumed 2,420,592 click candidates and merged 1,746,802 campaign intervals for 1,721,757 sessions. The largest observed bucket had 1,103,963 rows; the largest person had 255 session starts. No buckets were deferred under the default budget. Ingestion staged about 446.4 MiB. Scratch observed at stage boundaries reached 926.6 MiB; this is a sampled lower bound on the true peak.

### Next improvements to measure

1. **Ingestion:** profile individual file times before splitting large parts into LF-aligned byte ranges. Parsing plus Snappy writing still dominates ingestion. Linked-ID construction accounts for roughly 0.9 s on this seed; reducing repeated canonical-name allocation and measuring a faster byte-key hasher are additional candidates. Keep exact quotes, CR, UTF-8 bytes, ASCII digits, and invalid earliest candidates intact.
2. **Sessionization:** group equal person/timestamp events first, retaining event counts and purchase/category contributions. Sessionizing distinct timestamps may shrink the two ordered windows and avoid carrying event-ID strings through both sorts. Differential tests must preserve same-millisecond events and session-start-day attribution.
3. **Deduplication and partitions:** retain the packed `arg_min` default. The window reference matched exactly; single native trials were 17.18 s for `arg_min` versus 17.43 s for `row_number` on seed 7, which does not establish a statistically meaningful large-data winner. Exploratory single runs with 8 and 16 attribution buckets took 16.23 s and 15.22 s, versus 17.18 s with 4. Repeat these trials before changing defaults; they are not equivalent to an 8-CPU scorer-scale benchmark.
4. **Lower priority:** attribution is only about 3% of profiled work. Optimize sorting, batch sizes/codecs, or interchange only after addressing ingestion and the DuckDB windows; avoid expanding session–campaign pairs. At much larger scales, measure hash-partitioning event IDs if global deduplication begins spilling.

## Correctness and fallback acceptance

- Native harness: all 36 fixtures passed. Official seed-1 output matched `dev-answer/`; seed-7 and seed-19 outputs matched the verified original baseline.
- Container harness: all 36 fixtures passed; every warmup, measurement, and profiling run matched its reference with the exact schema, Parquet DATE columns, unique keys, and cross-table invariants.
- Rust: five tests passed, including 150 seeded timelines against a brute-force credit/assist oracle, timestamp/CSV edge cases, and a 1,000-node linked-ID chain.
- Python: seven hybrid/contract regressions and all 12 harness tests passed. Coverage includes invalid packed dedup winners, byte-preserving binary IDs/canonical names, 1/4/8 buckets, both dedup plans, multi-file buckets, missing clicks, and typed empty outputs.
- Forced sorted fallback: the full official 1M dataset matched exactly with one bucket and a 32-MiB ordinary-attribution admission budget. The profile confirms one deferred bucket and execution of `sorted_fallback`. This is a trigger budget rather than a hard RSS cap on Arrow decoder workspace.

## Resource and measurement limitations

Docker inspection confirmed `NanoCpus=4000000000`, `Memory=8589934592`, and `MemorySwap=8589934592`; the visible CPU quota was `400000 100000`. However, the visible `memory.max` and `docker stats` exposed a 16-GiB limit and apparently broader memory/process usage. Therefore container memory isolation could not be verified, and those samples are not reported as this implementation’s memory peak. Raw diagnostic samples are retained. CPU and requested memory settings were identical for both images. Full 8-vCPU/24-GiB scorer-scale validation remains unmeasured on this host.

The container builds used the committed multi-stage Dockerfile with a temporary build-only local CA secret for this environment’s HTTPS trust chain. No certificate exception or environment-specific CA was added to the repository; runtime verification used `--network none`. The baseline comparison image came from the verified baseline at revision `fbcef8c`.

## Commands and artifacts

Actual checks included `cargo test --locked`, `cargo build --release --locked`, native fixture/harness runs, Python unittest discovery for `starter-duckdb/tests` and `testing/tests`, amd64 Docker builds, and the generic harness with `testing/docker_adapter.sh`. The default and variant reports record their expanded commands and input/reference fingerprints.

For a fresh native run (after building the Rust binary):

```sh
HYBRID_BIN=/absolute/path/to/shopping-hybrid python3 testing/harness.py \
  --name rust-hybrid --benchmark dev-1m /path/to/dev dev-answer \
  --repetitions 3 --warmups 1 --profile-runs 2 --cpus 4 --mem-gb 8 \
  -- python3 starter-duckdb/pipeline/pipeline.py
```

For container benchmarks, replace the command with `bash testing/docker_adapter.sh shopping-sessions:hybrid` and set `--resource-mode wall-only`. Add the seed-7 input and a previously generated baseline output as a second `--benchmark`.

Artifacts are intentionally Git-ignored: each contains `report.md`, `results.json`, `results.csv`, validation details, stdout/stderr, output Parquet, and profile JSONL. Local run directories:

- baseline: `artifacts/duckdb-baseline-container-comparison-20261121T040521379576Z-2059e8`
- hybrid: `artifacts/rust-hybrid-container-20261121T040942166595Z-1d6fc4`
- native: `artifacts/rust-hybrid-native-acceptance-20261121T040139659571Z-5e0301`
- row_number: `artifacts/rust-hybrid-row-number-20261121T040253951336Z-3e1c1f`
- fallback: `artifacts/rust-hybrid-forced-fallback-20261121T040441069201Z-a8bdb5`
- buckets8: `artifacts/rust-hybrid-buckets-8-20261121T040402491871Z-abb229`
- buckets16: `artifacts/rust-hybrid-buckets-16-20261121T040423812433Z-90d2b3`

- telemetry validation: `artifacts/rust-hybrid-container-telemetry-check-20261121T041518640403Z-3c5044` (seven valid JSON samples, no monitor in its measured run, exact output parity).
