# Rust pipeline performance on v5

The combined optimizations on `optimize/rust-measured` completed a local **60M-event, 15M-click** v5 dataset in **100.79 seconds**, with **10.89 GiB** sampled process peak RSS. All output rows matched DuckDB. The original release was `95bc77597993d366e6051d65a9575ad3725d4901`. The optimized Rust implementation is `69fdcba46085a50ff5309a9e40c21654e1fe87a7`. Release revision `42c1048f2d40a039071541a00b0fb8bba86e9c3d` contains that code, the v5 inputs update, the long-ID regression and the verified root Dockerfile. Later commits add measurement tools and documentation.

## Full-size validation

Seed 971 produced 63,241,815 physical event lines, 15,548,131 click lines and 2,670,007 link lines in 6,126,889,125 bytes. The input fingerprint was `658b7d8c80b3b5e47346c859a76f0561f9fd1b583f5ae1ed6a348a1476485f57`. The 15M-click assumption scales the public development ratio. The announcement gave a 60M dataset size.

The optimized image retained 58,768,128 events in 10,022,205 sessions. An independent DuckDB run produced **6,628,067 user-day rows and 28,014 campaign-day rows**. Every value and key matched. There were zero missing rows, extra rows, duplicate keys or unexpected nulls. Both tables passed schema checks and all seven aggregate invariants.

The scorer wrapper ran with four vCPUs, 15 GiB, no swap, no network, a read-only root and input, and UID/GID 1000. Wall time includes container startup and teardown. Linux `VmHWM` sampled every 200 ms measures pipeline process memory. The full-size result is one optimized run. The original 60M time and a full-size speedup remain unmeasured. The official finals use nine interleaved runs across three private datasets on eight vCPUs and 24 GB.

## Original versus optimized native releases

Each executable received a warmup per workload. Both releases used the identical v5 files, fresh output directories and alternating run order. Each entry gives median wall seconds and the full measured range when available. The 8M row shows the one completed measurement pair after warmup. Peak RSS is the maximum process high-water mark among the measured runs. On the small skew case a separate stdlib-only launcher collected three RSS measurements to avoid inheriting the Python harness’s memory footprint.

| Seed · events / clicks | Measured runs per build | Original seconds | Optimized seconds | Speedup / less time | Peak MiB, original → optimized |
|---|---:|---:|---:|---:|---:|
| 1 · 1M / 250K | 3 | 5.020 (4.813–5.104) | 1.286 (0.860–1.337) | 3.90× / 74.4% | 254.2 → 188.5 (-25.9%) |
| 2 · 2M / 500K | 3 | 10.696 (8.399–11.445) | 1.494 (1.414–1.704) | 7.16× / 86.0% | 502.4 → 375.1 (-25.3%) |
| 42 · 3.5M / 875K | 3 | 15.178 (14.957–15.506) | 3.666 (2.686–3.945) | 4.14× / 75.8% | 748.2 → 633.3 (-15.3%) |
| 17 · 8M / 2M | 1 | 50.579 | 8.236 | 6.14× / 83.7% | 1988.2 → 1431.8 (-28.0%) |
| Linked skew · 81K / 32.4K physical lines | 3 | 0.169 (0.160–0.258) | 0.084 (0.075–0.085) | 2.01× / 50.4% | 16.0 → 23.8 (+48.5%) |

The local host was an Intel Xeon Platinum 8573C with a four-CPU cgroup quota, five visible CPUs and 16 GiB of memory. Rust 1.90.0 compiled both releases with locked dependencies and profiling excluded. Timings include process startup, output writes and exit. Input generation, exact validation and profiling were timed separately. Repeated measurements reused the same files on a shared host.

## Docker and correctness

- The tracked root Dockerfile built for Linux AMD64. The resulting image is `sha256:2c82ab6155606a84865d78a1e055a17d84215189b7343b63bd1ea1993999f69f` (81,865,242 bytes). The host proxy required a build-only CA bundle to prime the Cargo registry cache. The standard root build then succeeded and produced the same image.
- The optimized image passed 36/36 scorer-wrapper fixtures. Across three Docker dev measurements after a warmup, its median was **4.457 s** (range 4.041–4.725 s). The original and optimized native results both matched the v5 public answer, with 118,107 user rows and 27,449 campaign rows.
- The final Rust pipeline passed 11 tests in each feature configuration, formatting and Clippy with warnings denied. All 36 native fixtures and 12 harness tests passed. Both public Parquet files were byte-identical at one, two and four workers. Forcing diagnostics left production checkpointing disabled.
- DuckDB 1.5.6 matched the public answer and supplied reference tables for seeds 2, 42, 17, 971 and the linked, duplicate-heavy stress case. Polars 2.1.0 performed exact comparisons. Additional tests cover shared 40-byte ID prefixes, forced hash collisions, quoted IDs, invalid earliest winners, long CSV lines, dates, nullability and click tie-breaking.
- The supplied generator, development answers and rules match the uploaded bytes. The generator built in release mode and produced every v5 input used here. Its separate unit-test target cannot compile because the supplied archive imports the absent `shopping_sessions_reference` crate.

## Experiments and remaining opportunities

Seven of eight benchmarked changes were retained. The largest improvements came from parsing provisional event winners once and parallelizing projection while reserving exact string-keyed deduplication for possible repeats. All-ID sharding improved the 8M median by only 1.6% on the previous generator and raised peak RSS by 32%. The [experiment log](OPTIMIZATION_LOG.md) records the intermediate commits, tradeoffs, oracles and reproduction commands.

Two separate coarse profiling runs on v5 seed 1 measured the following optimized phase medians. The profile clocks exclude some process startup and teardown costs.

| Phase | Seconds |
|---|---:|
| `events_project_candidates` | 0.234 |
| `events_read_repeat_filter` | 0.131 |
| `events_resolve_repeats` | 0.111 |
| `catalogs` | 0.095 |
| `links_parse_intern_union` | 0.076 |
| `events_sort` | 0.064 |
| `clicks_read_validate` | 0.050 |
| `attribution_campaign_aggregation` | 0.050 |
| `parquet_write` | 0.041 |
| `sessions_user_aggregation` | 0.024 |
| `clicks_sort_dedup` | 0.015 |

The next experiments in priority order are improving locality of user lookup during event projection, partitioning only the exact repeat fallback on large inputs, and profiling click lookups and attribution under concentrated linked-user activity. All three merit fresh full-size measurements before integration.
