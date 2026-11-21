# Detailed event-phase profiling

Historical validation/profiling record. Current root packaging and the submission binary with checkpointing compiled out are documented in `SUBMISSION.md`. Profiling now requires an explicit `--features profiling` development build.

The added checkpoints split the two expensive event phases into exclusive operation buckets. All changes stay under `rust-pipeline/`; the shared harness and test oracle are unchanged. `profile.rs` implements const-specialized timing, `csv.rs` adds an instrumented scan using the same strict parser, `events.rs` times dedup/validation operations, and `main.rs` puts the breakdown into the existing parent checkpoint's `metrics`. The harness still sees a monotonic, non-overlapping parent timeline and preserves nested data in `results.json`.

## Main findings

For the **Docker image**, medians of three detailed runs:

| Operation | 1M events | 2M events |
| --- | ---: | ---: |
| Hash-index insert / replacement / growth | 0.5270 s | 1.1809 s |
| Input CSV structure / UTF-8 parsing | 0.1930 s | 0.3815 s |
| Dedup hash lookup / ingest comparison | 0.1482 s | 0.3789 s |
| Buffered line reads / LF removal | 0.1247 s | 0.2583 s |
| Event-ID / winning-row allocation and copying | 0.0880 s | 0.2024 s |
| Ingest timestamp parsing | 0.0729 s | 0.1410 s |
| Retained-row CSV reparsing during validation | 0.3970 s | 0.8481 s |
| Identity lookup / interning / canonical-person resolution | 0.2455 s | 0.6526 s |
| Product-to-category lookup | 0.1457 s | 0.3021 s |
| Event timestamp parsing / window check | 0.0603 s | 0.1221 s |

Index maintenance is the largest individually measured dedup operation: **36.4%** of the instrumented 1M phase and **38.8%** at 2M. During validation, reparsing retained CSV rows is largest (**29.2% / 28.2%**), followed by identity resolution (**18.1% / 21.7%**) and product lookup (**10.7% / 10.0%**). Native profiles agree on these main hotspots. The checkpoint does not separate insertion from rehashing within the combined index-maintenance operation; those contributions cannot be inferred individually from this data.

The full tables below include required IDs, type/price validation, lateness, event-vector allocation/appends, iterator traversal, and explicit raw-record/index cleanup. Values are operation wall time **inside detailed profiles**, not estimates of uninstrumented costs.

## Instrumentation cost and ordinary runs

Every operation on every row is timed, including rare hash-table growth. There is no sampling or extrapolation. Clock reads perturb short operations and execution/cache behavior. End-clock-read cost is included in operation totals; timer bookkeeping and untimed control flow remain in the separately reported residual. The residual is not a pure measurement of overhead. Medians are computed independently and need not add exactly.

| Dataset / execution | Ordinary median, 3 runs | Detailed median, 3 runs | Observed difference |
| --- | ---: | ---: | ---: |
| 1M native | 2.6400 s | 3.6301 s | +37.5% |
| 2M native | 6.4327 s | 9.5085 s | +47.8% |
| 1M Docker, including startup/cleanup | 3.6092 s | 4.8700 s | +34.9% |
| 2M Docker, including startup/cleanup | 7.2321 s | 9.0180 s | +24.7% |

These are observed differences between run groups, not isolated causal overhead estimates. Timing variation and cache/allocator behavior remain present. Detailed profiles do not enter ordinary benchmark statistics. No algorithm optimization was attempted.

A separate three-profile **coarse-only** native pass (`EVENT_PROFILE_DETAIL=0`) measured parent phases without per-operation timers:

| Parent phase | 1M events | 2M events |
| --- | ---: | ---: |
| Event parsing / dedup | 0.9636 s | 2.5779 s |
| Winner validation / identity resolution | 0.7057 s | 1.4807 s |

Do not combine these coarse parent times with the detailed subphase percentages to produce purported exact normal costs. Detailed profiling changed the execution being observed.

Before instrumentation edits, the old release binary's ordinary medians on the same inputs were 2.5598 s (1M) and 6.2963 s (2M). The updated ordinary measurements are within the overlapping observed ranges; these small sessions do not establish a speedup or statistical equivalence. Compilation was finished before the reported benchmark sessions. Native runs use NCPU=5 / MEM_GB=24 hints; Docker enforces five CPUs / 24 GB on the existing host and uses wall-only resource accounting. All runs are local and warm-cache; no eight-CPU scorer timings are claimed.

## Correctness and profiling controls

- `cargo fmt --check`, `cargo test --locked` (**7 tests**), `cargo clippy --locked --all-targets -- -D warnings`, and locked release compilation passed.
- The unchanged fixture runner passed **36/36 native ordinary**, **36/36 native detailed**, and **36/36 Docker**.
- Every ordinary, coarse, and detailed native/Docker development invocation exactly matched `dev-answer/`. Explicit comparator calls for the final detailed native and Docker development outputs both printed **MATCH**, with 117,038 user rows, 27,400 campaign rows and zero differences/duplicate keys.
- Every 2M-event run matched the previously validated independent DuckDB reference, including schema, nullability, unique keys and conservation checks. All four harness reports completed with zero failed runs and zero checkpoint warnings.
- A new black-box test verifies row-level call counts, timer accounting, monotonic checkpoint timestamps, and both coarse-only and disabled modes. An explicit `CHECKPOINTS_ENABLED=0` suppresses detailed profiling even if `DIAGNOSTICS=1` and `EVENT_PROFILE_DETAIL=1` are inherited.
- Profiling is enabled automatically by `CHECKPOINTS_ENABLED=1` plus `CHECKPOINTS_FILE`, or by manual `DIAGNOSTICS=1` when profiling has not explicitly been disabled. `EVENT_PROFILE_DETAIL=0` retains coarse-only parent checkpoints. Normal measured invocations select const-specialized event code with no per-operation clocks or counter updates.
- There are no new dependencies. The final dispatch-only guard for an inherited diagnostics flag does not change the operation bodies measured below; its new environment combination is covered by the regression test.

## Reproduce

From repository root, using the existing generated input/reference directories and Polars 2.1.0:

```sh
cargo fmt --check --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --manifest-path rust-pipeline/Cargo.toml
cargo clippy --locked --manifest-path rust-pipeline/Cargo.toml --all-targets -- -D warnings
cargo build --release --locked --manifest-path rust-pipeline/Cargo.toml
python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- ./rust-pipeline/target/release/shopping-sessions
DIAGNOSTICS=1 python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- ./rust-pipeline/target/release/shopping-sessions

python3 testing/harness.py --name rust-event-details-native --skip-fixtures \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 \
  --warmups 1 --repetitions 3 --profile-runs 3 --cpus 5 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts -- ./rust-pipeline/target/release/shopping-sessions
EVENT_PROFILE_DETAIL=0 python3 testing/harness.py --name rust-event-coarse-native --skip-fixtures \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 \
  --warmups 0 --repetitions 1 --profile-runs 3 --cpus 5 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts -- ./rust-pipeline/target/release/shopping-sessions

# Build using the temporary CA-secret setup already documented in VALIDATION.md:
docker buildx build --platform linux/amd64 --load \
  -f /tmp/rust-pipeline-session.Dockerfile \
  --secret id=proxy_ca,src=/etc/ssl/certs/ca-certificates.crt \
  -t shopping-sessions-rust:latest rust-pipeline
IMAGE=shopping-sessions-rust:latest CPUS=5 MEM=24g PLATFORM=linux/amd64 \
  python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- sh starter-duckdb/scripts/docker_run.sh
python3 testing/harness.py --name rust-event-details-docker --skip-fixtures --resource-mode wall-only \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 \
  --warmups 1 --repetitions 3 --profile-runs 3 --cpus 5 --mem-gb 24 \
  --artifacts rust-pipeline/bench-artifacts -- bash testing/docker_adapter.sh shopping-sessions-rust:latest

python3 rust-pipeline/scripts/summarize_event_profiles.py \
  rust-pipeline/bench-artifacts/rust-event-details-native-*/results.json \
  rust-pipeline/bench-artifacts/rust-event-details-docker-*/results.json
```

These commands were run, with native Cargo/Python tooling under the development-only `/tmp` paths documented in `VALIDATION.md`. Baseline measurements used a copy of the old release binary at `/tmp/rust-pipeline-before-details` with one warmup and three measurements per dataset. Raw report directories:

- `bench-artifacts/rust-before-details-20261121T041430101950Z-6177a4/`
- `bench-artifacts/rust-event-details-native-20261121T041901618687Z-d6eb59/`
- `bench-artifacts/rust-event-coarse-native-20261121T042102041939Z-4a39d3/`
- `bench-artifacts/rust-event-details-docker-20261121T042217397143Z-a4543f/`

## Detailed event timings: rust-event-details-native

Source: `rust-pipeline/bench-artifacts/rust-event-details-native-20261121T041901618687Z-d6eb59/results.json`

### dev-1m: events_parse_dedup

Profiles: 3. Median phase: **1.459136 s**; timed operations: 1.220635 s; residual: 0.238501 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| index_insert_replace_growth | 0.504088 | 34.5% | 1,028,618 |
| csv_structure_utf8 | 0.201624 | 13.8% | 1,072,086 |
| dedup_hash_lookup_compare | 0.154677 | 10.6% | 1,071,554 |
| buffered_line_read | 0.123316 | 8.5% | 1,072,116 |
| winner_allocation_copy | 0.083952 | 5.8% | 1,028,618 |
| ingest_timestamp_parse | 0.070640 | 4.8% | 1,071,554 |
| required_id_checks | 0.027121 | 1.9% | 1,071,635 |
| file_discovery_open_header | 0.025329 | 1.7% | 31 |
| Untimed control flow / timer bookkeeping | 0.238501 | 16.3% | — |

### dev-1m: events_validate_resolve

Profiles: 3. Median phase: **1.414834 s**; timed operations: 1.118150 s; residual: 0.297817 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| winner_csv_reparse | 0.409126 | 28.9% | 999,601 |
| identity_resolution | 0.271840 | 19.2% | 977,930 |
| product_category_lookup | 0.153508 | 10.8% | 977,930 |
| event_timestamp_window | 0.061312 | 4.3% | 997,120 |
| raw_winner_release | 0.053658 | 3.8% | 999,601 |
| winner_table_iteration | 0.048399 | 3.4% | 999,602 |
| compact_event_push | 0.038297 | 2.7% | 977,930 |
| event_type_purchase_price | 0.025803 | 1.8% | 992,518 |
| required_id_checks | 0.025204 | 1.8% | 999,601 |
| lateness_check | 0.025002 | 1.8% | 990,046 |
| winner_table_release | 0.003071 | 0.2% | 1 |
| event_buffer_allocation | 0.000012 | 0.0% | 1 |
| Untimed control flow / timer bookkeeping | 0.297817 | 21.0% | — |

### seed2-2m: events_parse_dedup

Profiles: 3. Median phase: **4.355125 s**; timed operations: 3.719735 s; residual: 0.495836 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| index_insert_replace_growth | 1.881070 | 43.2% | 2,050,415 |
| dedup_hash_lookup_compare | 0.450327 | 10.3% | 2,126,730 |
| csv_structure_utf8 | 0.412205 | 9.5% | 2,127,777 |
| buffered_line_read | 0.342029 | 7.9% | 2,127,872 |
| winner_allocation_copy | 0.290459 | 6.7% | 2,050,415 |
| ingest_timestamp_parse | 0.147419 | 3.4% | 2,126,730 |
| file_discovery_open_header | 0.092303 | 2.1% | 96 |
| required_id_checks | 0.054430 | 1.2% | 2,126,898 |
| Untimed control flow / timer bookkeeping | 0.495836 | 11.4% | — |

### seed2-2m: events_validate_resolve

Profiles: 3. Median phase: **3.143793 s**; timed operations: 2.530667 s; residual: 0.608743 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| winner_csv_reparse | 0.895487 | 28.5% | 1,999,221 |
| identity_resolution | 0.703880 | 22.4% | 1,955,518 |
| product_category_lookup | 0.339775 | 10.8% | 1,955,518 |
| event_timestamp_window | 0.125957 | 4.0% | 1,994,247 |
| raw_winner_release | 0.122825 | 3.9% | 1,999,221 |
| winner_table_iteration | 0.118958 | 3.8% | 1,999,222 |
| compact_event_push | 0.082002 | 2.6% | 1,955,518 |
| event_type_purchase_price | 0.052747 | 1.7% | 1,979,157 |
| required_id_checks | 0.051402 | 1.6% | 1,999,221 |
| lateness_check | 0.051035 | 1.6% | 1,974,227 |
| winner_table_release | 0.005334 | 0.2% | 1 |
| event_buffer_allocation | 0.000012 | 0.0% | 1 |
| Untimed control flow / timer bookkeeping | 0.608743 | 19.4% | — |

Operation timings include clock-read cost. Residual also includes uninstrumented control flow, so it is not a pure overhead estimate. Medians are computed independently and need not add exactly. These are diagnostic measurements; ordinary benchmark samples disable detailed timers.

## Detailed event timings: rust-event-details-docker

Source: `rust-pipeline/bench-artifacts/rust-event-details-docker-20261121T042217397143Z-a4543f/results.json`

### dev-1m: events_parse_dedup

Profiles: 3. Median phase: **1.449112 s**; timed operations: 1.173535 s; residual: 0.256809 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| index_insert_replace_growth | 0.527032 | 36.4% | 1,028,618 |
| csv_structure_utf8 | 0.193001 | 13.3% | 1,072,086 |
| dedup_hash_lookup_compare | 0.148195 | 10.2% | 1,071,554 |
| buffered_line_read | 0.124665 | 8.6% | 1,072,116 |
| winner_allocation_copy | 0.088032 | 6.1% | 1,028,618 |
| ingest_timestamp_parse | 0.072919 | 5.0% | 1,071,554 |
| required_id_checks | 0.027070 | 1.9% | 1,071,635 |
| file_discovery_open_header | 0.010323 | 0.7% | 31 |
| Untimed control flow / timer bookkeeping | 0.256809 | 17.7% | — |

### dev-1m: events_validate_resolve

Profiles: 3. Median phase: **1.359298 s**; timed operations: 1.059789 s; residual: 0.299509 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| winner_csv_reparse | 0.397017 | 29.2% | 999,601 |
| identity_resolution | 0.245527 | 18.1% | 977,930 |
| product_category_lookup | 0.145737 | 10.7% | 977,930 |
| event_timestamp_window | 0.060256 | 4.4% | 997,120 |
| raw_winner_release | 0.050614 | 3.7% | 999,601 |
| winner_table_iteration | 0.045319 | 3.3% | 999,602 |
| compact_event_push | 0.037522 | 2.8% | 977,930 |
| event_type_purchase_price | 0.026097 | 1.9% | 992,518 |
| required_id_checks | 0.025230 | 1.9% | 999,601 |
| lateness_check | 0.025011 | 1.8% | 990,046 |
| winner_table_release | 0.003032 | 0.2% | 1 |
| event_buffer_allocation | 0.000010 | 0.0% | 1 |
| Untimed control flow / timer bookkeeping | 0.299509 | 22.0% | — |

### seed2-2m: events_parse_dedup

Profiles: 3. Median phase: **3.041763 s**; timed operations: 2.566697 s; residual: 0.475067 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| index_insert_replace_growth | 1.180898 | 38.8% | 2,050,415 |
| csv_structure_utf8 | 0.381517 | 12.5% | 2,127,777 |
| dedup_hash_lookup_compare | 0.378926 | 12.5% | 2,126,730 |
| buffered_line_read | 0.258313 | 8.5% | 2,127,872 |
| winner_allocation_copy | 0.202449 | 6.7% | 2,050,415 |
| ingest_timestamp_parse | 0.141022 | 4.6% | 2,126,730 |
| required_id_checks | 0.053342 | 1.8% | 2,126,898 |
| file_discovery_open_header | 0.029402 | 1.0% | 96 |
| Untimed control flow / timer bookkeeping | 0.475067 | 15.6% | — |

### seed2-2m: events_validate_resolve

Profiles: 3. Median phase: **3.006109 s**; timed operations: 2.404029 s; residual: 0.597617 s.

| Subphase | Median seconds | Share of phase | Median calls |
| --- | ---: | ---: | ---: |
| winner_csv_reparse | 0.848116 | 28.2% | 1,999,221 |
| identity_resolution | 0.652564 | 21.7% | 1,955,518 |
| product_category_lookup | 0.302080 | 10.0% | 1,955,518 |
| event_timestamp_window | 0.122094 | 4.1% | 1,994,247 |
| compact_event_push | 0.121983 | 4.1% | 1,955,518 |
| raw_winner_release | 0.101413 | 3.4% | 1,999,221 |
| winner_table_iteration | 0.099110 | 3.3% | 1,999,222 |
| event_type_purchase_price | 0.051866 | 1.7% | 1,979,157 |
| required_id_checks | 0.050640 | 1.7% | 1,999,221 |
| lateness_check | 0.049424 | 1.6% | 1,974,227 |
| winner_table_release | 0.006296 | 0.2% | 1 |
| event_buffer_allocation | 0.000011 | 0.0% | 1 |
| Untimed control flow / timer bookkeeping | 0.597617 | 19.9% | — |

Operation timings include clock-read cost. Residual also includes uninstrumented control flow, so it is not a pure overhead estimate. Medians are computed independently and need not add exactly. These are diagnostic measurements; ordinary benchmark samples disable detailed timers.


## Final image and dispatch-guard recheck

After adding the explicit benchmark gate for inherited diagnostics, the final release binary again passed all 36 native fixtures in ordinary and detailed modes. The final rebuilt image again passed all 36 Docker fixtures under the unchanged scorer wrapper (five-CPU / 24-GB cap), plus a further ordinary and detailed 1M development run through the shared harness. Both final invocations passed exact output/schema/invariant validation with no checkpoint warnings; these two smoke samples are excluded from the three-run tables above. Their report is `bench-artifacts/rust-event-final-gate-20261121T042757350730Z-4e6367/`.

Current image: `shopping-sessions-rust:latest`, ID `sha256:a6866b52b3c1079815a55ae8eb7aa1cf84e2ba0251ff6403f645578295a2893e`, Linux AMD64, UID/GID 1000. The current Docker-save archive is `shopping-sessions-rust-linux-amd64.tar` (84,883,456 bytes), SHA-256 `3a5bb2aef17f73793e9517978d19dddadc3a0edc5ab1524c36b454f2b07c6abf`. Its manifest/config were read back and verified against that image ID, platform and user. Both builds used the existing temporary CA-secret setup; tracked Dockerfile and Cargo dependencies were unchanged.
