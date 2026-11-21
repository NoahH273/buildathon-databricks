# Shopping Sessions DuckDB and Rust hybrid specification

Implement the hybrid in `starter-duckdb/` of `NoahH273/buildathon-databricks`. Rust parses the source files and computes last-click credit and campaign assists. DuckDB selects winning events, builds sessions and user totals, and writes both final Parquet tables. A narrow Parquet handoff lets the two engines run in separate phases and keeps the existing Python and Docker entry points.

Use `rules.md` as the source of truth. The checked-in fixtures and development answer are acceptance tests. Use the DuckDB baseline as a reference for differential tests on additional seeds. [Challenge rules](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/rules.md) · [DuckDB starter](https://github.com/NoahH273/buildathon-databricks/tree/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb)

## 1. Repository and harness integration

| File or directory | Responsibility |
| --- | --- |
| `starter-duckdb/rust/` | Standalone Cargo binary named `shopping-hybrid`, with a committed `Cargo.lock`. Provide `ingest` and `attribute` subcommands. Keep parsing and attribution independent of the Parquet readers and writers so either can be tested using small in-memory inputs. |
| `starter-duckdb/pipeline/pipeline.py` | Orchestrate the phases, create per-run scratch paths, invoke the Rust binary with checked subprocess calls, execute DuckDB SQL, and write final outputs. Keep `duckdb==1.5.6`. |
| `starter-duckdb/Dockerfile` | Use a pinned Rust builder that can build the locked dependencies and targets the same Debian release as the Python runtime. Run `cargo build --release --locked`, and copy the executable to `/app/bin/shopping-hybrid` in the final image. Keep the current runtime user and `CMD`. Add `rust/target/` to `.dockerignore` so local builds stay outside the Docker context. |
| `starter-duckdb/run.sh`, `Makefile`, `fixtures/`, `scripts/` | Reuse the existing shell entry point, image build and test targets, fixtures, and comparators. |

In Python, resolve the binary from `HYBRID_BIN` if set and otherwise use `/app/bin/shopping-hybrid`. Read `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, and `MEM_GB` as the starter does. The image runs with no arguments. The native fixture runner sets paths in the environment and also appends them as positional arguments. Use environment paths at the Python entry point, then pass explicit paths to the Rust subcommands.

Use this internal command contract. The paths shown are per-run scratch subdirectories except for the original input. The attribution budget is in bytes.

```text
shopping-hybrid ingest --data DATA_DIR --out WORK/ingest --threads NCPU
shopping-hybrid attribute --input WORK/handoff --out WORK/metrics --threads NCPU --memory-bytes BUDGET
shopping-hybrid attribute --input WORK/sorted --out WORK/metrics --threads 1 --memory-bytes BUDGET --sorted-bucket N
```

Write `deferred_buckets.json` with a JSON array of bucket numbers on a successful ordinary attribution run, including an empty array when every bucket finished. Use a nonzero status for unexpected failures. The sorted invocation processes one reported bucket and writes its missing metrics shard. Reserve separate names such as `bucket-0.parquet` for the metric files and scan just those files for final aggregation.

The scorer builds `linux/amd64` and runs as UID 1000 with 8 vCPUs, 24 GB of RAM, no swap, no network, and read-only input and root filesystems. Build the Rust binary for the container target with a portable CPU baseline. Resolve all dependencies at image build time. Write intermediate files and spills under `SCRATCH_DIR` and final files under `OUTPUT_DIR`. Close the DuckDB connection before launching the memory-intensive Rust attribution phase. Keep its persisted relations or handoff files in scratch so Python can resume with a fresh connection. Exit successfully only after both output tables have been written. [Starter runtime contract](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/README.md#container-limits) · [Fixture runner](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/scripts/run_fixtures.py)

## 2. Rust ingestion and typed staging

1. Read the manifest and compute signed UTC millisecond bounds. Load products and campaigns into byte-keyed lookup maps. Assign category codes in ascending UTF-8 byte order, including `unknown`. Assign dense campaign codes from 1 upward, with 0 reserved for `organic`. Persist both code-to-label dimensions as typed Parquet.
2. Read all link files and intern the valid endpoints. Use union–find with path compression and union by size. Keep the byte-wise minimum ID for each component independently of the union root, including linked IDs absent from both event and click files. Freeze a raw-to-canonical map that ingestion workers can share. A nonempty ID absent from the map is its own canonical ID.
3. Parse files in parallel using at most `NCPU` workers. Schedule larger files first and reuse a line buffer per worker. Split only on LF and remove only the terminal LF. Skip exactly the first record in each file. Scan comma-delimited fields into borrowed byte slices with exact field counts. A quote pair may wrap an entire field and the interior cannot contain another quote. Discard structurally bad records before considering their IDs. Convert empty fields, including `""`, to null and preserve all other bytes, including spaces, CR, and case. Use the same parser for lookups and links. A later record equal to the header remains data.
4. Parse timestamps from byte slices. Fast-path digits-only epoch milliseconds with checked arithmetic and allow arbitrarily many leading zeroes. For UTC and offset forms, check exact punctuation, lengths, ASCII digits, Gregorian date validity, and time ranges. Subtract the signed offset, allowing hours 00–23 and minutes 00–59. Reject fractional seconds, leap seconds, invalid dates and offsets, and stray characters. For purchase prices, use checked ASCII decimal conversion with inclusive bounds 1–10,000,000. Use integer arithmetic throughout and perform the window check before computing ingest lag.
5. Write bounded Arrow record batches to per-worker Parquet shards with the Rust Arrow/Parquet crates. Lock compatible crate versions and enable the reader/writer support and compression codecs used by both stages. Start with Snappy intermediates, explicit schemas, and measured row-group sizes. Reuse builders and bound bytes in flight as well as row counts. Ensure empty streams are represented by an empty file with the same schema or by an explicitly typed empty DuckDB relation. [Rust Parquet batch reader and writer](https://docs.rs/parquet/latest/parquet/arrow/index.html)

| Ingest relation | Fields and contract |
| --- | --- |
| `event_candidates` | `event_id VARCHAR`, `ingest_ms BIGINT`, `is_valid BOOLEAN`, `person_id VARCHAR`, `event_ms BIGINT`, `purchase_cents BIGINT`, `category_code INTEGER`. One candidate for each structurally valid row with a nonempty event ID. The key, ingest time, and validity flag are required. Invalid candidates retain these three fields and can have null payload fields. |
| `click_candidates` | `person_id VARCHAR`, `click_ms BIGINT`, `campaign_code INTEGER`, `click_id BLOB`. Keep byte-exact normalized click IDs as Parquet binary for the Rust tie-break. All four fields are required on emitted rows. |
| Dimensions | `categories(category_code INTEGER, category VARCHAR)` and `campaigns(campaign_code INTEGER, campaign_id VARCHAR, channel VARCHAR)`. Include the organic row in the latter. |

Under `WORK/ingest/`, put per-worker shards in `event_candidates/` and `click_candidates/` and write the dimensions as `categories.parquet` and `campaigns.parquet`.

**Event candidates.** Compute `is_valid` from the source row’s nonempty user and product IDs, exact event type, timestamp in `[window_start_ms, window_end_ms)`, purchase price bounds when applicable, and `ingest_ms - event_ms <= 86400000`. For valid nonpurchase events, set cents to zero and category to null. For valid purchases, map the product category or use `unknown`. Preserve invalid candidates so an invalid earliest copy still wins global deduplication and drops the event. A malformed physical record or empty event ID never becomes a candidate.

**Click candidates.** Require a nonempty click and user ID, a known campaign, and a timestamp in `[window_start_ms - 604800000, window_end_ms)`. Filtering before explicit click-ID deduplication is equivalent here because copies of a click ID have identical values. The attribution algorithm can absorb those copies directly. Choosing the minimum on ties and taking the union of assist intervals are both idempotent. If a diagnostic explicitly reports unique clicks, compute that count by click ID.

## 3. DuckDB deduplication, sessions, and handoff

Configure the starter’s thread count and spill path. Start with roughly 80% of `MEM_GB` for DuckDB’s buffer manager and measure process or container memory independently. Keep major stages individually inspectable.

1. **Choose one event globally per ID.** Start with `GROUP BY event_id` and `arg_min(struct_pack(...), ingest_ms)` over a single packed payload containing `is_valid` and the other payload fields. Pass a non-null struct for every candidate, including invalid candidates with null fields. Filter on the winner’s validity after aggregation. Equal-ingest copies are identical. Compare this plan with a `ROW_NUMBER()` reference on fixtures and dev. If aggregation is slower or uses more memory at scorer scale, use the measured winner. [DuckDB aggregate semantics](https://duckdb.org/docs/1.5/sql/functions/aggregates#general-aggregate-functions)
2. **Build sessions and user totals.** For each canonical person, order events by `(event_ms, event_id)`. Compute the previous timestamp and a cumulative start flag using the same order in both windows. Start a session only for the first event or when the gap is strictly greater than `1800000` ms. Assign all events to the UTC day of their session start. Roll up counts and revenue by person/day. Sum purchase revenue by category over the entire person/day and select the largest total, then the smallest category code on a tie. Keep `top_category` null for a day without purchases. Use signed 64-bit counts and cents at handoff and output boundaries.
3. **Prepare one numeric key space for attribution.** Materialize a unique mapping from people with sessions to dense numeric `person_key` values. The numbering only needs to be stable within that run. Join both the session and click handoff to this exact mapping. The click join drops people without sessions. Optionally prune each person’s clicks to `[first_session_start - 604800000, last_session_start]` if those bounds can be obtained cheaply from the session rollup.
4. **Partition the handoff by person.** Compute the same `bucket = person_key % B` in both exports, using nonnegative keys. Use a single DuckDB partitioned Parquet write per relation into fresh directories for the run. Start with one bucket for fixtures and up to one per available worker for larger runs. Benchmark 4, 8, and 16 at scorer scale. Every file in a bucket belongs to that bucket, and an individual person is always local to one worker. Process all files in a bucket. File order, row order, row-group boundaries, and the presence of a corresponding click partition are unspecified. [DuckDB partitioned writes](https://duckdb.org/docs/1.5/data/partitioning/partitioned_writes)

| Handoff relation | Required fields inside each partition |
| --- | --- |
| `sessions/` | `person_key BIGINT, start_ms BIGINT, revenue_cents BIGINT, has_purchase BOOLEAN`. Session starts are unique within a person. |
| `clicks/` | `person_key BIGINT, click_ms BIGINT, campaign_code INTEGER, click_id BLOB`. Keep every distinct eligible campaign at a timestamp, even when only one can win credit. |
| Rust `campaign_metrics` | `campaign_code INTEGER, epoch_day INTEGER, sessions BIGINT, purchasing_sessions BIGINT, revenue_cents BIGINT, assisted_sessions BIGINT`. Emit one partial row per bucket and active campaign/day. `epoch_day` is `start_ms.div_euclid(86400000)`. |

Store ordinary handoff files under `WORK/handoff/{sessions,clicks}/bucket=N/`. Store each sorted fallback pair under `WORK/sorted/bucket=N/{sessions,clicks}.parquet`. Put a copy of `campaigns.parquet` in each handoff root so Rust can size the interval and metric state once before scheduling workers. Metrics shards go directly under `WORK/metrics/`.

Preserve the prepared user totals and label dimensions until final export. Once a stage has consumed its raw shards, release any scratch copies it no longer needs unless diagnostics are enabled. Avoid round trips of individual events through Python.

## 4. Rust attribution algorithms

### Bucket scheduling and memory

Load each bucket’s session and click records into compact vectors, then sort them by `(person_key, time_ms)` with an unstable sort. Sort clicks by person and time only. Compare click IDs directly as bytes when candidate times tie. Keep click ID bytes in a contiguous arena and store offsets in the sortable records. Run independent buckets in parallel with a bounded queue, largest first. An empty click partition means all its sessions are organic. Skip click-only groups.

Size work using Parquet footers and row counts, then enforce the Rust allocation budget as vectors and arenas grow. Include decoding buffers and sort allocations in the budget. Cap worker count at `NCPU` and admit work according to total estimated bytes in flight. An oversized bucket should be deferred before publishing any metrics for it. Report deferred bucket IDs back to Python. For each deferred bucket, use DuckDB to spill-sort each relation by person and time and write one ordered file per relation using `COPY ... (FORMAT parquet, PRESERVE_ORDER true)`. Close DuckDB and stream those files with the same per-person algorithm. The streaming path needs the current person’s session starts, current click record and tie winner, and pending campaign intervals. Publish one metrics shard for each completed bucket so a deferred or retried bucket contributes exactly once. Exercise the fallback with a forced low budget. [DuckDB COPY options](https://duckdb.org/docs/1.5/sql/statements/copy)

### One click cursor for credit and assists

Use `W = 604800000` milliseconds. Join the sorted person groups by advancing their cursors. For a person with sessions, store that person’s ordered session starts and precompute contiguous session-index ranges for each occupied UTC day. In the generated 14-day window there are at most 48 starts per UTC day because successive session starts for a person are more than 30 minutes apart. Keep the algorithm correct for other manifest windows.

For each session in increasing `start_ms`, consume all of the person’s as-yet-unconsumed clicks with `click_ms <= start_ms`. Skip clicks older than the first session start minus `W`; clicks later than the last session start are never consumed.

- **Credit.** Maintain the latest consumed timestamp and the byte-wise smallest click ID at that timestamp. A newer timestamp replaces the winner, and an equal timestamp replaces it only when its click ID is smaller. Consume every click at the session’s start before crediting the session. If the latest timestamp is at least `start_ms - W`, credit its campaign. Otherwise credit organic. Add one session, its purchase flag, and its revenue to the campaign/day totals.
- **Assists.** Each consumed click at time `t` opens or extends an inclusive interval `[t, t + W]` for its campaign. Keep one pending interval per campaign for the current person. If the next click for that campaign begins at or before the pending interval’s end, extend the end. Otherwise, count the completed interval and replace it. Flush remaining intervals after that person’s last session. With dense campaign codes, reuse a worker-owned array of interval slots and clear only the codes touched for that person.

For each completed interval `[lo, hi]`, use two binary searches on the person’s session starts:

```text
left  = first index whose start_ms is >= lo
right = first index whose start_ms is > hi
```

The covered sessions are the half-open index range `[left, right)`. If it is nonempty, use the day indices stored beside sessions `left` and `right - 1` to jump directly to the first and last occupied day ranges. Intersect those ranges and any occupied ranges between them with `[left, right)`. Add each positive intersection size directly to that campaign/day’s assist count. The union is computed per campaign, so repeated clicks cannot double-count a session and different campaigns can both assist. Accumulate into worker-local metrics and emit aggregates, avoiding a shared counter or a row per session–campaign pair.

After sorting, credit is linear in the number of sessions and clicks. Assist work adds two searches per merged interval and one counter update per intersected day with sessions. Memory for a person’s sweep is proportional to their session starts and campaign interval state. This is particularly useful for users combined through shared-device links, where a click-to-session join can repeat the same campaign many times.

### Final output

DuckDB sums the Rust metrics by `(campaign_code, epoch_day)` and joins the campaign dimension. Construct `day` with `DATE '1970-01-01' + CAST(epoch_day AS INTEGER)`. A row can contain only assist metrics. The organic row gets channel `organic` and zero assists. Write the following exact columns as Parquet under `OUTPUT_DIR`, including readable, correctly typed files for zero-row outputs.

| Directory | Columns |
| --- | --- |
| `user_daily/` | `user_id VARCHAR, day DATE, sessions BIGINT, events BIGINT, revenue_cents BIGINT, top_category VARCHAR` |
| `campaign_daily/` | `campaign_id VARCHAR, channel VARCHAR, day DATE, sessions BIGINT, purchasing_sessions BIGINT, revenue_cents BIGINT, assisted_sessions BIGINT` |

Cast final sums to `BIGINT`; `top_category` is nullable. Keep the two output directories free of intermediate Parquet. Output row order and file count are immaterial to the checker.

## 5. Tests and acceptance

Run from `starter-duckdb/` in a checkout with an activated Python environment, a Rust toolchain compatible with the lockfile, and Docker with buildx.

```bash
python3 -m pip install -r requirements.txt polars
cargo test --locked --manifest-path rust/Cargo.toml
cargo build --release --locked --manifest-path rust/Cargo.toml
native_scratch=$(mktemp -d)
HYBRID_BIN="$PWD/rust/target/release/shopping-hybrid" SCRATCH_DIR="$native_scratch" \
  python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py

# Uses the existing linux/amd64 build and scorer-style Docker wrapper.
make fixtures IMAGE=shopping-sessions:hybrid

# The official seed-1 dev dataset and exact expected-output comparison.
cargo build --release --locked --manifest-path ../generator/Cargo.toml
PATH="$PWD/../generator/target/release:$PATH" make dev
make run-dev IMAGE=shopping-sessions:hybrid
make check-dev ANSWER=../dev-answer
```

Require `36/36 fixtures passed` both natively and inside the container, and `MATCH` with zero duplicate keys on the dev dataset. If `CARGO_TARGET_DIR` is set, point `HYBRID_BIN` to that build’s actual executable. Run the existing suites without editing expected data. [Fixture catalog](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/fixtures/README.md) · [Docker runner](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/scripts/docker_run.sh) · [Make targets](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/Makefile)

Add focused checks for behavior the hybrid changes:

- **Rust parser and staging.** Verify CR preservation, header-only files and repeated header-like data, quoted empty values, leading-zero numeric fields, F2 offset extremes, invalid calendar dates, and arbitrary order of long transitive link chains. Round-trip click IDs and canonical names through Parquet. Use E4 and E5 to confirm that the packed DuckDB winner retains an invalid earliest row even when the payload is null or later copies name another user.
- **Rust attribution oracle.** Generate seeded small timelines and compare all four campaign metrics against brute force. For each session, explicitly enumerate valid clicks, pick the latest timestamp and then the smallest byte-wise click ID, and count each eligible campaign once. Include clicks at both inclusive bounds and one millisecond outside, duplicate IDs with identical values, different campaigns at the same timestamp, byte-order ties, days with no sessions, midnight, clicks with no sessions, and organic-only inputs.
- **Partition stress.** Compare results with one bucket, multiple buckets, and a forced spill-sort fallback. Include a bucket with sessions and no click files, a person split across multiple physical files in one bucket, and a heavy canonical person. Verify that one whole fixture or a generated case with zero sessions produces both typed outputs.
- **End-to-end invariants.** Event count equals the number of valid winning events, both outputs agree on total sessions and revenue, purchasing sessions never exceed credited sessions, organic has zero assists, and total assists cover at least the non-organic credited sessions. Check unique output keys and Parquet `DATE` schema.

## 6. Profile before changing the algorithm

Build the baseline and hybrid as separate image tags. Generate each comparison dataset once and run both images against that directory with the same platform, CPU limit, and memory limit. For example, use 10 million events and 2.5 million clicks as an intermediate step before a scorer-scale run. This command assumes the verified baseline image is tagged `shopping-sessions:baseline`.

```bash
bench_dir=$(mktemp -d)
../generator/target/release/generate --seed 7 --events 10000000 --clicks 2500000 \
  --output "$bench_dir/input"
IMAGE=shopping-sessions:baseline CPUS=8 MEM=24g \
  scripts/docker_run.sh "$bench_dir/input" "$bench_dir/baseline"
IMAGE=shopping-sessions:hybrid CPUS=8 MEM=24g \
  scripts/docker_run.sh "$bench_dir/input" "$bench_dir/hybrid"
python3 scripts/compare.py "$bench_dir/hybrid" "$bench_dir/baseline"
```

Record wall time by phase, bytes written and read at each handoff, largest bucket and person group, count of merged intervals, peak container memory, and peak scratch usage. Obtain container memory from cgroup metrics or container monitoring when available. A timer wrapped around the Docker CLI measures the client process’s memory. Preserve scratch for one diagnostic run and keep the regular timed path free of extra counting queries.

| Bottleneck | Next change to measure |
| --- | --- |
| Long tail in Rust ingestion | Schedule the largest input files first. If a single part still dominates, split it into byte ranges and assign each physical line to the worker whose range contains its starting byte. Read through that line’s next LF or EOF. Only the range owning the first line skips the header. |
| Global event deduplication spills heavily | Compare hash aggregation to the window-based reference. If both struggle, prepartition candidates by a stable hash of `event_id`, deduplicate using the full ID within each partition, and combine valid winners before sessionizing across people. |
| Session windows dominate | Benchmark grouping by person and timestamp first, retaining event counts and purchase sums. Sessionize distinct timestamps, and join the resulting session day back to the category contribution by person and timestamp. Preserve all contributions at a shared millisecond. |
| Parquet exchange or attribution sort dominates | Compare row-group sizes, Snappy and alternative supported codecs, byte-aware partition counts, and early removal of clicks outside a person’s session range. If sorted spill work is routine, compare that path directly with the ordinary in-memory bucket sort. |

Report which changes improved end-to-end time with exact output parity. The interval algorithm reduces the amount of attribution work by design; scorer performance still depends on parsing, DuckDB grouping and sorting, and the cost of moving batches between engines.
