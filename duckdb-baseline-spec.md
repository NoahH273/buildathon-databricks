# Shopping Sessions DuckDB baseline specification

Implement the Shopping Sessions pipeline in `starter-duckdb/` of `NoahH273/buildathon-databricks`. The baseline should produce both daily Parquet tables, pass all 36 fixtures, and exactly match the public development answer. Favor an implementation whose intermediate results can be inspected and whose run fits the scorer’s container. Use a bounded click-to-session range join as the reference implementation for assists.

The repository’s `rules.md` governs behavior. The source links below pin the reviewed revision. Check the working checkout for any later rules changes before implementing. [Rules](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/rules.md) · [DuckDB starter](https://github.com/NoahH273/buildathon-databricks/tree/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb)

## 1. Entry point and working files

- Replace the skeleton in `starter-duckdb/pipeline/pipeline.py`. Small parser and SQL modules may live beside it if that makes the stages easier to inspect. The Dockerfile already copies that directory. Keep the existing fixture and comparison scripts and their expected outputs.
- Use the starter’s pinned `duckdb==1.5.6`. The image must build for `linux/amd64`. The existing `CMD` calls the pipeline with zero arguments. Read `DATA_DIR`, `OUTPUT_DIR`, and `SCRATCH_DIR` from the environment, with the starter defaults. Native fixture runs also append two positional arguments. Use the environment for paths so both entry points work.
- The scorer uses 8 vCPUs, 24 GB of RAM, no swap, no network, a read-only root and data mount, and UID 1000. Write working files under `SCRATCH_DIR` and outputs under `OUTPUT_DIR`. Use `NCPU` and `MEM_GB` for DuckDB’s thread and memory settings. Start with the starter’s roughly 80% DuckDB memory setting, a spill directory under scratch, and `preserve_insertion_order = false`. This setting leaves headroom for Python and allocations outside DuckDB’s buffer manager.
- Use a per-run work directory inside scratch for normalized input, the DuckDB database, and spills. Materialize the major stages in a file-backed connection so they can be queried independently. Close the connection before cleaning up that run’s working files. Propagate unexpected I/O, database, and output errors as a nonzero exit.

[Container limits and local workflow](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/README.md) · [DuckDB workload tuning](https://duckdb.org/docs/1.5/guides/performance/how_to_tune_workloads)

## 2. Input parsing and typed staging

Read the manifest once. Compute integer UTC millisecond bounds `window_start_ms` and `window_end_ms` and check that the end is greater than the start. Read every `*.csv` directly inside `events/`, `clicks/`, and `links/`; read `products.csv` and `campaigns.csv` as the lookup tables. File order has no semantic meaning and header-only files are valid.

| Input | Positional fields |
| --- | --- |
| Events | `event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents` |
| Clicks | `click_id, user_id, campaign_id, ts` |
| Links | `user_id_a, user_id_b` |
| Products | `product_id, category` |
| Campaigns | `campaign_id, channel` |

Use a streaming Python parser over binary file handles. Discard exactly the first physical record of each input file as its header. For each subsequent record, remove one terminal LF if present and split on commas. Require exactly the expected number of fields. A token with quotes is valid only when it has exactly one opening and one closing double quote, enclosing the entire token, with no double quote inside. Remove the enclosing quotes. Reject the entire record if any token is malformed, including tokens whose values would later be ignored. Convert a zero-length value to null. Decode the remaining field bytes as UTF-8, preserving spaces, case, tabs, carriage returns, and other value bytes. A later record whose text equals the header is still data.

For the baseline, stage accepted values as compact newline-delimited JSON objects in scratch. Use the JSON encoder for escaping so an embedded carriage return or tab survives the handoff. Keep raw fields as strings or JSON null, including IDs and numeric-looking fields. Stream the writes, retaining one row at a time, and stage only structurally valid records with the required nonempty event ID, click ID, or link endpoints. Load staging files with `read_json`, explicit `VARCHAR` columns, and newline-delimited format. When a relation has zero accepted rows, create the same typed table with zero rows. Delete normalized files once their corresponding tables are materialized. Verify that the built image can load the JSON extension offline. Bake it into the image at build time if the pinned wheel does not bundle it. [DuckDB JSON reader](https://duckdb.org/docs/1.5/data/json/loading_json) · [DuckDB JSON extension](https://duckdb.org/docs/1.5/data/json/overview)

### Timestamp and price conversion

Implement one shared timestamp conversion used for event and click `ts`. The result is a nullable signed `BIGINT` in milliseconds. Use full string matches with `[0-9]`, and convert only after recognizing the format.

| Form | Conversion |
| --- | --- |
| F1 | Match `YYYY-MM-DDTHH:MM:SSZ` exactly. Parse the first 19 characters as a naive timestamp representing UTC. Check calendar validity and require a format round trip of those 19 characters. Convert with `epoch_ms`. |
| F2 | Match `YYYY-MM-DDTHH:MM:SS±HH:MM` exactly. Parse and validate the local date and time as for F1. Require offset hours 00–23 and minutes 00–59. Subtract the signed offset in milliseconds from the local millisecond value. Both signs of zero represent UTC. |
| F3 | Match one or more ASCII digits. Strip leading zeroes from the numeric token for conversion, treating an all-zero token as zero. Use a checked `BIGINT` conversion. An overflow is outside the admissible window and drops the row without aborting the run. |

For F1 and F2, use the exact calendar format `%Y-%m-%dT%H:%M:%S` with `try_strptime` and a matching `strftime` check. Invalid shapes or calendar values return null. Parse `ingest_ts` as F1 after event deduplication; its source values are guaranteed valid. For purchase prices, use the same ASCII unsigned-digit conversion as F3 and require the inclusive range 1–10,000,000. Keep timestamps as integers through temporal comparisons. Convert a validated session start with `CAST(make_timestamp_ms(start_ms) AS DATE)` for its UTC day. [Date parsing](https://duckdb.org/docs/1.5/sql/functions/dateformat) · [Timestamp functions](https://duckdb.org/docs/1.5/sql/functions/timestamp)

## 3. Build the pipeline in this order

The names below are suggested inspection points. Use explicit types and columns at stage boundaries. Keep DuckDB’s default byte-wise string comparison, without locale or case-insensitive collations. Explicit `ORDER BY encode(value)` is suitable for ties when the ordering needs to be visible in SQL.

### 3.1 Canonical person map

Run union–find over valid link endpoints across all link files. Use path compression and union by size or rank. Track the byte-wise smallest UTF-8 ID in each component independently of the chosen union root. Materialize a unique mapping `person_map(raw_user_id, person_id)` with one row for each linked ID, including IDs that never appear in events or clicks. Apply it to both streams before temporal processing. A left lookup with `COALESCE(person_id, raw_user_id)` gives an unlinked ID its own person. The lookup must preserve row counts. Repeated and reversed edges and self-links have no effect.

### 3.2 Global deduplication and validation

**Events**

1. Materialize `raw_events` with structurally valid, nonempty event IDs. Across all files, use `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts)` and retain row 1. The raw F1 ingest strings are fixed-width UTC, so byte-wise order is chronological. Tied copies are identical by contract. Deduplicate globally by event ID even when copies name different users.
2. Validate the winning row. Require a nonempty user ID and product ID, an event timestamp in `[window_start_ms, window_end_ms)`, and exactly `view`, `cart`, or `purchase`. Apply price bounds only to purchases. A bad winner drops the event even if a later valid copy exists.
3. Keep rows with `ingest_ms - event_ms <= 86400000`. This keeps an event exactly 24 hours late and keeps negative ingest lag. Attach the canonical person and project narrow columns for the next stage. For purchases, resolve categories by left joining `products`; use `unknown` for an absent product. A missing product lookup never drops an event whose product ID is present. Represent a nonpurchase’s revenue contribution as zero.

**Clicks**

1. Materialize `raw_clicks` with structurally valid, nonempty click IDs. Deduplicate globally before validation. A distinct projection of the four parsed fields is sufficient because copies of a click ID have identical values.
2. Require a nonempty user ID, a campaign present in `campaigns`, and a click timestamp in `[window_start_ms - 604800000, window_end_ms)`. Attach the canonical person. Keep `valid_clicks(person_id, click_id, campaign_id, click_ms)` with all surviving campaigns and click IDs for assists.

### 3.3 Sessionization and user totals

1. Compute `LAG(event_ms)` partitioned by `person_id` and ordered by `event_ms, event_id`. Set a start flag for the first event or a gap strictly greater than `1800000` ms. In a second window, cumulatively sum that flag using the same ordering and an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame. Equal timestamps stay together. The unique event ID gives both windows the same tie order.
2. Materialize `sessions` with one row per `(person_id, session_no)`, the minimum event time as `start_ms`, the UTC date of that start as `day`, event count, purchase count, and summed purchase cents. The date comes from the session start even when later events cross midnight.
3. Group the sessions by person and day for session count, event count, and revenue. Separately group purchase events by person, session start day, and category after attaching each event to its session. Sum category revenue across the entire day. Pick the category with the largest total and the byte-wise smallest name on ties. Left join this winner into the user totals, so days without purchases have null `top_category`. Expose `person_id` as `user_id`.

### 3.4 Last-click credit

Create a credit-only relation with one click per `(person_id, click_ms)`, chosen by ascending byte-wise `click_id`. Match one click per session with the following predicate. [DuckDB ASOF joins](https://duckdb.org/docs/1.5/guides/sql_features/asof_join)

```sql
FROM sessions AS s
ASOF LEFT JOIN credit_clicks AS c
  ON s.person_id = c.person_id
 AND s.start_ms >= c.click_ms
```

After the join, choose `c.campaign_id` when `s.start_ms - c.click_ms <= 604800000`; use `organic` for no match or a match older than the cutoff. Keep these sessions in the result. Aggregate by campaign and session day to obtain credited sessions, credited sessions whose purchase count is positive, and credited revenue.

### 3.5 Campaign assists and combined totals

Join `sessions` to `valid_clicks` on equal canonical person, `click_ms <= start_ms`, and `click_ms >= start_ms - 604800000`. Project and deduplicate `(person_id, session_no, day, campaign_id)` before counting by campaign and day. This counts the credited campaign and any other eligible campaign, once each per session. Same-millisecond clicks from different campaigns can both assist even though only one wins credit.

Combine credit and assist counts using `UNION ALL` with zeroes for the other branch’s metrics and a final group by `(campaign_id, day)`. This retains assist-only rows with zero credited metrics. Join in `campaigns.channel` and set the channel for `organic` to `organic`. Its assist count is zero.

Keep the assist query narrow. If its intermediate dominates memory, execute it one session day at a time using the same exact join predicate. For a day with UTC bounds `[D, D + 86400000)`, prefilter clicks to `[D - 604800000, D + 86400000)` before the join. Since a session has exactly one start day, the per-day results can be appended without cross-day deduplication. Re-run the fixture and dev checks after this change.

## 4. Output contract

Create both directories under `OUTPUT_DIR`. Write one regular `part-0.parquet` per directory using DuckDB `COPY`, including a schema-correct empty file when there are no rows. The table definitions below use `BIGINT` for counts and cents. Cast final sums explicitly so wide DuckDB aggregates are written as Parquet integers.

| Output directory | Unique key | Exact columns |
| --- | --- | --- |
| `user_daily/` | `(user_id, day)` | `user_id VARCHAR, day DATE, sessions BIGINT, events BIGINT, revenue_cents BIGINT, top_category VARCHAR NULL` |
| `campaign_daily/` | `(campaign_id, day)` | `campaign_id VARCHAR, channel VARCHAR, day DATE, sessions BIGINT, purchasing_sessions BIGINT, revenue_cents BIGINT, assisted_sessions BIGINT` |

Output only the listed columns. Metrics are non-null, and `top_category` is the only nullable output field. Row order is immaterial. Exit successfully after both writes complete.

## 5. Build and verify

Run the commands below from `starter-duckdb/` in a checkout with Python 3.10+, Docker with buildx, and Rust 1.85+ for the generator. Install local Python dependencies in an activated virtual environment.

```bash
python3 -m pip install -r requirements.txt polars

# Native fixture loop.
native_scratch=$(mktemp -d)
SCRATCH_DIR="$native_scratch" python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py

# Build the amd64 image and run the scorer-style fixture suite.
# make fixtures depends on make build.
make fixtures

# Reproduce the published seed-1 dev dataset.
cargo build --release --locked --manifest-path ../generator/Cargo.toml
PATH="$PWD/../generator/target/release:$PATH" make dev
make run-dev
make check-dev ANSWER=../dev-answer
```

Expect `36/36 fixtures passed` from both fixture runs and `MATCH` with zero duplicate keys from the dev comparison. The dev target generates 1,000,000 events and 250,000 clicks. After those checks, run a larger generated seed under the scorer limits and record wall time, peak memory, and scratch usage. A non-dev seed has no checked-in expected answer. Use the invariants below and compare against the verified baseline before changing the algorithm. If hardware prevents a scorer-sized local run, report the limits actually used with the timing. [Fixture runner](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/scripts/run_fixtures.py) · [Make targets](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/Makefile)

## 6. Debug by stage

Time each stage separately. During debugging, record row counts for parsed, deduplicated, and valid events and clicks, the number of sessions, the number with non-organic credit, and the number of distinct assist pairs. Make extra diagnostic counts opt-in for timed runs. Preserve an isolated scratch database for SQL inspection during a local diagnostic run if needed. Check the earliest stage whose output differs before changing downstream queries.

| Symptom | First check |
| --- | --- |
| Split users or unexpected canonical names | Inspect the map and check that every link endpoint is represented. Use L1–L4 for transitivity, unused IDs, malformed or repeated links, and cross-ID credit. |
| Extra events after cleaning | Use E4–E6 to inspect the winning ingest copy, the validation order, and the inclusive late boundary. |
| Wrong counts around gaps or midnight | Use E1–E3 and E16 to inspect ordered events, the start flag, session number, and UTC start day. |
| Missing or altered IDs and timestamps | Use E15 and E17–E20 for quotes, offset bounds, ASCII digits, preserved spaces, and carriage returns. Inspect the parser-to-DuckDB handoff as well as timestamp conversion. |
| Wrong credit or assist-only rows missing | Use A2, A4, A5, and A10–A12. Inspect the credit tie winner separately from the assist candidates. Check both inclusive attribution bounds and the zero-valued credited metrics on assist-only rows. |
| Correct small runs but slow or failing large runs | Collect per-stage timing, peak RSS, and spill size. Inspect the assist join cardinality and the largest canonical person groups. Use `EXPLAIN` first and `EXPLAIN ANALYZE` on a representative local run because it executes the query. Distinguish a container memory kill from a DuckDB exception or full scratch volume. |

Reproduce one fixture without the harness’s output capture. This example isolates E5.

```bash
debug_dir=$(mktemp -d)
mkdir -p "$debug_dir/out" "$debug_dir/scratch"
DATA_DIR="$PWD/fixtures/E5/input" OUTPUT_DIR="$debug_dir/out" SCRATCH_DIR="$debug_dir/scratch" \
  python3 pipeline/pipeline.py
python3 scripts/compare.py "$debug_dir/out" fixtures/E5/expected
```

Before calling the baseline complete, verify these invariants alongside the exact comparisons.

- Every surviving event belongs to one session. Summed `user_daily.events` equals the valid event count after cleaning and lateness.
- Overall session totals and revenue totals agree between the two outputs. For every campaign/day, `0 <= purchasing_sessions <= sessions`.
- `organic` has zero assists. Summed assists are at least the credited non-organic session total, and assist-only rows have positive assists with zero credited metrics.
- Both output keys are unique, both dates are Parquet `DATE`, and both directories contain readable Parquet even for a dataset with zero sessions.

[Fixture catalog](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/fixtures/README.md) · [Exact comparator](https://github.com/NoahH273/buildathon-databricks/blob/5bcaa83e1a6b357f79f8f905b9d4b75dd93d99ed/starter-duckdb/scripts/compare.py)
