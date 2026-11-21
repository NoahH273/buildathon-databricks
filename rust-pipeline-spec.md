# Rust pipeline implementation plan — Shopping Sessions

> **Execution brief for Codex Cloud:** Implement the complete Rust solution described here, not merely a design or an empty-output skeleton. First read root `rules.md`, root `AGENTS.md`, `description.txt`, the current test harness and generator. **The live `rules.md` wins over this file if they disagree.** Do not edit the rules or expected answers to make tests pass. The definition of done is a correct, scorer-compatible Rust image, not just a fast local binary.

## 1. Deliverable, integration and ownership

Create an isolated `rust-pipeline/` directory containing a Rust Cargo binary crate, source modules, `Cargo.toml`, committed `Cargo.lock`, `Dockerfile`, `.dockerignore`, `Makefile`, and a short `README.md` with actual commands. Use the existing Python comparison scripts **unchanged if possible** (either refer to them at `../starter-duckdb/scripts/` from the monorepo or copy them unchanged into `rust-pipeline/scripts/` for a standalone submission). Do not modify the DuckDB/Polars/PySpark pipelines, `generator/`, fixtures, `dev-answer/`, `AGENTS.md` or `rules.md`.

**Important submission constraint:** `description.txt` requires a public GitHub repository with a **Dockerfile at the repository root** of the submitted commit (repository <=50 MB). A `rust-pipeline/Dockerfile` works for development with `docker build -f rust-pipeline/Dockerfile rust-pipeline`, **not** necessarily for directly submitting this monorepo. Before final submission, either publish `rust-pipeline/` as the root of a separate public repository with its own Dockerfile and tests, or deliberately promote the Rust Dockerfile to this repo's root as an explicitly coordinated integration change. Don't silently overwrite other agents' root configuration; note the required packaging step in the README/report.

Entry point: native binary must run with **zero positional arguments**, taking `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, and `MEM_GB` environment variables (default to `/data`, `/output`, `/scratch` for container use). The existing harness appends positional `data output` arguments; either accept those **as an optional override** for native test runs or ignore them and rely on environment values, which the harness also sets. Never require arguments in Docker `CMD`. Exit nonzero on fatal failure; no partial success or synthetic empty results.

Environment: Linux amd64, 8 allocated vCPUs, 24 GB RAM, no swap, network disabled at runtime, UID 1000, read-only root and input, writable `/output` and `/scratch`, tiny tmpfs `/tmp` (64 MB). Build has network and <=10 min. Image <=8 GB, combined output+scratch <=50 GB, <=4096 processes/threads; avoid voluminous stdout (<50 MB; diagnostics to stderr). Place all temp/cache/spill files under scratch. Use a multi-stage image: pin Rust toolchain/toolchain version compatible with dependencies, `cargo build --release --locked` in builder, and copy the executable plus any required shared libs into a minimal compatible runtime; verify uid 1000/no-network/read-only operation. Prefer a portable release build over aggressive native CPU features; optional AVX-512 after verifying scorer compatibility.

## 2. Data contract — exact behavior, not approximations

Read every file matching `events/*.csv`, `clicks/*.csv`, `links/*.csv`, plus `products.csv`, `campaigns.csv`, and `manifest.json`. Each CSV file has a header; header-only files are valid. Part count and size are variable. Do not hardcode users, campaigns, file counts, seed, timestamps, date ranges or rates.

CSV: UTF-8, records separated **only** by LF byte; CR is content (a trailing CR makes last field different/possibly invalid). Commas are field separators; any field may be wholly double-quoted, but commas/quotes/newlines never occur within values. Quotes may occur **only as an enclosing pair around a whole field**. Reject lines with wrong field count, partial/unbalanced/extra quotes, empty lines or invalid UTF-8 (if encountered); strip one enclosing quote pair; map a zero-length decoded field to null; do **not** trim spaces/CR/case. Perform structural parse for all fields even if one is subsequently ignored. Validate required IDs as specified, not all fields indiscriminately.

Timestamps `ts` in event/click: F1 exact `YYYY-MM-DDTHH:MM:SSZ`; F2 exact local date/time followed by `+HH:MM` or `-HH:MM` (hour 00–23, minute 00–59; subtract signed offset to UTC); F3 one or more **ASCII** decimal digits representing nonnegative Unix milliseconds (leading zeroes allowed). Reject fractional seconds, invalid calendars, lower-case letter separators, leap seconds, etc. Parse to integer UTC milliseconds, with checked arithmetic; an enormous F3 token is **outside the window, not a parser crash**. A robust optimization: compare a normalized digit string to the decimal representation of the admissible nonnegative upper bound before converting to i64; do not interpret huge values as valid via overflow/wrap. F1/F2 parse date with strict round-trip validation. Use UTC for the manifest and daily grouping; use integer milliseconds everywhere for comparisons. Event window is [start, end); click window [start-7d, end). The manifest differs between datasets.

Links: valid nonempty endpoints join transitively. Find byte-wise minimum raw ID for each connected component (including IDs which appear *only* in links). Unlinked IDs identify themselves. Links apply across both time windows and all event/click records, regardless of order.

Events, in this **order**:
1. Discard malformed rows and blank `event_id` (not a later invalid `ts`).
2. Deduplicate globally on raw `event_id`: retain earliest `ingest_ts` (guaranteed valid F1); ties have identical content. This must precede all event validation. Later valid duplicate does **not** rescue an invalid earliest copy.
3. Validate kept row: nonempty `user_id`, valid in-window `ts`, exact event type `view`, `cart`, `purchase`, nonempty `product_id`, and for purchases `price_cents` matches ASCII digits with value 1..10,000,000 inclusive. Ignore price entirely for non-purchases.
4. Drop if `ingest_ms - event_ms > 86_400_000` (exactly 24h kept; negative lag kept).
5. Group surviving events for each **canonical person** in increasing event `ts`; start a new session only for a gap **strictly >1_800_000 ms**. Equal-time events always share one session. All events, including cross-midnight purchases, count on the session-start's UTC date. Products absent from clean `products.csv` are `unknown`, unless product_id is empty (invalid event).

Clicks: discard malformed/empty click IDs; dedup globally by click ID (copies identical); validate nonempty user, valid timestamp in click window and campaign present in clean `campaigns.csv`; attach canonical person. A session is credited to the person's qualifying click with greatest click time `<= session start` and `>= session start - 7d`; equal-time ties choose lexicographically/byte-wise smallest **click_id**. A click at session start is eligible; a click 1ms after is not. If no click qualifies, use campaign/channel `organic`. All distinct qualifying campaigns assist the session **once each**, including the credited campaign. `organic` has zero assists. One click can credit/assist many sessions.

Outputs (one or more `*.parquet` files directly under each directory, ignoring dot/underscore-prefixed files; no nested datasets):
- `user_daily` unique key `(user_id, day)`: `user_id` string canonical byte-min person ID; `day` Parquet logical DATE (UTC session-start); `sessions` integer; `events` integer; `revenue_cents` integer; `top_category` nullable string. Sum purchase cents for **all sessions starting that day** by category; choose greatest category revenue then byte-wise smallest category. No purchases => null.
- `campaign_daily` unique key `(campaign_id, day)`: `campaign_id` string; `channel` string from campaigns lookup (`organic` for organic); `day` DATE; `sessions` integer credited; `purchasing_sessions` integer credited sessions containing >=1 purchase; `revenue_cents` integer credited; `assisted_sessions` integer. Include **assist-only** campaign/day rows with credited metrics all zero. No unrequested columns; counts/cents are non-null; empty outputs still need at least one valid schema-bearing Parquet file per directory.

## 3. High-performance architecture

Favor **bounded-memory, narrow, cache-friendly records**, and preserve correctness. Suggested module boundaries (separate modules make concurrent agent ownership possible): `csv.rs` streaming parser, `timestamp.rs` strict parser, `ids.rs` interner and DSU, `events.rs` dedup/validation, `sessions.rs` sort and grouping, `clicks.rs` attribution/assists, `aggregate.rs` day-level results, `parquet.rs` output, `main.rs` orchestration. These are suggestions, not a license to restructure unrelated code.

### 3.1 Low-allocation parsing and string encoding

- Read files with `BufReader` or buffered byte scanning, using LF as the only boundary. Reuse buffers. Validate quoting at field byte level and slice fields into one reusable buffer before copying accepted values. Never use permissive general CSV settings that reinterpret CRLF or normalize whitespace.
- Intern repeated IDs, campaigns, categories and event IDs to compact integer handles. Use a map keyed by original byte contents; keep a stable byte-exact string table. Do not normalize identifiers. Avoid cloning strings for every event.
- Use integer timestamps, integer cents, compact record structs and per-row flags instead of strings/JSON intermediates. The fallback should handle unusually long identifiers safely.
- Profile hotspots before using unsafe code, SIMD, custom allocators or aggressive parallelism. Avoid allocating small strings inside time-critical comparisons; cache category/campaign/person handles.

### 3.2 Union-find / canonical IDs

Build the full DSU from parsed links before processing events and clicks. Maintain `parent` and rank/size arrays, path-compress on find and union by size. Track (or compute after union) each component's byte-wise smallest UTF-8 ID separately from the root index. After unions, map each observed ID to canonical person handle. Any ID absent from links is a singleton; preserve exact case. Complexity O((V+L) alpha(V)) expected, with hash interning. Tests must include chains length 400+, repeated reversed/self links, unused canonical minima, upper-case IDs, and shared-device mega-components.

### 3.3 Global event dedup before validation

Keep a hash index `event_id -> winning record index` or `event_id -> (earliest ingest, data)`. Compare ingest timestamps before expensive event validation. Replacements may change user, timestamp or whether the record is valid. Never optimize by deduping only valid events or per person/part file. Ingest ties are guaranteed identical. Drop malformed structural rows before considering dedup. If memory becomes tight at full scale, use sharded on-disk partitions by stable event-id hash and dedup per partition; never sacrifice **global** dedup semantics.

### 3.4 Sorting / sessionization

After validation and person resolution, sort compact events by `(canonical_person, ts, event_id)`; an additional deterministic tie key is fine but may not alter boundaries. Scan sequentially per person, starting a session on first event or when distinct successive timestamp gap >1_800_000. Aggregate counts/cents as you go. Emit compact session records with unique session index, person, start_ms/day, event count, purchases-present flag, revenue. Collect purchase-category totals for user/day from events mapped to the session start day, not event day. Sort costs O(E log E); use an efficient Rust parallel sort only when measurements justify overhead. Whale people are deliberately skewed; a global sort avoids per-user vector explosion. Avoid storing duplicate event contents once narrow records suffice.

### 3.5 Click attribution and assists: per-person two-pointer sweep

Avoid a naive range join between every session and every click: whale users/campaigns can cause huge intermediate outputs. Sort clicks by `(person, ts, click_id)`; sort sessions by `(person, start_ms)`. Process one person's sessions in increasing start order with an advancing pointer:
- Add all clicks with `click.ts <= start_ms` (including equal time) to the active window.
- Expire clicks with `click.ts < start_ms - 604_800_000`; the lower boundary is inclusive.
- Primary winner is the latest eligible click time, then byte-wise smallest click_id at that time. With clicks sorted by `(ts, click_id)`, the last timestamp's **first** click ID is the winner; do not just use the final click record if several share the time.
- Maintain `active_count[campaign]` over the current window and a compact collection of campaigns with positive counts. Each active campaign contributes **one** assist for the current session, no matter how many clicks belong to it. When count reaches zero, remove it. If maintaining a dense index plus swap-remove active vector, ensure the index map updates after swaps.
- One click may remain active for and contribute to multiple sessions; do not consume clicks after first credit. Campaigns are smallish (~2,000) but their frequency can be highly skewed.
- Add credited metrics and assists directly into `(campaign, day)` counters. For organic, credit metrics only. Avoid a materialized per-click-per-session join; complexity approximately O(C log C + S + C + A), where A is the number of distinct active campaign/session assist relationships that must be counted. A may be large in extreme cases; optimize active-campaign enumeration only after profiling.

**Potential stronger assist optimization:** for each person and session day, the output only needs per-campaign counts of assisted sessions. When a campaign becomes continuously active over a sequence of session starts, update a prefix/difference accumulator over session indices or day buckets instead of enumerating that campaign for every session. Campaign eligibility is the union of inclusive time intervals `[click.ts, click.ts + 7d]`; merge overlapping/adjacent intervals per person+campaign and count session starts falling inside them via binary searches on that person's sorted session starts, then aggregate counts by UTC day. This is an optional advanced alternative; benchmark its extra sorting and grouping costs before replacing the simple sweep. The primary latest-click credit still needs correct time and click_id tie semantics.

### 3.6 Aggregation and Parquet

- Accumulate user metrics keyed by `(canonical_person, UTC start_day)`. Accumulate category purchase totals on the same person/day, then choose max cents with byte-wise min category on ties. Keep category totals integer-only.
- Accumulate campaign metrics keyed by `(campaign_id, UTC start_day)`, including separate credited and assisted counters; preserve assist-only rows.
- Prefer `i64` or wider checked intermediates for sums and counts; avoid `usize`-to-small-int truncation. For output `day`, write Arrow/Parquet **Date32** (days since Unix epoch), not UTF-8 or Timestamp. Use pinned `arrow`/`parquet` crates or another maintained Rust writer supported offline; write schema-correct empty tables.
- Use bounded row groups, batch writing, and optional compression with favorable throughput (e.g. snappy/zstd after benchmarking). Avoid accumulating giant Arrow string arrays in memory. Ensure files close/flush fully before exit.

## 4. Test harness: reuse, adapt minimally, do not weaken

**Reviewed harness location:** `starter-duckdb/scripts/run_fixtures.py`, `starter-duckdb/scripts/compare.py`, and `starter-duckdb/scripts/docker_run.sh`; `starter-duckdb/Makefile` illustrates the expected workflow. Confirm live paths before running. The fixture runner enumerates case directories, uses `case/data` or `case/input`, sets `DATA_DIR` and `OUTPUT_DIR`, and **appends those two paths** to the invoked command. It reads `expected/user_daily.csv` and `expected/campaign_daily.csv`, loads actual Parquet via Polars, requires exact column sets and types (including physical DATE), and compares values independent of row order. The comparator also checks duplicate output keys and prints `MATCH`/ `DIFFER`. These are the correctness oracle. Its Python dependency is Polars; do not introduce DuckDB/Spark into the Rust production binary merely to run the harness.

**Fixture path caveat:** `starter-duckdb/fixtures/` exists in the current repo; the Makefile's fallback `../../fixtures` may not exist. Verify actual locations with `find`/ `ls`, and use an explicit path in your Rust Makefile. No need to copy 36 fixtures into Rust source. It is fine to pass the existing fixture path as an absolute path. Fixtures and dev-answer stay read-only.

### Native fast-feedback loop (from repository root)

```sh
cargo build --release --locked --manifest-path rust-pipeline/Cargo.toml
python3 -m pip install polars==2.1.0
python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- ./rust-pipeline/target/release/shopping-sessions
```

The expected name `shopping-sessions` must be set in `rust-pipeline/Cargo.toml` (or change commands to actual binary name). The harness sets path environment variables and appends path arguments; make the binary compatible with both. A native run should not write inside source trees except its explicit temp/build locations.

### Scorer-style Docker tests

Provide `rust-pipeline/Makefile` targets `build`, `fixtures`, `dev`, `run-dev`, `check-dev` with semantics compatible with the original Makefile. Set the image name and Docker platform explicitly, and ensure the shared `docker_run.sh` can run the Rust image via `IMAGE=... CPUS=8 MEM=24g PLATFORM=linux/amd64`; it mounts the inputs and outputs, sets the scorer env, enforces no network/read-only root/UID 1000, and cleans scratch. Make sure `rust-pipeline/Dockerfile` has the Rust binary as `CMD` and is buildable using `docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:latest rust-pipeline`.

Examples from repo root:

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:latest rust-pipeline
IMAGE=shopping-sessions-rust:latest python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- starter-duckdb/scripts/docker_run.sh
```

If `docker_run.sh` is not executable, invoke with `sh starter-duckdb/scripts/docker_run.sh` after `--`. Do not edit this shared script simply to bypass its resource or security limits.

### Public development oracle

```sh
cargo build --release --locked --manifest-path generator/Cargo.toml
generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output rust-pipeline/dev
# Native run (or run via shared docker_run.sh for scorer-parity):
DATA_DIR="$PWD/rust-pipeline/dev" OUTPUT_DIR="$PWD/rust-pipeline/out" SCRATCH_DIR="$(mktemp -d)" ./rust-pipeline/target/release/shopping-sessions
python3 starter-duckdb/scripts/compare.py rust-pipeline/out dev-answer
```

Prefer a scratch directory under `rust-pipeline/` or the host temporary area that exists and is writable, clean it after tests, and keep generated `dev/`, `out/`, scratch, build outputs and datasets out of Git via `.gitignore`/`.dockerignore`. The dev oracle is 1,000,000 events + 250,000 clicks and must compare with **MATCH**, zero duplicate keys. The comparator's expected `dev-answer/` is in repo root.

### Additional targeted and stress tests

- **CSV**: quoted empty versus empty unquoted; spaces, trailing CR, malformed quotes, wrong arity, blank line, header-only part, every part file; tiny and huge integers; ASCII versus Unicode digits.
- **Time**: leap day 2028, invalid Feb 29, F2 UTC conversion crossing midnight, F3 leading zeroes, F3 overflow, precise 7-day and 24-hour inclusivity, exactly 30:00.000 vs 30:00.001, timestamp equalities, click at session start and click 1ms after it.
- **Identity/dedup**: reversed/repeated/self links; disconnected upper-case IDs, unused smallest linked ID, long link chain, giant shared-device component; duplicate winner with bad timestamp and later valid copy; duplicate event referencing different person.
- **Attribution**: multiple clicks same millisecond across campaigns, smallest click_id tie; click outside 7d; several sessions per click; assist-only campaigns; many clicks from one campaign only count as one assist per session; organic sessions never assist.
- **Outputs**: cross-midnight sessions, unknown category, no-purchase null top category, equal category revenues by byte name, zero-event case, schema-correct DATE, unique keys.
- Run 36/36 fixtures *and* dev comparator. Add Rust unit tests (`cargo test --locked`) for parser, timestamp and DSU; add only **supplemental** fixtures to Rust-specific tests without changing provided expected fixtures.
- Stress multiple non-dev seeds and sizes including significant skew; validate invariants and, if available, exact-match against a separately verified DuckDB baseline. A matching checksum alone is not enough when row order varies; compare sorted keys/values.

## 5. Performance method and acceptance criteria

Measure pipeline phases individually in diagnostic mode: file read/parse, interning+links, event dedup, validation, sorting, sessionization, click processing, aggregation, Parquet write. Avoid printing per-row diagnostics on timed runs. Track wall time, peak RSS, CPU utilization, and scratch/output bytes; use `/usr/bin/time -v` when available and compare at equivalent NCPU/memory limits. Benchmark release build, not debug; compare warm/cold filesystem effects and use repeat runs. Never assume a faster 1M seed proves a faster final workload. The generator deliberately includes whale users (1–2% of events each), campaigns (~30% of clicks concentrated in a few), long link chains, and shared devices; stress these rather than optimizing only uniform data.

**Hard gates before claiming completion:**
1. `cargo fmt --check`, `cargo test --locked`, `cargo build --release --locked` pass for Rust crate (or document exact blocked checks).
2. Native **36/36** fixture comparisons pass; scorer-style Docker **36/36** pass on linux/amd64 under resource/security flags.
3. Public development dataset output matches `dev-answer/` exactly (`MATCH`; zero duplicate keys), both native and Docker if practical.
4. Schema/type checks pass including actual Parquet DATE, output dirs contain valid Parquet even if empty, no extra columns.
5. Invariants: total sessions and total revenue agree between output tables; sum assists >= sum sessions credited to non-organic campaigns; organic assists =0; no duplicate keys; assist-only rows retained.
6. Correct execution with `--network none`, read-only root, UID 1000, <=24 GB RAM and 8 vCPUs; no writes outside output/scratch (except permitted small /tmp), no runtime dependency downloads. Record image/repo sizes and assess final <=50 MB repo limit.
7. Final report includes exact commands run and their actual outcomes, implementation files touched, runtime/memory measurements (if obtained), and every unverified constraint. Never claim a test passed unless run.

## 6. Parallel agent integration rules

Follow root `AGENTS.md`. Work in an isolated branch/worktree. A useful division is: agent A owns `csv.rs` + `timestamp.rs`; B owns `ids.rs`; C owns `events.rs` + `sessions.rs`; D owns `clicks.rs` + `aggregate.rs`; one integrator owns `main.rs`, `Cargo.toml`/`Cargo.lock`, Dockerfile, Makefile and test integration. Agree on exact public Rust structs, ID handle type, person canonicalization ownership, timestamp units, and error types *before* parallel implementations. Avoid simultaneous edits to Cargo files, entrypoint or shared test scripts. When dependencies must change, ask integrator to make the minimal update or submit the proposed change separately. Do not broad-format other agents' work; preserve uncommitted changes, never force-push/reset/clean without authorization. Integrator merges incrementally and reruns full suite after changes.

### Suggested sequencing

1. Scaffold Rust crate, Docker packaging, harness-compatible CLI and empty schema-aware writer; verify the image launches (this is only a smoke test, **not** a passing solution).
2. Implement strict parser/timestamps and DSU with unit tests.
3. Implement global event and click dedup/validation.
4. Sessionization + user_daily and assertions.
5. Last-click attribution + assisted campaign enumeration + campaign_daily.
6. Pass all fixtures and dev exact oracle.
7. Benchmark, optimize hot spots (especially sort/attribution/allocations), re-run every correctness check.
8. Publish or explicitly integrate a **root-level Dockerfile** for submission.

**Do not stop at a partial pipeline without stating what remains; prioritize correctness over micro-optimizations.**
