# Shopping Sessions testing harness

The current `starter-duckdb/` entry point is the Rust–DuckDB hybrid; build its binary and export `HYBRID_BIN` for native runs. The verified original baseline remains available at revision `fbcef8c` for comparisons. The harness command contract is identical for both implementations.

The harness runs any implementation as an external command. There are no implementation imports, language bindings, or engine dependencies: Python, Rust, C++, JVM programs, shell launchers, and containers use the same input/output contract from `../rules.md`.

The harness needs Python 3.10+ on Linux/macOS and Polars for Parquet validation. The tested dependency is pinned separately from implementation dependencies:

```sh
python3 -m pip install -r testing/requirements.txt
python3 -m unittest discover -s testing/tests -v
```

Run these commands from the repository root. Installing DuckDB is necessary only for the DuckDB implementation.

## Run fixtures and benchmark the hybrid

Generate the public seed-1 dataset if it is not already available. `dev-answer/` contains its published answer:

```sh
cd generator
cargo build --release
cd ..
generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output datasets/dev
python3 -m pip install -r starter-duckdb/requirements.txt
cargo build --release --locked --manifest-path starter-duckdb/rust/Cargo.toml
export HYBRID_BIN="$PWD/starter-duckdb/rust/target/release/shopping-hybrid"
python3 testing/harness.py \
  --name rust-duckdb-hybrid \
  --benchmark dev-1m datasets/dev dev-answer \
  --repetitions 5 --warmups 1 --profile-runs 2 \
  --cpus 4 --mem-gb 8 \
  -- python3 starter-duckdb/pipeline/pipeline.py
```

By default the harness runs all 36 hand-written fixtures, then one warmup, three measured runs, and one separate profiling run per benchmark. Every invocation, including warmups and profiles, is checked against the expected tables. Use `--skip-fixtures` for a benchmark-only run, `--profile-runs 0` to disable profiling, and repeat `--benchmark NAME INPUT EXPECTED` to add datasets. An expected folder can contain `user_daily.csv` and `campaign_daily.csv`, or the two Parquet output subfolders.

Each run gets fresh output and scratch directories. Previous outputs cannot make a failed invocation appear correct. Runs execute serially. Scratch is removed after each invocation; outputs, logs, validation failures, and checkpoints are retained. The harness stops a timed-out process group, allowing a short cleanup grace period, and continues to other runs. Exit codes are 0 for success, 1 for implementation failures, 2 for harness/configuration errors, and 130 for interruption.

## Plug in another implementation

Implementations receive these environment variables:

| Variable | Contract |
| --- | --- |
| `DATA_DIR` | Absolute input directory, including `manifest.json` |
| `OUTPUT_DIR` | Empty output directory; write `user_daily/*.parquet` and `campaign_daily/*.parquet` |
| `SCRATCH_DIR` | Per-invocation writable directory for spills and temporary files |
| `NCPU`, `MEM_GB` | Requested resource hints; the command runner does not enforce limits |
| `CPUS`, `MEM` | Equivalent hints for existing container wrappers |
| `CHECKPOINTS_ENABLED` | `0` for fixtures, warmups, and measured runs; `1` only for profiling |
| `CHECKPOINTS_FILE` | Set **only** in profiling runs; optional UTF-8 JSONL output file |

The command following `--` is an argument vector, not a shell expression. Use an explicit shell launcher if necessary. Commands with positional input/output arguments can use literal `{data}`, `{output}`, and `{scratch}` placeholders; these are replaced inside individual arguments without shell evaluation. Paths with spaces remain a single argument.

```sh
# Compiled binary reading environment variables:
python3 testing/harness.py --name rust-baseline -- ./target/release/shopping-sessions

# Implementation requiring positional arguments:
python3 testing/harness.py --name cpp-baseline -- ./build/pipeline '{data}' '{output}'

# JVM program:
python3 testing/harness.py --name jvm-baseline -- java -jar pipeline.jar
```

The command defaults to the repository root as its working directory; override it with `--cwd`. Set `--timeout SECONDS` per invocation (default 120).

The generic container adapter enforces CPU/RAM limits, removes network access, mounts inputs read-only, uses a read-only root filesystem, and mounts output/scratch. It forwards optional checkpoints through a separate profiling mount. It runs as the host UID/GID, which is UID 1000 in this workspace. The image must be readable by that user and have a working default command, or accept a command after its image name.

```sh
python3 testing/harness.py \
  --name container-baseline --resource-mode wall-only \
  --benchmark dev-1m datasets/dev dev-answer --cpus 4 --mem-gb 8 \
  -- bash testing/docker_adapter.sh shopping-sessions:hybrid
```

Use wall-only mode with Docker or remote launchers: host CLI CPU/RSS would not represent the implementation. Container wall times include container startup/cleanup and must be compared to other implementations using the same execution mode. The container adapter requires Docker on the local host; force-killing an arbitrary remote launcher cannot reliably clean up work in another daemon or machine.

Only in profiling runs, the Docker adapter also writes sampled `docker stats` output to `profiling/container-stats.jsonl`. This is diagnostic container memory/CPU information, not an exact memory high-water mark; short peaks may be missed. No monitor runs in warmups, fixtures, or official measured samples.

## Optional checkpoints without contaminating benchmark times

**Checkpoint collection never runs in the official timing samples.** The harness explicitly removes an inherited `CHECKPOINTS_FILE` and sets `CHECKPOINTS_ENABLED=0` during fixtures, warmups, and measurements. Only additional profiling invocations receive a destination file. Profiling durations never contribute to median, P95, throughput, or official CPU/RSS summaries. Checkpoints are optional; implementations without them are tested normally.

Instrumentation has overhead. Implementations must use an uninstrumented execution path when checkpoints are disabled, rather than recording and throwing events away. The hybrid executes each normal SQL stage group without checkpoint callbacks on this path; splitting those groups at diagnostic markers and writing JSONL happen only in profiling runs. For compiled programs needing zero instrumentation in their measured binary, supply a separate instrumented executable:

```sh
python3 testing/harness.py \
  --name native --benchmark dev-1m datasets/dev dev-answer \
  --profile-command './build/instrumented-pipeline {data} {output}' \
  -- ./build/release-pipeline '{data}' '{output}'
```

`--profile-command` is shell-quoted argv parsed without invoking a shell. Both binaries must produce the same correct outputs; profiles are validated too. Disabled code paths can still have branch overhead, so a separately compiled binary is the strongest option when comparing very small runtime differences.

Write one JSON object per line. A checkpoint names the stage just completed; `elapsed_seconds` uses a monotonic clock relative to implementation startup. Names need not match across implementations. Durations are differences between consecutive valid checkpoints, starting at zero. Repeated names are summed within each profiling invocation before computing stage medians.

```json
{"name":"parse_inputs","elapsed_seconds":2.51,"metrics":{"events":1000000}}
{"name":"sessionize","elapsed_seconds":3.12}
{"name":"write_outputs","elapsed_seconds":3.80}
```

For example, in Python:

```python
import json, os, time
started = time.perf_counter()
path = os.environ.get("CHECKPOINTS_FILE") if os.environ.get("CHECKPOINTS_ENABLED") == "1" else None

# Perform the stage, then emit only on the profiling path:
if path:
    with open(path, "a") as stream:
        stream.write(json.dumps({"name": "parse_inputs", "elapsed_seconds": time.perf_counter() - started}) + "\n")
```

Any language can emit that JSONL format. Serialize writers when several threads share a file. Elapsed times must be finite, nonnegative, and nondecreasing. Optional `metrics` are retained as diagnostic context. Missing checkpoints are allowed; malformed events generate warnings without changing output correctness or official performance statistics. Stage timings include instrumentation overhead and the first stage excludes runtime startup before the implementation starts its clock. They diagnose bottlenecks; they do not rank implementations.

## Artifacts and metrics

`--artifacts DIRECTORY` defaults to repository-root `artifacts/`. Every invocation of the harness creates a unique directory, so reports never overwrite another implementation's results:

```text
artifacts/<name>-<UTC timestamp>-<unique suffix>/
  report.md                       human-readable results and stage breakdown
  results.json                    versioned full report, metadata, every run
  results.csv                     one row per invocation, with its phase
  E1/fixture-001/
    invocation.json               expanded argv and checkpoint mode
    stdout.log
    stderr.log
    validation.json               schemas, counts, differences, invariants
    output/user_daily/*.parquet
    output/campaign_daily/*.parquet
  dev-1m/measurement-001/...
  dev-1m/profile-001/
    profiling/checkpoints.jsonl
    ...
```

Reports are refreshed after each invocation and marked complete only after the whole suite finishes. JSON includes platform, Python/Polars versions, Git SHA/dirty state, actual argv, dataset/reference SHA-256 fingerprints, input byte sizes, and physical data-line counts. Artifact directories and generated datasets are Git-ignored.

Correctness requires exact schema and values, unique logical keys, valid nullability, and matching row counts. Only visible regular `*.parquet` files directly inside each table folder count; column order, row order, integer widths, string encodings, and file count may vary. Additional checks compare session/revenue totals across tables and validate count/category/organic-assist invariants. Differences include a small sample of missing and extra rows.

Only correct **measurement** runs enter timing statistics; counts of successful and attempted runs make failures visible. Median, interpolated P95, min/max, and input event lines/second use wall time from process launch through exit, including initialization and output writes. Validation and input hashing occur outside that interval. Physical input event lines include dirt/duplicates, so that throughput is not a surviving-event count. The harness does not flush OS caches; warmups and repeated runs describe warm-cache local performance.

In process resource mode, `wait4` collects CPU usage and maximum RSS separately per invocation. RSS is the largest individual process high-water mark, including waited-for children, **not summed process-tree or cgroup memory**. CPU totals include waited-for descendants; detached processes are outside these metrics. For container/remote launchers use `--resource-mode wall-only`, which reports CPU/RSS as unavailable. The harness itself is not included in implementation resource usage. Resource limits are enforced only by an adapter/launcher, such as the provided Docker adapter.
