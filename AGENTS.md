# Codex Agent Instructions — Shopping Sessions

This file applies to the entire repository. Read `rules.md` before changing any implementation. The challenge specification is authoritative; starter README files and code are only scaffolds.

## Goal and repository layout

Build a correct, performant, reproducible **linux/amd64** Docker pipeline that reads the read-only input under `/data` and writes exactly:
- `/output/user_daily/*.parquet`
- `/output/campaign_daily/*.parquet`

The output schema, nullability, `day` Parquet DATE type, time boundaries, CSV grammar, and operation ordering must match `rules.md` precisely. There must be no network access at runtime. Build time is not scored, runtime is.

The repo contains independent `starter-duckdb/`, `starter-polars/`, and `starter-pyspark/` skeletons; `generator/` (Rust); `dev-answer/` (expected 1M-event development answers); and `rules.md`. The starters currently output empty tables and are **not** correct solutions. Prefer working inside one explicitly assigned starter/dedicated solution directory rather than modifying all three.

## First steps for every task

1. Read `rules.md` and the relevant starter's README and pipeline. Check `git status --short`, current branch, recent commits, and existing changes before editing. Never discard work you did not create.
2. Identify the requested deliverable and **smallest owned set of files**. Restrict edits to that area. Check whether another agent or branch has modified these files before integrating.
3. State any assumptions or limitations in the final task report. Do not silently change documented semantics to improve speed.

## Correctness invariants

- Parse only the specified CSV dialect, retaining spaces and case exactly. `\n` alone ends lines; `\r` is data. Reject malformed lines and invalid timestamp formats; ASCII digits mean `[0-9]`, not Unicode `\d`. Read **all** matching CSV part files, including empty-header-only files.
- Resolve linked user IDs transitively *before* person-level processing. Canonical person ID is the byte-wise smallest ID in the component, including linked IDs that never occur in events/clicks. A DSU/union-find implementation is acceptable.
- Events: parse -> deduplicate on `event_id` by earliest `ingest_ts` -> validate the retained row -> reject lateness >24h -> sessionize by canonical person with gap **strictly greater than** 30 min. Deduplication must happen before validation. The timestamp of the session's first event determines its UTC day; events after midnight stay in that session/day.
- Clicks: parse -> deduplicate -> validate -> attribute each session to the latest eligible click within the inclusive prior seven days, breaking equal timestamps by byte-wise smallest `click_id`. A click after the session starts is ineligible. No qualifying click means `organic`. Assist each session once per distinct qualifying campaign, including the credited one; `organic` never assists.
- Preserve exact input strings; no trimming, case-folding, or unsafe timestamp coercion. Purchase price must be ASCII digits and within 1..10,000,000 inclusive. Unknown nonempty products get category `unknown`.
- Keep numerical sums sufficiently wide; no overflow, accidental floating-point price math, nondeterministic tie breaking, or generated test data hard-coding.

## Dependencies and Codex Cloud setup

- A Codex Cloud checkout may be ephemeral. **Do not assume** packages installed in one task persist to later tasks or to the scorer. Commit reproducible dependency declarations (`requirements.txt`, lockfiles, `Cargo.lock`, Dockerfile, etc.) with the code that needs them.
- Prefer dependencies already present in the chosen starter. Add a package only if necessary and justify it; pin compatible versions (or use an appropriate lockfile). Update the **chosen solution's** dependency file and Dockerfile together. Do not casually modify unrelated starters or shared configs.
- When adding a Python dependency, install it in the task environment for testing (for example, `python3 -m pip install -r requirements.txt` in the chosen starter); ensure the Docker build installs it too. For Rust, update `Cargo.toml` and commit `Cargo.lock`; verify a release build. Do not depend on manual global installations or untracked files.
- Dependencies may be fetched during development/image **build** if allowed by the environment, but the submitted container must work with `--network none` at **runtime**. No runtime package installs, extension downloads, API calls, or remote datasets. Use `/scratch` for temporary files; root filesystem is read-only except a tiny `/tmp`.
- If package installation, Docker, or network access is unavailable in Codex Cloud, first try the already-installed tools and supported offline workflow. Do not silently skip checks or claim a successful build. Report the exact blocked command, error, and what remains unverified. Do not replace dependencies or rewrite build files solely to work around a transient cloud restriction without approval.
- Do not commit virtual environments, downloaded datasets, generated Parquet outputs, caches, build artifacts, secrets, or machine-specific configuration.

## Required testing workflow

Run the smallest meaningful tests first, then the whole selected solution's available suite. The commands below are run **from the chosen starter directory** (e.g. `cd starter-duckdb`); run `make help` or inspect the Makefile if targets differ.

1. For rapid iteration, run edge fixtures without Docker if dependencies are available:
   - DuckDB: `python3 -m pip install duckdb==1.5.6 polars` then `python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py`
   - Polars: `python3 -m pip install polars==2.1.0` then the same fixture-runner command.
   - PySpark: `python3 -m pip install pyspark==4.0.1 polars` (Java 17+ required), then the same fixture-runner command.
   - If fixtures are stored outside the starter in this monorepo, resolve their actual path (e.g. `../fixtures` or the appropriate starter's `fixtures`); do not assume the README's standalone layout exists.
2. Run `make build` to build the scorer-compatible `linux/amd64` container, then `make fixtures` for all **36** edge-case fixtures, adapting only the fixture path/layout when necessary.
3. Build the generator once if needed: `cd generator && cargo build --release` (Rust 1.85+). Make `target/release/generate` available on `PATH` for `make dev`, or invoke it directly. Generate the specified seeded dev input (`--seed 1 --events 1000000 --clicks 250000`) and run `make run-dev` / `make check-dev` against `dev-answer/` using the correct actual paths.
4. Verify exact output schemas and Parquet DATE type, and the invariants: total sessions and revenue agree between user_daily and campaign_daily; `assisted_sessions` is at least non-organic credited sessions in aggregate. These are necessary, **not sufficient**, checks.
5. For performance changes, first establish a correct baseline. Repeat equivalent seeded workloads, record wall-clock/runtime and peak memory where possible, compare exact outputs before/after, and avoid tuning exclusively to seed 1. Investigate skewed whale users/campaigns and larger data sizes while respecting 8 vCPUs/24 GB.
6. In the final report list commands **actually run**, results (fixture count, dev diff, performance if measured), and any skipped tests with reasons. Never invent passing tests.

If the cloud environment lacks Docker, run the non-Docker fixture tests and report Docker/scorer verification as pending; do not claim end-to-end container compatibility.

## Multi-agent collaboration / conflict avoidance

- **One task, one branch or isolated Codex worktree.** Do not develop directly on a shared branch when parallel agents are active. Use a focused feature branch/PR and small, comprehensible commits.
- **Respect file ownership.** An agent assigned parsing should not rewrite sessionization; an agent implementing attribution should avoid editing generic configuration. Coordinate a shared interface contract before separately implementing producers/consumers.
- Avoid unnecessary edits to high-contention files: `rules.md`, this `AGENTS.md`, root configuration, Dockerfiles, lockfiles, shared Makefiles, generated fixture expectations, and common pipeline entrypoints. Change them only when required by your task, and note the change prominently.
- Do not run broad auto-formatters or dependency upgrades across the repository; format only files you changed. Avoid unrelated refactors, renames, directory reshuffles, or committing bulk generated data.
- Treat `rules.md`, fixture inputs/expected answers, and `dev-answer/` as **read-only test oracle**. Never modify them to make a failing implementation pass.
- Before committing, run `git diff --check`, inspect `git diff --stat` and `git diff`, and ensure only task-related files are staged. Never use `git add -A` without inspecting status. Do not reset, clean, revert, force push, or overwrite another agent's changes.
- Before integrating, fetch/review the current target branch and overlapping PRs. Rebase/merge carefully only your own work; resolve conflicts deliberately, preserve unrelated changes, and rerun relevant tests after integration. Do not auto-resolve semantic conflicts by simply taking ours/theirs.
- If another agent owns a file you need, document the requested interface or patch, coordinate through the task/PR description, and avoid editing that file in parallel unless explicitly authorized.
- Prefer separate modules and tests with stable interfaces over concurrent edits of a large `pipeline.py`. Keep public entrypoints stable unless explicitly coordinating their change.

## Delivery checklist

- Implementation matches `rules.md` and works across arbitrary seeded windows and part counts.
- Runtime container uses `linux/amd64`, requires no network, reads `/data`, and writes required Parquet tables to `/output`.
- Dependencies and build steps are reproducible from tracked files.
- Fixtures and dev-answer comparison pass, or limitations are disclosed exactly.
- Diff is scoped to the assigned task; no unrelated churn or other agents' work overwritten.
- Final message summarizes files changed, validation performed, remaining risks, and any coordination needed.
