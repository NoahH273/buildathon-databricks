# Validation and delivery record

This records the initial implementation at `819a52e`. For the later detailed-checkpoint changes, current image ID, and new measurements, see `EVENT_PROFILE.md`.

The implementation, lockfile, tests and packaging are confined to `rust-pipeline/`. No shared harness, generator source, fixture, expected answer, starter implementation, or root configuration was changed. Branch `rust-pipeline` starts at `fbcef8c` (current `origin/main` when fetched).

## Results

- `cargo fmt --check`, `cargo test --locked` (4 unit + 2 supplemental black-box tests), `cargo build --release --locked`, and `cargo clippy --locked --all-targets -- -D warnings`: **passed** with Rust 1.90.0.
- Unchanged `starter-duckdb/scripts/run_fixtures.py`: **36/36 native**, **36/36 Docker**. The original fixture directory was used for both passing runs.
- Seed 1, 1,000,000 events / 250,000 clicks: native and Docker each **MATCH** `dev-answer/`; user rows **117,038**, campaign rows **27,400**; zero missing/extra rows, duplicate keys or unexpected nulls.
- The independent generic harness also passed native fixtures, every warmup/measurement/profile, DATE/schema checks and all seven aggregate invariants. Its Docker benchmark run intentionally skipped fixtures because the scorer wrapper had already passed all 36.
- Seed 2, 2,000,000 events / 500,000 clicks, and seed 42, 100,000 events / 25,000 clicks: every native run exactly matched the unchanged DuckDB baseline. Seed 2 also matched in every Docker run. Before using that baseline as a reference, it passed all 36 fixtures and the seed-1 development oracle through the generic harness.
- Supplemental tests cover strict CSV/UTF-8, strict calendar/offset/ASCII decimal parsing, long/reversed/self/shared-device links and unused minima, sweep-versus-brute-force attribution, empty schema-bearing outputs and required field nullability, global dedup across users with an invalid earliest winner, cross-midnight leap-day sessions, category ties, click-ID ties, and assist-only rows.
- `make dev DEV=bench-make-dev` succeeded with its isolated generator target directory. All generated files were byte-identical to the separately generated seed-1 input used for validation.

Development invariants from the Docker output: sessions **179,366** in both tables; revenue **376,461,330 cents** in both; non-organic credited sessions **109,923**; assists **1,932,099**; organic assists **0**; assist-only campaign/day rows **12,886**. Polars reads `day` as `Date` in both tables. The empty-output Rust test also checks DATE32 and required/optional schema fields.

## Performance

Release binary; existing `testing/harness.py`; one warmup, three measured runs and one separate profile per dataset. Every invocation was validated. Warmups, profiles and validation are excluded from the following medians. Native CPU use was approximately one full core (median 98.96–99.88%). These are local warm-cache measurements, not scorer results. Host: Linux x86_64, five visible CPUs, about 17 GiB physical RAM. Native NCPU=8/MEM_GB=24 are hints; the binary is serial. Container measurements use enforced five-CPU/24-GB limits.

| Dataset | Native median | Native min–max | Native peak process RSS | Docker median | Docker min–max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Seed 1, 1M / 250K | 2.5327 s | 2.2890–2.6725 s | 246.9 MiB | 3.6402 s | 3.5281–3.7729 s |
| Seed 2, 2M / 500K | 5.5215 s | 5.3789–5.6576 s | 487.7 MiB | 6.6289 s | 6.1492–6.8192 s |
| Seed 42, 100K / 25K | 0.1846 s | 0.1692–0.2919 s | 255.5 MiB | — | — |

Native RSS uses the harness's wait4 high-water mark, including inherited/forked process accounting; it is not summed tree or cgroup memory. Docker uses wall-only mode; its client RSS/CPU are deliberately not presented as container usage. Docker durations include launcher/startup/cleanup. Build pulls and normal development activity occurred during the exploratory native measurement session, so small timing differences should not be interpreted as optimization claims. No runtime tuning was performed after the passing correctness baseline.

A separate seed-1 native profile measured: parse/dedup 1.2100 s; winner validation/identity lookup 0.6367 s; session/user aggregation 0.3425 s; click parse/dedup/validation 0.1919 s; Parquet 0.0689 s; attribution/assists 0.0421 s; event sort 0.0390 s; links 0.0223 s; click sort 0.0138 s; catalogs 0.0110 s. Profiles have overhead and are excluded from measured samples. The pipeline uses zero scratch spill bytes. Typical output sizes were 0.96 MB (seed 1), 1.85 MB (seed 2), and 0.14 MB (seed 42).

Full local reports, fingerprints, logs, exact validation records and phase checkpoints are retained in ignored directories:

- `bench-artifacts/rust-native-20261121T035738136590Z-96ae82/`
- `bench-artifacts/rust-docker-20261121T040300525393Z-57c664/`
- `bench-artifacts/duckdb-reference-20261121T035622816304Z-c366b9/`

## Image

- Tag: `shopping-sessions-rust:latest`
- ID: `sha256:ef71917fb78c39bd7c2e5c1bfb8357d1909c4fdeacb502ec31ac1926a881369c`
- Platform: **linux/amd64**; image user **1000:1000**; zero-argument CMD `/usr/local/bin/shopping-sessions`.
- Image size: **81,763,746 bytes** (78.0 MiB), below 8 GB.
- Docker-save archive: `shopping-sessions-rust-linux-amd64.tar`, **84,860,416 bytes**; SHA-256 `ffc8aacd7ee532bab101726935bdb64a6039bd96f5f31d548f63ca99b7c144de`. The archive manifest/config were read back and match the tested image ID, platform, user and command. Load with `docker load -i shopping-sessions-rust-linux-amd64.tar`. Archive and generated data are not committed.
- The shared scorer wrapper passed with `--network none`, read-only root and input, 64-MB `/tmp`, UID 1000, all capabilities dropped, no-new-privileges, 4096-PID ceiling, no swap, five-CPU quota and 24-GB memory ceiling. Runtime dependencies are entirely in the image.
- Tracked repository content before these additions was 4.83 MB; this solution adds less than 0.1 MB, below the 50-MB repository limit. Image/archive/datasets/targets are ignored.

## Commands actually run

Commands below are from repository root unless noted. This environment lacked a native Rust toolchain and Polars initially; Rust 1.90.0/rustfmt/clippy were installed under `/tmp/rust-pipeline-{cargo,rustup}`, and Python validation packages under `/tmp/rust-pipeline-python`. Those locations are development-only. The production build installs everything through Cargo.lock in its builder image.

```sh
# Environment used for the native Cargo commands:
export PATH=/tmp/rust-pipeline-cargo/bin:$PATH
export CARGO_HOME=/tmp/rust-pipeline-cargo
export RUSTUP_HOME=/tmp/rust-pipeline-rustup
export PYTHONPATH=/tmp/rust-pipeline-python

cargo fmt --check --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --manifest-path rust-pipeline/Cargo.toml
cargo build --release --locked --manifest-path rust-pipeline/Cargo.toml
cargo clippy --locked --manifest-path rust-pipeline/Cargo.toml --all-targets -- -D warnings
python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- ./rust-pipeline/target/release/shopping-sessions

# Generator initially built via its standard Cargo command; generated target was then
# moved into rust-pipeline/target/generator to keep the final workspace scope isolated.
cargo build --release --locked --manifest-path generator/Cargo.toml
rust-pipeline/target/generator/release/generate --seed 1 --events 1000000 --clicks 250000 --output rust-pipeline/dev --threads 8
rust-pipeline/target/generator/release/generate --seed 2 --events 2000000 --clicks 500000 --output rust-pipeline/bench-seed2 --threads 8
rust-pipeline/target/generator/release/generate --seed 42 --events 100000 --clicks 25000 --output rust-pipeline/bench-seed42 --threads 8
make -C rust-pipeline dev DEV=bench-make-dev

DATA_DIR="$PWD/rust-pipeline/dev" OUTPUT_DIR="$PWD/rust-pipeline/out-native" DIAGNOSTICS=1 ./rust-pipeline/target/release/shopping-sessions
python3 starter-duckdb/scripts/compare.py rust-pipeline/out-native dev-answer

# Reference validation, then the extra-seed reference outputs:
python3 testing/harness.py --name duckdb-reference --benchmark dev-1m rust-pipeline/dev dev-answer --repetitions 1 --warmups 0 --profile-runs 0 --cpus 8 --mem-gb 24 --artifacts rust-pipeline/bench-artifacts -- python3 starter-duckdb/pipeline/pipeline.py
DATA_DIR="$PWD/rust-pipeline/bench-seed2" OUTPUT_DIR="$PWD/rust-pipeline/bench-answer2" SCRATCH_DIR="$PWD/rust-pipeline/scratch" NCPU=4 MEM_GB=8 python3 starter-duckdb/pipeline/pipeline.py
DATA_DIR="$PWD/rust-pipeline/bench-seed42" OUTPUT_DIR="$PWD/rust-pipeline/bench-answer42" SCRATCH_DIR="$PWD/rust-pipeline/scratch" NCPU=4 MEM_GB=8 python3 starter-duckdb/pipeline/pipeline.py

python3 testing/harness.py --name rust-native --benchmark dev-1m rust-pipeline/dev dev-answer --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 --benchmark seed42-100k rust-pipeline/bench-seed42 rust-pipeline/bench-answer42 --warmups 1 --repetitions 3 --profile-runs 1 --cpus 8 --mem-gb 24 --artifacts rust-pipeline/bench-artifacts -- ./rust-pipeline/target/release/shopping-sessions

IMAGE=shopping-sessions-rust:latest CPUS=5 MEM=24g PLATFORM=linux/amd64 python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- sh starter-duckdb/scripts/docker_run.sh
IMAGE=shopping-sessions-rust:latest CPUS=5 MEM=24g PLATFORM=linux/amd64 sh starter-duckdb/scripts/docker_run.sh rust-pipeline/dev rust-pipeline/out-docker
python3 starter-duckdb/scripts/compare.py rust-pipeline/out-docker dev-answer
python3 testing/harness.py --name rust-docker --skip-fixtures --resource-mode wall-only --benchmark dev-1m rust-pipeline/dev dev-answer --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 --warmups 1 --repetitions 3 --profile-runs 1 --cpus 5 --mem-gb 24 --artifacts rust-pipeline/bench-artifacts -- bash testing/docker_adapter.sh shopping-sessions-rust:latest

docker image inspect shopping-sessions-rust:latest
docker save shopping-sessions-rust:latest -o rust-pipeline/shopping-sessions-rust-linux-amd64.tar
sha256sum rust-pipeline/shopping-sessions-rust-linux-amd64.tar
```

## Environment failures and their resolution

The normal command `docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:latest rust-pipeline` failed at Cargo's crates.io request with `SSL certificate problem: self-signed certificate in certificate chain`. A retry with empty HTTP/HTTPS/ALL_PROXY build arguments failed DNS resolution. The Cloud runtime skill's documented session-only build setup resolved this while preserving the tracked Dockerfile, build context, ignore rules, dependencies and TLS verification:

```sh
python3 - <<'PY'
from pathlib import Path
source = Path('rust-pipeline/Dockerfile').read_text()
source = source.replace('RUN cargo build --release --locked',
    'RUN --mount=type=secret,id=proxy_ca,required=true CARGO_HTTP_CAINFO=/run/secrets/proxy_ca cargo build --release --locked')
Path('/tmp/rust-pipeline-session.Dockerfile').write_text(source)
PY
docker buildx build --platform linux/amd64 --load \
  -f /tmp/rust-pipeline-session.Dockerfile \
  --secret id=proxy_ca,src=/etc/ssl/certs/ca-certificates.crt \
  -t shopping-sessions-rust:latest rust-pipeline
```

That build succeeded: locked release compilation took 79 seconds, the Cargo build step 85.8 seconds, with the base layers already pulled. Initial base-image acquisition took approximately 155 seconds. The CA secret is mounted only during compilation and does not enter runtime layers. Plain builds in this particular proxy environment still require the documented temporary build setup; a build in a standard public-CA environment was not separately available.

The first Docker fixture attempt with `CPUS=8` failed before container startup: `range of CPUs is from 0.01 to 5.00, as there are only 5 CPUs available`. Re-running the unchanged wrapper with `CPUS=5` passed 36/36. An eight-CPU scorer host was unavailable; resource/security correctness is verified under the stricter five-CPU ceiling.

`/usr/bin/time -v` was unavailable (`No such file or directory`), so resource measurements use the existing wait4-based harness. An initial direct DuckDB fixture invocation omitted a writable SCRATCH_DIR and failed at `/scratch`; the generic harness supplied isolated scratch and subsequently passed every fixture and the oracle. Neither failure required changing the Rust pipeline or shared files.

## Remaining scope and limits

No correctness failures remain in the tested datasets. The implementation uses global in-memory dedup and sorting, with no sharded disk fallback. Full-scale scorer inputs and adversarial memory exhaustion were not available; very large datasets can exceed 24 GB. The largest tested input has 2M requested events and 500K clicks, includes the generator's skew/shared devices, and used under 0.5 GiB measured native RSS. Eight-core scorer timing is unverified.

Competition submission still requires publishing this directory as a repository root, or explicitly coordinating a root-level Dockerfile for the monorepo, as described in `README.md` and the specification. This task delivers the isolated branch and tested image; it does not submit to the competition or create a separate public repository.
