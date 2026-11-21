# Submission packaging and checkpoint exclusion

The repository-root `Dockerfile` now builds the Rust pipeline directly from the public monorepo. Submit the validated commit from **`rust-pipeline`** explicitly; `main` is deliberately unchanged per the final instruction. The root `.dockerignore` allows only the Rust Cargo manifests and `src/`, and root `README.md` documents the submission command. Other starters, shared tests, generator source and expected answers are unchanged.

## Compile-time guarantee

`Cargo.toml` now declares `default = []` and an opt-in `profiling` feature. Both the root and dedicated Dockerfiles run:

```sh
cargo build --release --locked --no-default-features
```

With profiling excluded:

- `timing.rs` is a zero-state no-op implementation. Checkpoint environment reads, JSONL file creation, phase clocks and diagnostic logs are excluded by conditional compilation.
- `profile.rs` has no duration/counter fields, clock reads or metric serialization. Its operation wrapper inlines the operation directly.
- The detailed-events selector is a compile-time false result, so release optimization removes the detailed scan path. Completion output contains counts only; even the final elapsed-time clock/log is excluded.
- Runtime `CHECKPOINTS_ENABLED`, `CHECKPOINTS_FILE`, `DIAGNOSTICS` and `EVENT_PROFILE_DETAIL` cannot activate any profiling. This also remains true if Cargo defaults are changed later, because Docker explicitly disables default features.

Native development profiling is retained only through an explicit `--features profiling` build. `make -C rust-pipeline profile-build` uses a separate target directory, keeping the production release binary intact.

## Validation actually performed

- Rust 1.90.0: `cargo fmt --check`, locked release compilation, and Clippy with warnings denied passed. Tests passed **7/7 without profiling** and **7/7 with the explicit profiling feature**.
- A production-only regression runs the binary with all profiling flags set to 1 and tries both writable and invalid checkpoint destinations. Both invocations produce the expected schema-bearing empty outputs, with no checkpoint file, phase logs or elapsed log. The feature-enabled regression still validates detailed checkpoints.
- The unchanged native fixture runner passed **36/36**, with all profiling flags forced on and a writable probe path. The probe file was never created.
- The root-context Linux AMD64 image passed the unchanged generic harness's **36/36 fixtures**, followed by one warmup and three measured runs each on seed 1 (1M events / 250K clicks) and seed 2 (2M / 500K). All **44 container invocations** had the profiling flags forcibly enabled inside the container command. Every invocation passed exact output, DATE/schema, nullability, duplicate-key and conservation validation. None created the checkpoint probe or printed phase/elapsed timing logs.
- Seed 1 matched `dev-answer/` in all invocations: **117,038 user rows**, **27,400 campaign rows**, zero differences/duplicate keys. Seed 2 matched the independently verified DuckDB reference from previous validation.
- The root image's default zero-argument CMD also passed a direct scorer-wrapper development run. The unchanged comparator printed **MATCH** for both tables with zero differences and duplicate keys.
- The actual release executable copied from the root image contains none of the checkpoint environment-variable names, `phase_detail`, or `detailed_event_profile` marker strings. This corroborates the compile-time exclusion and runtime probe tests.

The Docker adapter enforces no network, read-only root/input, a 64-MB `/tmp`, UID/GID 1000, dropped capabilities, no-new-privileges, a 4096-PID ceiling, a five-CPU quota and a 24-GB memory/no-swap limit. This host exposes five CPUs, so eight-CPU scorer measurements remain unavailable. The pipeline algorithms and checked arithmetic are unchanged.

## Performance and sizes

Three measured root-image runs per dataset, with one warmup and no profiling runs:

| Dataset | Median wall time | Min–max |
| --- | ---: | ---: |
| Seed 1, 1M events / 250K clicks | 4.2987 s | 3.0526–5.9984 s |
| Seed 2, 2M events / 500K clicks | 6.4585 s | 6.1874–6.5017 s |

Wall times include Docker startup/cleanup and vary on this host; they are not scorer times or a controlled speedup claim. Validation is outside the measured interval. The image has no checkpoint work regardless of the forced flags. Container RSS/CPU are not inferred from Docker client measurements.

The root build transferred **63.22 kB** of context, excluding all generated datasets, build targets, Git metadata and archives. The public repository's tracked content is approximately **5 MB**, below the 50-MB limit. The build's locked release step took **79.4 seconds** with the pinned base images already cached. Runtime image size is **81,729,194 bytes**, below 8 GB. The pipeline uses no scratch spills; the existing in-memory scaling limitation still applies to full-scale inputs not available here.

Image tags: `shopping-sessions-rust:submission` and `shopping-sessions-rust:latest`.
Image ID: `sha256:7f84b13cf24c130a2ccf136be332f1b039c04e3893ab0b426da178211a64c704`.
Platform/user/CMD: `linux/amd64`, `1000:1000`, `/usr/local/bin/shopping-sessions` with no arguments.

The refreshed ignored Docker-save archive is `shopping-sessions-rust-linux-amd64.tar`. It includes both image tags. Its manifest/config was read back and checked against the tested image ID, platform and user. Archive size: **84,826,112 bytes**. SHA-256: `ddc80718178f0ea79b7a7b99cb40eca014cb44c78325aa09bd334759c685db14`.

## Commands and local artifacts

Run these from repository root. Development tools in this workspace use the `/tmp` locations described in `VALIDATION.md`; Python validation uses Polars 2.1.0. The submitted image installs its locked Rust dependencies in the builder and needs neither Python nor network at runtime.

```sh
cargo fmt --check --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --manifest-path rust-pipeline/Cargo.toml
cargo test --locked --features profiling --manifest-path rust-pipeline/Cargo.toml
cargo clippy --locked --all-targets --manifest-path rust-pipeline/Cargo.toml -- -D warnings
cargo clippy --locked --all-targets --features profiling --manifest-path rust-pipeline/Cargo.toml -- -D warnings
cargo build --release --locked --no-default-features --manifest-path rust-pipeline/Cargo.toml

# Normal submission build on a public-CA build host:
docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:submission .

# Actual local build: same root context/ignore rules, session CA secret only during Cargo.
python3 - <<'PY'
from pathlib import Path
source = Path('Dockerfile').read_text().replace(
    'RUN cargo build --release --locked --no-default-features',
    'RUN --mount=type=secret,id=proxy_ca,required=true CARGO_HTTP_CAINFO=/run/secrets/proxy_ca cargo build --release --locked --no-default-features')
Path('/tmp/rust-submission-session.Dockerfile').write_text(source)
PY
docker buildx build --platform linux/amd64 --load \
  -f /tmp/rust-submission-session.Dockerfile \
  --secret id=proxy_ca,src=/etc/ssl/certs/ca-certificates.crt \
  -t shopping-sessions-rust:submission -t shopping-sessions-rust:latest .

# Actual container suite: deliberate attempts to enable every profiling switch.
python3 testing/harness.py --name rust-submission-root \
  --benchmark dev-1m rust-pipeline/dev dev-answer \
  --benchmark seed2-2m rust-pipeline/bench-seed2 rust-pipeline/bench-answer2 \
  --warmups 1 --repetitions 3 --profile-runs 0 --cpus 5 --mem-gb 24 \
  --resource-mode wall-only --artifacts rust-pipeline/bench-artifacts \
  -- bash testing/docker_adapter.sh shopping-sessions-rust:submission env \
  CHECKPOINTS_ENABLED=1 CHECKPOINTS_FILE=/output/checkpoint-must-not-exist.jsonl \
  DIAGNOSTICS=1 EVENT_PROFILE_DETAIL=1 /usr/local/bin/shopping-sessions

IMAGE=shopping-sessions-rust:submission CPUS=5 MEM=24g PLATFORM=linux/amd64 \
  sh starter-duckdb/scripts/docker_run.sh rust-pipeline/dev rust-pipeline/out-submission
python3 starter-duckdb/scripts/compare.py rust-pipeline/out-submission dev-answer

docker save shopping-sessions-rust:submission shopping-sessions-rust:latest \
  -o rust-pipeline/shopping-sessions-rust-linux-amd64.tar
```

Native fixtures were invoked with the same forced flags and a unique writable probe under `/tmp`. A post-run assertion verified the probe did not exist. After the container suite, assertions verified no probe file anywhere under the report directory and no `phase `, `phase_detail` or `elapsed=` output in any of the 44 stderr logs.

The complete, successful harness report, exact validations, fingerprints and logs are retained locally in the ignored directory:
`bench-artifacts/rust-submission-root-20261121T235025288355Z-bb280a/`.

The local proxy trust issue and temporary BuildKit CA-secret method are the same documented in `VALIDATION.md`; the tracked root Dockerfile has no environment-specific certificate workaround. The secret is not included in runtime layers. A separate ordinary public-CA environment was not available to test the unmodified build there. No competition submission was performed, and `main` is left unchanged.
