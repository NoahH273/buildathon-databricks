# Shopping Sessions submission

The repository-root Dockerfile builds the Rust Shopping Sessions pipeline for the competition. The source and development tools are in [`rust-pipeline/`](rust-pipeline/README.md); [`rules.md`](rules.md) defines the exact input/output contract.

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions-rust:latest .
IMAGE=shopping-sessions-rust:latest CPUS=8 MEM=24g \
  python3 starter-duckdb/scripts/run_fixtures.py starter-duckdb/fixtures -- sh starter-duckdb/scripts/docker_run.sh
```

The image runs as UID/GID 1000, needs no network, reads `/data`, and writes Parquet directly into `/output/user_daily/` and `/output/campaign_daily/`. It supports the scorer's read-only root/input, writable output/scratch and small `/tmp`. Build input is restricted to the Cargo manifests and Rust source by the root `.dockerignore`; generated datasets, outputs, Cargo targets and image archives stay out of the build and Git.

**Checkpointing is compiled out of the submission binary.** Both Dockerfiles use `cargo build --release --locked --no-default-features`. Environment variables, including `CHECKPOINTS_ENABLED=1`, `CHECKPOINTS_FILE`, `DIAGNOSTICS=1` and `EVENT_PROFILE_DETAIL=1`, cannot enable checkpoint files, diagnostic phase logs or per-operation timers in that binary. Profiling remains available only in an explicitly compiled development binary with `--features profiling`.

Submit this public repository with the validated `rust-pipeline` commit SHA. The default branch is left unchanged, so select this commit explicitly rather than scoring the default revision. Submission validation, exact commands and remaining resource limits are recorded in [`rust-pipeline/SUBMISSION.md`](rust-pipeline/SUBMISSION.md). Native development and profiling commands are in the [Rust README](rust-pipeline/README.md).
