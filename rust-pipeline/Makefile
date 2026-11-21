SHELL := /bin/bash
IMAGE ?= shopping-sessions-rust:latest
PLATFORM ?= linux/amd64
CPUS ?= 8
MEM ?= 24g
HARNESS ?= ../starter-duckdb/scripts
FIXTURES ?= ../starter-duckdb/fixtures
DEV ?= dev
OUT ?= out
ANSWER ?= ../dev-answer
export IMAGE PLATFORM CPUS MEM
.PHONY: build test fixtures native-fixtures dev run-dev check-dev
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .
test:
	cargo fmt --check
	cargo test --locked
	cargo build --release --locked
native-fixtures:
	cargo build --release --locked
	python3 $(HARNESS)/run_fixtures.py $(FIXTURES) -- ./target/release/shopping-sessions
fixtures: build
	python3 $(HARNESS)/run_fixtures.py $(FIXTURES) -- sh $(HARNESS)/docker_run.sh
dev:
	CARGO_TARGET_DIR=$(CURDIR)/target/generator cargo build --release --locked --manifest-path ../generator/Cargo.toml
	./target/generator/release/generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV) --threads $(CPUS)
run-dev: build
	time sh $(HARNESS)/docker_run.sh $(DEV) $(OUT)
check-dev:
	python3 $(HARNESS)/compare.py $(OUT) $(ANSWER)
