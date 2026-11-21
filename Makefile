PYTHON ?= python3
DEV ?= datasets/dev
ARTIFACTS ?= artifacts
CPUS ?= 4
MEM_GB ?= 8
REPETITIONS ?= 5
PROFILE_RUNS ?= 2
IMAGE ?= shopping-sessions:hybrid
HYBRID_BIN ?= $(abspath starter-duckdb/rust/target/release/shopping-hybrid)

.PHONY: test-harness benchmark-duckdb benchmark-hybrid benchmark-image

test-harness:
	$(PYTHON) -m unittest discover -s testing/tests -v

benchmark-duckdb: benchmark-hybrid

benchmark-hybrid:
	HYBRID_BIN="$(HYBRID_BIN)" $(PYTHON) testing/harness.py --name rust-duckdb-hybrid --artifacts $(ARTIFACTS) --benchmark dev-1m $(DEV) dev-answer --cpus $(CPUS) --mem-gb $(MEM_GB) --repetitions $(REPETITIONS) --profile-runs $(PROFILE_RUNS) -- $(PYTHON) starter-duckdb/pipeline/pipeline.py

benchmark-image:
	$(PYTHON) testing/harness.py --name rust-duckdb-hybrid-container --artifacts $(ARTIFACTS) --benchmark dev-1m $(DEV) dev-answer --cpus $(CPUS) --mem-gb $(MEM_GB) --repetitions $(REPETITIONS) --profile-runs $(PROFILE_RUNS) --resource-mode wall-only -- bash testing/docker_adapter.sh $(IMAGE)
