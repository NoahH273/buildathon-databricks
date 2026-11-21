PYTHON ?= python3
DEV ?= datasets/dev
ARTIFACTS ?= artifacts
CPUS ?= 4
MEM_GB ?= 8
REPETITIONS ?= 5
PROFILE_RUNS ?= 2
IMAGE ?= shopping-sessions:duckdb-baseline

.PHONY: test-harness benchmark-duckdb benchmark-image

test-harness:
	$(PYTHON) -m unittest discover -s testing/tests -v

benchmark-duckdb:
	$(PYTHON) testing/harness.py --name duckdb-baseline --artifacts $(ARTIFACTS) --benchmark dev-1m $(DEV) dev-answer --cpus $(CPUS) --mem-gb $(MEM_GB) --repetitions $(REPETITIONS) --profile-runs $(PROFILE_RUNS) -- $(PYTHON) starter-duckdb/pipeline/pipeline.py

benchmark-image:
	$(PYTHON) testing/harness.py --name container-baseline --artifacts $(ARTIFACTS) --benchmark dev-1m $(DEV) dev-answer --cpus $(CPUS) --mem-gb $(MEM_GB) --repetitions $(REPETITIONS) --profile-runs $(PROFILE_RUNS) --resource-mode wall-only -- bash testing/docker_adapter.sh $(IMAGE)
