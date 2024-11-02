
# removes static build artifacts
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp

build:
	go build -o  binaries/$(API) ./cmd/trcli/*.go

test:
	USE_TESTCONTAINERS=1 gotestsum --rerun-fails --format github-actions --packages="./cmd/..." -- -timeout=30m


# Define variables for the suite group, path, and name with defaults
SUITE_GROUP ?= 'tests/e2e'
SUITE_PATH ?= 'pg2pg'
SUITE_NAME ?= 'e2e-pg2pg'
SHELL := /bin/bash

# Define the `run-tests` target
run-tests:
	@echo "Running $(SUITE_GROUP) suite $(SUITE_NAME)"
	@export RECIPE_CLICKHOUSE_BIN=clickhouse; \
	export USE_TESTCONTAINERS=1; \
	export YA_TEST_RUNNER=1; \
	export YT_PROXY=localhost:8180; \
	for dir in $$(find ./$(SUITE_GROUP)/$(SUITE_PATH) -type d); do \
	  if ls "$$dir"/*_test.go >/dev/null 2>&1; then \
	    echo "::group::$$dir"; \
	    echo "Running tests for directory: $$dir"; \
	    sanitized_dir=$$(echo "$$dir" | sed 's|/|_|g'); \
	    gotestsum \
	      --junitfile="reports/$(SUITE_NAME)_$$sanitized_dir.xml" \
	      --junitfile-project-name="$(SUITE_GROUP)" \
	      --junitfile-testsuite-name="short" \
	      --rerun-fails \
	      --format github-actions \
	      --packages="$$dir" \
	      -- -timeout=15m; \
	    echo "::endgroup::"; \
	  else \
	    echo "No Go test files found in $$dir, skipping tests."; \
	  fi \
	done
