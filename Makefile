
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

# Define the `run-tests` target
run-tests:
	@echo "Running $(SUITE_GROUP) suite $(SUITE_NAME)"
	@for dir in $$(find ./$(SUITE_GROUP)/$(SUITE_PATH) -type d); do \
	  if ls "$$dir"/*_test.go >/dev/null 2>&1; then \
	    echo "$$dir not empty, running."; \
	    RECIPE_CLICKHOUSE_BIN=clickhouse USE_TESTCONTAINERS=1 gotestsum \
	      --junitfile="reports/$(SUITE_NAME)_$${dir//\//_}.xml" \
	      --junitfile-project-name="$(SUITE_GROUP)" \
	      --junitfile-testsuite-name="short" \
	      --rerun-fails \
	      --format github-actions \
	      --packages="$$dir" \
	      -- -timeout=15m; \
	  else \
	    echo "$$dir empty, skipping."; \
	  fi \
	done
