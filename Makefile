
# removes static build artifacts
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp

build:
	go build -o  binaries/$(API) ./transfer_manager/go/cmd/trcli/*.go

test:
	USE_TESTCONTAINERS=1 gotestsum --format github-actions ./transfer_manager/go/cmd/...  -timeout=30m
	USE_TESTCONTAINERS=1 gotestsum --format github-actions ./transfer_manager/go/tests/e2e/...  -timeout=30m
