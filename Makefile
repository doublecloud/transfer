
# removes static build artifacts
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp

build:
	go build -o  binaries/$(API) ./cmd/trcli/*.go

test:
	USE_TESTCONTAINERS=1 gotestsum --rerun-fails --format github-actions --packages="./cmd/..." -- -timeout=30m
