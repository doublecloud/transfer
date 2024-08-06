
# removes static build artifacts
clean:
	@echo "--------------> Running 'make clean'"
	@rm -rf binaries tmp

build:
	go build -o  binaries/$(API) ./transfer_manager/go/cmd/trcli/*.go
