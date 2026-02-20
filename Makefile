# DataCore Makefile
# Build and release automation

VERSION ?= $(shell grep 'version:' src/v.mod | sed "s/.*version: '\(.*\)'/\1/")
BINARY_NAME = datacore
BUILD_DIR = bin
SRC_DIR = src

# Build flags
V_FLAGS = -prod -enable-globals -d use_openssl -cflags "-I/opt/homebrew/include" -ldflags "-L/opt/homebrew/lib" -ldflags "-lsnappy" -ldflags "-llz4" -ldflags "-lzstd"
V_FLAGS_DEV = -enable-globals -d use_openssl -cflags "-I/opt/homebrew/include" -ldflags "-L/opt/homebrew/lib" -ldflags "-lsnappy" -ldflags "-llz4" -ldflags "-lzstd"

# Platforms
PLATFORMS = linux-amd64 linux-arm64 darwin-amd64 darwin-arm64

.PHONY: all build build-dev clean test fmt lint release docker help test-multibroker test-multibroker-compat test-multibroker-perf bench bench-replication

## Default target
all: build

## Build production binary
build:
	@echo "Building $(BINARY_NAME) v$(VERSION)..."
	@mkdir -p $(BUILD_DIR)
	cd $(SRC_DIR) && v $(V_FLAGS) -o ../$(BUILD_DIR)/$(BINARY_NAME) .
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)"

## Build development binary (with debug info)
build-dev:
	@echo "Building $(BINARY_NAME) (dev)..."
	@mkdir -p $(BUILD_DIR)
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) -o ../$(BUILD_DIR)/$(BINARY_NAME) .
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)"

## Run unit tests
test:
	@echo "Running tests..."
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) test domain/ config/ service/auth/ infra/auth/ infra/protocol/kafka/ infra/observability/ infra/storage/

## Run performance module tests (requires -enable-globals)
## Note: benchmarks/ is excluded as it contains performance benchmark tools, not unit tests
test-perf:
	@echo "Running performance tests..."
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) test infra/performance/core/ infra/performance/engines/ infra/performance/io/

## Run benchmark tests (requires -enable-globals)
test-bench:
	@echo "Running benchmark tests..."
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) test infra/performance/benchmarks/

## Run compatibility tests (requires Kafka CLI tools)
test-compat:
	@echo "Running compatibility tests..."
	@./scripts/run_compat_test.sh

## Run replication compatibility tests (requires 2+ brokers with replication enabled)
test-replication-compat:
	@echo "Running replication compatibility tests..."
	@./tests/compat/test_replication_compat.sh

## Run storage engine tests
test-storage:
	@echo "Running storage engine tests..."
	./scripts/test_storage.sh

## Run performance regression tests
test-performance:
	@echo "Running performance tests..."
	./scripts/test_performance.sh

## Run security tests (SSL, SASL)
test-security:
	@echo "Running security tests..."
	./scripts/test_security.sh

## Run long-running tests (24+ hours)
test-longrunning:
	@echo "Running long-running tests..."
	./scripts/test_longrunning.sh

## Run all tests (unit + compat + storage + security)
test-all: test test-compat test-replication-compat test-storage test-security

## Run multi-broker S3 integration tests (all)
test-multibroker:
	@echo "Running multi-broker S3 integration tests..."
	@./tests/multibroker/run_multibroker_test.sh

## Run multi-broker Kafka CLI compatibility tests only
test-multibroker-compat:
	@echo "Running multi-broker compatibility tests..."
	@./tests/multibroker/run_multibroker_test.sh --test=compat

## Run multi-broker performance benchmarks only
test-multibroker-perf:
	@echo "Running multi-broker performance benchmarks..."
	@./tests/multibroker/run_multibroker_test.sh --test=perf

## Run all replication benchmarks
bench:
	@echo "Building replication benchmarks..."
	@mkdir -p $(BUILD_DIR)
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) -o ../$(BUILD_DIR)/bench_replication ../tests/bench/
	@echo "Running all replication benchmarks..."
	@./$(BUILD_DIR)/bench_replication

## Run replication benchmarks only (throughput + latency + election + follower)
bench-replication:
	@echo "Building replication benchmarks..."
	@mkdir -p $(BUILD_DIR)
	cd $(SRC_DIR) && v $(V_FLAGS_DEV) -o ../$(BUILD_DIR)/bench_replication ../tests/bench/
	@echo "Running replication benchmarks..."
	@RUN_BENCH=throughput ./$(BUILD_DIR)/bench_replication
	@RUN_BENCH=latency ./$(BUILD_DIR)/bench_replication
	@RUN_BENCH=election ./$(BUILD_DIR)/bench_replication
	@RUN_BENCH=follower ./$(BUILD_DIR)/bench_replication

## Format code
fmt:
	@echo "Formatting code..."
	v fmt -w $(SRC_DIR)/

## Check format and lint
lint:
	@echo "Checking format..."
	v fmt -verify $(SRC_DIR)/
	@echo "Running vet..."
	v vet $(SRC_DIR)/

## Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)
	rm -rf release/

## Build for all platforms (requires cross-compilation setup)
build-all: clean
	@echo "Building for all platforms..."
	@mkdir -p $(BUILD_DIR)
	
	@echo "Building linux-amd64..."
	cd $(SRC_DIR) && v $(V_FLAGS) -o ../$(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 .
	
	@echo "Building darwin (current arch)..."
	cd $(SRC_DIR) && v $(V_FLAGS) -o ../$(BUILD_DIR)/$(BINARY_NAME)-darwin .
	
	@echo "Done! Binaries in $(BUILD_DIR)/"

## Create release archives
release: build
	@echo "Creating release v$(VERSION)..."
	@mkdir -p release
	
	@# Create archive
	@cp $(BUILD_DIR)/$(BINARY_NAME) release/
	@cp config.toml release/
	@cp -r docs release/ 2>/dev/null || true
	@cp README.md release/ 2>/dev/null || true
	@cp LICENSE release/ 2>/dev/null || true
	
	@cd release && tar -czvf ../$(BINARY_NAME)-$(VERSION).tar.gz .
	@echo "Created: $(BINARY_NAME)-$(VERSION).tar.gz"
	
	@# Generate checksum
	@shasum -a 256 $(BINARY_NAME)-$(VERSION).tar.gz > $(BINARY_NAME)-$(VERSION).tar.gz.sha256 2>/dev/null || \
		sha256sum $(BINARY_NAME)-$(VERSION).tar.gz > $(BINARY_NAME)-$(VERSION).tar.gz.sha256
	@echo "Created: $(BINARY_NAME)-$(VERSION).tar.gz.sha256"

## Build Docker image locally
docker:
	@echo "Building Docker image..."
	docker build -t $(BINARY_NAME):$(VERSION) -t $(BINARY_NAME):latest -t $(BINARY_NAME):local .
	@echo "Built: $(BINARY_NAME):$(VERSION)"

## Run the broker
run: build
	./$(BUILD_DIR)/$(BINARY_NAME) broker start

## Run with dev build
run-dev: build-dev
	./$(BUILD_DIR)/$(BINARY_NAME) broker start

## Create a new tag and push (triggers release workflow)
tag:
	@if [ -z "$(TAG)" ]; then \
		echo "Usage: make tag TAG=v0.1.0"; \
		exit 1; \
	fi
	@echo "Creating tag $(TAG)..."
	git tag -a $(TAG) -m "Release $(TAG)"
	git push origin $(TAG)
	@echo "Tag $(TAG) pushed. GitHub Actions will create the release."

## Show help
help:
	@echo "DataCore Build System"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Build Targets:"
	@echo "  build       Build production binary"
	@echo "  build-dev   Build development binary"
	@echo "  build-all   Build for all platforms"
	@echo "  clean       Clean build artifacts"
	@echo ""
	@echo "Test Targets:"
	@echo "  test            Run unit tests"
	@echo "  test-perf       Run performance tests (requires -enable-globals)"
	@echo "  test-bench      Run benchmark tests (requires -enable-globals)"
	@echo "  test-compat     Run compatibility tests (requires Kafka CLI)"
	@echo "  test-replication-compat Run replication CLI tests (2+ brokers)"
	@echo "  test-storage    Run storage engine tests (Memory, PostgreSQL, S3)"
	@echo "  test-performance Run performance regression tests"
	@echo "  test-security   Run security tests (SSL, SASL)"
	@echo "  test-longrunning Run long-running tests (24+ hours)"
	@echo "  test-all        Run all tests"
	@echo "  test-multibroker       Run multi-broker S3 integration tests"
	@echo "  test-multibroker-compat Run multi-broker compatibility tests"
	@echo "  test-multibroker-perf  Run multi-broker performance benchmarks"
	@echo "  bench                  Run all replication benchmarks"
	@echo "  bench-replication      Run replication benchmarks (throughput/latency/election/follower)"
	@echo ""
	@echo "Code Quality Targets:"
	@echo "  fmt         Format code"
	@echo "  lint        Check format and run vet"
	@echo ""
	@echo "Release Targets:"
	@echo "  release     Create release archive"
	@echo "  docker      Build Docker image locally"
	@echo "  tag         Create and push a git tag (TAG=v0.1.0)"
	@echo ""
	@echo "Run Targets:"
	@echo "  run         Build and run the broker"
	@echo "  run-dev     Build (dev) and run the broker"
	@echo ""
	@echo "Environment variables:"
	@echo "  VERSION     Override version (default: from v.mod)"
	@echo "  TAG         Tag name for 'make tag'"
	@echo ""
