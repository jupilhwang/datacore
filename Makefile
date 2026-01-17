# DataCore Makefile
# Build and release automation

VERSION ?= $(shell grep 'version:' src/v.mod | sed "s/.*version: '\(.*\)'/\1/")
BINARY_NAME = datacore
BUILD_DIR = bin
SRC_DIR = src

# Build flags
V_FLAGS = -prod
V_FLAGS_DEV =

# Platforms
PLATFORMS = linux-amd64 linux-arm64 darwin-amd64 darwin-arm64

.PHONY: all build build-dev clean test fmt lint release docker help

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
	cd $(SRC_DIR) && v test service/auth/ infra/auth/ infra/protocol/kafka/ infra/observability/

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
	@mkdir -p docker-context/linux-amd64
	cd $(SRC_DIR) && v $(V_FLAGS) -o ../docker-context/linux-amd64/$(BINARY_NAME) .
	docker build -t $(BINARY_NAME):$(VERSION) -t $(BINARY_NAME):latest .
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
	@echo "Targets:"
	@echo "  build       Build production binary"
	@echo "  build-dev   Build development binary"
	@echo "  test        Run unit tests"
	@echo "  fmt         Format code"
	@echo "  lint        Check format and run vet"
	@echo "  clean       Clean build artifacts"
	@echo "  build-all   Build for all platforms"
	@echo "  release     Create release archive"
	@echo "  docker      Build Docker image locally"
	@echo "  run         Build and run the broker"
	@echo "  run-dev     Build (dev) and run the broker"
	@echo "  tag         Create and push a git tag (TAG=v0.1.0)"
	@echo "  help        Show this help"
	@echo ""
	@echo "Environment variables:"
	@echo "  VERSION     Override version (default: from v.mod)"
	@echo "  TAG         Tag name for 'make tag'"
