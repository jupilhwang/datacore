# DataCore

**DataCore** is an Apache Kafka-compatible message broker implemented in the V language. It supports a plugin-based storage engine, a built-in schema registry, and zero-copy performance optimizations.

## Version

**v0.44.0**

## Key Features

- **Full Kafka API Compatibility**: Supports Kafka v1.1 through v4.1 protocols
- **Iceberg REST Catalog**: Apache Iceberg-compatible REST catalog (v3 supported)
- **Multiple Storage Engines**: Memory, S3, and PostgreSQL support
- **Built-in Schema Registry**: Avro, JSON Schema, and Protobuf support
- **High-Performance Networking**: io_uring, WebSocket, and SSE support
- **Enterprise-Grade Security**: SASL/SCRAM authentication and ACL permission management
- **Transaction Support**: Exactly-Once Semantics (EOS)

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DataCore                              │
├─────────────────────────────────────────────────────────────┤
│  Interface Layer                                             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Kafka   │ │   gRPC   │ │   REST   │ │  WebSocket/SSE  │ │
│  │ Protocol │ │ Handler  │ │  Server  │ │  Streaming      │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Service Layer                                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Topic   │ │ Consumer │ │  Auth    │ │  Streaming      │ │
│  │ Service  │ │  Service │ │ Service  │ │  Service        │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Storage Layer (Plugin-based)                                │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Memory  │ │    S3    │ │PostgreSQL│ │  (Custom)       │ │
│  │ Adapter  │ │ Adapter  │ │ Adapter  │ │  Adapter        │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Domain Layer                                                │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Record  │ │  Topic   │ │  Group   │ │  Schema         │ │
│  │  Model   │ │  Model   │ │  Model   │ │  Registry       │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Key Capabilities

### Message Compression Support

DataCore supports a variety of message compression algorithms to optimize network bandwidth and storage usage.

**Supported Compression Formats:**

- **Snappy**: High-speed compression with low CPU overhead
- **Gzip**: Balanced compression ratio and speed
- **LZ4**: Ultra-fast compression, optimized for real-time processing
- **Zstd**: Best-in-class compression ratio using a modern algorithm

**Compatibility:**

- Fully compatible with standard Kafka clients
- Supports `kafka-console-producer` and `kafka-console-consumer`
- Compatible with `kcat` (kafkacat)
- Verify standard Kafka client compatibility via `make test-compat`

```bash
# Run compression tests
make test-compat
```

### Multiple Storage Engines

DataCore supports a plugin-based storage architecture:

| Storage | Use Case | Characteristics |
|---------|----------|-----------------|
| **Memory** | Development / Testing | Fastest speed, no persistence |
| **S3** | Production | Infinite scaling, long-term storage |
| **PostgreSQL** | Production | Transaction support, complex queries |

### S3 Storage Improvements (v0.41.0)

**New Features:**

- **HTTP Connection Pooling**: Reduces latency through connection reuse
- **Parallel Processing Optimization**: Parallelized segment downloads and uploads
- **Enhanced Caching**: Optimized index cache TTL

**Performance Improvements:**

- Partition compaction: Sequential to parallel processing (up to 10x improvement)
- Offset commits: 5–10x faster with parallel commits
- Object deletion: 20x faster with parallel deletion

### Observability

**Metrics:**

- Detailed per-storage metrics (Memory, S3, PostgreSQL)
- Per-protocol-handler performance tracking
- Kafka API request/response statistics

**Logging:**

- Structured logging (JSON format)
- OpenTelemetry OTLP export support
- Dynamic log level adjustment

**Health Checks:**

- Kubernetes-compatible `/health`, `/ready`, and `/live` endpoints
- Prometheus-formatted `/metrics` endpoint

## Installation Guide

### 1. Download Binary

The fastest way to get started is to download a pre-built binary for your OS from the [GitHub Releases](https://github.com/jupilhwang/datacore/releases) page.

1. Navigate to the latest release page.
2. Download the binary for your operating system:
    - **Linux**: `datacore-linux-amd64-static.tar.gz` (static build, no dependencies)
    - **Linux ARM64**: `datacore-linux-arm64-static.tar.gz` (static build)
    - **macOS Intel**: `datacore-darwin-amd64.tar.gz` (dynamic build)
    - **macOS Apple Silicon**: `datacore-darwin-arm64.tar.gz` (dynamic build)
3. Extract the archive, grant execute permissions, and run.

    ```bash
    # Linux (static build - no dependencies required)
    tar -xzf datacore-linux-amd64-static.tar.gz
    chmod +x datacore
    ./datacore broker start
    
    # macOS (dynamic build - requires Homebrew libraries)
    tar -xzf datacore-darwin-arm64.tar.gz
    chmod +x datacore
    ./datacore broker start
    ```

### 2. Build from Source

Follow these steps if you want to build manually or contribute to development.

#### 2.1 Prerequisites

- **V Language**: Install V by following the instructions on the [official V website](https://vlang.io/).
- **C Compiler**: GCC, Clang, or MSVC is required.

#### 2.2 Install OS-Specific Dependency Libraries

DataCore uses external libraries for compression (Snappy, LZ4, Zstd), security (OpenSSL), and database (PostgreSQL) integration.

**macOS (using Homebrew)**

```bash
brew install openssl snappy lz4 zstd postgresql
```

**Linux (Ubuntu/Debian)**

```bash
sudo apt-get update
sudo apt-get install -y build-essential libssl-dev libsnappy-dev liblz4-dev libzstd-dev libpq-dev libnuma-dev liburing-dev zlib1g-dev
```

#### 2.3 Build Process

```bash
# Clone the repository
git clone https://github.com/jupilhwang/datacore.git
cd datacore

# Build the binary
make build
```

Upon a successful build, the `bin/datacore` binary will be created.

## Quick Start

### Build

```bash
make build
```

### Run

```bash
# Start the broker with default configuration
./bin/datacore broker start --config=config.toml

# Override settings with CLI arguments
./bin/datacore broker start --broker-port=9092 --storage-engine=memory
```

### Run with Docker

```bash
# Run with Docker Compose
docker-compose up -d

# Build and run Docker image directly
docker build -t datacore:latest .
docker run -p 9092:9092 -p 8080:8080 datacore:latest
```

### Example Configuration File (`config.toml`)

```toml
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1

[storage]
engine = "s3"

[s3]
endpoint = "http://localhost:9000"
bucket = "datacore"
region = "us-east-1"
access_key = "your-access-key"
secret_key = "your-secret-key"

[logging]
level = "info"
output = "stdout"
```

## Testing

### Unit Tests

The recommended approach is to use the `Makefile`:

```bash
# Run unit tests for key modules
make test

# Run all tests (including integration, storage, and security)
make test-all
```

When running `v test` manually, the `-enable-globals` flag is required for tests that depend on global variables, such as performance modules:

```bash
# Run all unit tests
v -enable-globals test src/

# Run a specific test file
v -enable-globals test src/config/config_test.v
```

### Integration Tests

```bash
# Run all integration tests
v -enable-globals test tests/

# Run storage-specific tests
./scripts/test_storage.sh all

# Run Kafka client compatibility tests
make test-compat

# Run performance regression tests
./scripts/test_performance.sh --save-baseline
```

### Test Suite

| Test Type | Description | Command |
|-----------|-------------|---------|
| **Storage Tests** | Validates Memory, PostgreSQL, and S3 storage | `./scripts/test_storage.sh` |
| **Message Format Tests** | Validates compression and encoding compatibility | `v test tests/` |
| **Admin API Tests** | Tests topic management and configuration changes | `v test tests/integration/` |
| **Compatibility Tests** | Validates compatibility with standard Kafka clients | `make test-compat` |
| **Long-Running Tests** | Validates stability over 24+ hours | `./scripts/test_longrunning.sh` |
| **Security Tests** | Tests SSL/TLS and SASL authentication | `./scripts/test_security.sh` |

## Kafka Client Compatibility

DataCore is fully compatible with standard Kafka clients:

```bash
# Produce messages with kafka-console-producer
kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

# Consume messages with kafka-console-consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning

# Inspect messages with kcat
kcat -b localhost:9092 -t test-topic
```

## Project Structure

```
datacore/
├── src/
│   ├── domain/          # Domain models
│   ├── service/         # Business logic
│   ├── infra/
│   │   ├── storage/     # Storage adapters
│   │   ├── protocol/    # Protocol handlers
│   │   └── server/      # Network servers
│   └── interface/       # External interfaces
├── tests/
│   ├── unit/            # Unit tests
│   └── integration/     # Integration tests
├── scripts/             # Utility scripts
├── docs/                # Documentation
├── config.toml          # Default configuration file
└── Makefile             # Build targets
```

## License

Apache License 2.0
