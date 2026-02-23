# DataCore

**DataCore** is an Apache Kafka-compatible message broker implemented in the V language.
It provides a plugin-based storage engine, multi-protocol gateway, built-in schema registry,
and enterprise-grade security — all in a single binary.

## Version

**v0.46.0**

## Key Features

- **Full Kafka API Compatibility**: Supports Kafka v1.1 through v4.1 protocols
- **KIP-932 Share Group**: Exactly-once consumption, poison message detection, and failover
- **Multi-Protocol Gateway**: Kafka (9092), REST/SSE/WebSocket (8080), gRPC (9094)
- **Transaction Support**: Exactly-Once Semantics (EOS) via Kafka Transaction API (API 22-28)
- **Admin API**: CreatePartitions, DescribeTopics, Config APIs
- **ACL Management**: Fine-grained access control (API 30-32)
- **SASL Authentication**: PLAIN, SCRAM-SHA-256/512, OAUTHBEARER
- **Multiple Storage Engines**: Memory, S3, and PostgreSQL (with Share Partition State Persistence)
- **OpenTelemetry Integration**: OTLP Exporter and 14+ metrics
- **Built-in Schema Registry**: Avro, JSON Schema, and Protobuf support
- **Message Compression**: Snappy, Gzip, LZ4, Zstd

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           DataCore                               │
├─────────────────────────────────────────────────────────────────┤
│  Interface Layer                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────────┐  │
│  │  Kafka   │ │   gRPC   │ │   REST   │ │  WebSocket / SSE  │  │
│  │ Protocol │ │ (9094)   │ │  (8080)  │ │     (8080)        │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Service Layer                                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────────┐  │
│  │  Topic   │ │ Consumer │ │  Auth /  │ │  Streaming        │  │
│  │ Service  │ │  Group   │ │  ACL     │ │  Service          │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Storage Layer (Plugin-based)                                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────────┐  │
│  │  Memory  │ │    S3    │ │PostgreSQL│ │  (Custom)         │  │
│  │ Adapter  │ │ Adapter  │ │ Adapter  │ │  Adapter          │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Domain Layer                                                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────────┐  │
│  │  Record  │ │  Topic   │ │  Group   │ │  Schema           │  │
│  │  Model   │ │  Model   │ │  Model   │ │  Registry         │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

> Clean Architecture: `Interface` → `Infra` → `Service` → `Domain`

## Port Configuration

| Protocol | Port | Description |
|----------|------|-------------|
| Kafka | 9092 | Kafka binary protocol (always active) |
| REST / SSE / WebSocket | 8080 | REST API, Server-Sent Events, WebSocket streaming |
| gRPC | 9094 | gRPC gateway (optional) |

---

## Quick Start

### 1. Build

```bash
# Clone the repository
git clone https://github.com/jupilhwang/datacore.git
cd datacore

# Build production binary
make build

# Build with debug symbols
make build-dev
```

The `bin/datacore` binary is created on success.

### 2. Run

```bash
# Start the broker
./bin/datacore broker start --config=config.toml

# Override settings via CLI flags
./bin/datacore broker start --broker-port=9092 --storage-engine=memory
```

### 3. Run with Docker

```bash
# Docker Compose (recommended)
docker-compose up -d

# Direct Docker run
docker build -t datacore:latest .
docker run -p 9092:9092 -p 8080:8080 -p 9094:9094 datacore:latest
```

---

## Installation

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| V Language | v0.5.0+ | [vlang.io](https://vlang.io/) |
| C Compiler | GCC / Clang / MSVC | Required by V |

### OS Dependencies

**macOS (Homebrew)**

```bash
brew install openssl snappy lz4 zstd postgresql
```

**Linux (Ubuntu / Debian)**

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential libssl-dev \
  libsnappy-dev liblz4-dev libzstd-dev \
  libpq-dev libnuma-dev liburing-dev zlib1g-dev
```

### Download Pre-built Binary

Download from the [GitHub Releases](https://github.com/jupilhwang/datacore/releases) page.

| Platform | File |
|----------|------|
| Linux x86-64 (static) | `datacore-linux-amd64-static.tar.gz` |
| Linux ARM64 (static) | `datacore-linux-arm64-static.tar.gz` |
| macOS Intel | `datacore-darwin-amd64.tar.gz` |
| macOS Apple Silicon | `datacore-darwin-arm64.tar.gz` |

```bash
# Linux
tar -xzf datacore-linux-amd64-static.tar.gz
chmod +x datacore
./datacore broker start

# macOS
tar -xzf datacore-darwin-arm64.tar.gz
chmod +x datacore
./datacore broker start
```

---

## Configuration

### Example `config.toml`

```toml
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1

[storage]
engine = "postgresql"   # memory | s3 | postgresql

[rest]
enabled = true
port = 8080

[grpc]
enabled = true
port = 9094

[telemetry]
enabled = true
service_name = "datacore"

[telemetry.otlp]
endpoint = "http://localhost:4317"

[logging]
level = "info"
output = "stdout"
```

### S3 Storage

```toml
[storage]
engine = "s3"

[s3]
endpoint   = "http://localhost:9000"
bucket     = "datacore"
region     = "us-east-1"
access_key = "your-access-key"
secret_key = "your-secret-key"
```

### PostgreSQL Storage

```toml
[storage]
engine = "postgresql"

[postgresql]
host     = "localhost"
port     = 5432
database = "datacore"
user     = "datacore"
password = "secret"
```

---

## CLI Usage

DataCore includes a built-in CLI for managing the broker without external tools.

### Topic Management

```bash
datacore topic create <name> --partitions <n>
datacore topic list
datacore topic describe <name>
datacore topic delete <name>
```

### Consumer Group

```bash
datacore group list
datacore group describe <group-id>
```

### Share Group (KIP-932)

```bash
datacore share-group list
datacore share-group describe <group-id>
```

### ACL Management

```bash
datacore acl create --principal <principal> --operation <op> --resource <resource>
datacore acl list
datacore acl delete --principal <principal>
```

### Cluster & Offset

```bash
datacore cluster describe
datacore offset list --topic <name>
datacore health
```

---

## Kafka API Support

| API Key | API Name | Notes |
|---------|----------|-------|
| 0 | Produce | |
| 1 | Fetch | |
| 2 | ListOffsets | |
| 3 | Metadata | |
| 8 | OffsetCommit | |
| 9 | OffsetFetch | |
| 10 | FindCoordinator | |
| 11 | JoinGroup | |
| 12 | Heartbeat | |
| 13 | LeaveGroup | |
| 14 | SyncGroup | |
| 15 | DescribeGroups | |
| 16 | ListGroups | |
| 18 | ApiVersions | |
| 19 | CreateTopics | |
| 20 | DeleteTopics | |
| 22 | InitProducerId | Transaction |
| 23 | AddPartitionsToTxn | Transaction |
| 24 | AddOffsetsToTxn | Transaction |
| 25 | EndTxn | Transaction |
| 26 | WriteTxnMarkers | Transaction |
| 27 | TxnOffsetCommit | Transaction |
| 28 | DescribeTransactions | Transaction |
| 29 | ListTransactions | Transaction |
| 30 | CreateAcls | ACL |
| 31 | DescribeAcls | ACL |
| 32 | DeleteAcls | ACL |
| 37 | CreatePartitions | Admin |
| 75 | DescribeTopicPartitions | Admin |

---

## Key Capabilities

### KIP-932 Share Group

DataCore implements the KIP-932 Share Group specification:

- **Exactly-once consumption**: Messages are delivered to exactly one consumer within a group
- **Poison message detection**: Automatically detects and handles unprocessable messages
- **Failover**: Seamlessly reassigns in-flight messages when a consumer fails
- **PostgreSQL persistence**: Share Partition State is durably persisted to PostgreSQL

### Multi-Protocol Gateway

| Protocol | Port | Use Case |
|----------|------|----------|
| Kafka TCP | 9092 | Standard Kafka clients (`kafka-console-*`, `kcat`) |
| REST | 8080 | HTTP produce/consume |
| SSE | 8080 | Real-time event streaming to browsers |
| WebSocket | 8080 | Bidirectional streaming (RFC 6455) |
| gRPC | 9094 | High-performance RPC clients |

### SASL Authentication

| Mechanism | Status |
|-----------|--------|
| PLAIN | Supported |
| SCRAM-SHA-256 | Supported |
| SCRAM-SHA-512 | Supported |
| OAUTHBEARER | Supported |

### Transactions (Exactly-Once Semantics)

Full Kafka Transaction API support (API 22-28):

```
InitProducerId → BeginTransaction → Produce
→ AddOffsetsToTxn → TxnOffsetCommit → EndTxn (Commit/Abort)
```

### Message Compression

| Algorithm | Characteristics |
|-----------|-----------------|
| Snappy | High speed, low CPU overhead |
| Gzip | Balanced ratio and speed |
| LZ4 | Ultra-fast, optimized for real-time |
| Zstd | Best-in-class compression ratio |

### Storage Engines

| Engine | Use Case | Characteristics |
|--------|----------|-----------------|
| Memory | Development / Testing | Fastest, no persistence |
| S3 | Production | Infinite scaling, long-term storage |
| PostgreSQL | Production | Transaction support, Share Group state persistence |

**S3 Optimizations (since v0.41.0)**

- HTTP connection pooling (reduced latency via reuse)
- Parallel segment download / upload (up to 10x improvement)
- Parallel offset commits (5-10x faster)
- Parallel object deletion (20x faster)

### OpenTelemetry Integration

DataCore exports metrics via OTLP to any OpenTelemetry-compatible backend (Jaeger, Grafana, Datadog, etc.).

**Available Metrics (14+ total)**

| Category | Metrics |
|----------|---------|
| Share Group | `datacore_share_group_acquired`, `acked`, `released`, `rejected`, `active_sessions` |
| gRPC Gateway | `datacore_grpc_requests_total`, `datacore_grpc_latency_seconds` |
| Partition | `datacore_partition_log_size`, `datacore_partition_offset`, `datacore_partition_lag` |
| Consumer Group | `datacore_consumer_group_members`, `datacore_consumer_group_lag` |

**Health & Observability Endpoints**

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Overall health check |
| `GET /ready` | Readiness probe (Kubernetes) |
| `GET /live` | Liveness probe (Kubernetes) |
| `GET /metrics` | Prometheus-formatted metrics |

---

## Kafka Client Compatibility

DataCore is fully compatible with standard Kafka clients:

```bash
# Produce
kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning

# Inspect
kcat -b localhost:9092 -t test-topic

# Run compatibility test suite
make test-compat
```

---

## Testing

```bash
# Unit tests
make test

# All tests (unit + integration + storage + security)
make test-all

# Kafka client compatibility
make test-compat

# Specific test file
v test src/config/config_test.v
```

### Test Suite

| Type | Description | Command |
|------|-------------|---------|
| Unit | Module-level correctness | `make test` |
| Integration | End-to-end flow | `make test-all` |
| Storage | Memory / PostgreSQL / S3 adapters | `./scripts/test_storage.sh` |
| Compatibility | Standard Kafka client interop | `make test-compat` |
| Security | SSL/TLS and SASL | `./scripts/test_security.sh` |
| Long-running | 24h+ stability | `./scripts/test_longrunning.sh` |

---

## Project Structure

```
datacore/
├── src/
│   ├── domain/          # Domain models (pure V, no external deps)
│   ├── service/         # Business logic
│   ├── infra/
│   │   ├── storage/     # Storage adapters (Memory, S3, PostgreSQL)
│   │   ├── protocol/    # Kafka, gRPC, HTTP protocol handlers
│   │   ├── gateway/     # Multi-protocol gateway adapter
│   │   ├── observability/ # OpenTelemetry, metrics, OTLP exporter
│   │   └── transaction/ # Transaction coordinator
│   └── interface/
│       ├── cli/         # CLI commands (topic, group, acl, ...)
│       ├── grpc/        # gRPC server (port 9094)
│       └── rest/        # REST / SSE / WebSocket server (port 8080)
├── tests/
│   ├── unit/            # Unit tests
│   └── integration/     # Integration tests
├── scripts/             # Utility scripts
├── docs/                # Documentation & changelogs
├── config.toml          # Default configuration
└── Makefile             # Build targets
```

---

## Development Guide

### Build Commands

```bash
make build       # Production binary → bin/datacore
make build-dev   # Debug build with symbols
make lint        # v fmt -verify + v vet
make clean       # Remove build artifacts
```

### Coding Standards

- **snake_case**: variables, functions, files
- **PascalCase**: structs, enums, interfaces
- **Error handling**: use `!` result type — no `panic()`
- **Logging**: `logger.info()` / `logger.warn()` / `logger.error()` only
- **Architecture**: outer layers depend on inner layers only

### Before Committing

```bash
v fmt -w .   # Apply formatting
v test .     # Run all tests
```

---

## License

Apache License 2.0
