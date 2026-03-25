# DataCore Changelog

## [v0.52.1] -- 2026-03-25

### Fixed
- handler 파일 DIP 일관성: observability.field_* -> port.field_* 통일 (309건)
- port.field_duration/field_bytes/field_float 포맷을 observability 버전과 동일하게 수정
- Sub-handler 미사용 필드 18개 제거 (7 SubHandler struct 정리, net -64줄)
- startup 시 인증 미설정 경고 로그 추가
- handler_offset_test.dSYM macOS 디버그 심볼 git에서 제거

## [v0.52.0] -- 2026-03-25

### Added
- 8 new Port interfaces for Handler DIP compliance: ProtocolMetricsPort, OffsetManagerPort, CompressionPort, AuditLoggerPort, SchemaRegistryPort, TransactionCoordinatorPort, PartitionAssignerPort, ShareGroupCoordinatorPort
- ISP split: BrokerRegistryPort (query-only) + BrokerLifecyclePort (lifecycle mutations)
- CompressionPortAdapter for bridging compression types via Port layer
- handler_factory.v: Composition Root pattern isolating all concrete dependencies

### Removed
- 8 concrete imports from handler.v (infra.auth, infra.compression, infra.observability, service.cluster, service.group, service.offset, service.schema, service.transaction)
- ~624 unnecessary pub fn converted to fn (module-internal visibility)
- `-enable-globals` flag from Makefile (no global variables in codebase)

### Fixed
- BrokerRegistryPort ISP split resolving interface mismatch with BrokerRegistry concrete
- Offset test type references updated for port module migration

## [v0.51.0] -- 2026-03-25

### Refactored
- `tcp.v`: `handle_connection` 240 -> 73 lines (6 private helpers extracted)
- `handler_metadata.v`: `process_metadata` 193 -> 63 lines, `MetadataResponse.encode` 151 -> 47 lines (6 helpers)
- `handler_produce.v`: `process_produce` 188 -> 62 lines (3 helpers)
- `handler_config.v`: `process_describe_configs` 185 -> 26 lines (4 helpers, DRY: config entry builder)
- `handler_fetch.v`: 3 God Functions split into 9 helpers
- `handler_offset.v`: `handle_offset_fetch` 133 -> 15 lines (3 helpers)

### Removed
- Dead functions: `init_telemetry`, `get_telemetry`, `shutdown_telemetry`, `compression_type_from_u8`

### Fixed
- DRY: Partition metadata generation duplicated 2x in process_metadata -> consolidated
- DRY: DescribeConfigsEntry struct literal repeated 7x -> build_config_entry() helper

## [Unreleased]

### Fixed
- Service 계층 CRITICAL `or {}` 에러 삼키기 17건 수정 (share_partition, kip848_coordinator, controller_election, broker_registry_heartbeat, tcp, rest/server, mmap, s3/cluster_metadata, s3/adapter_topic, handler_share_group)
- Domain 계층 Clean Architecture 위반 해소: grpc.v (`import common` 제거), replication.v (`import sync`/`import json` 제거), streaming.v (`import common` 제거)
- infra/replication/*.v: stdlib `import log` -> Logger Port 주입으로 교체

### Removed
- Stateless 설계에 불필요한 dead code 삭제: `trigger_rebalance_for_topic`, `generate_reassignment_plan`
- `SqliteStorageConfig` 완전 제거 (struct, parse, validate, save, config.toml)
- `TelemetryRootConfig` 트리 4개 구조체 + 파싱/저장 로직 제거
- 미구현 인터페이스 제거: `LockableStorage`, `Lock`, `MessageConsumerPort`
- Dead code 함수: `extract_json_field`, `extract_json_number`
- config.toml: `[profiles.*]` 3개 섹션, `resource_attributes` 필드

### Refactored
- 중복 코드 ~600줄 제거/통합: binary I/O, varint, escape_json, CRC32C, hex utilities
- `infra/performance/core/utils.v` 14개 함수를 `common` 위임 thin wrapper로 전환 (-110줄)
- CLI 로컬 함수 -> `common.*` import로 통합 (~120줄)
- `domain/replication.v`에서 `sync.Mutex` 제거, 스레드 안전성을 infra 계층으로 위임
- `domain/replication.v`의 `to_json()` -> `infra/replication/protocol.v`의 `serialize_replication_message()`로 이동

## v0.50.8 - 2026-03-23

### Refactoring (God Class Decomposition)
- Handler: Introduced HandlerContext + 7 sub-handler structs (ProduceSubHandler, FetchSubHandler, AuthSubHandler, TransactionSubHandler, GroupSubHandler, AdminSubHandler, ShareGroupSubHandler)
- Replication Manager: Extracted BrokerHealthTracker (92L), ReplicationStatsCollector (141L), ReplicaAssignmentStore (129L) from 586L Manager
- BrokerRegistry: Split into broker_registry.v (233L), broker_registry_heartbeat.v (133L), broker_registry_queries.v (119L)
- 13 new focused files created, all under 200 lines

## v0.50.7 - 2026-03-23

### ISP Consumer Migration
- controller_election.v: ClusterMetadataPort -> DistributedLockPort + ClusterStatePort
- partition_assigner.v: ClusterMetadataPort -> PartitionAssignmentPort + BrokerRegistryPort
- broker_registry.v: ClusterMetadataPort -> BrokerRegistryPort + ClusterStatePort + BrokerHealthPort
- New: cluster_port_adapter.v for V interface bridging

### Bug Fixes
- rate_limiter.v: Fixed token non-rollback in allow_request_with_bytes() via pre-check pattern
- Added can_consume() non-mutating method to TokenBucket
- New test: test_allow_request_with_bytes_no_token_leak

## v0.50.6 (2026-03-22) - Performance & ISP Optimization

### Performance
- connection_pool close_all(): TCP close moved outside lock (prevents shutdown stall)
- binary_helpers write_i16/i32/i64: zero-alloc bit-shift pattern (eliminates temp buffer allocation)
- rate_limiter: combined allow_request_with_bytes() reduces lock acquisition from 2x to 1x per request

### Architecture
- ClusterMetadataPort (15 methods) split into 5 ISP-compliant sub-interfaces: BrokerRegistryPort(6), ClusterStatePort(2), PartitionAssignmentPort(3), DistributedLockPort(3), BrokerHealthPort(2)

## v0.50.5 (2026-03-22)

### Bug Fixes
- Fixed binary protocol DoS vulnerability: added bounds checks to read_i16/i32/i64 in binary_helpers.v and binary_utils.v (prevents crash from truncated replication messages)
- Fixed config file permissions: saved config now set to 0600 (owner-only) to protect credentials

### Architecture
- Extracted port.LoggerPort interface: removed all 6 service-layer imports of infra.observability (Clean Architecture compliance)
- Created port.LogField, port.CounterMetric, port.HistogramMetric abstractions for service layer
- Added infra/observability/logger_adapter.v for DIP compliance

### Refactoring
- Split PostgreSQL adapter: 1367 lines -> 7 files (adapter, topic, record, group, offset, share, schema)
- Split Memory adapter: 1059 lines -> 6 files (adapter, topic, record, group, offset, share)
- Decomposed process_produce: 505-line function -> 170-line orchestrator + 6 focused helpers in handler_produce_helpers.v

## v0.50.4 (2026-03-22)
### Refactoring
- config.v God File split: 969 lines -> 5 files (config.v 218, config_parse.v 300, config_save.v 259, config_validate.v 76, config_cli.v 92)
- metrics_helpers.v: decomposed 296-line new_datacore_metrics() into 12 sub-functions (largest: 47 lines)
- Fixed Clean Architecture violation: domain/grpc.v no longer imports infra layer; extracted common/binary_utils.v

### Bug Fixes
- Fixed i16 producer_epoch overflow in TransactionCoordinator (wraps at 32767 -> 0)
- Added sync.Mutex to TransactionCoordinator for thread-safe state transitions
- Fixed recovery handler in ReplicationManager to return buffered data (was returning empty ACK)

## v0.50.3 (2026-03-22) - Post-Merge Refactoring

### Refactoring
- Split manager.v God Class into manager.v + manager_workers.v + manager_buffers.v
- Split metrics.v into metrics.v + metrics_types.v + metrics_helpers.v
- Extracted magic numbers to named constants in binary_protocol.v, coordinator.v
- Extracted contains_topic_partition() helper for DRY compliance
- Fixed stale hardcoded version in health_handler.v

### Bug Fixes
- Fixed TOCTOU race in connection_pool.v acquire() with post-dial limit re-check
- Fixed cleanup_idle() holding mutex during I/O operations

### Performance
- Replaced O(n) buffer size scan with O(1) incremental counter in replication manager

## v0.50.2 (2026-03-22) - Code Review Fixes

### Bug Fixes
- fix: binary protocol negative length validation (CRITICAL - C1, C2)
- fix: connection pool TOCTOU race condition (H1)
- fix: connection pool leak on send failure (H2)
- fix: replication server/manager data race with stdatomic (H3, H5)
- fix: manager cluster_broker_refs data race (H4)
- fix: binary protocol i16 overflow guard (H6)
- fix: SASL handler internal error message leak (H10)

### Refactoring
- refactor: extract broker_startup_helpers.v from broker_startup.v (269 lines, was 400)
- refactor: extract binary_helpers.v from binary_protocol.v (145 lines, was 311)
- refactor: DRY extraction in handler_transaction.v (-58 lines)

### Performance
- perf: replace O(n^2) string concatenation with strings.Builder in metrics export

## v0.50.1 (2026-03-22)

### Removed
- ISR Manager, Rebalance Trigger, Partition Leader Election -- incompatible with stateless architecture

### Changed
- Replication protocol migrated from JSON to compact binary format (reduced wire size, improved throughput)
- `infra/performance/io/` renamed to `sysio/` to resolve V 0.5 stdlib module name collision

### Added
- `[broker.rate_limit]` configuration section in config.toml (disabled by default)
- Audit logger wired into SASL authentication handlers
- Binary replication integration tests (client-server roundtrip, wire format verification)

### Fixed
- infra/performance V 0.5 compatibility: all 8 test files now pass (previously 5 failures)
- http_exporter.v adapted to V 0.5 stdlib (removed io.new_buffered_reader usage)

## v0.50.0 (2026-03-22) - Comprehensive Architecture Improvement

### Test Coverage
- Added 36+ test files covering ALL Kafka protocol handlers (produce, fetch, metadata, topic, offset, group, sasl, acl, transaction, consumer, share_group, config, describe_cluster, log_dirs, admin, incremental_alter_configs, api_versions, find_coordinator, list_offsets)
- Added service layer tests (produce, fetch, topic_manager, transaction_coordinator)
- Added infra tests (replication protocol/client/server, s3_client, SCRAM-SHA-512)
- Total: 125 test files, 1,704 test functions (from ~89 files)

### Breaking Changes
- Removed all `__global` variable declarations (replaced with const holder pattern)
- Requires V 0.5+ (no longer needs `-enable-globals` flag)

### High Availability
- ISR Manager: tracks replica offsets, shrinks/expands ISR set, validates min.insync.replicas, calculates high watermark
- Partition Rebalancing: RebalanceTrigger with debounce, wired into BrokerRegistry
- Partition-Level Leader Election: elect from ISR, unclean election option, preferred leader election, broker failure handling

### Performance
- Replication Connection Pooling: reusable TCP connections per host, idle cleanup
- Binary Replication Protocol: compact binary serialization (smaller than JSON)
- Rate Limiting: token bucket algorithm, global + per-IP limits, Kafka error 55 throttle response

### Security
- SCRAM-SHA-512: comprehensive test coverage (17 tests)
- Audit Logger: buffered event logging with type filtering

### Structural Improvements
- REST server.v split: 907 -> 375 lines (+health_handler, topic_handler, message_handler, metrics_handler)
- main.v split: 700 -> 112 lines (+broker_startup, cli_commands)
- WriteTxnMarkers: fully implemented (was TODO)

## v0.49.0 (2026-03-22)

### Breaking Changes
- StoragePort interface split into 6 sub-interfaces (TopicStoragePort, RecordStoragePort, GroupStoragePort, OffsetStoragePort, SharePartitionPort, StorageHealthPort). Composite StoragePort preserved for backward compatibility.

### New Features
- Confluent wire format schema encode/decode (Avro, JSON, Protobuf)
- LinuxPerformanceEngine with io_uring integration and fd caching
- S3 segment record index for Range request optimization
- S3 share partition state persistence (was stub)
- WebSocket continuation frame handling
- CLI consume: ListOffsets v7 parsing and RecordBatch v2 decoding
- DescribeConfigs: broker config entries returned
- SSRF endpoint validation with IPv4/IPv6 coverage
- S3 path traversal prevention via identifier validation

### Bug Fixes
- CRITICAL: append() offset race condition (per-partition exclusive lock)
- CRITICAL: compaction merge order (indexed results array)
- CRITICAL: S3 key path traversal via unvalidated group_id/topic
- Data races: compactor_running/is_flushing converted to stdatomic
- Config validation bypassed when file missing
- TOML injection in config.save() via unescaped strings
- AWS credentials with '=' silently dropped (split_nth fix)
- CLI parse_cli_args value skip after consuming args[i+1]
- Iceberg metadata filename bounds check
- delete_topic lock scope reduced (S3 I/O outside lock)
- create_topic TOCTOU removed (atomic conditional PUT)

### Performance
- S3 SigV4 signing key cached per UTC day
- buffer_lock changed from Mutex to RwMutex (concurrent fetch)
- url_decode rewritten from O(n*k) to O(n)
- Encoder instances cached globally
- io_uring FdCache with LRU eviction
- topic_id reverse cache for O(1) lookups

### Refactoring
- config.v split into config_types.v, config_identity.v, endpoint_validation.v
- adapter.v split into adapter_topic.v, adapter_record.v, adapter_group.v, adapter_share_partition.v
- s3_client.v split into s3_signing.v (922->658 lines)
- S3StorageAdapter decomposed into 5 sub-structs
- Custom JSON parser replaced with V stdlib json.decode + @[json:] attributes
- XML parser replaced with encoding.xml
- Config save() expanded to serialize all fields
- Dead code removed (CachedSignature, get_env_value, get_config_i64, unused imports)
- Magic numbers replaced with 23 named constants
- Shell commands replaced with file reads (config_identity.v)
- HadoopCatalog DIP fix (ObjectStore interface)
- Typed errors (S3NetworkError, S3ETagMismatchError)
- print_env_mapping rewritten data-driven
- ConfigSource struct reduces parameter count
- S3Config from_storage_config factory

### Architecture
- StoragePort ISP split (6 sub-interfaces + composite)
- StorageSubPorts delegation wrapper for V interface narrowing limitation
- CoordMockStorage simplified (23->4 methods)
- AlterConfigs logs changes instead of silent drop
- S3 auth downgrade warning log added
