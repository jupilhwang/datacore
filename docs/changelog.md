# DataCore Changelog

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
