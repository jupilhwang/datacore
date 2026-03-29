# DataCore Remaining Issues Plan

Updated: 2026-03-29 | Version: v0.55.0 | Branch: feature/codebase-health-v6

## Session History (v0.50.9 ~ v0.55.0)

| Version | Branch | Summary | Grade |
|---------|--------|---------|:-----:|
| v0.55.0 | feature/codebase-health-v6 | Performance hot-path, DIP violations, typed errors, SCRAM dedup, 86 files +2015/-1347 | - |
| v0.54.1 | feature/codebase-health-v5 | Security fixes: 6 HIGH issues (race condition, SSRF, plaintext passwords, CORS) | - |
| v0.54.0 | feature/codebase-health-v5 | 5 deferred issues resolved, -550 lines JSON migration, ObjectStore ISP | - |
| v0.50.9 | feature/codebase-health-v1 | Error swallowing 17 fixes, dedup -302 lines, architecture cleanup, dead code -289 lines | B (87.8) |
| v0.51.0 | feature/codebase-health-v2 | God Function 8 splits (1,413->404 lines), 4 dead fn deleted | B+ (89.1) |
| v0.52.0 | feature/codebase-health-v3 | Handler DIP (8 concrete->Port), pub removal 624 (38.4%), ISP split | B (84.3) |
| v0.52.1 | feature/codebase-health-v3 | field_* unification 309, SubHandler fields 18 removed, auth warning | B (85.1) |
| v0.53.0 | feature/codebase-health-v4 | 20 remaining issues fixed (P1-P4), 72 files, +3443/-1186 | B+ (87.3) |
| v0.54.0 | feature/codebase-health-v5 | 5 deferred issues resolved, -550 lines JSON migration, ObjectStore ISP | - |

---

## All P1-P4 Items: COMPLETED

No remaining codebase health issues. All 20 items resolved in v0.53.0.

---

## Blocked (external dependency)

| Item | Source | Notes |
|------|--------|-------|
| gRPC HTTP/2 limitation | FUTURE_FEATURES.md | Blocked by V lang |
| CRC32C hardware acceleration | FUTURE_FEATURES.md | Platform-specific |
| parse/encode function further splits | v2 Plan | Protocol-spec dependent |

---

## Completed Items (v0.50.8 ~ v0.54.0)

| Item | Version | Notes |
|------|---------|-------|
| Performance hot-path clone elimination | v0.55.0 | v6 produce/fetch/metadata |
| StreamingServicePorts DIP (gRPC/WS/SSE) | v0.55.0 | v6 Architecture |
| SchemaEncoderPort + factory pattern | v0.55.0 | v6 Architecture |
| IcebergCatalog port migration | v0.55.0 | v6 Architecture |
| RestServer SSE/WS handler ports | v0.55.0 | v6 Architecture |
| SCRAM SHA-256/512 dedup (-85 lines) | v0.55.0 | v6 Refactoring |
| domain.StorageError typed errors | v0.55.0 | v6 Refactoring |
| 15 magic numbers -> named constants | v0.55.0 | v6 Refactoring |
| store_with_auto_create misrouting fix | v0.55.0 | v6 Bug fix (HIGH) |
| S3 Range-request RecordIndex on all paths | v0.54.0 | v5 Deferred #1 |
| topic_id persistent reverse index | v0.54.0 | v5 Deferred #2 |
| JSON parser stdlib migration (-550 lines) | v0.54.0 | v5 Deferred #3 |
| ObjectStore ISP port (Reader/Writer/Full) | v0.54.0 | v5 Deferred #4 |
| Config CAS ETag compare-and-swap | v0.54.0 | v5 Deferred #5 |
| config.v split (969 -> 14 files) | v0.50.8 | S3 Review Phase 2 |
| S3StorageAdapter decomposition (1243 -> 49 files) | v0.50.8 | S3 Review Phase 2 |
| StoragePort ISP (17 methods -> 6 interfaces) | v0.50.8 | S3 Review Phase 2 |
| Error swallowing 17 CRITICAL/HIGH fixes | v0.50.9 | v1 PDCA |
| Domain purity (common/sync/json imports removed) | v0.50.9 | v1 PDCA |
| Unused interfaces removed (LockableStorage, Lock, MessageConsumerPort) | v0.50.9 | v1 PDCA |
| SQLite complete removal (PRD/TRD/code/config) | v0.50.9 | v1 PDCA |
| TelemetryRootConfig tree removal | v0.50.9 | v1 PDCA |
| God Function 8 splits (1,413 -> 404 lines) | v0.51.0 | v2 PDCA |
| Dead function 4 deleted | v0.51.0 | v2 PDCA |
| Handler DIP (8 concrete -> 0, 11 new Port interfaces) | v0.52.0 | v3 PDCA |
| pub fn removal 624 (API surface -38.4%) | v0.52.0 | v3 PDCA |
| BrokerRegistryPort ISP split (query + lifecycle) | v0.52.0 | v3 PDCA |
| handler field_* unification (309 replacements) | v0.52.1 | v3 PDCA |
| SubHandler duplicate fields removed (18) | v0.52.1 | v3 PDCA |
| Makefile -enable-globals removal | v0.52.0 | v3 PDCA |
| PRD/TRD updated to v0.50.9, dedup -1472 lines | v0.50.9 | v1 PDCA |
| ReplicationMetrics race condition fixed (metrics_lock) | v0.50.9 | v1 PDCA |
| read_i*_be silent error fixed (@[inline] + bit-shift) | v0.50.9 | v1 PDCA |
| dSYM cleanup + .gitignore | v0.52.1 | v3 PDCA |
| #1 S3 append() TOCTOU race fix | v0.53.0 | v4 P1 |
| #2 S3 key path traversal validation | v0.53.0 | v4 P1 |
| #3 S3 compaction merge order fix | v0.53.0 | v4 P1 |
| #4 SubHandler dead code deletion (-125 lines) | v0.53.0 | v4 P2 |
| #5 client_ip audit log propagation | v0.53.0 | v4 P2 |
| #6 IoUring close() resource cleanup | v0.53.0 | v4 P2 |
| #7 S3 signing key daily caching | v0.53.0 | v4 P2 |
| #8 S3 atomic flags (stdatomic) | v0.53.0 | v4 P2 |
| #9 TOML injection escape | v0.53.0 | v4 P2 |
| #10 compression DIP restored (port layer) | v0.53.0 | v4 P3 |
| #11 OTLP HTTP actual implementation | v0.53.0 | v4 P3 |
| #12 TransactionStore S3/Postgres | v0.53.0 | v4 P3 |
| #13 S3 buffer_lock RwMutex | v0.53.0 | v4 P3 |
| #14 S3 function decomposition (16 fns) | v0.53.0 | v4 P3 |
| #15 DMA fallback real POSIX I/O | v0.53.0 | v4 P3 |
| #16 Duplicate groups dedup (CRC32-IEEE, Reader, binary) | v0.53.0 | v4 P3 |
| #17 observability.field_ unified (89 occurrences) | v0.53.0 | v4 P4 |
| #18 WebSocket JSON (json.decode) | v0.53.0 | v4 P4 |
| #19 CLI topic partition details | v0.53.0 | v4 P4 |
| #20 streaming glob_match() | v0.53.0 | v4 P4 |
