# DataCore Remaining Issues Plan

Updated: 2026-03-25 | Version: v0.52.1 | Branch: main

## Session History (v0.50.9 ~ v0.52.1)

| Version | Branch | Summary | Grade |
|---------|--------|---------|:-----:|
| v0.50.9 | feature/codebase-health-v1 | Error swallowing 17 fixes, dedup -302 lines, architecture cleanup, dead code -289 lines | B (87.8) |
| v0.51.0 | feature/codebase-health-v2 | God Function 8 splits (1,413->404 lines), 4 dead fn deleted | B+ (89.1) |
| v0.52.0 | feature/codebase-health-v3 | Handler DIP (8 concrete->Port), pub removal 624 (38.4%), ISP split | B (84.3) |
| v0.52.1 | feature/codebase-health-v3 | field_* unification 309, SubHandler fields 18 removed, auth warning | B (85.1) |

---

## P1 -- CRITICAL/BLOCK (3 items)

| # | Issue | Source | Size | Est |
|---|-------|--------|:----:|:---:|
| 1 | S3 `append()` concurrency race -- partition-level exclusive lock needed | S3 Review CRIT-1 | L | 2h |
| 2 | S3 `group_id/topic` key path traversal -- `validate_identifier()` needed | S3 Review CRIT-3 | M | 2h |
| 3 | S3 Compaction merge order -- channel -> indexed results array | S3 Review CRIT-2 | S | 1h |

### Detail

**#1 append() concurrency race**
- File: `src/infra/storage/plugins/s3/adapter.v`
- Problem: high_watermark read-write gap under concurrent appends to same partition
- Fix: introduce partition-level exclusive lock, protect read-modify-write atomically
- Test: concurrent append unit test (2 goroutines, same partition)

**#2 group_id/topic key validation**
- Files: `src/infra/storage/plugins/s3/adapter.v` + 6 related functions
- Problem: S3 key constructed from user-supplied group_id/topic without validation
- Fix: `validate_identifier()` rejecting `/`, `..`, null byte, non-printable chars
- Test: path traversal attempt test (group_id = "../../topics/evil")

**#3 Compaction merge order**
- File: `src/infra/storage/plugins/s3/compaction.v:227-263`
- Problem: channel-based collection doesn't guarantee segment order after parallel download
- Fix: indexed results array `mut results := [][]u8{len: segments.len}`
- Test: 3-segment parallel download order verification

---

## P2 -- HIGH (6 items)

| # | Issue | Source | Size | Est |
|---|-------|--------|:----:|:---:|
| 4 | SubHandler dead code -- 8 fields + 7 _ctx.v files + HandlerContext deletion | v3 Review | S | 1h |
| 5 | `client_ip` audit log -- empty string passed in SASL handler (3 sites) | v3 Review | M | 2h |
| 6 | IoUring `close()` resource cleanup not implemented | TODO | S | 1h |
| 7 | S3 signing key daily caching (HMAC recomputed per request) | S3 Review CRIT-7 | M | 2h |
| 8 | S3 `compactor_running/is_flushing` not atomic (race condition) | S3 Review HIGH-2 | S | 1h |
| 9 | `config.save()` TOML injection prevention | S3 Review HIGH-5 | S | 1h |

### Detail

**#4 SubHandler dead code**
- Handler struct (handler.v:44-50): 7 empty SubHandler pointers + handler_ctx, all allocated but never accessed
- 7 _ctx.v files: each contains only empty struct definition
- HandlerContext (handler_context.v): allocated in factory but never read
- Fix: remove 8 fields from Handler, delete 7 _ctx.v files + handler_context.v, simplify handler_factory.v
- Expected: -120 lines, 9 files deleted, Handler fields 21->13

**#5 client_ip audit log**
- File: `src/infra/protocol/kafka/handler_sasl.v:233,261,285`
- Problem: `al.log_auth_failure('', ...)` -- empty string for client_ip
- Fix: propagate client address from TCP connection through RequestHandler interface or handler context
- Impact: audit forensics (failed login tracking, brute-force detection)

**#6 IoUring close() cleanup**
- File: `src/infra/performance/engines/linux.v:256`
- Problem: TODO comment noting io_uring resources not properly cleaned up on close

**#7 S3 signing key caching**
- File: `src/infra/storage/plugins/s3/s3_client.v:514-576`
- Fix: CachedSigningKey struct with date_day-based caching

**#8 S3 atomic flags**
- File: `src/infra/storage/plugins/s3/adapter.v:99,104,1170-1185`
- Fix: plain bool -> stdatomic.load_i32/store_i32, CAS on start_workers

**#9 TOML injection**
- File: `src/config/config_save.v`
- Fix: escape TOML special characters in string values

---

## P3 -- MEDIUM (7 items)

| # | Issue | Source | Size | Est |
|---|-------|--------|:----:|:---:|
| 10 | handler_fetch/produce_helpers compression DIP bypass | v3 Review | M | 2h |
| 11 | OTLP HTTP stub -- `send_otlp_http()` is no-op | v1 Exploration | M | 3h |
| 12 | TransactionStore S3/Postgres not implemented (memory-only) | v1 Exploration | L | 4h |
| 13 | S3 `buffer_lock` Mutex -> RwMutex (fetch is read-heavy) | S3 Review HIGH-7 | S | 1h |
| 14 | S3 large function decomposition (8 functions, 97-109 lines each) | S3 Review HIGH-12 | M | 3h |
| 15 | DMA fallback not implemented (sendfile/scatter/gather return 0) | v1 Exploration | M | 3h |
| 16 | Remaining 4 duplicate groups (Reader struct, CRC32-IEEE, mmap inline, binary_helpers) | v1 Exploration | M | 2h |

### Detail

**#10 compression DIP bypass**
- Files: handler_fetch.v, handler_produce_helpers.v
- Problem: directly import `infra.compression` for CompressionType enum and conversion functions
- Fix: route through port.CompressionPort or add compression type constants to port layer

**#11 OTLP HTTP stub**
- File: `src/infra/observability/logger.v:738-750`
- Problem: `send_otlp_http()` ignores endpoint and payload (assigns to `_`)
- Fix: implement actual HTTP POST with retry logic

**#12 TransactionStore persistence**
- Problem: `MemoryTransactionStore` only implementation. Broker restart loses all transaction metadata
- Fix: implement S3TransactionStore and PostgresTransactionStore

**#13 S3 buffer_lock RwMutex**
- Fix: fetch() uses rlock(), write path uses @lock()

**#14 S3 function decomposition**
- Target: append() 109 lines, fetch() 97 lines, parse_s3_config() 109 lines + 5 more
- Goal: each <= 50 lines

**#15 DMA fallback**
- File: `src/infra/performance/sysio/dma.v`
- Problem: sendfile_fallback, scatter_read_fallback, gather_write_fallback all return 0 bytes transferred
- Fix: implement actual data copy using standard I/O as fallback

**#16 Remaining duplicates**
- Reader structs (BinaryReader, ProtoReader, AvroReader) -- common ByteReader extraction
- CRC32-IEEE in utils.v -- move to common
- mmap inline binary writes -- use common.write_i32_be
- replication binary_helpers -- consolidate with common

---

## P4 -- LOW (4 items)

| # | Issue | Source | Size | Est |
|---|-------|--------|:----:|:---:|
| 17 | `observability.field_*` non-handler residual (89 occurrences, 11 files) | v3 Review | M | 2h |
| 18 | TODO: WebSocket JSON library replacement | TODO | M | 3h |
| 19 | TODO: CLI topic partition details | TODO | S | 1h |
| 20 | TODO: streaming glob/regex implementation | TODO | S | 1h |

---

## Deferred (separate planning needed)

These items require new feature development, not codebase health improvements:

| Item | Source | Notes |
|------|--------|-------|
| S3 Range-request random-access fetch | S3 Review CRIT-8 | New feature, 6h |
| S3 topic_id reverse index | S3 Review CRIT-9 | New feature, 4h |
| Custom JSON parser -> stdlib replacement | S3 Review HIGH-16 | Risky, 4h |
| ObjectStore interface introduction | S3 Review HIGH-15 | Architecture, 2h |
| S3 Config 3-way replication merge | S3 Review CRIT-10 | Complex, 3h |
| gRPC HTTP/2 limitation | FUTURE_FEATURES.md | Blocked by V lang |
| CRC32C hardware acceleration | FUTURE_FEATURES.md | Platform-specific |
| parse/encode function further splits | v2 Plan | Protocol-spec dependent |

---

## Recommended Execution Order

```
v4 Session 1 (P1+P2): S3 Security/Stability + Dead Code Cleanup
  [G1] #1 append() lock + #2 key validation + #3 compaction order  (parallel)
  [G2] #4 SubHandler delete + #5 client_ip + #6 IoUring cleanup    (parallel)
  [G3] #7 S3 signing cache + #8 atomic flags + #9 TOML injection   (parallel)

v4 Session 2 (P3): Architecture + Feature Gaps
  [G1] #10 compression DIP + #13 RwMutex + #14 S3 function splits  (parallel)
  [G2] #11 OTLP HTTP + #15 DMA fallback + #16 duplicate groups     (parallel)
  [G3] #12 TransactionStore S3/Postgres                             (solo -- large)

v4 Session 3 (P4): Polish
  [G1] #17 observability.field_ + #18 WebSocket JSON + #19 CLI + #20 glob/regex
```

## Completed Items (v0.50.8 ~ v0.52.1)

| Item | Version | Notes |
|------|---------|-------|
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
