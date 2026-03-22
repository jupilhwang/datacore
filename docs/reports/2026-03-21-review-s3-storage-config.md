# Code Review Report: S3 Storage + Config

**Date:** 2026-03-21
**Target:** `src/config/config.v`, `src/infra/storage/plugins/s3/adapter.v`, `src/infra/storage/plugins/s3/iceberg_catalog.v`
**Mode:** Full (6-Panel Analysis)
**Reviewer:** Code Review Panel v1.0

---

## 1. Executive Summary

- **Overall Grade:** F (57.3 points)
- **Verdict:** BLOCK (Security CRITICAL exists + score < 65)

**Summary:** The codebase demonstrates solid architectural patterns (ETag concurrency, buffer-flush-compact pipeline) but suffers from 3 CRITICAL bugs, 1 CRITICAL security vulnerability, 3 CRITICAL architecture violations, and 3 CRITICAL performance issues. God Class (S3StorageAdapter) and God File (config.v) patterns compound all other issues. Concurrent append race condition risks silent data corruption.

---

## 2. Score Dashboard

| Panel | Score | Weight | Weighted Score | Grade |
|-------|-------|--------|----------------|-------|
| Code Refactor | 48 | x1.0 | 48.0 | F |
| Bug Hunter | 55 | x1.3 | 71.5 | F |
| Security Auditor | 58 | x1.5 | 87.0 | F |
| Performance Analyst | 68 | x1.0 | 68.0 | D |
| Structure Critic | 52 | x1.0 | 52.0 | F |
| Architecture Reviewer | 62 | x1.2 | 74.4 | D |
| **Overall** | | | **57.3** | **F** |

**Grading Scale:** A (90-100), B (80-89), C (70-79), D (65-69), F (<65)

---

## 3. Critical Issues

10 total issues at CRITICAL severity.

---

### [CRITICAL] CRIT-1: Race condition in append() -- concurrent offset assignment

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/adapter.v:533-625`
- **Description:** `append()` reads `index.high_watermark` via rlock (shared read) to determine `base_offset`, but does NOT hold exclusive lock between reading offset and updating it. Concurrent appends read the same high_watermark, create overlapping offsets, and the last writer wins -- causing offset regression and data loss.
- **Impact:** Silent data corruption in concurrent produce. Lost records. Offset regression.
- **Fix:** Per-partition exclusive lock spanning offset read through write. Or atomic compare-and-swap on cached high_watermark.

---

### [CRITICAL] CRIT-2: Non-deterministic segment merge order in compaction

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/compaction.v:227-263`
- **Description:** `download_segments_parallel()` spawns concurrent goroutines. Results collected from channel in arrival order (non-deterministic), NOT segment order. Merged data contains records in random order.
- **Impact:** Records within merged segment stored out of offset order. Sequential consumers produce incorrect results.
- **Fix:** Use indexed results array: `mut results := [][]u8{len: segments.len, init: []u8{}}` preserving segment position.

---

### [CRITICAL] CRIT-3: Path traversal via unvalidated group_id in S3 key construction

- **Panel:** Security Auditor
- **Location:** `src/infra/storage/plugins/s3/adapter.v:1150-1158`
- **Description:** `group_key()`, `offset_key()` directly interpolate `group_id` into S3 paths without sanitization. While `create_topic()` validates topic names, `group_id` is NEVER validated. Malicious client can send JoinGroup with `group_id = "../../topics/admin-topic/metadata"`.
- **Impact:** S3 object read/write/delete outside intended prefix. Cross-tenant data access. Data corruption.
- **Fix:** Add `validate_identifier()` rejecting `/`, `..`, null bytes, non-printable chars. Apply in save_group, load_group, commit_offsets, fetch_offsets.

---

### [CRITICAL] CRIT-4: config.v God File with 5+ responsibilities (SRP violation)

- **Panel:** Structure Critic
- **Location:** `src/config/config.v` (1150 lines)
- **Description:** Single file handles: type definitions (17 structs), TOML parsing (7 functions), priority cascade resolution, CLI arg parsing, config persistence, validation, network identity detection (MAC/IP), hash/ID generation, env mapping display.
- **Fix:** Split into config_types.v, config_parser.v, config_validate.v, broker_identity.v.

---

### [CRITICAL] CRIT-5: S3StorageAdapter God Class (10+ concerns)

- **Panel:** Architecture Reviewer
- **Location:** `src/infra/storage/plugins/s3/adapter.v` (1243 lines, 24+ mutable fields)
- **Description:** Handles topic CRUD, record append/fetch, consumer groups, offset management, metrics, Iceberg integration, sync linger, health checks, worker lifecycle. 10+ maps, 7+ locks.
- **Fix:** Decompose into S3TopicAdapter, S3RecordAdapter, S3GroupAdapter, S3MetricsCollector, S3IcebergAdapter with shared S3Client.

---

### [CRITICAL] CRIT-6: StoragePort ISP violation (17 methods + 4 stubs)

- **Panel:** Architecture Reviewer
- **Location:** `src/service/port/storage_port.v:9-80`
- **Description:** 17-method interface. S3 adapter has 4 SharePartition stubs returning errors. Clients forced to depend on unused methods.
- **Fix:** Split into TopicStoragePort, RecordStoragePort, GroupStoragePort, OffsetStoragePort, SharePartitionPort.

---

### [CRITICAL] CRIT-7: S3 signing key recomputed every request

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/s3_client.v:514-576`
- **Description:** Full HMAC signing key chain (4 HMAC-SHA256) computed on every S3 API call. Keys valid per UTC day.
- **Impact:** ~4000 unnecessary HMAC computations/sec at 1000 req/s.
- **Fix:** Cache `k_signing` with `date_day` key. Recompute only when UTC date changes.

---

### [CRITICAL] CRIT-8: Full segment download for random-access fetch

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/adapter.v:669-674`
- **Description:** When offset != seg.start_offset, entire segment downloaded and fully deserialized even for 1-2 records. O(segment_size) I/O.
- **Fix:** In-segment offset index + S3 Range requests + binary search.

---

### [CRITICAL] CRIT-9: get_topic_by_id cache miss triggers O(T) S3 calls

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/adapter.v:459`
- **Description:** On cache miss, lists ALL S3 objects under topics/ prefix, GETs each uncached topic. For 1000 topics = ~1001 S3 API calls.
- **Fix:** Store reverse mapping topic_id -> topic_name as persistent S3 index.

---

### [CRITICAL] CRIT-10: Config duplication across 3 structs

- **Panel:** Architecture Reviewer
- **Location:** config.v:170-213, adapter.v:43-79, iceberg_types.v:255-266
- **Description:** S3StorageConfig, S3Config, IcebergConfig have overlapping fields with manual mapping.
- **Fix:** Single source of truth or shared config interface.

---

## 4. High Issues

16 total issues at HIGH severity.

---

### [HIGH] HIGH-1: Config validation bypassed when file missing

- **Panel:** Bug Hunter
- **Location:** `src/config/config.v:354-374`
- **Description:** When config file is missing, the loader silently applies defaults and skips all validation checks. Invalid combinations of defaults (e.g., S3 storage without endpoint) pass through unchecked.
- **Fix:** Run validate() unconditionally after config load, regardless of source (file, defaults, or environment).

---

### [HIGH] HIGH-2: compactor_running/is_flushing data races

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/adapter.v:99,104,1170-1185`
- **Description:** `compactor_running` and `is_flushing` are plain `bool` fields read and written from multiple threads without synchronization. This is a classic data race.
- **Fix:** Replace with `atomic.Bool` or `Atomic(bool)` for lock-free thread-safe access.

---

### [HIGH] HIGH-3: delete_topic holds write lock during S3 I/O

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/adapter.v:353-368`
- **Description:** Write lock on `topics_lock` held during S3 delete operations (network I/O). All other topic operations (create, list, append) are blocked for the duration of the S3 round-trip.
- **Fix:** Copy required data under lock, release lock, then perform S3 I/O, then re-acquire lock to update local state.

---

### [HIGH] HIGH-4: start_workers check-then-act race

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/adapter.v:1169-1179`
- **Description:** `start_workers()` checks `compactor_running` then sets it in a non-atomic sequence. Two concurrent calls can both see `false` and spawn duplicate worker goroutines.
- **Fix:** Use atomic compare-and-swap: `if !compactor_running.compare_and_swap(false, true) { return }`.

---

### [HIGH] HIGH-5: TOML injection in config.save()

- **Panel:** Security Auditor
- **Location:** `src/config/config.v:829-863`
- **Description:** `save()` serializes config values into TOML format using string interpolation without escaping special TOML characters (quotes, newlines, backslashes). A config value containing `"` or `\n` can corrupt the config file or inject arbitrary TOML keys.
- **Fix:** Escape TOML special characters (quote, backslash, newline, tab) in all string values before interpolation, or use a proper TOML serialization library.

---

### [HIGH] HIGH-6: Silent auth downgrade when S3 creds empty

- **Panel:** Security Auditor
- **Location:** `src/infra/storage/plugins/s3/s3_client.v:536-538`
- **Description:** When access_key or secret_key is empty, the S3 client silently falls through to unsigned requests instead of failing. This creates a silent security downgrade where requests may succeed against misconfigured buckets without authentication.
- **Fix:** Log a WARNING-level message when credentials are empty. In production mode, fail-fast with an explicit error rather than sending unsigned requests.

---

### [HIGH] HIGH-7: buffer_lock Mutex serializes reads

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/adapter.v:701`
- **Description:** `buffer_lock` is a `sync.Mutex`, which means all buffer reads (fetch operations) are serialized with writes (append operations). Under read-heavy workloads, this creates unnecessary contention.
- **Fix:** Change `buffer_lock` from `sync.Mutex` to `sync.RwMutex`. Use `rlock()` for read paths and `lock()` only for write paths.

---

### [HIGH] HIGH-8: metrics_lock acquired 6x per flush

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/buffer_manager.v:215-291`
- **Description:** Each flush cycle acquires and releases `metrics_lock` 6 separate times to update individual counters. Lock acquisition overhead is multiplied across high-frequency flush operations.
- **Fix:** Use `Atomic(i64)` for individual counters, or batch all 6 metric updates into a single lock-protected block.

---

### [HIGH] HIGH-9: O(n^2) url_decode string rebuilding

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/s3_client.v:653-669`
- **Description:** `url_decode()` builds the result string by repeated concatenation (`result += char`), which is O(n^2) for n-length strings due to repeated memory allocation and copying.
- **Fix:** Use `strings.Builder` for O(n) string construction.

---

### [HIGH] HIGH-10: Iceberg cache eviction O(n)

- **Panel:** Performance Analyst
- **Location:** `src/infra/storage/plugins/s3/iceberg_catalog.v:50-56`
- **Description:** Cache eviction iterates over all entries to find the oldest item. For large caches, this becomes a performance bottleneck on every cache insertion that triggers eviction.
- **Fix:** Implement LRU eviction with a doubly-linked list, or use batch eviction (remove oldest 10% when threshold reached).

---

### [HIGH] HIGH-11: All 3 files exceed 300-line limit

- **Panel:** Code Refactor
- **Location:** `config.v` (1150 lines), `adapter.v` (1243 lines), `iceberg_catalog.v` (482 lines)
- **Description:** All three target files exceed the project's 300-line hard limit for file size. This is a HIGH-severity code smell per project coding standards.
- **Fix:** Split each file per the recommendations in CRIT-4, CRIT-5, and the iceberg_catalog decomposition.

---

### [HIGH] HIGH-12: 8+ functions exceed 50-line limit

- **Panel:** Code Refactor
- **Location:** Multiple functions across all 3 files
- **Description:** At least 8 functions exceed the 50-line hard limit: `append()`, `fetch()`, `flush()`, `compact()`, `load_config()`, `parse_cli_args()`, `save()`, `create_or_load_table()`. This violates the project's code smell detection rules.
- **Fix:** Extract helper functions for distinct logical sections within each oversized function.

---

### [HIGH] HIGH-13: Dead code: CachedSignature, get_env_value, get_config_i64, 4 unused imports

- **Panel:** Code Refactor
- **Location:** Multiple locations across target files
- **Description:** `CachedSignature` struct is defined but never used. `get_env_value()` and `get_config_i64()` functions are unreferenced. 4 import statements reference modules that are not used in the importing file.
- **Fix:** Remove all dead code and unused imports. Verify with `v vet` after removal.

---

### [HIGH] HIGH-14: get_config_* functions take 6 parameters each

- **Panel:** Code Refactor
- **Location:** `src/config/config.v:711-800`
- **Description:** The family of `get_config_string()`, `get_config_int()`, `get_config_bool()`, etc. functions each take 6 parameters (toml_value, env_key, cli_key, default, cli_args, env_map). This makes call sites verbose and error-prone.
- **Fix:** Create a `ConfigSource` struct bundling toml_value, env_key, cli_key, and default. Pass a single struct instead of 6 individual parameters.

---

### [HIGH] HIGH-15: HadoopCatalog DIP violation, concrete S3StorageAdapter

- **Panel:** Structure Critic
- **Location:** `src/infra/storage/plugins/s3/iceberg_catalog.v:36`
- **Description:** `HadoopCatalog` directly depends on concrete `S3StorageAdapter` type instead of an abstraction. This violates the Dependency Inversion Principle and makes the catalog untestable without a full S3 adapter.
- **Fix:** Define an `ObjectStore` interface with the minimal methods HadoopCatalog needs (get, put, list, delete). Inject the interface instead of the concrete adapter.

---

### [HIGH] HIGH-16: Custom JSON parser instead of std lib

- **Panel:** Architecture Reviewer
- **Location:** `src/infra/storage/plugins/s3/iceberg_catalog_json.v`
- **Description:** A hand-rolled JSON parser is used for Iceberg metadata instead of V's standard library `json.decode()`. This introduces unnecessary maintenance burden, potential parsing bugs, and inconsistency with the rest of the codebase.
- **Fix:** Replace custom parser with `json.decode()` using properly annotated V structs. If std lib limitations exist, document them explicitly.

---

## 5. Medium Issues

14 total issues at MEDIUM severity.

---

### [MEDIUM] MED-1: DRY violation in 4 get_config_* functions

- **Panel:** Code Refactor
- **Location:** `src/config/config.v:711-800`
- **Description:** `get_config_string`, `get_config_int`, `get_config_bool`, `get_config_i64` share near-identical logic for priority resolution (CLI > env > TOML > default) with only the type conversion differing.
- **Fix:** Extract a generic `get_config_value[T]()` or a common `resolve_priority()` returning a string, with type-specific wrappers for conversion only.

---

### [MEDIUM] MED-2: Cache invalidation pattern repeated in HadoopCatalog

- **Panel:** Code Refactor
- **Location:** `src/infra/storage/plugins/s3/iceberg_catalog.v:192-215`
- **Description:** The cache-check, cache-miss-load, cache-store pattern is duplicated across multiple methods (load_table, get_current_snapshot, list_tables). Each repeats the same lock-check-load-store sequence.
- **Fix:** Extract a `cache_get_or_load(key, loader_fn)` helper that encapsulates the pattern.

---

### [MEDIUM] MED-3: print_env_mapping hardcoded println wall

- **Panel:** Code Refactor
- **Location:** `src/config/config.v:996-1051`
- **Description:** `print_env_mapping()` contains ~55 hardcoded `println()` statements, one per config key. Adding a new config field requires adding another println line.
- **Fix:** Generate the mapping from a data structure (array of structs or map) and iterate to print.

---

### [MEDIUM] MED-4: max_memory_mb = 20240 looks like typo for 20480

- **Panel:** Code Refactor
- **Location:** `src/config/config.v:145`
- **Description:** Default value `max_memory_mb = 20240` is suspiciously close to 20480 (20 * 1024), which would be the expected round binary value for 20 GB.
- **Fix:** Verify intended value. If 20 GB was intended, change to 20480. Add a comment explaining the chosen default.

---

### [MEDIUM] MED-5: Magic numbers in config defaults

- **Panel:** Code Refactor
- **Location:** `src/config/config.v` (multiple locations)
- **Description:** Default values like 104857600, 1048576, 32768 appear as raw numbers without named constants. Readers must mentally decode that 104857600 = 100 MB.
- **Fix:** Define named constants: `const default_segment_size = 100 * 1024 * 1024` and reference by name.

---

### [MEDIUM] MED-6: save() serializes incomplete config subset

- **Panel:** Code Refactor
- **Location:** `src/config/config.v:829-864`
- **Description:** `save()` only serializes a subset of config fields to TOML. Fields added later may be silently dropped, causing config drift between loaded and saved state.
- **Fix:** Generate the save output from the same struct definitions used for loading, ensuring all fields round-trip.

---

### [MEDIUM] MED-7: AWS creds with = silently dropped; split vs split_nth

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/s3_credentials.v:52-53`
- **Description:** Credential file parsing uses `split('=')` which breaks values containing `=` characters (common in base64-encoded keys). The value after the first `=` is silently truncated.
- **Fix:** Use `split_nth('=', 2)` to split on only the first `=`, preserving the full value.

---

### [MEDIUM] MED-8: stop_workers doesn't wait for completion

- **Panel:** Bug Hunter
- **Location:** `src/infra/storage/plugins/s3/adapter.v:1183-1185`
- **Description:** `stop_workers()` sets flags to signal workers to stop but does not wait for goroutines to actually finish. Subsequent operations (e.g., close, cleanup) may race with still-running workers.
- **Fix:** Use a `sync.WaitGroup` or channel-based shutdown to wait for all worker goroutines to complete before returning.

---

### [MEDIUM] MED-9: parse_cli_args doesn't skip consumed value

- **Panel:** Bug Hunter
- **Location:** `src/config/config.v:816-821`
- **Description:** When parsing `--key value` style CLI arguments, the parser consumes the key but does not advance the index past the value. The value is then re-parsed as a potential key in the next iteration.
- **Fix:** Increment the loop index after consuming a value argument to skip it in the next iteration.

---

### [MEDIUM] MED-10: SSRF potential via user-controlled S3 endpoint

- **Panel:** Security Auditor
- **Location:** `src/config/config.v:440`
- **Description:** The S3 endpoint URL is configurable and passed directly to the HTTP client without validation. A malicious configuration could point to internal network addresses (169.254.169.254, localhost, internal services).
- **Fix:** Validate the endpoint URL against a blocklist of internal/reserved IP ranges. In production, restrict to known S3-compatible endpoints or require explicit allowlisting.

---

### [MEDIUM] MED-11: Unbounded memory in list_objects

- **Panel:** Security Auditor
- **Location:** `src/infra/storage/plugins/s3/s3_client.v:391-406`
- **Description:** `list_objects()` accumulates all results in memory with no upper bound. A bucket with millions of objects (or a malicious S3-compatible endpoint) can cause OOM.
- **Fix:** Add a configurable max_results limit. Use pagination with early termination when the limit is reached.

---

### [MEDIUM] MED-12: Topic name allows `.` enabling S3 ambiguity

- **Panel:** Security Auditor
- **Location:** `src/infra/storage/plugins/s3/adapter.v:286`
- **Description:** Topic name validation allows `.` characters. Topic names like `my.topic` and `my/topic` can create ambiguous S3 key paths, especially combined with prefix-based listing operations.
- **Fix:** Restrict topic names to `[a-zA-Z0-9_-]` or explicitly document and handle `.` in S3 key construction with proper escaping.

---

### [MEDIUM] MED-13: config_test.v minimal coverage

- **Panel:** Structure Critic
- **Location:** `src/config/config_test.v`
- **Description:** Test file has minimal coverage. Priority cascade (CLI > env > TOML > default), validation edge cases, network identity detection, and config persistence are not tested.
- **Fix:** Add test cases for each priority level, invalid configurations, boundary values, and round-trip save/load.

---

### [MEDIUM] MED-14: Inconsistent config override support

- **Panel:** Architecture Reviewer
- **Location:** `src/config/config.v:405-422`
- **Description:** Not all config fields support the full priority cascade (CLI > env > TOML > default). Some fields only support TOML + default, while others support all four sources. This inconsistency is not documented.
- **Fix:** Ensure all config fields support the full cascade, or explicitly document which fields support which override levels and why.

---

## 6. Low Issues

| # | Panel | Location | Description |
|---|-------|----------|-------------|
| LOW-1 | Refactor | config.v:119 | `static_dir` default `'tests/web'` fails in production deployments where test directories are not present. Should default to a production-appropriate path. |
| LOW-2 | Refactor | config.v:278 | `service_version '0.44.4'` hardcoded in OtelConfig. Should read from `v.mod` to stay in sync with actual version. |
| LOW-3 | Refactor | adapter.v:1174 | Uses deprecated `go` keyword instead of `spawn` for goroutine creation. `go` is deprecated in current V versions. |
| LOW-4 | Refactor | config_test.v:26-41 | Priority cascade test does not actually test the cascade. It sets one source and checks the result, but never tests that CLI overrides env overrides TOML. |
| LOW-5 | Bug | adapter.v:524-531 | Empty records array returns misleading `base_offset: 0` instead of an error or explicit empty result, which can confuse consumers. |
| LOW-6 | Bug | adapter.v:1011 | `sync_append_total_ms` truncated from `i64` to `int`, losing precision for large values (>2B ms = ~24 days cumulative). |
| LOW-7 | Bug | buffer_manager.v:130-147 | `flush_worker` does not drain remaining buffers on shutdown. Buffered records can be lost during graceful stop. |
| LOW-8 | Security | s3_client.v:183-190 | S3 endpoint URLs, bucket names, and key prefixes leaked in retry error log messages. Infrastructure details should not appear in logs accessible to operators. |
| LOW-9 | Security | config.v:240 | PostgreSQL `sslmode` defaults to `'disable'`, transmitting credentials and data in plaintext. Should default to `'require'` or `'verify-full'`. |
| LOW-10 | Security | adapter.v:1219-1222 | MD5 used for topic ID generation. While not a security hash in this context, MD5 has weak collision resistance. Consider xxHash or FNV for non-crypto hashing. |
| LOW-11 | Perf | adapter.v:523-632 | Multiple separate lock acquire/release round-trips within a single `append()` call. Could be consolidated to reduce lock overhead. |
| LOW-12 | Arch | config.v:278 | Version string hardcoded in OtelConfig duplicates the version already declared in `v.mod`. Single source of truth violated. |
| LOW-13 | Arch | config.v:28-62,252-340 | Telemetry and observability configuration split across two non-adjacent sections of the same file with overlapping concerns. |
| LOW-14 | Arch | adapter.v:1226-1243 | SharePartition stub methods reference placeholder `jira#XXX` tickets that do not exist. Stubs should reference real tracking issues or be removed behind a feature flag. |

---

## 7. Cross-Dimension Insights

### Insight 1: God Class/File Pattern as Root Cause

- **Related Panels:** Code Refactor, Structure Critic, Architecture Reviewer, Performance Analyst
- **Root Cause:** S3StorageAdapter (1243 lines, 24+ fields) and config.v (1150 lines) concentrate too many responsibilities in single units.
- **Impact:** Amplifies every other finding -- lock contention (Performance), race conditions (Bug), maintenance risk (Refactor). Each new feature added to these units increases the blast radius of all existing issues.
- **Recommendation:** Decomposition is the highest-ROI fix addressing 60%+ of all findings. Start with S3StorageAdapter (CRIT-5) as it has the most downstream impact on concurrency and performance issues.

### Insight 2: Missing Input Validation Layer

- **Related Panels:** Security Auditor, Bug Hunter
- **Root Cause:** No centralized input validation for S3 key construction parameters. Validation exists for some inputs (topic names) but not others (group_id, config values).
- **Impact:** Path traversal (CRIT-3), TOML injection (HIGH-5), topic name ambiguity (MED-12), credential parsing errors (MED-7).
- **Recommendation:** Create a `validate` module with `validate_identifier()`, `validate_path_component()`, and `validate_config_value()` functions. Apply at all external input boundaries.

### Insight 3: Concurrency Model Inconsistencies

- **Related Panels:** Bug Hunter, Performance Analyst
- **Root Cause:** Mixed use of `sync.Mutex` vs `sync.RwMutex`, plain `bool` vs atomic types, deprecated `go` vs `spawn`. No documented concurrency guidelines.
- **Impact:** Data races (CRIT-1, HIGH-2), serialized reads (HIGH-7), check-then-act races (HIGH-4), deprecated API usage (LOW-3).
- **Recommendation:** Establish project concurrency guidelines: RwMutex for read-heavy shared state, atomic types for simple flags, `spawn` for all new goroutines. Audit all existing concurrency primitives against the guidelines.

---

## 8. Action Items

| Priority | Item | Related Issue | Effort | Assignee |
|----------|------|---------------|--------|----------|
| Immediate | Per-partition exclusive lock in append() | CRIT-1 | 2h | @dev |
| Immediate | Validate group_id/topic in S3 key paths | CRIT-3 | 2h | @dev |
| Immediate | Indexed results in compaction download | CRIT-2 | 1h | @dev |
| Immediate | Split config.v into 3-4 files | CRIT-4 | 4h | @dev |
| Immediate | Decompose S3StorageAdapter | CRIT-5 | 8h | @dev |
| Immediate | Split StoragePort interface | CRIT-6 | 4h | @dev |
| Before Merge | Cache S3 signing key per UTC day | CRIT-7 | 2h | @dev |
| Before Merge | Add atomics for compactor_running/is_flushing | HIGH-2 | 1h | @dev |
| Before Merge | Fix TOML save injection | HIGH-5 | 1h | @dev |
| Before Merge | Remove dead code | HIGH-13 | 1h | @dev |
| Before Merge | Change buffer_lock to RwMutex | HIGH-7 | 1h | @dev |
| Before Merge | Add config validation in no-file path | HIGH-1 | 0.5h | @dev |
| Later | Range-request for random-access fetch | CRIT-8 | 6h | @dev |
| Later | Reverse topic_id index | CRIT-9 | 4h | @dev |
| Later | Replace custom JSON parser with std lib | HIGH-16 | 4h | @dev |
| Later | Introduce ObjectStore interface for DIP | HIGH-15 | 2h | @dev |

---

Report generation complete: 2026-03-21 | File: docs/reports/2026-03-21-review-s3-storage-config.md

---

## Post-Fix Score (Phase 1-8 Complete)

| Panel | Initial | Final | Delta | Weight | Weighted |
|-------|---------|-------|-------|--------|----------|
| Code Refactor | 48 | 82 | +34 | x1.0 | 82.0 |
| Bug Hunter | 55 | 87 | +32 | x1.3 | 113.1 |
| Security Auditor | 58 | 80 | +22 | x1.5 | 120.0 |
| Performance Analyst | 68 | 82 | +14 | x1.0 | 82.0 |
| Structure Critic | 52 | 81 | +29 | x1.0 | 81.0 |
| Architecture Reviewer | 62 | 80 | +18 | x1.2 | 96.0 |
| **Weighted Average** | **57.3** | **82.0** | **+24.7** | /7.0 | **574.1/7.0** |

**Grade: F (57.3) -> B (82.0)**
**Verdict: BLOCK -> APPROVE** (0 security/correctness CRITICALs, grade B)

### Fix Summary (8 Phases, 7 Commits)

| Phase | Focus | Key Changes |
|-------|-------|-------------|
| 1 | CRITICAL bugs | append() race fix, compaction order, path traversal validation |
| 2 | File decomposition | config.v split (3 files), adapter.v split (4 files) |
| 3 | HIGH issues | Atomic flags + WaitGroup, TOML escape, signing key cache, dead code removal |
| 4 | MEDIUM bugs + refactor | AWS creds fix, CLI args fix, bounds check, named constants, RwMutex |
| 5 | Bug + function decomp | Config validation on no-file path, topic_by_id reverse cache, append/fetch decomposition |
| 6 | DIP + security + perf | ObjectStore interface, auth warning log, O(n) url_decode, encode/decode decomposition |
| 7 | Security + arch | SSRF endpoint validation, typed error structs, data-driven env mapping |
| 8 | Structure + test | ConfigSource struct (31 sites), S3Config factory, circular dep fix, 7 new tests |

### Remaining Items (Separate PRs Recommended)

| Priority | Item | Reason for Deferral |
|----------|------|---------------------|
| HIGH | StoragePort ISP split | 65+ references, 9+ mock implementations across test files |
| HIGH | S3 Range request for random-access fetch | Requires segment-internal byte offset index (new feature) |
| MEDIUM | S3StorageAdapter struct composition | Safe to do incrementally after ISP split |
| MEDIUM | Custom JSON/XML parser replacement | Risk of behavioral regression in Iceberg/S3 parsing |
| LOW | save() full serialization coverage | Needs design decision on which fields to persist |

*Updated: 2026-03-21*
