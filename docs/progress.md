# Progress Log

## 2026-03-07: Clean Code 리팩터링 2nd pass (feature/iceberg-duckdb-integration-v1)

### Task 1: v vet notice 해결
- `s3_xml_utils.v:8`: `s.len == 0` → `s == ''` 변경 (v vet notice 해결)

### Task 2: encode_file_metadata 함수 분할 (133줄 → 헬퍼 함수 3개로 분리)
- `encode_schema_elements(mut w, schema)` 추출: 루트 메시지 노드 + 리프 컬럼 노드 인코딩 (23줄)
- `encode_column_statistics(mut w, cm)` 추출: Statistics 구조체 인코딩 (21줄)
- `encode_column_chunk_list(mut w, col_chunks_meta, codec)` 추출: list<ColumnChunk> 인코딩 (26줄)
- `encode_file_metadata` 자체: 31줄로 축소

### Task 3: encode 함수 분할 (117줄 → 헬퍼 함수 3개로 분리)
- `compute_record_range(records)` 추출: 레코드 min/max 오프셋·타임스탬프 계산 (26줄)
- `build_column_chunks(cols, codec, mut file_bytes)` 추출: 컬럼 청크 인코딩 및 통계 수집 (34줄)
- `build_row_group_metadata(col_metas, count, range_, compression)` 추출: 로우 그룹 메타데이터 생성 (21줄)
- `encode()` 자체: 37줄로 축소

### Task 4: iceberg_writer.v 버그 수정 (기회 발생)
- `flush_all_partitions`: `ParquetCompression.none_compression` (존재하지 않는 enum 값) → `ParquetCompression.uncompressed` 로 수정

### 검증
- `v vet src/infra/storage/plugins/s3/s3_xml_utils.v`: notice 없음
- `make build`: 빌드 성공
- `make test`: 50/50 모든 테스트 통과

## 2026-03-04: escape_json_string wrapper 제거, extract_json_object 통합, TODO 포맷 정리 (feature/cleanup-duplicates-todos-v1)

### Task 1: escape_json_string wrapper 제거
- `logger.v`의 `fn escape_json_string` wrapper 제거 → 모든 호출처를 `core.escape_json_string`으로 직접 교체 (7곳)
- `otlp_exporter.v`도 같은 모듈 내 wrapper 참조 → `core.escape_json_string`으로 교체 (8곳), `import infra.performance.core` 추가
- `json_utils.v`의 `fn escape_json_string` (따옴표 추가 wrapper) 제거
  - `avro_encoder.v` 3곳: `'"${core.escape_json_string(s)}"'` 인라인으로 교체
  - `registry.v` 1곳: `'"${core.escape_json_string(s)}"'`으로 교체, `import infra.performance.core` 추가
- `logger_test.v`: `escape_json_string` → `core.escape_json_string` 교체, import 추가

### Task 2: extract_json_object 통합
- `json_utils.v`에 `pub fn extract_json_object(json_str string, key string) map[string]string` 추가 (websocket_handler.v 로직 이전)
- `websocket_handler.v`: `schema.extract_json_object` 호출로 변경, 기존 `fn extract_json_object` 정의 제거 (69줄 감소)

### Task 3: TODO 포맷 통일 (`// TODO:` → `// TODO(jira#XXX):`)
- `websocket_handler.v:431` - simple JSON parsing 주석
- `handler_config.v:305` - Add actual broker configs
- `crc32c.v:285` - hardware acceleration
- `postgres/adapter.v:156` - make cluster_id configurable
- `generic.v:111` - track other operations
- `manager.v:22` - LinuxPerformanceEngine 초기화
- `partition_assigner.v:483` - Need method to get topic list
- `broker_registry.v:431` - Perform rebalancing
- `streaming_port.v:109` - implement proper glob/regex

### 검증
- `v fmt -w .`: 이상 없음 (모든 파일 Already formatted)
- `make build`: 빌드 성공, 경고 없음 (v0.47.1)
- `v -enable-globals test infra/observability/logger_test.v`: PASS
- `v -enable-globals test service/schema/`: 2/2 PASS

---

## 2026-03-04: 중복 함수 제거 및 TODO/Stub 정리 (feature/cleanup-duplicates-todos-v1)

### Task 1: escape_json_str 중복 제거
- `encoder_json_helpers.v`의 `escape_json_str` 제거 (`.replace()` 체인 구현)
- `protobuf_encoder.v`에서 `core.escape_json_string` 직접 사용으로 변경 (canonical 구현 사용)

### Task 2: extract_json_string/int 중복 제거
- `json_utils.v`의 `extract_json_string`, `extract_json_int`를 `pub fn`으로 노출
- `websocket_handler.v`에 `import service.schema` 추가
- `websocket_handler.v`의 `extract_json_string`, `extract_json_int`, `unescape_json_string` 제거 (111줄 감소)
- `parse_ws_message`에서 `schema.extract_json_string`, `schema.extract_json_int` 사용

### Task 3: TODO 정리
- IMPORTANT 6개: `TODO(jira#XXX):` 자리표시 추가 (topic.v, websocket_handler.v, handler_admin.v, logger.v, s3/adapter.v, coordinator.v)
- FUTURE 4개: `docs/FUTURE_FEATURES.md`에 CRC32C 하드웨어 가속, 리밸런싱, 토픽 목록 메서드, JSON 파싱 라이브러리 섹션 추가

### 검증
- `v fmt -w .`: 이상 없음 (모든 파일 Already formatted)
- `make build`: 빌드 성공 (v0.47.1)
- 테스트 실패 70개 모두 기존 실패와 동일 (`writer_pool.v` 글로벌 변수 플래그 미지정 등 기존 문제)

---

## 2026-02-28: Iceberg 리팩토링 (refactor/iceberg-cleanup-v1)

- IcebergConfig 중복 타입 통합: config.IcebergConfig 제거, s3.IcebergConfig으로 단일화
- flush_all_partitions() stats 로직 추출: collect_column_stats() 메서드 분리
- iceberg_writer.v 파일 분할: 745줄 -> iceberg_writer.v(289), iceberg_avro.v(282), iceberg_metadata.v(182)
- QA: 68/68 테스트 통과, fmt/lint/build 클린

---

## 2026-02-28 - Iceberg Bug Fixes (Config/Stats/Tests)

- Config/Runtime disconnect fixed: enabled flag + format_version now properly injected
- Format version consistency: v3 default -> v2 across all components
- Manifest column stats: real min/max/null_count from Parquet chunks
- REST API tests: 16 endpoint tests via MockIcebergCatalog
- PDCA: 1 iteration (pre-existing test fix), 68/68 tests passing

---

## 2026-02-28 (2)

### 작업: IcebergCatalogAPI 단위 테스트 추가

**Branch:** `feature/iceberg-fixes-v1`

#### 완료 내용

- `src/interface/rest/iceberg_catalog_api_test.v` 신규 생성
- MockIcebergCatalog (인메모리) 구현: `IcebergCatalog` 인터페이스 전체 구현
- 총 16개 테스트 함수, 모두 통과

#### 테스트 목록

| 테스트 | 검증 내용 |
|--------|-----------|
| test_list_namespaces_empty | 빈 목록일 때 200 응답 |
| test_create_namespace | 네임스페이스 생성 및 200 반환 |
| test_create_namespace_already_exists | 중복 생성 시 500 오류 |
| test_drop_namespace | 빈 네임스페이스 삭제 시 204 반환 |
| test_drop_namespace_not_empty | 테이블 존재 시 409 반환 |
| test_get_namespace_not_found | 존재하지 않는 네임스페이스 404 |
| test_list_tables_empty | 빈 네임스페이스 테이블 목록 200 |
| test_create_table | 테이블 생성 메타데이터 반환 |
| test_load_table | 기존 테이블 메타데이터 로드 |
| test_table_not_found | 존재하지 않는 테이블 404 |
| test_drop_table | 테이블 삭제 후 204 반환 |
| test_invalid_path | 잘못된 경로 400 반환 |
| test_method_not_allowed | 허용되지 않은 메서드 405 |
| test_create_table_empty_name | 이름 없는 테이블 400 반환 |
| test_config_endpoint | /v1/config 엔드포인트 200 반환 |
| test_create_table_namespace_not_found | 없는 네임스페이스에 테이블 생성 404 |

#### 실행 명령

```
cd src && v -enable-globals -d use_openssl -cflags "-I/opt/homebrew/include" -ldflags "-L/opt/homebrew/lib" test interface/rest/
```

---

## 2026-02-28

### PDCA: Kafka 압축 버그 수정 (CHECK PASS)

**Branch:** `fix/kafka-compression-bug`

#### PDCA CHECK 결과: PASS (50/50)

`make test-compat` 전체 통과 (Schema Registry 3개 SKIP)

#### 완료된 수정 작업

- `snappy_c.v`: xerial snappy-java format (PPY\0 magic) 지원 추가
- `gzip.v`: Kafka 4바이트 BE length prefix 처리 (`has_kafka_gzip_prefix`)
- `lz4_c.v`: Kafka 4바이트 BE length prefix 처리 (`has_kafka_lz4_prefix`)
- `zstd_c.v`: u64->int 오버플로우 가드 + streaming 분기
- `handler_fetch.v`: Fetch 경로 재압축 제거, `.none` 고정
- `handler_produce.v`: `header_size = 61` 수정, `records_count` 명시적 read
- `codec.v`: `parse_nested_record_batch` → Record 리스트 직접 파싱
- `handler_describe_topic_partitions.v`: nullable compact array/struct 인코딩, cursor 파싱 수정

#### 추가된 테스트 파일

- `src/infra/protocol/kafka/describe_topic_partitions_test.v` (7개)
- `tests/integration/kafka_compat_test.v` (8개)
- `src/service/group/coordinator_test.v`

#### 커밋 이력 (7개)

- `862aa88` — refactor(kafka): handler 라우팅 개선, group coordinator 리팩터링, startup 정리
- `c1f32bc` — fix(kafka): DescribeTopicPartitions nullable 인코딩 및 cursor 파싱 수정
- `5b1f480` — fix(kafka): Fetch 경로에서 재압축 로직 제거로 Consumer 파싱 오류 수정
- `cc6ec28` — fix(compression): RecordBatch header_size 및 snappy xerial chunk format 수정
- `cec36b9` — fix(codec): parse_nested_record_batch to parse Records directly after decompression
- `243471d` — fix(compression): add PPY\0 old snappy-java format support for Confluent Kafka clients
- `63ceacb` — fix(compression): add Kafka-compatible decompression for snappy/lz4/zstd/gzip

## 2026-02-22

### Task #8: Kafka Admin API Extension (완료)

**Branch:** `feature/task8-admin-api-extension`

#### 구현 완료
- `handler_describe_topic_partitions.v` — DescribeTopicPartitions (API 75)
- `handler_incremental_alter_configs.v` — IncrementalAlterConfigs (API 44)
- `handler_log_dirs.v` — DescribeLogDirs (API 35) + AlterReplicaLogDirs (API 34)
- `handler.v` — API 34, 35, 44, 75 라우팅 추가
- `request.v` — is_flexible_version에 새 API 키 추가
- `types.v` — get_supported_api_versions에 새 API 버전 추가
- `admin_test.v` — 신규 API 4개 단위 테스트 추가

#### Share Group State APIs (API Keys 83-86, KIP-932)
- `share_group_state_types.v`, `share_group_state_parser.v`, `share_group_state_encoder.v` 신규
- `handler_share_group_state.v` — 4개 핸들러 구현
- `share_group_state_test.v` — 14개 단위 테스트

#### 테스트 수정 (2026-02-22 세션)
- `transaction_test.v`, `acl_test.v` — MockStorage에 share partition state 메서드 누락 수정
- 중복 메서드 제거 (transaction_test.v)
- v fmt 포맷팅 적용 (admin_test.v, integration_test.v)

#### 테스트 결과
- 29/32 통과 (3 pre-existing 실패: integration_test redefinition, acl_test/transaction_test C compiler arg count 에러)
- 빌드: 성공 (v0.44.4)
- Lint: 통과

#### 커밋
- `ca09c96` — fix(test): add missing share partition state methods to mock storages

## 2026-02-23

### GlueCatalog <-> HadoopCatalog 동기화 (완료)

**Branch:** `feature/gluecatalog-sync-v1`

#### 구현 완료

**`src/infra/storage/plugins/s3/iceberg_catalog.v`**

- `HadoopCatalog.commit_metadata(identifier, metadata)` — 버전 번호(`snapshots.len + 1`)로 새 메타데이터 JSON 파일을 S3에 기록하고 `version-hint.text`를 갱신하는 메서드 추가
- `GlueCatalog.export_table(identifier, mut hadoop_catalog)` — HadoopCatalog에서 테이블을 로드하여 Glue에 upsert (create 우선, 이미 존재하면 update)
- `GlueCatalog.export_all(mut hadoop_catalog, namespaces)` — 지정 namespace 목록의 모든 테이블을 export_table로 일괄 내보내기
- `GlueCatalog.import_table(identifier, mut hadoop_catalog)` — Glue에서 테이블을 로드하여 HadoopCatalog에 commit_metadata로 저장
- `GlueCatalog.import_all(mut hadoop_catalog, namespaces)` — 지정 namespace의 모든 Glue 테이블을 일괄 가져오기

**`src/infra/storage/plugins/s3/iceberg_glue_catalog_test.v`** (신규)

- `test_commit_metadata_encodes_and_builds_path` — encode/decode 왕복 검증
- `test_commit_metadata_version_path_formula` — 버전 경로 수식 검증
- `test_export_table_metadata_roundtrip` — export 시나리오 메타데이터 왕복
- `test_import_table_metadata_preservation` — import 시나리오 전체 필드 보존 검증
- `test_export_all_result_accumulation` — 결과 배열 누적 로직 검증
- `test_import_all_identifier_accumulation` — 식별자 누적 로직 검증
- `test_hadoop_table_path_*` — namespace 기반 경로 도출 3가지 케이스
- `test_export_table_upsert_*` — upsert 분기 로직(기존 location 사용 vs warehouse 폴백) 검증

---

## 2026-03-06 — S3 PUT 비용 최적화 완료

**브랜치:** feature/s3-put-cost-optimization-v1
**커밋:** 6f46772 ~ 45f89fa (7개 커밋)

### 완료 항목
- [x] Task 1: batch_max_bytes flush 임계값 구현
- [x] Task 2: 오프셋 이중 저장 경로 통합
- [x] Task 3: S3 DeleteObjects 배치 API 구현
- [x] Task 4: 인덱스 배치 업데이트 구현
- [x] Task 5: sync 경로 linger 도입
- [x] Task 6: 컴팩션 서버사이드 복사 인프라 구축
- [x] ACT: Critical 이슈 수정 (index_batch_size, graceful shutdown)

### 결과
- 예상 PUT 감소: 60-80%
- 예상 비용 절감: 월 $770 → $150-300
- 신규 테스트: 50개+ 모두 통과
- Task 6 서버사이드 복사: 세그먼트 포맷 제약으로 fallback 모드 (향후 포맷 변경 시 자동 활성화)

### 보고서
docs/reports/2026-03-06-s3-put-cost-optimization.md

---

## 2026-03-06 - 코드 리뷰 이슈 수정 (커밋: 4c0a270)
- H-1: XML injection 방어 - s3_xml_utils.v xml_escape() 추가, s3_batch_delete.v 키 escape 적용
- H-2: ETag XML escape 적용 (s3_multipart.v)
- H-3: s3_server_side_copy.v 파일 분리 → s3_multipart.v (265L) 신규
- M-2: sync_linger_worker 폴링 간격 동적 계산으로 CPU 최적화
- M-3: use_server_side_copy 기본값 false로 변경 (현재 항상 fallback이므로)
- M-5: 인덱스 배치 flush 실패 시 pending 세그먼트 restore 추가
- 테스트: 12/12 PASS (v -enable-globals test src/infra/storage/plugins/s3/)

## 2026-03-22: Comprehensive Architecture Improvement Sprint

### Overview
- Branch: `feature/comprehensive-improvements-v1`
- 15 tasks across 3 batches, all completed and QA-verified
- 116/116 module tests PASS, production build SUCCESS, lint CLEAN

### Batch 1 - Test Coverage + Stability (G1)
- Task A-C: 25 Kafka protocol handler test files (produce, fetch, metadata, topic, offset, group, find_coordinator, list_offsets, sasl, acl, transaction, api_versions, consumer)
- Task D: 4 service layer test files (produce, fetch, topic_manager, transaction_coordinator)
- Task E: writer_pool.v global variable fix (all `__global` -> const holder pattern), replication/s3 infra tests

### Batch 2 - HA Features + Structure (G2)
- Task F: 6 remaining handler tests (share_group, config, describe_cluster, log_dirs, admin, incremental_alter_configs)
- Task G: Replication connection pooling (connection_pool.v)
- Task H: ISR Manager (isr_manager.v) - ISR tracking, shrink/expand, min.insync.replicas, high watermark
- Task I: Partition rebalancing (rebalance_trigger.v) - wired into broker_registry.v
- Task J: File splitting - server.v (907->375), main.v (700->112)

### Batch 3 - Advanced Features (G3)
- Task K: Partition-level leader election (partition_leader_election.v)
- Task L: Binary replication protocol (binary_protocol.v)
- Task M: WriteTxnMarkers implementation (domain types + coordinator + handler)
- Task N: Rate limiting (rate_limiter.v + tcp.v integration)
- Task O: SCRAM-SHA-512 tests + Audit logging (audit_logger.v)

### Metrics
- New test files: 36+
- New test functions: 400+
- Total test files: 125 (from ~89)
- Total test functions: 1,704
- Source files modified: 12+
- New source files: 10+
- `__global` declarations: 0 remaining (was 8+)

### Decisions
- Used const holder pattern for global state (compatible without -enable-globals)
- Connection pool is optional in replication client (backward compatible)
- Rate limiter is optional in TCP server (backward compatible)
- ISR Manager, RebalanceTrigger, PartitionLeaderElector are standalone services (wired via setters)

## 2026-03-22 -- Residual Work Sprint (Post v0.50.0)

### Completed
- R0: Removed ISR/Rebalancing/LeaderElection (stateless architecture cleanup) - 6 files deleted, broker_registry.v reverted
- R2: Migrated replication from JSON to binary protocol (manager.v, client.v, server.v + integration tests)
- R3: Added rate limiter configuration to config.toml and wired into server startup
- R4: Wired audit logger into SASL authentication handlers (handler.v, handler_sasl.v)
- R5: Fixed infra/performance V 0.5 compatibility (io/ -> sysio/ rename, http_exporter.v fix)

### QA Verification
- 123/123 test files pass (1,672 test functions)
- Production build: SUCCESS, Lint: CLEAN
- All infra/performance tests now pass (previously 5 failures)

### Decisions
- DataCore is a STATELESS broker: ISR, partition rebalancing, and partition-level leader election are architectural mismatches. Removed to keep codebase clean.
- Binary replication protocol is now the default; JSON protocol.v kept as reference
- Rate limiter disabled by default (enabled = false) for backward compatibility

## Code Review Fixes Sprint (2026-03-22)

- 6-panel code review performed (Refactor/Bug/Security/Performance/Structure/Architecture)
- Initial score: F (59.7/100) - 3 CRITICAL, 14 HIGH issues
- Fixed: 2 CRITICAL bugs (binary protocol negative length), 7 HIGH bugs (race conditions, leaks, security), 3 MEDIUM refactors, 1 MEDIUM perf
- Post-fix QA: 123/123 test files pass, 1,674 test functions, build SUCCESS, lint CLEAN
- Remaining known issues (out of scope): domain/grpc.v imports infra (C3), credentials in config (H7), replication TLS (H9), iceberg direct import (H14)

## Post-Merge Refactoring Sprint (2026-03-22)
- Refactored manager.v (990 -> 579 + 300 + 121 lines): manager_workers.v, manager_buffers.v
- Refactored metrics.v (1031 -> 283 + 261 + 489 lines): metrics_types.v, metrics_helpers.v
- Extracted magic numbers to named constants (binary_protocol, coordinator, health_handler)
- Fixed DRY violations in coordinator.v (contains_topic_partition helper)
- Fixed O(n) buffer size scan -> incremental counter in manager.v
- Fixed TOCTOU residual in connection_pool.v acquire() with re-check after re-lock
- Fixed cleanup_idle() I/O under lock in connection_pool.v
- Fixed stale version string in health_handler.v (0.44.1 -> 0.50.2)
- QA: 123/123 test files pass, 1,675 test functions, build SUCCESS, lint CLEAN

## v0.50.4 Code Review Auto-Fixes (2026-03-22)
- config.v 969 -> 218 lines (5 files: parse, save, validate, cli, credentials)
- metrics_helpers.v new_datacore_metrics() 296 -> 47 lines (12 extracted sub-functions)
- domain/grpc.v Clean Architecture fix: common/binary_utils.v extracted, 6 service/domain files updated
- coordinator.v: i16 epoch overflow + Mutex for thread safety
- manager.v: recovery handler now returns buffered data
- Review score: D (69.6) -> C (74.5), target: B (80+)

## v0.50.5 - Code Review Auto-Fix Sprint (2026-03-22)

### Completed
- B4: binary_helpers/binary_utils bounds checks (DoS prevention)
- S1: config save file permissions 0600 (credential protection)
- Storage adapter splits: postgres 1367->7 files, memory 1059->6 files
- LoggerPort interface: 6 service->infra.observability imports eliminated
- process_produce decomposition: 505->170 lines + 6 helpers
- QA: 28 modules, 122/122 test files, 1,682 functions - all PASS
- Architecture: 0 domain->infra imports, 0 service->infra.observability imports

## v0.50.6 - Performance & ISP Optimization (2026-03-22)
- Fixed: close_all() TCP I/O under lock -> collect+close pattern
- Fixed: binary_helpers write_i16/i32/i64 temp buffer alloc -> zero-alloc bit-shift
- Fixed: rate_limiter dual mutex per request -> single combined call
- Refactored: ClusterMetadataPort 15-method ISP violation -> 5 sub-interfaces
- QA: 123/123 test files pass, build SUCCESS, lint CLEAN

## v0.50.7 Quick Fix Sprint - 2026-03-23

### ISP Consumer Migration
- Narrowed 3 service files from composite ClusterMetadataPort to focused sub-interfaces
- Created ClusterPortAdapter for V interface bridging (V does not support interface-to-sub-interface conversion)
- grep verification: 0 ClusterMetadataPort references in service consumer files

### Rate Limiter Fix
- Implemented can_consume() pre-check pattern to prevent token non-rollback
- Two-phase approach: pre-check all buckets (non-mutating) then consume (all-or-nothing)
- Test proves no global token leak on per-IP rejection

### QA
- 28 modules, 122/122 test files, 1,683 test functions - ALL PASS
- Build SUCCESS, Lint CLEAN, ISP migration verified

## v0.50.8 God Class Decomposition Sprint (2026-03-23)

### Completed
- Handler God Class: Introduced HandlerContext shared struct + 7 sub-handler structs (Phase 1 - struct creation, methods remain on Handler)
- Manager God Class: Extracted 3 focused components (health tracker, stats collector, assignment store), manager.v reduced 586->367 lines (-37%)
- BrokerRegistry God Class: Split into 3 focused files by concern (registration, heartbeat, queries), main file reduced 475->233 lines (-51%)

### Results
- 13 new files created, all under 200 lines
- QA: 123/123 test files pass, build SUCCESS, lint CLEAN
- Zero test modifications required

## v0.50.9 -- Codebase Health Refactoring (2026-03-25)

### Phase 1: CRITICAL Error Fixes
- 17개 `or {}` 에러 삼키기 패턴 수정 (service/infra/interface 전 계층)
- Stateless 설계에 불필요한 리밸런싱 dead code 삭제 (broker_registry.v, partition_assigner.v)

### Phase 2: Duplicate Code Removal (~300 lines)
- `infra/performance/core/utils.v`: 14개 중복 함수 -> `common` 위임 thin wrapper (-110줄)
- CLI 로컬 바이너리/CRC32C 함수 -> `common.*` import (~120줄 삭제)
- JSON escape/extract 중복 함수 정리

### Phase 3: Architecture Cleanup
- Domain 계층 외부 의존성 완전 제거: `import common`, `import sync`, `import json` 모두 0건
- infra/replication: stdlib log -> Logger Port 주입

### Phase 4: Dead Code + Config Cleanup (-289 lines)
- SQLiteStorageConfig 완전 제거, TelemetryRootConfig 트리 제거
- 미사용 인터페이스/함수/config 섹션 제거

### Impact
- 총 ~600줄 코드 제거/통합
- 124/124 테스트 통과
- Architecture violation 0건
- CHECK Grade: B (87.8/100)

