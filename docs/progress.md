# Progress Log

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

