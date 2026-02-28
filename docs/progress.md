# Progress Log

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

