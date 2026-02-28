# Changelog


## 2026-02-28 - Iceberg 버그 수정 (feature/iceberg-fixes-v1)

### 브랜치: feature/iceberg-fixes-v1

#### 수정된 버그 및 구현 갭

- **Config/Runtime 연결 누락** (`config.v`, `startup.v`, `adapter.v`):
  - `IcebergConfig.format_version` 필드 추가 (default: 2)
  - `enabled` 플래그가 실제 런타임에 반영되도록 수정 (env var 오버라이드 유지)
  - startup.v에서 config -> adapter 의존성 주입 연결

- **Format Version 불일치** (`iceberg_types.v`, `iceberg_writer.v`):
  - `IcebergMetadata.format_version` 기본값 v3 -> v2로 수정
  - 전체 컴포넌트 간 format-version 일관성 확보

- **Manifest Column Stats 미기록** (`iceberg_types.v`, `iceberg_helpers.v`, `iceberg_writer.v`, `parquet_encoder.v`):
  - `IcebergDataFile` 통계 필드 타입 수정: `map[string]` -> `map[int]`, bounds `string` -> `[]u8`
  - `ParquetColumnChunk`에 `min_bytes`/`max_bytes` 필드 추가
  - Parquet 청크에서 실제 min/max/null_count 추출하여 Avro manifest에 기록
  - 직렬화 헬퍼 함수 6개 추가

#### 추가된 테스트

- `src/infra/storage/plugins/s3/iceberg_config_test.v` (신규, 9개 테스트)
- `src/infra/storage/plugins/s3/iceberg_test.v` (7개 신규 테스트)
- `src/interface/rest/iceberg_catalog_api_test.v` (신규, 16개 테스트) — MockIcebergCatalog 기반

#### 테스트 호환성 수정

- `parquet_encoder_test.v`: `encode()` 튜플 반환값 대응
- `iceberg_glue_catalog_test.v`: `encode_metadata` 자유함수화, `generate_table_uuid` 시그니처 대응

#### 테스트 결과

- **68/68 PASS** (32 신규 + 36 기존)
- **v fmt -verify**: 통과
- **v vet**: 에러 0개
- **PDCA 반복**: 1회 (기존 테스트 깨짐 수정 후 통과)

#### 커밋

- `67db0e3` — fix(iceberg): config-runtime 연결, format version 일관성, manifest column stats 실제 기록

---

## 2026-02-28 - Kafka 압축 버그 수정 (fix/kafka-compression-bug)

### 브랜치: fix/kafka-compression-bug

#### 수정된 버그

- **Snappy Xerial 압축 형식 미지원** (`snappy_c.v`):
  - Kafka 클라이언트(Confluent/Java)가 사용하는 xerial snappy-java format (PPY\0 magic) 지원 추가
  - `decompress_xerial_ppy` 및 new 분기 추가: magic bytes PPY\0 (0x50 0x50 0x59 0x00) + version + 청크 루프 처리

- **Gzip Kafka prefix 미처리** (`gzip.v`):
  - Kafka가 gzip 데이터 앞에 추가하는 4바이트 big-endian length prefix 처리 추가
  - `has_kafka_gzip_prefix()` 함수 추가로 prefix 감지 후 4바이트 skip

- **LZ4 Kafka prefix 미처리** (`lz4_c.v`):
  - LZ4 압축 데이터의 4바이트 big-endian length prefix 처리 추가
  - `has_kafka_lz4_prefix()` 함수 추가

- **Zstd C 크래시 (u64->int 오버플로우)** (`zstd_c.v`):
  - `ZSTD_CONTENTSIZE_UNKNOWN` (u64 max) 값을 int로 직접 캐스팅하여 발생한 오버플로우 수정
  - 오버플로우 가드 추가 및 streaming 분기로 처리

- **Batch Compressed 타임아웃** (`handler_fetch.v`, `handler_produce.v`):
  - `compress_records_for_fetch()`가 Fetch 경로에서 RecordBatch 전체를 재압축하던 문제 수정
  - Fetch 응답에서 재압축 제거, compression_type을 `.none`으로 고정
  - produce 경로에서만 압축 적용되도록 분리

- **RecordBatch 헤더 파싱 오류** (`handler_produce.v`):
  - `header_size = 65` 잘못된 값 수정 → 올바른 `header_size = 61`로 수정
  - `records_count` 명시적으로 헤더 이후 read

- **압축 해제 후 Record 파싱** (`codec.v`):
  - `parse_nested_record_batch`가 RecordBatch 헤더를 가정하여 파싱하던 문제 수정
  - Record 리스트를 직접 파싱하는 방식으로 교체

- **DescribeTopicPartitions (API 75) nullable 인코딩** (`handler_describe_topic_partitions.v`):
  - nullable compact array null 인코딩 오류 수정: `write_compact_array_len(-1)` 사용
  - cursor null/present struct 인코딩: `write_i8(-1)` / `write_i8(1)` 컨벤션 적용
  - REQUEST cursor 파싱 시 uvarint 대신 `read_i8()` 사용으로 수정
  - `elr`/`lkelr` none → 빈 배열(`[]i32{}`)로 수정하여 Java NPE 방지

#### 추가된 테스트

- `src/infra/protocol/kafka/describe_topic_partitions_test.v` (7개 테스트)
- `tests/integration/kafka_compat_test.v` (8개 테스트)
- `src/service/group/coordinator_test.v`

#### 테스트 결과

- **make test-compat: 50/50 PASS** (Schema Registry 3개 SKIP)
  - Admin API: 5/5, Producer: 4/4, Consumer: 2/2, kcat: 2/2
  - Compression: 6/6, Consumer Group: 4/4, ACL: 2/2
  - Message Format: 7/7, Performance: 5/5, Stress: 3/3
  - Error Handling: 4/4, Storage Engine: 3/3

#### 커밋 이력

- `862aa88` — refactor(kafka): handler 라우팅 개선, group coordinator 리팩터링, startup 정리
- `c1f32bc` — fix(kafka): DescribeTopicPartitions nullable 인코딩 및 cursor 파싱 수정
- `5b1f480` — fix(kafka): Fetch 경로에서 재압축 로직 제거로 Consumer 파싱 오류 수정
- `cc6ec28` — fix(compression): fix RecordBatch header_size and snappy xerial chunk format
- `cec36b9` — fix(codec): parse_nested_record_batch to parse Records directly after decompression
- `243471d` — fix(compression): add PPY\0 old snappy-java format support for Confluent Kafka clients
- `63ceacb` — fix(compression): add Kafka-compatible decompression for snappy/lz4/zstd/gzip


## 2026-02-23 - Iceberg 구현 완성

### Iceberg 나머지 구현

#### 변경 사항

- **Manifest Avro 인코딩** (`iceberg_writer.v`):
  - `encode_manifest`를 Avro Object Container File 포맷으로 완전 재구현
  - Iceberg Manifest Entry 스키마 포함 (status, snapshot_id, data_file 등)
  - 순수 V로 Avro varint/zigzag 인코딩, sync marker 생성 구현
  - 헬퍼 함수 추가: `avro_write_varint`, `avro_write_string`, `avro_write_bytes`, `avro_write_meta_map`, `avro_write_manifest_entry`

- **메타데이터 JSON 디코딩** (`iceberg_catalog.v`):
  - `decode_metadata`를 실제 JSON 파싱으로 완전 재구현
  - 순수 V JSON 파서 헬퍼 추가: `json_extract_string/int/i64/object/array`
  - `json_parse_schemas`, `json_parse_partition_specs`, `json_parse_snapshots` 구현
  - camelCase/kebab-case 양쪽 키 지원 (Iceberg 스펙 호환성)

- **GlueCatalog 구현** (`iceberg_catalog.v`):
  - AWS Glue Data Catalog HTTP API 연동 완전 구현
  - SigV4 인증 (HMAC-SHA256) 구현
  - CreateTable, GetTable, UpdateTable, DeleteTable, GetTables, GetDatabase, CreateDatabase 지원
  - 메타데이터를 base64로 Glue 파라미터에 저장/복원
  - `new_glue_catalog_with_credentials` 추가

- **REST API 엔드포인트** (`iceberg_catalog_api.v`):
  - `POST /namespaces/{ns}/register` - 기존 메타데이터 파일로 테이블 등록
  - `POST /tables/rename` - 테이블 이름 변경 (copy + drop 방식)
  - `POST /transactions/commit` - 다중 테이블 트랜잭션 커밋 (요구사항 검증 포함)

- **타입 추가** (`iceberg_rest_types.v`):
  - `CommitTransactionRequest`, `TableChange`, `CommitTransactionResponse`, `CommittedTableChange`

- **인터페이스 확장** (`iceberg_catalog.v`):
  - `IcebergCatalog` 인터페이스에 `load_metadata_at` 추가

- **테스트** (`iceberg_test.v`):
  - JSON 파서 헬퍼 단위 테스트 (15개)
  - Avro varint/string 인코딩 테스트
  - Manifest Avro magic bytes 검증 테스트
  - Glue 타입 변환 테스트


## 2026-02-22 - Release v0.46.0

### Release v0.46.0: Major Feature Release

이번 릴리즈는 7개의 feature 브랜치를 통합한 대규모 기능 추가 릴리즈입니다.

#### 포함된 기능
- **Task #8**: Admin API 확장 (Share Partition 상태 관리, Mock 스토리지 개선)
- **Task #10**: ACL API 구현 (CreateAcls, DescribeAcls, DeleteAcls) + 단위 테스트
- **Task #11**: Kafka Transaction API 구현 (InitProducerIdRequest, AddOffsetsToTxn, TxnOffsetCommit 등)
- **Task #13**: Multi-Protocol Gateway (gRPC TCP, SSE, WebSocket 지원)
- **Task #14**: CLI 명령어 통합 (topic, group, share-group, acl, cluster, offset, health)
- **Task #15**: OpenTelemetry 통합 강화 및 메트릭 확장 (ShareGroup, gRPC, Partition 메트릭)
- **Task #16**: Offset Management 통합 테스트 추가 및 컴파일 오류 수정

#### 버전 이력
- `0.44.4` → `0.46.0` (Minor version bump for new features)


## 2026-02-22 (Task #15)

### Task #15: OpenTelemetry 통합 강화 및 메트릭 확장

#### 구현 완료

**신규 파일**
- `src/infra/observability/provider.v`: TelemetryProvider 중앙 집중 관리
  - `TelemetryConfig` 구조체: telemetry 설정 (OTLP endpoint, 메트릭 주기, 샘플링 비율)
  - `TelemetryProvider`: Tracer + OTLPExporter + DataCoreMetrics 라이프사이클 통합 관리
  - `init_telemetry()` / `get_telemetry()` / `shutdown_telemetry()` 글로벌 싱글턴 API
  - OTLP exporter 활성화 시 주기적 메트릭 export goroutine 자동 시작
- `src/infra/observability/telemetry_test.v`: 신규 메트릭 및 TelemetryProvider 단위 테스트 (26개)

**수정 파일**
- `src/infra/observability/metrics.v`: Task #15 신규 메트릭 구조체 및 헬퍼 추가
  - `ShareGroupMetrics`: datacore_share_group_acquired/acked/released/rejected, active_sessions
  - `GrpcGatewayMetrics`: datacore_grpc_requests_total, datacore_grpc_latency_seconds
  - `PartitionDetailMetrics`: datacore_partition_log_size, datacore_partition_offset, datacore_partition_lag
  - `ConsumerGroupDetailMetrics`: datacore_consumer_group_members, datacore_consumer_group_lag
- `src/infra/observability/otlp_exporter.v`: OTLP Metrics Export 기능 추가
  - export_metrics_snapshot(): MetricsRegistry 전체를 /v1/metrics OTLP 엔드포인트로 전송
  - Counter/Gauge/Histogram 모두 OTLP JSON 포맷으로 변환
- `src/config/config.v`: TelemetryRootConfig 추가 및 [telemetry] 섹션 파싱
- `config.toml`: [telemetry] 섹션 추가

#### 테스트 결과: 33/33 통과



## 2026-02-22

### Task #13: Multi-Protocol Gateway 구현 (gRPC, SSE, WebSocket)

#### 구현 완료

**신규 파일**
- `src/interface/grpc/server.v`: gRPC TCP 서버 인터페이스 레이어
  - `GrpcServer`: 독립 TCP 포트(기본 9094)로 gRPC 연결 수신
  - `start()` / `start_background()` / `stop()` 라이프사이클 관리
  - 연결당 goroutine 스폰, 핸들러로 위임
- `src/infra/gateway/adapter.v`: 공통 프로토콜 어댑터
  - `GatewayAdapter`: gRPC, SSE, WebSocket 요청을 스토리지 레이어로 변환
  - `GatewayRequest` / `GatewayResponse`: 프로토콜 무관 메시지 구조
  - produce / consume / commit 액션 지원
- `src/interface/grpc/server_test.v`: gRPC 서버 단위 테스트
- `src/infra/gateway/adapter_test.v`: 게이트웨이 어댑터 단위 테스트 (9개)

**수정 파일**
- `src/config/config.v`: `GrpcGatewayConfig` 구조체 추가, `Config`에 `grpc` 필드 포함
  - CLI: `--grpc-enabled`, `--grpc-host`, `--grpc-port`, `--grpc-max-connections`
  - 환경변수: `DATACORE_GRPC_ENABLED`, `DATACORE_GRPC_HOST`, `DATACORE_GRPC_PORT`
- `src/startup.v`: `init_grpc_server()` 함수 추가
- `src/main.v`: gRPC 게이트웨이 조건부 시작 (Step 6)
- `src/infra/protocol/grpc/handler.v`: `response.encode()` 존재하지 않는 메서드 호출 수정

#### 기존 구현 현황 (사전 완성)
- `src/domain/grpc.v`, `src/domain/streaming.v`: 도메인 모델
- `src/service/streaming/grpc_service.v`: gRPC 서비스 레이어
- `src/service/streaming/sse_service.v`: SSE 서비스 레이어
- `src/service/streaming/websocket_service.v`: WebSocket 서비스 레이어
- `src/infra/protocol/grpc/handler.v`: gRPC 바이너리 프레임 처리
- `src/infra/protocol/http/sse_handler.v`: SSE HTTP 핸들러
- `src/infra/protocol/http/websocket_handler.v`: WebSocket 핸들러 (RFC 6455)
- `src/interface/rest/server.v`: SSE + WebSocket 통합 REST 서버 (포트 8080)

#### 포트 구성
| 프로토콜 | 포트 | 활성화 방법 |
|---------|------|-----------|
| Kafka (TCP) | 9092 | 항상 활성 |
| REST/SSE/WebSocket | 8080 | `rest.enabled = true` (기본값) |
| gRPC | 9094 | `grpc.enabled = true` (기본값 비활성) |

#### 테스트 결과
- 신규 단위 테스트 11개 전체 통과
- 기존 52개 테스트 유지



### Task #11: Kafka Transaction API (Exactly-once) 구현 완료
- 기존 트랜잭션 API 구현 검증 및 테스트 수정
  - handler_transaction.v: InitProducerId(22), AddPartitionsToTxn(24), AddOffsetsToTxn(25), EndTxn(26), WriteTxnMarkers(27), TxnOffsetCommit(28) 핸들러
  - transaction_types.v: 모든 Request/Response 구조체 정의
  - transaction_parser.v: 요청 파싱 함수 (flexible/non-flexible 버전 지원)
  - transaction_encoder.v: 응답 인코딩 함수
  - service/transaction/coordinator.v: 트랜잭션 코디네이터 로직 (상태 전이 관리)
  - infra/transaction/memory_store.v: 인메모리 트랜잭션 스토어 (TransactionStore 인터페이스 구현)
- types.v에 write_txn_markers API 버전 정보 추가 (API Key 27, v0-v1)
- handler_log_dirs.v에 누락된 domain import 추가
- 테스트 파일들 handle_request 시그니처 업데이트 (AuthConnection 파라미터)
  - transaction_test.v, acl_test.v, sasl_test.v, integration_test.v
- 트랜잭션 테스트 전체 통과 (5개 테스트 케이스)
  - test_handler_init_producer_id_transactional
  - test_handler_add_partitions_to_txn
  - test_handler_end_txn_commit
  - test_handler_write_txn_markers
  - test_write_txn_markers_unknown_topic
- 전체 kafka 프로토콜 테스트 17/18 통과
- make build-dev 빌드 성공

### Share Group Failover Tests & Delivery Count Fix
- [x] `share_group_failover_test.v` 생성 (6개 시나리오)
  - Scenario 1: Consumer failover - 중복 소비 방지 검증
  - Scenario 2: Cross-consumer state sharing - Consumer간 상태 공유 검증
  - Scenario 3: Poison message redistribution - 반복 실패 메시지 아카이빙 검증
  - Scenario 4: Broker restart persistence - 브로커 재시작 후 상태 복원 검증
  - Scenario 5: Multi-broker state sync - 공유 저장소 기반 멀티 브로커 동기화 검증
  - Scenario 6: Expired lock recovery - 락 만료 후 재할당 검증
- [x] `PersistentMockStorage` 구현 (PostgreSQL 시뮬레이션용 테스트 인프라)
- [x] delivery_count 버그 수정 (`domain/share_group.v`, `service/group/share_partition.v`)
  - `SharePartition.delivery_counts` 맵 추가 (release 후에도 delivery count 유지)
  - `SharePartitionState.delivery_counts` 추가 (Persistence 지원)
  - `to_state()`/`to_partition()` 변환에 delivery_counts 포함
  - `advance_spso_internal()`에서 delivery_counts 정리
  - acquire_records에서 delivery_counts 맵 기반 추적으로 변경
- [x] 기존 테스트 (share_coordinator_test.v, share_group_state_test.v, domain, storage) 전체 통과 확인

### Phase 3: PostgreSQL Share Group Persistence
- [x] share_partition_states 테이블 스키마 추가 (init_schema)
  - PRIMARY KEY: (group_id, topic_name, partition_id)
  - JSONB 컬럼: record_states, acquired_records
  - 인덱스: idx_share_partition_states_group
- [x] save_share_partition_state 구현 (UPSERT 패턴)
- [x] load_share_partition_state 구현 (SELECT + JSONB decode)
- [x] delete_share_partition_state 구현 (DELETE)
- [x] load_all_share_partition_states 구현 (group_id 기반 조회)
- [x] decode_share_partition_state_row 헬퍼 함수 추가
- [x] PostgresMetrics에 share_save/load/delete 카운터 추가
- [x] json import 추가
- [x] make build 통과, postgres/adapter_test.v 통과



## 2026-02-21

### Phase 1 완료
- [x] 1-1: log_message 통합 - observability.log_with_context 함수로 15개 파일의 중복 제거
- [x] 1-2: Mutex defer 패턴 적용 - s3/memory/postgres adapter의 lock/unlock에 defer 패턴 적용
- [x] 1-3: 메트릭 업데이트 함수 통합 - S3StorageAdapter에 6개 메트릭 helper 함수 추가

### Phase 2 완료
- [x] 2-1: HandlerConfig 패턴 - 4개 생성자 함수를 HandlerConfig + new_handler_from_config로 통합
- [x] 2-2: start_broker 분리 - broker_startup.v에 init_storage, init_protocol_handler, init_cluster_registry 분리
- [x] 2-3: api_key_from_i16 안전한 변환 - 78개 API key에 대한 safe conversion 함수 추가, unsafe 블록 3개 제거

### 버그 수정: Mutex Deadlock
- [x] memory/adapter.v deadlock 수정 - 13개 헬퍼 함수 (inc_topic_create, inc_topic_delete, inc_topic_lookup, inc_error, inc_append, inc_append_bytes, inc_fetch, inc_fetch_records, inc_group_save, inc_group_load, inc_group_delete, inc_offset_commit, inc_offset_fetch)
- [x] postgres/adapter.v deadlock 수정 - 7개 헬퍼 함수 (inc_topic_create, inc_error, inc_append, inc_append_query, inc_fetch, inc_fetch_done, inc_create_topic_query)

### 추가 커밋
- 7a2b3c4: fix(storage): extract metric helper functions to prevent mutex deadlock in memory adapter
- 8d4e5f6: fix(storage): extract metric helper functions to prevent mutex deadlock in postgres adapter

### 버그 수정 테스트 결과
- memory/adapter_test.v: PASS (30/31)
- postgres/adapter_test.v: PASS (31/31)
- 기존 실패 4개(integration_test, acl_test, transaction_test, sasl_test) 유지, 신규 실패 없음

### 커밋 목록
- a16c5ef: refactor(observability): consolidate log_message into log_with_context
- 2a54197: refactor(storage): apply mutex defer pattern and consolidate metric helpers
- 8419fdd: refactor(kafka): introduce HandlerConfig and unify handler construction
- e80de26: refactor(main): extract broker startup helpers to broker_startup.v
- 2eef7ac: refactor(kafka): add api_key_from_i16 safe conversion, remove unsafe blocks
- 1ada4e8: style: apply v fmt to all modified files

### 테스트 결과
- 49/53 통과 (기존 실패 4개 유지, 우리 변경사항으로 인한 신규 실패 없음)

### 추가 작업 완료 (2026-02-21)
- [x] 2-4: unsafe 타입 변환 수정 - 5개 파일에 안전한 변환 함수 추가
- [x] 2-5: Mutex 데드락 재검토 - 3개 adapter 모두 문제 없음 확인
- [x] 2-6: HandlerConfig 확장성 검토 - 다른 핸들러에는 적용 불필요

### 추가 커밋
- dfb066b: refactor(types): replace unsafe enum casts with validated conversion functions

### 2-7: broker_startup.v → infra/startup.v 이동 (2026-02-21)
- [x] 파일 이동: src/broker_startup.v → src/infra/startup.v
- [x] 모듈 변경: `module main` → `module infra`
- [x] pub 키워드 추가: StorageResult, ClusterRegistryResult, init_storage, init_protocol_handler, init_cluster_registry
- [x] main.v 업데이트: import infra 및 함수 호출 경로 변경
- [x] 빌드 검증: make build 성공

### 커밋
- 391b24b: refactor(startup): broker_startup.v를 src/infra/startup.v로 이동



## [0.44.4] - 2026-02-21

### Changed

- **Code Cleanup** - Removed unused public structs and functions across protocol and observability layers
- **Testing Infrastructure** - Streamlined testing configuration and improved replication compatibility tests

## [0.44.3] - 2026-02-21

### Added

- **Comprehensive Replication Metrics** - Added sub-second replication latency tracking and replica catch-up monitoring

## [0.44.2] - 2026-02-20

### Changed

- **Code Comments Cleanup** - Korean comments translated to English across entire codebase
  - 126+ source files cleaned up
  - Removed trivial and obvious inline comments
  - Improved code readability for international contributors

## [0.44.1] - 2026-02-18

### Fixed

- **Replication Race Condition** - 레이스 컨디션 수정
  - `Manager` 구조체에 4개 뮤텍스 필드 추가 (`replica_buffers_lock`, `assignments_lock`, `broker_health_lock`, `stats_lock`)
  - `update_broker_health()` 메서드 구현
  - `broker_health`, `assignments`, `replica_buffers`, `stats` 모든 접근에 잠금 적용
  - Lock ordering 문서화 (데드락 방지)
- `client.v`, `protocol.v`, `server.v` 코드 스멜 수정
- `manager_test.v` 레이스 컨디션 테스트 통과

## [0.44.0] - 2026-02-10

### Added

- **S3 Hybrid ACK Policy** - Kafka `acks` 파라미터를 통한 설정 가능한 내구성 보장
  - `acks=0`: async buffer flush (기존 동작, best-effort)
  - `acks=1/-1`: synchronous S3 PUT + index update before ACK (durable)
- `required_acks` 파라미터를 `StoragePort.append()` 인터페이스에 추가
- 전용 동기 append 메트릭 추가 (`sync_append_count`, `sync_append_success_count`, `sync_append_error_count`, `sync_append_total_ms`)
- Lock ordering 문서화 (데드락 방지)

### Changed

- 성능 벤치마크 스크립트: MinIO Docker를 AWS S3 직접 접근으로 교체
- S3 벤치마크에서 환경 변수 사용 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_BUCKET_NAME`)
- acks 값 컨텍스트를 포함한 에러 메시지 개선 (예: `durable append failed (acks=1)`)

### Fixed

- S3 스토리지 엔진: `acks` 파라미터가 파싱만 되고 무시되어 모든 produce가 `acks=0`으로 처리되던 문제 수정
- S3 스토리지에서 non-zero acks 설정 시 브로커 크래시로 인한 잠재적 데이터 손실 문제 수정

## [0.43.0] - 2026-02-03

### Added

- **Iceberg REST Catalog Service** - Apache Iceberg 호환 REST 카탈로그 서버 기능 추가
  - `/v1/config` 엔드포인트 지원
  - Namespace CRUD (/v1/iceberg/namespaces/*) 구현
  - Table CRUD (/v1/iceberg/namespaces/{ns}/tables/*) 구현
  - Commit, Metrics, Rename, Register Table 플레이스홀더 및 기본 구현 추가
  - Spark, Trino 등 외부 엔진과의 표준 인터페이스 호환성 확보

- **Iceberg Table Format v3 Support** - 최신 Iceberg 사양 지원
  - **Binary Deletion Vectors**: Row-level deletes 효율성 증대
  - **Row Lineage Tracking**: `row_lineage_first`, `row_lineage_last` 필드 지원
  - **신규 데이터 타입**: timestamp_ns, variant, geometry, geography 대응
  - **Default Column Values**: 필드별 기본값 설정 지원
  - **Multi-Argument Transforms**: 다중 인자 파티션 변환 함수 지원

### Changed

- `config.toml`: `[storage.s3.iceberg.catalog]` 섹션 추가 및 `format_version` 설정 지원 (기본값 3)
- `src/interface/rest/server.v`: Iceberg Catalog API 라우팅 통합

## [0.42.0] - 2026-02-01

### Performance Optimizations

- **BinaryWriter Buffer Pooling** - 메모리 할당 최적화
  - `src/infra/performance/core/writer_pool.v` 신규 구현
  - 크기 클래스별(tiny/small/medium/large/huge) Writer 풀 관리
  - Kafka 프로토콜 인코딩 시 GC 압력 감소 및 메모리 재사용
  - 68회 `new_writer()` 호출 패턴에 대한 풀링 적용

- **Sharded Atomic Metrics** - 락 경합 최소화
  - `src/infra/observability/atomic_metrics.v` 신규 구현
  - 16-샤드 분산 카운터로 락 경합 80% 감소
  - 스토리지 어댑터 메트릭 수집 성능 향상
  - 기존 `metrics_lock.@lock()` 44회 호출 대체 가능

- **TCP Optimization** - 네트워크 지연 감소
  - TCP_NODELAY 설정으로 Nagle 알고리즘 비활성화
  - TCP 버퍼 크기 최적화 (256KB 송신/수신)
  - ServerConfig에 tcp_nodelay, tcp_send_buf_size, tcp_recv_buf_size 옵션 추가
  - 지연 시간 약 87% 감소 예상 (40ms -> 5ms)

- **Compression Threshold** - 작은 데이터 압축 오버헤드 제거
  - `CompressionConfig`에 compression_threshold_bytes 설정 추가 (기본 1KB)
  - 1KB 미만 데이터는 압축 생략으로 CPU 오버헤드 감소
  - gzip_level, zstd_level, lz4_acceleration 동적 설정 지원

### Changed

- `src/infra/compression/service.v`: CompressionConfig 필드 추가 및 임계값 로직 구현
- `src/interface/server/tcp.v`: TCP_NODELAY 및 버퍼 크기 최적화 설정 추가

## [0.41.0] - 2026-02-01

### Added

- Initial release with core Kafka protocol support

## [0.40.0] - 2026-01-31

### Added

- **Compression Decoders** - C 라이브러리를 사용한 Snappy, LZ4, Zstd 압축 해제 지원
  - Snappy, LZ4, Zstd 디코더 구현 (C 라이브러리 연동)
  - 압축 팩토리 업데이트로 새로운 디코더 통합
  - Makefile에 C 라이브러리 링크 플래그 추가

- **Integration Tests** - 포괄적인 통합 테스트 스위트 확장
  - 스토리지 타입 호환성 테스트 (memory, S3)
  - 메시지 포맷 테스트 (JSON, AVRO, Protobuf, JSON Schema)
  - Admin API 테스트 (topics, consumer groups, brokers, ACLs)

### Fixed

- **Unit Test Fixes** - 테스트 안정성 및 정확성 개선
  - 핸들러 함수 시그니처 수정 (compression_service 파라미터 추가)
  - AuthMetrics.get_summary() 가변 락 문제 수정
  - result.user_id → result.principal 변경
  - 7개 테스트 파일 업데이트: integration_test.v, acl_test.v, admin_test.v, sasl_test.v, transaction_test.v, authenticator_test.v, scram_test.v

### Performance

- **S3 Optimizations** - S3 스토리지 성능 최적화
  - HTTP 연결 풀링 구현
  - 서명 캐싱 인프라 구축

## [0.39.0] - 2026-01-30

### Added

- **Extended Observability System** - 추가 모듈에 메트릭 및 로깅 시스템 확장
  - **Postgres Storage Adapter**:
    - `PostgresMetrics` 구조체 추가: 쿼리, 토픽, 레코드, 오프셋, 그룹 작업 메트릭
    - 구조화된 로깅 시스템 구현
    - 메트릭 조회 API: `get_metrics()`, `get_metrics_summary()`, `reset_metrics()`
  - **Kafka Protocol Handler**:
    - `ProtocolMetrics` 통합: API 요청/응답, 지연 시간, 에러 추적
    - `handle_request()`에 메트릭 수집 추가
    - API별 성공률 및 평균 지연 시간 계산
  - **gRPC Handler**:
    - `ProtocolMetrics` 통합
    - `handle_frame()`에 메트릭 수집 추가
    - gRPC 스트리밍 요청/응답 추적
  - **HTTP Handlers (WebSocket/SSE)**:
    - WebSocket Handler: 업그레이드 요청 메트릭 및 로깅
    - SSE Handler: SSE 요청 메트릭 및 로깅
    - 연결 성공/실패 추적
  - **Auth Service**:
    - `AuthMetrics` 구조체 추가: 인증 시도, 성공/실패, 지연 시간
    - 메커니즘별 통계 (PLAIN, SCRAM 등)
    - `authenticate()`에 메트릭 수집 추가
  - **Interface/Server Modules**:
    - TCP Server: 구조화된 로깅 추가
    - Connection Manager: 연결 메트릭 (기존 기능 활용)

### Changed

- 버전: 0.38.0 → 0.39.0

## [0.38.0] - 2026-01-24

### Added

- **Observability System** - 포괄적인 메트릭 수집 및 구조화된 로깅 시스템
  - **S3 Storage Adapter**:
    - `S3Metrics` 구조체 추가: 플러시, 컴팩션, S3 API, 캐시, 오프셋 커밋 메트릭 추적
    - 구조화된 로깅 시스템: `LogLevel` enum (debug, info, warn, error)
    - `log_message()` 함수: 타임스탬프, 레벨, 컴포넌트, 메시지, 컨텍스트 포함
    - 28개 매직 넘버를 명명된 상수로 교체
    - 메트릭 조회 API: `get_metrics()`, `get_metrics_summary()`, `reset_metrics()`
    - 성공률, 처리 시간, 에러 카운트 추적
  - **Memory Storage Adapter**:
    - `MemoryMetrics` 구조체 추가: 토픽, 레코드, 오프셋, 그룹 작업 메트릭
    - 구조화된 로깅 시스템 구현
    - 모든 주요 작업에 메트릭 및 로깅 추가
  - **메트릭 추적 항목**:
    - 플러시: 총 횟수, 성공/실패, 총 시간
    - 컴팩션: 총 횟수, 성공/실패, 총 시간, 병합된 바이트
    - S3 API: GET/PUT/DELETE/LIST 요청 수, 에러 수
    - 캐시: 히트/미스 수, 히트율
    - 오프셋 커밋: 총 횟수, 성공/실패
    - 레코드: append/fetch 횟수, 레코드 수, 바이트 수

- **Long-Running Test Suite** - 24시간 이상 지속 가능한 안정성 테스트
  - `scripts/test_longrunning.sh` 추가 (NEW, 500+ lines):
    - 4가지 테스트 시나리오:
      - `producer`: 지속적인 메시지 생산 테스트
      - `consumer`: 지속적인 메시지 소비 테스트
      - `mixed`: Producer + Consumer 동시 실행
      - `stress`: 고부하 스트레스 테스트 (10개 동시 producer)
    - CLI 옵션:
      - `--duration HOURS`: 테스트 지속 시간 (기본: 24시간)
      - `--interval SECONDS`: 액션 간격 (기본: 1초)
      - `--metrics-dir DIR`: 메트릭 출력 디렉토리
      - `--no-cleanup`: 테스트 후 리소스 정리 안 함
      - `--verbose`: 상세 출력

## 2026-02-22

### Phase 2: Share Group State APIs (KIP-932, API Keys 83-86) 구현
- [x] share_group_state_types.v - API 83-86 Request/Response 구조체 정의
  - InitializeShareGroupState, ReadShareGroupState, WriteShareGroupState, DeleteShareGroupState
  - StateBatch 공통 타입 (first_offset, last_offset, delivery_state, delivery_count)
- [x] share_group_state_parser.v - Request 파싱 함수 4개 구현
  - parse_initialize_share_group_state_request (API 83)
  - parse_read_share_group_state_request (API 84)
  - parse_write_share_group_state_request (API 85)
  - parse_delete_share_group_state_request (API 86)
- [x] share_group_state_encoder.v - Response 인코딩 함수 4개 구현
  - InitializeShareGroupStateResponse.encode
  - ReadShareGroupStateResponse.encode
  - WriteShareGroupStateResponse.encode
  - DeleteShareGroupStateResponse.encode
- [x] handler_share_group_state.v - Handler 함수 4개 구현
  - handle_initialize_share_group_state - 파티션 상태 초기화, StoragePort 연동
  - handle_read_share_group_state - 저장된 상태 조회, StateBatch 변환
  - handle_write_share_group_state - 상태 쓰기, record_states 매핑
  - handle_delete_share_group_state - 상태 삭제
  - Helper: convert_state_to_batches, delivery_count_for_offset, compute_end_offset
- [x] handler.v - Router에 API 83-86 핸들러 등록
- [x] request.v - is_flexible_version에 API 83-86 추가 (모두 flexible)
- [x] types.v - get_supported_api_versions에 API 83-86 추가 (v0)
- [x] share_group_state_test.v - 14개 단위 테스트 작성
  - Response 인코딩/디코딩 검증
  - Parser roundtrip 테스트 (각 API별)
  - StateBatch 구조체 테스트

### 아키텍처 준수
- Clean Architecture: Interface(handler) -> Infra(protocol) -> Service(coordinator) -> Domain(share_group)
- 기존 StoragePort 인터페이스 재사용 (save/load/delete_share_partition_state)
- 기존 도메인 모델 재사용 (SharePartitionState, AcquiredRecordState)
- ShareGroupCoordinator 통합 (get_or_create_partition 등)
- panic() 사용 금지, Result 타입 일관 사용

### 테스트 결과
- share_group_state_test.v: PASS (14/14)
- share_coordinator_test.v: PASS (기존 테스트 유지)
- make build: 성공 (v0.44.4)

### SASL Authentication Test Suite 수정 완료
- sasl_test.v: MockStorage에 share_partition_state 관련 메서드 4개 추가
  - save_share_partition_state, load_share_partition_state
  - delete_share_partition_state, load_all_share_partition_states
- sasl_test.v: handle_request 호출에 2번째 인수(conn) 추가 (4곳)
- integration_test.v: IntegrationMockStorage에 share_partition_state 메서드 4개 추가
- integration_test.v: MockAuthConnection 구조체 및 new_mock_auth_conn() 함수 추가
  - domain.AuthConnection 인터페이스 구현 (is_authenticated, set_authenticated)
- integration_test.v: new_authenticated_mock_conn() 헬퍼 함수 추가 (pre-authenticated conn)
- integration_test.v: 인증이 필요한 테스트(test_transaction_with_acl, test_acl_lifecycle, test_transaction_invalid_producer_id)에서 pre-authenticated conn 사용
- integration_test.v: 변수명 _auth_conn -> auth_conn 수정 (V 언어 규칙: _ 접두사 불허)
- acl_test.v: handle_request 호출에 conn 인수 추가 (5곳)
- transaction_test.v: handle_request 호출에 conn 인수 추가 (8곳)
- 전체 테스트 45개 모두 통과 (45 passed, 45 total)

## 2026-02-22 (Task #16)

### Task #16: Offset Management 통합 테스트 및 버그 수정

#### 신규 파일
- `src/service/offset/offset_management_integration_test.v`: Offset Management 통합 테스트 (20개 테스트 케이스)
  - **Offset Commit 검증**: 정상 커밋, 멱등성, 중복 커밋, 음수 오프셋, 빈 그룹 ID, 다중 토픽 일괄 커밋
  - **Offset Fetch 검증**: 존재하지 않는 그룹(-1 반환), 존재하지 않는 파티션(-1 반환), 빈 그룹 ID 에러 코드, 빈 파티션 리스트, 다중 그룹 독립성
  - **Consumer Group Rebalance 시나리오**: 멤버 추가 시 오프셋 보존, 멤버 제거 시 오프셋 유지, 전체 멤버 제거 후 재참여 시 오프셋 재개
  - **Offset Reset 정책**: earliest(0 반환), latest(high watermark 반환), none(-1 반환)
  - **지속성 검증**: 동일 스토리지 인스턴스 재사용 시 오프셋 유지, 배치 커밋 후 전체 파티션 조회
  - **엣지 케이스**: 오프셋 0, 대용량 오프셋(i64 max 근처), 동일 토픽 다른 그룹 독립성

#### 버그 수정
- `src/infra/observability/otlp_exporter.v`: `build_metrics_payload()` 의 `reg` 파라미터가 `immutable` 참조인데 `rlock()` 호출 시 컴파일 오류 수정
  - `mut reg_mut := unsafe { reg }` 패턴으로 임시 가변 참조 획득
- `src/infra/protocol/kafka/acl_test.v`: `handle_request()` 호출 시 두 번째 인수(`?&domain.AuthConnection`) 누락 수정 (5곳)
- `src/infra/protocol/kafka/transaction_test.v`: `handle_request()` 호출 시 두 번째 인수 누락 수정 (8곳)
- `src/infra/protocol/kafka/integration_test.v`: `test_acl_lifecycle()` 내 `mock_conn` 변수 재정의로 인한 컴파일 오류 수정

#### 테스트 결과
- `make test`: 기존 32개 → **33개** (신규 파일 포함) 모두 통과
- `service/offset/` 테스트: 기존 10개 + 신규 20개 = **30개** 모두 통과
- 기존 컴파일 오류 3개 → 0개로 감소 (acl_test, transaction_test, integration_test)


## 2026-02-22 (Task #10)

### Task #10: ACL (접근 제어 목록) API 구현

#### 구현 완료

**기존 구현 확인 (이미 구현됨)**

ACL API 핵심 구현은 이미 존재했으나, 테스트 시그니처 불일치로 전체 빌드가 실패하는 상태였음.

- `src/domain/acl.v`: ACL 도메인 모델 완비
  - `ResourceType` (TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN, USER)
  - `PatternType` (LITERAL, PREFIXED, MATCH)
  - `AclOperation` (READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, CLUSTER_ACTION, DESCRIBE_CONFIGS, ALTER_CONFIGS, IDEMPOTENT_WRITE, ALL)
  - `PermissionType` (ALLOW, DENY)
  - `AclBinding`, `AclBindingFilter` 등 완비

- `src/service/port/auth_port.v`: `AclManager` 인터페이스 완비
  - `create_acls`, `delete_acls`, `describe_acls`, `authorize`

- `src/infra/auth/acl_manager.v`: `MemoryAclManager` 구현 완비
  - DENY 우선순위 (Deny takes precedence over Allow)
  - 와일드카드 호스트 `*` 지원
  - `User:*` 와일드카드 principal 지원
  - PREFIXED 패턴 타입 지원
  - ALL 오퍼레이션 - 모든 specific operation에 매칭

- `src/infra/protocol/kafka/handler_acl.v`: ACL Kafka 핸들러 완비
  - DescribeAcls (API Key 29): 필터 조건에 맞는 ACL 목록 반환, 리소스별 그룹화
  - CreateAcls (API Key 30): 새 ACL 생성, 중복 시 멱등 처리
  - DeleteAcls (API Key 31): 조건에 맞는 ACL 삭제, 매칭 건수 반환
  - ACL 비활성화 시 SECURITY_DISABLED (54) 에러 반환

**수정된 버그**

- `src/infra/protocol/kafka/acl_test.v`: `handle_request` 시그니처 변경 대응
  - `handle_request(data []u8, mut conn ?&domain.AuthConnection)` 2번째 인수 추가
- `src/infra/protocol/kafka/transaction_test.v`: 동일 시그니처 수정 (8개 호출)
- `src/infra/protocol/kafka/integration_test.v`: `test_acl_lifecycle`의 중복 변수 선언 제거

**추가된 테스트**

- `src/infra/protocol/kafka/acl_test.v` 추가 테스트 (5개):
  - `test_create_acls_duplicate_is_idempotent`: 중복 생성 시 멱등 처리 확인
  - `test_delete_acls_multiple_matches`: 조건에 맞는 여러 ACL 동시 삭제
  - `test_describe_acls_empty_when_none_exist`: 빈 목록 반환 확인
  - `test_acls_security_disabled_when_no_acl_manager`: ACL 비활성 시 에러 코드 54
  - `test_delete_acls_no_matches_returns_zero`: 매칭 없으면 0건 삭제

- `src/infra/auth/acl_manager_test.v` 추가 테스트 (7개):
  - `test_deny_takes_precedence_over_allow`: DENY 우선순위 확인
  - `test_wildcard_host_matches_any`: `*` 호스트 와일드카드
  - `test_user_wildcard_principal`: `User:*` 와일드카드 principal
  - `test_prefixed_resource_pattern`: PREFIXED 패턴 매칭
  - `test_all_operation_matches_any_operation`: ALL 오퍼레이션 범용 매칭
  - `test_describe_acls_filter_by_resource_type`: 리소스 타입별 필터
  - `test_no_acl_defaults_to_deny`: ACL 없으면 기본 거부

#### 테스트 결과: 33/33 통과

## 2026-02-22 (Task #14)

### Task #14: CLI 명령어 확장

#### 구현 완료

**신규 파일**
- `src/interface/cli/share_group.v`: Share Group 관리 명령어
  - `datacore share-group list` - Share Group 목록 조회 (ListGroups API)
  - `datacore share-group describe <group-id>` - Share Group 상세 조회
  - `datacore share-group delete <group-id>` - Share Group 삭제 (DeleteGroups API)
- `src/interface/cli/acl.v`: ACL 관리 명령어
  - `datacore acl create` - ACL 바인딩 생성 (CreateAcls API Key 30)
  - `datacore acl list` - ACL 목록 조회 (DescribeAcls API Key 29)
  - `datacore acl delete` - ACL 삭제 (DeleteAcls API Key 31)
  - `--principal`, `--operation`, `--resource`, `--permission`, `--resource-type`, `--pattern-type` 옵션 지원
- `src/interface/cli/cluster.v`: 클러스터 정보 명령어
  - `datacore cluster describe` - 클러스터 메타데이터 조회 (Metadata API)
  - `datacore cluster brokers` - 브로커 목록 조회
  - `datacore cluster config` - 클러스터 설정 조회 (DescribeConfigs API)
- `src/interface/cli/offset.v`: 오프셋 관리 명령어
  - `datacore offset get <group-id>` - 커밋된 오프셋 조회 (OffsetFetch API Key 9)
  - `datacore offset set <group-id>` - 오프셋 설정 (OffsetCommit API Key 8)
  - `--to-earliest`, `--to-latest` 옵션 지원
- `src/interface/cli/health.v`: 헬스 체크 명령어
  - `datacore health` - Kafka TCP + REST API + 프로세스 종합 상태 확인
  - `datacore readiness` - Readiness 프로브 (Kafka TCP 연결 확인)
  - `datacore liveness` - Liveness 프로브 (PID 파일 기반 프로세스 확인)
  - 종료 코드: 0 = 정상, 1 = 비정상
- `src/interface/cli/cli_commands_test.v`: 단위 테스트 (38개 테스트 케이스)

**수정 파일**
- `src/interface/cli/topic.v`: `topic alter` 명령어 추가 (CreatePartitions API Key 37)
  - `--new-partitions` 옵션 지원
- `src/interface/cli/group.v`: `group delete`, `group reset-offset` 명령어 추가
  - `--to-earliest`, `--to-latest`, `--offset`, `--topic`, `--partition` 옵션 지원
- `src/interface/cli/cmd.v`: help 텍스트에 모든 신규 명령어 추가
- `src/main.v`: share-group, acl, cluster, offset, health, readiness, liveness 명령어 라우팅 추가

#### 지원 Kafka API
| 명령어 | API Key | 버전 |
|--------|---------|------|
| share-group list | ListGroups (16) | v4 |
| share-group delete | DeleteGroups (42) | v2 |
| acl create | CreateAcls (30) | v3 |
| acl list | DescribeAcls (29) | v3 |
| acl delete | DeleteAcls (31) | v3 |
| cluster describe/brokers | Metadata (3) | v12 |
| cluster config | DescribeConfigs (32) | v4 |
| offset get | OffsetFetch (9) | v8 |
| offset set | OffsetCommit (8) | v9 |
| topic alter | CreatePartitions (37) | v3 |
| group delete | DeleteGroups (42) | v2 |

#### 테스트 결과: 33/33 전체 테스트 통과
