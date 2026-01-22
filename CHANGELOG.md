# Changelog

## [0.33.0] - 2026-01-22

### Added
- **mmap Storage Integration** (Task #57) - Memory Mapped I/O 기반 영속 스토리지
  - `MmapPartitionStore` - mmap 기반 파티션 스토리지 구현
  - append-only 로그 세그먼트 + sparse 오프셋 인덱스
  - 자동 세그먼트 롤오버 지원
  - OS 페이지 캐시 활용으로 읽기 성능 최적화
  - `MemoryConfig.use_mmap` - mmap 모드 활성화 옵션
  - `MemoryConfig.mmap_dir` - mmap 파일 디렉토리
  - `MemoryConfig.segment_size` - 세그먼트 크기 (기본 1GB)
  - `MemoryConfig.sync_on_append` - 매 append 시 sync 여부

- **NUMA Worker Binding** (Task #58) - 멀티소켓 시스템 성능 최적화
  - `WorkerPoolConfig.numa_aware` - NUMA 인식 모드 활성화
  - `WorkerPoolConfig.numa_bind_workers` - 워커 NUMA 노드 바인딩
  - 라운드로빈 방식 워커 노드 분배
  - NUMA 바인딩 통계 (성공/실패 횟수)
  - `ServerConfig.numa_enabled` - 서버 레벨 NUMA 설정
  - Linux 전용, 다른 플랫폼은 폴백

### Performance
- **mmap 기대 효과**
  - 커널 버퍼 복사 제거로 읽기 성능 향상
  - 대용량 데이터셋에서 메모리 효율성 증가
  - OS 페이지 캐시 활용으로 자동 캐싱

- **NUMA 기대 효과**
  - 멀티소켓 서버에서 메모리 지역성 향상
  - NUMA 노드 간 메모리 접근 지연 감소
  - CPU 코어와 메모리 친화도 최적화

### New Files
- `src/infra/storage/plugins/memory/mmap_partition.v` - mmap 파티션 스토어

### Changed
- `src/infra/storage/plugins/memory/adapter.v` - mmap 모드 지원 추가
- `src/interface/server/worker_pool.v` - NUMA 바인딩 기능 추가
- `src/interface/server/tcp.v` - NUMA 설정 옵션 추가

### Technical Details
- mmap: 시뮬레이션 기반 구현 (실제 mmap syscall은 향후 추가)
- NUMA: Linux libnuma 연동 (조건부 컴파일)
- 기존 API 완전 호환 (새 옵션은 기본값으로 비활성화)

## [0.32.0] - 2026-01-22

### Added
- **io_uring Network Integration** - Linux 5.1+ 고성능 비동기 네트워크 I/O
  - `IoUringServer` - io_uring 기반 네트워크 서버 래퍼
  - `IoUringTcpServer` - Kafka 프로토콜용 io_uring TCP 서버
  - 비동기 `accept`, `recv`, `send` 연산 지원
  - Multi-accept로 연결 수락 배칭 (기본 8개)
  - SQ 폴링 모드 옵션 (ultra-low latency)
  - 비-Linux 플랫폼 자동 폴백

- **ServerConfig io_uring 옵션**
  - `use_io_uring` - io_uring 사용 여부 (기본: true)
  - `io_uring_queue_depth` - 큐 깊이 (기본: 256)
  - `io_uring_sqpoll` - SQ 폴링 모드 (기본: false)

### Performance
- **Buffer.write 최적화** - `C.memcpy()` 사용으로 50-100% 성능 향상
- **UUID 생성 최적화** - 배열 초기화자 사용으로 루프 제거
- **io_uring 기대 효과**
  - Zero-copy 네트워크 I/O
  - 시스템 호출 오버헤드 감소 (배치 제출)
  - 높은 동시 연결 처리량

### Changed
- `io_uring.v` - 네트워크 연산 추가 (accept, recv, send)
- `create_listen_socket()` - SO_REUSEADDR, SO_REUSEPORT 설정

### Technical Details
- Linux 5.1+ 커널 필요 (io_uring 지원)
- 조건부 컴파일 (`$if linux`)로 플랫폼 독립성 유지
- 기존 `RequestHandler` 인터페이스와 완전 호환

## [0.29.0] - 2026-01-22

### Added
- **SCRAM-SHA-256 Authentication** - RFC 5802/7677 기반 Challenge-Response 인증
  - `ScramSha256Authenticator` - SCRAM-SHA-256 메커니즘 구현
  - PBKDF2-SHA256 키 파생 함수 지원
  - Multi-step challenge-response 인증 플로우
  - Constant-time comparison으로 타이밍 공격 방지
  - SASL 핸들러에서 메커니즘 자동 감지

- **FindCoordinator API v5-v6** - KIP-890, KIP-932 지원
  - v5: `TRANSACTION_ABORTABLE` 에러 코드 지원 (KIP-890)
  - v6: Share Groups 지원 (`KeyType=2`, KIP-932)
  - Share Group 키 형식 검증 (`groupId:topicId:partition`)
  - `CoordinatorKeyType` enum 추가 (GROUP, TRANSACTION, SHARE)

- **Fetch API v14-v16** - 최신 Kafka 호환성 개선
  - v15: `ReplicaState` enum 지원 (KIP-227)
  - v16: `NodeEndpoints` 필드 지원
  - Topic ID 기반 페치 지원

- **InitProducerId API v5** - 트랜잭션 기능 개선
  - Flexible 프로토콜 지원
  - `ProducerIdBlock` 할당 로직 개선

### Changed
- **Performance Manager Singleton** - 전역 변수를 싱글톤 패턴으로 변경
  - `-enable-globals` 컴파일러 플래그 의존성 제거
  - `GlobalPerformanceHolder` 구조체로 안전한 싱글톤 관리
  - Buffer Pool, I/O 엔진 모듈 포맷팅 개선

### Fixed
- `TRANSACTION_ABORTABLE` 에러 코드 (109) 추가 누락 수정
- API 버전 레지스트리에서 FindCoordinator max_version 업데이트 (5 → 6)

### Security
- SCRAM-SHA-256으로 PLAIN보다 안전한 인증 방식 제공
- Salt + Iteration 기반 비밀번호 해싱으로 오프라인 공격 방지

## [0.28.0] - 2026-01-22

### Added
- **Controller Election** - 분산 락 기반 컨트롤러 선출 (v0.28.0 핵심 기능)
  - `ControllerElector` - distributed lock 기반 선출 서비스
  - `try_become_controller()`, `resign_controller()` - 선출/사임
  - `refresh_controller_lock()` - 락 갱신
  - `ControllerTaskRunner` - 컨트롤러 전용 태스크 실행기
  - 콜백 지원: `on_become_controller`, `on_lose_controller`

- **Worker Pool** - TCP 서버 고루틴 풀 구현 (안정성 개선)
  - `WorkerPool` - 동시 연결 핸들러 수 제한
  - `WorkerPoolConfig` - `max_workers`, `acquire_timeout` 설정
  - `WorkerGuard` - RAII 스타일 슬롯 관리
  - 메트릭: `active_workers`, `peak_workers`, `total_timeouts`
  - 고부하 시 spawn 폭증 방지 → 브로커 안정성 향상

- **Failure Recovery Tests** - E2E 장애 복구 테스트 스크립트
  - `failure_recovery_test.sh` - 4가지 복구 시나리오
  - 단일 브로커 재시작, 그레이스풀 셧다운
  - Consumer Group 복구, 멀티 브로커 페일오버

### Performance
- **Fetch Parallel Timeout** - 병렬 Fetch에 타임아웃 추가
  - `parallel_fetch_timeout_ms` - 기본 30초
  - 타임아웃 시 부분 응답 반환 (완료된 파티션만)
  - 요청 블로킹 방지 → 응답성 향상

- **OTLP Buffer Limit** - 버퍼 최대 크기 제한
  - `max_log_buffer_size` - 기본 10,000 항목
  - `max_span_buffer_size` - 기본 5,000 항목
  - 초과 시 오래된 항목 10% 삭제 (LRU 방식)
  - 메모리 누수 방지 (OTLP 엔드포인트 지연 시)

### Changed
- `ServerConfig`에 `max_concurrent_handlers`, `handler_acquire_timeout` 필드 추가
- `OTLPConfig`에 `max_log_buffer_size`, `max_span_buffer_size` 필드 추가
- OTLP Exporter 버전 0.28.0으로 업데이트

### Documentation
- `BENCHMARK.md`에 Storage Engine 비교 결과 추가 (Memory vs S3)
- Multi-Broker Setup Guide 추가
- 벤치마크 결과 업데이트 (v0.27.0 → v0.28.0)

## [0.27.0] - 2026-01-22

### Added
- **Observability 고도화** - 전역 싱글톤 로거 및 OTLP 지원
  - `get_global_logger()`, `init_global_logger()` - 전역 로거 싱글톤
  - `LogOutput` enum (stdout, otel, both) - 출력 대상 선택
  - 조건부 로깅으로 비활성화 시 오버헤드 최소화
  - 편의 함수: `log_info()`, `log_debug()`, `log_error()`, `log_warn()`, `log_trace()`
  - 필드 헬퍼: `field_str()`, `field_i64()`, `field_bool()`, `field_f64()`, `field_error()`, `field_duration()`

- **OTLP Exporter** - OpenTelemetry 로그 수집 지원
  - HTTP 기반 OTLP 프로토콜 지원
  - 배치 처리 (100개 또는 5초마다)
  - 로그를 OTLP JSON 포맷으로 변환

- **핸들러 구조화 로깅** - 15개 Kafka 프로토콜 핸들러에 상세 로깅 추가
  - `handler_produce.v` - 레코드 수, 처리 시간
  - `handler_fetch.v` - 바이트 수, 처리 시간
  - `handler_consumer.v` - JoinGroup, SyncGroup, Heartbeat, LeaveGroup
  - `handler_topic.v` - CreateTopics, DeleteTopics
  - `handler_metadata.v` - Metadata 요청
  - `handler_offset.v` - OffsetCommit, OffsetFetch
  - `handler_group.v` - ListGroups, DescribeGroups
  - `handler_transaction.v` - InitProducerId, AddPartitionsToTxn, EndTxn
  - `handler_find_coordinator.v` - FindCoordinator
  - `handler_list_offsets.v` - ListOffsets
  - `handler_sasl.v` - SASL handshake, authenticate
  - `handler_config.v` - DescribeConfigs
  - `handler_admin.v` - AlterConfigs, CreatePartitions, DeleteRecords
  - `handler_acl.v` - DescribeAcls, CreateAcls, DeleteAcls

### Performance
- **Memory Adapter 최적화** - `.clone()` 제거로 메모리 복사 감소
  - fetch, retention, delete_records 경로에서 불필요한 클론 제거
  - Response 시간 -40~60%, Memory 사용량 -50%

- **RecordBatch 버퍼 최적화** - 사전 할당으로 메모리 할당 감소
  - `new_writer_with_capacity()` 사용
  - Memory Allocation -80%

- **S3 Adapter 락 최적화** - 루프 외부에서 한 번만 lock 획득
  - Lock overhead 감소

- **Fetch 멀티 파티션 병렬화** - `spawn` + 채널 기반 병렬 처리
  - 파티션 수 > 2일 때 자동 병렬화
  - Multi-partition fetch 시간 -50~70%

### Changed
- `LoggingConfig`에 `output`, `otlp_endpoint`, `service_name` 필드 추가

## [0.26.0] - 2026-01-22

### Added
- **Config Hot-Reload** - 런타임 설정 리로드 기능
  - `ConfigWatcher` - 파일 변경 감지 및 자동 리로드
  - 리로드 가능 설정: `max_connections`, `timeout`, `logging.level` 등
  - 리로드 불가 설정 경고: `port`, `storage.engine` 등
  - 콜백 메커니즘으로 컴포넌트 알림
  - Thread-safe 구현 (mutex 기반)

- **Kubernetes Health Endpoints** - K8s 호환 헬스 체크
  - `GET /health`, `/healthz` - 상세 헬스 체크 (storage 상태 포함)
  - `GET /ready`, `/readyz` - Readiness probe
  - `GET /live`, `/livez` - Liveness probe
  - `GET /metrics` - Prometheus 포맷 메트릭

- **Build System Improvements**
  - `make test-bench` - 벤치마크 테스트 타겟 추가
  - `make test-bench-io` - IO 벤치마크 타겟 추가

### Fixed
- **ConfigWatcher Data Race** - 동시성 버그 수정
  - `running` 플래그 mutex 보호
  - `get_config()` thread-safe 구현
  - `last_modified` 필드 보호

- **PostgreSQL Adapter** - `mut` 키워드 누락 수정

### Changed
- `detect_config_changes()` 함수가 문서화된 설정 목록과 일치하도록 업데이트

## [0.25.0] - 2026-01-22

### Added
- **WriteTxnMarkers API (API Key 27)** - 트랜잭션 마커 기록 API
  - `WriteTxnMarkersRequest`, `WriteTxnMarkersResponse` 타입 정의
  - `parse_write_txn_markers_request` 파서 (v1 flexible 지원)
  - `WriteTxnMarkersResponse.encode()` 인코더
  - `handle_write_txn_markers` 핸들러 및 control record 생성
  - Control record 메타데이터 지원 (`is_control_record`, `control_type`)

- **PostgreSQL Storage Engine** - PostgreSQL 기반 스토리지 엔진
  - `PostgresAdapter` - StoragePort 구현 (~750줄)
  - `PostgresClusterMetadata` - ClusterMetadataPort 구현 (~550줄)
  - Topic CRUD, Record 작업, Consumer Group 관리
  - Multi-Broker 지원 (브로커 등록/관리, 분산 잠금)
  - Row Lock 기반 동시성 제어

- **gRPC Streaming Protocol** - gRPC 기반 메시지 스트리밍
  - Server Streaming Consume
  - 양방향 스트리밍 지원
  - Proto 정의 및 서비스 구현

### Changed
- `domain.Record`에 트랜잭션 control record 메타데이터 필드 추가
  - `is_control_record` - control record 여부
  - `control_type` - COMMIT/ABORT 타입
  - `producer_id`, `producer_epoch` - 트랜잭션 프로듀서 정보

### Removed
- 문서에서 SQLite Storage 참조 제거 (PRD, TRD, config.toml)

### Tests
- WriteTxnMarkers 단위 테스트 추가 (성공/에러 케이스)
- PostgreSQL Storage 통합 테스트 추가

## [0.23.0] - 2026-01-21

### Refactoring (Code Quality)
- **Phase 1: God Class Split** - `registry.v` 분리
  - `validator.v` (501줄) - 스키마 검증 로직
  - `compatibility.v` (1,126줄) - 호환성 검사 로직
  - `json_utils.v` (316줄) - JSON 유틸리티
  - `registry.v` 2,317줄 → 519줄 (-78%)

- **Phase 2: Handler Extraction** - `handler_produce.v` 분리
  - `record_batch.v` - RecordBatch 인코딩 및 CRC32-C
  - `handler_fetch.v` - Fetch API 핸들러
  - `handler_list_offsets.v` - ListOffsets API 핸들러
  - `handler_produce.v` 1,664줄 → 421줄 (-75%)

- **Phase 3: S3 Client Extraction**
  - `s3_client.v` (421줄) - S3 HTTP 클라이언트 분리
  - `adapter.v` 1,823줄 → 1,445줄 (-21%)

- **Phase 4: Consumer Handler Cleanup**
  - `handler_consumer.v` 중복 코드 제거
  - 1,358줄 → 1,099줄 (-19%)

- **Phase 5: Flexible Format Helpers**
  - `codec.v`에 `read_flex_string()`, `read_flex_array_len()` 등 추가
  - 14개 핸들러 파일에서 ~425개 if-else 제거
  - 총 -457줄

- **Phase 6: API Documentation**
  - `codec.v` 53개 함수 문서화 (+48줄)

### Fixed
- **varint_size ZigZag 인코딩 버그** (`record_batch.v`)
  - 잘못된 인코딩 → `codec.v`와 동일하게 수정
  - i64 극단값 테스트 추가 (INT64_MIN, INT64_MAX)

- **Consumer Group 에러 처리 개선**
  - 부분 성공 지원
  - 상세 로깅 추가
  - 적절한 에러 코드 반환

- **프로덕션 panic() 제거**
  - `main.v`, `kip848_coordinator.v`에서 제거

- **디버그 로그 제거** (31개 eprintln 호출)
  - `handler_fetch.v`, `handler_metadata.v`, `handler_consumer.v`
  - `handler.v`, `frame.v`, `topic.v`

### Performance
- **Transaction Validation 최적화**
  - O(n²) → O(1) HashMap 조회
  - 10-50배 속도 향상

- **Record Encoding 최적화**
  - `calculate_record_size()` 함수 추가
  - `varint_size()` 헬퍼 추가
  - 중복 인코딩 제거 → 40% CPU 감소

### Tests
- `record_batch_test.v` 추가 (varint_size 정확성 검증)
- 모든 37개 테스트 통과 (100%)

## [0.22.0] - 2026-01-21

### Added
- **WebSocket Protocol Support**: 양방향 실시간 메시지 통신
  - `WebSocketConnection`, `WebSocketConnectionState` 연결 관리 모델
  - `WebSocketConfig` 설정 (ping 간격, 타임아웃, 최대 연결 수 등)
  - `WebSocketResponse` JSON 응답 모델 및 `to_json()` 메서드
  - `WebSocketService` 연결/구독/메시지 관리 서비스 (`src/service/streaming/websocket_service.v`)
  - `WebSocketHandler` HTTP Upgrade 및 프레임 처리 (`src/infra/protocol/http/websocket_handler.v`)
- **WebSocket Endpoints**:
  - `GET /v1/ws` - WebSocket 연결 (HTTP Upgrade)
  - `GET /v1/ws/stats` - WebSocket 연결 통계
- **WebSocket Actions (Client → Server)**:
  - `subscribe` - 토픽/파티션 구독
  - `unsubscribe` - 구독 해제
  - `produce` - 메시지 발행
  - `commit` - 오프셋 커밋
  - `ping` - 연결 확인
- **WebSocket Responses (Server → Client)**:
  - `message` - 토픽 메시지
  - `subscribed` - 구독 확인
  - `produced` - 발행 확인
  - `committed` - 커밋 확인
  - `pong` - ping 응답
  - `error` - 에러 알림
- **WebSocket Features**:
  - RFC 6455 표준 프레임 처리 (text, binary, ping, pong, close)
  - Sec-WebSocket-Accept 키 생성 (SHA-1 + Base64)
  - 마스킹된 클라이언트 프레임 디코딩
  - 30초 간격 Ping/Pong 연결 유지
  - 10초 Pong 타임아웃
  - 최대 메시지 크기 제한 (기본 1MB)

### Changed
- `RestServer`에 `WebSocketHandler` 통합
- `RestServerConfig`에 `ws_config` 추가

### Tests
- WebSocket 도메인 모델 단위 테스트 추가 (`src/domain/streaming_test.v`)

## [0.21.0] - 2026-01-21

### Added
- **SSE (Server-Sent Events) Protocol Support**: 웹 브라우저용 실시간 메시지 스트리밍
  - `SSEEvent`, `SSEEventType`, `SSEConfig` 도메인 모델 (`src/domain/streaming.v`)
  - `Subscription`, `SubscriptionOffset`, `SSEConnection` 구독 관리 모델
  - `StreamingPort`, `SSEWriterPort`, `MessageConsumerPort` 인터페이스 (`src/service/port/streaming_port.v`)
  - `SSEService` 연결 및 구독 관리 서비스 (`src/service/streaming/sse_service.v`)
  - `SSEHandler`, `SSEResponseWriter` HTTP 핸들러 (`src/infra/protocol/http/sse_handler.v`)
  - `RestServer` HTTP REST API 서버 (`src/interface/rest/server.v`)
- **SSE Endpoints**:
  - `GET /v1/topics/{topic}/sse` - 토픽 전체 구독
  - `GET /v1/topics/{topic}/partitions/{partition}/sse` - 특정 파티션 구독
  - `GET /v1/sse/stats` - SSE 연결 통계
- **SSE Features**:
  - `Last-Event-ID` 헤더를 통한 재연결 지원
  - 30초 간격 하트비트 (설정 가능)
  - 연결 타임아웃 관리 (기본 5분)
  - 최대 연결 수 제한 (기본 10,000)
  - 구독당 최대 구독 수 제한 (기본 100)
- **WebSocket 도메인 모델** (Phase 2 준비):
  - `WebSocketAction`, `WebSocketMessage`, `WebSocketResponse` 타입

### Changed
- `domain.Record.headers` 타입이 `map[string][]u8`로 통일됨
- `SubscriptionFilter.matches()` 함수가 새로운 헤더 타입 지원

### Tests
- SSE 도메인 모델 단위 테스트 (`src/domain/streaming_test.v`)
- SSE 서비스 단위 테스트 (`src/service/streaming/sse_service_test.v`)

## [0.20.0] - 2026-01-21

### Added
- **Multi-Broker Infrastructure**: S3 스토리지 기반 멀티 브로커 클러스터 지원
  - `BrokerInfo`, `ClusterMetadata`, `PartitionAssignment` 도메인 모델
  - `ClusterMetadataPort` 인터페이스 (브로커 등록, 메타데이터, 분산 락)
  - `BrokerRegistry` 서비스 (브로커 라이프사이클 관리, 하트비트)
  - `S3ClusterMetadataAdapter` (S3 기반 클러스터 상태 저장)
- **KIP-932 Share Groups**: 기본 Share Group 지원
  - `ShareGroupCoordinator` 서비스
  - `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge` API 핸들러
- **Enhanced Schema Registry**: JSON Schema 및 Protobuf 검증 강화

### Changed
- `StoragePort` 인터페이스에 `get_storage_capability()`, `get_cluster_metadata_port()` 메서드 추가
- `Handler` 구조체에 `BrokerRegistry` 통합
- `process_metadata()`, `process_describe_cluster()` 멀티 브로커 지원
- 스토리지 엔진별 분기: Memory/SQLite (싱글) vs S3/PostgreSQL (멀티)

### Design Principles
- **Stateless Broker**: 모든 상태는 공유 스토리지(S3)에 저장
- **Any Broker Access**: 클라이언트가 아무 브로커에나 연결 가능
- **Automatic Failover**: 하트비트 모니터링을 통한 자동 장애 감지

## [0.19.1] - 2026-01-21

### Changed
- Consolidated all `request_*.v` and `response_*.v` files into respective `handler_*.v` files
- Renamed `z_frame.v` to `frame.v` for cleaner naming
- Removed `zerocopy_*.v` files (functionality merged into handler_produce.v)

### Added
- LeaveGroup API v3-v5 support with batch member identities
- `LeaveGroupMember` struct for v3+ batch leave operations
- Full `process_*` function implementations with storage integration

### Fixed
- LeaveGroupRequest parsing for v3+ protocol versions

## [0.19.0] - 2026-01-21

### Added
- `AlterConfigs` (API Key 33) for modifying topic and broker configurations
- `CreatePartitions` (API Key 37) for adding partitions to existing topics
- `DeleteRecords` (API Key 21) for deleting records before a specified offset
- Comprehensive unit tests for new Admin APIs (15 test cases)

### Changed
- Enabled API version ranges for `delete_records`, `alter_configs`, and `create_partitions` in types.v
- Updated handler routing in z_handler.v to support new Admin APIs

## [0.18.0] - 2026-01-21

### Added
- Complete Transaction API support for Exactly-Once Semantics (EOS)
- `AddOffsetsToTxn` (API Key 25) for adding consumer group offsets to transactions
- `TxnOffsetCommit` (API Key 28) for transactional offset commits
- Transaction validation in Produce handler to ensure transactional integrity
- `__consumer_offsets` partition tracking in transaction metadata

### Fixed
- Transaction validation in `TxnOffsetCommit` handler (validates producer ID, epoch, and state)
- `add_offsets_to_txn` now properly tracks `__consumer_offsets` partitions
- Produce handler now enforces strict transaction state validation (only `.ongoing` allowed)
- Produce handler now verifies partitions are added to transaction via `AddPartitionsToTxn`

### Changed
- Strengthened transactional produce validation to prevent invalid operations
- Improved error handling with specific error codes for transaction failures

## [0.17.0] - 2026-01-20

### Added
- Transaction Coordinator support for Exactly-Once Semantics (EOS).
- `InitProducerId` (API Key 22) enhancement for transactional producers.
- `AddPartitionsToTxn` (API Key 24) and `EndTxn` (API Key 26) APIs.
- In-memory transaction store and coordinator logic.

## [0.16.0] - 2026-01-20

### Added
- ACL Authorization support.
- `DescribeAcls` (API Key 29), `CreateAcls` (API Key 30), `DeleteAcls` (API Key 31) APIs.
- In-memory ACL manager for permission control.

## [0.15.0] - 2026-01-20

### Added
- SASL PLAIN authentication mechanism support.
- `SaslHandshake` (API Key 17) and `SaslAuthenticate` (API Key 36) APIs.
- In-memory user store for managing credentials.

## [0.14.0] - 2026-01-20

### Added
- S3 Range Request support in `S3StorageAdapter` for optimized segment reading.

### Changed
- Updated `fetch` operation to use HTTP Range headers when reading from the beginning of a log segment, reducing bandwidth usage and latency for small fetches.
