# Changelog

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
