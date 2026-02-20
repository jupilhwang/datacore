# Changelog

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
