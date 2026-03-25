# DataCore - Product Requirements Document (PRD)

## 0. 버전 정보

| 항목 | 값 |
|------|-----|
| **현재 버전** | v0.50.9 |
| **릴리즈 날짜** | 2026-03-25 |
| **문서 버전** | 2.0 |

### 버전 히스토리

| 버전 | 날짜 | 주요 변경사항 |
|------|------|---------------|
| v0.50.9 | 2026-03-25 | Service 계층 에러 처리 17건 수정, Domain Clean Architecture 위반 해소, 중복 코드 ~600줄 제거/통합 |
| v0.50.8 | 2026-03-23 | God Class 분해 (Handler, ReplicationManager, BrokerRegistry), 13개 신규 파일 생성 |
| v0.50.7 | 2026-03-23 | ISP Consumer Migration (controller_election, partition_assigner, broker_registry), rate_limiter 토큰 롤백 수정 |
| v0.50.6 | 2026-03-22 | connection_pool 성능 개선, binary_helpers zero-alloc, ClusterMetadataPort ISP 5개 sub-interface 분리 |
| v0.50.5 | 2026-03-22 | binary protocol DoS 취약점 수정 (bounds check), config 퍼미션 0600, LoggerPort 인터페이스 추출 |
| v0.50.4 | 2026-03-22 | config.v God File 5개 파일 분할, i16 producer_epoch 오버플로우 수정, Clean Architecture 위반 수정 |
| v0.50.3 | 2026-03-22 | manager.v God Class 분할, TOCTOU 레이스 수정, O(1) incremental buffer counter |
| v0.50.2 | 2026-03-22 | binary protocol/connection pool 버그 수정 7건, O(n^2) string concatenation 최적화 |
| v0.50.1 | 2026-03-22 | ISR Manager 제거 (stateless), JSON->binary 복제 프로토콜, Rate Limit 설정 추가 |
| v0.50.0 | 2026-03-22 | 125 테스트 파일/1,704 테스트, HA (ISR/Rebalance/Election), SCRAM-SHA-512, Audit Logger |
| v0.49.0 | 2026-03-22 | StoragePort ISP 6개 분리, S3 보안 강화 (SSRF/path traversal), offset 레이스 수정, Confluent wire format |
| v0.1.0 | 2025-01-17 | 초기 개발: Kafka 프로토콜, Storage, CLI |
| v0.2.0 | 2025-01-17 | 성능 최적화: Buffer Pool, Object Pool, Zero-Copy |
| v0.3.0 | 2025-01-17 | 성능 통합: Performance Manager, Benchmark Suite |
| v0.4.0 | 2026-01-18 | 고급 I/O: Slice, Mmap, DMA, io_uring, NUMA |
| v0.5.0 | 2026-01-18 | KIP-848 New Consumer Protocol, Zero-Copy 프로토콜 통합 |
| v0.6.0 | 2026-01-18 | Schema Registry 호환성 검사 완성 (Avro/JSON/Protobuf) |
| v0.7.0 | 2026-01-18 | Admin API 완성 (CreateTopics, DeleteTopics, DescribeConfigs) |
| v0.8.0 | 2026-01-18 | Schema Encoding (Avro/JSON/Protobuf 이진 인코딩) |
| v0.9.0 | 2026-01-18 | Consumer Group Assignors (Sticky, Cooperative, Uniform) |
| v0.10.0 | 2026-01-18 | Stateless 아키텍처 최적화 및 코드 단순화 |
| v0.11.0 | 2026-01-19 | Produce API v13+ 지원 (Topic ID, Flexible Version) |
| v0.42.0 | 2026-02-01 | BinaryWriter Buffer Pooling, Sharded Atomic Metrics, TCP Optimization |
| v0.43.0 | 2026-02-03 | Iceberg REST Catalog Service Implementation (v3 format support) |
| v0.44.0 | 2026-02-10 | S3 Hybrid ACK Policy (`acks=0`/`acks=1/-1`), Kubernetes/Docker 배포 설정 |
| v0.44.1 | 2026-02-18 | Inter-Broker In-Memory Replication 구현, 레이스 컨디션 수정 (4개 mutex 추가) |
| v0.44.2 | 2026-02-20 | 코드 주석 영어 번역 (126+ 파일) |
| v0.44.3 | 2026-02-21 | Comprehensive Replication Metrics (throughput, latency, queue, connection) |
| v0.44.4 | 2026-02-21 | Auth 보안 취약점 수정, 공통 유틸리티 `infra.performance.core`로 중앙화 |
| v0.48.0 | 2026-03-06 | S3 PUT 비용 최적화 (flush 임계값, 인덱스 배치, Multi-Object Delete, sync linger) |

---

## 1. 제품 비전 및 목표

### 1.1 제품 개요

| 항목 | 내용 |
|------|------|
| **제품명** | DataCore |
| **목표** | Kafka API 완전 호환 (v1.1 ~ v4.1), 플러그인 스토리지, 내장 스키마 레지스트리 |
| **핵심 가치** | 경량화, 확장성, 성능 최적화 (Zero-Copy), 다중 스토리지 지원 |
| **지원 플랫폼** | Linux, macOS |
| **구현 언어** | V (vlang) |

### 1.2 제품 비전

**DataCore**는 V 언어로 구현하는 Apache Kafka 프로토콜 완전 호환 브로커입니다. 플러그인 기반 스토리지 엔진, 내장 Schema Registry(버전 관리 포함), Zero-Copy 등 성능 최적화를 지원합니다.

### 1.3 핵심 차별점

- **Stateless Broker**: 브로커가 상태를 갖지 않아 수평 확장이 용이
- **플러그인 스토리지**: Memory, S3, PostgreSQL 등 다양한 백엔드 지원
- **내장 Schema Registry**: 별도 서비스 없이 브로커에 내장된 스키마 관리
- **Iceberg REST Catalog**: 최신 v3 테이블 포맷을 지원하는 REST 카탈로그 내장
- **OpenTelemetry 통합**: 메트릭, 로깅, 트레이싱 통합 관측성
- **경량화**: 단일 바이너리, 최소 리소스 사용

---

## 2. 핵심 기능 요구사항

### 2.1 Kafka API 호환성 (전 버전 하위 호환)

클라이언트가 사용하는 Kafka 버전에 관계없이 API 버전 협상을 통해 적절한 프로토콜로 통신합니다.

```
버전 협상 로직:
1. 클라이언트가 ApiVersions 요청
2. 브로커가 지원하는 모든 API와 버전 범위 응답
3. 클라이언트는 자신이 지원하는 버전 중 브로커도 지원하는 최신 버전 선택
4. 이후 통신은 협상된 버전으로 진행
```

#### 2.1.1 Core APIs (Producer/Consumer)

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| ApiVersions | 18 | v0-v3 | P0 |
| Produce | 0 | v0-v13 | P0 |
| Fetch | 1 | v0-v18 | P0 |
| Metadata | 3 | v0-v12 | P0 |
| ListOffsets | 2 | v0-v7 | P0 |
| OffsetCommit | 8 | v0-v8 | P0 |
| OffsetFetch | 9 | v0-v8 | P0 |

#### 2.1.2 Consumer Group APIs (Classic Protocol)

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| FindCoordinator | 10 | v0-v4 | P0 |
| JoinGroup | 11 | v0-v9 | P0 |
| SyncGroup | 14 | v0-v5 | P0 |
| Heartbeat | 12 | v0-v4 | P0 |
| LeaveGroup | 13 | v0-v5 | P0 |
| ListGroups | 16 | v0-v4 | P0 |
| DescribeGroups | 15 | v0-v5 | P0 |
| DeleteGroups | 42 | v0-v2 | P1 |

#### 2.1.2.1 Consumer Group APIs (New Consumer Protocol - KIP-848)

Kafka 4.0+의 새로운 Consumer Protocol로, 파티션 할당을 **Broker가 직접 수행**합니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| ConsumerGroupHeartbeat | 68 | v0 | P0 |
| ConsumerGroupDescribe | 69 | v0 | P0 |
| GetTelemetrySubscriptions | 71 | v0 | P1 |
| PushTelemetry | 72 | v0 | P1 |

#### 2.1.2.2 Share Groups APIs (KIP-932)

Kafka 4.0+의 새로운 그룹 타입으로, **레코드 단위 분배**를 지원합니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| ShareGroupHeartbeat | 76 | v0 | P1 |
| ShareGroupDescribe | 77 | v0 | P1 |
| ShareFetch | 78 | v0 | P1 |
| ShareAcknowledge | 79 | v0 | P1 |
| InitializeShareGroupState | 83 | v0 | P1 |
| ReadShareGroupState | 84 | v0 | P1 |
| WriteShareGroupState | 85 | v0 | P1 |
| DeleteShareGroupState | 86 | v0 | P1 |

#### 2.1.3 Admin APIs - Topic Management

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| CreateTopics | 19 | v0-v7 | P0 |
| DeleteTopics | 20 | v0-v6 | P0 |
| CreatePartitions | 37 | v0-v3 | P1 |
| DeleteRecords | 21 | v0-v2 | P1 |
| DescribeTopics | 75 | v0 | P1 |

#### 2.1.4 Admin APIs - Config Management

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| DescribeConfigs | 32 | v0-v4 | P1 |
| AlterConfigs | 33 | v0-v2 | P1 |
| IncrementalAlterConfigs | 44 | v0-v1 | P2 |

#### 2.1.5 Admin APIs - Cluster Management

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| DescribeCluster | 60 | v0-v1 | P1 |
| DescribeBrokers | - | - | P1 |
| DescribeLogDirs | 35 | v0-v4 | P2 |
| AlterReplicaLogDirs | 34 | v0-v2 | P2 |

#### 2.1.6 SASL 인증 APIs

Kafka 클라이언트 인증을 위한 SASL (Simple Authentication and Security Layer) API입니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| SaslHandshake | 17 | v0-v1 | P1 |
| SaslAuthenticate | 36 | v0-v2 | P1 |

**지원 SASL 메커니즘:**

| 메커니즘 | 설명 | 우선순위 |
|----------|------|----------|
| PLAIN | 사용자명/비밀번호 기반 (TLS 권장) | P1 |
| SCRAM-SHA-256/512 | Challenge-Response 방식, 안전한 비밀번호 저장 | P2 |
| OAUTHBEARER | OAuth 2.0 토큰 기반 인증 | P2 |

#### 2.1.7 ACL (Access Control List) APIs

리소스별 권한 관리를 위한 ACL API입니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| DescribeAcls | 29 | v0-v3 | P1 |
| CreateAcls | 30 | v0-v3 | P1 |
| DeleteAcls | 31 | v0-v3 | P1 |

**ACL 리소스 타입:**

| 리소스 | 예시 권한 |
|--------|----------|
| Topic | READ, WRITE, DESCRIBE, DELETE |
| Group | READ, DESCRIBE, DELETE |
| Cluster | CREATE, ALTER, DESCRIBE |
| TransactionalId | WRITE, DESCRIBE |

#### 2.1.8 Transaction APIs

Exactly-once 시맨틱을 위한 트랜잭션 API입니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| InitProducerId | 22 | v0-v4 | P0 (완료) |
| AddPartitionsToTxn | 24 | v0-v4 | P1 |
| AddOffsetsToTxn | 25 | v0-v3 | P1 |
| EndTxn | 26 | v0-v3 | P1 |
| TxnOffsetCommit | 28 | v0-v3 | P1 |
| WriteTxnMarkers | 27 | v0-v1 | P2 |

**트랜잭션 워크플로우:**

```
1. Producer: InitProducerId(transactional_id) → producer_id, epoch
2. Producer: AddPartitionsToTxn(topic, partitions) → 트랜잭션에 파티션 등록
3. Producer: Produce(records with transactional_id)
4. Producer: AddOffsetsToTxn(group_id) → Consumer offset 커밋 준비
5. Producer: TxnOffsetCommit(offsets) → Consumer offset 커밋
6. Producer: EndTxn(COMMIT/ABORT) → 트랜잭션 완료
```

#### 2.1.9 Offset Management APIs

Consumer Group이 처리한 메시지 offset을 저장하고 조회하는 API입니다.

| 기능 | API Key | 지원 버전 범위 | 우선순위 | 상태 |
|------|---------|----------------|----------|------|
| OffsetCommit | 8 | v0-v9 | P0 | ✅ 구현 |
| OffsetFetch | 9 | v0-v10 | P0 | ✅ 구현 |
| ListOffsets | 2 | v0-v10 | P0 | ✅ 구현 |
| **메타데이터 저장** | - | v1+ | P1 | ✅ Task #49 |
| **Leader Epoch 추적** | - | v6+ | P1 | ✅ Task #50 |
| **Transactional Offset** | - | v28 | P1 | ✅ Task #51 |
| **Offset 만료 정책** | - | Internal | P1 | ✅ Task #52 |

**주요 기능:**
- OffsetCommit: Consumer가 처리한 offset 저장
- OffsetFetch: 저장된 offset 조회
- 메타데이터 저장: committed_metadata 필드
- Leader Epoch: 브로커 에포크와 동기
- Transactional Offset: 트랜잭션과 격리
- 만료 정책: offsets.retention.minutes 기반 자동 삭제
- OffsetCommit API (메타데이터, 타임스탬프)
- OffsetFetch API (Leader Epoch, Topic ID)
- Offset 만료 정책 (offsets.retention.minutes)
- Transactional Isolation

#### 2.1.10 Admin APIs - Quota Management (Optional)

| 기능 | API Key | 지원 버전 범위 | 우선순위 |
|------|---------|----------------|----------|
| DescribeClientQuotas | 48 | v0-v1 | P3 |
| AlterClientQuotas | 49 | v0-v1 | P3 |

#### 2.1.11 Multi-Protocol Gateway (P2)

Kafka Protocol 외에 추가 프로토콜을 지원하여 다양한 클라이언트 환경을 수용합니다.

| 프로토콜 | 포트 | 방식 | 사용 사례 | 우선순위 |
|----------|------|------|----------|----------|
| **Kafka Protocol** | 9092 | Pull (Binary) | Kafka 클라이언트 호환 | P0 |
| **Iceberg REST** | 8080 | Pull/Push (HTTP) | Spark, Trino 데이터 레이크 연동 | P2 |
| **gRPC Streaming** | 9093 | Push (HTTP/2) | 마이크로서비스, 양방향 스트리밍 | P2 |
| **SSE** | 8080 | Push (HTTP/1.1) | 웹 브라우저, 대시보드 | P2 |
| **WebSocket** | 8080 | Push (HTTP) | 웹 기반 양방향 통신 | P2 |

**Multi-Protocol 아키텍처:**

```
클라이언트 → [Kafka/gRPC/SSE/WebSocket] → Protocol Adapter → Core Engine → Storage
```

---

### 2.2 스토리지 엔진 (Plugin Architecture)

플러그인 형태로 다양한 스토리지 백엔드를 지원하며, 사용자가 커스텀 플러그인을 추가할 수 있습니다.

| 엔진 | 우선순위 | 용도 | 동시성 제어 |
|------|----------|------|-------------|
| **Memory** | P0 | 개발/테스트 | Mutex + Atomic CAS |
| **S3** | P1 | 프로덕션, 고내구성 | Conditional Writes (ETag) |
| **PostgreSQL** | P2 | 프로덕션, 다중 브로커 | Transaction + Row Lock |

#### 2.2.1 Plugin Interface

모든 스토리지 엔진이 구현해야 하는 인터페이스:

- **메타데이터**: 플러그인 이름, 버전, 설명
- **라이프사이클**: 초기화, 종료, 헬스체크
- **StorageEngine**: 실제 스토리지 엔진 구현체 반환

---

### 2.3 내장 Schema Registry (버전 관리) ✅ 구현 완료

브로커에 내장된 Schema Registry로 별도 서비스 없이 스키마 관리가 가능합니다.

**v0.6.0 구현 상태:**
- ✅ 스키마 등록/조회/삭제
- ✅ Subject/Version 관리
- ✅ Avro 호환성 검사 (Backward/Forward/Full)
- ✅ JSON Schema 호환성 검사
- ✅ Protobuf 호환성 검사
- ✅ Global/Subject Config 설정
- ✅ Boot Recovery (__schemas 토픽에서 복구)

#### 2.3.1 지원 스키마 타입

| 스키마 타입 | 파일 확장자 | 우선순위 |
|-------------|-------------|----------|
| Apache Avro | .avsc | P0 |
| JSON Schema | .json | P1 |
| Protocol Buffers | .proto | P1 |

#### 2.3.2 스키마 버전 관리

| 기능 | 설명 |
|------|------|
| 등록 | 새 스키마 등록 시 자동 버전 부여 (v1, v2, ...) |
| 조회 | Subject + Version 또는 Schema ID로 조회 |
| 호환성 검사 | BACKWARD, FORWARD, FULL, NONE 모드 |
| 삭제 | Soft delete (비활성화) / Hard delete |

#### 2.3.3 스키마 저장

스키마는 내부 토픽 `__schemas`에 저장됩니다:

```json
{
  "subject": "user-events-value",
  "version": 3,
  "id": 10042,
  "schema_type": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"User\",...}",
  "compatibility": "BACKWARD",
  "created_at": "2026-01-16T10:30:00Z",
  "deleted": false
}
```

#### 2.3.4 Schema Registry REST API

```
GET    /schemas/ids/{id}              - ID로 스키마 조회
GET    /subjects                       - 모든 Subject 목록
GET    /subjects/{subject}/versions    - Subject의 버전 목록
GET    /subjects/{subject}/versions/{version}
POST   /subjects/{subject}/versions    - 새 스키마 등록
POST   /compatibility/subjects/{subject}/versions/{version}
DELETE /subjects/{subject}/versions/{version}
```

---

### 2.4 Kafka Admin Client 지원

Java, Python, Go 등 다양한 언어의 Kafka AdminClient를 완전 지원합니다.

| 카테고리 | 기능 | 설명 |
|----------|------|------|
| **Topic 관리** | createTopics() | 토픽 생성 (파티션 수, 설정 지정) |
| | deleteTopics() | 토픽 삭제 |
| | listTopics() | 토픽 목록 조회 |
| | describeTopics() | 토픽 상세 정보 (파티션, 리더 등) |
| | createPartitions() | 파티션 추가 |
| **Config 관리** | describeConfigs() | 브로커/토픽 설정 조회 |
| | alterConfigs() | 설정 변경 |
| **Consumer Group** | listConsumerGroups() | 그룹 목록 조회 |
| | describeConsumerGroups() | 그룹 상세 (멤버, 오프셋) |
| | deleteConsumerGroups() | 그룹 삭제 |
| | listConsumerGroupOffsets() | 그룹 오프셋 조회 |
| **Cluster** | describeCluster() | 클러스터 정보 (브로커 목록) |
| | describeLogDirs() | 로그 디렉토리 정보 |

---

### 2.5 성능 최적화

| 최적화 기법 | 설명 | 적용 위치 |
|-------------|------|-----------|
| **Zero-Copy** | 커널-유저 공간 복사 최소화, sendfile() 활용 | Fetch 응답, 파일 I/O |
| **Memory Pooling** | `BinaryWriter` 버퍼 풀링으로 GC 부담 및 메모리 할당 감소 | Kafka 프로토콜 인코딩 |
| **Sharded Atomic Metrics** | 16-샤드 분산 카운터로 멀티코어 환경의 락 경합 최소화 | 메트릭 수집 시스템 |
| **TCP Optimization** | TCP_NODELAY 및 최적 버퍼 크기(256KB) 설정으로 지연 감소 | TCP 서버 레이어 |
| **Compression Threshold** | 1KB 미만 데이터 압축 생략, 동적 압축 레벨 설정 | 압축 서비스 |
| **Batch Processing** | 메시지 배치 단위 처리 | Produce, Fetch, Commit |
| **Connection Pooling** | HTTP/S3 및 DB 연결 재사용 | 스토리지 어댑터 |
| **Non-Blocking I/O** | V coroutines + epoll/kqueue (io_uring 예정) | 클라이언트 연결 |
| **S3 Hybrid ACK Policy** | `acks=0` 비동기 버퍼, `acks=1/-1` 동기 S3 PUT으로 내구성 선택 | S3 스토리지 어댑터 |
| **S3 PUT 비용 최적화** | flush 임계값, 인덱스 배치, Multi-Object Delete, sync linger로 PUT 60-80% 절감 | S3 스토리지 어댑터 |

#### 2.5.1 Client Connection 관리

Kafka 프로토콜 호환을 위해 클라이언트 연결을 지속적으로 유지합니다.

| 기능 | 설명 | 설정 |
|------|------|------|
| **Persistent Connection** | TCP 연결 유지 및 재사용 | 기본 활성화 |
| **Connection Limits** | 최대 연결 수 제한 | `max_connections` |
| **Idle Timeout** | 유휴 연결 자동 종료 | `connection_idle_timeout_ms` |
| **Per-IP Limits** | IP당 연결 수 제한 | `max_connections_per_ip` |

#### 2.5.2 Non-Blocking I/O

V 언어의 coroutines를 활용하여 수천 개의 동시 연결을 효율적으로 처리합니다.

| 구성요소 | 설명 |
|----------|------|
| **I/O Multiplexing** | epoll (Linux) / kqueue (macOS) 자동 사용 |
| **Connection per Coroutine** | 각 연결을 별도 coroutine에서 처리 |
| **Request Pipelining** | 단일 연결에서 여러 요청 동시 처리 |
| **Request Queue** | 요청 큐잉으로 과부하 방지 |

---

### 2.6 Observability (OpenTelemetry)

OpenTelemetry 기반의 통합 관측성을 제공합니다.

#### 2.6.1 지원 신호

| 신호 | 설명 | Exporter |
|------|------|----------|
| **Metrics** | 처리량, 지연시간, 에러율, 파티션 상태 등 | OTLP, Prometheus |
| **Logs** | 구조화된 로그, 컨텍스트 전파 | OTLP, stdout (JSON) |
| **Traces** | 요청 추적, 분산 트레이싱 | OTLP, Jaeger, Zipkin |

#### 2.6.2 주요 메트릭

| 메트릭 | 타입 | 설명 |
|--------|------|------|
| `datacore_messages_produced_total` | Counter | 생산된 메시지 총 수 |
| `datacore_messages_consumed_total` | Counter | 소비된 메시지 총 수 |
| `datacore_request_duration_seconds` | Histogram | 요청 처리 시간 |
| `datacore_partition_lag` | Gauge | 컨슈머 그룹 랙 |
| `datacore_active_connections` | Gauge | 활성 연결 수 |
| `datacore_connections_total` | Counter | 총 연결 수 (누적) |
| `datacore_connections_rejected_total` | Counter | 거부된 연결 수 |
| `datacore_storage_bytes` | Gauge | 스토리지 사용량 |

---

### 2.7 Consumer Group Protocol 비교

DataCore는 세 가지 Consumer Group Protocol을 모두 지원합니다.

#### 2.7.1 Protocol 타입 비교

| 항목 | Classic Protocol | Consumer Protocol (KIP-848) | Share Groups (KIP-932) |
|------|------------------|----------------------------|------------------------|
| **Kafka 버전** | 0.9+ | 4.0+ | 4.0+ |
| **할당 주체** | Client-side (Leader Consumer) | **Server-side (Broker)** | **Server-side (Broker)** |
| **할당 단위** | 파티션 | 파티션 | **레코드** |
| **리밸런스** | Eager (전체 중단) | **Incremental (일부만 영향)** | 불필요 |
| **파티션 공유** | 불가 (배타적) | 불가 (배타적) | **가능 (협력적)** |
| **컨슈머 수 제한** | ≤ 파티션 수 | ≤ 파티션 수 | **무제한** |
| **순서 보장** | 파티션 내 보장 | 파티션 내 보장 | **보장 안됨** |
| **Ack 방식** | 오프셋 커밋 | 오프셋 커밋 | **개별 레코드 Ack/Nack** |
| **사용 사례** | 이벤트 스트리밍 | 이벤트 스트리밍 | **작업 큐, Task 분배** |

#### 2.7.2 Classic Protocol (Client-side Assignment)

```
1. Consumer → Broker: FindCoordinator(group_id)
2. Broker → Consumer: Coordinator = Broker X
3. All Consumers → Coordinator: JoinGroup(protocols, metadata)
4. Coordinator → Leader Consumer: 모든 멤버 정보 전달
5. Leader Consumer: 파티션 할당 계산 (RoundRobin, Range, Sticky 등)
6. Leader → Coordinator: SyncGroup(assignments)
7. Coordinator → All Consumers: 각자의 할당 정보
```

**특징:**
- 리밸런스 시 모든 컨슈머가 파티션 소유권 포기 (Stop-the-world)
- 클라이언트 라이브러리가 할당 전략 구현

#### 2.7.3 Consumer Protocol (Server-side Assignment, KIP-848)

```
1. Consumer → Broker: ConsumerGroupHeartbeat(subscribedTopics, assignedPartitions)
2. Broker (Coordinator): 파티션 할당 계산 (Server-side Assignor)
3. Broker → Consumer: ConsumerGroupHeartbeat Response (newAssignment)
4. Consumer: Incremental하게 파티션 추가/제거
```

**특징:**
- 브로커가 직접 할당 계산 → 클라이언트 단순화
- Incremental Rebalancing → 영향받는 컨슈머만 변경
- Leader 선출 불필요

#### 2.7.4 Share Groups (Record-level Distribution, KIP-932)

```
1. Consumer → Broker: ShareGroupHeartbeat(subscribedTopics)
2. Broker: 모든 파티션을 모든 컨슈머가 공유하도록 설정
3. Consumer → Broker: ShareFetch(topic, partitions)
4. Broker → Consumer: Records (with acquisition lock)
5. Consumer → Broker: ShareAcknowledge(recordIds, ackType)
   - ACCEPT: 처리 완료
   - RELEASE: 다른 컨슈머에게 반환
   - REJECT: Dead Letter Queue로 이동
```

**특징:**
- 파티션 경계 없이 레코드 단위 분배
- 컨슈머 수가 파티션 수보다 많아도 됨
- 개별 메시지 Ack/Nack 지원 (작업 큐 패턴)
- In-flight 레코드 수 제한 (`share.record.lock.duration.ms`)

#### 2.7.5 예시: 브로커 3개, 파티션 4개, 컨슈머 3개

| Protocol | 할당 결과 |
|----------|----------|
| **Classic/Consumer** | C1: [P0, P1], C2: [P2], C3: [P3] |
| **Share Group** | 모든 컨슈머가 모든 파티션의 레코드를 경쟁적으로 소비 |

---

### 2.8 Stateless Broker + 동시성 제어

브로커는 상태를 갖지 않으며, 모든 상태는 스토리지 엔진에서 관리합니다.

### 2.9 Inter-Broker In-Memory Replication ✅ 구현 완료 (v0.44.1+)

DataCore는 브로커 간 인메모리 복제를 지원하여 고가용성을 제공합니다.

#### 2.9.1 개요

- **목적**: `acks=0` 비동기 구간(`batch_timeout_ms` 동안 메모리에만 존재)에서 브로커 크래시 시 데이터 손실 방지
- **방식**: Push 기반 TCP 인메모리 복제 (S3 flush 전까지)
- **복제본 수**: 설정 가능 (기본 2개, 원본 포함)
- **구현 상태**: v0.44.1 완전 구현, v0.44.3에서 포괄적인 메트릭 추가

#### 2.9.2 S3 Hybrid ACK Policy (v0.44.0)

| acks 값 | 동작 | 레이턴시 | 내구성 |
|---------|------|----------|--------|
| `acks=0` | 비동기 버퍼 flush (best-effort) | 최소 (<1ms) | 낮음 |
| `acks=1` | 동기 S3 PUT + index update | 50~200ms | 높음 |
| `acks=-1` | 동기 S3 PUT + 전체 복제 확인 (stateless 설계에서는 단일 브로커 확인과 동일) | 50~200ms | 최고 |

다중 브로커 환경에서는 `acks=1/-1` 시 인메모리 복제(1~5ms)로 처리하여 S3 직접 PUT 대비 레이턴시를 10~40배 개선합니다.

#### 2.9.3 복제 프로세스

1. Main Broker가 레코드 수신 → 메모리 버퍼(TopicPartitionBuffer) 추가
2. Replica Broker로 REPLICATE 메시지 TCP 전송 (Push 방식)
3. Replica Broker가 ReplicaBuffer에 저장
4. REPLICATE_ACK 수신 후 ProduceResponse 반환 (`acks=1/-1` 시)
5. `batch_timeout_ms` 경과 후 flush_worker가 S3 PUT 실행
6. S3 flush 완료 후 FLUSH_ACK 전송 → ReplicaBuffer 삭제

#### 2.9.4 메시지 타입

| 타입 | 값 | 설명 |
|------|-----|------|
| REPLICATE | 0x01 | 레코드 복제 요청 |
| REPLICATE_ACK | 0x02 | 복제 확인 응답 |
| FLUSH_ACK | 0x03 | S3 flush 완료 통지 |
| HEARTBEAT | 0x04 | 브로커 생존 확인 |

#### 2.9.5 Replication Metrics (v0.44.3)

| 메트릭 | 타입 | 설명 |
|--------|------|------|
| `bytes_in_per_sec` | Gauge | 초당 수신 바이트 (rolling rate) |
| `bytes_out_per_sec` | Gauge | 초당 송신 바이트 (rolling rate) |
| `avg_replication_lag_ms` | Gauge | 평균 복제 지연 (ms) |
| `p95_replication_lag_ms` | Gauge | 95th 백분위 복제 지연 (ms) |
| `p99_replication_lag_ms` | Gauge | 99th 백분위 복제 지연 (ms) |
| `pending_requests` | Gauge | 현재 처리 중인 복제 요청 수 |
| `failed_requests` | Counter | 누적 실패 복제 요청 수 |
| `active_connections` | Gauge | 활성 TCP 연결 수 |

#### 2.9.6 브로커 상태 관리 및 장애 복구

- 하트비트(기본 3초 간격)로 브로커 생존 확인
- 장애 감지 시: ReplicaBuffer 내 orphan 배치 감지 (기본 60초 TTL)
- Broker A 크래시 시: Broker B가 orphan 감지 후 직접 S3 PUT 수행 → 데이터 유실 방지
- 중복 쓰기 방지: S3 partition index 기반 batch_id 중복 확인

#### 2.9.7 설정

```toml
[replication]
enabled                    = false  # 복제 활성화 여부
replication_port           = 9094   # 복제용 TCP 포트
replica_count              = 2      # 복제본 수 (원본 포함)
replica_timeout_ms         = 5000   # 복제 ACK 대기 시간
heartbeat_interval_ms      = 3000   # 하트비트 간격
reassignment_interval_ms   = 30000  # 복제 재할당 간격
orphan_cleanup_interval_ms = 60000  # 고아 버퍼 정리 간격
retry_count                = 3      # 최대 복제 재시도 횟수
replica_buffer_ttl_ms      = 60000  # 복제 버퍼 최대 보관 시간 (ms)
max_replica_buffer_size_bytes = 0   # 복제 버퍼 최대 크기 (0 = 무제한)
```

## 3. CLI 도구

### 3.1 명령어 목록

```bash
# 브로커 실행
datacore broker start --config=/etc/datacore/config.toml

# 토픽 관리
datacore topic create my-topic --partitions=6 --replication=1
datacore topic list
datacore topic describe my-topic
datacore topic delete my-topic
datacore topic add-partitions my-topic --partitions=12

# 메시지 produce/consume
datacore produce my-topic --file=data.json
datacore consume my-topic --from-beginning --max-messages=100

# Consumer Group 관리 (Admin)
datacore group list
datacore group describe my-consumer-group
datacore group delete my-consumer-group
datacore group reset-offsets my-consumer-group --topic=my-topic --to-earliest

# Config 관리 (Admin)
datacore config describe --entity-type=topic --entity-name=my-topic
datacore config alter --entity-type=topic --entity-name=my-topic \
    --add-config retention.ms=604800000
datacore config describe --entity-type=broker --entity-name=1

# Cluster 정보 (Admin)
datacore cluster describe
datacore cluster brokers
datacore cluster log-dirs --broker=1

# 스키마 관리
datacore schema register user-events --file=user.avsc --type=avro
datacore schema list
datacore schema get user-events --version=latest
datacore schema compatibility user-events --file=user_v2.avsc

# 상태 확인
datacore status
datacore metrics
```

---

## 4. 비기능 요구사항

### 4.1 성능 목표

| 항목 | 목표 |
|------|------|
| **처리량** | 500k+ msg/s (Memory), 100k+ msg/s (S3) |
| **지연시간** | p99 < 5ms (Memory), p99 < 50ms (S3) |
| **바이너리 크기** | < 30MB |
| **메모리 사용** | < 100MB (기본 설정) |
| **지원 플랫폼** | Linux (x86_64, arm64), macOS (x86_64, arm64) |
| **S3 PUT 비용** | 월 $150-300 수준 (기존 대비 60-80% 절감) |

### 4.2 안정성

- 스토리지 엔진 장애 시 적절한 에러 반환
- 연결 끊김 시 graceful shutdown
- 메모리 누수 없는 장기 운영

### 4.3 보안

| 기능 | 설명 | 우선순위 |
|------|------|----------|
| **TLS** | 암호화된 통신 (선택적) | P1 |
| **SASL 인증** | PLAIN, SCRAM, OAUTHBEARER 메커니즘 | P1 |
| **ACL** | 리소스별 접근 권한 관리 | P1 |
| **Transaction** | Exactly-once 시맨틱, 원자적 쓰기 | P1 |

---

## 5. 개발 마일스톤

| Phase | 내용 | 기간 | 우선순위 |
|-------|------|------|----------|
| **P0-1** | 프로젝트 구조, Kafka 프로토콜 파싱 (ApiVersions, Metadata) | 3주 | P0 |
| **P0-2** | Memory Storage Plugin, Produce/Fetch API | 4주 | P0 |
| **P0-3** | Consumer Group Classic Protocol (Join/Sync/Heartbeat/Leave/Commit) | 3주 | P0 |
| **P0-4** | Consumer Group New Protocol (KIP-848, Server-side Assignment) | 2주 | P0 |
| **P0-5** | Admin API 기본 (CreateTopics, DeleteTopics, ListGroups, DescribeGroups) | 2주 | P0 |
| **P0-6** | Schema Registry 기본 (Avro), __schemas 토픽 | 3주 | P0 |
| **P0-7** | OpenTelemetry 기반 Observability (Metrics, Logging, Tracing) | 2주 | P0 |
| **P0-8** | CLI (broker, topic, produce, consume, group) | 2주 | P0 |
| **P1-1** | S3 Storage Plugin + Conditional Writes | 4주 | P1 |
| **P1-2** | Zero-Copy 최적화, Buffer Pooling | 2주 | P1 |
| **P1-3** | JSON Schema, Protobuf 지원 | 3주 | P1 |
| **P1-4** | Admin API 확장 (CreatePartitions, DeleteRecords, Configs, Cluster) | 3주 | P1 |
| **P1-5** | Admin CLI (config, cluster) | 1주 | P1 |
| **P1-6** | Share Groups 기본 (KIP-932, Record-level Distribution) | 3주 | P1 |
| **P1-7** | SASL 인증 (PLAIN, SCRAM) | 2주 | P1 |
| **P1-8** | ACL 권한 관리 | 2주 | P1 |
| **P1-9** | Transaction 지원 (Exactly-once) | 3주 | P1 |
| **P2-1** | PostgreSQL Storage Plugin | 4주 | P2 |
| **P2-2** | Data Lake (Parquet, Iceberg REST Catalog v3) | 4주 | P2 | ✅ 구현 완료 (v0.43.0) |
| **P2-3** | gRPC Streaming Protocol | 3주 | P2 |
| **P2-4** | SSE/WebSocket Protocol | 2주 | P2 |
| **P3-1** | Quota Admin APIs (선택적) | 2주 | P3 |
| **총계** | | **60주 (약 15개월)** | |

---

## 6. 제외 범위 (Out of Scope)

다음 기능들은 초기 버전에서 제외됩니다:

- Raft 기반 브로커 복제 (Stateless 구조 사용)
- Kafka Connect 호환
- Kafka Streams 호환
- 복잡한 ACL 모델 (P3에서 기본 지원)
- Windows 플랫폼 지원

### 6.1 Stateless 아키텍처로 인한 단순화

DataCore는 **Stateless Broker** 아키텍처를 사용합니다. 모든 브로커가 공유 스토리지(S3, PostgreSQL 등)에 접근하므로, 기존 Kafka의 일부 기능들은 **프로토콜 호환성은 유지**하되 **내부 구현이 단순화**됩니다:

| 기능 | Kafka 용도 | DataCore 구현 | 이유 |
|------|-----------|--------------|------|
| **Sticky Assignor** | 파티션-브로커 친화성 유지 | RoundRobin으로 단순화 | 브로커 친화성 불필요 |
| **Cooperative Sticky** | 점진적 리밸런싱 | Sticky와 동일하게 처리 | 리밸런싱 비용 거의 0 |
| **leader_epoch** | 리더 브로커 추적 | 항상 0 반환 | 단일 논리적 브로커 |
| **replica_nodes/isr_nodes** | 복제본/ISR 목록 | `[broker_id]` 고정 | 복제 없음 |
| **Fetch Session** | 증분 Fetch 최적화 | 항상 새 세션 (id=0) | Stateless |
| **preferred_read_replica** | 팔로워 읽기 | -1 (비활성) | 복제본 없음 |
| **Rack-aware 할당** | 데이터센터 지역성 | 무시 | 원격 스토리지 |

#### 설계 원칙

```
┌─────────────────────────────────────────────────────────────┐
│  외부 (Kafka Protocol)     │  내부 (DataCore 구현)          │
├─────────────────────────────────────────────────────────────┤
│  모든 필드/API 지원        │  단순화된 로직                 │
│  클라이언트 기대값 반환     │  불필요한 상태 관리 생략       │
│  버전별 응답 형식 준수      │  고정값 또는 위임              │
└─────────────────────────────────────────────────────────────┘
```

---

## 7. Storage Engine 구조 및 플러그인 정책 (2026-01-18, v0.11.0)

- Storage Engine은 **Plugin** 방식으로 설계
- 기본 엔진: MemoryStorage (최우선, 빠른 처리)
- 보조 엔진: S3, DB, Key-Value Store (config로 선택)
- config 예시:
  ```toml
  [storage]
  engine = "memory" # "s3", "postgres" 등 지정 가능
  ```
- 운영 중 엔진 변경 시, 데이터 마이그레이션 정책 별도 문서화 예정

---

## 8. 개발 프로세스

### 8.1 Git 브랜치 정책

> **⚠️ main 브랜치 머지는 반드시 사용자 승인 후 진행합니다.**

```
┌─────────────────────────────────────────────────────────────────┐
│  ⛔ main 브랜치 직접 커밋 금지                                   │
│  ⛔ main 브랜치 머지 시 사용자 승인 필수                         │
│  ✅ 새 브랜치 생성 → 개발 → 테스트 → 사용자 승인 → main 머지     │
└─────────────────────────────────────────────────────────────────┘
```

AI 어시스턴트(GitHub Copilot 등)가 작업 시:
1. 작업 시작 전 새 브랜치 생성
2. 작업 완료 후 테스트 통과 확인
3. **main 머지 전 반드시 사용자에게 승인 요청**
4. 사용자 승인 후에만 main 머지 실행

상세 가이드라인은 [CONTRIBUTING.md](CONTRIBUTING.md)를 참조하세요.

---

## 9. 용어 정의

| 용어 | 정의 |
|------|------|
| **Broker** | Kafka 프로토콜을 처리하는 서버 인스턴스 |
| **Topic** | 메시지가 발행되는 논리적 채널 |
| **Partition** | 토픽을 구성하는 순서가 보장되는 메시지 큐 |
| **Consumer Group** | 파티션 단위로 메시지를 소비하는 컨슈머 집합 (Classic/Consumer Protocol) |
| **Share Group** | 레코드 단위로 메시지를 소비하는 컨슈머 집합 (KIP-932) |
| **Offset** | 파티션 내 메시지의 고유 순서 번호 |
| **Schema Registry** | 메시지 스키마를 관리하는 서비스 |
| **Storage Engine** | 메시지를 저장하는 백엔드 시스템 |
| **Iceberg REST Catalog** | 데이터 레이크 통계 및 메타데이터를 관리하는 표준 REST 인터페이스 |
| **Classic Protocol** | Client-side 파티션 할당 방식 (Leader Consumer가 계산) |
| **Consumer Protocol** | Server-side 파티션 할당 방식 (Broker가 계산, KIP-848) |
| **Incremental Rebalancing** | 변경된 파티션만 재할당하는 방식 |

---

## 참고문서

- [TRD.md](TRD.md) - 기술 요구사항 문서
- [BENCHMARK.md](BENCHMARK.md) - 성능 벤치마크 결과
- [CONTRIBUTING.md](CONTRIBUTING.md) - 기여 가이드라인

---

> **Note**: 이 문서는 프로젝트 진행에 따라 업데이트됩니다.
> 최종 수정: 2026-03-25
