# DataCore 아키텍처 종합 분석 보고서

- **프로젝트**: DataCore (Kafka-compatible message broker in V)
- **버전**: v0.49.0
- **분석 일자**: 2026-03-22
- **분석 범위**: 소스 코드 314개 .v 파일 (224 소스 + 90 테스트), 문서, CI/CD, 인프라 전체

---

## 목차

1. [아키텍처 평가](#1-아키텍처-평가)
2. [PRD 목표 대비 구현 현황](#2-prd-목표-대비-구현-현황)
3. [안정성 분석](#3-안정성-분석)
4. [성능 분석](#4-성능-분석)
5. [고가용성 분석](#5-고가용성-분석)
6. [보안 분석](#6-보안-분석)
7. [종합 의견 및 우선순위 권고](#7-종합-의견-및-우선순위-권고)

---

## 1. 아키텍처 평가

### 등급: B+

DataCore는 4계층 Clean Architecture를 선언하고 있으며, 전반적으로 잘 지켜지고 있다.

| 계층 | 역할 | 파일 수 | 준수 상태 |
|------|------|---------|-----------|
| Domain (18 files) | 순수 V 코드, 외부 의존 없음 | 18 | 우수 - 외부 의존성 0 |
| Service/Port (6 interfaces) | ISP 준수 인터페이스 정의 | 6 | 우수 - v0.49.0에서 6개로 분리 완료 |
| Infra (11 subdirs) | Port 구현체 | 130+ | 양호 - 일부 God Class 잔존 |
| Interface (4 subdirs) | CLI/TCP/REST/gRPC | 30+ | 양호 - REST server.v (907줄) 비대 |

### 강점

- **Port/Adapter 패턴**: StoragePort가 6개의 ISP 준수 서브 인터페이스(TopicStoragePort, RecordStoragePort, GroupStoragePort, OffsetStoragePort, SharePartitionPort, StorageHealthPort)로 분리되어 있음
- **도메인 모델 충실도**: Kafka 스펙을 충실히 반영하며, KIP-932(ShareGroup), KIP-848(ConsumerGroupHeartbeat) 등 최신 프로토콜까지 지원
- **의존성 방향 일관성**: Interface -> Infra -> Service -> Domain 방향이 일관적으로 유지됨
- **동시성 패턴**: spawn, chan, Mutex/RwMutex, stdatomic을 적절히 조합하여 사용. Replication Manager에는 락 순서가 문서화되어 있음

### 약점

- **main.v 비대**: 700줄의 main.v가 DI 컨테이너 역할을 하면서 지나치게 비대함
- **REST server.v**: 907줄에 라우팅, 핸들러, 미들웨어가 혼재
- **S3 어댑터 복잡도**: 4파일로 분리됐지만 여전히 45+ 파일로 서브모듈 복잡도가 높음
- **gRPC 미완성**: V 언어의 HTTP/2 미지원으로 gRPC 프로토콜이 차단 상태

---

## 2. PRD 목표 대비 구현 현황

### 전체 달성률: 약 75-80%

| PRD 목표 | 상태 | 비고 |
|----------|------|------|
| Kafka API P0 (Core) | 완료 | Produce v0-13, Fetch v0-16, Metadata v0-12, ListOffsets, OffsetCommit/Fetch, JoinGroup/SyncGroup/Heartbeat/LeaveGroup, FindCoordinator, CreateTopics/DeleteTopics, ApiVersions |
| Kafka API P1 (Admin/Auth) | 거의 완료 | SASL (PLAIN, SCRAM-SHA-256), ACL CRUD, Transactions (InitProducerId, AddPartitions/OffsetsToTxn, EndTxn, TxnOffsetCommit), ConsumerGroupHeartbeat (KIP-848), ShareGroup APIs (KIP-932). WriteTxnMarkers만 TODO |
| Kafka API P2 (Extended) | 부분 구현 | IncrementalAlterConfigs, DescribeLogDirs/AlterReplicaLogDirs 있음 |
| 플러그인 스토리지 | 완료 | Memory, S3, PostgreSQL 3종 구현 완료 |
| Schema Registry | 완료 | Avro/JSON/Protobuf 지원, 호환성 체크, Confluent wire format 인코딩/디코딩 |
| Multi-Protocol Gateway | 부분 완료 | Kafka TCP(9092) + REST/SSE/WebSocket(8080) 작동. gRPC(9094)는 V의 HTTP/2 미지원으로 차단 |
| Observability (OTel) | 완료 | Metrics, Logging, Tracing 통합. OTLP exporter, Prometheus /metrics 엔드포인트 |
| Replication | 부분 구현 | TCP 기반 브로커 간 복제 작동, 병렬 전송, 지수 백오프 재시도. 파티션 리밸런싱 미구현 |
| Cluster Management | 부분 구현 | 컨트롤러 선출(분산 락 기반) + 브로커 레지스트리 작동. 파티션 리밸런싱 TODO |
| Iceberg REST Catalog | 완료 | S3 기반, Parquet 포맷, Glue Catalog 지원 |
| Performance Optimization | 완료 | zero-copy, buffer pool, TCP 최적화, io_uring, NUMA, S3 PUT 비용 최적화(60-80% 감소) |

### 미구현 핵심 항목

1. **gRPC 프로토콜**: V 언어 HTTP/2 제한으로 코드는 존재하나 실행 불가
2. **파티션 리밸런싱**: `broker_registry.v:434`에 TODO 상태
3. **WriteTxnMarkers**: `coordinator.v:256`에 TODO 상태
4. **CRC32C 하드웨어 가속**: 소프트웨어 테이블 룩업 방식만 구현

---

## 3. 안정성 분석

### 등급: C+ (개선 필요)

### 3-1. 테스트 커버리지

89개 단위 테스트 파일이 존재하지만, 핵심 경로(hot path)에 테스트가 부재하다.

**테스트가 충실한 영역:**

| 모듈 | 테스트 파일 수 | 비고 |
|------|----------------|------|
| config/ | 4 | CLI 파싱, 검증, TOML 이스케이프, SSRF, 감시자, 아이덴티티 |
| domain/ | 3 | Records, streaming, replication metrics |
| service/auth/ | 2 | SCRAM, authenticator |
| service/cluster/ | 2 | 컨트롤러 선출, 파티션 할당자 |
| service/group/ | 4 | 코디네이터, share 코디네이터, failover, KIP-848 |
| service/offset/ | 2 | 매니저, 통합 |
| service/schema/ | 2 | 레지스트리, 인코더 |
| infra/protocol/kafka/ | 21 | 프로토콜 파서, codec, SASL, ACL, 트랜잭션, share groups |
| infra/storage/plugins/s3/ | 16 | 서명, XML, 페이지네이션, Iceberg, 배치 연산 |
| infra/replication/ | 1 (30 tests) | 매니저 (락 순서, 버퍼 관리, 복제 흐름) |
| infra/compression/ | 2 | 일반 압축, Kafka 호환 |
| infra/observability/ | 3 | 로거, 텔레메트리, 트레이싱 |
| infra/encoding/ | 6 | Thrift, RLE, Parquet, LE utils, dict decoder, 보안 |
| infra/performance/ | 7 | DMA, mmap, slice, io_uring, NUMA, 벤치마크 |
| interface/server/ | 3 | 연결, 파이프라인, 워커 풀 |

**테스트가 부재한 핵심 영역 (CRITICAL GAPS):**

| 미테스트 영역 | 위험도 | 이유 |
|---------------|--------|------|
| 모든 handler_*.v (16+개 파일) | CRITICAL | Kafka 요청 처리의 핵심 비즈니스 로직 연결부 |
| service/broker/produce.v | CRITICAL | 메시지 생산의 핵심 경로 |
| service/broker/fetch.v | CRITICAL | 메시지 소비의 핵심 경로 |
| service/transaction/coordinator.v | HIGH | 트랜잭션 상태 머신 |
| service/topic/manager.v | HIGH | 토픽 관리 서비스 |
| interface/rest/server.v (907줄) | HIGH | REST API 전체 라우팅 및 핸들러 |
| infra/storage/plugins/s3/compaction.v | HIGH | 데이터 정합성에 직접 영향 |
| infra/storage/plugins/s3/s3_client.v | HIGH | S3 HTTP 클라이언트 코어 |
| infra/storage/plugins/s3/buffer_manager.v | HIGH | 버퍼 관리, 플러시 로직 |
| infra/replication/client.v, server.v, protocol.v | HIGH | 복제 통신 계층 전체 |
| infra/performance/core/writer_pool.v | HIGH | 70개 기존 테스트 실패 유발 |
| main.v, startup.v | MEDIUM | 진입점, DI 로직 |

프로토콜 계층의 codec/serialization 테스트는 충실하지만, request handler(비즈니스 로직 연결부)가 전혀 테스트되지 않아서 리그레션 위험이 크다.

### 3-2. 알려진 버그 현황

v0.49.0에서 다수의 CRITICAL 이슈가 수정됐다고 changelog에 기록되어 있으나:

- **writer_pool.v 글로벌 변수 문제**: 70개 기존 테스트 실패가 지속
- **수정 검증 부재**: CRIT 이슈 수정사항(race condition, compaction merge order, path traversal)에 대한 회귀 테스트가 없음
- **코드 리뷰 등급**: F (57.3점), BLOCK 상태 -- 10 CRITICAL, 16 HIGH, 14 MEDIUM, 14 LOW 이슈
  - CRIT-1: append() race condition (silent data corruption) -- v0.49.0에서 수정 선언
  - CRIT-2: Non-deterministic compaction merge order -- v0.49.0에서 수정 선언
  - CRIT-3: Path traversal via unvalidated group_id -- v0.49.0에서 수정 선언
  - CRIT-4: config.v God File (1150줄) -- v0.49.0에서 분할 완료
  - CRIT-5: S3StorageAdapter God Class (1243줄) -- v0.49.0에서 4파일 분할
  - CRIT-6: StoragePort ISP violation (17 methods) -- v0.49.0에서 6개 서브 인터페이스로 분리 완료

### 3-3. 에러 처리 패턴 평가

**강점:**
- panic() 사용 금지 원칙 준수
- V의 Result 타입(`!`)을 일관적으로 사용
- Kafka 호환 에러 코드 매핑 (110+ 에러 코드)
- 지수 백오프 재시도 (복제 클라이언트: 100ms * 2^attempt, 최대 5s)
- 브로커 등록 재시도 (3회, 지수 대기: 2s, 4s)

**약점:**
- 일부 `or { }` 블록에서 에러를 삼키는 패턴이 존재할 가능성
- 복제 재시도 소진 시 데이터 유실 경로 존재
- ErrorPattern이 7개만 기록되어 있음 (lessons.md) -- 축적된 지식이 부족

---

## 4. 성능 분석

### 등급: B (양호, 최적화 여지 있음)

### 4-1. 구현된 최적화 기법

| 기법 | 위치 | 효과 |
|------|------|------|
| Zero-copy I/O | ByteView (codec.v), mmap (io/) | 불필요한 메모리 복사 방지 |
| Buffer Pool / Object Pool | performance/core/ | GC 압력 감소, 메모리 재활용 |
| TCP 최적화 | interface/server/tcp.v | TCP_NODELAY, 256KB 송수신 버퍼, pre-allocated 64KB 요청 버퍼 |
| Worker Pool | interface/server/worker_pool.v | 채널 기반 세마포어, NUMA-aware 워커 바인딩 |
| io_uring | performance/engines/linux.v | Linux에서 비동기 I/O (syscall 오버헤드 감소) |
| NUMA-aware 처리 | performance/engines/numa.v | NUMA 노드별 워커 바인딩으로 메모리 지역성 향상 |
| S3 PUT 비용 최적화 | storage/plugins/s3/ | sync_linger, batch buffering으로 60-80% PUT 비용 감소 |
| 병렬 Fetch | service/broker/fetch.v | 2개+ 파티션 시 spawn + chan으로 병렬 처리 |
| SigV4 서명 키 캐싱 | storage/plugins/s3/s3_signing.v | S3 요청 시 서명 연산 재활용 |
| RecordBatch zero-copy 인코딩 | protocol/kafka/record_batch.v | pre-allocation + CRC32C + zero-copy |
| 파티션별 락 | storage/plugins/memory/adapter.v | 파티션 수준 RwMutex로 경합 최소화 |
| O(1) 멤버 조회 | service/group/coordinator.v | members_map으로 O(1) 룩업 |
| Atomic 메트릭 카운터 | memory adapter, s3 adapter, server | 락 없이 카운터 증감 |

### 4-2. 식별된 병목 지점

| 병목 | 영향도 | 설명 |
|------|--------|------|
| Replication 연결 미풀링 | HIGH | send() 호출마다 새 TCP 연결 생성/파괴. 고부하 시 커넥션 폭증, 지연 증가 |
| Replication JSON 직렬화 | MEDIUM | 바이너리 프로토콜 대비 JSON은 파싱 오버헤드와 대역폭 낭비. length-prefixed JSON 프로토콜 사용 |
| Memory 어댑터 topics_lock 경합 | MEDIUM | 파티션별 락은 있지만, 토픽 수준의 topics_lock(RwMutex)이 토픽 생성/삭제 시 병목 가능 |
| S3 어댑터 다중 락 계층 | MEDIUM | topic_lock, group_lock, offset_lock, buffer_lock, partition_append_locks -- 데드락 위험은 낮지만 경합 가능 |
| CRC32C 소프트웨어 구현 | LOW | 하드웨어 가속(SSE4.2) 없이 테이블 룩업 방식 -- throughput에 소폭 영향 |

### 4-3. 벤치마크 인프라 현황

벤치마크 프레임워크는 잘 갖춰져 있다:

- **복제 벤치마크** (tests/bench/): throughput/latency, 256B/1KB/64KB 레코드, 동시 고루틴, 목표 10K+ ops/sec
- **스토리지 비교** (tests/benchmark/): Memory vs S3, 100K 레코드 프로듀스/컨슘
- **I/O 벤치마크** (infra/performance/benchmarks/): io_uring, DMA, mmap 성능 측정
- **장애 복구 테스트** (tests/benchmark/failure_recovery_test.sh)
- **결과 저장**: benchmark_results/, tests/benchmark/results/ (타임스탬프 디렉토리)

---

## 5. 고가용성 분석

### 등급: C (기본 구조만 갖춤, 프로덕션 미달)

### 5-1. 구현된 HA 요소

| 요소 | 상태 | 설명 |
|------|------|------|
| 컨트롤러 선출 | 구현됨 | 분산 락 기반, 주기적 갱신, election_loop 백그라운드 워커 |
| 브로커 등록/탐지 | 구현됨 | 하트비트 기반, 만료 감지, heartbeat_loop 백그라운드 워커 |
| 데이터 복제 | 기본 구현 | 병렬 복제(spawn), 지수 백오프 재시도, Fisher-Yates 셔플 |
| Health Probes | 구현됨 | /health, /healthz, /ready, /readyz, /live, /livez, /metrics |
| Kubernetes 통합 | 구현됨 | readinessProbe + livenessProbe, ConfigMap, Secret, Deployment |
| Graceful Shutdown | 구현됨 | drain timeout 지원, shutdown_chan |
| 모니터링 스택 | 완비 | Grafana + Prometheus + Loki + OTel Collector + AlertManager |
| 복제 메트릭 | 구현됨 | ReplicationMetrics: 처리량, 지연, p95/p99, 롤링 윈도우(1024) |

### 5-2. 부재한 HA 요소 (심각)

| 부재 요소 | 영향도 | 설명 |
|-----------|--------|------|
| 파티션 리밸런싱 | CRITICAL | 브로커 추가/제거 시 파티션이 재분배되지 않음. 특정 브로커에 부하 집중. broker_registry.v:434에 TODO |
| ISR (In-Sync Replicas) 관리 | CRITICAL | 도메인 모델에 isr_nodes 필드가 있지만 실제 ISR 추적/관리 로직 미구현. 복제 지연 감지 불가 |
| 파티션 레벨 리더 선출 | CRITICAL | Partition.leader_epoch이 항상 0. 리더 장애 시 자동 페일오버 불가 |
| 복제 ACK 일관성 | HIGH | min.insync.replicas 설정은 있지만 produce 시 ISR 기반 ACK 검증 미구현. acks=all 시맨틱 보장 불가 |
| 복제 연결 풀링 | HIGH | 연결 수명이 단일 요청. 네트워크 자원 낭비, 지연 증가, 고부하 시 커넥션 폭증 |
| Split-brain 방지 | HIGH | 컨트롤러 선출은 있지만 네트워크 파티션 시 split-brain 시나리오 미처리 |
| 데이터 복구 프로토콜 | MEDIUM | recover 메시지 타입은 정의됐지만 완전한 복구 플로우 미구현 |

### 5-3. Stateless 아키텍처의 트레이드오프

DataCore의 핵심 차별점인 "Stateless Broker"는 양날의 검이다:

**장점:**
- 브로커 인스턴스를 쉽게 스케일 아웃 가능
- 로컬 디스크 의존 없음, 운영 단순화
- 브로커 교체/재시작이 빠름 (로컬 상태 복구 불필요)

**단점:**
- S3/PostgreSQL이 Single Point of Failure가 됨
- 스토리지 계층의 가용성에 전적으로 의존하며, 스토리지 장애 시 전체 클러스터 다운
- 네트워크 지연이 모든 연산에 영향 (로컬 디스크 대비)
- S3 eventual consistency가 메시지 정합성에 영향 가능

**권고:** S3 자체의 99.99% 가용성에 의존하되, 스토리지 계층 장애 시 graceful degradation 전략(예: 임시 로컬 버퍼링)을 고려해야 한다.

---

## 6. 보안 분석

### 등급: B- (기본 수준 이상, 추가 강화 필요)

### 6-1. 구현된 보안 기능

| 기능 | 구현 상태 | 비고 |
|------|-----------|------|
| SASL PLAIN 인증 | 구현됨 | 기본 사용자/비밀번호 |
| SASL SCRAM-SHA-256 | 구현됨 | Challenge-response 프로토콜 |
| ACL 기반 인가 | 구현됨 | 13가지 operation, 8가지 resource type, literal/prefixed 패턴 |
| S3 SSRF 방지 | 구현됨 | 20+ 테스트 케이스 (IPv4/IPv6, metadata, ULA, link-local 등) |
| TOML Injection 방지 | 구현됨 | escape_toml_string()으로 특수 문자 이스케이프 |
| AWS SigV4 서명 | 구현됨 | S3 요청 인증, 서명 키 캐싱 |
| TLS 지원 | 설정 존재 | config에 TLS 설정 필드 있음 |
| Principal 모델 | 구현됨 | 익명/인증 사용자 구분, 연결별 인증 상태 관리 |

### 6-2. 미흡한 영역

| 부재 기능 | 영향도 | 비고 |
|-----------|--------|------|
| SCRAM-SHA-512 | MEDIUM | 도메인에 정의만 존재, 구현 없음 |
| OAuthBearer | MEDIUM | 도메인에 정의만 존재, 구현 없음 |
| Rate Limiting | HIGH | DoS 공격에 취약. max_connections는 있지만 요청 레벨 제한 없음 |
| Audit Logging | MEDIUM | 인증/인가 이벤트 감사 로그 미구현 |
| 비밀번호 해시 강도 | LOW | SCRAM iterations 설정의 기본값 검증 필요 |
| 인증서 자동 갱신 | LOW | TLS 인증서 갱신 자동화 미구현 |

---

## 7. 종합 의견 및 우선순위 권고

### 영역별 등급 요약

| 영역 | 등급 | 프로덕션 준비도 |
|------|------|----------------|
| 아키텍처 설계 | B+ | 구조적으로 건실 |
| Kafka API 호환성 | A- | P0/P1 거의 완비 |
| 안정성 (테스트) | C+ | 핵심 경로 테스트 부재 |
| 성능 | B | 최적화 기법 다수 적용 |
| 고가용성 | C | 프로덕션 미달 |
| 보안 | B- | 기본 인증/인가 있음 |
| 운영 인프라 | B+ | 모니터링/CI/CD/K8s 완비 |

### 우선순위 개선 로드맵

#### 1순위: 안정성 확보 (테스트)

핵심 경로의 테스트 부재는 어떤 변경이든 리그레션을 유발할 수 있어, 추가 기능 개발보다 테스트 보강이 선행되어야 한다.

- handler_*.v 16개 파일의 단위 테스트 작성
- produce.v, fetch.v 핵심 경로 테스트
- writer_pool.v 70개 실패 해결
- v0.49.0 CRIT 수정사항(race condition, compaction merge order, path traversal)에 대한 회귀 테스트
- transaction coordinator 테스트

#### 2순위: 고가용성 핵심 기능

멀티 브로커 환경에서의 안정적 운영을 위해 필수적인 기능들이다.

- 파티션 리밸런싱 구현 (broker_registry.v:434 TODO)
- ISR 추적 및 관리 로직 구현
- 파티션 레벨 리더 선출 (leader_epoch 활용)
- 복제 TCP 연결 풀링
- Split-brain 방지 메커니즘

#### 3순위: 성능 병목 해소

현재 성능 최적화 기법이 잘 적용되어 있으나, 복제 계층에 병목이 존재한다.

- Replication 프로토콜을 JSON에서 바이너리(length-prefixed binary)로 전환
- 복제 TCP 연결 풀링 (2순위와 중복)
- CRC32C 하드웨어 가속 (SSE4.2/ARM CRC)
- REST server.v 분할 (성능 + 구조 개선)

#### 4순위: 구조적 개선 및 미구현 기능

- main.v DI 로직을 별도 모듈로 분리
- WriteTxnMarkers 구현
- Rate limiting 추가 (보안)
- SCRAM-SHA-512, OAuthBearer 인증 구현
- Audit logging 추가

### 전체 평가

DataCore는 V 언어로 Kafka 호환 브로커를 구현한다는 도전적인 목표에 대해 설계와 프로토콜 구현 측면에서 인상적인 진척을 보여주고 있다. Clean Architecture 준수, 46개 API 버전 범위 지원, 3종 스토리지 플러그인, KIP-932/KIP-848 같은 최신 프로토콜 지원은 상당한 성과이다.

그러나 프로덕션 배포를 위해서는 테스트 커버리지와 고가용성이 가장 큰 걸림돌이다. 특히 요청 핸들러 계층의 테스트 부재는 어떤 변경이든 리그레션을 유발할 수 있어, 추가 기능 개발보다 테스트 보강이 먼저 이루어져야 한다. 파티션 리밸런싱과 ISR 관리 없이는 멀티 브로커 환경에서의 안정적 운영이 불가능하다.

현재 단계에서는 "기능 구현 완성도"보다 "기존 구현의 안정화"에 집중하는 것이 바람직하며, 테스트 보강 -> HA 핵심 기능 -> 성능 최적화 -> 구조 개선 순서로 진행할 것을 권고한다.

---

## 8. 2026-03-22 개선 스프린트 이후 업데이트

### 등급 변경

| 영역 | 이전 | 이후 | 변경 사항 |
|------|------|------|-----------|
| 아키텍처 설계 | B+ | B+ | 유지 (구조 분리 개선: server.v, main.v) |
| Kafka API 호환성 | A- | A | WriteTxnMarkers 구현 완료 |
| 안정성 (테스트) | C+ | B+ | 89 -> 125 테스트 파일, 1,704 함수 |
| 성능 | B | B+ | 연결 풀링, 바이너리 프로토콜, Rate Limiting |
| 고가용성 | C | B- | ISR 관리, 파티션 리밸런싱, 리더 선출 |
| 보안 | B- | B | SCRAM-SHA-512 테스트, 감사 로깅 |
| 운영 인프라 | B+ | B+ | 유지 |

### 전체 등급: C+ -> B

### 잔여 작업
- ISR Manager를 produce 경로에 연결 (min.insync.replicas 검증)
- RebalanceTrigger를 startup.v에 연결
- PartitionLeaderElector를 클러스터 관리에 연결
- 복제 프로토콜을 JSON에서 바이너리로 마이그레이션
- Rate Limiter 설정을 config.toml에 추가
- Audit Logger를 SASL 인증 핸들러에 연결
- infra/performance V 0.5 호환성 이슈 수정

---

## 9. v0.50.1 Stateless Architecture Correction

v0.50.0 sprint에서 ISR Manager, Rebalance Trigger, Partition Leader Election이 추가되었으나,
DataCore의 핵심 설계 원칙인 Stateless Broker 아키텍처와 충돌하는 것으로 확인되어 v0.50.1에서 제거했다.

Stateless Broker에서는:
- 모든 데이터가 외부 스토리지(S3/PostgreSQL/Memory)에 존재
- 어떤 브로커든 어떤 파티션이든 서빙 가능
- 파티션 "소유권" 개념이 없으므로 ISR/리밸런싱/리더선출 불필요
- leader_epoch는 항상 0 (설계 의도)

추가 개선 사항:
- Replication: JSON -> Binary 프로토콜 전환 (wire size 감소)
- Rate Limiter: config.toml에서 설정 가능
- Audit Logger: SASL 인증 이벤트 기록
- V 0.5 호환성: infra/performance 모듈 전체 테스트 통과
