# DataCore - Technical Requirements Document (TRD)

## 0. 버전 정보

| 항목 | 값 |
|------|-----|
| **현재 버전** | v0.48.0 |
| **릴리즈 날짜** | 2026-03-06 |
| **버전 체계** | `<릴리즈>.<메이저>.<마이너>` |

### 버전 정책

DataCore는 **릴리즈.메이저.마이너** 버전 체계를 따릅니다:

| 구분 | 설명 | 예시 |
|------|------|------|
| **릴리즈** | 대규모 변경, 안정 버전 출시 | 0.x → 1.0.0 |
| **메이저** | 새로운 기능 그룹 추가 | 0.1.0 → 0.2.0 |
| **마이너** | 버그 수정, 작은 개선 | 0.2.0 → 0.2.1 |

### 버전 히스토리

| 버전 | 날짜 | 주요 변경사항 |
|------|------|---------------|
| v0.1.0 | 2025-01-17 | 초기 개발: Kafka 프로토콜, Storage, CLI |
| v0.2.0 | 2025-01-17 | 성능 최적화: Buffer Pool, Object Pool, Zero-Copy |
| v0.3.0 | 2025-01-17 | 성능 통합: Performance Manager, Benchmark Suite |
| v0.4.0 | 2026-01-18 | 고급 I/O: Slice, Mmap, DMA, io_uring, NUMA |
| v0.5.0 | 2026-01-18 | KIP-848 New Consumer Protocol, Zero-Copy 프로토콜 통합 |
| v0.6.0 | 2026-01-18 | Schema Registry 호환성 검사 완성 (Avro/JSON/Protobuf) |
| v0.7.0 | 2026-01-18 | S3 Storage Plugin 구현 (ETag 기반 조건부 쓰기, 캐싱) |
| v0.8.0 | 2026-01-18 | Admin API 기본 완성 (CreateTopics, DeleteTopics, ListGroups, DescribeGroups) |
| v0.9.0 | 2026-01-18 | 성능 모듈 아키텍처 개선 (Plugin 기반, Manager 전략 패턴 적용) |
| v0.10.0 | 2026-01-18 | Kafka API 호환성 개선 (Downgrade for stability) |
| v0.11.0 | 2026-01-19 | Produce API v13+ 지원 (Topic ID, Flexible Version) |
| v0.42.0 | 2026-02-01 | BinaryWriter Buffer Pooling, Sharded Atomic Metrics, TCP Optimization |
| v0.43.0 | 2026-02-03 | Iceberg REST Catalog Service Implementation (v3 format support) |
| v0.44.0 | 2026-02-10 | S3 Hybrid ACK Policy, Kubernetes/Docker 프로덕션 배포 설정, Linux 정적 빌드 |
| v0.44.1 | 2026-02-18 | Inter-Broker In-Memory Replication 구현, 레이스 컨디션 수정 (4개 mutex) |
| v0.44.2 | 2026-02-20 | 코드 주석 영어 번역 (126+ 파일) |
| v0.44.3 | 2026-02-21 | Comprehensive Replication Metrics (throughput, latency, queue, connection) |
| v0.44.4 | 2026-02-21 | Auth 보안 취약점 수정, 공통 유틸리티 `infra.performance.core`로 중앙화 |
| v0.48.0 | 2026-03-06 | S3 PUT 비용 최적화 (flush 임계값, 인덱스 배치, Multi-Object Delete, sync linger, 서버사이드 복사) |

---

## 1. Clean Architecture (필수 준수)

> ⚠️ **중요**: 모든 코드는 반드시 Clean Architecture 원칙을 따라야 합니다.

### 1.1 아키텍처 레이어

```
┌─────────────────────────────────────────────────────────────────┐
│                    Interface Layer                              │
│         (CLI, Kafka Protocol Server, REST API, gRPC)           │
├─────────────────────────────────────────────────────────────────┤
│                    Infra Layer                                  │
│    (Protocol Handlers, Storage Adapters, External Gateways)    │
├─────────────────────────────────────────────────────────────────┤
│                    Service Layer                                │
│     (Broker Service, Topic Manager, Group Coordinator)         │
├─────────────────────────────────────────────────────────────────┤
│                    Domain Layer                                 │
│        (Record, Topic, Partition, ConsumerGroup, Schema)       │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 의존성 규칙 (Dependency Rule)

```
Interface → Infra → Service → Domain
   (↓)       (↓)      (↓)       (X)
```

- **Domain**: 어떤 레이어에도 의존하지 않음 (가장 안정적)
- **Service**: Domain에만 의존
- **Infra**: Service와 Domain에 의존
- **Interface**: Infra, Service, Domain에 의존 가능

### 1.3 레이어별 책임

| 레이어 | 책임 | 예시 |
|--------|------|------|
| **Domain** | 핵심 비즈니스 로직, 도메인 모델 | Record, Topic, Partition, ConsumerGroup |
| **Service** | 애플리케이션 비즈니스 규칙 | ProduceService, FetchService, TopicManager |
| **Infra** | 외부 시스템과의 데이터 변환 | StorageAdapter, KafkaProtocolHandler |
| **Interface** | 외부 인터페이스 제공 | TCP Server, CLI, REST API |

### 1.4 코드 작성 원칙

```v
// ✅ 올바른 예: Service는 Domain만 import
module service

import domain  // OK - 하위 레이어
// import infra  // ❌ 금지 - 상위 레이어

// ✅ 올바른 예: Infra는 Service, Domain import 가능
module infra

import domain   // OK
import service  // OK
// import interface   // ❌ 금지 - 같은/상위 레이어
```

### 1.5 인터페이스 정의 위치

- **Port (인터페이스)는 Service 레이어에 정의**
- **Adapter가 해당 Port를 구현**

```v
// service/port/storage_port.v - Service 레이어에 인터페이스 정의
module port

pub interface StoragePort {
    append(topic string, partition int, records []domain.Record) !domain.AppendResult
    fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult
}

// infra/storage/memory_adapter.v - Infra가 구현
module storage

import service.port
import domain

pub struct MemoryAdapter {}

pub fn (m &MemoryAdapter) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
    // 구현
}
```

---

## 2. 프로젝트 구조 (Clean Architecture 기반)

```
datacore/
├── src/
│   ├── main.v                           # Entry point
│   │
│   ├── domain/                          # 🔵 Domain Layer (도메인 모델)
│   │   ├── record.v                     # Record, RecordBatch
│   │   ├── topic.v                      # Topic, TopicConfig
│   │   ├── partition.v                  # Partition, PartitionInfo
│   │   ├── consumer_group.v             # ConsumerGroup, GroupMember
│   │   ├── schema.v                     # Schema, SchemaVersion
│   │   ├── offset.v                     # OffsetInfo, PartitionOffset
│   │   └── error.v                      # 도메인 에러 정의
│   │
│   ├── service/                         # 🟢 Service Layer (비즈니스 로직)
│   │   ├── port/                        # 포트 (인터페이스) 정의
│   │   │   ├── storage_port.v           # 스토리지 포트
│   │   │   ├── protocol_port.v          # 프로토콜 포트
│   │   │   └── observability_port.v     # 관측성 포트
│   │   │
│   │   ├── broker/                      # 브로커 서비스
│   │   │   ├── produce.v                # Produce 처리
│   │   │   ├── fetch.v                  # Fetch 처리
│   │   │   └── metadata.v               # Metadata 처리
│   │   │
│   │   ├── topic/                       # 토픽 관리
│   │   │   └── manager.v                # TopicManager
│   │   │
│   │   ├── group/                       # Consumer Group 관리
│   │   │   ├── coordinator.v            # GroupCoordinator
│   │   │   └── rebalance.v              # 리밸런싱 로직
│   │   │
│   │   ├── offset/                      # 오프셋 관리
│   │   │   └── manager.v                # OffsetManager
│   │   │
│   │   └── schema/                      # Schema Registry
│   │       ├── registry.v               # SchemaRegistry
│   │       └── compatibility.v          # 호환성 검사
│   │
│   ├── infra/                           # 🟡 Infra Layer (외부 시스템 연결)
│   │   ├── storage/                     # 스토리지 인프라
│   │   │   ├── interface.v              # StoragePlugin 인터페이스
│   │   │   ├── registry.v               # Plugin Registry
│   │   │   └── plugins/
│   │   │       ├── memory/              # Memory Storage (P0)
│   │   │       │   ├── adapter.v
│   │   │       │   └── partition.v
│   │   │       ├── s3/                  # S3 Storage (P1)
│   │   │       │   ├── adapter.v        # S3 Storage Adapter
│   │   │       │   ├── client.v         # S3 Client with Retry logic
│   │   │       │   ├── iceberg_catalog.v # Iceberg Catalog Implementation
│   │   │       │   ├── iceberg_writer.v  # Parquet/Iceberg Writer
│   │   │       │   ├── iceberg_types.v   # Iceberg Metadata v2/v3 Types
│   │   │       │   └── iceberg_rest_types.v # Iceberg REST API Types
│   │   │       └── postgres/            # PostgreSQL Storage (P2)
│   │   │
│   │   ├── protocol/                    # 프로토콜 인프라
│   │   │   ├── kafka/                   # Kafka Protocol
│   │   │   │   ├── handler.v            # Kafka Request Handler
│   │   │   │   ├── codec.v              # Binary Encoder/Decoder
│   │   │   │   ├── request.v            # Request Parsing
│   │   │   │   ├── response.v           # Response Building
│   │   │   │   └── types.v              # Kafka Types (ApiKey, ErrorCode)
│   │   │   ├── grpc/                    # gRPC (P2)
│   │   │   ├── sse/                     # SSE (P2)
│   │   │   └── websocket/               # WebSocket (P2)
│   │   │
│   │   └── observability/               # 관측성 인프라
│   │       ├── otel.v                   # OpenTelemetry
│   │       ├── metrics.v                # Metrics
│   │       ├── tracing.v                # Tracing
│   │       └── logging.v                # Logging
│   │
│   │   └── performance/                 # ⚙️ 성능 최적화 인프라
│   │       ├── core/                    # Pool/Buffer Management
│   │       │   └── writer_pool.v        # BinaryWriter Buffer Pooling
│   │       ├── io/                      # Zero-Copy, Mmap, Slice
│   │       ├── observability/            # Performance Metrics 
│   │       │   └── atomic_metrics.v     # Sharded Atomic Counters
│   │       ├── engines/                 # Performance Engine Plugins
│   │       └── benchmarks/              # 성능 벤치마크
│   │
│   │
│   ├── interface/                       # 🔴 Interface Layer (외부 인터페이스)
│   │   ├── server/                      # TCP 서버
│   │   │   ├── tcp.v                    # TCP Server
│   │   │   ├── connection.v             # Connection Manager
│   │   │   └── pipeline.v               # Request Pipelining
│   │   │
│   │   ├── cli/                         # CLI 인터페이스
│   │   │   ├── cmd.v                    # CLI Entry
│   │   │   ├── broker.v                 # broker 명령
│   │   │   ├── topic.v                  # topic 명령
│   │   │   └── group.v                  # group 명령
│   │   │
│   │   └── rest/                        # REST API
│   │       ├── schema.v                 # Schema REST API
│   │       └── iceberg_catalog_api.v    # Iceberg REST Catalog API
│   │
│   └── config/                          # 설정
│       └── config.v                     # 설정 로드/파싱
│
├── tests/
│   ├── unit/
│   │   ├── domain/
│   │   ├── service/
│   │   └── infra/
│   ├── integration/
│   └── e2e/
│
├── docs/
├── v.mod
└── README.md
```

---

## 3. 레이어간 데이터 흐름

### 3.1 Produce 요청 흐름

```
[Kafka Client]
      │
      ▼
┌─────────────────┐
│ interface/server│  TCP 수신, 바이너리 파싱 요청
│ (Interface)     │
└────────┬────────┘
         │ raw bytes
         ▼
┌─────────────────┐
│ infra/protocol  │  Kafka 바이너리 → ProduceRequest 변환
│ (Infra)         │
└────────┬────────┘
         │ domain.Record[]
         ▼
┌─────────────────┐
│ service/broker  │  비즈니스 로직 실행 (검증, 처리)
│ (Service)       │
└────────┬────────┘
         │ domain.Record[]
         ▼
┌─────────────────┐
│ infra/storage   │  스토리지에 저장
│ (Infra)         │
└─────────────────┘
```

### 3.2 의존성 주입 (DI)

```v
// main.v에서 의존성 조립
fn main() {
    // Domain: 의존성 없음
    
    // Infra 생성
    storage := infra.new_memory_storage()
    protocol := infra.new_kafka_handler()
    
    // Service 생성 (Infra를 Port로 주입)
    broker := service.new_broker_service(storage, protocol)
    topic_mgr := service.new_topic_manager(storage)
    
    // Interface 생성 (Service 주입)
    server := interface.new_server(broker, topic_mgr)
    server.start()!
}
```

### 3.3 Offset Service Refactoring (v0.35.0)

Clean Architecture를 강화하기 위해 기존 Kafka Handler에 산재해 있던 오프셋 관리 로직을 전용 서비스 레이어로 분리했습니다.

- **OffsetManager (Service)**: 오프셋 커밋/페치 비즈니스 로직 담당
- **Benefit**:
  - 핸들러와 스토리지 간의 결합도 감소
  - 단위 테스트 용이성 확보 (Mocking 가능)
  - 오프셋 만료 및 트랜잭션 오프셋 로직 중앙 집중화

---

## 4. Storage Plugin Interface

### 4.1 플러그인 메타데이터

```v
// storage/plugin.v
module storage

// 플러그인 메타데이터
pub struct PluginInfo {
pub:
    name        string
    version     string
    description string
    author      string
}
```

### 4.2 플러그인 인터페이스

```v
// 플러그인 인터페이스 - 모든 스토리지 플러그인이 구현
pub interface StoragePlugin {
    // 메타데이터
    info() PluginInfo
    
    // 라이프사이클
    init(config map[string]string) !
    shutdown() !
    health_check() !HealthStatus
    
    // 엔진 인스턴스 반환
    create_engine() !StorageEngine
}
```

### 4.3 스토리지 엔진 인터페이스

```v
// 스토리지 엔진 인터페이스
pub interface StorageEngine {
    // Topic 관리
    create_topic(name string, partitions int, config TopicConfig) !
    delete_topic(name string) !
    list_topics() ![]TopicMetadata
    get_topic(name string) !TopicMetadata
    add_partitions(name string, new_count int) !
    
    // 메시지 저장/조회
    append(topic string, partition int, records []Record) !AppendResult
    fetch(topic string, partition int, offset i64, max_bytes int) !FetchResult
    delete_records(topic string, partition int, before_offset i64) !
    
    // 오프셋 관리
    get_offsets(topic string, partition int) !OffsetInfo
    
    // Consumer Group
    save_group_metadata(group_id string, metadata GroupMetadata) !
    load_group_metadata(group_id string) !GroupMetadata
    commit_offsets(group_id string, offsets []PartitionOffset) !
    fetch_offsets(group_id string, partitions []TopicPartition) ![]PartitionOffset
    list_groups() ![]GroupInfo
    delete_group(group_id string) !
    
    // Config 관리
    get_config(entity_type ConfigEntityType, entity_name string) !map[string]ConfigEntry
    set_config(entity_type ConfigEntityType, entity_name string, configs map[string]string) !
    
    // 동시성 제어
    acquire_partition_lock(topic string, partition int) !Lock
}
```

### 4.4 Admin Manager 인터페이스

```v
// Admin Manager 인터페이스
pub interface AdminManager {
    // Topic Admin
    create_topic(req CreateTopicRequest) !CreateTopicResponse
    delete_topic(name string) !
    create_partitions(topic string, new_total int) !
    delete_records(topic string, partitions map[int]i64) !map[int]i64
    
    // Group Admin
    list_groups(states []string) ![]GroupListing
    describe_groups(group_ids []string) ![]GroupDescription
    delete_groups(group_ids []string) !map[string]ErrorCode
    
    // Config Admin
    describe_configs(resources []ConfigResource) ![]DescribeConfigsResult
    alter_configs(resources []AlterConfigsResource) ![]AlterConfigsResult
    
    // Cluster Admin
    describe_cluster() !ClusterDescription
    describe_log_dirs(broker_ids []int) ![]LogDirDescription
}
```

### 4.5 결과 타입

```v
pub struct AppendResult {
pub:
    base_offset   i64
    log_append_time i64
}

pub struct FetchResult {
pub:
    records      []Record
    high_watermark i64
    // Zero-Copy 지원 시 파일 디스크립터 반환
    zero_copy_fd ?int
    zero_copy_offset i64
    zero_copy_len int
}
```

### 4.6 S3 Storage Plugin (v0.7.0)

S3 스토리지 플러그인은 AWS S3 호환 객체 스토리지(S3, MinIO, LocalStack 등)를 백엔드로 사용합니다.

#### 4.6.1 S3 키 구조

```
{prefix}/
├── topics/
│   └── {topic_name}/
│       ├── metadata.json              # TopicMetadata
│       └── partitions/
│           └── {partition}/
│               ├── index.json         # PartitionIndex
│               └── log-{start}-{end}.bin  # Log Segments
├── groups/
│   └── {group_id}/
│       └── state.json                 # ConsumerGroup state
└── offsets/
    └── {group_id}/
        └── {topic}:{partition}.json   # Committed offsets
```

#### 4.6.2 설정

```v
pub struct S3Config {
pub:
    bucket_name  string = 'datacore'
    region       string = 'us-east-1'
    endpoint     string = ''           // MinIO/LocalStack용 커스텀 엔드포인트
    access_key   string
    secret_key   string
    prefix       string = 'datacore'   // S3 키 프리픽스

    // S3 PUT 비용 최적화 (v0.48.0)
    min_flush_bytes          int  = 4096  // 최소 flush 크기 (이하면 skip)
    max_flush_skip_count     int  = 40    // 최대 skip 횟수 (초과 시 강제 flush)
    index_batch_size         int  = 1     // 인덱스 배치 크기 (1=즉시, N=N개 세그먼트 누적 후 1회 PUT)
    index_flush_interval_ms  int  = 500   // 인덱스 강제 flush 간격 (ms)
    sync_linger_ms           int  = 0     // sync 경로 linger (0=비활성, 양수=ms 단위 대기)
    use_server_side_copy     bool = true  // 컴팩션 시 서버사이드 복사 시도
}
```

#### 4.6.3 동시성 제어 (Conditional Writes)

S3의 ETag를 사용한 조건부 쓰기로 동시성 제어:

```v
// 읽기 시 ETag 저장
obj := s3.get_object(key)!
etag := obj.etag

// 쓰기 시 If-Match로 조건부 업데이트
s3.put_object(key, data, if_match: etag) or {
    if err.msg().contains('PreconditionFailed') {
        // 충돌 - 재시도 필요
    }
}
```

#### 4.6.4 캐싱 전략

로컬 캐시로 S3 요청 최소화:

| 캐시 대상 | TTL | 용도 |
|----------|-----|------|
| TopicMetadata | 30초 | 토픽 목록, 설정 조회 |
| PartitionIndex | 10초 | 오프셋, 세그먼트 정보 |
| ConsumerGroup | 5초 | 그룹 상태 조회 |
| Iceberg Metadata | 30초 | Iceberg 테이블 조회 |

#### 4.6.5 안정성 및 성능 최적화 (v0.35.0+)

- **S3 Retry Logic**: 네트워크 transient 에러(OpenSSL syscall 5 등) 대응
  - 최대 3회 재시도, Exponential Backoff 적용
  - `max_consecutive_failures` 기반 워커 백오프
- **Global Config Pattern**: V 언어의 Struct 복사 문제를 해결하기 위한 `__global` 설정 적용
- **HTTP Connection Pooling**: S3 클라이언트 수준의 연결 재사용으로 지연 시간 감소
- **병렬 처리**: 객체 삭제 및 오프셋 커밋 시 병렬 처리 (v0.41.0+)

#### 4.6.5.1 S3 Hybrid ACK Policy (v0.44.0)

`acks` 파라미터를 통해 쓰기 내구성과 성능을 트레이드오프 조정합니다:

| acks 값 | S3 경로 | 레이턴시 | 내구성 |
|---------|---------|----------|--------|
| `acks=0` | 비동기 버퍼 → flush_worker (기존 동작) | <1ms | 낮음 |
| `acks=1` | 동기 S3 PUT + index update | 50~200ms | 높음 |
| `acks=-1` | 동기 S3 PUT + 모든 ISR 확인 | 50~200ms | 최고 |

다중 브로커 환경에서는 `acks=1/-1` 시 인메모리 복제(1~5ms)로 처리하여 레이턴시를 크게 개선합니다.

**S3 어댑터 흐름 (단일 브로커):**
```
acks=0: append() → TopicPartitionBuffer → flush_worker → S3 PUT
acks=1: append() → S3 PUT (동기) → index update → 반환
```

**추가된 메트릭:** `sync_append_count`, `sync_append_success_count`, `sync_append_error_count`, `sync_append_total_ms`

#### 4.6.5.2 S3 PUT 비용 최적화 (v0.48.0)

S3 PUT 요청을 60-80% 절감하여 월 비용을 $770에서 $150-300 수준으로 낮추는 최적화입니다.

**최적화 전략 요약:**

| 전략 | 설명 | 예상 절감 |
|------|------|-----------|
| **flush 임계값** | 버퍼 < `min_flush_bytes`이면 flush skip (최대 `max_flush_skip_count`회) | PUT 30-50% |
| **오프셋 저장 배치화** | 개별 JSON PUT 제거, 배치 바이너리 스냅샷만 사용 | PUT 20-30% |
| **Multi-Object Delete** | S3 Multi-Object Delete API로 최대 1000개 일괄 삭제 | DELETE 90%+ |
| **인덱스 배치 업데이트** | N개 세그먼트 누적 후 1회 PUT (`index_batch_size`) | PUT 10-20% |
| **sync linger** | `acks=1/-1` 요청에 짧은 linger 적용 (선택적) | PUT 5-10% |
| **서버사이드 복사** | 컴팩션 시 GET+PUT 대신 S3 CopyObject 사용 | GET+PUT 제거 |

**flush 결정 로직:**
```
flush_requested()
  → if buffer.len < min_flush_bytes AND skip_count < max_flush_skip_count:
      skip_count++
      return (skip)
  → else:
      skip_count = 0
      execute S3 PUT
```

**인덱스 배치 흐름 (`IndexBatchManager`):**
```
segment_closed(seg)
  → batch_buffer.append(seg)
  → if batch_buffer.len >= index_batch_size
     OR elapsed >= index_flush_interval_ms:
      PUT index.json (1회)
      batch_buffer.clear()
```

**sync linger 흐름 (`SyncLinger`):**
```
sync_append(record)
  → if sync_linger_ms == 0:
      immediate S3 PUT
  → else:
      linger_buffer.append(record)
      wait sync_linger_ms
      batch S3 PUT (누적 레코드)
```

**신규 구현 파일:**

| 파일 | 역할 |
|------|------|
| `s3/index_batch_manager.v` | 인덱스 배치 업데이트 관리 |
| `s3/sync_linger.v` | sync 경로 linger 버퍼 |
| `s3/s3_batch_delete.v` | S3 Multi-Object Delete API |
| `s3/s3_server_side_copy.v` | 서버사이드 복사 인프라 |

#### 4.6.6 바이너리 레코드 포맷

Log segment 파일의 레코드 포맷:

```
┌─────────────────────────────────────────────────────┐
│ Record Entry                                        │
├─────────────┬───────────────────────────────────────┤
│ offset      │ 8 bytes (i64, big-endian)             │
│ timestamp   │ 8 bytes (i64, unix_milli, big-endian) │
│ key_len     │ 4 bytes (u32, big-endian)             │
│ key         │ {key_len} bytes                       │
│ value_len   │ 4 bytes (u32, big-endian)             │
│ value       │ {value_len} bytes                     │
│ headers_cnt │ 4 bytes (u32, big-endian)             │
│ headers[]   │ [name_len(2), name, val_len(2), val]  │
└─────────────┴───────────────────────────────────────┘
```

#### 4.6.7 사용 예시

```v
import infra.storage.plugins.s3

// S3 어댑터 생성
config := s3.S3Config{
    bucket_name: 'my-datacore-bucket'
    region: 'ap-northeast-2'
    access_key: env('AWS_ACCESS_KEY_ID')
    secret_key: env('AWS_SECRET_ACCESS_KEY')
}
adapter := s3.new_s3_adapter(config)

// StoragePort로 사용
adapter.create_topic('my-topic', 3, {})!
adapter.append('my-topic', 0, records)!
result := adapter.fetch('my-topic', 0, 0, 1048576)!
```

---

### 4.7 Iceberg REST Catalog (v0.43.0+)

DataCore는 Apache Iceberg REST Catalog API를 준수하여 Spark, Trino와 같은 외부 컴퓨팅 엔진이 DataCore의 데이터를 직접 쿼리할 수 있도록 지원합니다.

#### 4.7.1 아키텍처

- **REST Interface**: `/v1/iceberg/*` 엔드포인트를 통해 표준 Iceberg 요청 처리
- **Metadata Management**: S3 저장소에 Iceberg 스펙(v2/v3)에 맞는 Metadata JSON 및 Manifest 파일 관리
- **Format Support**: Iceberg Table Format v3 지원 (Binary Deletion Vectors, Row Lineage 등)

#### 4.7.2 지원 엔드포인트

- **Config**: `/v1/config` (카탈로그 설정 정보 전달)
- **Namespaces**: `GET/POST /v1/iceberg/namespaces` (CRUD 및 속성 관리)
- **Tables**: `GET/POST /v1/iceberg/namespaces/{ns}/tables` (테이블 로드, 생성, 업데이트, 커밋)

#### 4.7.3 v3 테이블 포맷 특화 기능

- **Binary Deletion Vectors**: 개별 행 삭제를 효율적으로 처리하기 위한 바이너리 벡터 지원
- **Row Lineage Tracking**: 데이터 파일의 시작/끝 도메인 행 번호 추적
- **Advanced Types**: timestamp_ns, variant 등 최신 데이터 타입 메타데이터 대응

---

### 4.8 Inter-Broker In-Memory Replication (v0.44.1+)

#### 4.8.1 설계 동기

S3 어댑터의 `acks=0` 비동기 경로에서 `batch_timeout_ms`(기본 25ms) 동안 데이터가 메모리에만 존재합니다. 이 구간에서 브로커 크래시 시 데이터가 유실됩니다. 인메모리 복제는 이 문제를 낮은 레이턴시(1~5ms)로 해결합니다.

**브로커 수에 따른 분기 전략:**

| 브로커 수 | acks=0 | acks=1/-1 |
|-----------|--------|-----------|
| 1개 | 버퍼 + flush_worker | S3 직접 PUT (현행) |
| 2개+ | 버퍼 + 비동기 복제 + flush_worker | 인메모리 복제 + flush_worker |

#### 4.8.2 전체 복제 흐름 (정상 경로)

```
Producer    Broker A (수신)          Broker B (복제)        S3
  │              │                        │                  │
  │── Produce ──→│                        │                  │
  │              │── 1) 메모리 버퍼 추가   │                  │
  │              │── 2) REPLICATE 전송 ──→│                  │
  │              │                        │── 3) ReplicaBuf  │
  │              │◄── 4) REPLICATE_ACK ───│                  │
  │◄── 5) 응답 ─│                        │                  │
  │              │  ... batch_timeout_ms ...                  │
  │              │── 6) flush_worker S3 PUT ─────────────────→│
  │              │◄──────────────────────────────── 7) OK ───│
  │              │── 8) FLUSH_ACK 전송 ──→│                  │
  │              │                        │── 9) 버퍼 삭제    │
```

#### 4.8.3 Clean Architecture 레이어 매핑

```
┌─────────────────────────────────────────────────┐
│ Interface: handler_produce.v                    │
└─────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────┐
│ Service/Port: ReplicationPort                   │
│   send_replicate(), send_flush_ack()            │
│   store/delete_replica_buffer()                 │
│   get_stats(), update_broker_health()           │
└─────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────┐
│ Infra: infra/replication/                       │
│   manager.v  - ReplicationManager (구현체)      │
│   server.v   - TCP 복제 서버                    │
│   client.v   - TCP 복제 클라이언트              │
│   protocol.v - 바이너리 프로토콜 (4종 메시지)   │
└─────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────┐
│ Domain: domain/replication.v                    │
│   ReplicationMessage, ReplicaBuffer             │
│   ReplicationConfig, ReplicationStats           │
│   ReplicationMetrics, ReplicaAssignment         │
│   BrokerRef, ReplicationHealth                  │
└─────────────────────────────────────────────────┘
```

#### 4.8.4 도메인 모델 핵심 (domain/replication.v)

```v
// 메시지 타입
pub enum ReplicationType {
    replicate      // REPLICATE: 레코드 복제 요청
    replicate_ack  // REPLICATE_ACK: 복제 확인
    flush_ack      // FLUSH_ACK: S3 flush 완료 통지
    heartbeat      // HEARTBEAT: 브로커 생존 확인
    recover        // RECOVER: 장애 복구
}

// 복제 설정 (기본값)
pub struct ReplicationConfig {
pub mut:
    enabled                       bool        // 복제 활성화 (기본: false)
    replication_port               int         // TCP 포트 (기본: 9094)
    replica_count                  int         // 복제본 수 (기본: 2)
    replica_timeout_ms             int         // ACK 대기 시간 (기본: 5000)
    heartbeat_interval_ms          int         // 하트비트 간격 (기본: 3000)
    reassignment_interval_ms       int         // 재할당 주기 (기본: 30000)
    orphan_cleanup_interval_ms     int         // orphan 정리 주기 (기본: 60000)
    retry_count                    int = 3     // 최대 재시도 횟수
    replica_buffer_ttl_ms          i64 = 60000 // 버퍼 TTL (ms)
    max_replica_buffer_size_bytes  i64         // 버퍼 최대 크기 (0=무제한)
}
```

#### 4.8.5 ReplicationPort 인터페이스 (service/port/replication_port.v)

```v
pub interface ReplicationPort {
mut:
    start() !
    stop() !
    send_replicate(topic string, partition i32, offset i64, records_data []u8) !
    send_flush_ack(topic string, partition i32, offset i64) !
    store_replica_buffer(buffer domain.ReplicaBuffer) !
    delete_replica_buffer(topic string, partition i32, offset i64) !
    get_all_replica_buffers() ![]domain.ReplicaBuffer
    get_stats() domain.ReplicationStats
    update_broker_health(broker_id string, health domain.ReplicationHealth)
}
```

#### 4.8.6 TCP 복제 프로토콜 (infra/replication/protocol.v)

바이너리 인코딩 메시지 포맷:

```
ReplicateMessage 구조:
┌─────────────────────────────────────────────────┐
│ message_type (1 byte) = 0x01                    │
│ batch_id (8 bytes, u64)                         │
│ topic_len (2 bytes) + topic (UTF-8)             │
│ partition (4 bytes, i32)                        │
│ records_count (4 bytes) + records (RecordBatch) │
│ checksum (4 bytes, CRC32)                       │
└─────────────────────────────────────────────────┘
```

#### 4.8.7 ReplicationManager 핵심 기능 (infra/replication/manager.v)

- **복제 브로커 선정**: 랙 분산 → 파티션 균등 분배 → 부하 균형 → broker_id 오름차순
- **동시성 제어**: 4개 독립 mutex (`replica_buffers_lock`, `assignments_lock`, `broker_health_lock`, `stats_lock`) - 데드락 방지용 lock ordering 적용
- **orphan 감지**: `orphan_cleanup_worker`가 주기적으로 FLUSH_ACK 미수신 배치 확인 → 브로커 하트비트 실패 시 직접 S3 PUT
- **메트릭**: `ReplicationMetrics`로 throughput, latency percentile(p95/p99), queue depth, connection 상태 추적

#### 4.8.8 구현 파일 목록

| 파일 | 설명 |
|------|------|
| `src/domain/replication.v` | 도메인 모델 전체 |
| `src/service/port/replication_port.v` | ReplicationPort 인터페이스 |
| `src/infra/replication/manager.v` | ReplicationManager 구현 |
| `src/infra/replication/server.v` | TCP 복제 서버 |
| `src/infra/replication/client.v` | TCP 복제 클라이언트 |
| `src/infra/replication/protocol.v` | 바이너리 프로토콜 (4종 메시지) |

#### 4.8.9 config.toml 설정

```toml
[replication]
enabled                       = false   # 복제 활성화
replication_port               = 9094   # 복제용 TCP 포트
replica_count                  = 2      # 복제본 수 (원본 포함)
replica_timeout_ms             = 5000   # 복제 ACK 대기 시간
heartbeat_interval_ms          = 3000   # 하트비트 간격
reassignment_interval_ms       = 30000  # replica_brokers 재할당 주기
orphan_cleanup_interval_ms     = 60000  # orphan buffer 정리 주기
retry_count                    = 3      # 최대 복제 재시도 횟수
replica_buffer_ttl_ms          = 60000  # 버퍼 TTL (ms)
max_replica_buffer_size_bytes  = 0      # 버퍼 최대 크기 (0=무제한)
```

---

## 5. Kafka Protocol 버전 처리

### 5.1 요청 파싱

```v
// protocol/request.v
module protocol

// API 버전별 요청 파서
pub fn parse_request(api_key i16, api_version i16, data []u8) !Request {
    match api_key {
        0 => parse_produce_request(api_version, data)
        1 => parse_fetch_request(api_version, data)
        3 => parse_metadata_request(api_version, data)
        18 => parse_api_versions_request(api_version, data)
        // ... 기타 API
        else => error('Unknown API key: ${api_key}')
    }
}
```

### 5.2 버전별 필드 처리

```v
// Produce 요청 - 버전별 필드 차이 처리
fn parse_produce_request(version i16, data []u8) !ProduceRequest {
    mut req := ProduceRequest{}
    mut reader := new_reader(data)
    
    // v3+: Transactional ID
    if version >= 3 {
        req.transactional_id = reader.read_nullable_string()!
    }
    
    req.acks = reader.read_i16()!
    req.timeout_ms = reader.read_i32()!
    
    // Topic Loop
    topic_count := reader.read_array_len()!
    for _ in 0 .. topic_count {
        // v13+: Topic ID (UUID) 사용
        if version >= 13 {
            topic_id := reader.read_uuid()!
        } else {
            name := reader.read_string()!
        }
        
        // ... partition loop ...
        // v0-v2: MessageSet, v3+: RecordBatch
    }
    
    return req
}
```

---

## 6. Admin API 프로토콜 처리

### 6.1 CreateTopics 처리

```v
// protocol/kafka/admin/topics.v
module admin

// CreateTopics 요청 처리 (API Key 19, v0-v7)
pub fn handle_create_topics(version i16, req CreateTopicsRequest, 
                             admin &AdminManager) !CreateTopicsResponse {
    mut responses := []CreateTopicsResponseTopic{}
    
    for topic in req.topics {
        // 토픽 생성 시도
        result := admin.create_topic(CreateTopicRequest{
            name: topic.name
            num_partitions: topic.num_partitions
            replication_factor: topic.replication_factor
            assignments: topic.assignments
            configs: topic.configs
        }) or {
            responses << CreateTopicsResponseTopic{
                name: topic.name
                error_code: err_to_kafka_error(err)
                error_message: if version >= 1 { err.msg() } else { '' }
            }
            continue
        }
        
        responses << CreateTopicsResponseTopic{
            name: topic.name
            error_code: .none
            num_partitions: if version >= 5 { result.num_partitions } else { 0 }
            replication_factor: if version >= 5 { result.replication_factor } else { 0 }
            configs: if version >= 5 { result.configs } else { [] }
        }
    }
    
    return CreateTopicsResponse{
        throttle_time_ms: if version >= 2 { 0 } else { 0 }
        topics: responses
    }
}
```

### 6.2 DescribeGroups 처리

```v
// DescribeGroups 요청 처리 (API Key 15, v0-v5)
pub fn handle_describe_groups(version i16, req DescribeGroupsRequest,
                               admin &AdminManager) !DescribeGroupsResponse {
    groups := admin.describe_groups(req.groups)!
    
    mut response_groups := []DescribedGroup{}
    for g in groups {
        response_groups << DescribedGroup{
            error_code: .none
            group_id: g.group_id
            group_state: g.state
            protocol_type: g.protocol_type
            protocol_data: g.protocol_data
            members: g.members.map(fn (m) DescribedGroupMember {
                return DescribedGroupMember{
                    member_id: m.member_id
                    client_id: m.client_id
                    client_host: m.client_host
                    member_metadata: m.metadata
                    member_assignment: m.assignment
                }
            })
            authorized_operations: if version >= 3 { g.authorized_ops } else { 0 }
        }
    }
    
    return DescribeGroupsResponse{
        throttle_time_ms: if version >= 1 { 0 } else { 0 }
        groups: response_groups
    }
}
```

---

## 7. Consumer Group Protocol 구현

### 7.1 Protocol 타입 정의

```v
// protocol/kafka/consumer_group.v
module kafka

// Group Protocol 타입
pub enum GroupProtocolType {
    classic   // Client-side assignment (JoinGroup/SyncGroup)
    consumer  // Server-side assignment (KIP-848)
    share     // Record-level distribution (KIP-932)
}

// Group 상태
pub enum GroupState {
    empty
    preparing_rebalance
    completing_rebalance
    stable
    dead
}
```

### 7.2 Classic Protocol 처리 (Client-side Assignment)

```v
// protocol/kafka/consumer_group.v

// JoinGroup 처리 - Leader가 assignment 계산
pub fn handle_join_group(version i16, req JoinGroupRequest, 
                          coordinator &GroupCoordinator) !JoinGroupResponse {
    // 1. 그룹 메타데이터 로드/생성
    group := coordinator.get_or_create_group(req.group_id)!
    
    // 2. 멤버 등록
    member_id := if req.member_id == '' {
        generate_member_id(req.client_id)
    } else {
        req.member_id
    }
    
    group.add_member(member_id, req.protocols, req.session_timeout_ms)!
    
    // 3. 리밸런스 대기 (모든 멤버 JoinGroup 할 때까지)
    group.await_join(req.rebalance_timeout_ms)!
    
    // 4. Leader 선출 및 응답
    is_leader := group.leader == member_id
    return JoinGroupResponse{
        generation_id: group.generation
        protocol_name: group.protocol
        leader: group.leader
        member_id: member_id
        members: if is_leader { group.get_members_for_leader() } else { [] }
    }
}

// SyncGroup 처리 - Leader의 assignment를 모든 멤버에게 전달
pub fn handle_sync_group(version i16, req SyncGroupRequest,
                          coordinator &GroupCoordinator) !SyncGroupResponse {
    group := coordinator.get_group(req.group_id)!
    
    // Leader가 보낸 assignment 저장
    if req.assignments.len > 0 {
        group.set_assignments(req.assignments)!
    }
    
    // 해당 멤버의 assignment 반환
    assignment := group.get_assignment(req.member_id)!
    return SyncGroupResponse{
        assignment: assignment
    }
}
```

### 7.3 Consumer Protocol 처리 (Server-side Assignment, KIP-848)

```v
// protocol/kafka/consumer_protocol.v
module kafka

// ConsumerGroupHeartbeat - 새로운 Protocol의 핵심 API
pub fn handle_consumer_group_heartbeat(version i16, req ConsumerGroupHeartbeatRequest,
                                        coordinator &GroupCoordinator) !ConsumerGroupHeartbeatResponse {
    group := coordinator.get_or_create_group(req.group_id)!
    
    // 1. 멤버 상태 업데이트
    member := group.update_member(
        member_id: req.member_id
        instance_id: req.instance_id
        subscribed_topics: req.subscribed_topic_names
        owned_partitions: req.topic_partitions
    )!
    
    // 2. Server-side Assignment 계산 (Broker가 직접 수행)
    if group.needs_rebalance() {
        // Incremental Assignment 계산
        new_assignment := coordinator.compute_assignment(
            group: group
            assignor: req.server_assignor or { 'uniform' }
        )!
        group.apply_assignment(new_assignment)!
    }
    
    // 3. 해당 멤버의 새 할당 정보 반환
    return ConsumerGroupHeartbeatResponse{
        member_id: member.id
        member_epoch: member.epoch
        heartbeat_interval_ms: group.heartbeat_interval_ms
        assignment: group.get_target_assignment(member.id)
    }
}

// Server-side Assignor Interface
pub interface ServerAssignor {
    name() string
    assign(members []MemberSubscription, topics []TopicMetadata) !map[string]Assignment
}

// Uniform Assignor (기본)
pub struct UniformAssignor {}

pub fn (a &UniformAssignor) name() string {
    return 'uniform'
}

pub fn (a &UniformAssignor) assign(members []MemberSubscription, 
                                    topics []TopicMetadata) !map[string]Assignment {
    mut assignments := map[string]Assignment{}
    
    // 모든 파티션 수집
    mut all_partitions := []TopicPartition{}
    for topic in topics {
        for p in 0 .. topic.partition_count {
            all_partitions << TopicPartition{topic.name, p}
        }
    }
    
    // 균등 분배
    for i, partition in all_partitions {
        member_idx := i % members.len
        member_id := members[member_idx].member_id
        if member_id !in assignments {
            assignments[member_id] = Assignment{}
        }
        assignments[member_id].partitions << partition
    }
    
    return assignments
}
```

### 7.4 Share Groups 처리 (Record-level Distribution, KIP-932)

```v
// protocol/kafka/share_group.v
module kafka

// Share Group 상태
pub struct ShareGroup {
pub mut:
    group_id            string
    members             map[string]ShareGroupMember
    subscribed_topics   []string
    // Share Partition 상태 (파티션별 in-flight 레코드 관리)
    share_partitions    map[TopicPartition]SharePartitionState
}

pub struct SharePartitionState {
pub mut:
    // In-flight 레코드 추적
    in_flight_records   map[i64]InFlightRecord  // offset -> record state
    start_offset        i64
    end_offset          i64
    // 설정
    record_lock_duration_ms  int = 30000
    max_in_flight_records    int = 5000
}

pub struct InFlightRecord {
pub:
    offset          i64
    acquired_by     string  // member_id
    acquired_at     time.Time
    delivery_count  int
}

pub enum AcknowledgeType {
    accept   // 처리 완료 - 다시 전달 안함
    release  // 반환 - 다른 컨슈머에게 전달 가능
    reject   // 거부 - Dead Letter Queue로 이동
}

// ShareGroupHeartbeat 처리
pub fn handle_share_group_heartbeat(version i16, req ShareGroupHeartbeatRequest,
                                     coordinator &ShareGroupCoordinator) !ShareGroupHeartbeatResponse {
    group := coordinator.get_or_create_share_group(req.group_id)!
    
    // 멤버 등록/갱신
    member := group.update_member(req.member_id, req.subscribed_topic_names)!
    
    // Share Group은 파티션 할당 없음 - 모든 멤버가 모든 파티션 접근 가능
    return ShareGroupHeartbeatResponse{
        member_id: member.id
        member_epoch: member.epoch
        heartbeat_interval_ms: group.heartbeat_interval_ms
        // assignment는 없음 - ShareFetch에서 동적으로 레코드 획득
    }
}

// ShareFetch 처리 - 레코드 단위 분배
pub fn handle_share_fetch(version i16, req ShareFetchRequest,
                           coordinator &ShareGroupCoordinator) !ShareFetchResponse {
    group := coordinator.get_share_group(req.group_id)!
    
    mut responses := []ShareFetchResponseTopic{}
    
    for topic_req in req.topics {
        mut partitions := []ShareFetchResponsePartition{}
        
        for partition_req in topic_req.partitions {
            tp := TopicPartition{topic_req.name, partition_req.partition}
            share_state := group.share_partitions[tp]
            
            // 사용 가능한 레코드 획득 (lock)
            records := share_state.acquire_records(
                member_id: req.member_id
                max_records: partition_req.max_records
                max_bytes: req.max_bytes
            )!
            
            partitions << ShareFetchResponsePartition{
                partition: partition_req.partition
                records: records
                acquired_records: records.map(|r| AcquiredRecord{
                    offset: r.offset
                    delivery_count: share_state.in_flight_records[r.offset].delivery_count
                })
            }
        }
        
        responses << ShareFetchResponseTopic{
            topic: topic_req.name
            partitions: partitions
        }
    }
    
    return ShareFetchResponse{
        topics: responses
    }
}

// ShareAcknowledge 처리 - 레코드 Ack/Nack
pub fn handle_share_acknowledge(version i16, req ShareAcknowledgeRequest,
                                 coordinator &ShareGroupCoordinator) !ShareAcknowledgeResponse {
    group := coordinator.get_share_group(req.group_id)!
    
    mut responses := []ShareAcknowledgeResponseTopic{}
    
    for topic_req in req.topics {
        mut partitions := []ShareAcknowledgeResponsePartition{}
        
        for partition_req in topic_req.partitions {
            tp := TopicPartition{topic_req.name, partition_req.partition}
            share_state := group.share_partitions[tp]
            
            // 각 레코드의 acknowledge 처리
            for ack in partition_req.acknowledgements {
                match ack.acknowledge_type {
                    .accept {
                        // 처리 완료 - in-flight에서 제거
                        share_state.complete_record(ack.offset)!
                    }
                    .release {
                        // 반환 - 다른 컨슈머가 가져갈 수 있도록
                        share_state.release_record(ack.offset)!
                    }
                    .reject {
                        // 거부 - DLQ로 이동 또는 재시도 한계 도달 시 제거
                        share_state.reject_record(ack.offset)!
                    }
                }
            }
            
            partitions << ShareAcknowledgeResponsePartition{
                partition: partition_req.partition
                error_code: .none
            }
        }
        
        responses << ShareAcknowledgeResponseTopic{
            topic: topic_req.name
            partitions: partitions
        }
    }
    
    return ShareAcknowledgeResponse{
        topics: responses
    }
}
```

### 7.5 Classic vs Consumer vs Share Protocol 비교

| 항목 | Classic | Consumer (KIP-848) | Share (KIP-932) |
|------|---------|-------------------|-----------------|
| **API** | JoinGroup, SyncGroup, Heartbeat | ConsumerGroupHeartbeat | ShareGroupHeartbeat, ShareFetch, ShareAcknowledge |
| **할당 계산** | Leader Consumer (Client) | Broker (Server) | 없음 (레코드 단위) |
| **리밸런스** | Stop-the-world | Incremental | 불필요 |
| **상태 저장** | __consumer_offsets | __consumer_offsets | __share_group_state |

---

## 8. Schema Registry 구현

### 8.1 Schema Registry 구조체

```v
// schema/registry.v
module schema

pub struct SchemaRegistry {
mut:
    cache       map[int]Schema           // ID → Schema
    subjects    map[string][]SchemaVersion  // Subject → Versions
    storage     StorageEngine
    config      RegistryConfig
}

pub struct Schema {
pub:
    id          int
    schema_type SchemaType  // AVRO, JSON, PROTOBUF
    schema      string
    references  []SchemaReference
}

pub struct SchemaVersion {
pub:
    version     int
    schema_id   int
    subject     string
    compatibility CompatibilityLevel
    created_at  time.Time
    deleted     bool
}
```

### 8.2 스키마 등록

```v
pub fn (mut r SchemaRegistry) register(subject string, schema_str string, 
                                        schema_type SchemaType) !int {
    // 1. 스키마 파싱 및 유효성 검사
    parsed := r.parse_schema(schema_type, schema_str)!
    
    // 2. 기존 스키마와 동일한지 확인
    if existing := r.find_by_schema(subject, schema_str) {
        return existing.id
    }
    
    // 3. 호환성 검사
    if r.subjects[subject].len > 0 {
        latest := r.get_latest_version(subject)!
        r.check_compatibility(latest.schema_id, parsed)!
    }
    
    // 4. 새 ID 할당 및 저장
    new_id := r.allocate_id()
    version := r.subjects[subject].len + 1
    
    // __schemas 토픽에 저장
    r.persist_schema(subject, version, new_id, schema_str, schema_type)!
    
    // 캐시 업데이트
    r.cache[new_id] = Schema{
        id: new_id
        schema_type: schema_type
        schema: schema_str
    }
    
    return new_id
}
```

### 8.3 스키마 검증

```v
pub fn (r &SchemaRegistry) validate(schema_id int, data []u8) !bool {
    schema := r.cache[schema_id] or {
        return error('Schema not found: ${schema_id}')
    }
    
    return match schema.schema_type {
        .avro => validate_avro(schema.schema, data)
        .json => validate_json_schema(schema.schema, data)
        .protobuf => validate_protobuf(schema.schema, data)
    }
}
```

---

## 9. Multi-Protocol Gateway (P2)

### 9.1 Protocol Adapter 아키텍처

```v
// protocol/adapter.v
module protocol

// Protocol Adapter Interface - 모든 프로토콜이 구현
pub interface ProtocolAdapter {
    name() string
    start(config AdapterConfig) !
    stop() !
    
    // Core Engine과의 연결
    set_message_handler(handler fn(msg Message) !)
}

// Core Engine - 모든 프로토콜이 공유
pub struct CoreEngine {
mut:
    storage         &StorageEngine
    group_manager   &GroupManager
    schema_registry &SchemaRegistry
}

// 메시지 추상화 - 프로토콜 독립적
pub struct Message {
pub:
    topic       string
    partition   int
    key         []u8
    value       []u8
    headers     map[string]string
    timestamp   i64
}
```

### 9.2 gRPC Streaming 구현

```protobuf
// proto/datacore.proto
syntax = "proto3";

package datacore;

service DataCoreService {
    // Produce - Unary RPC
    rpc Produce(ProduceRequest) returns (ProduceResponse);
    
    // Consume - Server Streaming (Push 방식)
    rpc Consume(ConsumeRequest) returns (stream Record);
    
    // Bidirectional Streaming
    rpc Stream(stream ClientMessage) returns (stream ServerMessage);
    
    // Admin APIs
    rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
    rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
}

message ConsumeRequest {
    string topic = 1;
    string group_id = 2;
    int64 offset = 3;
    bool auto_commit = 4;
}

message Record {
    string topic = 1;
    int32 partition = 2;
    int64 offset = 3;
    bytes key = 4;
    bytes value = 5;
    int64 timestamp = 6;
    map<string, string> headers = 7;
}

message ClientMessage {
    oneof payload {
        ProduceRequest produce = 1;
        AckRequest ack = 2;
        SubscribeRequest subscribe = 3;
    }
}

message ServerMessage {
    oneof payload {
        Record record = 1;
        ProduceResponse produce_response = 2;
        ErrorResponse error = 3;
    }
}
```

```v
// protocol/grpc/adapter.v
module grpc

pub struct GrpcAdapter {
mut:
    server      &GrpcServer
    core        &CoreEngine
    config      GrpcConfig
}

pub struct GrpcConfig {
pub:
    port                int = 9093
    max_message_size    int = 104857600  // 100MB
    keepalive_time_ms   int = 60000
    keepalive_timeout_ms int = 20000
}

// Server Streaming - Consumer에게 메시지 Push
pub fn (mut a GrpcAdapter) consume(req ConsumeRequest, stream &ServerStream) ! {
    // Consumer Group 등록
    consumer := a.core.group_manager.join(req.group_id, req.topic)!
    defer { a.core.group_manager.leave(consumer.id) }
    
    // 메시지 스트리밍 (Push)
    for {
        // 새 메시지 대기 및 전송
        records := a.core.storage.fetch(
            topic: req.topic
            partition: consumer.assigned_partition
            offset: consumer.current_offset
        )!
        
        for record in records {
            stream.send(record)!
            
            // Auto commit
            if req.auto_commit {
                consumer.commit(record.offset + 1)!
            }
        }
        
        // Back-pressure: 클라이언트 처리 속도에 맞춤
        if stream.is_flow_controlled() {
            time.sleep(10 * time.millisecond)
        }
    }
}
```

### 9.3 SSE (Server-Sent Events) 구현

```v
// protocol/sse/adapter.v
module sse

import vweb

pub struct SseAdapter {
    vweb.Context
mut:
    core    &CoreEngine
}

// GET /topics/{topic}/events?group_id=xxx&offset=0
['/topics/:topic/events']
pub fn (mut a SseAdapter) events(topic string) vweb.Result {
    group_id := a.query['group_id'] or { 'sse-' + generate_id() }
    offset := a.query['offset'].i64() or { -1 }
    
    // SSE 헤더 설정
    a.set_content_type_header('text/event-stream')
    a.add_header('Cache-Control', 'no-cache')
    a.add_header('Connection', 'keep-alive')
    
    // Consumer 등록
    consumer := a.core.group_manager.join(group_id, topic) or {
        return a.text('event: error\ndata: ${err}\n\n')
    }
    defer { a.core.group_manager.leave(consumer.id) }
    
    // 메시지 스트리밍
    for {
        records := a.core.storage.fetch(
            topic: topic
            partition: consumer.assigned_partition
            offset: consumer.current_offset
        ) or { break }
        
        for record in records {
            // SSE 형식으로 전송
            event := 'event: message\n'
            data := 'data: ${json.encode(record)}\n\n'
            a.write(event + data) or { break }
            a.flush()
        }
        
        // Heartbeat (연결 유지)
        if records.len == 0 {
            a.write('event: heartbeat\ndata: ${time.now().unix()}\n\n') or { break }
            a.flush()
            time.sleep(5 * time.second)
        }
    }
    
    return vweb.Result{}
}
```

### 9.4 WebSocket 구현

```v
// protocol/websocket/adapter.v
module websocket

import net.websocket

pub struct WebSocketAdapter {
mut:
    server  &websocket.Server
    core    &CoreEngine
    clients map[string]&WebSocketClient
}

pub struct WebSocketClient {
mut:
    conn            &websocket.Client
    subscriptions   []string  // 구독 중인 토픽
    group_id        string
}

pub fn (mut a WebSocketAdapter) on_message(mut client websocket.Client, msg websocket.Message) ! {
    request := json.decode(ClientRequest, msg.payload.str())!
    
    match request.type_ {
        'subscribe' {
            // 토픽 구독
            a.handle_subscribe(client, request.subscribe)!
        }
        'unsubscribe' {
            // 구독 해제
            a.handle_unsubscribe(client, request.unsubscribe)!
        }
        'produce' {
            // 메시지 발행
            result := a.core.produce(request.produce)!
            client.write_string(json.encode(result))!
        }
        'ack' {
            // 메시지 확인
            a.handle_ack(client, request.ack)!
        }
        else {
            client.write_string('{"error": "unknown request type"}')!
        }
    }
}

// 구독자에게 메시지 Push
fn (mut a WebSocketAdapter) push_messages(topic string, records []Record) {
    for _, client in a.clients {
        if topic in client.subscriptions {
            for record in records {
                client.conn.write_string(json.encode(ServerMessage{
                    type_: 'message'
                    record: record
                })) or { continue }
            }
        }
    }
}
```

### 9.5 Protocol 비교 및 선택 가이드

| 기준 | Kafka Protocol | gRPC Streaming | SSE | WebSocket |
|------|----------------|----------------|-----|-----------|
| **성능** | ★★★★★ | ★★★★☆ | ★★★☆☆ | ★★★★☆ |
| **브라우저 지원** | ❌ | ⚠️ (grpc-web) | ✅ | ✅ |
| **양방향 통신** | ✅ | ✅ | ❌ | ✅ |
| **Back-pressure** | ✅ | ✅ | ❌ | ⚠️ |
| **재연결 처리** | 클라이언트 | 클라이언트 | 자동 | 클라이언트 |
| **순서 보장** | ✅ | ✅ | ✅ | ⚠️ |
| **사용 사례** | 백엔드 시스템 | 마이크로서비스 | 대시보드 | 채팅, 실시간 앱 |

---

## 10. OpenTelemetry 구현

### 10.1 OTel Provider 초기화

```v
// observability/otel.v
module observability

import time

// OpenTelemetry Provider 초기화
pub struct OTelConfig {
pub:
    service_name    string
    service_version string
    metrics         MetricsConfig
    tracing         TracingConfig
    logging         LoggingConfig
    otlp            OTLPConfig
}

pub fn init_otel(config OTelConfig) !&OTelProvider {
    mut provider := &OTelProvider{
        config: config
    }
    
    // 메트릭 Exporter 초기화
    if config.metrics.enabled {
        provider.meter = init_meter(config)!
        provider.metrics = create_datacore_metrics(provider.meter)
    }
    
    // 트레이싱 Exporter 초기화
    if config.tracing.enabled {
        provider.tracer = init_tracer(config)!
    }
    
    // 로깅 초기화
    if config.logging.enabled {
        provider.logger = init_logger(config)!
    }
    
    return provider
}
```

### 10.2 메트릭 정의

```v
// observability/metrics.v - 메트릭 정의
pub struct DataCoreMetrics {
pub mut:
    messages_produced  Counter
    messages_consumed  Counter
    request_duration   Histogram
    partition_lag      Gauge
    active_connections Gauge
    storage_bytes      Gauge
}

pub fn create_datacore_metrics(meter &Meter) DataCoreMetrics {
    return DataCoreMetrics{
        messages_produced: meter.create_counter(
            name: 'datacore_messages_produced_total'
            description: 'Total number of messages produced'
        )
        request_duration: meter.create_histogram(
            name: 'datacore_request_duration_seconds'
            description: 'Request processing duration'
            boundaries: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        // ... 기타 메트릭
    }
}
```

### 10.3 분산 트레이싱

```v
// observability/tracing.v - 분산 트레이싱
pub fn trace_request(tracer &Tracer, api string, topic string) &Span {
    return tracer.start_span(
        name: api
        kind: .server
        attributes: [
            attr('messaging.system', 'kafka')
            attr('messaging.destination.name', topic)
            attr('messaging.operation', api)
        ]
    )
}
```

### 10.4 사용 예시

```v
// 사용 예시 - Produce 핸들러에서 관측성 적용
fn handle_produce(ctx &Context, req ProduceRequest) !ProduceResponse {
    // 트레이스 시작
    span := trace_request(ctx.tracer, 'produce', req.topic)
    defer { span.end() }
    
    start := time.now()
    
    // 비즈니스 로직 실행
    result := ctx.storage.append(req.topic, req.partition, req.records)!
    
    // 메트릭 기록
    ctx.metrics.request_duration.record(time.since(start).seconds(),
        attr('api', 'produce'), attr('topic', req.topic))
    ctx.metrics.messages_produced.add(req.records.len,
        attr('topic', req.topic))
    
    // 구조화된 로그
    ctx.logger.info('Produce completed',
        attr('topic', req.topic)
        attr('partition', req.partition)
        attr('offset', result.base_offset)
        attr('count', req.records.len))
    
    return ProduceResponse{ base_offset: result.base_offset }
}
```

---

## 11. Connection 관리 및 Non-Blocking I/O

### 11.1 Connection Manager 구현

```v
// broker/connection.v
module broker

import net
import time

pub struct ConnectionManager {
mut:
    connections     map[int]&ClientConnection  // fd -> connection
    config          ConnectionConfig
    metrics         &ConnectionMetrics
}

pub struct ConnectionConfig {
pub:
    max_connections         int = 10000
    max_connections_per_ip  int = 100
    idle_timeout_ms         int = 600000  // 10분
    request_timeout_ms      int = 30000
    max_request_size        int = 104857600  // 100MB
}

pub struct ClientConnection {
pub mut:
    fd              int
    remote_addr     net.Addr
    connected_at    time.Time
    last_active_at  time.Time
    request_count   u64
    bytes_received  u64
    bytes_sent      u64
    // Kafka 프로토콜 상태
    api_versions    []ApiVersionRange  // 클라이언트 지원 버전
    client_id       string
}

pub fn (mut cm ConnectionManager) accept(conn net.TcpConn) !&ClientConnection {
    // 연결 수 제한 확인
    if cm.connections.len >= cm.config.max_connections {
        conn.close()
        return error('max connections reached')
    }
    
    // IP당 연결 수 확인
    ip := conn.peer_addr()!.address
    ip_count := cm.count_connections_by_ip(ip)
    if ip_count >= cm.config.max_connections_per_ip {
        conn.close()
        return error('max connections per IP reached')
    }
    
    // 연결 등록
    client := &ClientConnection{
        fd: conn.sock.handle
        remote_addr: conn.peer_addr()!
        connected_at: time.now()
        last_active_at: time.now()
    }
    cm.connections[client.fd] = client
    cm.metrics.active_connections.inc()
    
    return client
}

pub fn (mut cm ConnectionManager) close(fd int) {
    if conn := cm.connections[fd] {
        cm.connections.delete(fd)
        cm.metrics.active_connections.dec()
        cm.metrics.total_connections.inc()
    }
}

// Idle 연결 정리 (주기적 실행)
pub fn (mut cm ConnectionManager) cleanup_idle_connections() {
    now := time.now()
    for fd, conn in cm.connections {
        idle_ms := (now - conn.last_active_at).milliseconds()
        if idle_ms > cm.config.idle_timeout_ms {
            cm.close(fd)
        }
    }
}
```

### 11.2 Non-Blocking I/O Server

```v
// broker/server.v
module broker

import net

pub struct BrokerServer {
mut:
    listener    net.TcpListener
    conn_mgr    &ConnectionManager
    handler     &RequestHandler
    running     bool
}

pub fn (mut s BrokerServer) start() ! {
    s.running = true
    
    // Idle connection cleanup 스케줄러
    spawn s.idle_cleanup_loop()
    
    // Accept loop
    for s.running {
        conn := s.listener.accept() or { continue }
        
        // 각 연결을 별도 coroutine에서 처리 (Non-Blocking)
        spawn s.handle_connection(conn)
    }
}

fn (mut s BrokerServer) handle_connection(conn net.TcpConn) {
    // Connection 등록
    client := s.conn_mgr.accept(conn) or {
        eprintln('Connection rejected: ${err}')
        return
    }
    defer { s.conn_mgr.close(client.fd) }
    
    // Request loop (Persistent Connection)
    for s.running {
        // Request 읽기 (타임아웃 적용)
        request := s.read_request(conn, client) or {
            break  // 연결 종료 또는 타임아웃
        }
        
        // 마지막 활동 시간 갱신
        client.last_active_at = time.now()
        client.request_count++
        
        // Request 처리
        response := s.handler.handle(request, client) or {
            s.send_error_response(conn, err)
            continue
        }
        
        // Response 전송
        s.send_response(conn, response)!
    }
}

fn (mut s BrokerServer) read_request(conn net.TcpConn, client &ClientConnection) !Request {
    // 1. Request Size 읽기 (4 bytes, Big Endian)
    mut size_buf := []u8{len: 4}
    conn.read(mut size_buf)!
    size := binary.big_endian_u32(size_buf)
    
    // Size 검증
    if size > s.conn_mgr.config.max_request_size {
        return error('request too large: ${size}')
    }
    
    // 2. Request Body 읽기
    mut body := []u8{len: int(size)}
    conn.read(mut body)!
    
    client.bytes_received += 4 + size
    
    // 3. Request 파싱
    return parse_request(body)
}
```

### 11.3 Request Pipelining 지원

```v
// broker/pipeline.v
module broker

// Kafka 클라이언트는 응답을 기다리지 않고 여러 요청을 보낼 수 있음
// 순서대로 처리하고 순서대로 응답해야 함

pub struct PipelinedConnection {
mut:
    pending_requests  []PendingRequest
    max_pending       int = 100  // 동시 대기 요청 수 제한
}

struct PendingRequest {
    correlation_id  i32
    api_key         i16
    received_at     time.Time
}

pub fn (mut p PipelinedConnection) enqueue(req Request) ! {
    if p.pending_requests.len >= p.max_pending {
        return error('too many pending requests')
    }
    
    p.pending_requests << PendingRequest{
        correlation_id: req.correlation_id
        api_key: req.api_key
        received_at: time.now()
    }
}

pub fn (mut p PipelinedConnection) dequeue(correlation_id i32) ! {
    // 순서대로 응답해야 하므로 첫 번째가 맞는지 확인
    if p.pending_requests.len == 0 {
        return error('no pending request')
    }
    if p.pending_requests[0].correlation_id != correlation_id {
        return error('out of order response')
    }
    p.pending_requests.delete(0)
}
```

### 11.4 Connection 관련 메트릭

```v
// observability/connection_metrics.v
module observability

pub struct ConnectionMetrics {
pub mut:
    // 연결 수
    active_connections      Gauge    // 현재 활성 연결
    total_connections       Counter  // 총 연결 수 (누적)
    rejected_connections    Counter  // 거부된 연결 수
    
    // 트래픽
    bytes_received_total    Counter
    bytes_sent_total        Counter
    requests_total          Counter
    
    // 지연시간
    request_queue_time      Histogram  // 큐 대기 시간
    connection_duration     Histogram  // 연결 유지 시간
}

pub fn create_connection_metrics(meter &Meter) ConnectionMetrics {
    return ConnectionMetrics{
        active_connections: meter.create_gauge(
            name: 'datacore_active_connections'
            description: 'Number of active client connections'
        )
        total_connections: meter.create_counter(
            name: 'datacore_connections_total'
            description: 'Total number of connections accepted'
        )
        rejected_connections: meter.create_counter(
            name: 'datacore_connections_rejected_total'
            description: 'Total number of connections rejected'
            labels: ['reason']  // max_connections, max_per_ip, etc.
        )
        // ... 기타 메트릭
    }
}
```

---

## 12. Zero-Copy 구현

### 12.1 sendfile() 래퍼

```v
// optimize/zero_copy.v
module optimize

import os

// Linux sendfile() 시스템 콜 래퍼
pub fn sendfile(out_fd int, in_fd int, offset i64, count int) !int {
    $if linux {
        return C.sendfile(out_fd, in_fd, &offset, count)
    } $else $if macos {
        // macOS는 sendfile 시그니처가 다름
        mut len := i64(count)
        result := C.sendfile(in_fd, out_fd, offset, &len, C.NULL, 0)
        if result == -1 {
            return error('sendfile failed')
        }
        return int(len)
    }
}
```

### 12.2 Zero-Copy Fetch 응답

```v
// Zero-Copy Fetch 응답
pub fn send_fetch_response_zero_copy(conn &net.TcpConn, 
                                      response_header []u8,
                                      file_path string,
                                      offset i64, 
                                      length int) ! {
    // 1. 응답 헤더 전송 (일반 복사)
    conn.write(response_header)!
    
    // 2. 메시지 데이터 Zero-Copy 전송
    file := os.open(file_path)!
    defer { file.close() }
    
    socket_fd := conn.sock.handle
    file_fd := file.fd
    
    mut sent := 0
    for sent < length {
        n := sendfile(socket_fd, file_fd, offset + sent, length - sent)!
        sent += n
    }
}
```

---

## 13. 개발 마일스톤

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
| **P2-1** | PostgreSQL Storage Plugin | 4주 | P2 |
| **P2-2** | Data Lake (Parquet, Iceberg) | 4주 | P2 |
| **P2-3** | gRPC Streaming Protocol | 3주 | P2 |
| **P2-4** | SSE/WebSocket Protocol | 2주 | P2 |
| **P3-1** | ACL/Quota Admin APIs (선택적) | 3주 | P3 |
| **총계** | | **53주 (약 13개월)** | |

---

## 14. 설정 파일 예시

```toml
# /etc/datacore/config.toml

[broker]
id = 1
listener = "0.0.0.0:9092"
advertised_listener = "kafka.example.com:9092"
max_connections = 10000
max_connections_per_ip = 100
idle_timeout_ms = 600000
request_timeout_ms = 30000
max_request_size = 104857600
max_pending_requests = 100

# Connection 관리
[broker.connection]
max_connections = 10000
max_connections_per_ip = 100
idle_timeout_ms = 600000        # 10분
request_timeout_ms = 30000
max_request_size = 104857600    # 100MB
max_pending_requests = 100      # Request Pipelining 제한

[storage]
plugin = "memory"  # memory | s3 | postgres

[storage.memory]
max_partition_size = "1GB"
flush_interval_ms = 1000

[storage.s3]
endpoint = "https://s3.amazonaws.com"
bucket = "datacore-data"
region = "us-east-1"
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"

# S3 PUT 비용 최적화 (v0.48.0)
min_flush_bytes = 4096           # 최소 flush 크기 (이하면 skip)
max_flush_skip_count = 40        # 최대 skip 횟수 (초과 시 강제 flush)
index_batch_size = 1             # 인덱스 배치 크기 (1=즉시, N=N개 세그먼트 누적 후 1회 PUT)
index_flush_interval_ms = 500    # 인덱스 강제 flush 간격 (ms)
sync_linger_ms = 0               # sync 경로 linger (0=비활성, 양수=ms 단위 대기)
use_server_side_copy = true      # 컴팩션 시 서버사이드 복사 시도

[storage.postgres]
host = "localhost"
port = 5432
database = "datacore"
user = "datacore"
password = "${POSTGRES_PASSWORD}"
pool_size = 10

[schema_registry]
enabled = true
compatibility_level = "BACKWARD"

[schema_registry.rest]
enabled = true
port = 8081

# Consumer Group Protocol 설정
[group]
# 지원 프로토콜: classic, consumer, share
supported_protocols = ["classic", "consumer", "share"]
# 기본 Server-side Assignor (consumer protocol)
default_assignor = "uniform"  # uniform | range | sticky
# Share Group 설정
share_record_lock_duration_ms = 30000
share_max_in_flight_records = 5000

# Multi-Protocol Gateway (P2)
[protocols.grpc]
enabled = false
port = 9093
max_message_size = 104857600
keepalive_time_ms = 60000

[protocols.sse]
enabled = false
port = 8080
path = "/events"
heartbeat_interval_ms = 5000

[protocols.websocket]
enabled = false
port = 8080
path = "/ws"
max_frame_size = 1048576

[observability]
service_name = "datacore"
service_version = "1.0.0"

[observability.metrics]
enabled = true
exporter = "prometheus"  # prometheus | otlp | stdout
prometheus_port = 9090

[observability.tracing]
enabled = true
exporter = "otlp"  # otlp | jaeger | zipkin | stdout
sampling_ratio = 1.0

[observability.logging]
enabled = true
level = "info"  # trace | debug | info | warn | error
format = "json"  # json | text
exporter = "stdout"  # stdout | otlp

[observability.otlp]
endpoint = "http://localhost:4317"
protocol = "grpc"  # grpc | http
headers = {}
timeout_ms = 10000
```

---

## 15. 기술 스택

### 15.1 언어 및 런타임

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| 언어 | V (vlang) | 최신 stable 버전 (0.5) |
| 네트워크 | V 내장 net 모듈 | TCP/TLS 지원 |
| 비동기 처리 | V coroutines | 고성능 I/O |

### 15.2 데이터베이스 드라이버

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| PostgreSQL | vlib/db/pg | 내장 라이브러리 |
| S3 클라이언트 | C FFI (aws-sdk-c) | 바인딩 필요 |

### 15.3 스키마 처리

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| Avro | 직접 구현 또는 C 바인딩 | 파서/검증기 |
| JSON Schema | 직접 구현 | 파서/검증기 |
| Protobuf | C FFI 또는 직접 구현 | 파서/검증기 |

---

## 16. 테스트 전략

### 16.1 테스트 유형

| 테스트 유형 | 범위 | 도구 |
|-------------|------|------|
| 단위 테스트 | 각 모듈 | V 내장 테스트 |
| 통합 테스트 | Storage Engine, API | Docker Compose |
| 호환성 테스트 | Kafka 클라이언트 | kafka-python, franz-go |
| 성능 테스트 | Throughput, Latency | 커스텀 벤치마크 |
| 스트레스 테스트 | 장시간 부하 | k6, locust |

### 16.2 테스트 커버리지 목표

- 단위 테스트: 80% 이상
- 통합 테스트: 주요 시나리오 100%
- 호환성 테스트: Kafka 클라이언트 v2.x, v3.x 지원


### 16.3 빌드

```bash
# 릴리스 빌드
v -prod -o datacore src/main.v

# 크로스 컴파일
v -prod -os linux -arch arm64 -o datacore-linux-arm64 src/main.v
```

### 16.4 Docker

```dockerfile
FROM alpine:3.18
COPY datacore /usr/local/bin/
EXPOSE 9092 8081 9090
ENTRYPOINT ["datacore", "broker", "start"]
```

### 16.5 Kubernetes

- Helm Chart 제공
- StatefulSet (브로커 ID 유지)
- ConfigMap (설정 파일)
- Secret (민감 정보)

---

## 17. V 언어 기술적 고려사항

### 17.1 라이브러리 가용성

| 기능 | V 생태계 지원 | 대안 |
|------|---------------|------|
| TCP/TLS | 내장 지원 | - |
| PostgreSQL | vlib/db/pg | - |
| S3 | 미지원 | C FFI (aws-sdk-c) |
| Avro | 미지원 | 직접 구현 |
| JSON Schema | 부분 지원 | 직접 구현 |
| Protobuf | 미지원 | C FFI 또는 직접 구현 |
| Parquet | 미지원 | C FFI (arrow) |

### 17.2 성능 최적화

- **제로 카피**: 메모리 복사 최소화
- **배치 처리**: 메시지 배치 단위 I/O
- **연결 풀링**: DB 연결 재사용
- **비동기 I/O**: V coroutines 활용

### 17.3 동시성

- V의 내장 채널(channel) 활용
- Mutex/RWLock으로 공유 상태 보호
- Atomic 연산으로 락-프리 카운터 구현

---

## 18. 보안 고려사항

### 18.1 통신 보안

- TLS 1.2+ 지원 (선택적)
- 인증서 기반 클라이언트 인증

### 18.2 SASL 인증 (P1)

SASL (Simple Authentication and Security Layer)을 통한 클라이언트 인증을 지원합니다.

```v
// service/auth/sasl.v
module auth

pub enum SaslMechanism {
    plain       // PLAIN - 사용자명/비밀번호 (TLS 필수 권장)
    scram_sha_256  // SCRAM-SHA-256 - Challenge-Response
    scram_sha_512  // SCRAM-SHA-512 - Challenge-Response
    oauthbearer    // OAUTHBEARER - OAuth 2.0 토큰
}

pub interface SaslAuthenticator {
    // 인증 메커니즘 반환
    mechanism() SaslMechanism
    
    // 초기 응답 처리 (client-first)
    authenticate(auth_bytes []u8) !AuthResult
    
    // Challenge-Response 단계 (SCRAM용)
    step(response []u8) !AuthResult
}

pub struct AuthResult {
pub:
    complete     bool        // 인증 완료 여부
    challenge    []u8        // 다음 challenge (SCRAM)
    principal    ?Principal  // 인증된 사용자 정보
    error_code   ?ErrorCode
}

pub struct Principal {
pub:
    name         string      // 사용자 이름
    principal_type string   // "User", "ServiceAccount"
}
```

**SASL 인증 흐름:**

```
1. Client → Broker: SaslHandshake(mechanism="PLAIN")
2. Broker → Client: SaslHandshake Response (mechanisms supported)
3. Client → Broker: SaslAuthenticate(auth_bytes)
4. Broker: 사용자 검증
5. Broker → Client: SaslAuthenticate Response (success/failure)
```

**사용자 저장소:**

```v
// 사용자 정보 인터페이스
pub interface UserStore {
    // 사용자 조회
    get_user(username string) !User
    
    // SCRAM용: salt와 iterations 조회
    get_scram_credentials(username string) !ScramCredentials
    
    // 사용자 생성/삭제 (Admin용)
    create_user(username string, password string, mechanism SaslMechanism) !
    delete_user(username string) !
}
```

### 18.3 ACL 권한 관리 (P1)

리소스별 세밀한 권한 제어를 위한 ACL (Access Control List)을 지원합니다.

```v
// service/auth/acl.v
module auth

pub enum ResourceType {
    topic
    group
    cluster
    transactional_id
    delegation_token
}

pub enum Operation {
    read
    write
    create
    delete
    alter
    describe
    cluster_action
    describe_configs
    alter_configs
    idempotent_write
    all
}

pub enum PermissionType {
    allow
    deny
}

pub enum PatternType {
    literal    // 정확히 일치
    prefixed   // 접두사 일치
    any        // 모든 리소스
}

// ACL 엔트리
pub struct AclEntry {
pub:
    resource_type   ResourceType
    resource_name   string
    pattern_type    PatternType
    principal       string      // "User:alice", "User:*"
    host           string      // "*" 또는 특정 IP
    operation      Operation
    permission     PermissionType
}

// ACL 관리 인터페이스
pub interface AclManager {
    // ACL CRUD
    create_acls(entries []AclEntry) ![]AclResult
    delete_acls(filters []AclFilter) ![]AclDeleteResult
    describe_acls(filter AclFilter) ![]AclEntry
    
    // 권한 검사
    authorize(principal Principal, resource Resource, operation Operation) !bool
}
```

**ACL 검사 흐름:**

```
1. 요청 수신 → Principal 식별 (SASL 인증 결과)
2. 리소스와 작업 결정 (예: Topic "orders", WRITE)
3. AclManager.authorize() 호출
4. 허용 시 요청 처리, 거부 시 AUTHORIZATION_FAILED 반환
```

**기본 ACL 정책:**

| 리소스 | 작업 | 필요 권한 |
|--------|------|----------|
| Topic | Produce | WRITE on Topic |
| Topic | Fetch | READ on Topic |
| Topic | CreateTopics | CREATE on Cluster |
| Topic | DeleteTopics | DELETE on Topic |
| Group | JoinGroup | READ on Group |
| Group | OffsetCommit | READ on Group |
| Cluster | DescribeCluster | DESCRIBE on Cluster |
| TransactionalId | InitProducerId | WRITE on TransactionalId |

### 18.4 Transaction 지원 (P1)

Exactly-once 시맨틱을 위한 트랜잭션을 지원합니다.

```v
// service/txn/coordinator.v
module txn

pub enum TransactionState {
    empty                    // 초기 상태
    ongoing                  // 진행 중
    prepare_commit           // 커밋 준비
    prepare_abort            // 롤백 준비
    complete_commit          // 커밋 완료
    complete_abort           // 롤백 완료
    dead                     // 만료됨
}

pub struct TransactionMetadata {
pub:
    transactional_id     string
    producer_id          i64
    producer_epoch       i16
    timeout_ms           i32
    state                TransactionState
    partitions           []TopicPartition    // 트랜잭션에 참여한 파티션
    pending_state        ?TransactionState
    last_update_time     i64
}

// Transaction Coordinator 인터페이스
pub interface TransactionCoordinator {
    // Producer ID 발급
    init_producer_id(transactional_id ?string, timeout_ms i32) !InitProducerIdResult
    
    // 트랜잭션에 파티션 추가
    add_partitions_to_txn(transactional_id string, producer_id i64, 
                          epoch i16, partitions []TopicPartition) ![]PartitionResult
    
    // 트랜잭션에 Consumer Group 오프셋 추가
    add_offsets_to_txn(transactional_id string, producer_id i64,
                       epoch i16, group_id string) !ErrorCode
    
    // 트랜잭션 오프셋 커밋
    txn_offset_commit(transactional_id string, group_id string,
                      producer_id i64, epoch i16, offsets []PartitionOffset) ![]OffsetCommitResult
    
    // 트랜잭션 종료
    end_txn(transactional_id string, producer_id i64, epoch i16, commit bool) !ErrorCode
}

pub struct InitProducerIdResult {
pub:
    producer_id    i64
    producer_epoch i16
    error_code     ErrorCode
}
```

**Transaction 상태 전이:**

```
                    ┌─────────────────┐
                    │  Unsubscribed   │
                    └────────┬────────┘
                             │ subscribe
                             ▼
                    ┌─────────────────┐
          ┌────────│   Subscribing   │────────┐
          │        └────────┬────────┘        │
          │                 │                 │
          ▼                 ▼                 ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │  Assigning  │  │   Stable    │  │ Reconciling │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │
          └────────────────┴────────────────┘
                           │ unsubscribe
                           ▼
                  ┌─────────────────┐
                  │ Unsubscribing   │
                  └─────────────────┘
```

### 7.3 Consumer Protocol 처리 (Server-side Assignment, KIP-848)

```v
// protocol/kafka/consumer_protocol.v
module kafka

// ConsumerGroupHeartbeat - 새로운 Protocol의 핵심 API
pub fn handle_consumer_group_heartbeat(version i16, req ConsumerGroupHeartbeatRequest,
                                        coordinator &GroupCoordinator) !ConsumerGroupHeartbeatResponse {
    group := coordinator.get_or_create_group(req.group_id)!
    
    // 1. 멤버 상태 업데이트
    member := group.update_member(
        member_id: req.member_id
        instance_id: req.instance_id
        subscribed_topics: req.subscribed_topic_names
        owned_partitions: req.topic_partitions
    )!
    
    // 2. Server-side Assignment 계산 (Broker가 직접 수행)
    if group.needs_rebalance() {
        // Incremental Assignment 계산
        new_assignment := coordinator.compute_assignment(
            group: group
            assignor: req.server_assignor or { 'uniform' }
        )!
        group.apply_assignment(new_assignment)!
    }
    
    // 3. 해당 멤버의 새 할당 정보 반환
    return ConsumerGroupHeartbeatResponse{
        member_id: member.id
        member_epoch: member.epoch
        heartbeat_interval_ms: group.heartbeat_interval_ms
        assignment: group.get_target_assignment(member.id)
    }
}

// Server-side Assignor Interface
pub interface ServerAssignor {
    name() string
    assign(members []MemberSubscription, topics []TopicMetadata) !map[string]Assignment
}

// Uniform Assignor (기본)
pub struct UniformAssignor {}

pub fn (a &UniformAssignor) name() string {
    return 'uniform'
}

pub fn (a &UniformAssignor) assign(members []MemberSubscription, 
                                    topics []TopicMetadata) !map[string]Assignment {
    mut assignments := map[string]Assignment{}
    
    // 모든 파티션 수집
    mut all_partitions := []TopicPartition{}
    for topic in topics {
        for p in 0 .. topic.partition_count {
            all_partitions << TopicPartition{topic.name, p}
        }
    }
    
    // 균등 분배
    for i, partition in all_partitions {
        member_idx := i % members.len
        member_id := members[member_idx].member_id
        if member_id !in assignments {
            assignments[member_id] = Assignment{}
        }
        assignments[member_id].partitions << partition
    }
    
    return assignments
}
```

### 7.4 Share Groups 처리 (Record-level Distribution, KIP-932)

```v
// protocol/kafka/share_group.v
module kafka

// Share Group 상태
pub struct ShareGroup {
pub mut:
    group_id            string
    members             map[string]ShareGroupMember
    subscribed_topics   []string
    // Share Partition 상태 (파티션별 in-flight 레코드 관리)
    share_partitions    map[TopicPartition]SharePartitionState
}

pub struct SharePartitionState {
pub mut:
    // In-flight 레코드 추적
    in_flight_records   map[i64]InFlightRecord  // offset -> record state
    start_offset        i64
    end_offset          i64
    // 설정
    record_lock_duration_ms  int = 30000
    max_in_flight_records    int = 5000
}

pub struct InFlightRecord {
pub:
    offset          i64
    acquired_by     string  // member_id
    acquired_at     time.Time
    delivery_count  int
}

pub enum AcknowledgeType {
    accept   // 처리 완료 - 다시 전달 안함
    release  // 반환 - 다른 컨슈머에게 전달 가능
    reject   // 거부 - Dead Letter Queue로 이동
}

// ShareGroupHeartbeat 처리
pub fn handle_share_group_heartbeat(version i16, req ShareGroupHeartbeatRequest,
                                     coordinator &ShareGroupCoordinator) !ShareGroupHeartbeatResponse {
    group := coordinator.get_or_create_share_group(req.group_id)!
    
    // 멤버 등록/갱신
    member := group.update_member(req.member_id, req.subscribed_topic_names)!
    
    // Share Group은 파티션 할당 없음 - 모든 멤버가 모든 파티션 접근 가능
    return ShareGroupHeartbeatResponse{
        member_id: member.id
        member_epoch: member.epoch
        heartbeat_interval_ms: group.heartbeat_interval_ms
        // assignment는 없음 - ShareFetch에서 동적으로 레코드 획득
    }
}

// ShareFetch 처리 - 레코드 단위 분배
pub fn handle_share_fetch(version i16, req ShareFetchRequest,
                           coordinator &ShareGroupCoordinator) !ShareFetchResponse {
    group := coordinator.get_share_group(req.group_id)!
    
    mut responses := []ShareFetchResponseTopic{}
    
    for topic_req in req.topics {
        mut partitions := []ShareFetchResponsePartition{}
        
        for partition_req in topic_req.partitions {
            tp := TopicPartition{topic_req.name, partition_req.partition}
            share_state := group.share_partitions[tp]
            
            // 사용 가능한 레코드 획득 (lock)
            records := share_state.acquire_records(
                member_id: req.member_id
                max_records: partition_req.max_records
                max_bytes: req.max_bytes
            )!
            
            partitions << ShareFetchResponsePartition{
                partition: partition_req.partition
                records: records
                acquired_records: records.map(|r| AcquiredRecord{
                    offset: r.offset
                    delivery_count: share_state.in_flight_records[r.offset].delivery_count
                })
            }
        }
        
        responses << ShareFetchResponseTopic{
            topic: topic_req.name
            partitions: partitions
        }
    }
    
    return ShareFetchResponse{
        topics: responses
    }
}

// ShareAcknowledge 처리 - 레코드 Ack/Nack
pub fn handle_share_acknowledge(version i16, req ShareAcknowledgeRequest,
                                 coordinator &ShareGroupCoordinator) !ShareAcknowledgeResponse {
    group := coordinator.get_share_group(req.group_id)!
    
    mut responses := []ShareAcknowledgeResponseTopic{}
    
    for topic_req in req.topics {
        mut partitions := []ShareAcknowledgeResponsePartition{}
        
        for partition_req in topic_req.partitions {
            tp := TopicPartition{topic_req.name, partition_req.partition}
            share_state := group.share_partitions[tp]
            
            // 각 레코드의 acknowledge 처리
            for ack in partition_req.acknowledgements {
                match ack.acknowledge_type {
                    .accept {
                        // 처리 완료 - in-flight에서 제거
                        share_state.complete_record(ack.offset)!
                    }
                    .release {
                        // 반환 - 다른 컨슈머가 가져갈 수 있도록
                        share_state.release_record(ack.offset)!
                    }
                    .reject {
                        // 거부 - DLQ로 이동 또는 재시도 한계 도달 시 제거
                        share_state.reject_record(ack.offset)!
                    }
                }
            }
            
            partitions << ShareAcknowledgeResponsePartition{
                partition: partition_req.partition
                error_code: .none
            }
        }
        
        responses << ShareAcknowledgeResponseTopic{
            topic: topic_req.name
            partitions: partitions
        }
    }
    
    return ShareAcknowledgeResponse{
        topics: responses
    }
}
```

### 7.5 Classic vs Consumer vs Share Protocol 비교

| 항목 | Classic | Consumer (KIP-848) | Share (KIP-932) |
|------|---------|-------------------|-----------------|
| **API** | JoinGroup, SyncGroup, Heartbeat | ConsumerGroupHeartbeat | ShareGroupHeartbeat, ShareFetch, ShareAcknowledge |
| **할당 계산** | Leader Consumer (Client) | Broker (Server) | 없음 (레코드 단위) |
| **리밸런스** | Stop-the-world | Incremental | 불필요 |
| **상태 저장** | __consumer_offsets | __consumer_offsets | __share_group_state |

---

## 8. Schema Registry 구현

### 8.1 Schema Registry 구조체

```v
// schema/registry.v
module schema

pub struct SchemaRegistry {
mut:
    cache       map[int]Schema           // ID → Schema
    subjects    map[string][]SchemaVersion  // Subject → Versions
    storage     StorageEngine
    config      RegistryConfig
}

pub struct Schema {
pub:
    id          int
    schema_type SchemaType  // AVRO, JSON, PROTOBUF
    schema      string
    references  []SchemaReference
}

pub struct SchemaVersion {
pub:
    version     int
    schema_id   int
    subject     string
    compatibility CompatibilityLevel
    created_at  time.Time
    deleted     bool
}
```

### 8.2 스키마 등록

```v
pub fn (mut r SchemaRegistry) register(subject string, schema_str string, 
                                        schema_type SchemaType) !int {
    // 1. 스키마 파싱 및 유효성 검사
    parsed := r.parse_schema(schema_type, schema_str)!
    
    // 2. 기존 스키마와 동일한지 확인
    if existing := r.find_by_schema(subject, schema_str) {
        return existing.id
    }
    
    // 3. 호환성 검사
    if r.subjects[subject].len > 0 {
        latest := r.get_latest_version(subject)!
        r.check_compatibility(latest.schema_id, parsed)!
    }
    
    // 4. 새 ID 할당 및 저장
    new_id := r.allocate_id()
    version := r.subjects[subject].len + 1
    
    // __schemas 토픽에 저장
    r.persist_schema(subject, version, new_id, schema_str, schema_type)!
    
    // 캐시 업데이트
    r.cache[new_id] = Schema{
        id: new_id
        schema_type: schema_type
        schema: schema_str
    }
    
    return new_id
}
```

### 8.3 스키마 검증

```v
pub fn (r &SchemaRegistry) validate(schema_id int, data []u8) !bool {
    schema := r.cache[schema_id] or {
        return error('Schema not found: ${schema_id}')
    }
    
    return match schema.schema_type {
        .avro => validate_avro(schema.schema, data)
        .json => validate_json_schema(schema.schema, data)
        .protobuf => validate_protobuf(schema.schema, data)
    }
}
```

---

## 9. Multi-Protocol Gateway (P2)

### 9.1 Protocol Adapter 아키텍처

```v
// protocol/adapter.v
module protocol

// Protocol Adapter Interface - 모든 프로토콜이 구현
pub interface ProtocolAdapter {
    name() string
    start(config AdapterConfig) !
    stop() !
    
    // Core Engine과의 연결
    set_message_handler(handler fn(msg Message) !)
}

// Core Engine - 모든 프로토콜이 공유
pub struct CoreEngine {
mut:
    storage         &StorageEngine
    group_manager   &GroupManager
    schema_registry &SchemaRegistry
}

// 메시지 추상화 - 프로토콜 독립적
pub struct Message {
pub:
    topic       string
    partition   int
    key         []u8
    value       []u8
    headers     map[string]string
    timestamp   i64
}
```

### 9.2 gRPC Streaming 구현

```protobuf
// proto/datacore.proto
syntax = "proto3";

package datacore;

service DataCoreService {
    // Produce - Unary RPC
    rpc Produce(ProduceRequest) returns (ProduceResponse);
    
    // Consume - Server Streaming (Push 방식)
    rpc Consume(ConsumeRequest) returns (stream Record);
    
    // Bidirectional Streaming
    rpc Stream(stream ClientMessage) returns (stream ServerMessage);
    
    // Admin APIs
    rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
    rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse);
}

message ConsumeRequest {
    string topic = 1;
    string group_id = 2;
    int64 offset = 3;
    bool auto_commit = 4;
}

message Record {
    string topic = 1;
    int32 partition = 2;
    int64 offset = 3;
    bytes key = 4;
    bytes value = 5;
    int64 timestamp = 6;
    map<string, string> headers = 7;
}

message ClientMessage {
    oneof payload {
        ProduceRequest produce = 1;
        AckRequest ack = 2;
        SubscribeRequest subscribe = 3;
    }
}

message ServerMessage {
    oneof payload {
        Record record = 1;
        ProduceResponse produce_response = 2;
        ErrorResponse error = 3;
    }
}
```

```v
// protocol/grpc/adapter.v
module grpc

pub struct GrpcAdapter {
mut:
    server      &GrpcServer
    core        &CoreEngine
    config      GrpcConfig
}

pub struct GrpcConfig {
pub:
    port                int = 9093
    max_message_size    int = 104857600  // 100MB
    keepalive_time_ms   int = 60000
    keepalive_timeout_ms int = 20000
}

// Server Streaming - Consumer에게 메시지 Push
pub fn (mut a GrpcAdapter) consume(req ConsumeRequest, stream &ServerStream) ! {
    // Consumer Group 등록
    consumer := a.core.group_manager.join(req.group_id, req.topic)!
    defer { a.core.group_manager.leave(consumer.id) }
    
    // 메시지 스트리밍 (Push)
    for {
        // 새 메시지 대기 및 전송
        records := a.core.storage.fetch(
            topic: req.topic
            partition: consumer.assigned_partition
            offset: consumer.current_offset
        )!
        
        for record in records {
            stream.send(record)!
            
            // Auto commit
            if req.auto_commit {
                consumer.commit(record.offset + 1)!
            }
        }
        
        // Back-pressure: 클라이언트 처리 속도에 맞춤
        if stream.is_flow_controlled() {
            time.sleep(10 * time.millisecond)
        }
    }
}
```

### 9.3 SSE (Server-Sent Events) 구현

```v
// protocol/sse/adapter.v
module sse

import vweb

pub struct SseAdapter {
    vweb.Context
mut:
    core    &CoreEngine
}

// GET /topics/{topic}/events?group_id=xxx&offset=0
['/topics/:topic/events']
pub fn (mut a SseAdapter) events(topic string) vweb.Result {
    group_id := a.query['group_id'] or { 'sse-' + generate_id() }
    offset := a.query['offset'].i64() or { -1 }
    
    // SSE 헤더 설정
    a.set_content_type_header('text/event-stream')
    a.add_header('Cache-Control', 'no-cache')
    a.add_header('Connection', 'keep-alive')
    
    // Consumer 등록
    consumer := a.core.group_manager.join(group_id, topic) or {
        return a.text('event: error\ndata: ${err}\n\n')
    }
    defer { a.core.group_manager.leave(consumer.id) }
    
    // 메시지 스트리밍
    for {
        records := a.core.storage.fetch(
            topic: topic
            partition: consumer.assigned_partition
            offset: consumer.current_offset
        ) or { break }
        
        for record in records {
            // SSE 형식으로 전송
            event := 'event: message\n'
            data := 'data: ${json.encode(record)}\n\n'
            a.write(event + data) or { break }
            a.flush()
        }
        
        // Heartbeat (연결 유지)
        if records.len == 0 {
            a.write('event: heartbeat\ndata: ${time.now().unix()}\n\n') or { break }
            a.flush()
            time.sleep(5 * time.second)
        }
    }
    
    return vweb.Result{}
}
```

### 9.4 WebSocket 구현

```v
// protocol/websocket/adapter.v
module websocket

import net.websocket

pub struct WebSocketAdapter {
mut:
    server  &websocket.Server
    core    &CoreEngine
    clients map[string]&WebSocketClient
}

pub struct WebSocketClient {
mut:
    conn            &websocket.Client
    subscriptions   []string  // 구독 중인 토픽
    group_id        string
}

pub fn (mut a WebSocketAdapter) on_message(mut client websocket.Client, msg websocket.Message) ! {
    request := json.decode(ClientMessage, msg.payload.str())!
    
    match request.type_ {
        'subscribe' {
            // 토픽 구독
            a.handle_subscribe(client, request.subscribe)!
        }
        'unsubscribe' {
            // 구독 해제
            a.handle_unsubscribe(client, request.unsubscribe)!
        }
        'produce' {
            // 메시지 발행
            result := a.core.produce(request.produce)!
            client.write_string(json.encode(result))!
        }
        'ack' {
            // 메시지 확인
            a.handle_ack(client, request.ack)!
        }
        else {
            client.write_string('{"error": "unknown request type"}')!
        }
    }
}

// 구독자에게 메시지 Push
fn (mut a WebSocketAdapter) push_messages(topic string, records []Record) {
    for _, client in a.clients {
        if topic in client.subscriptions {
            for record in records {
                client.conn.write_string(json.encode(ServerMessage{
                    type_: 'message'
                    record: record
                })) or { continue }
            }
        }
    }
}
```

### 9.5 Protocol 비교 및 선택 가이드

| 기준 | Kafka Protocol | gRPC Streaming | SSE | WebSocket |
|------|----------------|----------------|-----|-----------|
| **성능** | ★★★★★ | ★★★★☆ | ★★★☆☆ | ★★★★☆ |
| **브라우저 지원** | ❌ | ⚠️ (grpc-web) | ✅ | ✅ |
| **양방향 통신** | ✅ | ✅ | ❌ | ✅ |
| **Back-pressure** | ✅ | ✅ | ❌ | ⚠️ |
| **재연결 처리** | 클라이언트 | 클라이언트 | 자동 | 클라이언트 |
| **순서 보장** | ✅ | ✅ | ✅ | ⚠️ |
| **사용 사례** | 백엔드 시스템 | 마이크로서비스 | 대시보드 | 채팅, 실시간 앱 |

---

## 10. OpenTelemetry 구현

### 10.1 OTel Provider 초기화

```v
// observability/otel.v
module observability

import time

// OpenTelemetry Provider 초기화
pub struct OTelConfig {
pub:
    service_name    string
    service_version string
    metrics         MetricsConfig
    tracing         TracingConfig
    logging         LoggingConfig
    otlp            OTLPConfig
}

pub fn init_otel(config OTelConfig) !&OTelProvider {
    mut provider := &OTelProvider{
        config: config
    }
    
    // 메트릭 Exporter 초기화
    if config.metrics.enabled {
        provider.meter = init_meter(config)!
        provider.metrics = create_datacore_metrics(provider.meter)
    }
    
    // 트레이싱 Exporter 초기화
    if config.tracing.enabled {
        provider.tracer = init_tracer(config)!
    }
    
    // 로깅 초기화
    if config.logging.enabled {
        provider.logger = init_logger(config)!
    }
    
    return provider
}
```

### 10.2 메트릭 정의

```v
// observability/metrics.v - 메트릭 정의
pub struct DataCoreMetrics {
pub mut:
    messages_produced  Counter
    messages_consumed  Counter
    request_duration   Histogram
    partition_lag      Gauge
    active_connections Gauge
    storage_bytes      Gauge
}

pub fn create_datacore_metrics(meter &Meter) DataCoreMetrics {
    return DataCoreMetrics{
        messages_produced: meter.create_counter(
            name: 'datacore_messages_produced_total'
            description: 'Total number of messages produced'
        )
        request_duration: meter.create_histogram(
            name: 'datacore_request_duration_seconds'
            description: 'Request processing duration'
            boundaries: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        // ... 기타 메트릭
    }
}
```

### 10.3 분산 트레이싱

```v
// observability/tracing.v - 분산 트레이싱
pub fn trace_request(tracer &Tracer, api string, topic string) &Span {
    return tracer.start_span(
        name: api
        kind: .server
        attributes: [
            attr('messaging.system', 'kafka')
            attr('messaging.destination.name', topic)
            attr('messaging.operation', api)
        ]
    )
}
```

### 10.4 사용 예시

```v
// 사용 예시 - Produce 핸들러에서 관측성 적용
fn handle_produce(ctx &Context, req ProduceRequest) !ProduceResponse {
    // 트레이스 시작
    span := trace_request(ctx.tracer, 'produce', req.topic)
    defer { span.end() }
    
    start := time.now()
    
    // 비즈니스 로직 실행
    result := ctx.storage.append(req.topic, req.partition, req.records)!
    
    // 메트릭 기록
    ctx.metrics.request_duration.record(time.since(start).seconds(),
        attr('api', 'produce'), attr('topic', req.topic))
    ctx.metrics.messages_produced.add(req.records.len,
        attr('topic', req.topic))
    
    // 구조화된 로그
    ctx.logger.info('Produce completed',
        attr('topic', req.topic)
        attr('partition', req.partition)
        attr('offset', result.base_offset)
        attr('count', req.records.len))
    
    return ProduceResponse{ base_offset: result.base_offset }
}
```

---

## 11. Connection 관리 및 Non-Blocking I/O

### 11.1 Connection Manager 구현

```v
// broker/connection.v
module broker

import net
import time

pub struct ConnectionManager {
mut:
    connections     map[int]&ClientConnection  // fd -> connection
    config          ConnectionConfig
    metrics         &ConnectionMetrics
}

pub struct ConnectionConfig {
pub:
    max_connections         int = 10000
    max_connections_per_ip  int = 100
    idle_timeout_ms         int = 600000  // 10분
    request_timeout_ms      int = 30000
    max_request_size        int = 104857600  // 100MB
}

pub struct ClientConnection {
pub mut:
    fd              int
    remote_addr     net.Addr
    connected_at    time.Time
    last_active_at  time.Time
    request_count   u64
    bytes_received  u64
    bytes_sent      u64
    // Kafka 프로토콜 상태
    api_versions    []ApiVersionRange  // 클라이언트 지원 버전
    client_id       string
}

pub fn (mut cm ConnectionManager) accept(conn net.TcpConn) !&ClientConnection {
    // 연결 수 제한 확인
    if cm.connections.len >= cm.config.max_connections {
        conn.close()
        return error('max connections reached')
    }
    
    // IP당 연결 수 확인
    ip := conn.peer_addr()!.address
    ip_count := cm.count_connections_by_ip(ip)
    if ip_count >= cm.config.max_connections_per_ip {
        conn.close()
        return error('max connections per IP reached')
    }
    
    // 연결 등록
    client := &ClientConnection{
        fd: conn.sock.handle
        remote_addr: conn.peer_addr()!
        connected_at: time.now()
        last_active_at: time.now()
    }
    cm.connections[client.fd] = client
    cm.metrics.active_connections.inc()
    
    return client
}

pub fn (mut cm ConnectionManager) close(fd int) {
    if conn := cm.connections[fd] {
        cm.connections.delete(fd)
        cm.metrics.active_connections.dec()
        cm.metrics.total_connections.inc()
    }
}

// Idle 연결 정리 (주기적 실행)
pub fn (mut cm ConnectionManager) cleanup_idle_connections() {
    now := time.now()
    for fd, conn in cm.connections {
        idle_ms := (now - conn.last_active_at).milliseconds()
        if idle_ms > cm.config.idle_timeout_ms {
            cm.close(fd)
        }
    }
}
```

### 11.2 Non-Blocking I/O Server

```v
// broker/server.v
module broker

import net

pub struct BrokerServer {
mut:
    listener    net.TcpListener
    conn_mgr    &ConnectionManager
    handler     &RequestHandler
    running     bool
}

pub fn (mut s BrokerServer) start() ! {
    s.running = true
    
    // Idle connection cleanup 스케줄러
    spawn s.idle_cleanup_loop()
    
    // Accept loop
    for s.running {
        conn := s.listener.accept() or { continue }
        
        // 각 연결을 별도 coroutine에서 처리 (Non-Blocking)
        spawn s.handle_connection(conn)
    }
}

fn (mut s BrokerServer) handle_connection(conn net.TcpConn) {
    // Connection 등록
    client := s.conn_mgr.accept(conn) or {
        eprintln('Connection rejected: ${err}')
        return
    }
    defer { s.conn_mgr.close(client.fd) }
    
    // Request loop (Persistent Connection)
    for s.running {
        // Request 읽기 (타임아웃 적용)
        request := s.read_request(conn, client) or {
            break  // 연결 종료 또는 타임아웃
        }
        
        // 마지막 활동 시간 갱신
        client.last_active_at = time.now()
        client.request_count++
        
        // Request 처리
        response := s.handler.handle(request, client) or {
            s.send_error_response(conn, err)
            continue
        }
        
        // Response 전송
        s.send_response(conn, response)!
    }
}

fn (mut s BrokerServer) read_request(conn net.TcpConn, client &ClientConnection) !Request {
    // 1. Request Size 읽기 (4 bytes, Big Endian)
    mut size_buf := []u8{len: 4}
    conn.read(mut size_buf)!
    size := binary.big_endian_u32(size_buf)
    
    // Size 검증
    if size > s.conn_mgr.config.max_request_size {
        return error('request too large: ${size}')
    }
    
    // 2. Request Body 읽기
    mut body := []u8{len: int(size)}
    conn.read(mut body)!
    
    client.bytes_received += 4 + size
    
    // 3. Request 파싱
    return parse_request(body)
}
```

### 11.3 Request Pipelining 지원

```v
// broker/pipeline.v
module broker

// Kafka 클라이언트는 응답을 기다리지 않고 여러 요청을 보낼 수 있음
// 순서대로 처리하고 순서대로 응답해야 함

pub struct PipelinedConnection {
mut:
    pending_requests  []PendingRequest
    max_pending       int = 100  // 동시 대기 요청 수 제한
}

struct PendingRequest {
    correlation_id  i32
    api_key         i16
    received_at     time.Time
}

pub fn (mut p PipelinedConnection) enqueue(req Request) ! {
    if p.pending_requests.len >= p.max_pending {
        return error('too many pending requests')
    }
    
    p.pending_requests << PendingRequest{
        correlation_id: req.correlation_id
        api_key: req.api_key
        received_at: time.now()
    }
}

pub fn (mut p PipelinedConnection) dequeue(correlation_id i32) ! {
    // 순서대로 응답해야 하므로 첫 번째가 맞는지 확인
    if p.pending_requests.len == 0 {
        return error('no pending request')
    }
    if p.pending_requests[0].correlation_id != correlation_id {
        return error('out of order response')
    }
    p.pending_requests.delete(0)
}
```

### 11.4 Connection 관련 메트릭

```v
// observability/connection_metrics.v
module observability

pub struct ConnectionMetrics {
pub mut:
    // 연결 수
    active_connections      Gauge    // 현재 활성 연결
    total_connections       Counter  // 총 연결 수 (누적)
    rejected_connections    Counter  // 거부된 연결 수
    
    // 트래픽
    bytes_received_total    Counter
    bytes_sent_total        Counter
    requests_total          Counter
    
    // 지연시간
    request_queue_time      Histogram  // 큐 대기 시간
    connection_duration     Histogram  // 연결 유지 시간
}

pub fn create_connection_metrics(meter &Meter) ConnectionMetrics {
    return ConnectionMetrics{
        active_connections: meter.create_gauge(
            name: 'datacore_active_connections'
            description: 'Number of active client connections'
        )
        total_connections: meter.create_counter(
            name: 'datacore_connections_total'
            description: 'Total number of connections accepted'
        )
        rejected_connections: meter.create_counter(
            name: 'datacore_connections_rejected_total'
            description: 'Total number of connections rejected'
            labels: ['reason']  // max_connections, max_per_ip, etc.
        )
        // ... 기타 메트릭
    }
}
```

---

## 12. Zero-Copy 구현

### 12.1 sendfile() 래퍼

```v
// optimize/zero_copy.v
module optimize

import os

// Linux sendfile() 시스템 콜 래퍼
pub fn sendfile(out_fd int, in_fd int, offset i64, count int) !int {
    $if linux {
        return C.sendfile(out_fd, in_fd, &offset, count)
    } $else $if macos {
        // macOS는 sendfile 시그니처가 다름
        mut len := i64(count)
        result := C.sendfile(in_fd, out_fd, offset, &len, C.NULL, 0)
        if result == -1 {
            return error('sendfile failed')
        }
        return int(len)
    }
}
```

### 12.2 Zero-Copy Fetch 응답

```v
// Zero-Copy Fetch 응답
pub fn send_fetch_response_zero_copy(conn &net.TcpConn, 
                                      response_header []u8,
                                      file_path string,
                                      offset i64, 
                                      length int) ! {
    // 1. 응답 헤더 전송 (일반 복사)
    conn.write(response_header)!
    
    // 2. 메시지 데이터 Zero-Copy 전송
    file := os.open(file_path)!
    defer { file.close() }
    
    socket_fd := conn.sock.handle
    file_fd := file.fd
    
    mut sent := 0
    for sent < length {
        n := sendfile(socket_fd, file_fd, offset + sent, length - sent)!
        sent += n
    }
}
```

---

## 13. 개발 마일스톤

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
| **P2-1** | PostgreSQL Storage Plugin | 4주 | P2 |
| **P2-2** | Data Lake (Parquet, Iceberg) | 4주 | P2 |
| **P2-3** | gRPC Streaming Protocol | 3주 | P2 |
| **P2-4** | SSE/WebSocket Protocol | 2주 | P2 |
| **P3-1** | ACL/Quota Admin APIs (선택적) | 3주 | P3 |
| **총계** | | **53주 (약 13개월)** | |

---

## 14. 설정 파일 예시

```toml
# /etc/datacore/config.toml

[broker]
id = 1
listener = "0.0.0.0:9092"
advertised_listener = "kafka.example.com:9092"
max_connections = 10000
max_connections_per_ip = 100
idle_timeout_ms = 600000
request_timeout_ms = 30000
max_request_size = 104857600
max_pending_requests = 100

# Connection 관리
[broker.connection]
max_connections = 10000
max_connections_per_ip = 100
idle_timeout_ms = 600000        # 10분
request_timeout_ms = 30000
max_request_size = 104857600    # 100MB
max_pending_requests = 100      # Request Pipelining 제한

[storage]
plugin = "memory"  # memory | s3 | postgres

[storage.memory]
max_partition_size = "1GB"
flush_interval_ms = 1000

[storage.s3]
endpoint = "https://s3.amazonaws.com"
bucket = "datacore-data"
region = "us-east-1"
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"

# S3 PUT 비용 최적화 (v0.48.0)
min_flush_bytes = 4096           # 최소 flush 크기 (이하면 skip)
max_flush_skip_count = 40        # 최대 skip 횟수 (초과 시 강제 flush)
index_batch_size = 1             # 인덱스 배치 크기 (1=즉시, N=N개 세그먼트 누적 후 1회 PUT)
index_flush_interval_ms = 500    # 인덱스 강제 flush 간격 (ms)
sync_linger_ms = 0               # sync 경로 linger (0=비활성, 양수=ms 단위 대기)
use_server_side_copy = true      # 컴팩션 시 서버사이드 복사 시도

[storage.postgres]
host = "localhost"
port = 5432
database = "datacore"
user = "datacore"
password = "${POSTGRES_PASSWORD}"
pool_size = 10

[schema_registry]
enabled = true
compatibility_level = "BACKWARD"

[schema_registry.rest]
enabled = true
port = 8081

# Consumer Group Protocol 설정
[group]
# 지원 프로토콜: classic, consumer, share
supported_protocols = ["classic", "consumer", "share"]
# 기본 Server-side Assignor (consumer protocol)
default_assignor = "uniform"  # uniform | range | sticky
# Share Group 설정
share_record_lock_duration_ms = 30000
share_max_in_flight_records = 5000

# Multi-Protocol Gateway (P2)
[protocols.grpc]
enabled = false
port = 9093
max_message_size = 104857600
keepalive_time_ms = 60000

[protocols.sse]
enabled = false
port = 8080
path = "/events"
heartbeat_interval_ms = 5000

[protocols.websocket]
enabled = false
port = 8080
path = "/ws"
max_frame_size = 1048576

[observability]
service_name = "datacore"
service_version = "1.0.0"

[observability.metrics]
enabled = true
exporter = "prometheus"  # prometheus | otlp | stdout
prometheus_port = 9090

[observability.tracing]
enabled = true
exporter = "otlp"  # otlp | jaeger | zipkin | stdout
sampling_ratio = 1.0

[observability.logging]
enabled = true
level = "info"  # trace | debug | info | warn | error
format = "json"  # json | text
exporter = "stdout"  # stdout | otlp

[observability.otlp]
endpoint = "http://localhost:4317"
protocol = "grpc"  # grpc | http
headers = {}
timeout_ms = 10000
```

---

## 15. 기술 스택

### 15.1 언어 및 런타임

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| 언어 | V (vlang) | 최신 stable 버전 (0.5) |
| 네트워크 | V 내장 net 모듈 | TCP/TLS 지원 |
| 비동기 처리 | V coroutines | 고성능 I/O |

### 15.2 데이터베이스 드라이버

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| PostgreSQL | vlib/db/pg | 내장 라이브러리 |
| S3 클라이언트 | C FFI (aws-sdk-c) | 바인딩 필요 |

### 15.3 스키마 처리

| 구성요소 | 기술 | 비고 |
|----------|------|------|
| Avro | 직접 구현 또는 C 바인딩 | 파서/검증기 |
| JSON Schema | 직접 구현 | 파서/검증기 |
| Protobuf | C FFI 또는 직접 구현 | 파서/검증기 |

---

## 16. 테스트 전략

### 16.1 테스트 유형

| 테스트 유형 | 범위 | 도구 |
|-------------|------|------|
| 단위 테스트 | 각 모듈 | V 내장 테스트 |
| 통합 테스트 | Storage Engine, API | Docker Compose |
| 호환성 테스트 | Kafka 클라이언트 | kafka-python, franz-go |
| 성능 테스트 | Throughput, Latency | 커스텀 벤치마크 |
| 스트레스 테스트 | 장시간 부하 | k6, locust |

### 16.2 테스트 커버리지 목표

- 단위 테스트: 80% 이상
- 통합 테스트: 주요 시나리오 100%
- 호환성 테스트: Kafka 클라이언트 v2.x, v3.x 지원


### 16.3 빌드

```bash
# 릴리스 빌드
v -prod -o datacore src/main.v

# 크로스 컴파일
v -prod -os linux -arch arm64 -o datacore-linux-arm64 src/main.v
```

### 16.4 Docker

```dockerfile
FROM alpine:3.18
COPY datacore /usr/local/bin/
EXPOSE 9092 8081 9090
ENTRYPOINT ["datacore", "broker", "start"]
```

### 16.5 Kubernetes

- Helm Chart 제공
- StatefulSet (브로커 ID 유지)
- ConfigMap (설정 파일)
- Secret (민감 정보)

---

## 17. V 언어 기술적 고려사항

### 17.1 라이브러리 가용성

| 기능 | V 생태계 지원 | 대안 |
|------|---------------|------|
| TCP/TLS | 내장 지원 | - |
| PostgreSQL | vlib/db/pg | - |
| S3 | 미지원 | C FFI (aws-sdk-c) |
| Avro | 미지원 | 직접 구현 |
| JSON Schema | 부분 지원 | 직접 구현 |
| Protobuf | 미지원 | C FFI 또는 직접 구현 |
| Parquet | 미지원 | C FFI (arrow) |

### 17.2 성능 최적화

- **제로 카피**: 메모리 복사 최소화
- **배치 처리**: 메시지 배치 단위 I/O
- **연결 풀링**: DB 연결 재사용
- **비동기 I/O**: V coroutines 활용

### 17.3 동시성

- V의 내장 채널(channel) 활용
- Mutex/RWLock으로 공유 상태 보호
- Atomic 연산으로 락-프리 카운터 구현

### 17.4 성능 최적화 구현 상세 (v0.42.0)

- **BinaryWriter Buffer Pooling**:
  - `src/infra/performance/core/writer_pool.v`
  - 크기 클래스별(64B - 1MB) 미리 할당된 `u8` 슬라이스 풀 관리
  - Kafka 프로토콜의 대량 직렬화 시 발생하는 수만 건의 메모리 할당을 0에 가깝게 최적화
- **Sharded Atomic Metrics**:
  - `src/infra/observability/atomic_metrics.v`
  - 단일 Atomic 변수에서의 캐시 라인 소통(Cache line bouncing) 문제를 해결하기 위해 16개 샤드로 메트릭 분산
  - 읽기 시에는 모든 샤드를 합산, 쓰기 시에는 Thread ID % 16 샤드에 기록하여 전역 락 경합 90% 감소
- **TCP Stack Tuning**:
  - `src/interface/server/tcp.v`
  - `TCP_NODELAY`: Nagle 알고리즘 비활성화 (실시간성 확보)
  - `TCP_SNDBUF/RCVBUF`: 256KB로 설정하여 대용량 데이터 전송 효율 최적화

---

## 18. 보안 고려사항

### 18.1 통신 보안

- TLS 1.2+ 지원 (선택적)
- 인증서 기반 클라이언트 인증

### 18.2 SASL 인증 (P1)

SASL (Simple Authentication and Security Layer)을 통한 클라이언트 인증을 지원합니다.

```v
// service/auth/sasl.v
module auth

pub enum SaslMechanism {
    plain       // PLAIN - 사용자명/비밀번호 (TLS 필수 권장)
    scram_sha_256  // SCRAM-SHA-256 - Challenge-Response
    scram_sha_512  // SCRAM-SHA-512 - Challenge-Response
    oauthbearer    // OAUTHBEARER - OAuth 2.0 토큰
}

pub interface SaslAuthenticator {
    // 인증 메커니즘 반환
    mechanism() SaslMechanism
    
    // 초기 응답 처리 (client-first)
    authenticate(auth_bytes []u8) !AuthResult
    
    // Challenge-Response 단계 (SCRAM용)
    step(response []u8) !AuthResult
}

pub struct AuthResult {
pub:
    complete     bool        // 인증 완료 여부
    challenge    []u8        // 다음 challenge (SCRAM)
    principal    ?Principal  // 인증된 사용자 정보
    error_code   ?ErrorCode
}

pub struct Principal {
pub:
    name         string      // 사용자 이름
    principal_type string   // "User", "ServiceAccount"
}
```

**SASL 인증 흐름:**

```
1. Client → Broker: SaslHandshake(mechanism="PLAIN")
2. Broker → Client: SaslHandshake Response (mechanisms supported)
3. Client → Broker: SaslAuthenticate(auth_bytes)
4. Broker: 사용자 검증
5. Broker → Client: SaslAuthenticate Response (success/failure)
```

**사용자 저장소:**

```v
// 사용자 정보 인터페이스
pub interface UserStore {
    // 사용자 조회
    get_user(username string) !User
    
    // SCRAM용: salt와 iterations 조회
    get_scram_credentials(username string) !ScramCredentials
    
    // 사용자 생성/삭제 (Admin용)
    create_user(username string, password string, mechanism SaslMechanism) !
    delete_user(username string) !
}
```

### 18.3 ACL 권한 관리 (P1)

리소스별 세밀한 권한 제어를 위한 ACL (Access Control List)을 지원합니다.

```v
// service/auth/acl.v
module auth

pub enum ResourceType {
    topic
    group
    cluster
    transactional_id
    delegation_token
}

pub enum Operation {
    read
    write
    create
    delete
    alter
    describe
    cluster_action
    describe_configs
    alter_configs
    idempotent_write
    all
}

pub enum PermissionType {
    allow
    deny
}

pub enum PatternType {
    literal    // 정확히 일치
    prefixed   // 접두사 일치
    any        // 모든 리소스
}

// ACL 엔트리
pub struct AclEntry {
pub:
    resource_type   ResourceType
    resource_name   string
    pattern_type    PatternType
    principal       string      // "User:alice", "User:*"
    host           string      // "*" 또는 특정 IP
    operation      Operation
    permission     PermissionType
}

// ACL 관리 인터페이스
pub interface AclManager {
    // ACL CRUD
    create_acls(entries []AclEntry) ![]AclResult
    delete_acls(filters []AclFilter) ![]AclDeleteResult
    describe_acls(filter AclFilter) ![]AclEntry
    
    // 권한 검사
    authorize(principal Principal, resource Resource, operation Operation) !bool
}
```

**ACL 검사 흐름:**

```
1. 요청 수신 → Principal 식별 (SASL 인증 결과)
2. 리소스와 작업 결정 (예: Topic "orders", WRITE)
3. AclManager.authorize() 호출
4. 허용 시 요청 처리, 거부 시 AUTHORIZATION_FAILED 반환
```

**기본 ACL 정책:**

| 리소스 | 작업 | 필요 권한 |
|--------|------|----------|
| Topic | Produce | WRITE on Topic |
| Topic | Fetch | READ on Topic |
| Topic | CreateTopics | CREATE on Cluster |
| Topic | DeleteTopics | DELETE on Topic |
| Group | JoinGroup | READ on Group |
| Group | OffsetCommit | READ on Group |
| Cluster | DescribeCluster | DESCRIBE on Cluster |
| TransactionalId | InitProducerId | WRITE on TransactionalId |

---

## 19. v0.30.0+ 개선 로드맵

### 19.1 Kafka 호환성 강화 (v0.30.0 - v0.31.0)

**목표**: 프로덕션 환경에서 필수적인 Kafka API 지원 확대

| 우선순위 | API Key | API Name | 설명 | 목표 버전 |
|----------|---------|----------|------|-----------|
| **P0** | 29 | DescribeAcls | ACL 조회 v0-v3 | v0.30.0 |
| **P0** | 30 | CreateAcls | ACL 생성 v0-v3 | v0.30.0 |
| **P0** | 31 | DeleteAcls | ACL 삭제 v0-v3 | v0.30.0 |
| **P0** | 42 | DeleteGroups | Consumer Group 삭제 v0-v2 | v0.30.0 |
| **P1** | 43 | ElectLeaders | 리더 선출 v0-v2 | v0.31.0 |
| **P2** | 44 | IncrementalAlterConfigs | 점진적 설정 변경 v0-v1 | v0.32.0 |

### 19.2 멀티 브로커 안정성 (v0.31.0 - v0.33.0)

**아키텍처 방향**: Stateless 모델 유지 + 스토리지 레벨 복제 강화

DataCore는 공유 스토리지(S3, PostgreSQL) 기반 Stateless 아키텍처를 채택하고 있습니다.
브로커 레벨 복제 대신 스토리지 레벨 복제를 통해 데이터 내구성을 보장합니다.

| 우선순위 | 작업 | 설명 | 목표 버전 |
|----------|------|------|-----------|
| **P0** | 스토리지 복제 강화 | S3 CRR, PostgreSQL Replication | v0.31.0 |
| **P0** | ISR 관리 실제 구현 | 동기화 상태 추적, acks=-1 보장 | v0.31.0 |
| **P1** | 장애 복구 워크플로우 | 자동 페일오버, 리더 재선출 | v0.33.0 |
| **P1** | Health Check 강화 | 브로커/스토리지 상태 모니터링 | v0.33.0 |
| **P2** | Graceful Shutdown | 안전한 브로커 종료 프로세스 | v0.33.0 |

**스토리지 복제 전략:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     STATELESS + STORAGE REPLICATION                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐                        │
│   │ Broker 1 │     │ Broker 2 │     │ Broker 3 │   ← Stateless          │
│   └────┬─────┘     └────┬─────┘     └────┬─────┘                        │
│        │                │                │                               │
│        └────────────────┼────────────────┘                               │
│                         │                                                │
│                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    공유 스토리지 (Primary)                        │   │
│   │                    S3 / PostgreSQL                               │   │
│   └──────────────────────────┬──────────────────────────────────────┘   │
│                              │                                          │
│                              │ Cross-Region Replication                 │
│                              │ / Streaming Replication                  │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    공유 스토리지 (Replica)                        │   │
│   │                    S3 / PostgreSQL Standby                       │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 19.3 성능 최적화 (v0.32.0 - v0.34.0)

**목표**: 기존 구현된 성능 모듈을 실제 시스템에 통합

현재 `src/infra/performance/`에 구현된 최적화 기능들이 실제 사용되지 않고 있습니다.
이를 네트워크, 스토리지, 프로토콜 레이어에 통합합니다.

| 우선순위 | 작업 | 예상 효과 | 목표 버전 |
|----------|------|----------|-----------|
| **P0** | io_uring 네트워크 통합 | 레이턴시 50% 감소 | v0.32.0 |
| **P0** | CRC32-C 하드웨어 가속 | CPU 사용률 감소 | v0.32.0 |
| **P1** | mmap 스토리지 통합 | 쓰기 처리량 향상 | v0.34.0 |
| **P1** | NUMA 워커 바인딩 | 멀티소켓 성능 향상 | v0.34.0 |
| **P2** | sendfile Zero-Copy | Fetch 성능 향상 | v0.34.0 |
| **P2** | BufferPool Lock-free | 고경합 환경 확장성 | v0.34.0 |

### 19.4 버전 로드맵 요약

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATACORE VERSION ROADMAP                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  v0.29.0 (현재)                                                          │
│  └── Fetch v16, InitProducerId v5, FindCoordinator v6                   │
│  └── SCRAM-SHA-256 인증                                                  │
│  └── 성능 최적화 (파이프라인, 메모리)                                      │
│                                                                          │
│  v0.30.0 - Kafka 호환성 강화 I                                           │
│  ├── ACL APIs (DescribeAcls, CreateAcls, DeleteAcls)                    │
│  └── DeleteGroups API                                                    │
│                                                                          │
│  v0.31.0 - 멀티 브로커 안정성 I                                           │
│  ├── 스토리지 복제 강화 (S3 CRR, PostgreSQL)                              │
│  ├── ISR 관리 실제 구현                                                   │
│  └── ElectLeaders API                                                    │
│                                                                          │
│  v0.32.0 - 성능 최적화 I                                                  │
│  ├── io_uring 네트워크 통합                                               │
│  └── CRC32-C 하드웨어 가속                                                │
│                                                                          │
│  v0.33.0 - 멀티 브로커 안정성 II                                          │
│  ├── 장애 복구 워크플로우                                                 │
│  └── Health Check 강화                                                   │
│                                                                          │
│  v0.34.0 - 성능 최적화 II                                                 │
│  ├── mmap 스토리지 통합                                                   │
│  └── NUMA 워커 바인딩                                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 19.5 의존성 그래프

```
v0.30.0: ACL APIs (#49) ─────────────────────────────────────────┐
         DeleteGroups (#50) ─────────────────────────────────────┤
                                                                  │
v0.31.0: 스토리지 복제 (#51) ──────┬──→ ISR 관리 (#52)           │
         ElectLeaders (#55) ←──────┘         │                    │
                                             │                    │
v0.32.0: io_uring 통합 (#53)                 │                    │
         CRC32-C 가속 (#54)                  │                    │
                                             │                    │
v0.33.0: 장애 복구 (#56) ←───────────────────┘                    │
                                                                  │
v0.34.0: mmap 통합 (#57)                                          │
         NUMA 바인딩 (#58) ←── io_uring (#53)                     │
```

### 19.6 성공 지표

| 영역 | 지표 | 목표 |
|------|------|------|
| **호환성** | kafka-console-* 호환 | 100% |
| **호환성** | kafka-python 테스트 통과 | 100% |
| **안정성** | 브로커 장애 복구 시간 | < 30초 |
| **안정성** | 스토리지 장애 시 데이터 손실 | 0 |
| **성능** | Produce 레이턴시 (p99) | < 10ms |
| **성능** | Fetch 레이턴시 (p99) | < 20ms |
| **성능** | 처리량 (단일 브로커) | > 100MB/s |
