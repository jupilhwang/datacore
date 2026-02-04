# DataCore

**DataCore**는 V 언어로 구현된 Apache Kafka 완전 호환 메시지 브로커입니다. 플러그인 기반 스토리지 엔진, 내장 스키마 레지스트리, 그리고 Zero-Copy 성능 최적화를 지원합니다.

## Version

**v0.43.0**

## 주요 특징

- **Kafka API 완전 호환**: Kafka v1.1 ~ v4.1 프로토콜 지원
- **Iceberg REST Catalog**: Apache Iceberg 호환 REST 카탈로그 (v3 지원)
- **다중 스토리지 엔진**: Memory, S3, PostgreSQL 지원
- **내장 스키마 레지스트리**: Avro, JSON Schema, Protobuf 지원
- **고성능 네트워킹**: io_uring, WebSocket, SSE 지원
- **기업급 보안**: SASL/SCRAM 인증, ACL 권한 관리
- **트랜잭션 지원**: Exactly-Once Semantics (EOS)

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                        DataCore                              │
├─────────────────────────────────────────────────────────────┤
│  Interface Layer                                             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Kafka   │ │   gRPC   │ │   REST   │ │  WebSocket/SSE  │ │
│  │ Protocol │ │ Handler  │ │  Server  │ │  Streaming      │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Service Layer                                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Topic   │ │ Consumer │ │  Auth    │ │  Streaming      │ │
│  │ Service  │ │  Service │ │ Service  │ │  Service        │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Storage Layer (Plugin-based)                                │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Memory  │ │    S3    │ │PostgreSQL│ │  (Custom)       │ │
│  │ Adapter  │ │ Adapter  │ │ Adapter  │ │  Adapter        │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Domain Layer                                                │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐ │
│  │  Record  │ │  Topic   │ │  Group   │ │  Schema         │ │
│  │  Model   │ │  Model   │ │  Model   │ │  Registry       │ │
│  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 주요 기능

### 메시지 압축 지원

DataCore는 다양한 메시지 압축 알고리즘을 지원하여 네트워크 대역폭과 스토리지 사용량을 최적화합니다.

**지원되는 압축 방식:**
- **Snappy**: 고속 압축, 낮은 CPU 오버헤드
- **Gzip**: 균형 잡힌 압축률과 속도
- **LZ4**: 초고속 압축, 실시간 처리 최적화
- **Zstd**: 최상의 압축률, 현대적 알고리즘

**호환성:**
- 표준 Kafka 클라이언트와 완전 호환
- `kafka-console-producer`, `kafka-console-consumer` 지원
- `kcat` (kafkacat) 호환
- `make test-compat`로 표준 Kafka 클라이언트 호환성 검증 가능

```bash
# 압축 테스트 실행
make test-compat
```

### 다중 스토리지 엔진

DataCore는 플러그인 기반 스토리지 아키텍처를 지원합니다:

| 스토리지 | 용도 | 특징 |
|----------|------|------|
| **Memory** | 개발/테스트 | 가장 빠른 속도, 영속성 없음 |
| **S3** | 프로덕션 | 무한 확장, 장기 저장소 |
| **PostgreSQL** | 프로덕션 | 트랜잭션 지원, 복잡한 쿼리 |

### S3 스토리지 개선 (v0.41.0)

**새로운 기능:**
- **HTTP Connection Pooling**: 연결 재사용으로 지연 시간 감소
- **병렬 처리 최적화**: 세그먼트 다운로드/업로드 병렬화
- **향상된 캐싱**: 인덱스 캐시 TTL 최적화

**성능 향상:**
- 파티션 컴팩션: 순차 → 병렬 처리 (최대 10배 향상)
- 오프셋 커밋: 5-10배 빠른 병렬 커밋
- 객체 삭제: 20배 빠른 병렬 삭제

### 관측성 (Observability)

**메트릭:**
- 스토리지별 상세 메트릭 (Memory, S3, PostgreSQL)
- 프로토콜 핸들러별 성능 추적
- Kafka API 요청/응답 통계

**로깅:**
- 구조화된 로깅 (JSON 형식)
- OpenTelemetry OTLP 내보내기 지원
- 로그 레벨 동적 조절

**헬스 체크:**
- Kubernetes 호환 `/health`, `/ready`, `/live` 엔드포인트
- Prometheus 형식 `/metrics` 엔드포인트

## 설치 가이드 (Installation Guide)

### 1. 바이너리 다운로드 (Download Binary)

가장 빠른 방법은 [GitHub Releases](https://github.com/jupilhwang/datacore/releases) 페이지에서 자신의 OS에 맞는 바이너리를 직접 다운로드하는 것입니다.

1.  최신 버전의 릴리스 페이지로 이동합니다.
2.  사용 중인 운영체제에 맞는 바이너리를 다운로드합니다:
    - **Linux**: `datacore-linux-amd64-static.tar.gz` (정적 빌드, 의존성 없음)
    - **Linux ARM64**: `datacore-linux-arm64-static.tar.gz` (정적 빌드)
    - **macOS Intel**: `datacore-darwin-amd64.tar.gz` (동적 빌드)
    - **macOS Apple Silicon**: `datacore-darwin-arm64.tar.gz` (동적 빌드)
3.  압축을 풀고 실행 권한을 부여한 후 실행합니다.
    ```bash
    # Linux (정적 빌드 - 의존성 필요 없음)
    tar -xzf datacore-linux-amd64-static.tar.gz
    chmod +x datacore
    ./datacore broker start
    
    # macOS (동적 빌드 - Homebrew 라이브러리 필요)
    tar -xzf datacore-darwin-arm64.tar.gz
    chmod +x datacore
    ./datacore broker start
    ```

### 2. 소스 코드에서 빌드 (Build from Source)

직접 빌드하고 싶거나 개발에 참여하시려면 다음 단계를 따르세요.

#### 2.1 사전 요구 사항 (Prerequisites)

- **V 언어**: [V 공식 홈페이지](https://vlang.io/)의 안내에 따라 V를 설치해야 합니다.
- **C 컴파일러**: GCC, Clang 또는 MSVC가 필요합니다.

#### 2.2 OS별 의존성 라이브러리 설치

DataCore는 압축(Snappy, LZ4, Zstd), 보안(OpenSSL), 데이터베이스(PostgreSQL) 연동을 위해 외부 라이브러리를 사용합니다.

**macOS (Homebrew 사용)**
```bash
brew install openssl snappy lz4 zstd postgresql
```

**Linux (Ubuntu/Debian)**
```bash
sudo apt-get update
sudo apt-get install -y build-essential libssl-dev libsnappy-dev liblz4-dev libzstd-dev libpq-dev libnuma-dev liburing-dev zlib1g-dev
```

#### 2.3 빌드 과정

```bash
# 저장소 클론
git clone https://github.com/jupilhwang/datacore.git
cd datacore

# 바이너리 빌드
make build
```

빌드가 성공하면 `bin/datacore` 파일이 생성됩니다.

## 빠른 시작 (Quick Start)

### 빌드

```bash
make build
```

### 실행

```bash
# 기본 설정으로 브로커 실행
./bin/datacore broker start --config=config.toml

# CLI 인자로 설정 오버라이드
./bin/datacore broker start --broker-port=9092 --storage-engine=memory
```

### Docker 실행

```bash
# Docker Compose로 실행
docker-compose up -d

# Docker 이미지 직접 빌드
docker build -t datacore:latest .
docker run -p 9092:9092 -p 8080:8080 datacore:latest
```

### 설정 파일 예시 (`config.toml`)

```toml
[broker]
host = "0.0.0.0"
port = 9092
broker_id = 1

[storage]
engine = "s3"

[s3]
endpoint = "http://localhost:9000"
bucket = "datacore"
region = "us-east-1"
access_key = "your-access-key"
secret_key = "your-secret-key"

[logging]
level = "info"
output = "stdout"
```

## 테스트

### 단위 테스트

권장하는 방식은 `Makefile`을 이용하는 것입니다:

```bash
# 주요 모듈 단위 테스트 실행
make test

# 모든 테스트 실행 (통합, 스토리지, 보안 포함)
make test-all
```

수동으로 `v test`를 실행할 경우, 성능 모듈 등 글로벌 변수가 필요한 테스트를 위해 `-enable-globals` 플래그가 필요합니다:

```bash
# 모든 단위 테스트 실행
v -enable-globals test src/

# 특정 테스트 파일 실행
v -enable-globals test src/config/config_test.v
```

### 통합 테스트

```bash
# 모든 통합 테스트 실행
v -enable-globals test tests/

# 스토리지별 테스트
./scripts/test_storage.sh all

# Kafka 클라이언트 호환성 테스트
make test-compat

# 성능 회귀 테스트
./scripts/test_performance.sh --save-baseline
```

### 테스트 스위트

| 테스트 유형 | 설명 | 명령어 |
|------------|------|--------|
| **스토리지 테스트** | Memory, PostgreSQL, S3 스토리지 검증 | `./scripts/test_storage.sh` |
| **메시지 포맷 테스트** | 압축, 인코딩 호환성 검증 | `v test tests/` |
| **Admin API 테스트** | 토픽 관리, 설정 변경 테스트 | `v test tests/integration/` |
| **호환성 테스트** | 표준 Kafka 클라이언트 호환성 | `make test-compat` |
| **장기 실행 테스트** | 24시간+ 안정성 검증 | `./scripts/test_longrunning.sh` |
| **보안 테스트** | SSL/TLS, SASL 인증 테스트 | `./scripts/test_security.sh` |

## Kafka 클라이언트 호환성

DataCore는 표준 Kafka 클라이언트와 완전 호환됩니다:

```bash
# kafka-console-producer로 메시지 생산
kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

# kafka-console-consumer로 메시지 소비
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning

# kcat으로 메시지 확인
kcat -b localhost:9092 -t test-topic
```

## 프로젝트 구조

```
datacore/
├── src/
│   ├── domain/          # 도메인 모델
│   ├── service/         # 비즈니스 로직
│   ├── infra/
│   │   ├── storage/     # 스토리지 어댑터
│   │   ├── protocol/    # 프로토콜 핸들러
│   │   └── server/      # 네트워크 서버
│   └── interface/       # 외부 인터페이스
├── tests/
│   ├── unit/            # 단위 테스트
│   └── integration/     # 통합 테스트
├── scripts/             # 유틸리티 스크립트
├── docs/                # 문서
├── config.toml          # 기본 설정 파일
└── Makefile             # 빌드 타겟
```

## 라이선스

Apache License 2.0
