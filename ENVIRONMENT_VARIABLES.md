# DataCore Environment Variables

DataCore는 환경 변수를 통해 모든 설정을 오버라이드할 수 있습니다. 환경 변수는 `config.toml` 파일보다 우선순위가 높습니다.

## 🎯 Naming Convention

환경 변수 이름은 TOML 키에서 자동으로 생성됩니다.

**지원 형식** (모두 동일하게 동작):
1. **대문자** (권장): `SECTION_KEY`
2. **소문자**: `section_key`
3. **DATACORE_ 접두사**: `DATACORE_SECTION_KEY`

**변환 규칙**: `section.key`
- 점(`.`) → 언더스코어(`_`)
- 대문자 또는 소문자 모두 가능

### 예시

| TOML Key | 대문자 | 소문자 | DATACORE_접두사 |
|----------|--------|--------|-----------------|
| `broker.host` | `BROKER_HOST` | `broker_host` | `DATACORE_BROKER_HOST` |
| `broker.port` | `BROKER_PORT` | `broker_port` | `DATACORE_BROKER_PORT` |
| `storage.engine` | `STORAGE_ENGINE` | `storage_engine` | `DATACORE_STORAGE_ENGINE` |
| `s3.endpoint` | `S3_ENDPOINT` | `s3_endpoint` | `DATACORE_S3_ENDPOINT` |
| `postgres.password` | `POSTGRES_PASSWORD` | `postgres_password` | `DATACORE_POSTGRES_PASSWORD` |

### 예시

| TOML Key | 환경 변수 이름 |
|----------|---------------|
| `broker.host` | `BROKER_HOST` |
| `broker.port` | `BROKER_PORT` |
| `storage.engine` | `STORAGE_ENGINE` |
| `s3.endpoint` | `S3_ENDPOINT` |
| `postgres.password` | `POSTGRES_PASSWORD` |
| `observability.logging.level` | `OBSERVABILITY_LOGGING_LEVEL` |
| `compression.gzip_level` | `COMPRESSION_GZIP_LEVEL` |

## 🚀 Quick Start

```bash
# 1. 기본 설정 파일 사용
./bin/datacore broker start --config=config.toml

# 2. 환경 변수로 오버라이드
BROKER_PORT=9093 STORAGE_ENGINE=s3 ./bin/datacore broker start --config=config.toml

# 3. .env 파일 사용 (docker-compose 등)
export BROKER_HOST=0.0.0.0
export BROKER_PORT=9092
export STORAGE_ENGINE=memory
export LOGGING_LEVEL=debug
./bin/datacore broker start
```

## 📋 전체 환경 변수 목록

### [broker]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `broker.host` | `BROKER_HOST` | `0.0.0.0` | 브로커 바인딩 주소 |
| `broker.port` | `BROKER_PORT` | `9092` | Kafka 프로토콜 포트 |
| `broker.broker_id` | `BROKER_BROKER_ID` | `1` | 브로커 ID |
| `broker.cluster_id` | `BROKER_CLUSTER_ID` | `datacore-cluster` | 클러스터 ID |
| `broker.threads` | `BROKER_THREADS` | `4` | IO 스레드 수 |
| `broker.enable_pprof` | `BROKER_ENABLE_PPROF` | `false` | pprof 활성화 |

#### TCP 설정

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `broker.tcp_nodelay` | `BROKER_TCP_NODELAY` | `true` | TCP_NODELAY 활성화 |
| `broker.tcp_send_buf_size` | `BROKER_TCP_SEND_BUF_SIZE` | `262144` | TCP 송신 버퍼 (256KB) |
| `broker.tcp_recv_buf_size` | `BROKER_TCP_RECV_BUF_SIZE` | `262144` | TCP 수신 버퍼 (256KB) |

### [storage]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `storage.engine` | `STORAGE_ENGINE` | `memory` | 저장소 엔진 (`memory`, `s3`, `postgres`) |
| `storage.topic_retention_hours` | `STORAGE_TOPIC_RETENTION_HOURS` | `168` | 토픽 보관 기간 (7일) |
| `storage.segment_max_bytes` | `STORAGE_SEGMENT_MAX_BYTES` | `1073741824` | 세그먼트 최대 크기 (1GB) |
| `storage.segment_max_age_ms` | `STORAGE_SEGMENT_MAX_AGE_MS` | `3600000` | 세그먼트 최대 수명 (1시간) |

### [s3]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `s3.endpoint` | `S3_ENDPOINT` | `http://localhost:9000` | S3 엔드포인트 |
| `s3.bucket` | `S3_BUCKET` | `datacore` | 버킷 이름 |
| `s3.region` | `S3_REGION` | `us-east-1` | 리전 |
| `s3.access_key` | `S3_ACCESS_KEY` | - | 액세스 키 |
| `s3.secret_key` | `S3_SECRET_KEY` | - | 시크릿 키 |
| `s3.use_ssl` | `S3_USE_SSL` | `false` | SSL 사용 |
| `s3.path_style` | `S3_PATH_STYLE` | `true` | 경로 스타일 엔드포인트 |

### [storage.s3.iceberg]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `storage.s3.iceberg.enabled` | `STORAGE_S3_ICEBERG_ENABLED` | `false` | Iceberg 형식 사용 여부 |
| `storage.s3.iceberg.format` | `STORAGE_S3_ICEBERG_FORMAT` | `parquet` | 파일 형식 (parquet, orc, avro) |
| `storage.s3.iceberg.catalog.type` | `STORAGE_S3_ICEBERG_CATALOG_TYPE` | `hadoop` | 카탈로그 타입 (hadoop, rest) |
| `storage.s3.iceberg.catalog.warehouse` | `STORAGE_S3_ICEBERG_CATALOG_WAREHOUSE` | - | Warehouse 위치 (S3 경로) |
| `storage.s3.iceberg.catalog.format_version` | `STORAGE_S3_ICEBERG_CATALOG_FORMAT_VERSION` | `3` | Iceberg 테이블 포맷 버전 (1, 2, 3) |

### [postgres]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `postgres.host` | `POSTGRES_HOST` | `localhost` | PostgreSQL 호스트 |
| `postgres.port` | `POSTGRES_PORT` | `5432` | PostgreSQL 포트 |
| `postgres.database` | `POSTGRES_DATABASE` | `datacore` | 데이터베이스 이름 |
| `postgres.user` | `POSTGRES_USER` | `datacore` | 사용자 이름 |
| `postgres.password` | `POSTGRES_PASSWORD` | - | 비밀번호 |
| `postgres.sslmode` | `POSTGRES_SSLMODE` | `disable` | SSL 모드 |
| `postgres.max_connections` | `POSTGRES_MAX_CONNECTIONS` | `20` | 최대 연결 수 |
| `postgres.connection_timeout` | `POSTGRES_CONNECTION_TIMEOUT` | `30` | 연결 타임아웃 (초) |

### [schema_registry]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `schema_registry.enabled` | `SCHEMA_REGISTRY_ENABLED` | `true` | 스키마 레지스트리 활성화 |
| `schema_registry.port` | `SCHEMA_REGISTRY_PORT` | `8081` | 스키마 레지스트리 포트 |
| `schema_registry.storage_path` | `SCHEMA_REGISTRY_STORAGE_PATH` | `./schemas` | 스키마 저장 경로 |

### [logging]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `logging.level` | `LOGGING_LEVEL` | `info` | 로그 레벨 (`trace`, `debug`, `info`, `warn`, `error`) |
| `logging.output` | `LOGGING_OUTPUT` | `stdout` | 출력 대상 (`stdout`, `file`, `both`) |
| `logging.file_path` | `LOGGING_FILE_PATH` | - | 로그 파일 경로 |
| `logging.format` | `LOGGING_FORMAT` | `json` | 로그 형식 (`json`, `text`) |
| `logging.max_size` | `LOGGING_MAX_SIZE` | `100` | 최대 파일 크기 (MB) |
| `logging.max_age` | `LOGGING_MAX_AGE` | `7` | 최대 보관 일수 |

### [rest]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `rest.enabled` | `REST_ENABLED` | `true` | REST API 활성화 |
| `rest.port` | `REST_PORT` | `8080` | REST API 포트 |

### [grpc]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `grpc.enabled` | `GRPC_ENABLED` | `true` | gRPC 활성화 |
| `grpc.port` | `GRPC_PORT` | `50051` | gRPC 포트 |

### [websocket]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `websocket.enabled` | `WEBSOCKET_ENABLED` | `true` | WebSocket 활성화 |
| `websocket.port` | `WEBSOCKET_PORT` | `8081` | WebSocket 포트 |
| `websocket.path` | `WEBSOCKET_PATH` | `/ws` | WebSocket 경로 |

### [compression]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `compression.compression_threshold_bytes` | `COMPRESSION_COMPRESSION_THRESHOLD_BYTES` | `1024` | 압축 임계값 (1KB) |
| `compression.gzip_level` | `COMPRESSION_GZIP_LEVEL` | `6` | Gzip 레벨 (1-9) |
| `compression.zstd_level` | `COMPRESSION_ZSTD_LEVEL` | `3` | ZSTD 레벨 (1-22) |
| `compression.lz4_acceleration` | `COMPRESSION_LZ4_ACCELERATION` | `1` | LZ4 가속 (1-12) |

### [security]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `security.enable_auth` | `SECURITY_ENABLE_AUTH` | `false` | 인증 활성화 |
| `security.jwt_secret` | `SECURITY_JWT_SECRET` | - | JWT 시크릿 |
| `security.jwt_expiry_hours` | `SECURITY_JWT_EXPIRY_HOURS` | `24` | JWT 만료 시간 (시간) |

### [observability]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `observability.enabled` | `OBSERVABILITY_ENABLED` | `true` | 옵저버빌리티 활성화 |
| `observability.metrics_port` | `OBSERVABILITY_METRICS_PORT` | `9090` | 메트릭 포트 |
| `observability.pprof_port` | `OBSERVABILITY_PPROF_PORT` | `6060` | pprof 포트 |

#### [observability.otel]

| TOML Key | 환경 변수 | 기본값 | 설명 |
|----------|-----------|--------|------|
| `observability.otel.otlp_endpoint` | `OBSERVABILITY_OTEL_OTLP_ENDPOINT` | - | OTLP 엔드포인트 |
| `observability.otel.otlp_insecure` | `OBSERVABILITY_OTEL_OTLP_INSECURE` | `true` | 비보안 OTLP |

## 📝 사용 예시

### Docker Compose

```yaml
version: "3.8"

services:
  datacore:
    image: datacore:latest
    environment:
      - BROKER_HOST=0.0.0.0
      - BROKER_PORT=9092
      - STORAGE_ENGINE=s3
      - S3_ENDPOINT=http://minio:9000
      - S3_BUCKET=datacore
      - S3_ACCESS_KEY=minioadmin
      - S3_SECRET_KEY=minioadmin
      - LOGGING_LEVEL=info
      - LOGGING_OUTPUT=stdout
      - COMPRESSION_ZSTD_LEVEL=3
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datacore
spec:
  template:
    spec:
      containers:
        - name: datacore
          image: datacore:latest
          env:
            - name: BROKER_HOST
              value: "0.0.0.0"
            - name: BROKER_PORT
              value: "9092"
            - name: STORAGE_ENGINE
              value: "postgres"
            - name: POSTGRES_HOST
              valueFrom:
                secretKeyRef:
                  name: postgres-secret
                  key: host
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: postgres-secret
                  key: password
```

### 쉘 스크립트

```bash
#!/bin/bash

# 환경 변수 설정
export BROKER_HOST="0.0.0.0"
export BROKER_PORT="9092"
export BROKER_CLUSTER_ID="production-cluster"

export STORAGE_ENGINE="s3"
export S3_ENDPOINT="https://s3.amazonaws.com"
export S3_BUCKET="my-datacore-bucket"
export S3_REGION="us-west-2"
# 민감한 정보는 별도로 관리
export S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}"
export S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}"

export LOGGING_LEVEL="warn"
export LOGGING_OUTPUT="file"
export LOGGING_FILE_PATH="/var/log/datacore/datacore.log"

export COMPRESSION_ZSTD_LEVEL="5"

# DataCore 시작
./bin/datacore broker start --config=/etc/datacore/config.toml
```

## 🔍 환경 변수 확인

DataCore는 시작 시 환경 변수 매핑을 출력할 수 있습니다:

```bash
# 환경 변수 매핑 출력 (디버깅용)
./bin/datacore config env-mapping

# 또는 상세 모드
./bin/datacore broker start --config=config.toml --verbose
```

## ⚠️ 주의사항

1. **우선순위**: 환경 변수 > CLI 인자 > config.toml > 기본값
2. **민감한 정보**: `S3_SECRET_KEY`, `POSTGRES_PASSWORD`, `SECURITY_JWT_SECRET` 등은 안전하게 관리하세요
3. **타입**: 환경 변수는 문자열로 처리되며, 내부적으로 적절한 타입으로 변환됩니다
4. **빈 문자열**: 빈 문자열(`""`)은 유효한 값으로 처리됩니다. 기본값을 사용하려면 환경 변수를 설정하지 마세요.

## 🔧 디버깅

환경 변수가 제대로 적용되지 않는 경우:

```bash
# 1. 환경 변수 확인
echo $BROKER_PORT

# 2. DataCore 로그 확인
./bin/datacore broker start --config=config.toml --log-level=debug

# 3. 설정 덤프
./bin/datacore config dump
```

---

**버전**: v0.43.0  
**업데이트 날짜**: 2026-02-03
