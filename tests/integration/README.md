# DataCore Integration Tests

이 디렉토리에는 Kafka CLI 도구와 Schema Registry REST API를 사용한 통합 테스트가 포함되어 있습니다.

## 사전 요구사항

### 1. Kafka CLI 도구 설치

```bash
# macOS (Homebrew)
brew install kafka

# Linux (Ubuntu/Debian)
# Download from https://kafka.apache.org/downloads
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
export PATH=$PATH:$(pwd)/kafka_2.13-3.6.0/bin
```

### 2. Avro 도구 설치 (선택사항)

Avro 테스트를 위해서는 Confluent Schema Registry CLI가 필요합니다:

```bash
# Confluent CLI 설치
curl -sL https://cnfl.io/cli | sh -s -- latest

# 또는 직접 다운로드
# https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html
```

## 테스트 실행

### 1. DataCore 브로커 실행

```bash
# 프로젝트 루트에서
cd src
v run . broker start
```

### 2. Kafka CLI 테스트 실행

```bash
# 기본 설정 (localhost:9092)
./test_kafka_cli.sh

# 커스텀 설정
BOOTSTRAP_SERVER=myhost:9092 ./test_kafka_cli.sh
```

### 3. Schema Registry 테스트 실행

```bash
# 기본 설정 (http://localhost:8081)
./test_schema_registry.sh

# 커스텀 설정
SCHEMA_REGISTRY_URL=http://myhost:8081 ./test_schema_registry.sh
```

### 4. 전체 테스트 실행

```bash
./run_all_tests.sh
```

## 테스트 목록

### Kafka CLI Tests (`test_kafka_cli.sh`)

| 테스트 | 설명 |
|--------|------|
| Create Topic | kafka-topics --create 으로 토픽 생성 |
| List Topics | kafka-topics --list 로 토픽 목록 조회 |
| Describe Topic | kafka-topics --describe 로 토픽 상세 조회 |
| Delete Topic | kafka-topics --delete 로 토픽 삭제 |
| Produce/Consume Simple | 단순 메시지 송수신 |
| Produce Multiple Messages | 여러 메시지 일괄 송수신 |
| Produce/Consume with Key | 키-값 형태 메시지 송수신 |
| Create Avro Topic | Avro 테스트용 토픽 생성 |
| Avro Produce/Consume | Avro 스키마 메시지 송수신 |
| Avro with Key | 키가 있는 Avro 메시지 송수신 |
| Consumer Group | Consumer Group 기능 테스트 |

### Schema Registry Tests (`test_schema_registry.sh`)

| 테스트 | 엔드포인트 |
|--------|-----------|
| Register Schema | POST /subjects/{subject}/versions |
| Get Schema by ID | GET /schemas/ids/{id} |
| List Subjects | GET /subjects |
| List Versions | GET /subjects/{subject}/versions |
| Get Schema by Version | GET /subjects/{subject}/versions/{version} |
| Get Latest Schema | GET /subjects/{subject}/versions/latest |
| Get Raw Schema | GET /subjects/{subject}/versions/{version}/schema |
| Get Compatibility | GET /config/{subject} |
| Set Compatibility | PUT /config/{subject} |
| Check Compatibility | POST /compatibility/subjects/{subject}/versions/{version} |
| Register Multiple Versions | 여러 버전 등록 |
| Delete Version | DELETE /subjects/{subject}/versions/{version} |
| Delete Subject | DELETE /subjects/{subject} |
| JSON Schema | JSON Schema 타입 등록 |
| Protobuf Schema | Protobuf 타입 등록 |
| Schema Not Found (404) | 에러 처리 검증 |
| Subject Not Found (404) | 에러 처리 검증 |

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `BOOTSTRAP_SERVER` | localhost:9092 | Kafka 브로커 주소 |
| `SCHEMA_REGISTRY_URL` | http://localhost:8081 | Schema Registry URL |
| `TIMEOUT` | 10 | Consumer 타임아웃 (초) |

## 테스트 결과 해석

```
[PASS] - 테스트 성공
[FAIL] - 테스트 실패
[INFO] - 정보 메시지 (건너뛴 테스트 등)
```

## 트러블슈팅

### Connection refused

```bash
# 브로커가 실행 중인지 확인
netstat -an | grep 9092

# 브로커 로그 확인
tail -f /var/log/datacore/broker.log
```

### Kafka CLI not found

```bash
# PATH 확인
which kafka-topics.sh
which kafka-topics

# Kafka 설치 경로를 PATH에 추가
export PATH=$PATH:/path/to/kafka/bin
```

### Avro tools not found

```bash
# Confluent 도구 확인
which kafka-avro-console-producer

# Avro 테스트는 선택사항이므로 건너뛰어도 됨
```

## 수동 테스트 예제

### 토픽 생성

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create \
    --topic my-topic \
    --partitions 3 \
    --replication-factor 1
```

### 메시지 전송

```bash
echo "Hello DataCore" | kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic
```

### 메시지 수신

```bash
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning
```

### Schema Registry 스키마 등록

```bash
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
    http://localhost:8081/subjects/user-value/versions
```
