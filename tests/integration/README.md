# DataCore Integration Tests

이 디렉토리에는 Kafka CLI 도구, gRPC 스트리밍, Schema Registry REST API를 사용한 통합 테스트가 포함되어 있습니다.

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

### 2. Python 3 설치 (gRPC 테스트용)

```bash
# macOS (Homebrew)
brew install python3

# Linux (Ubuntu/Debian)
sudo apt-get install python3 python3-pip

# 버전 확인
python3 --version  # 3.6 이상 필요
```

### 3. Avro 도구 설치 (선택사항)

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

### 3. gRPC 스트리밍 테스트 실행

```bash
# 기본 설정 (gRPC: localhost:9093, Kafka: localhost:9092)
./run_grpc_tests.sh

# 커스텀 설정
./run_grpc_tests.sh --grpc-port 9094 --kafka-port 9092

# 환경 변수 사용
GRPC_HOST=192.168.1.100 GRPC_PORT=9093 ./run_grpc_tests.sh
```

### 4. Schema Registry 테스트 실행

```bash
# 기본 설정 (http://localhost:8081)
./test_schema_registry.sh

# 커스텀 설정
SCHEMA_REGISTRY_URL=http://myhost:8081 ./test_schema_registry.sh
```

### 5. 전체 테스트 실행

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

### gRPC Streaming Tests (`test_grpc_streaming.py`)

| 테스트 | 설명 |
|--------|------|
| gRPC Ping/Pong | gRPC 연결 및 keepalive 테스트 |
| gRPC Produce/Consume Simple | gRPC로 메시지 produce 및 consume |
| gRPC Multiple Batch Produce | 여러 배치 메시지 전송 |
| gRPC Produce with Key | 키가 있는 메시지 전송 |
| gRPC Streaming Consume | 스트리밍 방식 메시지 수신 |
| Kafka CLI → gRPC | Kafka CLI로 produce, gRPC로 consume |
| gRPC → Kafka CLI | gRPC로 produce, Kafka CLI로 consume |
| gRPC Offset Commit | Consumer group 오프셋 커밋 |
| gRPC Message Headers | 메시지 헤더 전송 및 수신 |

**테스트 시나리오:**
- **gRPC ↔ gRPC**: 양방향 gRPC 스트리밍 테스트
- **Kafka CLI → gRPC**: Kafka 호환성 검증 (produce)
- **gRPC → Kafka CLI**: Kafka 호환성 검증 (consume)

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
| `GRPC_HOST` | localhost | gRPC 서버 호스트 |
| `GRPC_PORT` | 9093 | gRPC 서버 포트 |
| `KAFKA_HOST` | localhost | Kafka 서버 호스트 |
| `KAFKA_PORT` | 9092 | Kafka 서버 포트 |
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
netstat -an | grep 9092  # Kafka port
netstat -an | grep 9093  # gRPC port

# 브로커 로그 확인
tail -f /var/log/datacore/broker.log
```

### gRPC connection failed

```bash
# gRPC 포트 확인
nc -zv localhost 9093

# config.toml에서 gRPC 설정 확인
grep -A 5 "\[grpc\]" config.toml

# gRPC가 활성화되어 있는지 확인
# [grpc]
# enabled = true
# port = 9093
```

### Python import errors

```bash
# grpc_client.py가 같은 디렉토리에 있는지 확인
ls -la tests/integration/grpc_client.py

# Python 경로 확인
cd tests/integration
python3 -c "import grpc_client"
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

### Kafka CLI 예제

#### 토픽 생성

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create \
    --topic my-topic \
    --partitions 3 \
    --replication-factor 1
```

#### 메시지 전송

```bash
echo "Hello DataCore" | kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic
```

#### 메시지 수신

```bash
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic my-topic \
    --from-beginning
```

### gRPC 예제

#### Python 클라이언트로 메시지 전송

```python
from grpc_client import GrpcProducer, GrpcRecord

# Producer 생성
producer = GrpcProducer("localhost", 9093)
producer.connect()

# 메시지 전송
records = [
    GrpcRecord(value=b"Hello from gRPC"),
    GrpcRecord(key=b"key1", value=b"Message with key")
]

base_offset, count = producer.produce("my-topic", records, partition=0)
print(f"Produced {count} messages at offset {base_offset}")

producer.close()
```

#### Python 클라이언트로 메시지 수신

```python
from grpc_client import GrpcConsumer

# Consumer 생성
consumer = GrpcConsumer("localhost", 9093)
consumer.connect()

# 구독 시작
consumer.subscribe("my-topic", partition=0, offset=0)

# 메시지 폴링
for _ in range(10):
    result = consumer.poll(timeout=5.0)
    if result:
        topic, partition, offset, record = result
        print(f"Received: {record.value.decode('utf-8')}")

consumer.close()
```

### Schema Registry 예제

#### 스키마 등록

```bash
curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
    http://localhost:8081/subjects/user-value/versions
```
