# gRPC 스트리밍 통합 테스트 가이드

## 개요

DataCore의 gRPC 스트리밍 기능을 테스트하기 위한 Python 기반 통합 테스트 스위트입니다.

## 구성 요소

```
tests/integration/
├── grpc_client.py              # Python gRPC 클라이언트 라이브러리
├── test_grpc_streaming.py      # 통합 테스트 스크립트
├── run_grpc_tests.sh           # 테스트 실행 스크립트
├── validate_grpc_client.py     # 클라이언트 검증 테스트
└── GRPC_TESTING.md             # 이 문서
```

## 사전 요구사항

### 1. DataCore 브로커 실행

```bash
# 프로젝트 루트에서
make build
./bin/datacore broker start --config=config.toml
```

**중요**: gRPC 포트(9093)가 열려 있어야 합니다.

### 2. Python 3.6+ 설치

```bash
# 버전 확인
python3 --version

# macOS
brew install python3

# Linux
sudo apt-get install python3
```

### 3. Kafka CLI 도구 (선택사항)

크로스 호환성 테스트를 위해 필요합니다.

```bash
# macOS
brew install kafka

# Linux
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
export PATH=$PATH:$(pwd)/kafka_2.13-3.6.0/bin
```

## 빠른 시작

### 1. 클라이언트 라이브러리 검증

```bash
cd tests/integration
python3 validate_grpc_client.py
```

**예상 출력:**
```
==================================================
  gRPC Client Library Validation
==================================================

Testing GrpcRecord encoding/decoding...
  Encoded size: 72 bytes
  Decoded 72 bytes
  ✓ Encoding/decoding works correctly

Testing record without key...
  ✓ Record without key works

Testing record with empty headers...
  ✓ Record with empty headers works

==================================================
Results: 3 passed, 0 failed
==================================================
```

### 2. 전체 통합 테스트 실행

```bash
cd tests/integration
./run_grpc_tests.sh
```

또는 Python 직접 실행:

```bash
python3 test_grpc_streaming.py
```

## 테스트 시나리오

### 시나리오 1: gRPC ↔ gRPC

양방향 gRPC 스트리밍 테스트

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Python  │────▶│ DataCore │────▶│  Python  │
│ Producer │     │  Broker  │     │ Consumer │
│  (gRPC)  │     │  :9093   │     │  (gRPC)  │
└──────────┘     └──────────┘     └──────────┘
```

**테스트:**
- gRPC Ping/Pong
- gRPC Produce/Consume Simple
- gRPC Multiple Batch Produce
- gRPC Produce with Key
- gRPC Streaming Consume
- gRPC Offset Commit
- gRPC Message Headers

### 시나리오 2: Kafka CLI → gRPC

Kafka 호환성 검증 (Produce)

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│   kcat   │────▶│ DataCore │────▶│  Python  │
│ Producer │     │  :9092   │     │ Consumer │
│ (Kafka)  │     │  :9093   │     │  (gRPC)  │
└──────────┘     └──────────┘     └──────────┘
```

**테스트:**
- Kafka CLI Produce → gRPC Consume

### 시나리오 3: gRPC → Kafka CLI

Kafka 호환성 검증 (Consume)

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Python  │────▶│ DataCore │────▶│   kcat   │
│ Producer │     │  :9093   │     │ Consumer │
│  (gRPC)  │     │  :9092   │     │ (Kafka)  │
└──────────┘     └──────────┘     └──────────┘
```

**테스트:**
- gRPC Produce → Kafka CLI Consume

## 사용 예제

### Python Producer 예제

```python
from grpc_client import GrpcProducer, GrpcRecord

# Producer 생성 및 연결
producer = GrpcProducer("localhost", 9093)
producer.connect()

# 메시지 생성
records = [
    GrpcRecord(value=b"Hello from gRPC"),
    GrpcRecord(key=b"key1", value=b"Message with key"),
    GrpcRecord(
        key=b"key2",
        value=b"Message with headers",
        headers={"content-type": b"application/json"}
    )
]

# 메시지 전송
base_offset, count = producer.produce("my-topic", records, partition=0)
print(f"Produced {count} messages at offset {base_offset}")

producer.close()
```

### Python Consumer 예제

```python
from grpc_client import GrpcConsumer

# Consumer 생성 및 연결
consumer = GrpcConsumer("localhost", 9093, group_id="my-group")
consumer.connect()

# 토픽 구독
consumer.subscribe("my-topic", partition=0, offset=0)

# 메시지 폴링
for _ in range(10):
    result = consumer.poll(timeout=5.0)
    if result:
        topic, partition, offset, record = result
        print(f"[{topic}:{partition}@{offset}] {record.value.decode('utf-8')}")

# 오프셋 커밋
consumer.commit([("my-topic", 0, offset + 1)])

consumer.close()
```

### Ping/Pong 예제

```python
from grpc_client import GrpcClient

client = GrpcClient("localhost", 9093)
client.connect()

timestamp = client.ping()
print(f"Server timestamp: {timestamp}")

client.close()
```

## 커스텀 설정

### 환경 변수

```bash
# gRPC 서버 설정
export GRPC_HOST=192.168.1.100
export GRPC_PORT=9094

# Kafka 서버 설정
export KAFKA_HOST=192.168.1.100
export KAFKA_PORT=9092

# 테스트 실행
./run_grpc_tests.sh
```

### 명령줄 인자

```bash
# gRPC 포트 변경
./run_grpc_tests.sh --grpc-port 9094

# 전체 설정
./run_grpc_tests.sh \
  --grpc-host 192.168.1.100 \
  --grpc-port 9094 \
  --kafka-host 192.168.1.100 \
  --kafka-port 9092
```

### Python 직접 실행

```bash
python3 test_grpc_streaming.py \
  --grpc-host localhost \
  --grpc-port 9093 \
  --kafka-host localhost \
  --kafka-port 9092
```

## 트러블슈팅

### Connection refused (gRPC)

```bash
# gRPC 포트 확인
nc -zv localhost 9093

# 브로커 로그 확인
tail -f /var/log/datacore/broker.log

# config.toml 확인 (gRPC 설정이 있어야 함)
grep -A 5 "\[grpc\]" config.toml
```

**해결 방법:**
1. 브로커가 실행 중인지 확인
2. config.toml에 gRPC 설정 추가
3. 방화벽 설정 확인

### Python import errors

```bash
# grpc_client.py 위치 확인
ls -la tests/integration/grpc_client.py

# Python 경로 문제
cd tests/integration
python3 -c "import grpc_client"
```

**해결 방법:**
- `tests/integration` 디렉토리에서 실행
- 또는 `PYTHONPATH` 설정

### Kafka CLI not found

```bash
# Kafka CLI 확인
which kafka-topics.sh
which kafka-console-producer.sh
```

**해결 방법:**
- Kafka CLI 도구 설치
- 또는 Kafka CLI 테스트 건너뛰기 (자동으로 스킵됨)

### Test timeout

```bash
# 타임아웃 증가
python3 test_grpc_streaming.py  # 기본 타임아웃 사용

# 또는 코드에서 timeout 파라미터 조정
consumer.poll(timeout=10.0)  # 10초로 증가
```

## 테스트 결과 해석

### 성공 예시

```
==========================================
  DataCore gRPC Streaming Tests
==========================================

gRPC Server: localhost:9093
Kafka Server: localhost:9092

--- Basic gRPC Tests ---

[INFO] Running: gRPC Ping/Pong
[PASS] gRPC Ping/Pong

--- gRPC ↔ gRPC Tests ---

[INFO] Running: gRPC Produce/Consume Simple
[PASS] gRPC Produce/Consume Simple

...

==========================================
  Test Summary
==========================================
Total:  9
Passed: 9
Failed: 0
```

### 실패 예시

```
[INFO] Running: gRPC Produce/Consume Simple
[FAIL] gRPC Produce/Consume Simple
       Error: Connection refused
```

**일반적인 실패 원인:**
1. 브로커가 실행되지 않음
2. gRPC 포트가 열려있지 않음
3. 네트워크 연결 문제
4. 토픽 생성 실패

## 성능 고려사항

### 배치 크기

```python
# 작은 배치 (빠른 응답)
records = [GrpcRecord(value=b"msg") for _ in range(10)]

# 큰 배치 (높은 처리량)
records = [GrpcRecord(value=b"msg") for _ in range(1000)]
```

### 폴링 타임아웃

```python
# 짧은 타임아웃 (빠른 반응)
result = consumer.poll(timeout=0.5)

# 긴 타임아웃 (CPU 절약)
result = consumer.poll(timeout=5.0)
```

### 연결 재사용

```python
# 좋은 예: 연결 재사용
producer = GrpcProducer("localhost", 9093)
producer.connect()
for batch in batches:
    producer.produce("topic", batch)
producer.close()

# 나쁜 예: 매번 새 연결
for batch in batches:
    producer = GrpcProducer("localhost", 9093)
    producer.connect()
    producer.produce("topic", batch)
    producer.close()
```

## 추가 리소스

- **DataCore 문서**: `docs/TRD.md`
- **gRPC 프로토콜**: `src/domain/grpc.v`
- **gRPC 서비스**: `src/service/streaming/grpc_service.v`
- **gRPC 핸들러**: `src/infra/protocol/grpc/handler.v`

## 기여하기

새로운 테스트 케이스를 추가하려면:

1. `test_grpc_streaming.py`에 테스트 함수 추가
2. `main()` 함수에서 `run_test()` 호출
3. 테스트 실행 및 검증
4. Pull Request 생성

## 라이선스

Apache 2.0
