# DataCore Replication Compatibility Tests

이 디렉토리에는 DataCore 복제 기능과 표준 Kafka 클라이언트의 호환성을 테스트하는 스크립트가 포함되어 있습니다.

## 테스트 목록

### test_replication_compat.sh
DataCore 복제가 활성화된 경우 표준 Kafka CLI 도구로 호환성을 테스트합니다.

**테스트 카테고리:**
1. Broker Connectivity - 브로커 연결 테스트
2. Create Topic with Replication Factor > 1 - 복제 팩터 2 이상으로 토픽 생성
3. kafka-console-producer - ISRs로 메시지 복제 테스트
4. kafka-console-consumer from Leader - 리더에서 소비 테스트
5. kafka-console-consumer from Follower - 팔로워에서 소비 테스트
6. kcat produce - kcat으로 생산 테스트
7. kcat consume from replica - kcat으로 복제본에서 소비 테스트
8. Cross-Broker Replication - 크로스 브로커 복제 테스트
9. Consumer Group with Replication - 복제 환경에서 그룹 소비자 테스트
10. Compression with Replication - 복제와 함께 압축 테스트

## 실행 방법

### 필수 조건
- 2개 이상의 DataCore 브로커가 복제 활성화 상태로 실행 중
- Kafka CLI 도구 (kafka-topics, kafka-console-producer, kafka-console-consumer)
- 선택: kcat (kafkacat)

### 브로커 시작
복제 테스트를 실행하기 전에 2개 이상의 브로커를 시작해야 합니다:

```bash
# Broker 1 (포트 9092, 복제 포트 9093)
./bin/datacore broker start --config=config-broker1.toml &

# Broker 2 (포트 9094, 복제 포트 9095)  
./bin/datacore broker start --config=config-broker2.toml &
```

### 테스트 실행

```bash
# 기본 실행 (기본값: 127.0.0.1:9092,127.0.0.1:9094)
make test-replication-compat

# 사용자 정의 브로커 설정
BOOTSTRAP_SERVERS="host1:9092,host2:9094" make test-replication-compat

# 단일 브로커로 테스트 실행 (복제 없이)
make test-compat
```

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| BOOTSTRAP_SERVERS | 127.0.0.1:9092,127.0.0.1:9094 | 브로커 서버 목록 |

## 테스트 결과

테스트는 다음 상태를 반환합니다:
- PASS: 모든 검증 통과
- FAIL: 하나 이상의 검증 실패
- SKIP: 선택적 도구(kcat) 미설치로 건너뛰기

## 문제 해결

### 브로커가 연결되지 않음
- 브로커가 실행 중인지 확인: `nc -z 127.0.0.1 9092`
- 로그 확인: `cat broker1.log`

### 복제가 작동하지 않음
- config 파일에서 `replication.enabled = true` 확인
- `cluster_brokers` 목록에 모든 브로커 포함 확인
