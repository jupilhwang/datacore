# DataCore Observability Stack

OpenTelemetry Collector를 사용하여 DataCore의 메트릭과 로그를 수집하고, Grafana로 시각화하는 완전한 모니터링 스택입니다.

## 🏗️ 아키텍처

```
┌─────────────┐      OTLP      ┌──────────────────┐
│  DataCore   │ ──────────────> │ OTel Collector   │
│  (Broker)   │   메트릭/로그    │                  │
└─────────────┘                 └────────┬─────────┘
                                         │
                     ┌───────────────────┼───────────────────┐
                     │                   │                   │
                     ▼                   ▼                   ▼
            ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
            │  Prometheus  │   │     Loki     │   │   Grafana    │
            │   (메트릭)    │   │   (로그)     │   │ (시각화/알림) │
            └──────────────┘   └──────────────┘   └──────────────┘
```

## 🚀 빠른 시작

### 1. 사전 요구사항

- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ RAM 권장

### 2. 모니터링 스택 시작

```bash
# 프로젝트 루트에서 실행
cd /Users/jhwang/works/test/datacore

# 모니터링 스택 시작
docker-compose -f docker-compose.monitoring.yml up -d

# 모든 서비스가 준비될 때까지 대기 (약 30초)
sleep 30

# 상태 확인
docker-compose -f docker-compose.monitoring.yml ps
```

### 3. 접속 정보

| 서비스 | URL | 기본 계정 |
|--------|-----|----------|
| **Grafana** | http://localhost:3000 | admin/admin |
| **Prometheus** | http://localhost:9090 | 없음 |
| **Loki** | http://localhost:3100 | 없음 |
| **OTel Collector** | http://localhost:13133 | 없음 |

## 📊 Grafana 대시보드

### DataCore Overview 대시보드

자동으로 등록되는 대시보드입니다:

- **Overview**: 서비스 상태, 요청 지연시간, 처리량
- **Storage Metrics**: 저장소 사용량, 메시지 처리량
- **Logs**: 실시간 에러/경고 로그

접속: http://localhost:3000/d/datacore-overview

### 주요 메트릭

#### 1. 성능 메트릭
- `datacore_request_duration_seconds` - 요청 처리 시간
- `datacore_requests_total` - 총 요청 수
- `datacore_messages_produced_total` - 생산된 메시지 수
- `datacore_messages_consumed_total` - 소비된 메시지 수

#### 2. 저장소 메트릭
- `datacore_storage_bytes_total` - 총 저장소 크기
- `datacore_storage_bytes_used` - 사용 중인 저장소
- `datacore_storage_messages_total` - 저장된 메시지 수

#### 3. 시스템 메트릭
- `process_cpu_seconds_total` - CPU 사용량
- `process_resident_memory_bytes` - 메모리 사용량
- `go_goroutines` - Go 루틴 수

## 📝 로그 수집

### DataCore 로그 설정

DataCore의 `config.toml`에서 로그 출력을 설정합니다:

```toml
[logging]
level = "info"
output = "file"  # 또는 "stdout", "both"
file_path = "/var/log/datacore/datacore.log"
format = "json"  # JSON 형식 권장 (자동 파싱)
```

### 로그 쿼리 예제 (Grafana)

**에러 로그만 보기:**
```
{service="datacore"} |~ "ERROR"
```

**특정 토픽 로그:**
```
{service="datacore"} |= "topic=my-topic"
```

**최근 1시간 WARN 이상:**
```
{service="datacore"} 
| json
| level =~ "WARN|ERROR|FATAL"
```

## 🔔 알림 설정

### 기본 알림 규칙

Prometheus에 설정된 알림 규칙 (`monitoring/prometheus/alert_rules.yml`):

1. **DataCoreHighErrorRate** - 오류율이 높을 때
2. **DataCoreHighLatency** - 지연시간이 높을 때
3. **DataCoreHighMemoryUsage** - 메모리 사용량이 85% 이상
4. **DataCoreHighDiskUsage** - 디스크 사용량이 85% 이상
5. **DataCoreDown** - 서비스가 다운되었을 때

### 알림 채널 설정 (Grafana)

1. Grafana에 로그인: http://localhost:3000
2. Alerting → Notification channels
3. New channel 추가:
   - **Slack**: Webhook URL 입력
   - **Email**: SMTP 설정
   - **PagerDuty**: Integration key 입력
   - **Webhook**: Custom endpoint

## 🛠️ 고급 설정

### OTel Collector 커스터마이징

`monitoring/otel-collector/otel-config.yaml` 수정:

```yaml
# 추가 수집기 설정
receivers:
  # 파일 로그 수집
  filelog:
    include: ["/var/log/datacore/*.log"]
    
  # Prometheus 메트릭 수집
  prometheus:
    config:
      scrape_configs:
        - job_name: 'custom-app'
          static_configs:
            - targets: ['my-app:8080']

# 추가 프로세서
processors:
  # 필터링
  filter:
    metrics:
      include:
        match_type: regexp
        metric_names:
          - "my_app_.*"

# 추가 익스포터
exporters:
  # AWS X-Ray
  awsxray:
    region: us-west-2
    
  # Datadog
  datadog:
    api:
      key: ${DATADOG_API_KEY}
```

### 보안 설정

#### 1. Grafana HTTPS 활성화

```yaml
# docker-compose.monitoring.yml 수정
grafana:
  environment:
    - GF_SERVER_PROTOCOL=https
    - GF_SERVER_CERT_FILE=/etc/grafana/cert.pem
    - GF_SERVER_CERT_KEY=/etc/grafana/key.pem
  volumes:
    - ./ssl:/etc/grafana:ro
```

#### 2. 인증 활성화

```yaml
# prometheus.yml 수정
basic_auth:
  username: admin
  password: ${PROMETHEUS_PASSWORD}
```

### 성능 튜닝

#### Prometheus

```yaml
# 더 긴 보관 기간
command:
  - '--storage.tsdb.retention.time=30d'  # 30일 보관
  - '--storage.tsdb.retention.size=10GB'  # 최대 10GB
```

#### Loki

```yaml
# 더 긴 로그 보관
limits_config:
  reject_old_samples_max_age: 720h  # 30일
```

#### OTel Collector

```yaml
# 배치 크기 조정
processors:
  batch:
    timeout: 5s           # 더 긴 배치 윈도우
    send_batch_size: 2048 # 더 큰 배치 크기
```

## 🔍 문제 해결

### 서비스가 시작되지 않음

```bash
# 로그 확인
docker-compose -f docker-compose.monitoring.yml logs -f otel-collector
docker-compose -f docker-compose.monitoring.yml logs -f prometheus
docker-compose -f docker-compose.monitoring.yml logs -f loki

# 설정 검증
docker-compose -f docker-compose.monitoring.yml config
```

### 메트릭이 보이지 않음

1. **OTel Collector 상태 확인:**
   ```bash
   curl http://localhost:13133
   curl http://localhost:8889/metrics
   ```

2. **Prometheus 타겟 확인:**
   - http://localhost:9090/targets
   - "datacore" job 상태 확인

3. **DataCore 메트릭 엔드포인트:**
   ```bash
   curl http://localhost:9092/metrics
   ```

### 로그가 보이지 않음

1. **Loki 상태 확인:**
   ```bash
   curl http://localhost:3100/ready
   ```

2. **파일 경로 확인:**
   ```bash
   docker exec datacore-loki ls -la /var/log/datacore/
   ```

3. **로그 레벨 확인:**
   - DataCore 설정에서 `level = "info"` 또는 "debug"

### 성능 문제

**높은 메모리 사용량:**
```bash
# OTel Collector 메모리 제한
docker update --memory=2g --memory-swap=2g datacore-otel-collector
```

**높은 CPU 사용량:**
```yaml
# 배치 크기 줄이기
processors:
  batch:
    timeout: 100ms
    send_batch_size: 512
```

## 📚 참고 자료

- [OpenTelemetry Collector 문서](https://opentelemetry.io/docs/collector/)
- [Prometheus 문서](https://prometheus.io/docs/)
- [Grafana 문서](https://grafana.com/docs/)
- [Loki 문서](https://grafana.com/docs/loki/latest/)

## 📄 파일 구조

```
monitoring/
├── otel-collector/
│   └── otel-config.yaml      # OTel Collector 설정
├── prometheus/
│   ├── prometheus.yml        # Prometheus 스크랩 설정
│   └── alert_rules.yml       # 알림 규칙
├── loki/
│   └── loki-config.yaml      # Loki 설정
├── grafana/
│   ├── datasources/
│   │   └── datasources.yaml  # 데이터 소스 설정
│   └── dashboards/
│       ├── dashboard-provider.yaml
│       └── datacore-dashboard.json  # DataCore 대시보드
└── ...

docker-compose.monitoring.yml  # 모니터링 스택
MONITORING.md                  # 이 문서
```

## 🤝 기여

개선사항이나 버그 제보는 프로젝트 이슈 트래커를 이용해 주세요.

## 📜 라이선스

Apache License 2.0

---

**버전**: v0.42.0  
**최종 업데이트**: 2026-02-01
