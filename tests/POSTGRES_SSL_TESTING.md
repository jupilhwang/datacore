# PostgreSQL SSL Connection Testing

이 문서는 DataCore의 PostgreSQL SSL 연결 기능을 테스트하는 방법을 설명합니다.

## 개요

DataCore는 PostgreSQL 연결 시 다양한 SSL 모드를 지원합니다:

| SSL Mode      | 설명                        | 보안 수준  |
| ------------- | --------------------------- | ---------- |
| `disable`     | SSL 사용 안함 (기본값)      | ⭐         |
| `allow`       | 서버가 요구하면 SSL 사용    | ⭐⭐       |
| `prefer`      | 가능하면 SSL 사용           | ⭐⭐⭐     |
| `require`     | SSL 필수 (인증서 검증 안함) | ⭐⭐⭐⭐   |
| `verify-ca`   | SSL + CA 인증서 검증        | ⭐⭐⭐⭐⭐ |
| `verify-full` | SSL + CA + 호스트명 검증    | ⭐⭐⭐⭐⭐ |

## 사전 요구사항

### 1. PostgreSQL 서버

테스트를 위해 PostgreSQL 서버가 필요합니다:

```bash
# Docker로 PostgreSQL 실행 (SSL 비활성화)
docker run -d \
  --name postgres-test \
  -e POSTGRES_USER=datacore \
  -e POSTGRES_PASSWORD=datacore \
  -e POSTGRES_DB=datacore_test \
  -p 5432:5432 \
  postgres:15-alpine

# 또는 SSL 활성화된 PostgreSQL
docker run -d \
  --name postgres-ssl-test \
  -e POSTGRES_USER=datacore \
  -e POSTGRES_PASSWORD=datacore \
  -e POSTGRES_DB=datacore_test \
  -p 5432:5432 \
  postgres:15-alpine \
  -c ssl=on \
  -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
  -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key
```

### 2. 환경 변수 설정

```bash
export DATACORE_PG_HOST=localhost
export DATACORE_PG_PORT=5432
export DATACORE_PG_USER=datacore
export DATACORE_PG_PASSWORD=datacore
export DATACORE_PG_DATABASE=datacore_test
export DATACORE_PG_SSLMODE=disable  # 또는 require, prefer 등
```

## 테스트 방법

### 방법 1: 연결 테스트 프로그램

간단한 연결 테스트:

```bash
# 환경 변수 설정 후
v run tests/postgres_ssl_connection_test.v
```

**출력 예시:**

```
=================================
PostgreSQL SSL Connection Test
=================================

📋 Configuration:
  Host:     localhost
  Port:     5432
  User:     datacore
  Database: datacore_test
  SSL Mode: require

🔌 Testing connection...
✅ Connection successful!

🏥 Running health check...
✅ Health check passed: healthy

📊 Storage capabilities:
  Name:                  postgresql
  Multi-broker support:  true
  Transaction support:   true
  Persistent:            true
  Distributed:           true

🔐 SSL Mode: require
  ✅ SSL is required - connection is encrypted

=================================
✅ All tests passed!
=================================
```

### 방법 2: 단위 테스트

전체 테스트 스위트 실행:

```bash
# 테스트 스크립트 실행
./tests/postgres_ssl_test.sh
```

또는 직접 V 테스트 실행:

```bash
v test src/infra/storage/plugins/postgres/adapter_test.v
```

### 방법 3: 수동 테스트

각 SSL 모드를 개별적으로 테스트:

```bash
# SSL 비활성화 모드
export DATACORE_PG_SSLMODE=disable
v run tests/postgres_ssl_connection_test.v

# SSL 선호 모드
export DATACORE_PG_SSLMODE=prefer
v run tests/postgres_ssl_connection_test.v

# SSL 필수 모드
export DATACORE_PG_SSLMODE=require
v run tests/postgres_ssl_connection_test.v
```

## 테스트 케이스

### 1. SSL 비활성화 (`disable`)

```bash
export DATACORE_PG_SSLMODE=disable
v run tests/postgres_ssl_connection_test.v
```

**예상 결과**: 암호화 없이 연결 성공

### 2. SSL 선호 (`prefer`)

```bash
export DATACORE_PG_SSLMODE=prefer
v run tests/postgres_ssl_connection_test.v
```

**예상 결과**:

- 서버가 SSL 지원 → 암호화 연결
- 서버가 SSL 미지원 → 비암호화 연결

### 3. SSL 필수 (`require`)

```bash
export DATACORE_PG_SSLMODE=require
v run tests/postgres_ssl_connection_test.v
```

**예상 결과**:

- 서버가 SSL 지원 → 암호화 연결 성공
- 서버가 SSL 미지원 → 연결 실패

### 4. 설정 검증

```bash
v test src/infra/storage/plugins/postgres/adapter_test.v -run test_ssl_config_validation
```

**예상 결과**: 모든 유효한 SSL 모드 검증 성공

## 문제 해결

### 연결 실패

**증상**: `Connection failed: server does not support SSL`

**해결**:

1. PostgreSQL 서버가 SSL을 지원하는지 확인
2. `sslmode`를 `disable` 또는 `prefer`로 변경
3. 서버 설정에서 SSL 활성화

### 인증서 오류

**증상**: `SSL certificate verification failed`

**해결**:

1. `sslmode=require` 사용 (인증서 검증 안함)
2. 올바른 CA 인증서 제공
3. 자체 서명 인증서의 경우 `verify-ca` 대신 `require` 사용

### 환경 변수 미설정

**증상**: `DATACORE_PG_HOST not set`

**해결**:

```bash
export DATACORE_PG_HOST=localhost
export DATACORE_PG_USER=datacore
export DATACORE_PG_PASSWORD=datacore
```

## 프로덕션 권장사항

### 보안 설정

프로덕션 환경에서는 다음 설정을 권장합니다:

```toml
[storage.postgres]
host = "production-db.example.com"
port = 5432
database = "datacore"
user = "datacore_user"
# password는 환경 변수로 관리
pool_size = 20
sslmode = "require"  # 최소 require 이상
```

### 환경 변수

민감한 정보는 환경 변수로 관리:

```bash
export DATACORE_PG_PASSWORD="secure_password"
export DATACORE_PG_SSLMODE="require"
```

### 인증서 검증

최고 수준의 보안이 필요한 경우:

```toml
[storage.postgres]
sslmode = "verify-full"
```

## 참고 자료

- [PostgreSQL SSL Support](https://www.postgresql.org/docs/current/libpq-ssl.html)
- [PostgreSQL Connection Strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
- [DataCore Configuration Guide](../docs/TRD.md)

## 변경 이력

- **2026-01-23**: PostgreSQL SSL 지원 추가
  - sslmode 필드 추가
  - 연결 문자열 방식 구현
  - SSL 연결 테스트 추가
