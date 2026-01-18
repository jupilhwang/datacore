# DataCore 개발 가이드라인

> 이 문서는 AI 모델과 개발자가 일관된 코드 스타일과 개발 프로세스를 따르도록 하기 위한 가이드라인입니다.
> 모든 코드 작성, 리뷰, 커밋 시 이 문서를 참조하세요.

## 목차

1. [Code Convention](#1-code-convention)
2. [테스트 계획](#2-테스트-계획)
3. [Git 브랜치 정책](#3-git-브랜치-정책)
4. [커밋 정책](#4-커밋-정책)
5. [버저닝](#5-버저닝)
6. [Code 주의사항](#6-code-주의사항)
7. [개발 명령어](#7-개발-명령어)
8. [AI/Task Manager 작업 가이드라인](#8-aitask-manager-작업-가이드라인)
9. [Kafka 프로토콜 참조](#9-kafka-프로토콜-참조)
10. [통합 테스트](#10-통합-테스트)

---

## 1. Code Convention

V 언어 공식 스타일([docs.vlang.io](https://docs.vlang.io/introduction.html))과 [Google Go 스타일 가이드](https://google.github.io/styleguide/go/)를 참고합니다.

### 1.1 네이밍 규칙

| 대상 | 스타일 | 예시 |
| ------ | -------- | ------ |
| **변수** | snake_case | `message_count`, `offset_value` |
| **함수** | snake_case | `parse_request()`, `handle_produce()` |
| **상수** | snake_case | `max_batch_size`, `default_timeout` |
| **구조체** | PascalCase | `ProduceRequest`, `ConsumerGroup` |
| **인터페이스** | PascalCase | `StorageEngine`, `ProtocolAdapter` |
| **Enum** | PascalCase (타입), snake_case (값) | `ErrorCode.invalid_topic` |
| **모듈** | snake_case | `consumer_group`, `storage_engine` |
| **파일** | snake_case | `produce_handler.v`, `memory_engine.v` |

### 1.2 파일 구조

```v
// 1. 모듈 선언
module kafka

// 2. Import (표준 라이브러리 먼저, 빈 줄로 구분)
import encoding.binary
import time

import datacore.storage
import datacore.schema

// 3. 상수
const (
    max_message_size = 1048576  // 1MB
    default_timeout_ms = 30000
)

// 4. 타입 정의 (struct, enum, interface)
pub struct ProduceRequest {
pub:
    topic       string
    partition   int
pub mut:
    records     []Record
}

// 5. 함수/메서드 구현
pub fn (req &ProduceRequest) validate() ! {
    if req.topic == '' {
        return error('topic is required')
    }
}
```

### 1.3 주석 스타일

```v
// 한 줄 주석: 코드 위에 작성
offset := get_offset()

/*
 * 여러 줄 주석: 복잡한 로직 설명
 * - 첫 번째 단계
 * - 두 번째 단계
 */

/// 문서화 주석: 공개 API에 필수
/// `parse_request`는 바이너리 데이터를 Kafka 요청으로 파싱합니다.
/// 
/// # Arguments
/// - `data`: 요청 바이너리 데이터
/// 
/// # Returns
/// - 파싱된 `Request` 객체
/// 
/// # Errors
/// - 데이터가 유효하지 않으면 에러 반환
pub fn parse_request(data []u8) !Request {
    // ...
}
```

### 1.4 에러 처리 패턴

```v
// ❌ 잘못된 예: panic 사용
fn bad_example() {
    panic('something went wrong')
}

// ✅ 올바른 예: Result 타입 사용
fn good_example() !Result {
    if condition {
        return error('descriptive error message')
    }
    return Result{}
}

// ✅ 에러 전파
fn caller() ! {
    result := good_example()!  // 에러 자동 전파
    // 또는
    result := good_example() or {
        log.error('Failed: ${err}')
        return err
    }
}
```

### 1.5 코드 포맷팅

```bash
# 저장 전 항상 포맷팅 실행
v fmt -w src/
```

### 1.6 가독성 원칙

- **함수는 짧게**: 50줄 이하 권장
- **중첩 최소화**: 3단계 이상 중첩 피하기
- **Early return**: 조건 검사 후 빠르게 반환

```v
// ❌ 중첩이 깊은 코드
fn process(req Request) !Response {
    if req.valid {
        if req.topic != '' {
            if req.partition >= 0 {
                // 실제 로직
            }
        }
    }
}

// ✅ Early return 패턴
fn process(req Request) !Response {
    if !req.valid {
        return error('invalid request')
    }
    if req.topic == '' {
        return error('topic required')
    }
    if req.partition < 0 {
        return error('invalid partition')
    }
    
    // 실제 로직
}
```

---

## 2. 테스트 계획

### 2.1 테스트 구조

```
tests/
├── unit/                    # 단위 테스트
│   ├── protocol/
│   │   ├── produce_test.v
│   │   └── fetch_test.v
│   └── storage/
│       ├── memory_test.v
│       └── s3_test.v
├── integration/             # 통합 테스트
│   ├── kafka_compat_test.v
│   └── consumer_group_test.v
├── e2e/                     # E2E 테스트
│   └── full_flow_test.v
└── benchmark/               # 성능 테스트
    └── throughput_test.v
```

### 2.2 단위 테스트 작성 규칙

```v
// 파일명: *_test.v
module storage

// 테스트 함수명: test_기능_시나리오
fn test_memory_engine_append_single_record() {
    // Arrange (준비)
    mut engine := MemoryEngine.new()
    record := Record{ key: 'k1', value: 'v1' }
    
    // Act (실행)
    result := engine.append('topic1', 0, [record]) or {
        assert false, 'append failed: ${err}'
        return
    }
    
    // Assert (검증)
    assert result.base_offset == 0
    assert result.record_count == 1
}

fn test_memory_engine_append_empty_records_returns_error() {
    mut engine := MemoryEngine.new()
    
    // 빈 레코드 추가 시 에러 예상
    engine.append('topic1', 0, []) or {
        assert err.msg().contains('empty records')
        return
    }
    assert false, 'expected error for empty records'
}
```

### 2.3 테스트 커버리지 목표

| 영역 | 목표 | 우선순위 |
| ------ | ------ | ---------- |
| Protocol 파싱 | 90% | P0 |
| Storage Engine | 85% | P0 |
| Consumer Group | 80% | P0 |
| Admin API | 75% | P1 |
| Schema Registry | 80% | P1 |

### 2.4 Kafka 호환성 테스트

```bash
# kafka-python 으로 호환성 테스트
python tests/compat/kafka_python_test.py

# franz-go (Go 클라이언트) 테스트
go test ./tests/compat/...
```

---

## 3. Git 브랜치 정책

### 3.1 핵심 원칙 ⚠️

> **모든 새로운 작업(Task)은 반드시 새 브랜치에서 진행합니다.**
> **main 브랜치에 직접 커밋하지 않습니다.**
> **⚠️ main 브랜치 머지는 반드시 사용자 승인 후 진행합니다.**

```
┌─────────────────────────────────────────────────────────────────┐
│  ⛔ main 브랜치 직접 커밋 금지                                   │
│  ⛔ main 브랜치 머지 시 사용자 승인 필수                         │
│  ✅ 새 브랜치 생성 → 개발 → 테스트 → 사용자 승인 → main 머지     │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 브랜치 구조

```
main                    # 프로덕션 릴리스 (보호됨)
├── feature/*           # 기능 개발
├── bugfix/*            # 버그 수정
├── refactor/*          # 리팩토링
├── release/*           # 릴리스 준비
└── hotfix/*            # 긴급 수정
```

### 3.3 브랜치 네이밍

| 타입 | 형식 | 예시 |
|------|------|------|
| **Feature** | `feature/간단설명` | `feature/add-otel-tracing` |
| **Bugfix** | `bugfix/간단설명` | `bugfix/fix-offset-commit` |
| **Hotfix** | `hotfix/버전-간단설명` | `hotfix/0.2.1-memory-leak` |
| **Release** | `release/버전` | `release/0.3.0` |
| **Refactor** | `refactor/영역` | `refactor/storage-interface` |

### 3.4 브랜치 워크플로우 (필수 준수)

#### 새 작업 시작 시

```bash
# 1. main 브랜치 최신화
git checkout main && git pull origin main

# 2. 새 브랜치 생성
git checkout -b feature/작업설명

# 3. 개발 진행 (여러 커밋 가능)
git add .
git commit -m "feat(scope): description"

# 4. 테스트 통과 확인
v test src/
make build  # 또는 v build src/

# 5. main 브랜치로 머지 준비
git checkout main
git pull origin main

# 6. 머지 (--no-ff로 머지 커밋 생성)
git merge --no-ff feature/작업설명 -m "Merge feature/작업설명"

# 7. 브랜치 삭제 (선택)
git branch -d feature/작업설명
```

#### 작업 중 main이 업데이트된 경우

```bash
# 작업 브랜치에서 main 변경사항 가져오기
git checkout feature/작업설명
git fetch origin
git rebase origin/main
# 충돌 해결 후 계속
```

### 3.5 AI/Copilot 작업 규칙

AI 어시스턴트(GitHub Copilot 등)가 작업 시:

1. **작업 시작 전** 새 브랜치 생성 확인
2. **main 브랜치 감지 시** 자동으로 브랜치 생성 제안
3. **작업 완료 후** 테스트 통과 확인
4. **⚠️ main 머지 전 반드시 사용자 승인 요청** (추가 작업 여부 확인)

```bash
# AI 작업 시작 시 자동 실행
git checkout main
git checkout -b feature/task-설명

# AI 작업 완료 후
v test src/                    # 테스트 통과 확인

# ⚠️ main 머지 전 사용자에게 확인 요청 ⚠️
# "작업이 완료되었습니다. main 브랜치에 머지해도 될까요?"
# 사용자 승인 후에만 아래 실행:
git checkout main
git merge --no-ff feature/task-설명
```

---

## 4. 커밋 정책

### 4.1 Conventional Commits 형식

```
<type>(<scope>): <subject>

[optional body]

[optional footer]
```

### 4.2 커밋 타입

| 타입 | 설명 | 예시 |
|------|------|------|
| `feat` | 새 기능 | `feat(storage): add S3 storage plugin` |
| `fix` | 버그 수정 | `fix(protocol): correct offset parsing` |
| `docs` | 문서 변경 | `docs: update API documentation` |
| `style` | 코드 포맷팅 | `style: apply v fmt` |
| `refactor` | 리팩토링 | `refactor(broker): extract connection manager` |
| `test` | 테스트 추가/수정 | `test(storage): add memory engine tests` |
| `chore` | 빌드/설정 변경 | `chore: update v.mod dependencies` |
| `perf` | 성능 개선 | `perf(fetch): implement zero-copy` |

### 4.3 커밋 메시지 예시

```
feat(protocol): implement Kafka Produce API v0-v9

- Add version-specific request parsing
- Support transactional ID for v3+
- Add RecordBatch format for v3+

Closes #42
```

```
fix(consumer): prevent duplicate message delivery

The consumer group was not properly tracking committed
offsets after rebalance, causing duplicate deliveries.

- Add offset validation before fetch
- Update commit logic to be atomic

Fixes #78
```

### 4.4 커밋 규칙

- 한 커밋 = 한 논리적 변경
- 제목 50자 이내, 본문 72자 줄바꿈
- 현재형 동사 사용 ("Add" not "Added")
- 이슈 번호 연결 (`Closes #123`, `Fixes #456`)

---

## 5. 버저닝

### 5.1 핵심 원칙 ⚠️

> **모든 feature 또는 fix 작업 완료 시 반드시 버전을 업데이트합니다.**
> **버전 업데이트 없이 main에 머지하지 않습니다.**

```
┌─────────────────────────────────────────────────────────────────┐
│  ⛔ 버전 업데이트 없이 feature/fix 머지 금지                     │
│  ✅ feature/fix 완료 → 버전 업데이트 → 테스트 → main 머지        │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 버전 정책

DataCore는 **릴리즈.메이저.마이너** 버전 체계를 따릅니다:

```
<릴리즈>.<메이저>.<마이너>[-PRERELEASE]

예시: 0.1.0, 0.2.0, 1.0.0, 1.1.5-alpha.1
```

| 구분 | 변경 시점 | 예시 |
|------|----------|------|
| **릴리즈 (Release)** | 대규모 아키텍처 변경, 안정 버전 출시 | 0.x → 1.0.0 (GA 출시) |
| **메이저 (Major)** | 새로운 기능 추가 (`feat`) | 1.0.0 → 1.1.0 (Consumer Group 추가) |
| **마이너 (Minor)** | 버그 수정 (`fix`), 문서갱신 (`docs`), `style`, `refactor`, `perf`, `testing`, 작은 개선 | 1.1.0 → 1.1.1 (버그 수정) |

### 5.3 브랜치 타입별 버전 업데이트 규칙

| 브랜치 타입 | 버전 변경 | 예시 |
| ------------ | ---------- | ------ |
| `feature/*` | **메이저 증가** (x.X.0) | 0.2.0 → 0.3.0 |
| `bugfix/*`, `fix/*` | **마이너 증가** (x.x.X) | 0.2.0 → 0.2.1 |
| `hotfix/*` | **마이너 증가** (x.x.X) | 0.2.0 → 0.2.1 |
| `refactor/*` | **마이너 증가** (x.x.X) | 0.2.0 → 0.2.1 |
| `docs/*` | **마이너 증가** (x.x.X) | 0.2.0 → 0.2.1 |
| `chore/*` | **마이너 증가** (x.x.X) | 0.2.0 → 0.2.1 |

### 5.4 버전 업데이트 체크리스트

모든 브랜치 작업 완료 시:

```bash
# 1. 버전 업데이트 (src/v.mod)
#    feature                      → 메이저 버전 증가 (0.2.0 → 0.3.0)
#    fix/refactor/docs/chore 등   → 마이너 버전 증가 (0.2.0 → 0.2.1)

# 2. config.toml의 서비스 버전 업데이트 (있는 경우)
#    service_version 필드 확인

# 3. 버전 업데이트 커밋 포함하여 머지
git commit -am "chore(release): bump version to 0.3.0"
```

### 5.5 AI/Copilot 버전 관리 규칙

AI 어시스턴트(GitHub Copilot 등)가 작업 시:

1. **feature 브랜치 작업 완료 시**: 메이저 버전 증가 (x.X.0)
2. **그 외 브랜치 작업 완료 시**: 마이너 버전 증가 (x.x.X)
   - `bugfix/*`, `fix/*`, `hotfix/*`, `refactor/*`, `docs/*`, `chore/*`
3. **main 머지 전** `src/v.mod` 버전 업데이트 확인
4. **태그 생성**: 머지 후 `v<버전>` 태그 생성

```bash
# AI feature 작업 완료 시 자동 실행
# 1. 버전 확인 및 업데이트
cat src/v.mod  # 현재 버전 확인
# src/v.mod 버전 수정 (0.2.0 → 0.3.0)

# 2. 커밋에 버전 변경 포함
git add src/v.mod
git commit -am "chore(release): bump version to 0.3.0"

# 3. main 머지
git checkout main
git merge --no-ff feature/task-설명

# 4. 태그 생성
git tag -a v0.3.0 -m "Release v0.3.0: 기능 설명"
```

### 5.6 버전 관리 기준

#### 릴리즈 버전 증가 (X.0.0)
- 프로젝트 초기 개발 완료 (0.x → 1.0.0)
- 대규모 아키텍처 재설계
- 하위 호환성이 깨지는 주요 변경

#### 메이저 버전 증가 (x.X.0) - feature 브랜치
- 새로운 기능 추가 (`feat` 커밋)
- 새로운 기능 그룹/모듈 추가
- API 확장 (기존 API 호환 유지)
- 성능 최적화 모듈 추가
- 새로운 프로토콜 지원

#### 마이너 버전 증가 (x.x.X) - feature 외 모든 브랜치
- 버그 수정 (`fix` 커밋)
- 리팩토링 (`refactor` 커밋)
- 문서 업데이트 (`docs` 커밋)
- 빌드/설정 변경 (`chore` 커밋)
- 기존 기능 개선
- 보안 패치
- 핫픽스

### 5.3 Git 태그 정책

모든 릴리즈는 Git 태그로 관리됩니다:

```bash
# 태그 형식
v<릴리즈>.<메이저>.<마이너>

# 예시
v0.1.0   # 초기 개발 버전
v0.2.0   # 성능 최적화 추가
v1.0.0   # 첫 안정 릴리즈
v1.1.0   # Consumer Group 기능 추가
```

#### 태그 생성 명령어

```bash
# 1. 태그 생성 (annotated tag 권장)
git tag -a v0.2.0 -m "Release v0.2.0: Performance optimization module"

# 2. 원격 저장소에 태그 푸시
git push origin v0.2.0

# 3. 모든 태그 푸시
git push origin --tags
```

### 5.4 버전 관리 위치

```v
// src/v.mod
Module {
    name: 'datacore'
    version: '0.2.0'  // 이 버전을 업데이트
    ...
}
```

### 5.5 릴리스 프로세스

```bash
# 1. release 브랜치 생성
git checkout -b release/0.2.0 develop

# 2. 버전 업데이트
# src/v.mod 수정

# 3. 변경사항 커밋
git commit -am "chore(release): bump version to 0.2.0"

# 4. main에 머지 + 태그
git checkout main
git merge --no-ff release/0.2.0
git tag -a v0.2.0 -m "Release v0.2.0: Performance optimization module"

# 5. develop에도 머지
git checkout develop
git merge --no-ff release/0.2.0

# 6. 태그 푸시
git push origin main --tags
```

### 5.6 버전 히스토리

| 버전 | 날짜 | 주요 변경사항 |
|------|------|---------------|
| v0.1.0 | 2025-01-17 | 초기 개발 버전: 기본 Kafka 프로토콜 구현 |
| v0.2.0 | 2025-01-17 | 성능 최적화: Buffer Pool, Object Pool, Zero-Copy I/O |

---

## 6. Code 주의사항

### 6.1 Kafka 프로토콜 호환성

```v
// ⚠️ 버전별 필드 차이 반드시 처리
pub fn parse_produce_request(version i16, data []u8) !ProduceRequest {
    mut req := ProduceRequest{}
    mut reader := new_reader(data)
    
    // v3+ 에서만 transactional_id 존재
    if version >= 3 {
        req.transactional_id = reader.read_nullable_string()!
    }
    
    req.acks = reader.read_i16()!
    req.timeout_ms = reader.read_i32()!
    
    // v0-v2: MessageSet, v3+: RecordBatch
    if version < 3 {
        req.records = reader.read_message_set()!
    } else {
        req.records = reader.read_record_batch()!
    }
    
    return req
}
```

### 6.2 Storage Engine 구현 시 주의사항

// ⚠️ 모든 Storage Engine 구현 시 확인 사항
// - 공통 인터페이스(StoragePort/StoragePlugin)만 사용
// - Memory, S3, DB, KVStore 등은 plugins/ 디렉터리에서 분리 구현
// - config.toml의 [storage] engine 값으로 런타임 선택
// - 잘못된 값/미구현 엔진은 Memory 엔진으로 fallback
// - 테스트는 tests/unit/storage/ 및 tests/integration/에서 엔진별로 분리

```v
// ⚠️ 모든 Storage Engine 구현 시 확인 사항
pub interface StorageEngine {
    // 1. 원자성 보장: append는 전체 성공 또는 전체 실패
    append(topic string, partition int, records []Record) !AppendResult
    
    // 2. 순서 보장: 같은 파티션 내 메시지는 순서 유지
    fetch(topic string, partition int, offset i64, max_bytes int) !FetchResult
    
    // 3. 동시성 제어: 멀티 브로커 환경 고려
    acquire_partition_lock(topic string, partition int) !Lock
}

// ⚠️ 동시성 제어 방식 (Storage별)
// - Memory: Mutex + Atomic CAS
// - S3: Conditional Writes (ETag)
// - PostgreSQL: Transaction + Row Lock
// - SQLite: WAL + File Lock
```

### 6.3 메모리 관리

```v
// ⚠️ 대용량 버퍼 처리 시 pool 사용
import datacore.optimize

fn handle_fetch() ![]u8 {
    // ❌ 매번 새 버퍼 할당
    // buf := []u8{len: 1024 * 1024}
    
    // ✅ 버퍼 풀에서 가져오기
    buf := optimize.buffer_pool.get(1024 * 1024)
    defer { optimize.buffer_pool.put(buf) }
    
    // 버퍼 사용
    return buf
}
```

### 6.4 Consumer Group 상태 관리

```v
// ⚠️ Consumer Group 상태 전이 규칙
// Empty → PreparingRebalance → CompletingRebalance → Stable
//                    ↑                    │
//                    └────────────────────┘ (멤버 변경 시)

fn (mut g ConsumerGroup) transition_to(new_state GroupState) ! {
    // 유효한 전이인지 검증
    valid_transitions := {
        GroupState.empty: [GroupState.preparing_rebalance]
        GroupState.preparing_rebalance: [GroupState.completing_rebalance, GroupState.empty]
        GroupState.completing_rebalance: [GroupState.stable, GroupState.preparing_rebalance]
        GroupState.stable: [GroupState.preparing_rebalance, GroupState.empty]
    }
    
    if new_state !in valid_transitions[g.state] {
        return error('invalid state transition: ${g.state} → ${new_state}')
    }
    
    g.state = new_state
}
```

### 6.5 에러 코드 매핑

```v
// ⚠️ 모든 에러는 Kafka 에러 코드로 변환
pub fn to_kafka_error(err IError) ErrorCode {
    msg := err.msg()
    
    return match true {
        msg.contains('topic not found') => .unknown_topic_or_partition
        msg.contains('not leader') => .not_leader_or_follower
        msg.contains('invalid offset') => .offset_out_of_range
        msg.contains('group rebalancing') => .rebalance_in_progress
        msg.contains('authorization') => .topic_authorization_failed
        else => .unknown_server_error
    }
}
```

---

## 7. 개발 명령어

### 7.1 빌드 및 실행

```bash
# 개발 빌드
v -o datacore src/main.v

# 릴리스 빌드 (최적화)
v -prod -o datacore src/main.v

# 브로커 실행
./datacore broker start --config=config.toml

# 개발 모드 실행 (핫 리로드)
v watch run src/main.v -- broker start
```

### 7.2 테스트

```bash
# 전체 테스트
v test tests/

# 특정 모듈 테스트
v test tests/unit/protocol/

# 특정 테스트 파일
v test tests/unit/storage/memory_test.v

# 커버리지 포함
v -cov test tests/
```

### 7.3 코드 품질

```bash
# 포맷팅
v fmt -w src/

# 린트 (vet)
v vet src/

# 문서 생성
v doc -o docs/api src/
```

### 7.4 Docker

```bash
# 이미지 빌드
docker build -t datacore:latest .

# 컨테이너 실행
docker run -p 9092:9092 -v ./config.toml:/etc/datacore/config.toml datacore:latest

# Docker Compose (개발 환경)
docker-compose -f docker-compose.dev.yml up -d
```

### 7.5 성능 테스트

```bash
# 벤치마크 실행
v -prod run tests/benchmark/throughput_test.v

# 프로파일링
v -profile=profile.txt run src/main.v
v profile profile.txt
```

### 7.6 일상 개발 스크립트

```bash
#!/bin/bash
# scripts/dev.sh - 개발 시작 전 실행

# 1. 코드 포맷팅
v fmt -w src/

# 2. 린트 검사
v vet src/

# 3. 단위 테스트
v test tests/unit/

# 4. 빌드 확인
v -o /tmp/datacore src/main.v

echo "✅ All checks passed!"
```

```bash
#!/bin/bash
# scripts/pre-commit.sh - 커밋 전 실행

set -e

echo "🔍 Running pre-commit checks..."

# 포맷팅 검사
if ! v fmt -verify src/; then
    echo "❌ Format check failed. Run: v fmt -w src/"
    exit 1
fi

# 린트
v vet src/

# 유닛 테스트
v test src/

echo "✅ Pre-commit checks passed!"
```

---

## 8. AI/Task Manager 작업 가이드라인

> **중요**: AI 에이전트 및 Task Manager는 모든 Task 완료 시 이 가이드라인을 반드시 준수해야 합니다.

### 8.1 Task 완료 시 필수 작업

- Storage 엔진 플러그인 구조/Config 기반 선택/기본값 구현 시:
  - 관련 코드가 plugins/ 디렉터리로 분리되어 있는지 확인
  - main.v 등에서 config 기반 런타임 선택 분기 확인
  - Memory 엔진이 안전하게 fallback되는지 테스트
  - PRD, TRD, CONTRIBUTING.md에 정책/구현 내역 반영

1. **Task 상태 갱신**
   - Task Master에서 해당 task 상태를 `done`으로 변경
   - subtask가 있으면 모두 완료 처리
   - 관련 의존성 task 확인

2. **문서 업데이트**
   - 관련 TRD/PRD 문서에 구현 완료 표시
   - 새 API 추가 시 CONTRIBUTING.md 테스트 항목 업데이트
   - 변경사항 CHANGELOG.md에 기록
   - API 변경 시 Kafka 프로토콜 참조 문서 확인

3. **Git Commit**
   - Conventional Commits 형식 준수
   - 관련 Task ID 포함: `feat(protocol): implement InitProducerId API (Task #7)`
   - 변경된 모든 파일 포함

4. **Git Tagging (릴리스 시)**
   - 주요 기능 완료 시 pre-release 태그: `v0.1.0-alpha.N`
   - 마일스톤 완료 시 정식 태그: `v0.1.0`

### 8.2 Task 완료 체크리스트

| 항목 | 필수 | 설명 |
|-----|-----|-----|
| 코드 구현 | ✅ | 요구사항 충족 |
| 빌드 성공 | ✅ | `v .` 에러 없음 |
| 테스트 통과 | ✅ | 관련 테스트 실행 |
| Task 상태 갱신 | ✅ | Task Master `done` 처리 |
| 문서 업데이트 | ⚠️ | API 변경 시 필수 |
| Git Commit | ✅ | Conventional Commits |
| Git Tag | ⚠️ | 마일스톤 완료 시 |

### 8.3 Task 완료 명령어

Task 완료 후 실행:

```bash
#!/bin/bash
# scripts/task-complete.sh <task_id> <commit_message>

TASK_ID=$1
COMMIT_MSG=$2

# 1. 빌드 확인
echo "🔨 Building..."
cd src && v . || exit 1

# 2. 테스트 실행
echo "🧪 Running tests..."
v test . || exit 1

# 3. 변경사항 스테이징
echo "📦 Staging changes..."
git add -A

# 4. Conventional Commit
echo "📝 Committing..."
git commit -m "${COMMIT_MSG} (Task #${TASK_ID})"

# 5. Task Master 상태 갱신 (MCP 호출)
echo "✅ Task #${TASK_ID} completed!"
```

### 8.4 마일스톤 완료 시 태깅

```bash
# 버전 태그 생성
git tag -a v0.1.0-alpha.1 -m "Alpha release: Basic Kafka Protocol Support"

# 태그 푸시
git push origin v0.1.0-alpha.1
```

### 8.5 Task 우선순위 및 의존성

- P0 (필수) → P1 (중요) → P2 (향후) 순서로 진행
- 의존성이 있는 Task는 선행 Task 완료 후 진행
- 병렬 진행 가능한 Task는 동시 작업 가능

---

## 9. Kafka 프로토콜 참조

> **필수**: 모든 Kafka API 구현 시 공식 프로토콜 문서를 반드시 참조하세요.

### 9.1 공식 문서

| 문서 | URL | 용도 |
|-----|-----|-----|
| **Protocol Specification** | https://kafka.apache.org/protocol.html | API 메시지 형식, 필드 정의 |
| **Protocol API Keys** | https://kafka.apache.org/protocol.html#protocol_api_keys | API Key 번호, 버전 범위 |
| **Error Codes** | https://kafka.apache.org/protocol.html#protocol_error_codes | 에러 코드 정의 |
| **Primitive Types** | https://kafka.apache.org/protocol.html#protocol_types | INT8, VARINT, COMPACT_STRING 등 |
| **Message Format** | https://kafka.apache.org/documentation/#messageformat | RecordBatch, MessageSet 형식 |

### 9.2 주요 KIP (Kafka Improvement Proposal)

| KIP | 설명 | 구현 우선순위 |
|-----|-----|-------------|
| [KIP-482](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482) | Java API for Kafka Clients | 참조 |
| [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848) | New Consumer Rebalance Protocol | P0 |
| [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932) | Share Groups | P1 |
| [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500) | Replace ZooKeeper with KRaft | P2 |
| [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430) | Return Authorized Operations | P1 |

### 9.3 프로토콜 구현 시 필수 확인 사항

1. **버전별 필드 차이**
   - 각 API의 버전별 스키마 확인
   - 추가/삭제된 필드 처리
   - 기본값 설정

2. **Flexible Versions**
   - 특정 버전 이상에서 compact encoding 사용
   - Tagged fields 처리 필수
   - API별 flexible version 확인: `is_flexible_version()` 함수 참조

3. **Tagged Fields**
   - Flexible version에서 확장 필드 처리
   - 알 수 없는 태그는 무시 (forward compatibility)

4. **Error Code 매핑**
   - 모든 에러를 Kafka 에러 코드로 변환
   - `src/infra/protocol/kafka/types.v`의 `ErrorCode` enum 참조

### 9.4 API 구현 순서

새 API 구현 시:

```
1. 프로토콜 문서 확인
   └── https://kafka.apache.org/protocol.html 에서 해당 API 검색

2. Request 구조체 정의 (request.v)
   ├── 버전별 필드 확인
   └── 파싱 함수 구현

3. Response 구조체 정의 (response.v)
   ├── 버전별 필드 확인
   └── 인코딩 함수 구현

4. Handler 구현 (handler.v)
   ├── 비즈니스 로직
   └── Storage 연동

5. types.v 업데이트
   ├── API Key enum 추가 (없는 경우)
   └── ApiVersionRange 추가

6. 테스트 작성
   └── 단위 테스트 + 통합 테스트
```

### 9.5 자주 사용하는 프로토콜 참조

```v
// API Key 확인
// https://kafka.apache.org/protocol.html#protocol_api_keys

// Produce API (Key 0)
// https://kafka.apache.org/protocol.html#The_Messages_Produce

// Fetch API (Key 1)
// https://kafka.apache.org/protocol.html#The_Messages_Fetch

// Metadata API (Key 3)
// https://kafka.apache.org/protocol.html#The_Messages_Metadata

// InitProducerId API (Key 22)
// https://kafka.apache.org/protocol.html#The_Messages_InitProducerId
```

---

## 10. 통합 테스트 (Integration Tests)

- Storage 엔진별로 config.toml의 engine 값을 바꿔가며 정상 동작 확인
- Memory, S3 등 플러그인별 단위/통합 테스트를 tests/unit/storage/, tests/integration/에 추가
- 잘못된 engine 값 입력 시 Memory 엔진으로 fallback되는지 검증

### 8.1 테스트 도구 요구사항

```bash
# Kafka CLI 설치 (macOS)
brew install kafka

# Kafka CLI 설치 (Linux)
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
export PATH=$PATH:$(pwd)/kafka_2.13-3.6.0/bin

# Avro 도구 (선택사항 - Confluent CLI)
curl -sL https://cnfl.io/cli | sh -s -- latest
```

### 8.2 통합 테스트 실행

```bash
# 1. DataCore 브로커 실행
cd src && v run . broker start

# 2. 전체 통합 테스트 실행
cd tests/integration
bash run_all_tests.sh

# 3. 개별 테스트 실행
bash test_kafka_cli.sh           # Kafka CLI 테스트
bash test_schema_registry.sh     # Schema Registry 테스트
```

### 8.3 Kafka CLI 테스트 항목

| 테스트 | CLI 명령어 | 검증 내용 |
|--------|-----------|----------|
| 토픽 생성 | `kafka-topics --create` | CreateTopics API |
| 토픽 조회 | `kafka-topics --list` | Metadata API |
| 토픽 삭제 | `kafka-topics --delete` | DeleteTopics API |
| 메시지 전송 | `kafka-console-producer` | Produce API |
| 메시지 수신 | `kafka-console-consumer` | Fetch API |
| Avro 전송 | `kafka-avro-console-producer` | Schema Registry + Produce |
| Avro 수신 | `kafka-avro-console-consumer` | Schema Registry + Fetch |

### 8.4 Schema Registry 테스트 항목

| 테스트 | 엔드포인트 | HTTP 메서드 |
|--------|-----------|------------|
| 스키마 등록 | `/subjects/{subject}/versions` | POST |
| 스키마 조회 | `/schemas/ids/{id}` | GET |
| Subject 목록 | `/subjects` | GET |
| 버전 목록 | `/subjects/{subject}/versions` | GET |
| 호환성 설정 | `/config/{subject}` | GET/PUT |
| 호환성 검사 | `/compatibility/subjects/{subject}/versions/{version}` | POST |

### 8.5 환경 변수

```bash
# Kafka 브로커 주소
export BOOTSTRAP_SERVER=localhost:9092

# Schema Registry URL
export SCHEMA_REGISTRY_URL=http://localhost:8081

# Consumer 타임아웃 (초)
export TIMEOUT=10
```

### 8.6 수동 테스트 예제

```bash
# 토픽 생성
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic test-topic --partitions 3

# 메시지 전송
echo "Hello DataCore" | kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic test-topic

# 메시지 수신
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic test-topic --from-beginning --max-messages 1

# Schema Registry 스키마 등록
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
    http://localhost:8081/subjects/user-value/versions
```

---

## 참고 자료

### 개발 가이드
- [V 언어 공식 문서](https://docs.vlang.io/introduction.html)
- [Google Go 스타일 가이드](https://google.github.io/styleguide/go/)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)

### Kafka 프로토콜
- [Kafka Protocol Specification](https://kafka.apache.org/protocol.html) - **필수 참조**
- [Kafka Protocol API Keys](https://kafka.apache.org/protocol.html#protocol_api_keys)
- [Kafka Protocol Error Codes](https://kafka.apache.org/protocol.html#protocol_error_codes)
- [Kafka Protocol Primitive Types](https://kafka.apache.org/protocol.html#protocol_types)
- [Kafka Message Format](https://kafka.apache.org/documentation/#messageformat)

### Kafka Improvement Proposals (KIPs)
- [Kafka KIPs Index](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
- [KIP-848: New Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848)
- [KIP-932: Share Groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932)

---

> **Note**: 이 문서는 프로젝트 진행에 따라 업데이트됩니다.
> 최종 수정: 2026-01-17
