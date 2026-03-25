# Lessons Learned

<!-- Entry format:
## ErrorPattern_YYYY-MM-DD_<short-description>
- **Trigger**: When this mistake occurs
- **Mistake**: What went wrong
- **Rule**: Prevention rule going forward
- **Project**: Project name or 'global'
-->

## ErrorPattern_2026-03-25_domain-import-contamination

- **Trigger**: 아키텍처 경계를 넘는 중복 코드를 제거할 때
- **Mistake**: domain/streaming.v의 `escape_json_str`을 `common.escape_json_string`으로 교체하면서 domain 레이어에 `import common`을 도입. Clean Architecture의 domain 순수성 위반.
- **Rule**: 중복 제거 시 import 방향이 Clean Architecture를 위반하지 않는지 반드시 검증. domain 레이어에서 외부 모듈 import가 필요하면, domain-private 인라인 헬퍼를 생성하여 domain 순수성 유지. 변경 후 `grep -r "import" domain/` 으로 외부 의존성 확인 필수.
- **Project**: DataCore

## ErrorPattern_2026-03-25_stateless-todo-revaluation

- **Trigger**: 코드베이스 건강성 감사에서 TODO/FIXME의 심각도를 평가할 때
- **Mistake**: 리밸런싱 관련 TODO를 CRITICAL로 분류했으나, 실제로는 stateless broker 설계 전환과 KIP-848 consumer group 프로토콜로 이미 해결된 문제였음.
- **Rule**: TODO 심각도를 평가할 때 반드시 현재 아키텍처 결정사항을 기준으로 재평가. 과거 설계의 잔재(vestigial code)는 아키텍처 전환으로 불필요해졌을 수 있음. 현재 설계 문서와 대조하여 실제 영향도를 판단할 것.
- **Project**: DataCore

## ErrorPattern_2026-03-25_vlang-or-empty-distinction

- **Trigger**: V 언어 `or {}` 패턴을 일괄 감사할 때
- **Mistake**: 169개 `or {}` 패턴을 모두 에러 삼키기로 간주하려 했으나, 92개는 cleanup 코드(`conn.close() or {}`)로 idiomatic V 패턴이었음. 실제 에러 삼키기는 17개뿐.
- **Rule**: V 언어 `or {}` 패턴은 두 가지로 구분: (1) Cleanup 코드 (close/defer/cleanup 함수) = 허용, (2) 비즈니스 로직 (상태 저장, 할당, 쓰기) = 반드시 수정. 감사 시 함수 컨텍스트를 확인하여 cleanup과 business logic을 분리할 것.
- **Project**: DataCore

## ErrorPattern_2026-03-25_thin-wrapper-dedup

- **Trigger**: 많은 외부 호출자를 가진 모듈의 중복 코드를 제거할 때
- **Mistake**: 276+ 호출자가 있는 infra/performance/core/utils.v의 함수들을 직접 삭제하고 import를 일괄 변경하려 했음. 대규모 변경은 regression 위험이 높음.
- **Rule**: 외부 호출자가 많은 모듈의 중복 제거 시, thin wrapper 패턴 사용. 기존 함수 시그니처를 유지하면서 내부 구현만 canonical 구현체(common.*)로 위임. API 안정성을 보장하면서 구현 중복을 제거. 호출자 수가 50+ 이면 wrapper 우선 고려.
- **Project**: DataCore

## ErrorPattern_2026-03-25_check-regression-detection

- **Trigger**: 다중 병렬 작업(G1: 4 tasks)이 각각 테스트를 통과했지만 통합 후 아키텍처 위반이 존재할 때
- **Mistake**: 개별 task 레벨 테스트 통과를 전체 품질 보장으로 오인. G1-Task D에서 도입된 domain 오염이 개별 테스트에서는 미검출.
- **Rule**: 병렬 작업 통합 후 반드시 전체 CHECK 수행. 개별 task 테스트 통과 != 전체 아키텍처 무결성. CHECK에서 아키텍처 제약 조건(domain 순수성, 의존성 방향, SOLID 원칙)을 자동 검증하여 개별 작업 간 간섭을 탐지할 것.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-recordbatch-header-size

- **Trigger**: RecordBatch를 파싱할 때 헤더 오프셋 계산
- **Mistake**: `header_size = 65` 잘못된 값 사용. records_count를 헤더 크기에 포함했거나 잘못 계산함.
- **Rule**: Kafka RecordBatch 헤더는 정확히 61바이트: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) + recordCount(4) = 61. records_count는 헤더 이후 명시적으로 read해야 함.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-snappy-xerial-format

- **Trigger**: Snappy 압축 해제 시 Confluent/Java Kafka 클라이언트가 생성한 데이터 처리
- **Mistake**: 표준 snappy 형식만 지원하고 xerial snappy-java format을 미지원
- **Rule**: Kafka 클라이언트(특히 Confluent/Java)는 xerial snappy-java format 사용. magic bytes: PPY\0 (0x50 0x50 0x59 0x00) + version 1 + compat version 1 구조. 이후 4바이트 BE chunk_size -> snappy decompress 청크 루프 처리. 새 형식(일반 snappy stream)도 함께 지원해야 함.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-nullable-compact-array

- **Trigger**: nullable compact array를 응답에 쓸 때
- **Mistake**: 빈 배열(0x01)과 null(0x00)을 혼용하거나 null을 빈 배열로 잘못 인코딩
- **Rule**: Kafka nullable compact array 인코딩 규칙: null = 0x00 (`write_compact_array_len(-1)`), 빈 배열 = 0x01 (`write_compact_array_len(0)`), N개 원소 = N+1 (`write_compact_array_len(N)`). null과 빈 배열은 의미가 다름.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-nullable-struct-encoding

- **Trigger**: nullable struct(cursor 등)을 응답에 쓸 때
- **Mistake**: uvarint 또는 compact array 길이 방식으로 nullable struct presence byte를 인코딩
- **Rule**: Kafka nullable struct는 `write_i8(-1)` = 0xFF (null 표시), `write_i8(1)` = 0x01 (present 표시) 컨벤션 사용. compact array 길이 인코딩과 혼동하지 말 것.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-cursor-parsing

- **Trigger**: DescribeTopicPartitions request의 cursor 필드 파싱
- **Mistake**: uvarint로 cursor presence byte를 읽어 잘못된 값 해석
- **Rule**: cursor 파싱은 `read_i8()` 사용. -1(0xFF)이면 null cursor, 그 외 값이면 present cursor로 처리.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-fetch-recompression

- **Trigger**: Fetch handler에서 `compress_records_for_fetch()` 호출
- **Mistake**: RecordBatch 전체(헤더 포함)를 Fetch 응답 시 재압축하여 Consumer 파싱 오류 및 타임아웃 발생
- **Rule**: Fetch 응답 시 이미 저장된 RecordBatch를 그대로 반환해야 함. `compress_records_for_fetch`는 produce 경로에서만 사용. Fetch 경로에서는 재압축 금지, compression_type을 `.none`으로 고정.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-compression-length-prefix

- **Trigger**: gzip, lz4 압축 해제 시 Kafka가 생성한 데이터 처리
- **Mistake**: Kafka 특유의 4바이트 big-endian length prefix를 처리하지 않고 압축 해제 시도
- **Rule**: Kafka는 gzip/lz4 압축 데이터 앞에 4바이트 big-endian length prefix를 추가. 해제 전 `has_kafka_gzip_prefix()` / `has_kafka_lz4_prefix()` 로 확인 후 prefix 4바이트를 skip하고 실제 압축 데이터 전달.
- **Project**: DataCore

## ErrorPattern_2026-02-28_zstd-content-size-overflow

- **Trigger**: zstd 압축 해제 시 `ZSTD_getFrameContentSize()` 반환값을 int로 변환
- **Mistake**: u64 content_size를 int로 직접 캐스팅하여 ZSTD_CONTENTSIZE_UNKNOWN (u64 max = 0xFFFFFFFFFFFFFFFF) 처리 시 오버플로우 크래시
- **Rule**: `ZSTD_CONTENTSIZE_UNKNOWN` (u64 max)인 경우 streaming 분기로 처리. `content_size > max_int`이면 오버플로우 가드 적용. u64를 int로 직접 캐스팅하지 말 것.
- **Project**: DataCore
