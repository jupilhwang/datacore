# Lessons Learned

<!-- Entry format:
## ErrorPattern_YYYY-MM-DD_<short-description>
- **Trigger**: When this mistake occurs
- **Mistake**: What went wrong
- **Rule**: Prevention rule going forward
- **Project**: Project name or 'global'
-->

## ErrorPattern_2026-02-28_kafka-recordbatch-header-size

- **Trigger**: RecordBatch를 파싱할 때 헤더 오프셋 계산
- **Mistake**: `header_size = 65` 잘못된 값 사용. records_count를 헤더 크기에 포함했거나 잘못 계산함.
- **Rule**: Kafka RecordBatch 헤더는 정확히 61바이트: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) + baseTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) + recordCount(4) = 61. records_count는 헤더 이후 명시적으로 read해야 함.
- **Project**: DataCore

## ErrorPattern_2026-02-28_kafka-snappy-xerial-format

- **Trigger**: Snappy 압축 해제 시 Confluent/Java Kafka 클라이언트가 생성한 데이터 처리
- **Mistake**: 표준 snappy 형식만 지원하고 xerial snappy-java format을 미지원
- **Rule**: Kafka 클라이언트(특히 Confluent/Java)는 xerial snappy-java format 사용. magic bytes: PPY\0 (0x50 0x50 0x59 0x00) + version 1 + compat version 1 구조. 이후 4바이트 BE chunk_size → snappy decompress 청크 루프 처리. 새 형식(일반 snappy stream)도 함께 지원해야 함.
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
