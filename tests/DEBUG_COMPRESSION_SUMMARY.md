## Debug Results Summary

### Root Cause 분석

**문제 1: Kafka CLI 옵션 오류** ✅ **수정 완료**
- 잘못된 옵션: `--compression-type`
- 올바른 옵션: `--compression-codec`
- 수정 파일: 
  - `tests/integration/test_kafka_compat.sh`
  - `tests/integration/test_storage_types.sh`
  - `tests/integration/test_message_formats.sh`

**문제 2: RecordBatch 중첩 파싱 미구현** ✅ **부분 수정**
- `parse_nested_record_batch()` 함수 추가: `src/infra/protocol/kafka/codec.v`
- `ParsedRecordBatch`를 mutable로 변경
- `handler_produce.v`에서 중첩 RecordBatch 파싱 로직 추가

**문제 3: Kafka 압축 RecordBatch 구조 오해** ⚠️ **추가 수정 필요**

### 현재 문제 상황

압축 해제된 데이터가 잘못된 형식으로 파싱되고 있습니다:

```json
{
  "error": "nested record batch has invalid magic: 101"
}
```

**압축 해제된 데이터:**
```
38000000 012c 54657374206d657373616765207769746820677a69700
```

이 데이터는 **내부 RecordBatch의 헤더 + 레코드**가 아니라, **압축되기 전 원래 레코드 데이터**입니다.

### Kafka RecordBatch 압축 구조

```
┌────────────────────────────────────────────────────────────────┐
│ Outer RecordBatch (61 bytes header)                        │
├────────────────────────────────────────────────────────────────┤
│ CRC32-C (4 bytes) [헤더 57바이트에 대한 CRC]          │
├────────────────────────────────────────────────────────────────┤
│ Attributes (2 bytes): 0x01 (gzip)                        │
│ Last Offset Delta (4 bytes)                                │
│ First Timestamp (8 bytes)                                   │
│ Max Timestamp (8 bytes)                                     │
│ Producer ID (8 bytes)                                      │
│ Producer Epoch (2 bytes)                                   │
│ Base Sequence (4 bytes)                                     │
│ Record Count (4 bytes)                                      │
├────────────────────────────────────────────────────────────────┤
│ Records Data (압축됨 - gzip/snappy/lz4/zstd)          │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ Inner RecordBatch (중첩, 압축됨)                  │ │
│ │ ┌────────────────────────────────────────────────────┐ │ │
│ │ │ Inner RecordBatch Header (21 bytes)            │ │ │
│ │ │ - last_offset_delta                           │ │ │
│ │ │ - batch_length                                │ │ │
│ │ │ - partition_leader_epoch                       │ │ │
│ │ │ - magic (2)                                  │ │ │
│ │ │ - CRC (4 bytes)                              │ │ │
│ │ ├────────────────────────────────────────────────────┤ │ │
│ │ │ Attributes (2 bytes)                          │ │ │
│ │ │ Timestamps (16 bytes)                        │ │ │
│ │ │ Producer ID, Epoch, Sequence                  │ │ │
│ │ │ Record Count (4 bytes)                        │ │ │
│ │ ├────────────────────────────────────────────────────┤ │ │
│ │ │ Records: "Test message with gzip"             │ │ │
│ │ └────────────────────────────────────────────────────┘ │ │
│ └──────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

### 올바른 압축 해제 로직

**현재 잘못된 방식:**
```v
// handler_produce.v line 406
header_size := 61
compressed_data := records_to_parse[header_size..]  // CRC 포함!

// 전체 compressed_data를 압축 해제 (잘못됨!)
decompressed_data = h.compression_service.decompress(compressed_data, compression_type)!
```

**올바른 방식:**
```v
// Kafka RecordBatch 압축 구조:
// 1. Outer RecordBatch Header (61 bytes)
// 2. CRC32-C (4 bytes)
// 3. Inner RecordBatch Header (21 bytes)
// 4. Inner CRC32-C (4 bytes)
// 5. Attributes, Timestamps, Producer Info (32 bytes)
// 6. Records (압축됨)

// 압축되는 부분은 Records 데이터만!
header_size := 61
crc_size := 4
inner_header_size := 21
inner_crc_size := 4
producer_info_size := 32
records_start_offset := header_size + crc_size + inner_header_size + inner_crc_size + producer_info_size

compressed_records_only := records_to_parse[records_start_offset..]

// 압축 해제
decompressed_records = h.compression_service.decompress(compressed_records_only, compression_type)!
```

### 해결 방법

`src/infra/protocol/kafka/handler_produce.v` line 404-431 수정:

```v
// Kafka 압축 RecordBatch 구조에 따른 정확한 오프셋 계산
header_size := 61
crc_size := 4
inner_header_size := 21 // last_offset_delta(4) + batch_length(4) + partition_leader_epoch(4) + magic(1) + CRC(4) + attributes(2) + first_timestamp(8) + max_timestamp(8) + producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4)
inner_crc_size := 4

// Records 데이터 시작 위치
records_start_offset := header_size + crc_size + inner_header_size + inner_crc_size + 32 // 32 = attributes(2) + timestamps(16) + producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4)

if records_start_offset >= records_to_parse.len {
    h.logger.error('Compressed data too small for records', observability.field_int('buffer_size',
        records_to_parse.len), observability.field_int('records_start', records_start_offset))
    partitions << ProduceResponsePartition{
        index:      p.index
        error_code: i16(ErrorCode.corrupt_message)
        base_offset: -1
        log_append_time: -1
        log_start_offset: -1
    }
    continue
}

compressed_records_only := records_to_parse[records_start_offset..]

// 압축 해제
decompressed_data = h.compression_service.decompress(compressed_records_only, compression_type) or {
    // ... 에러 처리
}

// 압축 해제된 데이터: 이제 올바른 레코드 데이터임
```

### 추가 문제

**Snappy 압축 실패:**
```
ERROR: "snappy decompression failed with status: 1"
```
- 원인: Kafka CLI가 보낸 Snappy 데이터 형식 확인 필요
- `src/infra/compression/snappy.v` 또는 `snappy_c.v` 검토 필요

### 다음 단계

1. **Kafka 압축 RecordBatch 구조 수정** (CRITICAL)
   - `handler_produce.v` line 404-431 수정
   - 올바른 오프셋으로 압축된 레코드만 추출

2. **압축 해제 후 레코드 파싱 수정** (CRITICAL)
   - 압축 해제된 데이터는 이미 레코드 형식
   - RecordBatch 헤더가 없는 **순수 레코드 배열**

3. **Snappy 압축 형식 확인** (HIGH)
   - Kafka CLI가 보낸 Snappy 데이터 확인
   - hex dump로 형식 분석

### 테스트 상태

| 테스트 | 상태 | 설명 |
|--------|------|------|
| 수동 압축 테스트 | ⚠️ 3/4 통과 | Snappy, Gzip, Zstd 작동, LZ4 실패 |
| Kafka CLI 옵션 | ✅ 수정 완료 | `--compression-type` → `--compression-codec` |
| Kafka CLI 압축 테스트 | ❌ 실패 | RecordBatch 구조 오해 |

### 결론

**ROOT CAUSE:**
1. Kafka CLI 옵션 이름 오류 (`--compression-type` 대신 `--compression-codec`)
2. Kafka 압축 RecordBatch 구조 오해 (헤더 포함 전체 압축 해제 시도)

**해결 필요:**
1. 올바른 오프셋으로 압축된 레코드만 추출
2. 압축 해제된 레코드 올바르게 파싱 (RecordBatch 헤더 없음)
3. Snappy 압축 형식 호환성 확인
