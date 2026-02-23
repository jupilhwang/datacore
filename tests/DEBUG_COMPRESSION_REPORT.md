## Quality Report

### Summary
Kafka CLI 압축 테스트 실패 원인을 디버깅했습니다. **두 가지 독립적인 문제**가 발견되었습니다:

### Code Review

#### 문제 1: Kafka CLI 옵션 이름 오류 (CRITICAL)
**파일 영향**: 
- `tests/integration/test_kafka_compat.sh`
- `tests/integration/test_storage_types.sh`
- `tests/integration/test_message_formats.sh`
- `tests/manual_compression_test.sh` (직접 사용 아님, 참고용)

**설명**:
- 잘못된 옵션: `--compression-type`
- 올바른 옵션: `--compression-codec`

**증거**:
```bash
$ kafka-console-producer --compression-type snappy
compression-type is not a recognized option
Option: --compression-codec [String: compression-codec]
       The compression codec: either 'none', 'gzip', 'snappy', 'lz4', or 'zstd'.
```

**문제 분류**: **CRITICAL** - 모든 압축 테스트가 실패하는 원인

#### 문제 2: RecordBatch 압축 해제 후 파싱 실패 (CRITICAL)
**파일 영향**: `src/infra/protocol/kafka/handler_produce.v`

**설명**:
압축된 RecordBatch를 압축 해제한 후, 중첩 RecordBatch(Inner RecordBatch)를 올바르게 파싱하지 못합니다.

**문제 코드** (line 517-518):
```v
data_to_parse := if was_compressed { decompressed_data } else { records_to_parse }
parsed := parse_record_batch(data_to_parse) or {
```

**기술적 세부사항**:
1. `parse_record_batch`는 RecordBatch 헤더(base_offset + batch_length, 총 12바이트)를 기대
2. 압축 해제된 데이터는 중첩 RecordBatch이므로:
   - 첫 12바이트: `[last_offset_delta(4) | batch_length(4) | partition_leader_epoch(4)]`
   - 이를 base_offset/batch_length로 읽으면 잘못된 파싱 결과

**증거** (브로커 로그):
```json
{"level":"DEBUG","msg":"Decompression successful",
  "compressed_size":"49","decompressed_size":"29",
  "decompressed_start":"38000000012c54657374206d657373616765207769746820677a69700"}
// hex 분석:
// 38000000 = last_offset_delta (56)
// 01000000 = batch_length (1) - 잘못됨! 실제는 44바이트
// 12c5465... = 나머지 데이터
```

**문제 분류**: **CRITICAL** - 압축된 메시지를 처리하지 못함

#### 문제 3: field_hex 누락 (HIGH)
**파일 영향**: `src/infra/protocol/kafka/handler_produce.v`, `src/infra/compression/zstd_c.v`

**설명**: `observability.field_hex()` 함수가 존재하지 않습니다. `field_string()`으로 대체 필요.

**상태**: ✅ **수정 완료** - 이미 `field_string()`로 수정됨

### Testing

#### 커버리지:
- 수동 압축 테스트: ✅ 통과 (3/4 - Snappy, Gzip, Zstd 작동, LZ4 실패)
- Kafka CLI 압축 테스트: ❌ 실패 (옵션 오류 및 파싱 문제)

#### 테스트 결과:
1. **수동 압축 테스트** (`tests/manual_compression_test.sh`):
   - Snappy: ✅ PASS
   - Gzip: ✅ PASS
   - LZ4: ❌ FAIL (알려진 문제)
   - Zstd: ✅ PASS

2. **Kafka CLI 압축 테스트** (`tests/debug_compression.sh`):
   - Plain (uncompressed): ✅ PASS
   - Snappy: ❌ FAIL
   - Gzip: ❌ FAIL
   - LZ4: ❌ FAIL
   - Zstd: ❌ FAIL

### Overall: **REQUEST CHANGES**

### Blocking Issues

| Priority | Issue | File | Description |
|----------|-------|------|-------------|
| **CRITICAL** | Kafka CLI 옵션 오류 | `test_kafka_compat.sh` 등 | `--compression-type` → `--compression-codec` |
| **CRITICAL** | RecordBatch 파싱 실패 | `handler_produce.v:517-518` | 중첩 RecordBatch를 올바르게 파싱하지 않음 |
| **HIGH** | LZ4 압축 실패 | `compression/lz4.v` 또는 `lz4_c.v` | 수동 테스트에서도 실패 (별도 문제) |

### Recommendations

#### 1. 즉시 수정 필요 (CRITICAL)

**수정 1: Kafka CLI 옵션 변경**
```bash
# 수정 전
--compression-type snappy

# 수정 후
--compression-codec snappy
```

**수정 2: RecordBatch 중첩 파싱 구현**

`src/infra/protocol/kafka/codec.v`에 중첩 RecordBatch 파싱 함수 추가:

```v
// 중첩 RecordBatch (압축 해제 후) 파싱
fn parse_nested_record_batch(data []u8) !ParsedRecordBatch {
    if data.len < 20 {
        return error('nested record batch too small')
    }

    mut reader := new_reader(data)

    // 중첩 RecordBatch 헤더
    last_offset_delta := reader.read_i32()!  // 상대 오프셋
    batch_length := reader.read_i32()!
    partition_leader_epoch := reader.read_i32()!
    magic := reader.read_i8()!

    if magic != 2 {
        return error('nested record batch has invalid magic')
    }

    // CRC 검증 (선택사항 - Kafka는 중첩 RecordBatch CRC를 검증하지 않음)

    // 나머지 헤더 필드
    attributes := reader.read_i16()!
    first_timestamp := reader.read_i64()!
    max_timestamp := reader.read_i64()!
    producer_id := reader.read_i64()!
    producer_epoch := reader.read_i16()!
    base_sequence := reader.read_i32()!
    record_count := reader.read_i32()!

    // 레코드 파싱
    mut records := []domain.Record{cap: int(record_count)}
    for _ in 0 .. record_count {
        record := parse_record(mut reader, first_timestamp) or { break }
        records << record
    }

    // base_offset은 외부 RecordBatch에서 옴 (0으로 초기화)
    return ParsedRecordBatch{
        base_offset:            0  // 외부 RecordBatch의 base_offset 사용
        partition_leader_epoch: partition_leader_epoch
        magic:                  magic
        attributes:             attributes
        last_offset_delta:      last_offset_delta
        first_timestamp:        first_timestamp
        max_timestamp:          max_timestamp
        producer_id:            producer_id
        producer_epoch:         producer_epoch
        base_sequence:          base_sequence
        records:                records
    }
}
```

`src/infra/protocol/kafka/handler_produce.v` line 517-518 수정:
```v
// 수정 전
data_to_parse := if was_compressed { decompressed_data } else { records_to_parse }
parsed := parse_record_batch(data_to_parse) or {

// 수정 후
if was_compressed {
    // 중첩 RecordBatch 파싱 (압축 해제 후)
    parsed := parse_nested_record_batch(decompressed_data) or {
        partitions << ProduceResponsePartition{
            index:      p.index
            error_code: i16(ErrorCode.corrupt_message)
            base_offset: -1
            log_append_time: -1
            log_start_offset: -1
        }
        continue
    }
    // 외부 RecordBatch의 base_offset 사용
    result := parsed
    result.base_offset = base_offset
} else {
    // 일반 RecordBatch 파싱
    parsed := parse_record_batch(records_to_parse) or {
```

#### 2. 추가 디버깅 필요

**Snappy 압축 상태 1 오류**:
```
ERROR: "snappy decompression failed with status: 1"
```
- Kafka CLI가 보낸 Snappy 압축 데이터 형식 확인 필요
- `src/infra/compression/snappy.v` 또는 `snappy_c.v` 검토

#### 3. 테스트 개선

`tests/debug_compression.sh` 생성:
- Kafka CLI 옵션 사용 (`--compression-codec`)
- hex dump로 압축 데이터 비교
- 브로커 로그와 함께 출력

---

## 결론

**ROOT CAUSE 요약:**
1. **Kafka CLI 옵션 오류**: `--compression-type` 대신 `--compression-codec` 사용
2. **RecordBatch 중첩 파싱 미구현**: 압축 해제 후 중첩 RecordBatch를 올바르게 파싱하지 않음

**다음 단계:**
1. 모든 테스트 파일에서 `--compression-type` → `--compression-codec` 변경
2. `parse_nested_record_batch()` 함수 구현
3. `handler_produce.v`에서 중첩 RecordBatch 파싱 로직 추가
4. Snappy 압축 형식 호환성 검토
