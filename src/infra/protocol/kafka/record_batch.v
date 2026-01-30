// Kafka 프로토콜 - RecordBatch 인코딩
// RecordBatch 형식 인코딩 및 CRC32-C 체크섬 계산 제공
module kafka

import domain
import infra.protocol.kafka.crc32c

// 헬퍼 함수

/// varint_size는 varint의 인코딩된 크기를 계산합니다.
/// 정확성을 위해 codec.v의 write_varint와 동일한 ZigZag 인코딩을 사용합니다.
fn varint_size(val i64) int {
	// ZigZag 인코딩: (n << 1) ^ (n >> 63)
	// 부호 있는 시프트 경고를 피하기 위해 먼저 unsigned로 캐스팅
	v := (u64(val) << 1) ^ u64(val >> 63)
	mut size := 0
	mut value := v
	for {
		size++
		value >>= 7
		if value == 0 {
			break
		}
	}
	return size
}

/// calculate_record_size는 실제로 인코딩하지 않고 인코딩된 레코드의 크기를 계산합니다.
fn calculate_record_size(timestamp_delta i64, offset_delta i32, record &domain.Record) int {
	mut size := 0

	// attributes (1 byte)
	size += 1

	// timestamp_delta (varint)
	size += varint_size(timestamp_delta)

	// offset_delta (varint)
	size += varint_size(i64(offset_delta))

	// key length + key
	if record.key.len > 0 {
		size += varint_size(i64(record.key.len))
		size += record.key.len
	} else {
		size += varint_size(-1)
	}

	// value length + value
	if record.value.len > 0 {
		size += varint_size(i64(record.value.len))
		size += record.value.len
	} else {
		size += varint_size(-1)
	}

	// headers count (varint, 0 for no headers)
	size += varint_size(0)

	return size
}

// RecordBatch 인코딩

/// encode_record_batch_zerocopy는 레코드를 Kafka RecordBatch 형식으로 인코딩합니다.
/// RecordBatch 형식 (v2):
/// - Base Offset: i64
/// - Batch Length: i32
/// - Partition Leader Epoch: i32
/// - Magic: i8 (v2의 경우 2)
/// - CRC: u32 (CRC 필드 이후 데이터의 CRC32-C)
/// - Attributes: i16
/// - Last Offset Delta: i32
/// - First Timestamp: i64
/// - Max Timestamp: i64
/// - Producer ID: i64
/// - Producer Epoch: i16
/// - Base Sequence: i32
/// - Records Count: i32
/// - Records: 가변
pub fn encode_record_batch_zerocopy(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	// 최적화된 인코딩 경로 사용 (미리 할당된 버퍼)
	estimated_size := estimate_batch_size(records)

	// RecordBatch 헤더 - 미리 할당된 writer 사용
	mut writer := new_writer_with_capacity(estimated_size)

	// 기본 오프셋 (8바이트)
	writer.write_i64(base_offset)

	// 배치 길이 플레이스홀더 (4바이트) - 나중에 채움
	_ = writer.data.len
	writer.write_i32(0)

	// 파티션 리더 에포크 (4바이트)
	writer.write_i32(-1)

	// 매직 바이트 (1바이트) - 버전 2
	writer.write_i8(2)

	// CRC 플레이스홀더 (4바이트)
	crc_pos := writer.data.len
	writer.write_i32(0)

	// 속성 (2바이트) - 압축 없음, 타임스탬프 없음
	writer.write_i16(0)

	// 마지막 오프셋 델타
	writer.write_i32(i32(records.len - 1))

	// 첫 번째 타임스탬프 (첫 번째 레코드 사용)
	first_timestamp := if records.len > 0 { records[0].timestamp.unix_milli() } else { i64(0) }
	writer.write_i64(first_timestamp)

	// 최대 타임스탬프 (마지막 레코드 사용)
	max_timestamp := if records.len > 0 {
		records[records.len - 1].timestamp.unix_milli()
	} else {
		first_timestamp
	}
	writer.write_i64(max_timestamp)

	// 프로듀서 ID (비멱등성의 경우 -1)
	writer.write_i64(-1)

	// 프로듀서 에포크 (비멱등성의 경우 -1)
	writer.write_i16(-1)

	// 기본 시퀀스 (비멱등성의 경우 -1)
	writer.write_i32(-1)

	// 레코드 개수
	writer.write_i32(i32(records.len))

	// 레코드 인코딩
	for i, record in records {
		offset_delta := i32(i)
		timestamp_delta := record.timestamp.unix_milli() - first_timestamp

		// 전체 인코딩 없이 레코드 크기 계산
		record_size := calculate_record_size(timestamp_delta, offset_delta, record)

		// 레코드를 한 번만 작성
		writer.write_varint(i64(record_size))
		writer.write_i8(0) // 속성
		writer.write_varint(timestamp_delta)
		writer.write_varint(i64(offset_delta))

		// 키
		if record.key.len > 0 {
			writer.write_varint(i64(record.key.len))
			writer.write_raw(record.key)
		} else {
			writer.write_varint(-1)
		}

		// 값
		if record.value.len > 0 {
			writer.write_varint(i64(record.value.len))
			writer.write_raw(record.value)
		} else {
			writer.write_varint(-1)
		}

		// 헤더 (없음)
		writer.write_varint(0)
	}

	// 최종 데이터 획득
	mut batch_data := writer.bytes()

	// 배치 길이 계산 및 채우기 (전체 - base_offset - batch_length_field)
	batch_length := batch_data.len - 12
	batch_data[8] = u8(batch_length >> 24)
	batch_data[9] = u8(batch_length >> 16)
	batch_data[10] = u8(batch_length >> 8)
	batch_data[11] = u8(batch_length)

	// 배치의 CRC32c 계산 (속성부터 끝까지)
	crc := calculate_crc32c(batch_data[crc_pos + 4..])
	batch_data[crc_pos] = u8(crc >> 24)
	batch_data[crc_pos + 1] = u8(crc >> 16)
	batch_data[crc_pos + 2] = u8(crc >> 8)
	batch_data[crc_pos + 3] = u8(crc)

	return batch_data
}

/// estimate_batch_size는 인코딩된 레코드 배치의 크기를 추정합니다.
/// 미리 할당 최적화에 사용됩니다.
fn estimate_batch_size(records []domain.Record) int {
	// 기본 배치 오버헤드: 61바이트 (헤더)
	mut size := 61

	for record in records {
		// 레코드당 오버헤드: ~20바이트 (varints, 속성)
		size += 20
		size += record.key.len
		size += record.value.len
	}

	return size
}

// CRC32-C 체크섬 (crc32c 모듈 사용)

/// calculate_crc32c는 Castagnoli 다항식을 사용하여 CRC32-C 체크섬을 계산합니다.
/// Kafka RecordBatch 검증에 필수적인 함수입니다.
/// 최적화된 Slicing-by-8 알고리즘을 사용하는 crc32c 모듈에 위임합니다.
/// data: 체크섬을 계산할 바이트 배열
/// 반환값: 32비트 CRC32-C 체크섬 값
pub fn calculate_crc32c(data []u8) u32 {
	return crc32c.calculate(data)
}
