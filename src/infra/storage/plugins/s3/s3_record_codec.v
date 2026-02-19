// Infra Layer - S3 레코드 코덱
// 저장된 레코드의 인코딩 및 디코딩 로직
module s3

import time

/// StoredRecord는 오프셋을 포함한 스토리지용 내부 표현입니다.
struct StoredRecord {
	offset           i64
	timestamp        time.Time
	key              []u8
	value            []u8
	headers          map[string][]u8
	compression_type u8
}

/// encode_stored_records는 StoredRecord 목록을 바이너리 형식으로 인코딩합니다.
/// 반복적인 재할당을 피하기 위해 버퍼 용량을 미리 할당합니다.
fn encode_stored_records(records []StoredRecord) []u8 {
	// 버퍼 크기 추정: 4 (개수) + 레코드 * (8 오프셋 + 8 타임스탬프 + 4 키길이 + 4 값길이 + 4 헤더개수 + 평균 데이터)
	mut estimated_size := 4
	for rec in records {
		// 고정 오버헤드: 8 (오프셋) + 8 (타임스탬프) + 4 (키길이) + 4 (값길이) + 4 (헤더개수) + 1 (압축타입) = 29
		estimated_size += 29 + rec.key.len + rec.value.len
		// 헤더: 헤더당 2 (키길이) + 키 + 2 (값길이) + 값
		for h_key, h_val in rec.headers {
			estimated_size += 4 + h_key.len + h_val.len
		}
	}

	mut buf := []u8{cap: estimated_size}
	record_count := records.len
	buf << u8(record_count >> 24)
	buf << u8(record_count >> 16)
	buf << u8(record_count >> 8)
	buf << u8(record_count)

	for rec in records {
		// 오프셋 (8 바이트)
		for i := 7; i >= 0; i-- {
			buf << u8(rec.offset >> (i * 8))
		}

		// 타임스탬프 (8 바이트)
		ts := rec.timestamp.unix_milli()
		for i := 7; i >= 0; i-- {
			buf << u8(ts >> (i * 8))
		}

		// 키
		key_len := rec.key.len
		buf << u8(key_len >> 24)
		buf << u8(key_len >> 16)
		buf << u8(key_len >> 8)
		buf << u8(key_len)
		buf << rec.key

		// 값
		value_len := rec.value.len
		buf << u8(value_len >> 24)
		buf << u8(value_len >> 16)
		buf << u8(value_len >> 8)
		buf << u8(value_len)
		buf << rec.value

		// 헤더 (map[string][]u8)
		headers_count := rec.headers.len
		buf << u8(headers_count >> 24)
		buf << u8(headers_count >> 16)
		buf << u8(headers_count >> 8)
		buf << u8(headers_count)

		for h_key, h_val in rec.headers {
			// 헤더 키 길이와 값
			buf << u8(h_key.len >> 8)
			buf << u8(h_key.len)
			buf << h_key.bytes()

			// 헤더 값 길이와 값
			buf << u8(h_val.len >> 8)
			buf << u8(h_val.len)
			buf << h_val
		}

		// 압축 타입 (1 바이트)
		buf << rec.compression_type
	}

	return buf
}

/// decode_stored_records는 바이너리 데이터를 StoredRecord 목록으로 디코딩합니다.
fn decode_stored_records(data []u8) []StoredRecord {
	if data.len < 4 {
		return []
	}

	mut pos := 0
	record_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
		pos + 3])
	pos += 4

	mut records := []StoredRecord{}

	for _ in 0 .. record_count {
		if pos + 20 > data.len {
			break
		}

		// 오프셋
		mut offset := i64(0)
		for i := 0; i < 8; i++ {
			offset = i64((u64(offset) << 8) | u64(data[pos + i]))
		}
		pos += 8

		mut ts := i64(0)
		for i := 0; i < 8; i++ {
			ts = i64((u64(ts) << 8) | u64(data[pos + i]))
		}
		pos += 8

		// 키
		key_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		key := data[pos..pos + int(key_len)].clone()
		pos += int(key_len)

		// 값
		value_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		value := data[pos..pos + int(value_len)].clone()
		pos += int(value_len)

		// 헤더
		headers_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4

		mut headers := map[string][]u8{}
		for _ in 0 .. headers_count {
			h_key_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_key := data[pos..pos + int(h_key_len)].bytestr()
			pos += int(h_key_len)

			h_val_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_val := data[pos..pos + int(h_val_len)].clone()
			pos += int(h_val_len)

			headers[h_key] = h_val
		}

		// 압축 타입 (1 바이트, 하위 호환: 데이터가 없으면 0으로 기본값)
		mut compression_type := u8(0)
		if pos < data.len {
			compression_type = data[pos]
			pos += 1
		}

		records << StoredRecord{
			offset:           offset
			timestamp:        time.unix_milli(ts)
			key:              key
			value:            value
			headers:          headers
			compression_type: compression_type
		}
	}

	return records
}
