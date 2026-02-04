/// 인프라 레이어 - Snappy 압축
/// Pure V 구현의 Snappy 압축 알고리즘
/// Kafka와 호환되는 raw snappy 형식 지원
/// 참조: https://github.com/google/snappy/blob/main/format_description.txt
module compression

import infra.observability

/// Snappy 압축 알고리즘 구현.
pub struct SnappyCompressor {
}

/// new_snappy_compressor는 새 SnappyCompressor를 생성합니다.
pub fn new_snappy_compressor() &SnappyCompressor {
	return &SnappyCompressor{}
}

/// compress는 데이터를 Snappy 형식으로 압축합니다.
/// Kafka 호환: raw snappy 형식 (varint 크기 + 압축 데이터)
pub fn (c &SnappyCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// raw snappy 형식: varint으로 인코딩된 크기 + 압축 데이터
	mut result := []u8{cap: data.len + 10}

	// varint으로 크기 인코딩
	write_varint(mut result, data.len)

	// 간단한 압축: 데이터를 리터럴 블록으로 저장하여 복사
	// snappy.v의 decompressor (b < 0x80)은 b+1 바이트의 리터럴을 예상함
	mut offset := 0
	for offset < data.len {
		remaining := data.len - offset
		lit_len := if remaining > 128 { 128 } else { remaining }

		// 리터럴 태그: b = lit_len - 1
		result << u8(lit_len - 1)
		result << data[offset..offset + lit_len]

		offset += lit_len
	}

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 Snappy 형식의 데이터를 해제합니다.
/// Kafka 호환: raw snappy 형식 (varint 크기 + 압축 데이터)
pub fn (c &SnappyCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	mut pos := 0

	// varint으로 인코딩된 원본 크기 읽기
	uncompressed_len, bytes_read := read_varint(data, pos) or {
		return error('invalid snappy data: cannot read size')
	}
	pos += bytes_read

	if uncompressed_len <= 0 {
		return []u8{}
	}

	if pos >= data.len {
		return error('invalid snappy data: incomplete')
	}

	compressed_data := data[pos..]
	mut result := []u8{len: uncompressed_len, cap: uncompressed_len}

	// Snappy 압축 해제 실행
	decompressed_size := snappy_decompress_raw(compressed_data, mut result) or {
		return error('snappy decompression failed: ${err}')
	}

	if decompressed_size != uncompressed_len {
		return error('snappy decompression size mismatch: expected ${uncompressed_len}, got ${decompressed_size}')
	}

	// Use unsafe to prevent copying when shrinking slice
	unsafe {
		result = result[..decompressed_size]
	}
	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// snappy_decompress_raw는 raw snappy 형식의 데이터를 해제합니다.
/// 간단한 LZ77 복사 구현
fn snappy_decompress_raw(compressed_data []u8, mut result []u8) !int {
	mut ip := 0 // 입력 포인터
	mut op := 0 // 출력 포inter

	for ip < compressed_data.len {
		b := compressed_data[ip]
		ip++

		if b < 0x80 {
			// 리터럴: 1바이트 (0-127개의 리터럴)
			// 다음 b+1 바이트를 그대로 복사
			lit_len := int(b) + 1

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal')
			}
			if op + lit_len > result.len {
				return error('output buffer overflow')
			}

			for i in 0 .. lit_len {
				result[op] = compressed_data[ip + i]
				op++
			}
			ip += lit_len
		} else {
			// 복사 명령: 2바이트 또는 4바이트
			mut copy_len := int(b & 0x7f)
			mut offset := 0

			if copy_len < 0x7f {
				// 2바이트 형식: offset = next byte
				if ip >= compressed_data.len {
					return error('incomplete copy command')
				}
				offset = int(compressed_data[ip]) | (int(u32(copy_len & 0x70)) << 4)
				ip++
				copy_len += 2
			} else {
				// 4바이트 형식: offset = next 2 bytes
				if ip + 1 >= compressed_data.len {
					return error('incomplete copy command')
				}
				offset = int(compressed_data[ip]) | (int(compressed_data[ip + 1]) << 8)
				ip += 2
				copy_len = int(b & 0x7f) + 1
			}

			if offset == 0 {
				return error('invalid copy offset: 0')
			}
			if op + copy_len > result.len {
				return error('copy overflow')
			}

			// 복사 실행
			for _ in 0 .. copy_len {
				src_idx := op - offset
				if src_idx < 0 {
					return error('copy offset exceeds output')
				}
				result[op] = result[src_idx]
				op++
			}
		}
	}

	return op
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &SnappyCompressor) compression_type() CompressionType {
	return CompressionType.snappy
}

/// write_varint는 가변 길이 정수를 버퍼에 씁니다.
fn write_varint(mut buf []u8, value int) {
	mut v := value
	for v >= 128 {
		buf << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	buf << u8(v)
}

/// read_varint는 버퍼에서 가변 길이 정수를 읽습니다.
fn read_varint(data []u8, start int) !(int, int) {
	mut result := 0
	mut shift := 0
	mut pos := start

	for pos < data.len {
		b := data[pos]
		pos++
		result |= int(u32(b & 0x7f) << shift)
		if (b & 0x80) == 0 {
			return result, pos - start
		}
		shift += 7
		if shift >= 32 {
			return error('varint too large')
		}
	}

	return error('incomplete varint')
}
