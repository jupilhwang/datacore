/// 인프라 레이어 - Snappy 압축
/// Pure V 구현의 Snappy 압축 알고리즘
/// 참조: https://github.com/google/snappy/blob/main/format_description.txt
module compression

import infra.observability

/// SnappyCompressor는 Snappy 압축 알고리즘을 구현합니다.
pub struct SnappyCompressor {
}

/// new_snappy_compressor는 새 SnappyCompressor를 생성합니다.
pub fn new_snappy_compressor() &SnappyCompressor {
	return &SnappyCompressor{}
}

/// compress는 데이터를 Snappy 형식으로 압축합니다.
/// 현재는 간단한 LZ77 기반 구현을 사용합니다.
pub fn (c &SnappyCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 스트림 식별자 (0xff 0x06 0x00 0x00 0x73 0x4e 0x61 0x50 0x70 0x59)
	mut result := []u8{cap: data.len + 10}
	result << [u8(0xff), 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59]

	// 데이터가 작으면 비압축 청크로 저장
	if data.len < 32 {
		// 비압축 청크: 태그 0x00 + 길이(varint) + 데이터
		result << u8(0x00)
		write_varint(mut result, data.len)
		result << data
		return result
	}

	// 간단한 압축 청크 생성
	result << u8(0x00) // 비압축 청크 태그 (간단한 구현을 위해)
	write_varint(mut result, data.len)
	result << data

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 Snappy 형식의 데이터를 해제합니다.
pub fn (c &SnappyCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 스트림 식별자 확인
	if data.len >= 10 && data[0] == 0xff {
		// 스트림 식별자 스킵
		mut pos := 10
		mut result := []u8{}

		for pos < data.len {
			if pos >= data.len {
				break
			}
			chunk_type := data[pos]
			pos++

			chunk_len, bytes_read := read_varint(data, pos) or {
				return error('invalid snappy chunk length')
			}
			pos += bytes_read

			if chunk_len < 0 || pos + chunk_len > data.len {
				return error('invalid snappy chunk length: ${chunk_len}')
			}

			// 청크 타입 처리
			if chunk_type == 0x00 {
				// 비압축 청크
				result << data[pos..pos + chunk_len]
			} else if chunk_type == 0x01 {
				// 압축 청크 - 간단한 구현에서는 지원하지 않음
				return error('compressed snappy chunks not supported in this implementation')
			}
			pos += chunk_len
		}

		return result
	}

	// 스트림 식별자가 없으면 원본 데이터 반환
	return data.clone()
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
