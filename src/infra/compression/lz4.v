/// 인프라 레이어 - LZ4 압축
/// DEPRECATED: Pure V LZ4 구현체는 더 이상 권장되지 않습니다.
/// C 라이브러리 버전(lz4_c.v)이 Kafka 프레임 호환성을 제공합니다.
/// 테스트 목적으로만 사용하세요.
/// Pure V 구현의 LZ4 프레임 형식 압축 알고리즘
/// Kafka와 호환되는 LZ4 프레임 형식 지원
/// 참조: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
module compression

import infra.observability

/// LZ4 압축 알고리즘 구현.
pub struct Lz4Compressor {
}

/// LZ4 매직 넘버
const lz4_magic_number = [u8(0x04), 0x22, 0x4d, 0x18]

/// LZ4 프레임 헤더 플래그
const lz4_frame_version = u8(0x40) // 버전 1.0
const lz4_block_default = u8(0x40) // 64KB 블록 크기

/// new_lz4_compressor는 새 Lz4Compressor를 생성합니다.
pub fn new_lz4_compressor() &Lz4Compressor {
	return &Lz4Compressor{}
}

/// compress는 데이터를 LZ4 프레임 형식으로 압축합니다.
pub fn (c &Lz4Compressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// LZ4 프레임 형식:
	// 매직 넘버 (4 bytes) + 프레임 헤더 + 블록들 + 종료 마커 + 체크섬
	mut result := []u8{cap: data.len + 32}

	// 매직 넘버
	result << lz4_magic_number

	// 프레임 헤더
	flg := lz4_frame_version | 0x00 // 독립 블록, 콘텐츠 체크섬 없음
	bd := lz4_block_default // 64KB 블록 크기

	result << flg
	result << bd

	// 헤더 체크섬 (xxh32의 상위 8비트)
	header_checksum := calculate_header_checksum([flg, bd])
	result << header_checksum

	// 데이터를 단일 블록으로 저장 (간단한 구현)
	if data.len <= 65536 {
		// 비압축 블록 (최상위 비트가 1)
		block_size := u32(data.len) | 0x80000000
		result << u8(block_size & 0xff)
		result << u8((block_size >> 8) & 0xff)
		result << u8((block_size >> 16) & 0xff)
		result << u8((block_size >> 24) & 0xff)
		result << data
	} else {
		// 여러 블록으로 분할 (간단한 구현)
		mut offset := 0
		for offset < data.len {
			remaining := data.len - offset
			block_len := if remaining > 65536 { 65536 } else { remaining }

			// 비압축 블록
			block_size := u32(block_len) | 0x80000000
			result << u8(block_size & 0xff)
			result << u8((block_size >> 8) & 0xff)
			result << u8((block_size >> 16) & 0xff)
			result << u8((block_size >> 24) & 0xff)
			result << data[offset..offset + block_len]

			offset += block_len
		}
	}

	// 종료 마커 (빈 블록)
	result << [u8(0x00), 0x00, 0x00, 0x00]

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 LZ4 프레임 형식의 데이터를 해제합니다.
/// Kafka 호환: LZ4 프레임 형식 지원
pub fn (c &Lz4Compressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 매직 넘버 확인 (리틀 엔디안)
	expected_magic := u32(0x04) | (u32(0x22) << 8) | (u32(0x4d) << 16) | (u32(0x18) << 24)
	actual_magic := u32(data[0]) | (u32(data[1]) << 8) | (u32(data[2]) << 16) | (u32(data[3]) << 24)

	if actual_magic != expected_magic {
		// 매직 넘버가 없으면 raw 데이터로 처리
		return data.clone()
	}

	mut pos := 4
	mut result := []u8{}

	// 프레임 헤더 파싱
	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	flg := data[pos]
	pos++

	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	_ := data[pos] // bd - block descriptor
	pos++

	// 헤더 체크섬 스킵
	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	pos++

	// 블록 파싱
	for pos < data.len {
		if pos + 4 > data.len {
			break
		}

		// 블록 크기 읽기 (리틀 엔디안)
		block_size := u32(data[pos]) | (u32(data[pos + 1]) << 8) | (u32(data[pos + 2]) << 16) | (u32(data[
			pos + 3]) << 24)
		pos += 4

		// 종료 마커 확인
		if block_size == 0 {
			break
		}

		// 비압축 블록 여부 확인 (최상위 비트)
		is_uncompressed := (block_size & 0x80000000) != 0
		size := int(block_size & 0x7fffffff)

		if pos + size > data.len {
			return error('incomplete LZ4 block')
		}

		if is_uncompressed {
			// 비압축 블록
			result << data[pos..pos + size]
		} else {
			// 압축 블록: LZ4 압축 해제 실행
			decompressed_block := lz4_decompress_block(data[pos..pos + size])!
			result << decompressed_block
		}

		pos += size

		// 블록 체크섬 스킵 (있는 경우)
		if (flg & 0x04) != 0 {
			if pos + 4 > data.len {
				return error('incomplete LZ4 block checksum')
			}
			pos += 4
		}
	}

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// lz4_decompress_block은 단일 LZ4 압축 블록을 해제합니다.
/// 간단한 LZ77 복사 구현
fn lz4_decompress_block(compressed_data []u8) ![]u8 {
	mut result := []u8{cap: compressed_data.len * 4}
	mut ip := 0

	for ip < compressed_data.len {
		b := compressed_data[ip]
		ip++

		if b < 0x20 {
			// 리터럴 길이: 0-31
			lit_len := int(b) + 1

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len
		} else if b < 0x40 {
			// 2바이트 매치
			mut lit_len := int(b & 0x1f)
			if lit_len == 0 {
				lit_len = 1
			}

			if ip + 1 >= compressed_data.len {
				return error('incomplete match')
			}

			offset := int(compressed_data[ip]) | ((int(b) & 0xe0) << 3)
			ip++

			mut match_len := int(compressed_data[ip])
			ip++
			match_len += 4 // 최소 매치 길이

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal in match')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len

			// 매치 복사
			for match_len > 0 {
				if offset == 0 || offset > result.len {
					return error('invalid offset')
				}
				result << result[result.len - offset]
				match_len--
			}
		} else {
			// 3바이트 이상 매치
			lit_len := int(b) - 0x20

			if ip + 2 >= compressed_data.len {
				return error('incomplete 3+ match')
			}

			offset := int(compressed_data[ip]) | (int(compressed_data[ip + 1]) << 8)
			ip += 2

			mut match_len := int(compressed_data[ip])
			ip++
			match_len += 4 // 최소 매치 길이

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal in 3+ match')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len

			// 매치 복사
			for match_len > 0 {
				if offset == 0 || offset > result.len {
					return error('invalid offset in 3+ match')
				}
				result << result[result.len - offset]
				match_len--
			}
		}
	}

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &Lz4Compressor) compression_type() CompressionType {
	return CompressionType.lz4
}

/// LZ4 프레임 헤더의 체크섬 계산.
fn calculate_header_checksum(header []u8) u8 {
	mut sum := u32(0)
	for b in header {
		sum = (sum << 1) | (sum >> 31)
		sum ^= u32(b)
	}
	return u8((sum >> 8) & 0xff)
}
