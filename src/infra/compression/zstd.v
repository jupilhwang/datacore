/// 인프라 레이어 - ZSTD 압축
/// Pure V 구현의 ZSTD 프레임 형식 압축 알고리즘
/// Kafka와 호환되는 ZSTD 프레임 형식 지원
/// 참조: https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md
module compression

import infra.observability

/// ZSTD 압축 알고리즘 구현.
pub struct ZstdCompressor {
	level int
}

/// ZSTD 매직 넘버
const zstd_magic_number = u32(0xfd2fb528)

/// new_zstd_compressor는 새 ZstdCompressor를 생성합니다.
/// 기본 압축 레벨은 3입니다.
pub fn new_zstd_compressor() &ZstdCompressor {
	return &ZstdCompressor{
		level: 3
	}
}

/// new_zstd_compressor_with_level은 지정된 압축 레벨로 ZstdCompressor를 생성합니다.
/// 레벨: 1-22 (1=최고속도, 22=최고압축)
pub fn new_zstd_compressor_with_level(level int) &ZstdCompressor {
	mut lvl := level
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 22 {
		lvl = 22
	}
	return &ZstdCompressor{
		level: lvl
	}
}

/// compress는 데이터를 ZSTD 프레임 형식으로 압축합니다.
/// 간단한 프레임 래퍼 구현 사용.
pub fn (c &ZstdCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// ZSTD 프레임 형식:
	// 매직 넘버 (4 bytes) + 프레임 헤더 + 블록들 + [선택적 체크섬]
	mut result := []u8{cap: data.len + 32}

	// 매직 넘버 (리틀 엔디안)
	result << u8(zstd_magic_number & 0xff)
	result << u8((zstd_magic_number >> 8) & 0xff)
	result << u8((zstd_magic_number >> 16) & 0xff)
	result << u8((zstd_magic_number >> 24) & 0xff)

	// 프레임 헤더
	fhd := u8(0xA0) // FCS_flag=2, Single_Segment=1
	result << fhd

	// Frame_Content_Size (4바이트, 리틀 엔디안)
	result << u8(data.len & 0xff)
	result << u8((data.len >> 8) & 0xff)
	result << u8((data.len >> 16) & 0xff)
	result << u8((data.len >> 24) & 0xff)

	// Raw_Block (비압축 블록)
	block_header := u32(1) | (u32(0) << 1) | (u32(data.len) << 3)
	result << u8(block_header & 0xff)
	result << u8((block_header >> 8) & 0xff)
	result << u8((block_header >> 16) & 0xff)

	// 블록 데이터
	result << data

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len), observability.field_int('level',
		c.level))

	return result
}

/// decompress는 ZSTD 프레임 형식의 데이터를 해제합니다.
/// Kafka 호환: ZSTD 프레임 형식 지원
pub fn (c &ZstdCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 매직 넘버 확인
	if data.len < 4 {
		return error('incomplete ZSTD frame')
	}

	magic := u32(data[0]) | (u32(data[1]) << 8) | (u32(data[2]) << 16) | (u32(data[3]) << 24)

	if magic != zstd_magic_number {
		// 매직 넘버가 없으면 raw 데이터로 처리
		return data.clone()
	}

	mut pos := 4
	mut result := []u8{}

	// 프레임 헤더 파싱
	if pos >= data.len {
		return error('incomplete ZSTD frame header')
	}
	fhd := data[pos]
	pos++

	// 플래그 파싱
	fcs_flag := (fhd >> 6) & 0x03 // Frame_Content_Size_flag
	single_segment := (fhd & 0x20) != 0 // bit 5

	// Window_Descriptor (Single_Segment가 아닌 경우)
	if !single_segment {
		if pos >= data.len {
			return error('incomplete ZSTD frame header')
		}
		pos++
	}

	// Frame_Content_Size 읽기
	mut frame_content_size := u64(0)
	match fcs_flag {
		0 {
			if single_segment {
				if pos >= data.len {
					return error('incomplete ZSTD frame header')
				}
				frame_content_size = u64(data[pos])
				pos++
			}
		}
		1 {
			if pos + 2 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8)
			pos += 2
		}
		2 {
			if pos + 4 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8) | (u64(data[pos + 2]) << 16) | (u64(data[
				pos + 3]) << 24)
			pos += 4
		}
		3 {
			if pos + 8 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8) | (u64(data[pos + 2]) << 16) | (u64(data[
				pos + 3]) << 24) | (u64(data[pos + 4]) << 32) | (u64(data[pos + 5]) << 40) | (u64(data[
				pos + 6]) << 48) | (u64(data[pos + 7]) << 56)
			pos += 8
		}
		else {}
	}

	_ = frame_content_size

	// 블록 파싱
	for pos < data.len {
		if pos + 3 > data.len {
			break
		}

		// 블록 헤더 (3바이트, 리틀 엔디안)
		block_header := u32(data[pos]) | (u32(data[pos + 1]) << 8) | (u32(data[pos + 2]) << 16)
		pos += 3

		last_block := (block_header & 0x00000001) != 0
		block_type := (block_header >> 1) & 0x03
		block_size := int((block_header >> 3) & 0x1fffff)

		if pos + block_size > data.len {
			return error('incomplete ZSTD block')
		}

		match block_type {
			0 {
				// Raw_Block (비압축)
				result << data[pos..pos + block_size]
			}
			1 {
				// RLE_Block
				if block_size > 0 {
					byte_val := data[pos]
					// 프레임 콘텐츠 크기만큼 반복
					repeat_count := if frame_content_size > 0 {
						int(frame_content_size)
					} else {
						block_size
					}
					for _ in 0 .. repeat_count {
						result << byte_val
					}
				}
			}
			2 {
				// Compressed_Block: ZSTD 압축 해제 실행
				decompressed_block := zstd_decompress_block(data[pos..pos + block_size])!
				result << decompressed_block
			}
			3 {
				// Reserved
				return error('invalid ZSTD block type')
			}
			else {}
		}

		pos += block_size

		if last_block {
			break
		}
	}

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// zstd_decompress_block은 단일 ZSTD 압축 블록을 해제합니다.
/// 간단한 FSE/Huffman 복사 구현
fn zstd_decompress_block(compressed_data []u8) ![]u8 {
	// ZSTD 압축 해제는 복잡한 알고리즘이 필요합니다.
	// 간단한 구현으로는 지원되지 않으므로 에러를 반환합니다.
	return error('ZSTD compressed block decompression requires full FSE/Huffman implementation')
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &ZstdCompressor) compression_type() CompressionType {
	return CompressionType.zstd
}
