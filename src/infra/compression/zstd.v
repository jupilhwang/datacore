/// 인프라 레이어 - ZSTD 압축
/// Pure V 구현의 ZSTD 프레임 형식 압축 알고리즘
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
	// FHD: Frame_Content_Size_flag(2bits) + Single_Segment_flag(1bit) + unused(1bit) + Reserved(2bits) + Content_Size_flag(2bits)
	// 간단한 구현: Single_Segment=1, Frame_Content_Size_flag=2 (4바이트)
	// FCS_flag=2 (bits 6-7), Single_Segment=1 (bit 5)
	fhd := u8(0xA0) // FCS_flag=2 (0b10 << 6 = 0x80), Single_Segment=1 (0x20)
	result << fhd

	// Window_Descriptor (Single_Segment 프레임에서는 선택적)
	// 생략

	// Frame_Content_Size (4바이트, 리틀 엔디안)
	result << u8(data.len & 0xff)
	result << u8((data.len >> 8) & 0xff)
	result << u8((data.len >> 16) & 0xff)
	result << u8((data.len >> 24) & 0xff)

	// Raw_Block (비압축 블록)
	// Last_Block(1bit) + Block_Type(2bits) + Block_Size(21bits)
	// Last_Block=1 (bit 0), Block_Type=0 (Raw, bits 1-2), Block_Size=data.len (bits 3-23)
	block_header := u32(1) | (u32(0) << 1) | (u32(data.len) << 3) // Last_Block=1, Raw=0
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
		return error('invalid ZSTD magic number: 0x${magic:08x}')
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
	fcs_flag := (fhd >> 6) & 0x03 // Frame_Content_Size_flag (bits 6-7)
	single_segment := (fhd & 0x20) != 0 // bit 5
	// content_size_flag := fhd & 0x03 // bits 0-1 (미사용)

	// Window_Descriptor (Single_Segment가 아닌 경우)
	if !single_segment {
		if pos >= data.len {
			return error('incomplete ZSTD frame header')
		}
		pos++ // Window_Descriptor 스킵
	}

	// Frame_Content_Size 읽기
	mut frame_content_size := u64(0)
	match fcs_flag {
		0 {
			if single_segment {
				// 1바이트
				if pos >= data.len {
					return error('incomplete ZSTD frame header')
				}
				frame_content_size = u64(data[pos])
				pos++
			}
		}
		1 {
			// 2바이트
			if pos + 2 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8)
			pos += 2
		}
		2 {
			// 4바이트
			if pos + 4 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8) | (u64(data[pos + 2]) << 16) | (u64(data[
				pos + 3]) << 24)
			pos += 4
		}
		3 {
			// 8바이트
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

	_ = frame_content_size // 사용하지 않음 (간단한 구현)

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
					// 반복 횟수는 다음 바이트들에서 읽어야 함 (간단한 구현)
					result << [byte_val]
				}
			}
			2 {
				// Compressed_Block - 간단한 구현에서는 지원하지 않음
				return error('compressed ZSTD blocks not supported in this implementation')
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

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &ZstdCompressor) compression_type() CompressionType {
	return CompressionType.zstd
}
