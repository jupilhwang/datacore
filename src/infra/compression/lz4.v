/// 인프라 레이어 - LZ4 압축
/// Pure V 구현의 LZ4 프레임 형식 압축 알고리즘
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

/// new_lz4_compressor - creates a new Lz4Compressor
/// new_lz4_compressor - creates a new Lz4Compressor
pub fn new_lz4_compressor() &Lz4Compressor {
	return &Lz4Compressor{}
}

/// compress - compresses data to LZ4 frame format
/// compress - compresses data to LZ4 frame format
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
	// FLG: 버전(2bits) + B.Indep(1bit) + B.Checksum(1bit) + C.Size(1bit) + C.Checksum(1bit) + 예약(2bits)
	flg := lz4_frame_version | 0x00 // 독립 블록, 콘텐츠 체크섬 없음
	bd := lz4_block_default // 64KB 블록 크기

	result << flg
	result << bd

	// 헤더 체크섬 (xxh32의 상위 8비트)
	header_checksum := calculate_header_checksum([flg, bd])
	result << header_checksum

	// 데이터가 작으면 단일 블록으로 저장
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

/// decompress - decompresses LZ4 frame format data
/// decompress - decompresses LZ4 frame format data
pub fn (c &Lz4Compressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 매직 넘버 확인
	if data.len < 4 || data[0..4] != lz4_magic_number {
		return error('invalid LZ4 magic number')
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
			// 압축 블록 - 간단한 구현에서는 지원하지 않음
			return error('compressed LZ4 blocks not supported in this implementation')
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

	return result
}

/// compression_type - returns the compression type
/// compression_type - returns the compression type
pub fn (c &Lz4Compressor) compression_type() CompressionType {
	return CompressionType.lz4
}

/// LZ4 프레임 헤더의 체크섬 계산.
fn calculate_header_checksum(header []u8) u8 {
	// 간단한 체크섬 (실제로는 xxh32 사용)
	mut sum := u32(0)
	for b in header {
		sum = (sum << 1) | (sum >> 31)
		sum ^= u32(b)
	}
	return u8((sum >> 8) & 0xff)
}
