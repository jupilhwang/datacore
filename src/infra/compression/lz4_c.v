/// 인프라 레이어 - LZ4 압축 (C 라이브러리 사용)
/// LZ4 C 라이브러리를 사용한 고성능 압축/해제
module compression

import infra.observability

// C 라이브러리 링크
#flag -L/opt/homebrew/lib -llz4
#flag -I/opt/homebrew/include
#include <lz4.h>

/// Lz4CompressorC는 C 라이브러리를 사용한 LZ4 압축기입니다.
pub struct Lz4CompressorC {
}

/// new_lz4_compressor_c는 C 라이브러리를 사용하는 새 Lz4CompressorC를 생성합니다.
pub fn new_lz4_compressor_c() &Lz4CompressorC {
	return &Lz4CompressorC{}
}

/// compress는 데이터를 LZ4 형식으로 압축합니다.
pub fn (c &Lz4CompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 출력 버퍼 크기 계산
	max_dst_size := C.LZ4_compressBound(data.len)
	mut result := []u8{len: max_dst_size, cap: max_dst_size}

	// C 호출
	compressed_size := C.LZ4_compress_default(data.data, result.data, data.len, max_dst_size)
	if compressed_size <= 0 {
		return error('lz4 compression failed')
	}

	result = result[..compressed_size]

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 LZ4 형식의 데이터를 해제합니다.
pub fn (c &Lz4CompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// LZ4는 압축 시 원본 크기 정보를 저장하지 않으므로,
	// 안전한 최대 크기 사용 (압축률 1:4 가정, 최소 1MB)
	max_decompressed_size := if data.len * 4 < 1024 * 1024 { data.len * 4 } else { 1024 * 1024 }
	mut result := []u8{len: max_decompressed_size, cap: max_decompressed_size}

	// C 호출
	decompressed_size := C.LZ4_decompress_safe(data.data, result.data, data.len, max_decompressed_size)
	if decompressed_size <= 0 {
		return error('lz4 decompression failed')
	}

	result = result[..decompressed_size]

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &Lz4CompressorC) compression_type() CompressionType {
	return CompressionType.lz4
}

// C 함수 선언 (lz4.h에서 제공)
fn C.LZ4_compress_default(src &u8, dst &u8, srcSize int, maxDstSize int) int
fn C.LZ4_decompress_safe(src &u8, dst &u8, compressedSize int, maxDecompressedSize int) int
fn C.LZ4_compressBound(isize int) int
