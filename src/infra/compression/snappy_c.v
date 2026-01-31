/// 인프라 레이어 - Snappy 압축 (C 라이브러리 사용)
/// Google Snappy C 라이브러리를 사용한 고성능 압축/해제
module compression

import infra.observability

// C 라이브러리 링크
#flag -L/opt/homebrew/lib -lsnappy
#flag -I/opt/homebrew/include
#include <snappy-c.h>

/// SnappyCompressorC는 C 라이브러리를 사용한 Snappy 압축기입니다.
pub struct SnappyCompressorC {
}

/// new_snappy_compressor_c는 C 라이브러리를 사용하는 새 SnappyCompressorC를 생성합니다.
pub fn new_snappy_compressor_c() &SnappyCompressorC {
	return &SnappyCompressorC{}
}

/// compress는 데이터를 Snappy 형식으로 압축합니다.
pub fn (c &SnappyCompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 출력 버퍼 크기 계산
	mut max_out_len := int(C.snappy_max_compressed_length(usize(data.len)))
	mut result := []u8{len: max_out_len, cap: max_out_len}
	mut out_len := usize(max_out_len)

	// C 호출
	snappy_status := C.snappy_compress(data.data, usize(data.len), result.data, &out_len)
	if snappy_status != 0 {
		return error('snappy compression failed with status: ${snappy_status}')
	}

	result = result[..int(out_len)]

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 Snappy 형식의 데이터를 해제합니다.
pub fn (c &SnappyCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 출력 버퍼 크기 계산
	mut uncompressed_len := usize(0)
	C.snappy_uncompressed_length(data.data, usize(data.len), &uncompressed_len)

	if uncompressed_len == 0 {
		return []u8{}
	}

	mut result := []u8{len: int(uncompressed_len), cap: int(uncompressed_len)}
	mut out_len := uncompressed_len

	// C 호출
	snappy_status := C.snappy_uncompress(data.data, usize(data.len), result.data, &out_len)
	if snappy_status != 0 {
		return error('snappy decompression failed with status: ${snappy_status}')
	}

	result = result[..int(out_len)]

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &SnappyCompressorC) compression_type() CompressionType {
	return CompressionType.snappy
}

/// snappy_max_compressed_length는 지정된 원본 크기의 최대 압축 크기를 반환합니다.
fn snappy_max_compressed_length(input_len int) int {
	return input_len + (input_len >> 6) + 32
}

// C 함수 선언 (snappy-c.h에서 제공)
fn C.snappy_compress(src &u8, src_len usize, dst &u8, dst_len &usize) int
fn C.snappy_uncompress(src &u8, src_len usize, dst &u8, dst_len &usize) int
fn C.snappy_uncompressed_length(compressed &u8, compressed_len usize, result &usize)
fn C.snappy_max_compressed_length(source_len usize) usize
