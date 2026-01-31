/// 인프라 레이어 - Zstd 압축 (C 라이브러리 사용)
/// Facebook Zstd C 라이브러리를 사용한 고성능 압축/해제
module compression

import infra.observability

// C 라이브러리 링크
#flag -L/opt/homebrew/lib -lzstd
#flag -I/opt/homebrew/include
#include <zstd.h>

/// ZstdCompressorC는 C 라이브러리를 사용한 Zstd 압축기입니다.
pub struct ZstdCompressorC {
	level int
}

/// new_zstd_compressor_c는 C 라이브러리를 사용하는 새 ZstdCompressorC를 생성합니다.
/// 기본 압축 레벨은 3입니다.
pub fn new_zstd_compressor_c() &ZstdCompressorC {
	return &ZstdCompressorC{
		level: 3
	}
}

/// new_zstd_compressor_c_with_level은 지정된 압축 레벨로 ZstdCompressorC를 생성합니다.
/// 레벨: 1-22 (1=최고속도, 22=최고압축)
pub fn new_zstd_compressor_c_with_level(level int) &ZstdCompressorC {
	mut lvl := level
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 22 {
		lvl = 22
	}
	return &ZstdCompressorC{
		level: lvl
	}
}

/// compress는 데이터를 Zstd 형식으로 압축합니다.
pub fn (c &ZstdCompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 출력 버퍼 크기 계산
	max_dst_size := C.ZSTD_compressBound(usize(data.len))
	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// C 호출
	compressed_size := C.ZSTD_compress(result.data, max_dst_size, data.data, usize(data.len),
		c.level)

	if C.ZSTD_isError(compressed_size) != 0 {
		err_name := C.ZSTD_getErrorName(compressed_size)
		return error('zstd compression failed: ${cstring_to_string(err_name)}')
	}

	result = result[..int(compressed_size)]

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len), observability.field_int('level',
		c.level))

	return result
}

/// decompress는 Zstd 형식의 데이터를 해제합니다.
pub fn (c &ZstdCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 프레임에서 원본 크기 읽기
	content_size := C.ZSTD_getFrameContentSize(data.data, usize(data.len))
	max_dst_size := if content_size > 0 { usize(content_size) } else { usize(data.len * 10) }

	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// C 호출
	decompressed_size := C.ZSTD_decompress(result.data, max_dst_size, data.data, usize(data.len))

	if C.ZSTD_isError(decompressed_size) != 0 {
		err_name := C.ZSTD_getErrorName(decompressed_size)
		return error('zstd decompression failed: ${cstring_to_string(err_name)}')
	}

	result = result[..int(decompressed_size)]

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &ZstdCompressorC) compression_type() CompressionType {
	return CompressionType.zstd
}

/// cstring_to_string은 C 문자열을 V 문자열로 변환합니다.
fn cstring_to_string(cstr &u8) string {
	if isnulptr(cstr) {
		return ''
	}
	mut len := 0
	for unsafe { cstr[len] != 0 } {
		len++
	}
	mut res := []u8{len: len}
	for i in 0 .. len {
		res[i] = unsafe { cstr[i] }
	}
	return res.bytestr()
}

/// isnulptr은 포인터가 nullptr인지 확인합니다.
fn isnulptr(ptr &u8) bool {
	return ptr == unsafe { nil }
}

// C 함수 선언 (zstd.h에서 제공)
fn C.ZSTD_compress(dst &u8, dstCapacity usize, src &u8, srcSize usize, compressionLevel int) usize
fn C.ZSTD_decompress(dst &u8, dstCapacity usize, src &u8, compressedSize usize) usize
fn C.ZSTD_compressBound(srcSize usize) usize
fn C.ZSTD_getErrorName(code usize) &u8
fn C.ZSTD_isError(code usize) int
fn C.ZSTD_getFrameContentSize(src &u8, srcSize usize) u64
