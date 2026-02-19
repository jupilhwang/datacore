/// 인프라 레이어 - Zstd 압축 (C 라이브러리 사용)
/// Facebook Zstd C 라이브러리를 사용한 고성능 압축/해제 (Kafka 호환)
module compression

import infra.observability

// C 라이브러리 링크
#flag -L/opt/homebrew/lib -lzstd
#flag -I/opt/homebrew/include
#include <zstd.h>

// ZSTD 특수 반환값 상수
const zstd_contentsize_unknown = u64(0) - 1
const zstd_contentsize_error = u64(0) - 2

/// ZstdCompressorC는 C 라이브러리를 사용한 Zstd 압축기입니다.
/// Kafka와 호환되는 ZSTD Frame Format을 사용합니다.
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

/// compress는 데이터를 Zstd Frame 형식으로 압축합니다.
/// Kafka와 호환되는 ZSTD Frame Format (Magic: 0xFD2FB528)을 생성합니다.
pub fn (c &ZstdCompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 출력 버퍼 크기 계산
	max_dst_size := C.ZSTD_compressBound(usize(data.len))
	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// C 호출 - ZSTD_compress는 프레임 형식으로 압축
	compressed_size := C.ZSTD_compress(result.data, max_dst_size, data.data, usize(data.len),
		c.level)

	if C.ZSTD_isError(compressed_size) != 0 {
		err_name := C.ZSTD_getErrorName(compressed_size)
		return error('zstd compression failed: ${cstring_to_string(err_name)}')
	}

	result = unsafe { result[..int(compressed_size)] }

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd frame compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len), observability.field_int('level',
		c.level))

	return result
}

/// decompress는 Zstd Frame 형식의 데이터를 해제합니다.
/// Kafka에서 생성된 ZSTD 압축 데이터를 처리할 수 있습니다.
pub fn (c &ZstdCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 프레임에서 원본 크기 읽기
	content_size := C.ZSTD_getFrameContentSize(data.data, usize(data.len))

	// 특수 반환값 처리
	if content_size == zstd_contentsize_error {
		return error('zstd decompression failed: invalid frame header')
	}

	// 원본 크기 미포함 시 스트리밍 압축 해제 사용
	if content_size == zstd_contentsize_unknown || content_size == 0 {
		return c.decompress_streaming(data)
	}

	mut result := []u8{len: int(content_size), cap: int(content_size)}

	// C 호출
	decompressed_size := C.ZSTD_decompress(result.data, usize(content_size), data.data,
		usize(data.len))

	if C.ZSTD_isError(decompressed_size) != 0 {
		err_name := C.ZSTD_getErrorName(decompressed_size)
		return error('zstd decompression failed: ${cstring_to_string(err_name)}')
	}

	result = unsafe { result[..int(decompressed_size)] }

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd frame decompressed', observability.field_int('compressed_size',
		data.len), observability.field_int('decompressed_size', result.len))

	return result
}

/// decompress_streaming은 원본 크기를 알 수 없는 경우 스트리밍 방식으로 압축 해제합니다.
fn (c &ZstdCompressorC) decompress_streaming(data []u8) ![]u8 {
	// 스트리밍 압축 해제 컨텍스트 생성
	dctx := C.ZSTD_createDCtx()
	if dctx == unsafe { nil } {
		return error('zstd: failed to create decompression context')
	}
	defer {
		C.ZSTD_freeDCtx(dctx)
	}

	// 초기 버퍼 크기 (압축률 1:4 가정, 최소 64KB)
	mut estimated_size := data.len * 4
	if estimated_size < 65536 {
		estimated_size = 65536
	}

	mut result := []u8{len: estimated_size, cap: estimated_size}

	// 입출력 버퍼 설정
	mut in_buf := ZstdInBuffer{
		src:  data.data
		size: usize(data.len)
		pos:  0
	}

	mut out_buf := ZstdOutBuffer{
		dst:  result.data
		size: usize(result.len)
		pos:  0
	}

	// 스트리밍 압축 해제
	for {
		ret := C.ZSTD_decompressStream(dctx, &out_buf, &in_buf)

		if C.ZSTD_isError(ret) != 0 {
			err_name := C.ZSTD_getErrorName(ret)
			return error('zstd streaming decompression failed: ${cstring_to_string(err_name)}')
		}

		// 완료
		if ret == 0 {
			break
		}

		// 출력 버퍼가 가득 찬 경우 확장
		if out_buf.pos == out_buf.size {
			new_size := result.len * 2
			if new_size > 268435456 {
				// 256MB 제한
				return error('zstd: decompressed data too large')
			}
			mut new_result := []u8{len: new_size, cap: new_size}
			for i := 0; i < int(out_buf.pos); i++ {
				new_result[i] = result[i]
			}
			result = unsafe { new_result }
			out_buf.dst = result.data
			out_buf.size = usize(result.len)
		}
	}

	result = unsafe { result[..int(out_buf.pos)] }
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

// ZstdInBuffer 구조체 (snake_case)
struct ZstdInBuffer {
	src  voidptr
	size usize
mut:
	pos usize
}

// ZstdOutBuffer 구조체 (snake_case)
struct ZstdOutBuffer {
mut:
	dst  voidptr
	size usize
	pos  usize
}

// C 함수 선언 (zstd.h에서 제공)
fn C.ZSTD_compress(dst &u8, dstCapacity usize, src &u8, srcSize usize, compressionLevel int) usize
fn C.ZSTD_decompress(dst &u8, dstCapacity usize, src &u8, compressedSize usize) usize
fn C.ZSTD_compressBound(srcSize usize) usize
fn C.ZSTD_getErrorName(code usize) &u8
fn C.ZSTD_isError(code usize) int
fn C.ZSTD_getFrameContentSize(src &u8, srcSize usize) u64
fn C.ZSTD_createDCtx() voidptr
fn C.ZSTD_freeDCtx(dctx voidptr) usize
fn C.ZSTD_decompressStream(dctx voidptr, output &ZstdOutBuffer, input &ZstdInBuffer) usize
