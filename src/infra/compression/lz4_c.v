/// 인프라 레이어 - LZ4 압축 (C 라이브러리 사용)
/// LZ4 프레임 API를 사용한 고성능 압축/해제 (Kafka 호환)
module compression

import infra.observability

// C 라이브러리 링크 - LZ4 Frame API 사용
#flag -L/opt/homebrew/lib -llz4
#flag -I/opt/homebrew/include
#include <lz4frame.h>

/// Lz4CompressorC는 C 라이브러리를 사용한 LZ4 압축기입니다.
/// Kafka 호환을 위해 LZ4 Frame Format을 사용합니다.
pub struct Lz4CompressorC {
}

/// new_lz4_compressor_c는 C 라이브러리를 사용하는 새 Lz4CompressorC를 생성합니다.
pub fn new_lz4_compressor_c() &Lz4CompressorC {
	return &Lz4CompressorC{}
}

/// compress는 데이터를 LZ4 Frame 형식으로 압축합니다.
/// Kafka와 호환되는 LZ4 Frame Format (Magic: 0x184D2204)을 생성합니다.
pub fn (c &Lz4CompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 프레임 압축에 필요한 최대 버퍼 크기 계산
	max_dst_size := C.LZ4F_compressFrameBound(usize(data.len), unsafe { nil })
	if max_dst_size == 0 {
		return error('lz4 frame: failed to calculate bound')
	}

	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// 한 번의 호출로 전체 프레임 압축
	compressed_size := C.LZ4F_compressFrame(result.data, max_dst_size, data.data, usize(data.len),
		unsafe { nil })

	if C.LZ4F_isError(compressed_size) != 0 {
		err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(compressed_size)) }
		return error('lz4 frame compression failed: ${err_name}')
	}

	result = unsafe { result[..int(compressed_size)] }

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 frame compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress는 LZ4 Frame 형식의 데이터를 해제합니다.
/// Kafka에서 생성된 LZ4 압축 데이터를 처리할 수 있습니다.
pub fn (c &Lz4CompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 압축 해제 컨텍스트 생성
	mut dctx := unsafe { nil }
	create_result := C.LZ4F_createDecompressionContext(&dctx, lz4f_version)
	if C.LZ4F_isError(create_result) != 0 {
		err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(create_result)) }
		return error('lz4 frame: failed to create decompression context: ${err_name}')
	}
	defer {
		C.LZ4F_freeDecompressionContext(dctx)
	}

	// 프레임 헤더에서 원본 크기 정보 추출 시도
	mut frame_info := Lz4FrameInfo{}
	mut src_size := usize(data.len)
	header_result := C.LZ4F_getFrameInfo(dctx, &frame_info, data.data, &src_size)
	if C.LZ4F_isError(header_result) != 0 {
		err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(header_result)) }
		return error('lz4 frame: invalid frame header: ${err_name}')
	}

	// 원본 크기 결정 (프레임 헤더에 있으면 사용, 없으면 추정)
	content_size := frame_info.content_size
	estimated_size := if content_size > 0 {
		int(content_size)
	} else {
		// 원본 크기 미포함 시 압축률 1:4 가정, 최소 64KB, 최대 64MB
		mut size := data.len * 4
		if size < 65536 {
			size = 65536
		}
		if size > 67108864 {
			size = 67108864
		}
		size
	}

	mut result := []u8{len: estimated_size, cap: estimated_size}
	mut dst_offset := 0

	// 남은 데이터 압축 해제 (헤더 이후부터)
	mut src_offset := int(src_size)
	mut remaining := usize(data.len - src_offset)

	for remaining > 0 {
		mut dst_size := usize(result.len - dst_offset)
		mut src_consumed := remaining

		decomp_result := C.LZ4F_decompress(dctx, unsafe { &u8(result.data) + dst_offset },
			&dst_size, unsafe { &u8(data.data) + src_offset }, &src_consumed, unsafe { nil })

		if C.LZ4F_isError(decomp_result) != 0 {
			err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(decomp_result)) }
			return error('lz4 frame decompression failed: ${err_name}')
		}

		dst_offset += int(dst_size)
		src_offset += int(src_consumed)
		remaining -= src_consumed

		// 버퍼 확장이 필요한 경우
		if dst_offset >= result.len && remaining > 0 {
			new_size := result.len * 2
			if new_size > 268435456 {
				// 256MB 제한
				return error('lz4 frame: decompressed data too large')
			}
			mut new_result := []u8{len: new_size, cap: new_size}
			for i := 0; i < dst_offset; i++ {
				new_result[i] = result[i]
			}
			result = unsafe { new_result }
		}

		// 프레임 끝에 도달
		if decomp_result == 0 {
			break
		}
	}

	result = unsafe { result[..dst_offset] }

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 frame decompressed', observability.field_int('compressed_size',
		data.len), observability.field_int('decompressed_size', result.len))

	return result
}

/// compression_type은 압축 타입을 반환합니다.
pub fn (c &Lz4CompressorC) compression_type() CompressionType {
	return CompressionType.lz4
}

// LZ4 Frame API C 함수 선언 (lz4frame.h)
fn C.LZ4F_compressFrameBound(srcSize usize, prefsPtr voidptr) usize
fn C.LZ4F_compressFrame(dstBuffer voidptr, dstCapacity usize, srcBuffer voidptr, srcSize usize, prefsPtr voidptr) usize
fn C.LZ4F_createDecompressionContext(dctxPtr voidptr, version u32) usize
fn C.LZ4F_freeDecompressionContext(dctx voidptr) usize
fn C.LZ4F_getFrameInfo(dctx voidptr, frameInfoPtr voidptr, srcBuffer voidptr, srcSizePtr &usize) usize
fn C.LZ4F_decompress(dctx voidptr, dstBuffer voidptr, dstSizePtr &usize, srcBuffer voidptr, srcSizePtr &usize, dOptsPtr voidptr) usize
fn C.LZ4F_isError(code usize) u32
fn C.LZ4F_getErrorName(code usize) &char

// LZ4F 버전 상수
const lz4f_version = u32(100)

// Lz4FrameInfo 구조체 (V에서 사용) - snake_case 적용
struct Lz4FrameInfo {
	block_size_id         u32
	block_mode            u32
	content_checksum_flag u32
	frame_type            u32
	content_size          u64
	dict_id               u32
	block_checksum_flag   u32
}
