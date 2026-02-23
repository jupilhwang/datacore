/// Infrastructure layer - LZ4 compression (using C library)
/// High-performance compression/decompression using the LZ4 Frame API (Kafka compatible)
module compression

import infra.observability

// Link C library - using LZ4 Frame API
#flag -L/opt/homebrew/lib -llz4
#flag -I/opt/homebrew/include
#include <lz4frame.h>

/// Lz4CompressorC is an LZ4 compressor using the C library.
/// Uses LZ4 Frame Format for Kafka compatibility.
pub struct Lz4CompressorC {
}

/// new_lz4_compressor_c creates a new Lz4CompressorC using the C library.
pub fn new_lz4_compressor_c() &Lz4CompressorC {
	return &Lz4CompressorC{}
}

/// compress compresses data into LZ4 Frame format.
/// Produces LZ4 Frame Format (Magic: 0x184D2204) compatible with Kafka.
pub fn (c &Lz4CompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Calculate the maximum buffer size required for frame compression
	max_dst_size := C.LZ4F_compressFrameBound(usize(data.len), unsafe { nil })
	if max_dst_size == 0 {
		return error('lz4 frame: failed to calculate bound')
	}

	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// Compress the entire frame in a single call
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

/// decompress decompresses LZ4 Frame format data.
/// Can handle LZ4-compressed data produced by Kafka.
pub fn (c &Lz4CompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Create decompression context
	mut dctx := unsafe { nil }
	create_result := C.LZ4F_createDecompressionContext(&dctx, lz4f_version)
	if C.LZ4F_isError(create_result) != 0 {
		err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(create_result)) }
		return error('lz4 frame: failed to create decompression context: ${err_name}')
	}
	defer {
		C.LZ4F_freeDecompressionContext(dctx)
	}

	// Attempt to extract original size from the frame header
	mut frame_info := Lz4FrameInfo{}
	mut src_size := usize(data.len)
	header_result := C.LZ4F_getFrameInfo(dctx, &frame_info, data.data, &src_size)
	if C.LZ4F_isError(header_result) != 0 {
		err_name := unsafe { cstring_to_vstring(C.LZ4F_getErrorName(header_result)) }
		return error('lz4 frame: invalid frame header: ${err_name}')
	}

	// Determine original size (use from frame header if available, otherwise estimate)
	content_size := frame_info.content_size
	estimated_size := if content_size > 0 {
		int(content_size)
	} else {
		// Assume 1:4 ratio when original size is absent; minimum 64KB, maximum 64MB
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

	// Decompress remaining data (after the header)
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

		// Expand buffer if needed
		if dst_offset >= result.len && remaining > 0 {
			new_size := result.len * 2
			if new_size > 268435456 {
				// 256MB limit
				return error('lz4 frame: decompressed data too large')
			}
			mut new_result := []u8{len: new_size, cap: new_size}
			for i := 0; i < dst_offset; i++ {
				new_result[i] = result[i]
			}
			result = unsafe { new_result }
		}

		// Reached end of frame
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

/// compression_type returns the compression type.
pub fn (c &Lz4CompressorC) compression_type() CompressionType {
	return CompressionType.lz4
}

// LZ4 Frame API C function declarations (lz4frame.h)
fn C.LZ4F_compressFrameBound(srcSize usize, prefsPtr voidptr) usize
fn C.LZ4F_compressFrame(dstBuffer voidptr, dstCapacity usize, srcBuffer voidptr, srcSize usize, prefsPtr voidptr) usize
fn C.LZ4F_createDecompressionContext(dctxPtr voidptr, version u32) usize
fn C.LZ4F_freeDecompressionContext(dctx voidptr) usize
fn C.LZ4F_getFrameInfo(dctx voidptr, frameInfoPtr voidptr, srcBuffer voidptr, srcSizePtr &usize) usize
fn C.LZ4F_decompress(dctx voidptr, dstBuffer voidptr, dstSizePtr &usize, srcBuffer voidptr, srcSizePtr &usize, dOptsPtr voidptr) usize
fn C.LZ4F_isError(code usize) u32
fn C.LZ4F_getErrorName(code usize) &char

// LZ4F version constant
const lz4f_version = u32(100)

// Lz4FrameInfo struct (used in V) - snake_case applied
struct Lz4FrameInfo {
	block_size_id         u32
	block_mode            u32
	content_checksum_flag u32
	frame_type            u32
	content_size          u64
	dict_id               u32
	block_checksum_flag   u32
}
