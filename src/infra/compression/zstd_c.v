/// Infrastructure layer - Zstd compression (using C library)
/// High-performance compression/decompression using the Facebook Zstd C library (Kafka compatible)
module compression

import infra.observability

// Link C library
#flag -L/opt/homebrew/lib -lzstd
#flag -I/opt/homebrew/include
#include <zstd.h>

// ZSTD special return value constants
const zstd_contentsize_unknown = u64(0) - 1
const zstd_contentsize_error = u64(0) - 2

/// ZstdCompressorC is a Zstd compressor using the C library.
/// Uses ZSTD Frame Format compatible with Kafka.
pub struct ZstdCompressorC {
	level int
}

/// new_zstd_compressor_c creates a new ZstdCompressorC using the C library.
/// The default compression level is 3.
pub fn new_zstd_compressor_c() &ZstdCompressorC {
	return &ZstdCompressorC{
		level: 3
	}
}

/// new_zstd_compressor_c_with_level creates a ZstdCompressorC with the specified compression level.
/// Level: 1-22 (1=fastest, 22=best compression)
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

/// compress compresses data into Zstd Frame format.
/// Produces ZSTD Frame Format (Magic: 0xFD2FB528) compatible with Kafka.
pub fn (c &ZstdCompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Calculate output buffer size
	max_dst_size := C.ZSTD_compressBound(usize(data.len))
	mut result := []u8{len: int(max_dst_size), cap: int(max_dst_size)}

	// C call - ZSTD_compress compresses in frame format
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

// zstd_int_max is the maximum safe value for casting u64 to int without overflow.
const zstd_int_max = u64(0x7fff_ffff)

/// decompress decompresses Zstd Frame format data.
/// Can handle ZSTD-compressed data produced by Kafka.
/// Guards against u64->int overflow when ZSTD_getFrameContentSize returns
/// ZSTD_CONTENTSIZE_ERROR (u64_max - 1) or values larger than 2GB.
pub fn (c &ZstdCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Read original size from frame
	content_size := C.ZSTD_getFrameContentSize(data.data, usize(data.len))

	// Guard: ZSTD_CONTENTSIZE_ERROR = u64_max - 1, ZSTD_CONTENTSIZE_UNKNOWN = u64_max.
	// Any value near u64 max is either an error or unknown — use streaming instead.
	// Also guard against values that would overflow int (> 2GB).
	if content_size >= zstd_contentsize_error || content_size > zstd_int_max {
		// ZSTD_CONTENTSIZE_ERROR means invalid frame header
		if content_size == zstd_contentsize_error {
			return error('zstd decompression failed: invalid frame header')
		}
		// ZSTD_CONTENTSIZE_UNKNOWN or oversized: fall back to streaming
		return c.decompress_streaming(data)
	}

	// Use streaming decompression when original size is unknown or zero
	if content_size == zstd_contentsize_unknown || content_size == 0 {
		return c.decompress_streaming(data)
	}

	mut result := []u8{len: int(content_size), cap: int(content_size)}

	// C call
	decompressed_size := C.ZSTD_decompress(result.data, usize(content_size), data.data,
		usize(data.len))

	if C.ZSTD_isError(decompressed_size) != 0 {
		err_name := C.ZSTD_getErrorName(decompressed_size)
		return error('zstd decompression failed: ${cstring_to_string(err_name)}')
	}

	// Guard: decompressed_size must not exceed int range before slicing
	if decompressed_size > zstd_int_max {
		return error('zstd: decompressed data exceeds 2GB limit')
	}

	result = unsafe { result[..int(decompressed_size)] }

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd frame decompressed', observability.field_int('compressed_size',
		data.len), observability.field_int('decompressed_size', result.len))

	return result
}

/// decompress_streaming decompresses data using streaming mode when the original size is unknown.
fn (c &ZstdCompressorC) decompress_streaming(data []u8) ![]u8 {
	// Create streaming decompression context
	dctx := C.ZSTD_createDCtx()
	if dctx == unsafe { nil } {
		return error('zstd: failed to create decompression context')
	}
	defer {
		C.ZSTD_freeDCtx(dctx)
	}

	// Initial buffer size (assume 1:4 ratio, minimum 64KB)
	mut estimated_size := data.len * 4
	if estimated_size < 65536 {
		estimated_size = 65536
	}

	mut result := []u8{len: estimated_size, cap: estimated_size}

	// Set up input/output buffers
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

	// Streaming decompression
	for {
		ret := C.ZSTD_decompressStream(dctx, &out_buf, &in_buf)

		if C.ZSTD_isError(ret) != 0 {
			err_name := C.ZSTD_getErrorName(ret)
			return error('zstd streaming decompression failed: ${cstring_to_string(err_name)}')
		}

		// Complete
		if ret == 0 {
			break
		}

		// Expand buffer if full
		if out_buf.pos == out_buf.size {
			new_size := result.len * 2
			if new_size > 268435456 {
				// 256MB limit
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

/// compression_type returns the compression type.
pub fn (c &ZstdCompressorC) compression_type() CompressionType {
	return CompressionType.zstd
}

// ZstdInBuffer struct (snake_case)
struct ZstdInBuffer {
	src  voidptr
	size usize
mut:
	pos usize
}

// ZstdOutBuffer struct (snake_case)
struct ZstdOutBuffer {
mut:
	dst  voidptr
	size usize
	pos  usize
}

// C function declarations (provided by zstd.h)
fn C.ZSTD_compress(dst &u8, dstCapacity usize, src &u8, srcSize usize, compressionLevel int) usize
fn C.ZSTD_decompress(dst &u8, dstCapacity usize, src &u8, compressedSize usize) usize
fn C.ZSTD_compressBound(srcSize usize) usize
fn C.ZSTD_getErrorName(code usize) &u8
fn C.ZSTD_isError(code usize) int
fn C.ZSTD_getFrameContentSize(src &u8, srcSize usize) u64
fn C.ZSTD_createDCtx() voidptr
fn C.ZSTD_freeDCtx(dctx voidptr) usize
fn C.ZSTD_decompressStream(dctx voidptr, output &ZstdOutBuffer, input &ZstdInBuffer) usize
