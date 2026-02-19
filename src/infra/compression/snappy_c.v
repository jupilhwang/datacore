/// Infrastructure layer - Snappy compression (using C library)
/// High-performance compression/decompression using the Google Snappy C library
module compression

import infra.observability

// Link C library
#flag -L/opt/homebrew/lib -lsnappy
#flag -I/opt/homebrew/include
#include <snappy-c.h>

/// SnappyCompressorC is a Snappy compressor using the C library.
pub struct SnappyCompressorC {
}

/// new_snappy_compressor_c creates a new SnappyCompressorC using the C library.
pub fn new_snappy_compressor_c() &SnappyCompressorC {
	return &SnappyCompressorC{}
}

/// compress compresses data into Snappy format.
pub fn (c &SnappyCompressorC) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Calculate output buffer size
	mut max_out_len := int(C.snappy_max_compressed_length(usize(data.len)))
	mut result := []u8{len: max_out_len, cap: max_out_len}
	mut out_len := usize(max_out_len)

	// C call
	snappy_status := C.snappy_compress(data.data, usize(data.len), result.data, &out_len)
	if snappy_status != 0 {
		return error('snappy compression failed with status: ${snappy_status}')
	}

	unsafe {
		result = result[..int(out_len)]
	}
	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress decompresses Snappy format data.
pub fn (c &SnappyCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Calculate output buffer size
	mut uncompressed_len := usize(0)
	snappy_len_status := C.snappy_uncompressed_length(data.data, usize(data.len), &uncompressed_len)
	if snappy_len_status != 0 {
		return error('failed to get snappy uncompressed length: ${snappy_len_status}')
	}

	if uncompressed_len == 0 {
		return []u8{}
	}

	mut result := []u8{len: int(uncompressed_len), cap: int(uncompressed_len)}
	mut out_len := uncompressed_len

	// C call
	snappy_status := C.snappy_uncompress(data.data, usize(data.len), result.data, &out_len)
	if snappy_status != 0 {
		return error('snappy decompression failed with status: ${snappy_status}')
	}

	unsafe {
		result = result[..int(out_len)]
	}
	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// compression_type returns the compression type.
pub fn (c &SnappyCompressorC) compression_type() CompressionType {
	return CompressionType.snappy
}

/// snappy_max_compressed_length returns the maximum compressed size for the given input length.
fn snappy_max_compressed_length(input_len int) int {
	return input_len + (input_len >> 6) + 32
}

// C function declarations (provided by snappy-c.h)
fn C.snappy_compress(src &u8, src_len usize, dst &u8, dst_len &usize) int
fn C.snappy_uncompress(src &u8, src_len usize, dst &u8, dst_len &usize) int
fn C.snappy_uncompressed_length(compressed &u8, compressed_len usize, result &usize) int
fn C.snappy_max_compressed_length(source_len usize) usize
