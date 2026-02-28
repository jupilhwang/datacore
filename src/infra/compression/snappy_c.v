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

// xerial_snappy_magic is the 8-byte magic header used by xerial snappy-java (kafka-clients).
// Format: 0x82 'S' 'N' 'A' 'P' 'P' 'Y' 0x00
const xerial_snappy_magic = [u8(0x82), 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]

// xerial_snappy_header_len is the total size of the xerial snappy-java file header (16 bytes):
//   8 bytes magic + 4 bytes version + 4 bytes compatible version
const xerial_snappy_header_len = 16

// snappy_ppy_magic is the 4-byte magic header used by the old snappy-java format (1.0.x).
// Produced by older Kafka clients using snappy-java prior to the 0x82 SNAPPY\0 format.
// Format: 'P' 'P' 'Y' 0x00
const snappy_ppy_magic = [u8(0x50), 0x50, 0x59, 0x00]

// snappy_ppy_header_len is the total size of the old PPY\0 format header (12 bytes):
//   4 bytes magic + 4 bytes version + 4 bytes chunk_count
const snappy_ppy_header_len = 12

/// decompress decompresses Snappy format data.
/// Supports three formats:
///   1. xerial snappy-java new (kafka-clients Java): 0x82SNAPPY\0 16-byte header + chunk loop
///   2. snappy-java old PPY\0 format (older Kafka clients): PPY\0 12-byte header + chunk loop
///   3. Raw snappy (C library native format, no varint prefix)
pub fn (c &SnappyCompressorC) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Detect any xerial-family snappy format (new 0x82SNAPPY\0 or old PPY\0).
	if is_xerial_snappy(data) {
		return c.decompress_xerial(data)
	}

	// Raw snappy: pass data directly to the C library without stripping any prefix.
	// The C snappy_compress() output has no varint prefix; snappy_uncompressed_length
	// operates correctly at offset 0.
	return c.decompress_raw(data)
}

/// is_xerial_snappy reports whether data begins with either the new xerial snappy-java
/// magic (0x82 SNAPPY\0) or the old PPY\0 magic used by older snappy-java versions.
fn is_xerial_snappy(data []u8) bool {
	if data.len < snappy_ppy_magic.len {
		return false
	}
	// Check new xerial format first (8-byte magic).
	if data.len >= xerial_snappy_header_len {
		mut matched := true
		for i in 0 .. xerial_snappy_magic.len {
			if data[i] != xerial_snappy_magic[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	// Check old PPY\0 format (4-byte magic).
	for i in 0 .. snappy_ppy_magic.len {
		if data[i] != snappy_ppy_magic[i] {
			return false
		}
	}
	return true
}

/// decompress_xerial decompresses xerial-family snappy framed data.
/// Dispatches to the correct chunk parser based on the detected magic header:
///   - New xerial (0x82SNAPPY\0): chunks contain [4B uncomp_len][4B comp_len][data]
///   - Old PPY\0: chunks contain [4B comp_len][data] (no separate uncompressed_len field)
fn (c &SnappyCompressorC) decompress_xerial(data []u8) ![]u8 {
	if is_ppy_snappy(data) {
		return c.decompress_xerial_ppy(data)
	}
	return c.decompress_xerial_new(data)
}

/// is_ppy_snappy reports whether data begins with the old PPY\0 magic.
fn is_ppy_snappy(data []u8) bool {
	if data.len < snappy_ppy_magic.len {
		return false
	}
	for i in 0 .. snappy_ppy_magic.len {
		if data[i] != snappy_ppy_magic[i] {
			return false
		}
	}
	return true
}

/// decompress_xerial_new decompresses new-style xerial snappy-java framed data (kafka-clients format).
/// Frame layout:
///   [8 bytes magic 0x82SNAPPY\0] [4 bytes version BE] [4 bytes compat version BE]
///   then one or more chunks:
///     [4 bytes uncompressed_len BE] [4 bytes compressed_len BE] [compressed_len bytes]
fn (c &SnappyCompressorC) decompress_xerial_new(data []u8) ![]u8 {
	mut result := []u8{}
	mut pos := xerial_snappy_header_len // skip 16-byte header

	for pos < data.len {
		// Need at least 8 bytes for the two length fields
		if pos + 8 > data.len {
			return error('snappy xerial: truncated chunk header at offset ${pos}')
		}

		uncompressed_len := read_be_i32(data, pos)
		compressed_len := read_be_i32(data, pos + 4)
		pos += 8

		if compressed_len < 0 || uncompressed_len < 0 {
			return error('snappy xerial: negative chunk length (u=${uncompressed_len}, c=${compressed_len})')
		}
		if pos + compressed_len > data.len {
			return error('snappy xerial: chunk data truncated at offset ${pos}')
		}

		chunk := data[pos..pos + compressed_len]
		pos += compressed_len

		decoded := c.decompress_raw(chunk)!
		result << decoded
	}

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy xerial decompressed', observability.field_int('compressed_size',
		data.len), observability.field_int('decompressed_size', result.len))

	return result
}

/// decompress_xerial_ppy decompresses old PPY\0 snappy-java framed data (older Kafka clients).
/// Frame layout:
///   [4 bytes magic "PPY\0"] [4 bytes version BE] [4 bytes chunk_count BE] = 12-byte header
///   then one or more chunks:
///     [4 bytes compressed_len BE] [compressed_len bytes of snappy data]
/// Note: unlike the new xerial format, there is no separate uncompressed_len field per chunk.
fn (c &SnappyCompressorC) decompress_xerial_ppy(data []u8) ![]u8 {
	mut result := []u8{}
	mut pos := snappy_ppy_header_len // skip 12-byte header

	for pos < data.len {
		// Need at least 4 bytes for the compressed_len field
		if pos + 4 > data.len {
			return error('snappy ppy: truncated chunk header at offset ${pos}')
		}

		compressed_len := read_be_i32(data, pos)
		pos += 4

		if compressed_len <= 0 {
			return error('snappy ppy: invalid compressed_len=${compressed_len} at offset ${pos - 4}')
		}
		if pos + compressed_len > data.len {
			return error('snappy ppy: chunk data truncated at offset ${pos}')
		}

		chunk := data[pos..pos + compressed_len]
		pos += compressed_len

		decoded := c.decompress_raw(chunk)!
		result << decoded
	}

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy ppy decompressed', observability.field_int('compressed_size',
		data.len), observability.field_int('decompressed_size', result.len))

	return result
}

/// decompress_raw decompresses a single raw snappy block via the C library.
fn (c &SnappyCompressorC) decompress_raw(snappy_data []u8) ![]u8 {
	if snappy_data.len == 0 {
		return []u8{}
	}

	// Calculate output buffer size from the C library
	mut uncompressed_len := usize(0)
	snappy_len_status := C.snappy_uncompressed_length(snappy_data.data, usize(snappy_data.len),
		&uncompressed_len)
	if snappy_len_status != 0 {
		return error('failed to get snappy uncompressed length: ${snappy_len_status}')
	}

	if uncompressed_len == 0 {
		return []u8{}
	}

	mut result := []u8{len: int(uncompressed_len), cap: int(uncompressed_len)}
	mut out_len := uncompressed_len

	// Decompress pure snappy data via C library
	snappy_status := C.snappy_uncompress(snappy_data.data, usize(snappy_data.len), result.data,
		&out_len)
	if snappy_status != 0 {
		return error('snappy decompression failed with status: ${snappy_status}')
	}

	unsafe {
		result = result[..int(out_len)]
	}
	return result
}

/// read_be_i32 reads a big-endian int32 from data at the given offset.
fn read_be_i32(data []u8, offset int) i32 {
	return i32(u32(data[offset]) << 24 | u32(data[offset + 1]) << 16 | u32(data[offset + 2]) << 8 | u32(data[
		offset + 3]))
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
