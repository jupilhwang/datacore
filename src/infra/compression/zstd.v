/// Infrastructure layer - ZSTD compression
/// DEPRECATED: The pure V ZSTD implementation is no longer recommended.
/// The C library version (zstd_c.v) provides full functionality and performance.
/// For testing purposes only.
/// ZSTD frame format compression algorithm in pure V
/// Supports ZSTD frame format compatible with Kafka
/// Reference: https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md
module compression

import infra.observability

/// ZSTD compression algorithm implementation.
pub struct ZstdCompressor {
	level int
}

/// ZSTD magic number
const zstd_magic_number = u32(0xfd2fb528)

/// new_zstd_compressor creates a new ZstdCompressor.
/// The default compression level is 3.
pub fn new_zstd_compressor() &ZstdCompressor {
	return &ZstdCompressor{
		level: 3
	}
}

/// new_zstd_compressor_with_level creates a ZstdCompressor with the specified compression level.
/// Level: 1-22 (1=fastest, 22=best compression)
pub fn new_zstd_compressor_with_level(level int) &ZstdCompressor {
	mut lvl := level
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 22 {
		lvl = 22
	}
	return &ZstdCompressor{
		level: lvl
	}
}

/// compress compresses data into ZSTD frame format.
/// Uses a simple frame wrapper implementation.
pub fn (c &ZstdCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// ZSTD frame format:
	// Magic number (4 bytes) + frame header + blocks + [optional checksum]
	mut result := []u8{cap: data.len + 32}

	// Magic number (little-endian)
	result << u8(zstd_magic_number & 0xff)
	result << u8((zstd_magic_number >> 8) & 0xff)
	result << u8((zstd_magic_number >> 16) & 0xff)
	result << u8((zstd_magic_number >> 24) & 0xff)

	// Frame header
	fhd := u8(0xA0)
	result << fhd

	// Frame_Content_Size (4 bytes, little-endian)
	result << u8(data.len & 0xff)
	result << u8((data.len >> 8) & 0xff)
	result << u8((data.len >> 16) & 0xff)
	result << u8((data.len >> 24) & 0xff)

	// Raw_Block (uncompressed block)
	block_header := u32(1) | (u32(0) << 1) | (u32(data.len) << 3)
	result << u8(block_header & 0xff)
	result << u8((block_header >> 8) & 0xff)
	result << u8((block_header >> 16) & 0xff)

	// Block data
	result << data

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len), observability.field_int('level',
		c.level))

	return result
}

/// decompress decompresses ZSTD frame format data.
/// Kafka compatible: supports ZSTD frame format
pub fn (c &ZstdCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Verify magic number
	if data.len < 4 {
		return error('incomplete ZSTD frame')
	}

	magic := u32(data[0]) | (u32(data[1]) << 8) | (u32(data[2]) << 16) | (u32(data[3]) << 24)

	if magic != zstd_magic_number {
		// If magic number is absent, treat as raw data
		return data.clone()
	}

	mut pos := 4
	mut result := []u8{}

	// Parse frame header
	if pos >= data.len {
		return error('incomplete ZSTD frame header')
	}
	fhd := data[pos]
	pos++

	// Parse flags
	fcs_flag := (fhd >> 6) & 0x03
	single_segment := (fhd & 0x20) != 0

	// Window_Descriptor (when not Single_Segment)
	if !single_segment {
		if pos >= data.len {
			return error('incomplete ZSTD frame header')
		}
		pos++
	}

	// Read Frame_Content_Size
	mut frame_content_size := u64(0)
	match fcs_flag {
		0 {
			if single_segment {
				if pos >= data.len {
					return error('incomplete ZSTD frame header')
				}
				frame_content_size = u64(data[pos])
				pos++
			}
		}
		1 {
			if pos + 2 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8)
			pos += 2
		}
		2 {
			if pos + 4 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8) | (u64(data[pos + 2]) << 16) | (u64(data[
				pos + 3]) << 24)
			pos += 4
		}
		3 {
			if pos + 8 > data.len {
				return error('incomplete ZSTD frame header')
			}
			frame_content_size = u64(data[pos]) | (u64(data[pos + 1]) << 8) | (u64(data[pos + 2]) << 16) | (u64(data[
				pos + 3]) << 24) | (u64(data[pos + 4]) << 32) | (u64(data[pos + 5]) << 40) | (u64(data[
				pos + 6]) << 48) | (u64(data[pos + 7]) << 56)
			pos += 8
		}
		else {}
	}

	_ = frame_content_size

	// Parse blocks
	for pos < data.len {
		if pos + 3 > data.len {
			break
		}

		// Block header (3 bytes, little-endian)
		block_header := u32(data[pos]) | (u32(data[pos + 1]) << 8) | (u32(data[pos + 2]) << 16)
		pos += 3

		last_block := (block_header & 0x00000001) != 0
		block_type := (block_header >> 1) & 0x03
		block_size := int((block_header >> 3) & 0x1fffff)

		if pos + block_size > data.len {
			return error('incomplete ZSTD block')
		}

		match block_type {
			0 {
				// Raw_Block (uncompressed)
				result << data[pos..pos + block_size]
			}
			1 {
				// RLE_Block
				if block_size > 0 {
					byte_val := data[pos]
					// Repeat for frame content size
					repeat_count := if frame_content_size > 0 {
						int(frame_content_size)
					} else {
						block_size
					}
					for _ in 0 .. repeat_count {
						result << byte_val
					}
				}
			}
			2 {
				// Compressed_Block: run ZSTD decompression
				decompressed_block := zstd_decompress_block(data[pos..pos + block_size])!
				result << decompressed_block
			}
			3 {
				// Reserved
				return error('invalid ZSTD block type')
			}
			else {}
		}

		pos += block_size

		if last_block {
			break
		}
	}

	mut logger := observability.get_named_logger('zstd_compressor')
	logger.debug('zstd decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// zstd_decompress_block decompresses a single ZSTD compressed block.
/// Simple FSE/Huffman copy implementation
fn zstd_decompress_block(compressed_data []u8) ![]u8 {
	// ZSTD decompression requires a complex algorithm.
	// Not supported in this simple implementation; returns an error.
	return error('ZSTD compressed block decompression requires full FSE/Huffman implementation')
}

/// compression_type returns the compression type.
pub fn (c &ZstdCompressor) compression_type() CompressionType {
	return CompressionType.zstd
}
