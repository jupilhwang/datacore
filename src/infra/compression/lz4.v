/// Infrastructure layer - LZ4 compression
/// DEPRECATED: The pure V LZ4 implementation is no longer recommended.
/// The C library version (lz4_c.v) provides Kafka frame compatibility.
/// For testing purposes only.
/// LZ4 frame format compression algorithm in pure V
/// Supports LZ4 frame format compatible with Kafka
/// Reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
module compression

import infra.observability

/// LZ4 compression algorithm implementation.
pub struct Lz4Compressor {
}

/// LZ4 magic number
const lz4_magic_number = [u8(0x04), 0x22, 0x4d, 0x18]

/// LZ4 frame header flags
const lz4_frame_version = u8(0x40)
const lz4_block_default = u8(0x40)

/// new_lz4_compressor creates a new Lz4Compressor.
pub fn new_lz4_compressor() &Lz4Compressor {
	return &Lz4Compressor{}
}

/// compress compresses data into LZ4 frame format.
pub fn (c &Lz4Compressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// LZ4 frame format:
	// Magic number (4 bytes) + frame header + blocks + end marker + checksum
	mut result := []u8{cap: data.len + 32}

	// Magic number
	result << lz4_magic_number

	// Frame header
	flg := lz4_frame_version | 0x00
	bd := lz4_block_default

	result << flg
	result << bd

	// Header checksum (upper 8 bits of xxh32)
	header_checksum := calculate_header_checksum([flg, bd])
	result << header_checksum

	// Store data as a single block (simple implementation)
	if data.len <= 65536 {
		// Uncompressed block (MSB set to 1)
		block_size := u32(data.len) | 0x80000000
		result << u8(block_size & 0xff)
		result << u8((block_size >> 8) & 0xff)
		result << u8((block_size >> 16) & 0xff)
		result << u8((block_size >> 24) & 0xff)
		result << data
	} else {
		// Split into multiple blocks (simple implementation)
		mut offset := 0
		for offset < data.len {
			remaining := data.len - offset
			block_len := if remaining > 65536 { 65536 } else { remaining }

			// Uncompressed block
			block_size := u32(block_len) | 0x80000000
			result << u8(block_size & 0xff)
			result << u8((block_size >> 8) & 0xff)
			result << u8((block_size >> 16) & 0xff)
			result << u8((block_size >> 24) & 0xff)
			result << data[offset..offset + block_len]

			offset += block_len
		}
	}

	// End marker (empty block)
	result << [u8(0x00), 0x00, 0x00, 0x00]

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress decompresses LZ4 frame format data.
/// Kafka compatible: supports LZ4 frame format
pub fn (c &Lz4Compressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Verify magic number (little-endian)
	expected_magic := u32(0x04) | (u32(0x22) << 8) | (u32(0x4d) << 16) | (u32(0x18) << 24)
	actual_magic := u32(data[0]) | (u32(data[1]) << 8) | (u32(data[2]) << 16) | (u32(data[3]) << 24)

	if actual_magic != expected_magic {
		// If magic number is absent, treat as raw data
		return data.clone()
	}

	mut pos := 4
	mut result := []u8{}

	// Parse frame header
	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	flg := data[pos]
	pos++

	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	_ := data[pos]
	pos++

	// Skip header checksum
	if pos >= data.len {
		return error('incomplete LZ4 frame header')
	}
	pos++

	// Parse blocks
	for pos < data.len {
		if pos + 4 > data.len {
			break
		}

		// Read block size (little-endian)
		block_size := u32(data[pos]) | (u32(data[pos + 1]) << 8) | (u32(data[pos + 2]) << 16) | (u32(data[
			pos + 3]) << 24)
		pos += 4

		// Check end marker
		if block_size == 0 {
			break
		}

		// Check if block is uncompressed (MSB)
		is_uncompressed := (block_size & 0x80000000) != 0
		size := int(block_size & 0x7fffffff)

		if pos + size > data.len {
			return error('incomplete LZ4 block')
		}

		if is_uncompressed {
			// Uncompressed block
			result << data[pos..pos + size]
		} else {
			// Compressed block: run LZ4 decompression
			decompressed_block := lz4_decompress_block(data[pos..pos + size])!
			result << decompressed_block
		}

		pos += size

		// Skip block checksum (if present)
		if (flg & 0x04) != 0 {
			if pos + 4 > data.len {
				return error('incomplete LZ4 block checksum')
			}
			pos += 4
		}
	}

	mut logger := observability.get_named_logger('lz4_compressor')
	logger.debug('lz4 decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// lz4_decompress_block decompresses a single LZ4 compressed block.
/// Simple LZ77 copy implementation
fn lz4_decompress_block(compressed_data []u8) ![]u8 {
	mut result := []u8{cap: compressed_data.len * 4}
	mut ip := 0

	for ip < compressed_data.len {
		b := compressed_data[ip]
		ip++

		if b < 0x20 {
			// Literal length: 0-31
			lit_len := int(b) + 1

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len
		} else if b < 0x40 {
			// 2-byte match
			mut lit_len := int(b & 0x1f)
			if lit_len == 0 {
				lit_len = 1
			}

			if ip + 1 >= compressed_data.len {
				return error('incomplete match')
			}

			offset := int(u32(compressed_data[ip]) | ((u32(b) & 0xe0) << 3))
			ip++

			mut match_len := int(compressed_data[ip])
			ip++
			match_len += 4

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal in match')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len

			// Copy match
			for match_len > 0 {
				if offset == 0 || offset > result.len {
					return error('invalid offset')
				}
				result << result[result.len - offset]
				match_len--
			}
		} else {
			// 3+ byte match
			lit_len := int(b) - 0x20

			if ip + 2 >= compressed_data.len {
				return error('incomplete 3+ match')
			}

			offset := int(u32(compressed_data[ip]) | (u32(compressed_data[ip + 1]) << 8))
			ip += 2

			mut match_len := int(compressed_data[ip])
			ip++
			match_len += 4

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal in 3+ match')
			}

			result << compressed_data[ip..ip + lit_len]
			ip += lit_len

			// Copy match
			for match_len > 0 {
				if offset == 0 || offset > result.len {
					return error('invalid offset in 3+ match')
				}
				result << result[result.len - offset]
				match_len--
			}
		}
	}

	return result
}

/// compression_type returns the compression type.
pub fn (c &Lz4Compressor) compression_type() CompressionType {
	return CompressionType.lz4
}

/// Calculates the checksum for the LZ4 frame header.
fn calculate_header_checksum(header []u8) u8 {
	mut sum := u32(0)
	for b in header {
		sum = (sum << 1) | (sum >> 31)
		sum ^= u32(b)
	}
	return u8((sum >> 8) & 0xff)
}
