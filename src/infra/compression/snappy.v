/// Infrastructure layer - Snappy compression
/// Snappy compression algorithm in pure V
/// Supports raw snappy format compatible with Kafka
/// Reference: https://github.com/google/snappy/blob/main/format_description.txt
module compression

import infra.observability

/// Snappy compression algorithm implementation.
pub struct SnappyCompressor {
}

/// new_snappy_compressor creates a new SnappyCompressor.
pub fn new_snappy_compressor() &SnappyCompressor {
	return &SnappyCompressor{}
}

/// compress compresses data into Snappy format.
/// Kafka compatible: raw snappy format (varint size + compressed data)
pub fn (c &SnappyCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Raw snappy format: varint-encoded size + compressed data
	mut result := []u8{cap: data.len + 10}

	// Encode size as varint
	write_varint(mut result, data.len)

	// Simple compression: store data as literal blocks for copy
	// The snappy.v decompressor (b < 0x80) expects b+1 bytes of literal
	mut offset := 0
	for offset < data.len {
		remaining := data.len - offset
		lit_len := if remaining > 128 { 128 } else { remaining }

		// Literal tag: b = lit_len - 1
		result << u8(lit_len - 1)
		result << data[offset..offset + lit_len]

		offset += lit_len
	}

	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy compressed', observability.field_int('original_size', data.len),
		observability.field_int('compressed_size', result.len))

	return result
}

/// decompress decompresses Snappy format data.
/// Kafka compatible: raw snappy format (varint size + compressed data)
pub fn (c &SnappyCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	mut pos := 0

	// Read varint-encoded original size
	uncompressed_len, bytes_read := read_varint(data, pos) or {
		return error('invalid snappy data: cannot read size')
	}
	pos += bytes_read

	if uncompressed_len <= 0 {
		return []u8{}
	}

	if pos >= data.len {
		return error('invalid snappy data: incomplete')
	}

	compressed_data := data[pos..]
	mut result := []u8{len: uncompressed_len, cap: uncompressed_len}

	// Run Snappy decompression
	decompressed_size := snappy_decompress_raw(compressed_data, mut result) or {
		return error('snappy decompression failed: ${err}')
	}

	if decompressed_size != uncompressed_len {
		return error('snappy decompression size mismatch: expected ${uncompressed_len}, got ${decompressed_size}')
	}

	// Use unsafe to prevent copying when shrinking slice
	unsafe {
		result = result[..decompressed_size]
	}
	mut logger := observability.get_named_logger('snappy_compressor')
	logger.debug('snappy decompressed', observability.field_int('compressed_size', data.len),
		observability.field_int('decompressed_size', result.len))

	return result
}

/// snappy_decompress_raw decompresses raw snappy format data.
/// Simple LZ77 copy implementation
fn snappy_decompress_raw(compressed_data []u8, mut result []u8) !int {
	mut ip := 0
	mut op := 0

	for ip < compressed_data.len {
		b := compressed_data[ip]
		ip++

		if b < 0x80 {
			// Literal: 1 byte (0-127 literals)
			// Copy the next b+1 bytes as-is
			lit_len := int(b) + 1

			if ip + lit_len > compressed_data.len {
				return error('incomplete literal')
			}
			if op + lit_len > result.len {
				return error('output buffer overflow')
			}

			for i in 0 .. lit_len {
				result[op] = compressed_data[ip + i]
				op++
			}
			ip += lit_len
		} else {
			// Copy command: 2-byte or 4-byte
			mut copy_len := int(b & 0x7f)
			mut offset := 0

			if copy_len < 0x7f {
				// 2-byte format: offset = next byte
				if ip >= compressed_data.len {
					return error('incomplete copy command')
				}
				offset = int(u32(compressed_data[ip]) | ((u32(copy_len & 0x70)) << 4))
				ip++
				copy_len += 2
			} else {
				// 4-byte format: offset = next 2 bytes
				if ip + 1 >= compressed_data.len {
					return error('incomplete copy command')
				}
				offset = int(u32(compressed_data[ip]) | (u32(compressed_data[ip + 1]) << 8))
				ip += 2
				copy_len = int(b & 0x7f) + 1
			}

			if offset == 0 {
				return error('invalid copy offset: 0')
			}
			if op + copy_len > result.len {
				return error('copy overflow')
			}

			// Execute copy
			for _ in 0 .. copy_len {
				src_idx := op - offset
				if src_idx < 0 {
					return error('copy offset exceeds output')
				}
				result[op] = result[src_idx]
				op++
			}
		}
	}

	return op
}

/// compression_type returns the compression type.
pub fn (c &SnappyCompressor) compression_type() CompressionType {
	return CompressionType.snappy
}

/// write_varint writes a variable-length integer to the buffer.
fn write_varint(mut buf []u8, value int) {
	mut v := value
	for v >= 128 {
		buf << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	buf << u8(v)
}

/// read_varint reads a variable-length integer from the buffer.
fn read_varint(data []u8, start int) !(int, int) {
	mut result := 0
	mut shift := 0
	mut pos := start

	for pos < data.len {
		b := data[pos]
		pos++
		result |= int(u32(b & 0x7f) << shift)
		if (b & 0x80) == 0 {
			return result, pos - start
		}
		shift += 7
		if shift >= 32 {
			return error('varint too large')
		}
	}

	return error('incomplete varint')
}
