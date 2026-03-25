/// Infrastructure layer - Gzip compression
/// Gzip Compressor implementation using the V standard library compress.gzip
module compression

import compress.gzip

// gzip_magic_0 and gzip_magic_1 are the two-byte gzip magic number (0x1f 0x8b).
const gzip_magic_0 = u8(0x1f)
const gzip_magic_1 = u8(0x8b)

/// Gzip compression algorithm implementation.
pub struct GzipCompressor {
	level int
}

/// new_gzip_compressor creates a new GzipCompressor.
/// The default compression level is 6 (balanced).
fn new_gzip_compressor() &GzipCompressor {
	return &GzipCompressor{
		level: 6
	}
}

/// new_gzip_compressor_with_level creates a GzipCompressor with the specified compression level.
/// Level: 1-9 (1=fastest, 9=best compression)
fn new_gzip_compressor_with_level(level int) &GzipCompressor {
	mut lvl := level
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 9 {
		lvl = 9
	}
	return &GzipCompressor{
		level: lvl
	}
}

/// compress compresses data using Gzip.
pub fn (c &GzipCompressor) compress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	compressed := gzip.compress(data) or { return error('gzip compression failed: ${err}') }

	return compressed
}

/// decompress decompresses Gzip-compressed data.
/// Handles two variants:
///   1. Standard gzip stream (magic 1F 8B at offset 0): decompress directly.
///   2. Kafka kafka-clients format (4-byte BE length prefix before gzip stream):
///      [4 bytes original_size BE] [gzip magic 1F 8B ...]
pub fn (c &GzipCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Detect Kafka's 4-byte prefix: magic is NOT at offset 0 but IS at offset 4.
	actual_data := if has_kafka_gzip_prefix(data) { data[4..] } else { data }

	decompressed := gzip.decompress(actual_data) or {
		return error('gzip decompression failed: ${err}')
	}

	return decompressed
}

/// has_kafka_gzip_prefix reports whether data has a 4-byte Kafka length prefix
/// before a valid gzip magic (1F 8B).
fn has_kafka_gzip_prefix(data []u8) bool {
	if data.len < 6 {
		return false
	}
	// Gzip magic must NOT be at offset 0
	if data[0] == gzip_magic_0 && data[1] == gzip_magic_1 {
		return false
	}
	// Gzip magic must be at offset 4
	return data[4] == gzip_magic_0 && data[5] == gzip_magic_1
}

/// compression_type returns the compression type.
pub fn (c &GzipCompressor) compression_type() CompressionType {
	return CompressionType.gzip
}
