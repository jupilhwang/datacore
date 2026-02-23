/// Infrastructure layer - Gzip compression
/// Gzip Compressor implementation using the V standard library compress.gzip
module compression

import compress.gzip

/// Gzip compression algorithm implementation.
pub struct GzipCompressor {
	level int
}

/// new_gzip_compressor creates a new GzipCompressor.
/// The default compression level is 6 (balanced).
pub fn new_gzip_compressor() &GzipCompressor {
	return &GzipCompressor{
		level: 6
	}
}

/// new_gzip_compressor_with_level creates a GzipCompressor with the specified compression level.
/// Level: 1-9 (1=fastest, 9=best compression)
pub fn new_gzip_compressor_with_level(level int) &GzipCompressor {
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
pub fn (c &GzipCompressor) decompress(data []u8) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	decompressed := gzip.decompress(data) or { return error('gzip decompression failed: ${err}') }

	return decompressed
}

/// compression_type returns the compression type.
pub fn (c &GzipCompressor) compression_type() CompressionType {
	return CompressionType.gzip
}
