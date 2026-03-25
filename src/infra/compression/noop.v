/// Infrastructure layer - Noop compression (pass-through)
/// Compressor implementation that passes data through without compression
module compression

/// NoopCompressor passes data through without compression.
pub struct NoopCompressor {
}

/// new_noop_compressor creates a new NoopCompressor.
fn new_noop_compressor() &NoopCompressor {
	return &NoopCompressor{}
}

/// compress returns the data as-is (no compression).
pub fn (c &NoopCompressor) compress(data []u8) ![]u8 {
	return data.clone()
}

/// decompress returns the data as-is (no decompression).
pub fn (c &NoopCompressor) decompress(data []u8) ![]u8 {
	return data.clone()
}

/// compression_type returns the compression type.
pub fn (c &NoopCompressor) compression_type() CompressionType {
	return CompressionType.none
}
