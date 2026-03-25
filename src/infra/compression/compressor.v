/// Infrastructure layer - Compression interface
/// Defines compression types and the Compressor interface compatible with the Kafka protocol
module compression

/// Compression algorithm type.
pub enum CompressionType {
	none   = 0
	gzip   = 1
	snappy = 2
	lz4    = 3
	zstd   = 4
}

/// Converts a CompressionType to its string representation.
pub fn (ct CompressionType) str() string {
	return match ct {
		.none { 'none' }
		.gzip { 'gzip' }
		.snappy { 'snappy' }
		.lz4 { 'lz4' }
		.zstd { 'zstd' }
	}
}

/// Parses a CompressionType from a string.
pub fn compression_type_from_string(s string) !CompressionType {
	return match s.to_lower() {
		'none', 'noop', '' { CompressionType.none }
		'gzip', 'gz' { CompressionType.gzip }
		'snappy' { CompressionType.snappy }
		'lz4' { CompressionType.lz4 }
		'zstd', 'zstandard' { CompressionType.zstd }
		else { return error('unknown compression type: ${s}') }
	}
}

/// Converts an i16 value to a CompressionType with validation.
/// Used when parsing Kafka RecordBatch attributes (lower 3 bits).
/// Returns an error for values outside the valid range (0-4).
pub fn compression_type_from_i16(val i16) !CompressionType {
	return match val {
		0 { CompressionType.none }
		1 { CompressionType.gzip }
		2 { CompressionType.snappy }
		3 { CompressionType.lz4 }
		4 { CompressionType.zstd }
		else { error('unknown compression type value: ${val}') }
	}
}

/// Interface.
pub interface Compressor {
	compress(data []u8) ![]u8
	decompress(data []u8) ![]u8
	compression_type() CompressionType
}

/// CompressorError represents a compression-related error.
pub struct CompressorError {
	message string
	typ     string
}

/// new_compressor_error creates a new CompressorError.
fn new_compressor_error(message string, typ string) CompressorError {
	return CompressorError{
		message: message
		typ:     typ
	}
}

/// msg returns the error message.
pub fn (e CompressorError) msg() string {
	return '${e.typ}: ${e.message}'
}

/// code returns the error code.
pub fn (e CompressorError) code() int {
	return match e.typ {
		'invalid_input' { 1 }
		'compression_failed' { 2 }
		'decompression_failed' { 3 }
		'unsupported_type' { 4 }
		else { 0 }
	}
}
