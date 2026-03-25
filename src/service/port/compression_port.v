// Compression interface following DIP.
// Abstracts data compression/decompression from the infrastructure layer.
// Uses i16 for compression type to avoid coupling with infra enum.
module port

// Compression type constants (Kafka protocol compatible).
// Maps to RecordBatch attributes bits 0-2.
pub const compression_none = i16(0)
pub const compression_gzip = i16(1)
pub const compression_snappy = i16(2)
pub const compression_lz4 = i16(3)
pub const compression_zstd = i16(4)

/// CompressionPort abstracts data compression and decompression.
/// Implemented by infra/compression CompressionService via an adapter.
pub interface CompressionPort {
mut:
	/// Compresses data using the specified compression type.
	/// compression_type: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
	compress(data []u8, compression_type i16) ![]u8

	/// Decompresses data using the specified compression type.
	decompress(data []u8, compression_type i16) ![]u8
}

/// Returns a human-readable name for a compression type i16 value.
pub fn compression_type_name(compression_type i16) string {
	return match compression_type {
		compression_none { 'none' }
		compression_gzip { 'gzip' }
		compression_snappy { 'snappy' }
		compression_lz4 { 'lz4' }
		compression_zstd { 'zstd' }
		else { 'unknown(${compression_type})' }
	}
}

/// Parses a compression type name string to its i16 value.
/// Supports aliases: 'noop'/'', 'gz', 'zstandard'.
pub fn compression_type_from_string(s string) !i16 {
	return match s.to_lower() {
		'none', 'noop', '' { compression_none }
		'gzip', 'gz' { compression_gzip }
		'snappy' { compression_snappy }
		'lz4' { compression_lz4 }
		'zstd', 'zstandard' { compression_zstd }
		else { return error('unknown compression type: ${s}') }
	}
}

/// Validates that an i16 value is a known compression type (0-4).
pub fn validate_compression_type(val i16) !i16 {
	if val < 0 || val > 4 {
		return error('unknown compression type value: ${val}')
	}
	return val
}
