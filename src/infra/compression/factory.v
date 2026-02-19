/// Infrastructure layer - Compressor factory
/// Factory that creates the appropriate Compressor instance based on CompressionType
module compression

/// new_compressor creates a Compressor matching the specified CompressionType.
/// Returns a high-performance implementation using C libraries.
pub fn new_compressor(compression_type CompressionType) !Compressor {
	match compression_type {
		.none {
			return new_noop_compressor()
		}
		.gzip {
			return new_gzip_compressor()
		}
		.snappy {
			// Use C library
			return new_snappy_compressor_c()
		}
		.lz4 {
			// Use C library (Kafka Frame compatible)
			return new_lz4_compressor_c()
		}
		.zstd {
			// Use C library (Kafka Frame compatible)
			return new_zstd_compressor_c()
		}
	}
	return error('unsupported compression type: ${compression_type}')
}

/// new_compressor_with_level creates a Compressor with the specified CompressionType and level.
/// Only Gzip and ZSTD support levels.
/// C libraries are used for all compression types for consistency.
pub fn new_compressor_with_level(compression_type CompressionType, level int) !Compressor {
	match compression_type {
		.none {
			return new_noop_compressor()
		}
		.gzip {
			return new_gzip_compressor_with_level(level)
		}
		.snappy {
			// Snappy does not support levels; return default C implementation
			return new_snappy_compressor_c()
		}
		.lz4 {
			// LZ4 does not support levels; return default C implementation
			return new_lz4_compressor_c()
		}
		.zstd {
			// Use ZSTD C library with the specified level
			return new_zstd_compressor_c_with_level(level)
		}
	}
	return error('unsupported compression type: ${compression_type}')
}

/// Returns all available compression types.
pub fn list_available_compressors() []CompressionType {
	return [
		CompressionType.none,
		CompressionType.gzip,
		CompressionType.snappy,
		CompressionType.lz4,
		CompressionType.zstd,
	]
}
