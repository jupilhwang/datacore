/// Infrastructure layer - Compression service
/// High-level service that manages multiple compression algorithms and collects metrics
module compression

import infra.observability

/// Service.
@[heap]
pub struct CompressionService {
mut:
	config              CompressionConfig
	default_compression CompressionType
	compressors         map[CompressionType]Compressor
	metrics             CompressionMetrics
	logger              &observability.Logger
}

/// CompressionConfig holds the configuration for CompressionService.
pub struct CompressionConfig {
pub:
	default_compression CompressionType = CompressionType.none
	enable_metrics      bool            = true
	// Compression threshold setting (v0.42.0)
	compression_threshold_bytes int = 1024
	// Compression level settings
	gzip_level       int = 6
	zstd_level       int = 3
	lz4_acceleration int = 1
}

/// new_compression_service creates a new CompressionService.
pub fn new_compression_service(config CompressionConfig) !&CompressionService {
	mut service := &CompressionService{
		config:              config
		default_compression: config.default_compression
		compressors:         map[CompressionType]Compressor{}
		metrics:             new_compression_metrics()
		logger:              observability.get_named_logger('compression_service')
	}

	// Initialize default Compressors
	for ct in list_available_compressors() {
		compressor := new_compressor(ct) or {
			return error('failed to create compressor for ${ct}: ${err}')
		}
		service.compressors[ct] = compressor
	}

	service.logger.info('compression service initialized', observability.field_string('default',
		config.default_compression.str()))

	return service
}

/// new_default_compression_service creates a CompressionService with default settings.
pub fn new_default_compression_service() !&CompressionService {
	return new_compression_service(CompressionConfig{})
}

/// compress compresses data using the specified compression type.
/// Data smaller than the threshold is returned as-is without compression.
pub fn (mut s CompressionService) compress(data []u8, compression_type CompressionType) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Start timer
	mut timer := s.metrics.start_compress_timer(data.len)

	// Look up Compressor
	compressor := s.get_compressor(compression_type) or {
		timer.stop(0, false)
		return error('compressor not found for type ${compression_type}: ${err}')
	}

	// Perform compression
	compressed := compressor.compress(data) or {
		timer.stop(0, false)
		s.logger.error('compression failed', observability.field_string('type', compression_type.str()),
			observability.field_int('size', data.len), observability.field_err_str(err.str()))
		return err
	}

	// Record metric
	timer.stop(compressed.len, true)

	s.logger.debug('data compressed', observability.field_string('type', compression_type.str()),
		observability.field_int('original_size', data.len), observability.field_int('compressed_size',
		compressed.len))

	return compressed
}

/// decompress decompresses data using the specified compression type.
pub fn (mut s CompressionService) decompress(data []u8, compression_type CompressionType) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// Start timer
	mut timer := s.metrics.start_decompress_timer(data.len)

	// Look up Compressor
	compressor := s.get_compressor(compression_type) or {
		timer.stop(0, false)
		return error('compressor not found for type ${compression_type}: ${err}')
	}

	// Perform decompression
	decompressed := compressor.decompress(data) or {
		timer.stop(0, false)
		s.logger.error('decompression failed', observability.field_string('type', compression_type.str()),
			observability.field_int('size', data.len), observability.field_err_str(err.str()))
		return err
	}

	// Record metric
	timer.stop(decompressed.len, true)

	s.logger.debug('data decompressed', observability.field_string('type', compression_type.str()),
		observability.field_int('compressed_size', data.len), observability.field_int('decompressed_size',
		decompressed.len))

	return decompressed
}

/// compress_with_default compresses data using the default compression type.
pub fn (mut s CompressionService) compress_with_default(data []u8) ![]u8 {
	return s.compress(data, s.default_compression)
}

/// decompress_with_default decompresses data using the default compression type.
pub fn (mut s CompressionService) decompress_with_default(data []u8) ![]u8 {
	return s.decompress(data, s.default_compression)
}

/// get_compressor returns the Compressor for the specified type.
pub fn (mut s CompressionService) get_compressor(compression_type CompressionType) !Compressor {
	if compressor := s.compressors[compression_type] {
		return compressor
	}

	// Create a new one if not found
	compressor := new_compressor(compression_type) or {
		return error('failed to create compressor: ${err}')
	}
	s.compressors[compression_type] = compressor
	return compressor
}

/// set_default_compression sets the default compression type.
pub fn (mut s CompressionService) set_default_compression(ct CompressionType) {
	s.default_compression = ct
	s.logger.info('default compression changed', observability.field_string('type', ct.str()))
}

/// Returns the current metrics.
pub fn (s &CompressionService) get_metrics() CompressionMetrics {
	return s.metrics
}
