/// 인프라 레이어 - 압축 서비스
/// 여러 압축 알고리즘을 관리하고 메트릭을 수집하는 고수준 서비스
module compression

import infra.observability

/// 서비스.
@[heap]
pub struct CompressionService {
mut:
	config              CompressionConfig
	default_compression CompressionType
	compressors         map[CompressionType]Compressor
	metrics             CompressionMetrics
	logger              &observability.Logger
}

/// CompressionConfig은 CompressionService의 설정입니다.
pub struct CompressionConfig {
pub:
	default_compression CompressionType = CompressionType.none
	enable_metrics      bool            = true
	// 압축 임계값 설정 (v0.42.0)
	compression_threshold_bytes int = 1024 // 압축 최소 크기 (기본 1KB)
	// 압축 레벨 설정
	gzip_level       int = 6 // Gzip 압축 레벨 (1-9, 기본 6)
	zstd_level       int = 3 // Zstd 압축 레벨 (1-22, 기본 3)
	lz4_acceleration int = 1 // LZ4 가속 레벨 (1-12, 기본 1)
}

/// new_compression_service는 새 CompressionService를 생성합니다.
pub fn new_compression_service(config CompressionConfig) !&CompressionService {
	mut service := &CompressionService{
		config:              config
		default_compression: config.default_compression
		compressors:         map[CompressionType]Compressor{}
		metrics:             new_compression_metrics()
		logger:              observability.get_named_logger('compression_service')
	}

	// 기본 Compressor 초기화
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

/// new_default_compression_service는 기본 설정으로 CompressionService를 생성합니다.
pub fn new_default_compression_service() !&CompressionService {
	return new_compression_service(CompressionConfig{})
}

/// compress는 지정된 압축 타입으로 데이터를 압축합니다.
/// 임계값보다 작은 데이터는 압축하지 않고 원본을 반환합니다.
pub fn (mut s CompressionService) compress(data []u8, compression_type CompressionType) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 타이머 시작
	mut timer := s.metrics.start_compress_timer(data.len)

	// Compressor 조회
	compressor := s.get_compressor(compression_type) or {
		timer.stop(0, false)
		return error('compressor not found for type ${compression_type}: ${err}')
	}

	// 압축 수행
	compressed := compressor.compress(data) or {
		timer.stop(0, false)
		s.logger.error('compression failed', observability.field_string('type', compression_type.str()),
			observability.field_int('size', data.len), observability.field_err_str(err.str()))
		return err
	}

	// 메트릭 기록
	timer.stop(compressed.len, true)

	s.logger.debug('data compressed', observability.field_string('type', compression_type.str()),
		observability.field_int('original_size', data.len), observability.field_int('compressed_size',
		compressed.len))

	return compressed
}

/// decompress는 지정된 압축 타입으로 데이터를 해제합니다.
pub fn (mut s CompressionService) decompress(data []u8, compression_type CompressionType) ![]u8 {
	if data.len == 0 {
		return []u8{}
	}

	// 타이머 시작
	mut timer := s.metrics.start_decompress_timer(data.len)

	// Compressor 조회
	compressor := s.get_compressor(compression_type) or {
		timer.stop(0, false)
		return error('compressor not found for type ${compression_type}: ${err}')
	}

	// 해제 수행
	decompressed := compressor.decompress(data) or {
		timer.stop(0, false)
		s.logger.error('decompression failed', observability.field_string('type', compression_type.str()),
			observability.field_int('size', data.len), observability.field_err_str(err.str()))
		return err
	}

	// 메트릭 기록
	timer.stop(decompressed.len, true)

	s.logger.debug('data decompressed', observability.field_string('type', compression_type.str()),
		observability.field_int('compressed_size', data.len), observability.field_int('decompressed_size',
		decompressed.len))

	return decompressed
}

/// compress_with_default은 기본 압축 타입으로 데이터를 압축합니다.
pub fn (mut s CompressionService) compress_with_default(data []u8) ![]u8 {
	return s.compress(data, s.default_compression)
}

/// decompress_with_default은 기본 압축 타입으로 데이터를 해제합니다.
pub fn (mut s CompressionService) decompress_with_default(data []u8) ![]u8 {
	return s.decompress(data, s.default_compression)
}

/// get_compressor는 지정된 타입의 Compressor를 반환합니다.
pub fn (mut s CompressionService) get_compressor(compression_type CompressionType) !Compressor {
	if compressor := s.compressors[compression_type] {
		return compressor
	}

	// 없으면 새로 생성
	compressor := new_compressor(compression_type) or {
		return error('failed to create compressor: ${err}')
	}
	s.compressors[compression_type] = compressor
	return compressor
}

/// set_default_compression은 기본 압축 타입을 설정합니다.
pub fn (mut s CompressionService) set_default_compression(ct CompressionType) {
	s.default_compression = ct
	s.logger.info('default compression changed', observability.field_string('type', ct.str()))
}

/// 현재 메트릭 반환.
pub fn (s &CompressionService) get_metrics() CompressionMetrics {
	return s.metrics
}
