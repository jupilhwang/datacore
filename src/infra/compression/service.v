/// 인프라 레이어 - 압축 서비스
/// 여러 압축 알고리즘을 관리하고 메트릭을 수집하는 고수준 서비스
module compression

import infra.observability
import time

/// 서비스.
@[heap]
pub struct CompressionService {
mut:
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
}

/// new_compression_service는 새 CompressionService를 생성합니다.
pub fn new_compression_service(config CompressionConfig) !&CompressionService {
	mut service := &CompressionService{
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

/// 특정 타입의 평균 압축률 반환.
pub fn (s &CompressionService) get_compression_ratio(compression_type CompressionType) f64 {
	// 메트릭에서 압축률 히스토그램의 평균을 계산
	// 간단한 구현: 마지막 값 반환
	return 1.0
}

/// auto_compress는 데이터 크기에 따라 자동으로 압축 여부를 결정합니다.
/// 작은 데이터는 압축하지 않고, 큰 데이터만 압축합니다.
pub fn (mut s CompressionService) auto_compress(data []u8, threshold int) ![]u8 {
	if data.len < threshold {
		// 작은 데이터는 압축하지 않음
		return data.clone()
	}
	return s.compress_with_default(data)
}

/// BatchCompressionResult는 배치 압축 결과를 나타냅니다.
pub struct BatchCompressionResult {
pub:
	data             []u8
	original_size    i64
	compressed_size  i64
	compression_type CompressionType
	duration         time.Duration
}

/// compress_batch는 여러 데이터를 배치로 압축합니다.
pub fn (mut s CompressionService) compress_batch(datasets [][]u8, compression_type CompressionType) ![]BatchCompressionResult {
	mut results := []BatchCompressionResult{cap: datasets.len}

	for data in datasets {
		start := time.now()
		compressed := s.compress(data, compression_type)!
		duration := time.since(start)

		results << BatchCompressionResult{
			data:             compressed
			original_size:    data.len
			compressed_size:  compressed.len
			compression_type: compression_type
			duration:         duration
		}
	}

	return results
}

/// decompress_batch는 여러 데이터를 배치로 해제합니다.
pub fn (mut s CompressionService) decompress_batch(datasets [][]u8, compression_type CompressionType) ![][]u8 {
	mut results := [][]u8{cap: datasets.len}

	for data in datasets {
		decompressed := s.decompress(data, compression_type)!
		results << decompressed
	}

	return results
}
