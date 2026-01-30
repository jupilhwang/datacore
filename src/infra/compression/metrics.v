/// 인프라 레이어 - 압축 메트릭
/// 압축 작업의 성능 및 효율성 모니터링을 위한 메트릭 수집
module compression

import infra.observability
import time

/// CompressionMetrics는 압축 작업에 대한 메트릭을 수집합니다.
pub struct CompressionMetrics {
pub mut:
	// 압축 작업 카운터
	compress_total   &observability.Metric
	decompress_total &observability.Metric

	// 압축 실패 카운터
	compress_errors   &observability.Metric
	decompress_errors &observability.Metric

	// 압축 시간 (히스토그램)
	compress_duration   &observability.Metric
	decompress_duration &observability.Metric

	// 압축률 (원본 크기 / 압축 크기)
	compression_ratio &observability.Metric

	// 바이트 처리량
	bytes_compressed   &observability.Metric
	bytes_decompressed &observability.Metric
}

/// new_compression_metrics는 새 압축 메트릭을 생성합니다.
pub fn new_compression_metrics() CompressionMetrics {
	mut reg := observability.get_registry()

	return CompressionMetrics{
		compress_total:      reg.register('datacore_compression_compress_total', 'Total compression operations',
			.counter)
		decompress_total:    reg.register('datacore_compression_decompress_total', 'Total decompression operations',
			.counter)
		compress_errors:     reg.register('datacore_compression_compress_errors_total',
			'Total compression errors', .counter)
		decompress_errors:   reg.register('datacore_compression_decompress_errors_total',
			'Total decompression errors', .counter)
		compress_duration:   reg.register('datacore_compression_compress_duration_seconds',
			'Compression duration in seconds', .histogram)
		decompress_duration: reg.register('datacore_compression_decompress_duration_seconds',
			'Decompression duration in seconds', .histogram)
		compression_ratio:   reg.register('datacore_compression_ratio', 'Compression ratio (original/compressed)',
			.histogram)
		bytes_compressed:    reg.register('datacore_compression_bytes_compressed_total',
			'Total bytes compressed', .counter)
		bytes_decompressed:  reg.register('datacore_compression_bytes_decompressed_total',
			'Total bytes decompressed', .counter)
	}
}

/// record_compress는 압축 작업을 기록합니다.
pub fn (mut m CompressionMetrics) record_compress(original_size i64, compressed_size i64, duration time.Duration, success bool) {
	m.compress_total.inc()
	m.bytes_compressed.inc_by(f64(original_size))

	if !success {
		m.compress_errors.inc()
		return
	}

	seconds := f64(duration) / f64(time.second)
	m.compress_duration.observe(seconds)

	if compressed_size > 0 {
		ratio := f64(original_size) / f64(compressed_size)
		m.compression_ratio.observe(ratio)
	}
}

/// record_decompress는 해제 작업을 기록합니다.
pub fn (mut m CompressionMetrics) record_decompress(compressed_size i64, decompressed_size i64, duration time.Duration, success bool) {
	m.decompress_total.inc()
	m.bytes_decompressed.inc_by(f64(compressed_size))

	if !success {
		m.decompress_errors.inc()
		return
	}

	seconds := f64(duration) / f64(time.second)
	m.decompress_duration.observe(seconds)
}

/// CompressionTimer는 압축 작업 시간 측정을 위한 타이머입니다.
pub struct CompressionTimer {
	start_time time.Time
	metrics    &CompressionMetrics
	typ        string // 'compress' or 'decompress'
	size       i64
}

/// start_compress_timer는 압축 타이머를 시작합니다.
pub fn (mut m CompressionMetrics) start_compress_timer(size i64) CompressionTimer {
	return CompressionTimer{
		start_time: time.now()
		metrics:    unsafe { m }
		typ:        'compress'
		size:       size
	}
}

/// start_decompress_timer는 해제 타이머를 시작합니다.
pub fn (mut m CompressionMetrics) start_decompress_timer(size i64) CompressionTimer {
	return CompressionTimer{
		start_time: time.now()
		metrics:    unsafe { m }
		typ:        'decompress'
		size:       size
	}
}

/// stop은 타이머를 중지하고 메트릭을 기록합니다.
pub fn (mut t CompressionTimer) stop(result_size i64, success bool) {
	duration := time.since(t.start_time)
	unsafe {
		mut metrics := t.metrics
		if t.typ == 'compress' {
			metrics.record_compress(t.size, result_size, duration, success)
		} else {
			metrics.record_decompress(t.size, result_size, duration, success)
		}
	}
}
