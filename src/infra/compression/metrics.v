/// Infrastructure layer - Compression metrics
/// Metric collection for monitoring performance and efficiency of compression operations
module compression

import infra.observability
import time

/// Metric collection for compression operations.
pub struct CompressionMetrics {
pub mut:
	// Compression operation counters
	compress_total   &observability.Metric
	decompress_total &observability.Metric

	// Compression error counters
	compress_errors   &observability.Metric
	decompress_errors &observability.Metric

	// Compression duration (histogram)
	compress_duration   &observability.Metric
	decompress_duration &observability.Metric

	// Compression ratio (original size / compressed size)
	compression_ratio &observability.Metric

	// Byte throughput
	bytes_compressed   &observability.Metric
	bytes_decompressed &observability.Metric
}

/// Creates new compression metrics.
fn new_compression_metrics() CompressionMetrics {
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

/// Records a compression operation.
fn (mut m CompressionMetrics) record_compress(original_size i64, compressed_size i64, duration time.Duration, success bool) {
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

/// Records a decompression operation.
fn (mut m CompressionMetrics) record_decompress(compressed_size i64, decompressed_size i64, duration time.Duration, success bool) {
	m.decompress_total.inc()
	m.bytes_decompressed.inc_by(f64(compressed_size))

	if !success {
		m.decompress_errors.inc()
		return
	}

	seconds := f64(duration) / f64(time.second)
	m.decompress_duration.observe(seconds)
}

/// Timer for measuring compression operation duration.
pub struct CompressionTimer {
	start_time time.Time
	metrics    &CompressionMetrics
	typ        string
	size       i64
}

/// start_compress_timer starts a compression timer.
fn (mut m CompressionMetrics) start_compress_timer(size i64) CompressionTimer {
	return CompressionTimer{
		start_time: time.now()
		metrics:    unsafe { m }
		typ:        'compress'
		size:       size
	}
}

/// start_decompress_timer starts a decompression timer.
fn (mut m CompressionMetrics) start_decompress_timer(size i64) CompressionTimer {
	return CompressionTimer{
		start_time: time.now()
		metrics:    unsafe { m }
		typ:        'decompress'
		size:       size
	}
}

/// stop stops the timer and records the metric.
fn (mut t CompressionTimer) stop(result_size i64, success bool) {
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
