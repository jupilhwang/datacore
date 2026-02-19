/// Unit tests - Compression module
module compression

import time

/// test_compression_type_enum tests the CompressionType enum.
fn test_compression_type_enum() {
	// Verify values
	assert int(CompressionType.none) == 0
	assert int(CompressionType.gzip) == 1
	assert int(CompressionType.snappy) == 2
	assert int(CompressionType.lz4) == 3
	assert int(CompressionType.zstd) == 4

	// String conversion
	assert CompressionType.none.str() == 'none'
	assert CompressionType.gzip.str() == 'gzip'
	assert CompressionType.snappy.str() == 'snappy'
	assert CompressionType.lz4.str() == 'lz4'
	assert CompressionType.zstd.str() == 'zstd'
}

/// test_compression_type_from_string tests string parsing.
fn test_compression_type_from_string() {
	ct := compression_type_from_string('gzip')!
	assert ct == CompressionType.gzip

	ct2 := compression_type_from_string('SNAPPY')!
	assert ct2 == CompressionType.snappy

	ct3 := compression_type_from_string('Lz4')!
	assert ct3 == CompressionType.lz4

	ct4 := compression_type_from_string('ZSTD')!
	assert ct4 == CompressionType.zstd

	ct5 := compression_type_from_string('none')!
	assert ct5 == CompressionType.none
}

/// test_noop_compressor tests the NoopCompressor.
fn test_noop_compressor() {
	compressor := new_noop_compressor()

	// Verify compression type
	assert compressor.compression_type() == CompressionType.none

	// Compress and decompress data
	test_data := 'Hello, World!'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Noop does not modify data
	assert compressed == test_data
	assert decompressed == test_data
}

/// test_gzip_compressor tests the GzipCompressor.
fn test_gzip_compressor() {
	compressor := new_gzip_compressor()

	// Verify compression type
	assert compressor.compression_type() == CompressionType.gzip

	// Compress and decompress data
	test_data := 'Hello, World! This is a test message for gzip compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Compressed data differs from original
	assert compressed != test_data
	// Decompressed data matches original
	assert decompressed == test_data
}

/// test_gzip_compressor_with_level tests GzipCompressor with various levels.
fn test_gzip_compressor_with_level() {
	compressor1 := new_gzip_compressor_with_level(1)
	compressor9 := new_gzip_compressor_with_level(9)

	test_data := 'AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD'.bytes()

	compressed1 := compressor1.compress(test_data)!
	compressed9 := compressor9.compress(test_data)!

	// Level 9 produces output smaller than or equal to level 1
	assert compressed9.len <= compressed1.len
}

/// test_snappy_compressor tests the SnappyCompressor.
fn test_snappy_compressor() {
	compressor := new_snappy_compressor()

	// Verify compression type
	assert compressor.compression_type() == CompressionType.snappy

	// Compress and decompress data
	test_data := 'Hello, World! This is a test message for snappy compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Decompressed data matches original
	assert decompressed == test_data
}

/// test_lz4_compressor tests the Lz4Compressor.
fn test_lz4_compressor() {
	compressor := new_lz4_compressor()

	// Verify compression type
	assert compressor.compression_type() == CompressionType.lz4

	// Compress and decompress data
	test_data := 'Hello, World! This is a test message for lz4 compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Decompressed data matches original
	assert decompressed == test_data
}

/// test_zstd_compressor tests the ZstdCompressor.
fn test_zstd_compressor() {
	compressor := new_zstd_compressor()

	// Verify compression type
	assert compressor.compression_type() == CompressionType.zstd

	// Compress and decompress data
	test_data := 'Hello, World! This is a test message for zstd compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Decompressed data matches original
	assert decompressed == test_data
}

/// test_zstd_compressor_with_level tests ZstdCompressor with various levels.
fn test_zstd_compressor_with_level() {
	compressor1 := new_zstd_compressor_with_level(1)
	compressor22 := new_zstd_compressor_with_level(22)

	assert compressor1.compression_type() == CompressionType.zstd
	assert compressor22.compression_type() == CompressionType.zstd
}

/// test_new_compressor_factory tests the factory function.
fn test_new_compressor() {
	// Create a Compressor for each type
	noop := new_compressor(CompressionType.none)!
	assert noop.compression_type() == CompressionType.none

	gzip := new_compressor(CompressionType.gzip)!
	assert gzip.compression_type() == CompressionType.gzip

	snappy := new_compressor(CompressionType.snappy)!
	assert snappy.compression_type() == CompressionType.snappy

	lz4 := new_compressor(CompressionType.lz4)!
	assert lz4.compression_type() == CompressionType.lz4

	zstd := new_compressor(CompressionType.zstd)!
	assert zstd.compression_type() == CompressionType.zstd
}

/// test_compression_service tests the CompressionService.
fn test_compression_service() {
	config := CompressionConfig{
		default_compression: CompressionType.gzip
		enable_metrics:      true
	}

	mut service := new_compression_service(config)!

	// Compress and decompress data
	test_data := 'Hello, World! This is a test message for compression service.'.bytes()

	// Compress with gzip
	compressed := service.compress(test_data, CompressionType.gzip)!
	decompressed := service.decompress(compressed, CompressionType.gzip)!
	assert decompressed == test_data

	// Compress using the default compression type
	compressed2 := service.compress_with_default(test_data)!
	decompressed2 := service.decompress_with_default(compressed2)!
	assert decompressed2 == test_data
}

/// test_compression_service_empty_data tests handling of empty data.
fn test_compression_service_empty_data() {
	mut service := new_default_compression_service()!

	empty_data := []u8{}

	// Compress empty data
	compressed := service.compress(empty_data, CompressionType.gzip)!
	assert compressed.len == 0

	// Decompress empty data
	decompressed := service.decompress(empty_data, CompressionType.gzip)!
	assert decompressed.len == 0
}

/// test_compression_metrics tests metric collection.
fn test_compression_metrics() {
	mut metrics := new_compression_metrics()

	// The metric registry is a singleton, so initial values may be affected by previous tests.
	// Save current values
	initial_compress_total := metrics.compress_total.value
	initial_decompress_total := metrics.decompress_total.value

	// Record compression
	metrics.record_compress(100, 50, 1000000, true)
	assert metrics.compress_total.value == initial_compress_total + 1

	// Record decompression
	metrics.record_decompress(50, 100, 2000000, true)
	assert metrics.decompress_total.value == initial_decompress_total + 1
}

/// test_list_available_compressors tests the list of available compression types.
fn test_list_available_compressors() {
	compressors := list_available_compressors()

	// There should be 5 compression types
	assert compressors.len == 5

	// Verify all types are present
	assert CompressionType.none in compressors
	assert CompressionType.gzip in compressors
	assert CompressionType.snappy in compressors
	assert CompressionType.lz4 in compressors
	assert CompressionType.zstd in compressors
}

// Manual Compression Test - All Types with Timing
// This test validates all compression types (Snappy, Gzip, LZ4, Zstd)
// in an integration test environment.

/// test_all_compression_types_with_timing tests all compression types with timing.
/// This test uses pure V implementations and runs without C library dependencies.
fn test_all_compression_types_with_timing() {
	// Generate test data (approximately 12KB)
	message := '{"id":"12345","timestamp":"2026-02-01T12:00:00Z","data":"This is a test message for compression testing. It contains various patterns and repeated content to test compression efficiency. Repeated: ABCABCABCXYZXYZXYZ123123123","metadata":{"source":"manual_test","version":"1.0","tags":["test","compression","kafka"]}}'

	mut test_data := []u8{}
	for _ in 0 .. 100 {
		test_data << message.bytes()
		test_data << u8(`\n`)
	}

	// Test pure V implementations for each compression type
	// Note: snappy, lz4, zstd use C libraries by default
	// Only gzip is a pure V implementation

	// 1. Gzip test (pure V)
	mut gzip_service := new_compression_service(CompressionConfig{
		default_compression: CompressionType.gzip
	})!
	gzip_result := run_compression_test_with_timing(CompressionType.gzip, test_data, mut
		gzip_service)
	assert gzip_result.passed, 'Gzip test failed: ${gzip_result.error_msg}'

	// 2. Noop test (no compression)
	mut noop_service := new_compression_service(CompressionConfig{
		default_compression: CompressionType.none
	})!
	noop_result := run_compression_test_with_timing(CompressionType.none, test_data, mut
		noop_service)
	assert noop_result.passed, 'Noop test failed: ${noop_result.error_msg}'

	// Note: Snappy, LZ4, Zstd require C libraries,
	// so they are run in separate integration tests after verifying the system environment.
}

// Test result struct (local use)
struct TestResult {
	name            string
	passed          bool
	compress_time   time.Duration
	decompress_time time.Duration
	original_size   int
	compressed_size int
	ratio           f64
	error_msg       string
}

// Test a single compression type
fn run_compression_test_with_timing(ct CompressionType, test_data []u8, mut service CompressionService) TestResult {
	name := ct.str()

	// Compression test
	compress_start := time.now()
	compressed := service.compress(test_data, ct) or {
		return TestResult{
			name:      name
			passed:    false
			error_msg: 'Compression failed: ${err}'
		}
	}
	compress_time := time.since(compress_start)

	// Validate compression result
	if compressed.len == 0 && test_data.len > 0 {
		return TestResult{
			name:      name
			passed:    false
			error_msg: 'Compressed data is empty'
		}
	}

	// Decompression test
	decompress_start := time.now()
	decompressed := service.decompress(compressed, ct) or {
		return TestResult{
			name:      name
			passed:    false
			error_msg: 'Decompression failed: ${err}'
		}
	}
	decompress_time := time.since(decompress_start)

	// Validate data integrity
	if decompressed != test_data {
		return TestResult{
			name:      name
			passed:    false
			error_msg: 'Data mismatch after decompression'
		}
	}

	// Calculate compression ratio
	mut ratio := 0.0
	if compressed.len > 0 {
		ratio = f64(test_data.len) / f64(compressed.len)
	}

	return TestResult{
		name:            name
		passed:          true
		compress_time:   compress_time
		decompress_time: decompress_time
		original_size:   test_data.len
		compressed_size: compressed.len
		ratio:           ratio
	}
}
