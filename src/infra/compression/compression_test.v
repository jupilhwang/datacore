/// 단위 테스트 - 압축 모듈
module compression

/// test_compression_type_enum은 CompressionType 열거형을 테스트합니다.
fn test_compression_type_enum() {
	// 값 확인
	assert int(CompressionType.none) == 0
	assert int(CompressionType.gzip) == 1
	assert int(CompressionType.snappy) == 2
	assert int(CompressionType.lz4) == 3
	assert int(CompressionType.zstd) == 4

	// 문자열 변환
	assert CompressionType.none.str() == 'none'
	assert CompressionType.gzip.str() == 'gzip'
	assert CompressionType.snappy.str() == 'snappy'
	assert CompressionType.lz4.str() == 'lz4'
	assert CompressionType.zstd.str() == 'zstd'
}

/// test_compression_type_from_string은 문자열 파싱을 테스트합니다.
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

/// test_noop_compressor는 NoopCompressor를 테스트합니다.
fn test_noop_compressor() {
	compressor := new_noop_compressor()

	// 압축 타입 확인
	assert compressor.compression_type() == CompressionType.none

	// 데이터 압축/해제
	test_data := 'Hello, World!'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// Noop은 데이터를 변경하지 않음
	assert compressed == test_data
	assert decompressed == test_data
}

/// test_gzip_compressor는 GzipCompressor를 테스트합니다.
fn test_gzip_compressor() {
	compressor := new_gzip_compressor()

	// 압축 타입 확인
	assert compressor.compression_type() == CompressionType.gzip

	// 데이터 압축/해제
	test_data := 'Hello, World! This is a test message for gzip compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// 압축된 데이터는 원본과 다름
	assert compressed != test_data
	// 해제된 데이터는 원본과 동일
	assert decompressed == test_data
}

/// test_gzip_compressor_with_level은 레벨별 GzipCompressor를 테스트합니다.
fn test_gzip_compressor_with_level() {
	compressor1 := new_gzip_compressor_with_level(1)
	compressor9 := new_gzip_compressor_with_level(9)

	test_data := 'AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDD'.bytes()

	compressed1 := compressor1.compress(test_data)!
	compressed9 := compressor9.compress(test_data)!

	// 레벨 9는 레벨 1보다 더 작거나 같은 크기
	assert compressed9.len <= compressed1.len
}

/// test_snappy_compressor는 SnappyCompressor를 테스트합니다.
fn test_snappy_compressor() {
	compressor := new_snappy_compressor()

	// 압축 타입 확인
	assert compressor.compression_type() == CompressionType.snappy

	// 데이터 압축/해제
	test_data := 'Hello, World! This is a test message for snappy compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// 해제된 데이터는 원본과 동일
	assert decompressed == test_data
}

/// test_lz4_compressor는 Lz4Compressor를 테스트합니다.
fn test_lz4_compressor() {
	compressor := new_lz4_compressor()

	// 압축 타입 확인
	assert compressor.compression_type() == CompressionType.lz4

	// 데이터 압축/해제
	test_data := 'Hello, World! This is a test message for lz4 compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// 해제된 데이터는 원본과 동일
	assert decompressed == test_data
}

/// test_zstd_compressor는 ZstdCompressor를 테스트합니다.
fn test_zstd_compressor() {
	compressor := new_zstd_compressor()

	// 압축 타입 확인
	assert compressor.compression_type() == CompressionType.zstd

	// 데이터 압축/해제
	test_data := 'Hello, World! This is a test message for zstd compression.'.bytes()
	compressed := compressor.compress(test_data)!
	decompressed := compressor.decompress(compressed)!

	// 해제된 데이터는 원본과 동일
	assert decompressed == test_data
}

/// test_zstd_compressor_with_level은 레벨별 ZstdCompressor를 테스트합니다.
fn test_zstd_compressor_with_level() {
	compressor1 := new_zstd_compressor_with_level(1)
	compressor22 := new_zstd_compressor_with_level(22)

	assert compressor1.compression_type() == CompressionType.zstd
	assert compressor22.compression_type() == CompressionType.zstd
}

/// test_new_compressor_factory는 팩토리 함수를 테스트합니다.
fn test_new_compressor() {
	// 각 타입별 Compressor 생성
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

/// test_compression_service는 CompressionService를 테스트합니다.
fn test_compression_service() {
	config := CompressionConfig{
		default_compression: CompressionType.gzip
		enable_metrics:      true
	}

	mut service := new_compression_service(config)!

	// 데이터 압축/해제
	test_data := 'Hello, World! This is a test message for compression service.'.bytes()

	// gzip으로 압축
	compressed := service.compress(test_data, CompressionType.gzip)!
	decompressed := service.decompress(compressed, CompressionType.gzip)!
	assert decompressed == test_data

	// 기본 압축 타입으로 압축
	compressed2 := service.compress_with_default(test_data)!
	decompressed2 := service.decompress_with_default(compressed2)!
	assert decompressed2 == test_data
}

/// test_compression_service_empty_data는 빈 데이터 처리를 테스트합니다.
fn test_compression_service_empty_data() {
	mut service := new_default_compression_service()!

	empty_data := []u8{}

	// 빈 데이터 압축
	compressed := service.compress(empty_data, CompressionType.gzip)!
	assert compressed.len == 0

	// 빈 데이터 해제
	decompressed := service.decompress(empty_data, CompressionType.gzip)!
	assert decompressed.len == 0
}

/// test_compression_metrics는 메트릭 수집을 테스트합니다.
fn test_compression_metrics() {
	mut metrics := new_compression_metrics()

	// 메트릭 레지스트리는 싱글톤이므로 초기값은 이전 테스트의 영향을 받을 수 있음
	// 현재 값을 저장
	initial_compress_total := metrics.compress_total.value
	initial_decompress_total := metrics.decompress_total.value

	// 압축 기록
	metrics.record_compress(100, 50, 1000000, true) // 1ms
	assert metrics.compress_total.value == initial_compress_total + 1

	// 해제 기록
	metrics.record_decompress(50, 100, 2000000, true) // 2ms
	assert metrics.decompress_total.value == initial_decompress_total + 1
}

/// test_list_available_compressors는 사용 가능한 압축 타입 목록을 테스트합니다.
fn test_list_available_compressors() {
	compressors := list_available_compressors()

	// 5개의 압축 타입이 있어야 함
	assert compressors.len == 5

	// 모든 타입이 포함되어 있는지 확인
	assert CompressionType.none in compressors
	assert CompressionType.gzip in compressors
	assert CompressionType.snappy in compressors
	assert CompressionType.lz4 in compressors
	assert CompressionType.zstd in compressors
}
