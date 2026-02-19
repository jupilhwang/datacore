/// CRC32-C 모듈 테스트
module crc32c

// 기본 기능 테스트

fn test_calculate_empty() {
	result := calculate([]u8{})
	assert result == 0
}

fn test_calculate_known_values() {
	// 알려진 CRC32-C 테스트 벡터들
	// 출처: https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32c

	// "123456789" -> 0xE3069283
	data1 := '123456789'.bytes()
	crc1 := calculate(data1)
	assert crc1 == 0xE3069283, 'Expected 0xE3069283, got 0x${crc1:08X}'

	// 단일 바이트 테스트
	data2 := [u8(0x00)]
	crc2 := calculate(data2)
	assert crc2 == 0x527D5351, 'Expected 0x527D5351, got 0x${crc2:08X}'

	// 여러 바이트 테스트
	data3 := [u8(0x01), 0x02, 0x03, 0x04]
	crc3 := calculate(data3)
	// 이 값은 실제 CRC32-C 계산 결과
	assert crc3 != 0 // 기본 검증
}

fn test_calculate_consistency() {
	// 같은 데이터에 대해 항상 같은 결과를 반환하는지 확인
	data := 'Hello, World!'.bytes()

	crc1 := calculate(data)
	crc2 := calculate(data)
	crc3 := calculate(data)

	assert crc1 == crc2
	assert crc2 == crc3
}

fn test_software_implementation() {
	// 소프트웨어 구현 직접 테스트
	data := '123456789'.bytes()
	crc := crc32c_sw(data)
	assert crc == 0xE3069283, 'Software CRC32-C failed: expected 0xE3069283, got 0x${crc:08X}'
}

// 증분 계산 테스트

fn test_incremental_calculation() {
	// 전체 데이터의 CRC
	full_data := 'Hello, World!'.bytes()
	full_crc := calculate(full_data)

	// 증분 계산
	part1 := 'Hello, '.bytes()
	part2 := 'World!'.bytes()

	mut crc := init()
	crc = update(crc, part1)
	crc = update(crc, part2)
	incremental_crc := finalize(crc)

	assert full_crc == incremental_crc, 'Incremental CRC mismatch: full=0x${full_crc:08X}, incremental=0x${incremental_crc:08X}'
}

fn test_incremental_single_byte() {
	// 바이트 단위 증분 계산
	data := [u8(0x01), 0x02, 0x03, 0x04, 0x05]
	full_crc := calculate(data)

	mut crc := init()
	for b in data {
		crc = update(crc, [b])
	}
	incremental_crc := finalize(crc)

	assert full_crc == incremental_crc
}

// 하드웨어 가속 테스트

fn test_cpu_detection() {
	// CPU 기능 감지가 오류 없이 실행되는지 확인
	supported := cpu_supports_sse42()
	// 결과는 플랫폼에 따라 다름, 단지 오류가 없어야 함
	println('SSE4.2 지원: ${supported}')
}

fn test_is_hardware_accelerated() {
	// 공개 API가 올바르게 작동하는지 확인
	hw_accel := is_hardware_accelerated()
	println('하드웨어 가속 사용 가능: ${hw_accel}')
}

fn test_hw_sw_consistency() {
	// 하드웨어와 소프트웨어 구현이 같은 결과를 반환하는지 확인
	test_cases := [
		''.bytes(),
		'a'.bytes(),
		'ab'.bytes(),
		'abc'.bytes(),
		'abcd'.bytes(),
		'abcde'.bytes(),
		'abcdefgh'.bytes(), // 8바이트 - 64비트 경계
		'abcdefghi'.bytes(), // 9바이트
		'123456789'.bytes(),
		'Hello, World!'.bytes(),
		'The quick brown fox jumps over the lazy dog'.bytes(),
	]

	for data in test_cases {
		sw_crc := crc32c_sw(data)
		api_crc := calculate(data) // 하드웨어 또는 소프트웨어 사용

		assert sw_crc == api_crc, 'CRC mismatch for "${data.bytestr()}": sw=0x${sw_crc:08X}, api=0x${api_crc:08X}'
	}
}

// 대용량 데이터 테스트

fn test_large_data() {
	// 1KB 데이터
	mut data := []u8{len: 1024}
	for i in 0 .. data.len {
		data[i] = u8(i & 0xFF)
	}

	crc := calculate(data)
	assert crc != 0

	// 일관성 확인
	crc2 := calculate(data)
	assert crc == crc2
}

fn test_very_large_data() {
	// 64KB 데이터
	mut data := []u8{len: 65536}
	for i in 0 .. data.len {
		data[i] = u8(i & 0xFF)
	}

	crc := calculate(data)
	assert crc != 0

	// 소프트웨어와 비교
	sw_crc := crc32c_sw(data)
	assert crc == sw_crc, 'Large data CRC mismatch'
}

// 엣지 케이스 테스트

fn test_all_zeros() {
	data := []u8{len: 16, init: 0}
	crc := calculate(data)
	assert crc != 0 // CRC of zeros is not zero
}

fn test_all_ones() {
	data := []u8{len: 16, init: 0xFF}
	crc := calculate(data)
	assert crc != 0
}

fn test_alternating_bits() {
	data := [u8(0xAA), 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55]
	crc := calculate(data)
	assert crc != 0

	// 일관성
	assert calculate(data) == crc
}

// 벤치마크 (성능 측정)

fn test_benchmark_small() {
	// 작은 데이터 (64 바이트) - 일반적인 Kafka 레코드 크기
	data := []u8{len: 64, init: 0x42}

	iterations := 10000
	mut sum := u32(0)

	for _ in 0 .. iterations {
		sum += calculate(data)
	}

	// 결과 사용 (최적화 방지)
	assert sum != 0
	println('Small data (64B) x ${iterations} iterations completed')
}

fn test_benchmark_medium() {
	// 중간 크기 데이터 (4KB)
	data := []u8{len: 4096, init: 0x42}

	iterations := 1000
	mut sum := u32(0)

	for _ in 0 .. iterations {
		sum += calculate(data)
	}

	assert sum != 0
	println('Medium data (4KB) x ${iterations} iterations completed')
}

fn test_benchmark_large() {
	// 큰 데이터 (64KB)
	data := []u8{len: 65536, init: 0x42}

	iterations := 100
	mut sum := u32(0)

	for _ in 0 .. iterations {
		sum += calculate(data)
	}

	assert sum != 0
	println('Large data (64KB) x ${iterations} iterations completed')
}
