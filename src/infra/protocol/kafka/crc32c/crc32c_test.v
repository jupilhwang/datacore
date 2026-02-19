/// CRC32-C module tests
module crc32c

// Basic functionality tests

fn test_calculate_empty() {
	result := calculate([]u8{})
	assert result == 0
}

fn test_calculate_known_values() {
	// Known CRC32-C test vectors
	// Source: https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32c

	// "123456789" -> 0xE3069283
	data1 := '123456789'.bytes()
	crc1 := calculate(data1)
	assert crc1 == 0xE3069283, 'Expected 0xE3069283, got 0x${crc1:08X}'

	// single byte test
	data2 := [u8(0x00)]
	crc2 := calculate(data2)
	assert crc2 == 0x527D5351, 'Expected 0x527D5351, got 0x${crc2:08X}'

	// multi-byte test
	data3 := [u8(0x01), 0x02, 0x03, 0x04]
	crc3 := calculate(data3)
	// this value is the actual CRC32-C computation result
	assert crc3 != 0
}

fn test_calculate_consistency() {
	// verify the same data always returns the same result
	data := 'Hello, World!'.bytes()

	crc1 := calculate(data)
	crc2 := calculate(data)
	crc3 := calculate(data)

	assert crc1 == crc2
	assert crc2 == crc3
}

fn test_software_implementation() {
	// directly test the software implementation
	data := '123456789'.bytes()
	crc := crc32c_sw(data)
	assert crc == 0xE3069283, 'Software CRC32-C failed: expected 0xE3069283, got 0x${crc:08X}'
}

// Incremental computation tests

fn test_incremental_calculation() {
	// CRC of full data
	full_data := 'Hello, World!'.bytes()
	full_crc := calculate(full_data)

	// incremental computation
	part1 := 'Hello, '.bytes()
	part2 := 'World!'.bytes()

	mut crc := init()
	crc = update(crc, part1)
	crc = update(crc, part2)
	incremental_crc := finalize(crc)

	assert full_crc == incremental_crc, 'Incremental CRC mismatch: full=0x${full_crc:08X}, incremental=0x${incremental_crc:08X}'
}

fn test_incremental_single_byte() {
	// byte-by-byte incremental computation
	data := [u8(0x01), 0x02, 0x03, 0x04, 0x05]
	full_crc := calculate(data)

	mut crc := init()
	for b in data {
		crc = update(crc, [b])
	}
	incremental_crc := finalize(crc)

	assert full_crc == incremental_crc
}

// Hardware acceleration tests

fn test_cpu_detection() {
	// verify CPU feature detection runs without error
	supported := cpu_supports_sse42()
	// result varies by platform, just must not error
	println('SSE4.2 supported: ${supported}')
}

fn test_is_hardware_accelerated() {
	// verify the public API works correctly
	hw_accel := is_hardware_accelerated()
	println('Hardware acceleration available: ${hw_accel}')
}

fn test_hw_sw_consistency() {
	// verify hardware and software implementations return the same result
	test_cases := [
		''.bytes(),
		'a'.bytes(),
		'ab'.bytes(),
		'abc'.bytes(),
		'abcd'.bytes(),
		'abcde'.bytes(),
		'abcdefgh'.bytes(),
		'abcdefghi'.bytes(),
		'123456789'.bytes(),
		'Hello, World!'.bytes(),
		'The quick brown fox jumps over the lazy dog'.bytes(),
	]

	for data in test_cases {
		sw_crc := crc32c_sw(data)
		api_crc := calculate(data)

		assert sw_crc == api_crc, 'CRC mismatch for "${data.bytestr()}": sw=0x${sw_crc:08X}, api=0x${api_crc:08X}'
	}
}

// Large data tests

fn test_large_data() {
	// 1KB data
	mut data := []u8{len: 1024}
	for i in 0 .. data.len {
		data[i] = u8(i & 0xFF)
	}

	crc := calculate(data)
	assert crc != 0

	// consistency check
	crc2 := calculate(data)
	assert crc == crc2
}

fn test_very_large_data() {
	// 64KB data
	mut data := []u8{len: 65536}
	for i in 0 .. data.len {
		data[i] = u8(i & 0xFF)
	}

	crc := calculate(data)
	assert crc != 0

	// compare with software implementation
	sw_crc := crc32c_sw(data)
	assert crc == sw_crc, 'Large data CRC mismatch'
}

// Edge case tests

fn test_all_zeros() {
	data := []u8{len: 16, init: 0}
	crc := calculate(data)
	assert crc != 0
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

	// consistency
	assert calculate(data) == crc
}

// Benchmarks (performance measurement)

fn test_benchmark_small() {
	// small data (64 bytes) - typical Kafka record size
	data := []u8{len: 64, init: 0x42}

	iterations := 10000
	mut sum := u32(0)

	for _ in 0 .. iterations {
		sum += calculate(data)
	}

	// use result to prevent compiler optimization
	assert sum != 0
	println('Small data (64B) x ${iterations} iterations completed')
}

fn test_benchmark_medium() {
	// medium-sized data (4KB)
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
	// large data (64KB)
	data := []u8{len: 65536, init: 0x42}

	iterations := 100
	mut sum := u32(0)

	for _ in 0 .. iterations {
		sum += calculate(data)
	}

	assert sum != 0
	println('Large data (64KB) x ${iterations} iterations completed')
}
