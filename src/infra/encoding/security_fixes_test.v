// Security fix tests for C1, C2, C3, H4, H5, H6.
// RED phase: these tests must FAIL before the fixes are applied.
module encoding

// C1: footer_len이 파일 크기를 초과할 때 에러를 반환해야 한다.
fn test_c1_footer_len_exceeds_file_size() {
	// PAR1 magic (4) + footer_len 4바이트 (0xFFFFFFFF) + PAR1 magic (4) = 12바이트
	// footer_len = 0xFFFFFFFF — 파일 크기(12)보다 훨씬 크다.
	mut data := []u8{len: 12, init: 0}
	data[0] = u8(`P`)
	data[1] = u8(`A`)
	data[2] = u8(`R`)
	data[3] = u8(`1`)
	// data[data.len-8..data.len-5] = footer_len = 0xFFFFFFFF (little-endian)
	data[4] = 0xFF
	data[5] = 0xFF
	data[6] = 0xFF
	data[7] = 0xFF
	data[8] = u8(`P`)
	data[9] = u8(`A`)
	data[10] = u8(`R`)
	data[11] = u8(`1`)

	mut parser := new_parquet_metadata_parser()
	parser.parse(data) or {
		assert err.msg().contains('invalid footer length'), 'C1: should report invalid footer length, got: ${err.msg()}'
		return
	}
	assert false, 'C1: footer_len overflow should return an error but did not'
}

// C2: def_len이 u32 상위 비트 세트일 때 skip하지 않아야 한다.
fn test_c2_def_len_overflow_does_not_advance_pos() {
	// u32(0x80000000) = 2147483648 — remaining(6) < this → skip하지 않아야 함
	data := [u8(0x00), 0x00, 0x00, 0x80, 0xAA, 0xBB]
	result := skip_definition_levels_if_present(data, 0)
	assert result == 0, 'C2: oversized def_len should not advance pos, got: ${result}'
}

// C3: varint32가 5바이트를 초과하면 에러를 반환해야 한다.
fn test_c3_varint32_too_long_returns_error() {
	// 0x80 × 6 + 0x00 = 7바이트, 모두 continuation bit 세트 → 5바이트 초과
	data := [u8(0x80), 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]
	mut r := new_thrift_reader(data)
	r.read_varint32() or {
		assert err.msg().contains('varint32'), 'C3: error message should mention varint32, got: ${err.msg()}'
		return
	}
	assert false, 'C3: varint32 exceeding 5 bytes should return error but succeeded'
}

// C3: varint64가 10바이트를 초과하면 에러를 반환해야 한다.
fn test_c3_varint64_too_long_returns_error() {
	// 0x80 × 11 + 0x00 = 12바이트, continuation bit 계속 세트 → 10바이트 초과
	data := [u8(0x80), 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]
	mut r := new_thrift_reader(data)
	r.read_varint64() or {
		assert err.msg().contains('varint64'), 'C3: error message should mention varint64, got: ${err.msg()}'
		return
	}
	assert false, 'C3: varint64 exceeding 10 bytes should return error but succeeded'
}

// C3: rle_decoder의 read_varint도 최대 10바이트 소비 후 안전하게 종료해야 한다.
fn test_c3_rle_varint_bounded_consumption() {
	// 100바이트 모두 MSB=1인 악성 데이터
	data := []u8{len: 100, init: 0x80}
	_, consumed := read_varint(data, 0)
	assert consumed <= 10, 'C3: rle read_varint should consume at most 10 bytes, got: ${consumed}'
}

// H4: dict decoder에서 out-of-bounds 인덱스 시 에러를 반환해야 한다.
fn test_h4_dict_int32_out_of_bounds_returns_error() {
	dict_values := [i32(10), i32(20)]
	indices := [i32(0), i32(5)] // 인덱스 5는 범위 밖

	apply_int32_dictionary_safe(dict_values, indices) or {
		assert err.msg().contains('out of bounds'), 'H4: error should mention out of bounds, got: ${err.msg()}'
		return
	}
	assert false, 'H4: out-of-bounds int32 index should return error but succeeded'
}

fn test_h4_dict_int64_out_of_bounds_returns_error() {
	dict_values := [i64(100)]
	indices := [i32(0), i32(-1)] // 음수 인덱스

	apply_int64_dictionary_safe(dict_values, indices) or {
		assert err.msg().contains('out of bounds'), 'H4: error should mention out of bounds, got: ${err.msg()}'
		return
	}
	assert false, 'H4: negative int64 index should return error but succeeded'
}

fn test_h4_dict_byte_array_out_of_bounds_returns_error() {
	dict_values := [[u8(0x01), 0x02]]
	indices := [i32(0), i32(99)] // 범위 밖

	apply_byte_array_dictionary_safe(dict_values, indices) or {
		assert err.msg().contains('out of bounds'), 'H4: error should mention out of bounds, got: ${err.msg()}'
		return
	}
	assert false, 'H4: out-of-bounds byte array index should return error but succeeded'
}

// H4: 유효한 인덱스는 정상적으로 작동해야 한다.
fn test_h4_dict_valid_indices_work() {
	dict_values := [i32(10), i32(20), i32(30)]
	indices := [i32(0), i32(1), i32(2), i32(0)]
	result := apply_int32_dictionary_safe(dict_values, indices) or {
		assert false, 'H4: valid indices should not return error: ${err.msg()}'
		return
	}
	assert result.len == 4
	assert result[0] == 10
	assert result[1] == 20
	assert result[2] == 30
	assert result[3] == 10
}

// H6: Thrift double skip은 8바이트 raw read여야 한다 (varint가 아님).
// ThriftReader에 read_double_bytes 메서드가 추가되어야 한다.
fn test_h6_thrift_reader_skip_raw_bytes_advances_pos() {
	data := [u8(0x3F), 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] // 1.0 double
	mut r := new_thrift_reader(data)
	r.skip_raw_bytes(8) or { assert false, 'H6: skip_raw_bytes should not fail: ${err}' }
	assert r.pos == 8, 'H6: skipping a double should advance pos by 8, got: ${r.pos}'
}
