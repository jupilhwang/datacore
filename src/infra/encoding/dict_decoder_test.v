// Tests for Dictionary decoder.
module encoding

fn test_dict_decoder_int32() {
	// Dictionary values
	dict_values := [i32(10), i32(20), i32(30)]
	// Indices: 0, 1, 2, 0, 1
	indices := [i32(0), 1, 2, 0, 1]

	result := apply_int32_dictionary(dict_values, indices)
	assert result.len == 5
	assert result[0] == 10
	assert result[1] == 20
	assert result[2] == 30
	assert result[3] == 10
	assert result[4] == 20
}

fn test_dict_decoder_int64() {
	dict_values := [i64(100), i64(200)]
	indices := [i32(0), 1, 0, 0, 1]

	result := apply_int64_dictionary(dict_values, indices)
	assert result.len == 5
	assert result[0] == 100
	assert result[1] == 200
	assert result[2] == 100
	assert result[3] == 100
	assert result[4] == 200
}

fn test_dict_decoder_byte_array() {
	dict_values := [[u8(0x01), 0x02], [u8(0x03), 0x04, 0x05]]
	indices := [i32(0), 1, 0]

	result := apply_byte_array_dictionary(dict_values, indices)
	assert result.len == 3
	assert result[0] == [u8(0x01), 0x02]
	assert result[1] == [u8(0x03), 0x04, 0x05]
	assert result[2] == [u8(0x01), 0x02]
}

fn test_dict_decoder_empty() {
	dict_values := [i32(42)]
	indices := []i32{}

	result := apply_int32_dictionary(dict_values, indices)
	assert result.len == 0
}
