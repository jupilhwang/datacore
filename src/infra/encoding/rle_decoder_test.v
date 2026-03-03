// Tests for RLE decoder.
module encoding

fn test_rle_decode_definition_levels() {
	// Encode RLE definition levels for 10 values (all present)
	encoded := encode_rle_definition_levels(10)

	// Decode with new decoder
	decoded := decode_rle_definition_levels(encoded, 1) or { panic(err) }
	assert decoded.len == 10
	for val in decoded {
		assert val == 1, 'all values should be 1 (present)'
	}
}

fn test_rle_decode_empty() {
	encoded := [u8(0x00), 0x00, 0x00, 0x00] // empty RLE
	decoded := decode_rle_definition_levels(encoded, 1) or { panic(err) }
	assert decoded.len == 0
}

fn test_rle_decode_single_byte_values() {
	// Test bit-packed values: 8 values of 1 bit each packed into 1 byte
	// Header: 0x08 (count=4, bit-packed mode) + 0xFF (8 ones)
	encoded := [u8(0x08), 0xFF]
	decoded := decode_rle_bit_packed(encoded, 1, 4) or { panic(err) }
	assert decoded.len == 4
	assert decoded[0] == 1
	assert decoded[1] == 1
	assert decoded[2] == 1
	assert decoded[3] == 1
}
