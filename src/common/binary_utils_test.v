/// Tests for binary utility functions in the common module.
module common

// varint_size tests

/// Tests varint_size returns correct encoded size for zero
fn test_varint_size_zero() {
	assert varint_size(0) == 1
}

/// Tests varint_size returns correct encoded size for small positive values
fn test_varint_size_small_positive() {
	assert varint_size(1) == 1
	assert varint_size(63) == 1
}

/// Tests varint_size returns correct encoded size for small negative values
fn test_varint_size_small_negative() {
	assert varint_size(-1) == 1
	assert varint_size(-64) == 1
}

/// Tests varint_size returns correct encoded size for two-byte values
fn test_varint_size_two_bytes() {
	assert varint_size(64) == 2
	assert varint_size(-65) == 2
}

/// Tests varint_size is consistent with encode_varint length
fn test_varint_size_matches_encode_length() {
	values := [i64(0), 1, -1, 127, -128, 255, 300, -300, 1000000, -1000000]
	for val in values {
		encoded := encode_varint(val)
		assert varint_size(val) == encoded.len, 'varint_size(${val}) = ${varint_size(val)}, but encode_varint produces ${encoded.len} bytes'
	}
}

// uvarint_size tests

/// Tests uvarint_size returns correct encoded size for zero
fn test_uvarint_size_zero() {
	assert uvarint_size(0) == 1
}

/// Tests uvarint_size returns correct encoded size for single-byte values
fn test_uvarint_size_single_byte() {
	assert uvarint_size(1) == 1
	assert uvarint_size(127) == 1
}

/// Tests uvarint_size returns correct encoded size for multi-byte values
fn test_uvarint_size_multi_byte() {
	assert uvarint_size(128) == 2
	assert uvarint_size(16383) == 2
	assert uvarint_size(16384) == 3
}

/// Tests uvarint_size is consistent with encode_uvarint length
fn test_uvarint_size_matches_encode_length() {
	values := [u64(0), 1, 127, 128, 255, 16383, 16384, 2097151, 2097152]
	for val in values {
		encoded := encode_uvarint(val)
		assert uvarint_size(val) == encoded.len, 'uvarint_size(${val}) = ${uvarint_size(val)}, but encode_uvarint produces ${encoded.len} bytes'
	}
}
