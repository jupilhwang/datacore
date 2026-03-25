// Tests for compression type constants and utility functions in the port layer.
// Ensures handler code can reference compression types without importing infra.
module port

fn test_compression_type_constants_match_kafka_protocol() {
	// Kafka protocol defines compression types in RecordBatch attributes (bits 0-2)
	assert compression_none == i16(0)
	assert compression_gzip == i16(1)
	assert compression_snappy == i16(2)
	assert compression_lz4 == i16(3)
	assert compression_zstd == i16(4)
}

fn test_compression_type_name_returns_correct_names() {
	assert compression_type_name(compression_none) == 'none'
	assert compression_type_name(compression_gzip) == 'gzip'
	assert compression_type_name(compression_snappy) == 'snappy'
	assert compression_type_name(compression_lz4) == 'lz4'
	assert compression_type_name(compression_zstd) == 'zstd'
}

fn test_compression_type_name_returns_unknown_for_invalid() {
	assert compression_type_name(i16(99)) == 'unknown(99)'
	assert compression_type_name(i16(-1)) == 'unknown(-1)'
}

fn test_compression_type_from_string_valid_names() {
	assert compression_type_from_string('none')! == compression_none
	assert compression_type_from_string('gzip')! == compression_gzip
	assert compression_type_from_string('snappy')! == compression_snappy
	assert compression_type_from_string('lz4')! == compression_lz4
	assert compression_type_from_string('zstd')! == compression_zstd
}

fn test_compression_type_from_string_aliases() {
	assert compression_type_from_string('noop')! == compression_none
	assert compression_type_from_string('')! == compression_none
	assert compression_type_from_string('gz')! == compression_gzip
	assert compression_type_from_string('zstandard')! == compression_zstd
}

fn test_compression_type_from_string_case_insensitive() {
	assert compression_type_from_string('GZIP')! == compression_gzip
	assert compression_type_from_string('Snappy')! == compression_snappy
	assert compression_type_from_string('LZ4')! == compression_lz4
	assert compression_type_from_string('ZSTD')! == compression_zstd
}

fn test_compression_type_from_string_invalid() {
	if _ := compression_type_from_string('invalid') {
		assert false, 'expected error for invalid compression type'
	}
}

fn test_validate_compression_type_valid_range() {
	assert validate_compression_type(i16(0))! == i16(0)
	assert validate_compression_type(i16(1))! == i16(1)
	assert validate_compression_type(i16(2))! == i16(2)
	assert validate_compression_type(i16(3))! == i16(3)
	assert validate_compression_type(i16(4))! == i16(4)
}

fn test_validate_compression_type_invalid() {
	if _ := validate_compression_type(i16(5)) {
		assert false, 'expected error for value 5'
	}
	if _ := validate_compression_type(i16(-1)) {
		assert false, 'expected error for value -1'
	}
}
