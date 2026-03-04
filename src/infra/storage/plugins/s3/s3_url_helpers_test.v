module s3

// test_hex_to_u8_digits converts decimal digits 0-9
fn test_hex_to_u8_digits() {
	assert hex_to_u8('00') == u8(0x00)
	assert hex_to_u8('09') == u8(0x09)
	assert hex_to_u8('10') == u8(0x10)
	assert hex_to_u8('FF') == u8(0xFF)
	assert hex_to_u8('4A') == u8(0x4A)
	assert hex_to_u8('2F') == u8(0x2F)
}

// test_hex_to_u8_lowercase accepts lowercase hex characters
fn test_hex_to_u8_lowercase() {
	assert hex_to_u8('ff') == u8(0xFF)
	assert hex_to_u8('4a') == u8(0x4A)
	assert hex_to_u8('2f') == u8(0x2F)
}

// test_u8_to_hex_produces_uppercase_two_char_string verifies output format
fn test_u8_to_hex_produces_uppercase_two_char_string() {
	assert u8_to_hex(u8(0x00)) == '00'
	assert u8_to_hex(u8(0x0F)) == '0F'
	assert u8_to_hex(u8(0x10)) == '10'
	assert u8_to_hex(u8(0xFF)) == 'FF'
	assert u8_to_hex(u8(0x4A)) == '4A'
	assert u8_to_hex(u8(0x2F)) == '2F'
}

// test_u8_to_hex_roundtrip verifies hex_to_u8 and u8_to_hex are inverses
fn test_u8_to_hex_roundtrip() {
	for i in 0 .. 256 {
		c := u8(i)
		hex := u8_to_hex(c)
		assert hex.len == 2
		assert hex_to_u8(hex) == c
	}
}

// test_url_decode_percent_encoded decodes standard percent-encoded strings
fn test_url_decode_percent_encoded() {
	assert url_decode('hello%20world') == 'hello world'
	assert url_decode('%2F') == '/'
	assert url_decode('foo%3Dbar') == 'foo=bar'
	assert url_decode('no+encoding') == 'no+encoding'
}

// test_url_decode_passthrough leaves plain strings unchanged
fn test_url_decode_passthrough() {
	assert url_decode('hello') == 'hello'
	assert url_decode('') == ''
	assert url_decode('abc123') == 'abc123'
}

// test_url_decode_multiple_sequences decodes multiple sequences in one string
fn test_url_decode_multiple_sequences() {
	assert url_decode('foo%20bar%20baz') == 'foo bar baz'
	assert url_decode('%41%42%43') == 'ABC'
}

// test_canonicalize_query_sorts_and_encodes verifies alphabetical sort + encoding
fn test_canonicalize_query_sorts_and_encodes() {
	adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'test'
			region:      'us-east-1'
		}
	}
	// z before a should be reversed
	result := adapter.canonicalize_query('zoo=1&apple=2')
	assert result == 'apple=2&zoo=1'
}

// test_canonicalize_query_empty_returns_empty verifies empty input handling
fn test_canonicalize_query_empty_returns_empty() {
	adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'test'
			region:      'us-east-1'
		}
	}
	assert adapter.canonicalize_query('') == ''
}

// test_canonicalize_query_encodes_special_chars verifies SigV4 encoding
fn test_canonicalize_query_encodes_special_chars() {
	adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'test'
			region:      'us-east-1'
		}
	}
	// space in value should be encoded as %20
	result := adapter.canonicalize_query('key=hello%20world')
	assert result == 'key=hello%20world'
}
