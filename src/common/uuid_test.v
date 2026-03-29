// UUID v4 Generation Tests
module common

fn test_generate_uuid_v4_length() {
	uuid := generate_uuid_v4()
	assert uuid.len == 16
}

fn test_generate_uuid_v4_version_bits() {
	uuid := generate_uuid_v4()
	// UUID v4: byte 6 upper nibble must be 0x4
	assert (uuid[6] & 0xf0) == 0x40
}

fn test_generate_uuid_v4_variant_bits() {
	uuid := generate_uuid_v4()
	// RFC 4122 variant: byte 8 upper 2 bits must be 10
	assert (uuid[8] & 0xc0) == 0x80
}

fn test_generate_uuid_v4_uniqueness() {
	uuid1 := generate_uuid_v4()
	uuid2 := generate_uuid_v4()
	assert uuid1 != uuid2
}
