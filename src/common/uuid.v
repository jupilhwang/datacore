/// Common module - UUID v4 generation
/// Provides RFC 4122 compliant UUID v4 (random) generation.
module common

import rand

/// generate_uuid_v4 generates a random UUID v4 (16 bytes).
/// Sets version 4 (random) and RFC 4122 variant bits.
pub fn generate_uuid_v4() []u8 {
	mut uuid := []u8{len: 16}
	for i in 0 .. 16 {
		uuid[i] = u8(rand.intn(256) or { 0 })
	}
	// Set UUID version 4 (random): byte 6 upper nibble = 0100
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	// Set RFC 4122 variant: byte 8 upper 2 bits = 10
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return uuid
}
