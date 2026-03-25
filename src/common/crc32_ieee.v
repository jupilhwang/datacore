/// CRC32-IEEE checksum implementation using the IEEE polynomial (0xedb88320).
/// Provides a reusable CRC32 computation shared across architecture layers.
module common

/// Pre-computed CRC32 table (IEEE polynomial).
const crc32_ieee_table = init_crc32_ieee_table()

/// init_crc32_ieee_table initializes the CRC32 lookup table.
fn init_crc32_ieee_table() []u32 {
	mut table := []u32{len: 256}
	for i := 0; i < 256; i++ {
		mut crc := u32(i)
		for _ in 0 .. 8 {
			if crc & 1 != 0 {
				crc = (crc >> 1) ^ 0xedb88320
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

/// crc32_ieee computes CRC32 using the IEEE polynomial.
pub fn crc32_ieee(data []u8) u32 {
	mut crc := u32(0xffffffff)
	for b in data {
		idx := int((crc ^ u32(b)) & 0xff)
		crc = (crc >> 8) ^ crc32_ieee_table[idx]
	}
	return crc ^ 0xffffffff
}

/// crc32_ieee_update updates a running CRC32 with additional data.
pub fn crc32_ieee_update(crc u32, data []u8) u32 {
	mut result := crc ^ 0xffffffff
	for b in data {
		idx := int((result ^ u32(b)) & 0xff)
		result = (result >> 8) ^ crc32_ieee_table[idx]
	}
	return result ^ 0xffffffff
}
