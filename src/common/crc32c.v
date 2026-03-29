/// CRC32-C (Castagnoli) checksum implementation.
/// Uses polynomial 0x82F63B78 (reversed Castagnoli).
/// Provides a reusable CRC32-C computation shared across architecture layers.
module common

/// Pre-computed CRC32-C table (Castagnoli polynomial).
const crc32c_castagnoli_table = init_crc32c_table()

/// init_crc32c_table initializes the CRC32-C lookup table.
fn init_crc32c_table() []u32 {
	mut table := []u32{len: 256}
	for i := 0; i < 256; i++ {
		mut crc := u32(i)
		for _ in 0 .. 8 {
			if crc & 1 != 0 {
				crc = (crc >> 1) ^ 0x82f63b78
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

/// crc32c computes CRC32-C checksum using the Castagnoli polynomial.
pub fn crc32c(data []u8) u32 {
	if data.len == 0 {
		return 0
	}
	mut crc := u32(0xffffffff)
	for b in data {
		idx := int((crc ^ u32(b)) & 0xff)
		crc = (crc >> 8) ^ crc32c_castagnoli_table[idx]
	}
	return crc ^ 0xffffffff
}
