// RLE (Run-Length Encoding) and Bit-Packing decoder.
// Used for definition/repetition levels and dictionary indices in Parquet.
module encoding

// decode_rle_definition_levels decodes RLE-encoded definition levels.
// Format: 4-byte LE length prefix + rle_data.
// RLE header uses varint encoding: (count << 1 | mode) where mode=0 is RLE, mode=1 is bit-packed.
// max_def_level is the maximum definition level for this column.
pub fn decode_rle_definition_levels(data []u8, max_def_level int) ![]i32 {
	if data.len < 4 {
		return error('invalid RLE data: too short')
	}

	// Read 4-byte LE length prefix
	length := u32(data[0]) | u32(data[1]) << 8 | u32(data[2]) << 16 | u32(data[3]) << 24
	if length == 0 {
		return []i32{}
	}

	rle_data := data[4..]
	if rle_data.len == 0 {
		return []i32{}
	}

	return decode_rle_block(rle_data, max_def_level)
}

// decode_rle_bit_packed decodes bit-packed values from a RLE block.
// data starts with the varint header followed by the packed bytes.
// bit_width is the number of bits per value.
// num_values is the number of values to decode.
pub fn decode_rle_bit_packed(data []u8, bit_width int, num_values int) ![]i32 {
	if data.len == 0 {
		return error('invalid bit-packed data: empty')
	}
	// Skip the varint header byte(s)
	_, varint_len := read_varint(data, 0)
	payload := data[varint_len..]
	return decode_bit_packed_values(payload, bit_width, num_values)
}

// decode_rle_block decodes a sequence of RLE runs.
// Supports both RLE (mode=0) and bit-packed (mode=1) runs using varint headers.
fn decode_rle_block(data []u8, max_def_level int) []i32 {
	mut result := []i32{}
	mut pos := 0

	for pos < data.len {
		// Decode varint header
		header, varint_len := read_varint(data, pos)
		pos += varint_len

		if header & 1 == 0 {
			// RLE mode: header = (run_length << 1 | 0)
			run_length := int(header >> 1)
			if pos >= data.len {
				break
			}
			value := i32(data[pos])
			pos++
			for _ in 0 .. run_length {
				result << value
			}
		} else {
			// Bit-packed mode: header = (group_count << 1 | 1)
			// Each group contains 8 values
			group_count := int(header >> 1)
			num_values := group_count * 8
			bit_width := bits_required(max_def_level)
			bytes_needed := (num_values * bit_width + 7) / 8
			end := pos + bytes_needed
			if end > data.len {
				break
			}
			packed := decode_bit_packed_values(data[pos..end], bit_width, num_values)
			result << packed
			pos = end
		}
	}

	return result
}

// decode_bit_packed_values decodes values using LSB-first bit-packing.
// Parquet stores bits in LSB-first order within each byte.
fn decode_bit_packed_values(data []u8, bit_width int, num_values int) []i32 {
	mut result := []i32{cap: num_values}
	mut bit_pos := 0

	for _ in 0 .. num_values {
		mut value := i32(0)
		for b := 0; b < bit_width; b++ {
			byte_idx := bit_pos / 8
			bit_idx := bit_pos % 8
			if byte_idx < data.len {
				if data[byte_idx] & u8(u8(1) << u8(bit_idx)) != 0 {
					value |= i32(u32(1) << u32(b))
				}
			}
			bit_pos++
		}
		result << value
	}

	return result
}

// read_varint reads a variable-length encoded integer from data at pos.
// Returns the decoded value and the number of bytes consumed.
fn read_varint(data []u8, pos int) (u64, int) {
	mut value := u64(0)
	mut shift := u32(0)
	mut i := pos

	for i < data.len {
		b := data[i]
		value |= u64(b & 0x7F) << shift
		i++
		if b & 0x80 == 0 {
			break
		}
		shift += 7
	}

	return value, i - pos
}

// bits_required returns the minimum number of bits needed to represent max_value.
fn bits_required(max_value int) int {
	if max_value <= 0 {
		return 1
	}
	mut bits := 0
	mut v := max_value
	for v > 0 {
		bits++
		v >>= 1
	}
	return bits
}
