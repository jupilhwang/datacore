module kafka

import domain
import time

// Test varint_size matches actual varint encoding
fn test_varint_size_accuracy() {
	test_cases := [
		i64(0),
		i64(1),
		i64(-1),
		i64(127),
		i64(-128),
		i64(128),
		i64(-129),
		i64(16383),
		i64(-16384),
		i64(2097151),
		i64(-2097152),
		i64(9223372036854775807), // INT64_MAX
		i64(-9223372036854775808), // INT64_MIN
	]

	for val in test_cases {
		// Calculate size
		calc_size := varint_size(val)

		// Encode and measure actual size
		mut writer := new_writer()
		writer.write_varint(val)
		actual_size := writer.bytes().len

		assert calc_size == actual_size, 'varint_size mismatch for ${val}: calculated ${calc_size}, actual ${actual_size}'
	}
}

// Test calculate_record_size accuracy
fn test_calculate_record_size_accuracy() {
	test_cases := [
		[0, 0], // minimal
		[1, 1], // small
		[127, 127], // varint boundary
		[128, 128], // 2-byte varint
		[16383, 16383], // 3-byte varint boundary
		[1000, 10000], // realistic sizes
	]

	for case in test_cases {
		key_len := case[0]
		value_len := case[1]
		key := []u8{len: key_len, init: u8(42)}
		value := []u8{len: value_len, init: u8(84)}

		record := domain.Record{
			key:       key
			value:     value
			timestamp: time.unix(1640000000) // 2021-12-20
		}

		// Calculate size
		calc_size := calculate_record_size(0, 0, &record)

		// Encode and measure actual size (without length prefix)
		mut writer := new_writer()
		writer.write_i8(0) // attributes
		writer.write_varint(0) // timestamp_delta
		writer.write_varint(0) // offset_delta

		if record.key.len > 0 {
			writer.write_varint(i64(record.key.len))
			writer.write_raw(record.key)
		} else {
			writer.write_varint(-1)
		}

		if record.value.len > 0 {
			writer.write_varint(i64(record.value.len))
			writer.write_raw(record.value)
		} else {
			writer.write_varint(-1)
		}

		writer.write_varint(0) // headers count

		actual_size := writer.bytes().len

		assert calc_size == actual_size, 'Size mismatch for key=${key_len}, val=${value_len}: calculated ${calc_size}, actual ${actual_size}'
	}
}
