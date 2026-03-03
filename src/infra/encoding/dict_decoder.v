// Dictionary decoder for Parquet dictionary-encoded columns.
// Dictionary encoding stores unique values once, then references them by index.
module encoding

// apply_int32_dictionary applies dictionary indices to get actual values.
pub fn apply_int32_dictionary(dict_values []i32, indices []i32) []i32 {
	mut result := []i32{cap: indices.len}
	for idx in indices {
		if idx >= 0 && idx < dict_values.len {
			result << dict_values[idx]
		}
	}
	return result
}

// apply_int64_dictionary applies dictionary indices to get actual values.
pub fn apply_int64_dictionary(dict_values []i64, indices []i32) []i64 {
	mut result := []i64{cap: indices.len}
	for idx in indices {
		if idx >= 0 && idx < dict_values.len {
			result << dict_values[idx]
		}
	}
	return result
}

// apply_byte_array_dictionary applies dictionary indices to get actual byte arrays.
// Silently skips out-of-bounds indices.
pub fn apply_byte_array_dictionary(dict_values [][]u8, indices []i32) [][]u8 {
	mut result := [][]u8{cap: indices.len}
	for idx in indices {
		if idx >= 0 && idx < dict_values.len {
			result << dict_values[idx].clone()
		}
	}
	return result
}
