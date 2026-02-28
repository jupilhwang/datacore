/// Infrastructure layer - Compression utilities
/// Shared helper functions for C interop within the compression module.
module compression

/// cstring_to_string converts a C null-terminated string pointer to a V string.
/// Returns an empty string when the pointer is null.
fn cstring_to_string(cstr &u8) string {
	if isnulptr(cstr) {
		return ''
	}
	mut len := 0
	for unsafe { cstr[len] != 0 } {
		len++
	}
	mut res := []u8{len: len}
	for i in 0 .. len {
		res[i] = unsafe { cstr[i] }
	}
	return res.bytestr()
}

/// isnulptr reports whether the given pointer is null.
fn isnulptr(ptr &u8) bool {
	return ptr == unsafe { nil }
}
