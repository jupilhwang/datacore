// Infra Layer - S3 URL Utilities
// URL encoding/decoding helpers for AWS SigV4 signing
module s3

// hex_chars is the uppercase hex digit lookup table used by u8_to_hex.
const hex_chars = '0123456789ABCDEF'

/// url_decode decodes a percent-encoded string.
/// Example: %20 -> space, %2F -> /
/// Builds result as a byte array to avoid repeated string re-allocation.
fn url_decode(s string) string {
	mut result := []u8{cap: s.len}
	mut i := 0
	for i < s.len {
		if s[i] == u8(`%`) && i + 2 < s.len && is_hex_char(s[i + 1]) && is_hex_char(s[i + 2]) {
			result << hex_to_u8(s[i + 1..i + 3])
			i += 3
		} else {
			result << s[i]
			i++
		}
	}
	return result.bytestr()
}

/// is_hex_char checks whether a character is a valid hexadecimal digit.
/// Accepts characters in the range 0-9, A-F, a-f.
fn is_hex_char(c u8) bool {
	return (c >= `0` && c <= `9`) || (c >= `A` && c <= `F`) || (c >= `a` && c <= `f`)
}

/// hex_to_u8 converts a two-character hexadecimal string to a byte.
/// Example: "4A" -> 74
/// Uses match ranges for O(1) nibble conversion without if-else chains.
fn hex_to_u8(s string) u8 {
	high := hex_nibble(s[0])
	low := hex_nibble(s[1])
	return (high << 4) | low
}

/// hex_nibble converts a single hex character to its 4-bit value.
fn hex_nibble(c u8) u8 {
	return match c {
		`0`...`9` { c - `0` }
		`A`...`F` { c - `A` + 10 }
		else { c - `a` + 10 } // `a`...`f`
	}
}

/// u8_to_hex converts a byte to a two-character uppercase hexadecimal string.
/// Example: 74 -> "4A"
/// Uses hex_chars lookup table for O(1) nibble-to-char conversion.
fn u8_to_hex(c u8) string {
	high := hex_chars[c >> 4]
	low := hex_chars[c & 0x0F]
	return [high, low].bytestr()
}

/// url_encode_for_sigv4 encodes a string according to AWS SigV4 requirements.
/// Does not encode A-Z, a-z, 0-9, -, ., _, ~ characters.
/// All other characters are percent-encoded in %XX format.
fn url_encode_for_sigv4(s string) string {
	mut result := []u8{}
	for c in s {
		match c {
			`A`...`Z`, `a`...`z`, `0`...`9`, `-`, `.`, `_`, `~` {
				result << c
			}
			else {
				result << u8(`%`)
				hex := u8_to_hex(c)
				result << hex[0]
				result << hex[1]
			}
		}
	}
	return result.bytestr()
}

/// canonicalize_query sorts and encodes query parameters for AWS SigV4.
/// Sorts query parameters alphabetically and applies URL encoding.
fn (a &S3StorageAdapter) canonicalize_query(query string) string {
	if query == '' {
		return ''
	}

	// Parse query string into map
	mut params := map[string]string{}
	for pair in query.split('&') {
		parts := pair.split_nth('=', 2)
		if parts.len == 2 {
			// URL-decode then re-encode keys and values for AWS SigV4
			params[url_decode(parts[0])] = url_decode(parts[1])
		}
	}

	// Sort keys alphabetically and build canonical query string in one pass
	mut keys := params.keys()
	keys.sort()

	mut result := []string{cap: keys.len}
	for key in keys {
		result << '${url_encode_for_sigv4(key)}=${url_encode_for_sigv4(params[key])}'
	}

	return result.join('&')
}
