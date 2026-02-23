// Service Layer - Schema Encoder JSON Helpers
// JSON parsing utilities for Avro encoding/decoding
module schema

// Note: The following functions are defined in avro_encoder.v:
// - parse_json_bool
// - parse_json_long
// - parse_json_float
// - parse_json_double
// - parse_json_bytes
// - is_json_null
// - format_json_bytes

// Only functions unique to this module are kept here

fn parse_json_int(s string) ?int {
	trimmed := s.trim_space()
	return trimmed.int()
}

fn parse_json_string_value(s string) ?string {
	trimmed := s.trim_space()
	if trimmed.starts_with('"') && trimmed.ends_with('"') {
		return trimmed[1..trimmed.len - 1]
	}
	return none
}

// JSON Escape/Unescape

fn unescape_json_bytes(s string) []u8 {
	mut result := []u8{}
	mut i := 0

	for i < s.len {
		if s[i] == `\\` && i + 1 < s.len {
			match s[i + 1] {
				`n` {
					result << u8(`\n`)
					i += 2
				}
				`r` {
					result << u8(`\r`)
					i += 2
				}
				`t` {
					result << u8(`\t`)
					i += 2
				}
				`\\` {
					result << u8(`\\`)
					i += 2
				}
				`"` {
					result << u8(`"`)
					i += 2
				}
				`u` {
					// Unicode escape: \uXXXX
					if i + 5 < s.len {
						hex := s[i + 2..i + 6]
						val := hex_to_int(hex)
						if val < 256 {
							result << u8(val)
						}
						i += 6
					} else {
						result << s[i]
						i += 1
					}
				}
				else {
					result << s[i]
					i += 1
				}
			}
		} else {
			result << s[i]
			i += 1
		}
	}

	return result
}

fn hex_to_int(s string) int {
	mut result := 0
	for c in s {
		result = result * 16
		if c >= `0` && c <= `9` {
			result += int(c - `0`)
		} else if c >= `a` && c <= `f` {
			result += int(c - `a` + 10)
		} else if c >= `A` && c <= `F` {
			result += int(c - `A` + 10)
		}
	}
	return result
}

fn escape_json_str(s string) string {
	return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r',
		'\\r').replace('\t', '\\t')
}

// JSON Field Extraction

fn extract_json_field(json_str string, field_name string) ?string {
	// Find "field_name": value
	pattern := '"${field_name}"'
	idx := json_str.index(pattern) or { return none }

	mut pos := idx + pattern.len

	// Skip whitespace and colon
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Read value
	return read_json_value(json_str, pos)
}

fn read_json_value(s string, start int) ?string {
	if start >= s.len {
		return none
	}

	c := s[start]

	if c == `"` {
		// String value
		mut end := start + 1
		mut escaped := false
		for end < s.len {
			if escaped {
				escaped = false
				end += 1
				continue
			}
			if s[end] == `\\` {
				escaped = true
				end += 1
				continue
			}
			if s[end] == `"` {
				return s[start..end + 1]
			}
			end += 1
		}
		return none
	} else if c == `{` || c == `[` {
		// Object or array
		mut depth := 1
		mut end := start + 1
		mut in_string := false
		mut escaped := false

		open := c
		close := if c == `{` { `}` } else { `]` }

		for end < s.len && depth > 0 {
			if escaped {
				escaped = false
				end += 1
				continue
			}
			if s[end] == `\\` && in_string {
				escaped = true
				end += 1
				continue
			}
			if s[end] == `"` {
				in_string = !in_string
			} else if !in_string {
				if s[end] == open {
					depth += 1
				} else if s[end] == close {
					depth -= 1
				}
			}
			end += 1
		}

		return s[start..end]
	} else if c == `t` || c == `f` || c == `n` {
		// true, false, null
		if s[start..].starts_with('true') {
			return 'true'
		} else if s[start..].starts_with('false') {
			return 'false'
		} else if s[start..].starts_with('null') {
			return 'null'
		}
		return none
	} else if c == `-` || (c >= `0` && c <= `9`) {
		// Number
		mut end := start
		for end < s.len && (s[end] == `-` || s[end] == `+` || s[end] == `.`
			|| s[end] == `e` || s[end] == `E` || (s[end] >= `0` && s[end] <= `9`)) {
			end += 1
		}
		return s[start..end]
	}

	return none
}

// JSON Array/Map Parsing
// Note: parse_json_array and parse_json_map are defined in avro_encoder.v

fn parse_json_array(s string) ?[]string {
	trimmed := s.trim_space()
	if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
		return none
	}

	mut items := []string{}
	mut pos := 1

	for pos < trimmed.len - 1 {
		// Skip whitespace
		for pos < trimmed.len && (trimmed[pos] == ` ` || trimmed[pos] == `\t`
			|| trimmed[pos] == `\n` || trimmed[pos] == `,`) {
			pos += 1
		}

		if pos >= trimmed.len - 1 {
			break
		}

		if value := read_json_value(trimmed, pos) {
			items << value
			pos += value.len
		} else {
			break
		}
	}

	return items
}

fn parse_json_map(s string) ?map[string]string {
	trimmed := s.trim_space()
	if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
		return none
	}

	mut result := map[string]string{}
	mut pos := 1

	for pos < trimmed.len - 1 {
		// Skip whitespace
		for pos < trimmed.len && (trimmed[pos] == ` ` || trimmed[pos] == `\t`
			|| trimmed[pos] == `\n` || trimmed[pos] == `,`) {
			pos += 1
		}

		if pos >= trimmed.len - 1 {
			break
		}

		// Read key
		if trimmed[pos] != `"` {
			break
		}

		key_val := read_json_value(trimmed, pos) or { break }
		key := key_val[1..key_val.len - 1]
		pos += key_val.len

		// Skip colon and whitespace
		for pos < trimmed.len && (trimmed[pos] == `:` || trimmed[pos] == ` `
			|| trimmed[pos] == `\t` || trimmed[pos] == `\n`) {
			pos += 1
		}

		// Read value
		value := read_json_value(trimmed, pos) or { break }
		result[key] = value
		pos += value.len
	}

	return result
}
