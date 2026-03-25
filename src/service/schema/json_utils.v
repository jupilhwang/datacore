// Service Layer - JSON parsing utilities
// Provides JSON parsing helper functions for schema validation and compatibility checks
module schema

// extract_json_string extracts the string value for a given key from JSON
// Returns the value if found, otherwise returns none
pub fn extract_json_string(json_str string, key string) ?string {
	// Find "key": "value" pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip : and whitespace
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len || json_str[pos] != `"` {
		return none
	}

	// Find end quote
	start := pos + 1
	mut end := start
	mut escaped := false
	for end < json_str.len {
		if escaped {
			escaped = false
			end += 1
			continue
		}
		if json_str[end] == `\\` {
			escaped = true
			end += 1
			continue
		}
		if json_str[end] == `"` {
			break
		}
		end += 1
	}

	return json_str[start..end]
}

// extract_json_int extracts the integer value for a given key from JSON
// Returns the value if found, otherwise returns none
pub fn extract_json_int(json_str string, key string) ?int {
	// Find "key": value pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip : and whitespace
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Read number
	mut end := pos
	for end < json_str.len && ((json_str[end] >= `0` && json_str[end] <= `9`)
		|| json_str[end] == `-`) {
		end += 1
	}

	if end == pos {
		return none
	}

	return json_str[pos..end].int()
}

// extract_json_float extracts the float value for a given key from JSON
// Returns the value if found, otherwise returns none
fn extract_json_float(json_str string, key string) ?f64 {
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip : and whitespace
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Read number (including decimal point and exponent)
	mut end := pos
	for end < json_str.len && ((json_str[end] >= `0` && json_str[end] <= `9`)
		|| json_str[end] == `-` || json_str[end] == `.` || json_str[end] == `e`
		|| json_str[end] == `E` || json_str[end] == `+`) {
		end += 1
	}

	if end == pos {
		return none
	}

	num_str := json_str[pos..end]
	return num_str.f64()
}

// parse_json_string_array extracts a string array value for a given key from JSON
// Returns the array if found, none otherwise
fn parse_json_string_array(json_str string, key string) ?[]string {
	// Find "key": [...] pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip to [
	for pos < json_str.len && json_str[pos] != `[` {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Find matching ]
	mut depth := 1
	mut end := pos + 1
	for end < json_str.len && depth > 0 {
		if json_str[end] == `[` {
			depth += 1
		} else if json_str[end] == `]` {
			depth -= 1
		}
		end += 1
	}

	array_str := json_str[pos..end]

	// Extract strings from array
	mut result := []string{}
	mut in_string := false
	mut start := 0

	for i, c in array_str {
		if c == `"` && !in_string {
			in_string = true
			start = i + 1
		} else if c == `"` && in_string {
			in_string = false
			result << array_str[start..i]
		}
	}

	return result
}

// extract_json_object_value extracts a JSON object value starting at the given position
// Returns the object string including braces
fn extract_json_object_value(json_str string, start_pos int) string {
	mut pos := start_pos

	// Skip to {
	for pos < json_str.len && json_str[pos] != `{` {
		pos += 1
	}

	if pos >= json_str.len {
		return ''
	}

	// Find matching }
	mut depth := 1
	mut end := pos + 1
	mut in_string := false
	mut escape := false

	for end < json_str.len && depth > 0 {
		if escape {
			escape = false
			end += 1
			continue
		}

		c := json_str[end]

		if c == `\\` && in_string {
			escape = true
			end += 1
			continue
		}

		if c == `"` {
			in_string = !in_string
		} else if !in_string {
			if c == `{` {
				depth += 1
			} else if c == `}` {
				depth -= 1
			}
		}
		end += 1
	}

	return json_str[pos..end]
}

// is_valid_json performs basic JSON validation
// Returns true if the string appears to be valid JSON
fn is_valid_json(s string) bool {
	// Basic JSON validation
	trimmed := s.trim_space()
	if trimmed.len == 0 {
		return false
	}

	// Valid JSON can be: object, array, string, number, boolean, or null
	first := trimmed[0]

	// Boolean or null
	if trimmed == 'true' || trimmed == 'false' || trimmed == 'null' {
		return true
	}

	// String
	if first == `"` && trimmed.len >= 2 && trimmed[trimmed.len - 1] == `"` {
		// Basic string validation - check for proper escaping
		mut i := 1
		for i < trimmed.len - 1 {
			if trimmed[i] == `\\` {
				i += 2
			} else if trimmed[i] == `"` {
				return false
			} else {
				i += 1
			}
		}
		return true
	}

	// Number (simplified check)
	if first == `-` || (first >= `0` && first <= `9`) {
		mut valid := true
		for c in trimmed {
			if !((c >= `0` && c <= `9`) || c == `-` || c == `.` || c == `e` || c == `E` || c == `+`) {
				valid = false
				break
			}
		}
		return valid
	}

	// Object or Array
	if (trimmed.starts_with('{') && trimmed.ends_with('}'))
		|| (trimmed.starts_with('[') && trimmed.ends_with(']')) {
		// Basic bracket matching
		mut depth := 0
		mut in_string := false
		mut escape := false

		for c in trimmed {
			if escape {
				escape = false
				continue
			}

			if c == `\\` && in_string {
				escape = true
				continue
			}

			if c == `"` {
				in_string = !in_string
				continue
			}

			if in_string {
				continue
			}

			if c == `{` || c == `[` {
				depth += 1
			} else if c == `}` || c == `]` {
				depth -= 1
				if depth < 0 {
					return false
				}
			}
		}

		return depth == 0 && !in_string
	}

	return false
}

/// extract_json_object extracts an object value from JSON (simplified - returns single-level map).
pub fn extract_json_object(json_str string, key string) map[string]string {
	mut result := map[string]string{}

	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return result }
	start := idx + pattern.len

	// find opening brace
	mut pos := start
	for pos < json_str.len && json_str[pos] != `{` {
		pos++
	}
	if pos >= json_str.len {
		return result
	}

	// find matching closing brace
	mut depth := 1
	mut obj_start := pos + 1
	pos++
	for pos < json_str.len && depth > 0 {
		if json_str[pos] == `{` {
			depth++
		} else if json_str[pos] == `}` {
			depth--
		}
		pos++
	}

	if depth != 0 {
		return result
	}

	obj_str := json_str[obj_start..pos - 1]

	// parse key-value pairs (simplified)
	mut in_key := true
	mut current_key := ''
	mut i := 0
	for i < obj_str.len {
		if obj_str[i] == `"` {
			i++
			mut end := i
			for end < obj_str.len && obj_str[end] != `"` {
				if obj_str[end] == `\\` {
					end++
				}
				end++
			}
			str_val := obj_str[i..end]
			if in_key {
				current_key = str_val
			} else {
				result[current_key] = str_val
			}
			i = end + 1
		} else if obj_str[i] == `:` {
			in_key = false
			i++
		} else if obj_str[i] == `,` {
			in_key = true
			i++
		} else {
			i++
		}
	}

	return result
}
