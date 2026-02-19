// Service Layer - JSON 파싱 유틸리티
// 스키마 검증 및 호환성 검사를 위한 JSON 파싱 헬퍼 함수를 제공합니다
module schema

// extract_json_string은 JSON에서 주어진 키의 문자열 값을 추출합니다
// 찾으면 값을 반환하고, 그렇지 않으면 none을 반환합니다
fn extract_json_string(json_str string, key string) ?string {
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

// extract_json_int는 JSON에서 주어진 키의 정수 값을 추출합니다
// 찾으면 값을 반환하고, 그렇지 않으면 none을 반환합니다
fn extract_json_int(json_str string, key string) ?int {
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

// extract_json_float는 JSON에서 주어진 키의 실수 값을 추출합니다
// 찾으면 값을 반환하고, 그렇지 않으면 none을 반환합니다
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

// extract_json_number extracts an integer number value for a given key from JSON
// Returns the value if found, none otherwise
fn extract_json_number(json_str string, key string) ?int {
	return extract_json_int(json_str, key)
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

// escape_json_string escapes special characters for JSON embedding
// Returns the escaped string with quotes
fn escape_json_string(s string) string {
	// Escape special characters for JSON embedding
	return '"${s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r',
		'\\r').replace('\t', '\\t')}"'
}
