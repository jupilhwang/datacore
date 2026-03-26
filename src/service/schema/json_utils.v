// Service Layer - JSON parsing utilities
// Provides JSON parsing helper functions for schema validation and compatibility checks
module schema

import x.json2

// extract_json_string extracts the string value for a given key from JSON
// Returns the value if found, otherwise returns none
pub fn extract_json_string(json_str string, key string) ?string {
	raw := json2.decode[json2.Any](json_str) or { return none }
	obj := raw.as_map()
	val := obj[key] or { return none }
	str := val.str()
	if str.len == 0 && val.json_str() == '""' {
		return ''
	}
	if str.len == 0 {
		return none
	}
	return str
}

// extract_json_int extracts the integer value for a given key from JSON
// Returns the value if found, otherwise returns none
pub fn extract_json_int(json_str string, key string) ?int {
	raw := json2.decode[json2.Any](json_str) or { return none }
	obj := raw.as_map()
	val := obj[key] or { return none }
	return val.int()
}

// extract_json_float extracts the float value for a given key from JSON
// Returns the value if found, otherwise returns none
fn extract_json_float(json_str string, key string) ?f64 {
	raw := json2.decode[json2.Any](json_str) or { return none }
	obj := raw.as_map()
	val := obj[key] or { return none }
	return val.f64()
}

// parse_json_string_array extracts a string array value for a given key from JSON
// Returns the array if found, none otherwise
fn parse_json_string_array(json_str string, key string) ?[]string {
	raw := json2.decode[json2.Any](json_str) or { return none }
	obj := raw.as_map()
	arr_val := obj[key] or { return none }
	arr := arr_val.arr()
	mut result := []string{}
	for item in arr {
		result << item.str()
	}
	return result
}

// is_valid_json performs JSON validation using stdlib parser
// Returns true if the string is valid JSON
fn is_valid_json(s string) bool {
	trimmed := s.trim_space()
	if trimmed.len == 0 {
		return false
	}
	json2.decode[json2.Any](trimmed) or { return false }
	return true
}
