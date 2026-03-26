// Service Layer - Schema Encoder JSON Helpers
// JSON parsing utilities for Avro encoding/decoding
module schema

import x.json2

// parse_json_int parses a bare JSON integer string
fn parse_json_int(s string) ?int {
	raw := json2.decode[json2.Any](s.trim_space()) or { return none }
	return raw.int()
}

// parse_json_string_value extracts the string from a quoted JSON string value
fn parse_json_string_value(s string) ?string {
	trimmed := s.trim_space()
	if !trimmed.starts_with('"') || !trimmed.ends_with('"') {
		return none
	}
	raw := json2.decode[json2.Any](trimmed) or { return none }
	return raw.str()
}

// parse_json_array parses a JSON array, returning each element as its raw JSON string
fn parse_json_array(s string) ?[]string {
	raw := json2.decode[json2.Any](s.trim_space()) or { return none }
	arr := raw.arr()
	mut items := []string{}
	for item in arr {
		items << item.json_str()
	}
	return items
}

// parse_json_map parses a JSON object, returning values as raw JSON strings
fn parse_json_map(s string) ?map[string]string {
	raw := json2.decode[json2.Any](s.trim_space()) or { return none }
	obj := raw.as_map()
	mut result := map[string]string{}
	for k, v in obj {
		result[k] = v.json_str()
	}
	return result
}
