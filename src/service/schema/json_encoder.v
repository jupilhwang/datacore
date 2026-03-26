// Service Layer - JSON Schema encoder
// Provides validation-based encoding/decoding for JSON Schema
module schema

import regex
import x.json2

// JsonEncoder provides JSON schema validation and encoding
/// JsonEncoder provides JSON schema validation and encoding.
pub struct JsonEncoder {}

// new_json_encoder creates a new JSON encoder
/// new_json_encoder creates a new JSON encoder.
pub fn new_json_encoder() !JsonEncoder {
	return JsonEncoder{}
}

// format returns the encoding format
/// format returns the encoding format.
pub fn (mut e JsonEncoder) format() Format {
	return Format.json
}

// encode validates and encodes JSON data (passthrough for JSON)
/// encode validates and encodes JSON data (passthrough for JSON).
pub fn (mut e JsonEncoder) encode(data []u8, schema_str string) ![]u8 {
	// Parse and validate the schema
	schema := parse_json_schema(schema_str) or {
		return error('failed to parse JSON schema: ${err}')
	}

	// Validate the data
	json_str := data.bytestr()
	validate(json_str, schema) or { return error('JSON validation failed: ${err}') }

	// For JSON schema, we just return the data as-is (already valid JSON)
	return data
}

// decode decodes JSON data (passthrough for JSON)
/// decode decodes JSON data (passthrough for JSON).
pub fn (mut e JsonEncoder) decode(data []u8, schema_str string) ![]u8 {
	// Parse and validate the schema
	_ = parse_json_schema(schema_str) or { return error('failed to parse JSON schema: ${err}') }

	// For JSON schema, we just return the data as-is
	return data
}

// validate checks if JSON data conforms to schema
fn validate(json_str string, schema &JsonSchema) ! {
	trimmed := json_str.trim_space()

	// Check type constraint
	if schema.schema_type.len > 0 {
		actual_type := detect_json_type(trimmed)

		// Handle array of types
		if schema.schema_type !in ['any', actual_type] {
			// Check for numeric type compatibility
			if !(schema.schema_type == 'number' && actual_type == 'integer') {
				if !(schema.schema_type == 'integer' && actual_type == 'number'
					&& is_json_integer(trimmed)) {
					return error('type mismatch: expected ${schema.schema_type}, got ${actual_type}')
				}
			}
		}
	}

	// Validate based on type
	match detect_json_type(trimmed) {
		'object' {
			validate_object(trimmed, schema)!
		}
		'array' {
			validate_array(trimmed, schema)!
		}
		'string' {
			validate_string(trimmed, schema)!
		}
		'number', 'integer' {
			validate_number(trimmed, schema)!
		}
		else {
			// null, boolean - no additional validation
		}
	}
}

// validate_object validates JSON object against schema
fn validate_object(json_str string, schema &JsonSchema) ! {
	// Parse object properties
	props := parse_json_map(json_str) or { return error('invalid JSON object') }

	// Check required properties
	for required in schema.required {
		if required !in props {
			return error('missing required property: ${required}')
		}
	}

	// Validate each property
	for name, value in props {
		if prop_schema := schema.properties[name] {
			validate(value, prop_schema)!
		} else if !schema.additional_properties {
			// additionalProperties: false
			return error('unexpected property: ${name}')
		}
	}

	// Check minProperties/maxProperties
	if schema.min_properties > 0 && props.len < schema.min_properties {
		return error('too few properties: minimum ${schema.min_properties}, got ${props.len}')
	}
	if schema.max_properties > 0 && props.len > schema.max_properties {
		return error('too many properties: maximum ${schema.max_properties}, got ${props.len}')
	}
}

// validate_array validates JSON array against schema
fn validate_array(json_str string, schema &JsonSchema) ! {
	items := parse_json_array(json_str) or { return error('invalid JSON array') }

	// Check minItems/maxItems
	if schema.min_items > 0 && items.len < schema.min_items {
		return error('too few items: minimum ${schema.min_items}, got ${items.len}')
	}
	if schema.max_items > 0 && items.len > schema.max_items {
		return error('too many items: maximum ${schema.max_items}, got ${items.len}')
	}

	// Validate items against items schema
	if schema.items_schema != unsafe { nil } && schema.items_schema.schema_type.len > 0 {
		for i, item in items {
			validate(item, schema.items_schema) or {
				return error('invalid item at index ${i}: ${err}')
			}
		}
	}

	// Check uniqueItems
	if schema.unique_items {
		mut seen := map[string]bool{}
		for item in items {
			normalized := normalize_json(item)
			if normalized in seen {
				return error('duplicate item in array')
			}
			seen[normalized] = true
		}
	}
}

// validate_string validates JSON string against schema
fn validate_string(json_str string, schema &JsonSchema) ! {
	str := parse_json_string_value(json_str) or { return error('invalid JSON string') }

	// Check minLength/maxLength
	if schema.min_length > 0 && str.len < schema.min_length {
		return error('string too short: minimum ${schema.min_length}, got ${str.len}')
	}
	if schema.max_length > 0 && str.len > schema.max_length {
		return error('string too long: maximum ${schema.max_length}, got ${str.len}')
	}

	// Check pattern
	if schema.pattern.len > 0 {
		mut re := regex.regex_opt(schema.pattern) or {
			return error('invalid regex pattern: ${schema.pattern}')
		}
		if !re.matches_string(str) {
			return error('string does not match pattern: ${schema.pattern}')
		}
	}

	// Check enum values
	if schema.enum_values.len > 0 {
		mut found := false
		for v in schema.enum_values {
			if v == str {
				found = true
				break
			}
		}
		if !found {
			return error('value not in enum: ${str}')
		}
	}

	// Check format (common formats)
	if schema.format.len > 0 {
		match schema.format {
			'email' {
				if !str.contains('@') || !str.contains('.') {
					return error('invalid email format')
				}
			}
			'uri', 'url' {
				if !str.starts_with('http://') && !str.starts_with('https://') {
					return error('invalid URI format')
				}
			}
			'date' {
				// YYYY-MM-DD
				if str.len != 10 || str[4] != `-` || str[7] != `-` {
					return error('invalid date format')
				}
			}
			'date-time' {
				// ISO 8601
				if !str.contains('T') {
					return error('invalid date-time format')
				}
			}
			'uuid' {
				if str.len != 36 || str[8] != `-` || str[13] != `-` || str[18] != `-`
					|| str[23] != `-` {
					return error('invalid UUID format')
				}
			}
			else {
				// Unknown format - ignore
			}
		}
	}
}

// validate_number validates JSON number against schema
fn validate_number(json_str string, schema &JsonSchema) ! {
	val := json_str.trim_space().f64()

	// Check minimum/maximum
	if schema.has_minimum && val < schema.minimum {
		return error('value below minimum: ${val} < ${schema.minimum}')
	}
	if schema.has_maximum && val > schema.maximum {
		return error('value above maximum: ${val} > ${schema.maximum}')
	}
	if schema.has_exclusive_minimum && val <= schema.exclusive_minimum {
		return error('value not greater than exclusive minimum: ${val} <= ${schema.exclusive_minimum}')
	}
	if schema.has_exclusive_maximum && val >= schema.exclusive_maximum {
		return error('value not less than exclusive maximum: ${val} >= ${schema.exclusive_maximum}')
	}

	// Check multipleOf
	if schema.multiple_of > 0 {
		remainder := val - f64(int(val / schema.multiple_of)) * schema.multiple_of
		if remainder > 0.0001 && remainder < schema.multiple_of - 0.0001 {
			return error('value not multiple of ${schema.multiple_of}')
		}
	}
}

// JSON Schema Parsing

struct JsonSchema {
mut:
	schema_type           string
	properties            map[string]&JsonSchema
	required              []string
	additional_properties bool = true
	min_properties        int
	max_properties        int

	items_schema &JsonSchema = unsafe { nil }
	min_items    int
	max_items    int
	unique_items bool

	min_length  int
	max_length  int
	pattern     string
	format      string
	enum_values []string

	minimum               f64
	maximum               f64
	exclusive_minimum     f64
	exclusive_maximum     f64
	has_minimum           bool
	has_maximum           bool
	has_exclusive_minimum bool
	has_exclusive_maximum bool
	multiple_of           f64
}

fn parse_json_schema(schema_str string) !&JsonSchema {
	raw := json2.decode[json2.Any](schema_str)!
	return build_json_schema_from_map(raw.as_map())
}

// build_json_schema_from_map recursively builds a JsonSchema from a decoded JSON map.
// Terminates when no nested 'properties' or 'items' keys exist in the map.
fn build_json_schema_from_map(obj map[string]json2.Any) !&JsonSchema {
	mut result := &JsonSchema{
		properties: map[string]&JsonSchema{}
	}

	if v := obj['type'] {
		result.schema_type = v.str()
	}

	if props_val := obj['properties'] {
		for name, prop_any in props_val.as_map() {
			result.properties[name] = build_json_schema_from_map(prop_any.as_map())!
		}
	}

	if req_val := obj['required'] {
		for item in req_val.arr() {
			result.required << item.str()
		}
	}

	if ap_val := obj['additionalProperties'] {
		if ap_val.json_str() == 'false' {
			result.additional_properties = false
		}
	}

	if items_val := obj['items'] {
		items_map := items_val.as_map()
		if items_map.len > 0 {
			result.items_schema = build_json_schema_from_map(items_map)!
		}
	}

	apply_int_constraints(mut result, obj)
	apply_float_constraints(mut result, obj)
	apply_string_constraints(mut result, obj)

	return result
}

fn apply_string_constraints(mut schema JsonSchema, obj map[string]json2.Any) {
	if v := obj['pattern'] {
		schema.pattern = v.str()
	}
	if v := obj['format'] {
		schema.format = v.str()
	}

	if enum_val := obj['enum'] {
		for item in enum_val.arr() {
			schema.enum_values << item.str()
		}
	}

	if v := obj['uniqueItems'] {
		if v.json_str() == 'true' {
			schema.unique_items = true
		}
	}
}

fn apply_int_constraints(mut schema JsonSchema, obj map[string]json2.Any) {
	if v := obj['minLength'] {
		schema.min_length = v.int()
	}
	if v := obj['maxLength'] {
		schema.max_length = v.int()
	}
	if v := obj['minItems'] {
		schema.min_items = v.int()
	}
	if v := obj['maxItems'] {
		schema.max_items = v.int()
	}
	if v := obj['minProperties'] {
		schema.min_properties = v.int()
	}
	if v := obj['maxProperties'] {
		schema.max_properties = v.int()
	}
}

fn apply_float_constraints(mut schema JsonSchema, obj map[string]json2.Any) {
	if v := obj['minimum'] {
		schema.minimum = v.f64()
		schema.has_minimum = true
	}
	if v := obj['maximum'] {
		schema.maximum = v.f64()
		schema.has_maximum = true
	}
	if v := obj['exclusiveMinimum'] {
		schema.exclusive_minimum = v.f64()
		schema.has_exclusive_minimum = true
	}
	if v := obj['exclusiveMaximum'] {
		schema.exclusive_maximum = v.f64()
		schema.has_exclusive_maximum = true
	}
	if v := obj['multipleOf'] {
		schema.multiple_of = v.f64()
	}
}

// JSON Utility Functions

fn detect_json_type(s string) string {
	trimmed := s.trim_space()

	if trimmed.len == 0 {
		return 'null'
	}

	c := trimmed[0]

	if c == `{` {
		return 'object'
	} else if c == `[` {
		return 'array'
	} else if c == `"` {
		return 'string'
	} else if c == `t` || c == `f` {
		return 'boolean'
	} else if c == `n` {
		return 'null'
	} else if c == `-` || (c >= `0` && c <= `9`) {
		if trimmed.contains('.') || trimmed.contains('e') || trimmed.contains('E') {
			return 'number'
		}
		return 'integer'
	}

	return 'unknown'
}

fn is_json_integer(s string) bool {
	trimmed := s.trim_space()
	if trimmed.contains('.') {
		// Check if decimal part is zero
		parts := trimmed.split('.')
		if parts.len == 2 {
			decimal := parts[1].trim_right('0')
			return decimal.len == 0
		}
		return false
	}
	return !trimmed.contains('e') && !trimmed.contains('E')
}

fn normalize_json(s string) string {
	// Simple JSON normalization: remove extra whitespace
	mut result := []u8{}
	mut in_string := false
	mut escaped := false

	for c in s {
		if escaped {
			escaped = false
			result << c
			continue
		}

		if c == `\\` && in_string {
			escaped = true
			result << c
			continue
		}

		if c == `"` {
			in_string = !in_string
			result << c
			continue
		}

		if in_string {
			result << c
		} else if c != ` ` && c != `\t` && c != `\n` && c != `\r` {
			result << c
		}
	}

	return result.bytestr()
}
