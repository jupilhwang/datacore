// Service Layer - Avro Binary Encoder
// Provides binary encoding/decoding for Avro schemas
// https://avro.apache.org/docs/current/specification/
module schema

import infra.performance.core

// AvroEncoder provides binary encoding and decoding for Avro data
/// AvroEncoder provides binary encoding and decoding for Avro data.
pub struct AvroEncoder {}

// new_avro_encoder creates a new Avro encoder
/// new_avro_encoder creates a new Avro encoder.
pub fn new_avro_encoder() !AvroEncoder {
	return AvroEncoder{}
}

// format returns the encoding format
/// format returns the encoding format.
pub fn (mut e AvroEncoder) format() Format {
	return Format.avro
}

// encode encodes JSON data to Avro binary format
/// encode encodes JSON data to Avro binary format.
pub fn (mut e AvroEncoder) encode(data []u8, schema_str string) ![]u8 {
	// Parse the schema
	schema := parse_avro_schema(schema_str) or {
		return error('failed to parse Avro schema: ${err}')
	}

	// Convert data []u8 to string
	json_str := data.bytestr()

	// Encode based on schema type
	match schema.schema_type {
		'record' {
			data_map := parse_json_map(json_str) or { return error('invalid JSON object') }
			return e.encode_record(data_map, schema)
		}
		'enum' {
			return e.encode_enum_value(json_str, schema)
		}
		'array' {
			return e.encode_array(json_str, schema)
		}
		'map' {
			return e.encode_map(json_str, schema)
		}
		'string' {
			str := parse_json_string_value(json_str) or { return error('invalid string') }
			return encode_string(str)
		}
		'bytes' {
			bytes := parse_json_bytes(json_str) or { return error('invalid bytes') }
			return encode_bytes(bytes)
		}
		'int', 'long' {
			val := parse_json_long(json_str) or { return error('invalid integer') }
			return encode_varint_zigzag(val)
		}
		'float' {
			val := parse_json_float(json_str) or { return error('invalid float') }
			return encode_float(f32(val))
		}
		'double' {
			val := parse_json_double(json_str) or { return error('invalid double') }
			return encode_double(val)
		}
		'boolean' {
			val := parse_json_bool(json_str) or { return error('invalid boolean') }
			return if val { [u8(1)] } else { [u8(0)] }
		}
		'null' {
			return []u8{}
		}
		else {
			return error('unsupported schema type: ${schema.schema_type}')
		}
	}
}

// decode decodes Avro binary data to JSON
/// decode decodes Avro binary data to JSON.
pub fn (mut e AvroEncoder) decode(data []u8, schema_str string) ![]u8 {
	// Parse the schema
	schema := parse_avro_schema(schema_str) or {
		return error('failed to parse Avro schema: ${err}')
	}

	mut reader := AvroReader{
		data: data
		pos:  0
	}

	// Decode based on schema type and convert result to []u8
	result := match schema.schema_type {
		'record' {
			e.decode_record(mut reader, schema)!
		}
		'enum' {
			e.decode_enum(mut reader, schema)!
		}
		'array' {
			e.decode_array(mut reader, schema)!
		}
		'map' {
			e.decode_map(mut reader, schema)!
		}
		'string' {
			str := decode_string(mut reader)!
			escape_json_string(str)
		}
		'bytes' {
			bytes := decode_bytes(mut reader)!
			format_json_bytes(bytes)
		}
		'int', 'long' {
			val := decode_varint_zigzag(mut reader)!
			'${val}'
		}
		'float' {
			val := decode_float(mut reader)!
			'${val}'
		}
		'double' {
			val := decode_double(mut reader)!
			'${val}'
		}
		'boolean' {
			val := decode_bool(mut reader)!
			if val {
				'true'
			} else {
				'false'
			}
		}
		'null' {
			'null'
		}
		else {
			return error('unsupported schema type: ${schema.schema_type}')
		}
	}

	return result.bytes()
}

// Record encoding/decoding

fn (mut e AvroEncoder) encode_record(data map[string]string, schema AvroSchema) ![]u8 {
	mut result := []u8{}

	for field in schema.fields {
		value := data[field.name] or {
			// Field not present - use default if available
			if field.has_default {
				result << e.encode_field_value(field, field.field_type, field.is_nullable,
					field.union_types, '', schema)!
				continue
			}
			// Skip optional fields
			if field.is_nullable {
				// Encode null for nullable fields not present
				if field.is_union {
					result << e.encode_union_value('', field.union_types, schema)!
				} else {
					result << [u8(0)]
				}
				continue
			}
			continue
		}

		if is_json_null(value) {
			if field.is_nullable || field.is_union {
				if field.is_union {
					result << e.encode_union_value('', field.union_types, schema)!
				} else {
					result << [u8(0)]
				}
			}
			continue
		}

		result << e.encode_field_value(field, field.field_type, field.is_nullable, field.union_types,
			value, schema)!
	}

	return result
}

fn (mut e AvroEncoder) decode_record(mut reader AvroReader, schema AvroSchema) !string {
	mut fields := map[string]string{}

	for field in schema.fields {
		// For nullable fields, check if next byte indicates null
		if field.is_nullable || field.is_union {
			if reader.pos >= reader.data.len {
				// Use default if available
				if field.has_default {
					fields[field.name] = field.field_type
				}
				continue
			}
		}

		value := e.decode_field_value(field, mut reader, schema) or {
			if field.has_default {
				fields[field.name] = field.field_type // Simplified default
				continue
			}
			return error('failed to decode field ${field.name}: ${err}')
		}

		fields[field.name] = value
	}

	// Build JSON object
	mut parts := []string{}
	for name, value in fields {
		parts << '"${name}":${value}'
	}

	return '{${parts.join(',')}}'
}

fn (mut e AvroEncoder) encode_field_value(field AvroField, field_type string, is_nullable bool, union_types []string, value string, schema AvroSchema) ![]u8 {
	if field.is_union {
		return e.encode_union_value(value, union_types, schema)
	}

	return e.encode_value_by_type(field_type, value, schema)
}

fn (mut e AvroEncoder) encode_value_by_type(field_type string, value string, schema AvroSchema) ![]u8 {
	match field_type {
		'string' {
			str := parse_json_string_value(value) or { return error('invalid string') }
			return encode_string(str)
		}
		'bytes' {
			bytes := parse_json_bytes(value) or { return error('invalid bytes') }
			return encode_bytes(bytes)
		}
		'int' {
			val := parse_json_int(value) or { return error('invalid int') }
			return encode_varint_zigzag(i64(val))
		}
		'long' {
			val := parse_json_long(value) or { return error('invalid long') }
			return encode_varint_zigzag(val)
		}
		'float' {
			val := parse_json_float(value) or { return error('invalid float') }
			return encode_float(f32(val))
		}
		'double' {
			val := parse_json_double(value) or { return error('invalid double') }
			return encode_double(val)
		}
		'boolean' {
			val := parse_json_bool(value) or { return error('invalid boolean') }
			return if val { [u8(1)] } else { [u8(0)] }
		}
		'record' {
			// Handle nested record
			return e.encode_record(parse_json_map(value) or {
				return error('invalid nested record')
			}, schema)!
		}
		'array' {
			return e.encode_array(value, schema)
		}
		'map' {
			return e.encode_map(value, schema)
		}
		'null' {
			return []u8{}
		}
		else {
			// Check if it's an enum
			if field_type in schema.symbols {
				return e.encode_enum_value(value, schema)
			}
			return error('unsupported field type: ${field_type}')
		}
	}
}

fn (mut e AvroEncoder) decode_field_value(field AvroField, mut reader AvroReader, schema AvroSchema) !string {
	if field.is_union {
		return e.decode_union_value(field.union_types, mut reader, schema)
	}

	return e.decode_value_by_type(field.field_type, mut reader, schema)
}

fn (mut e AvroEncoder) decode_value_by_type(field_type string, mut reader AvroReader, schema AvroSchema) !string {
	match field_type {
		'string' {
			str := decode_string(mut reader)!
			return escape_json_string(str)
		}
		'bytes' {
			bytes := decode_bytes(mut reader)!
			return format_json_bytes(bytes)
		}
		'int' {
			val := decode_varint_zigzag(mut reader)!
			return '${val}'
		}
		'long' {
			val := decode_varint_zigzag(mut reader)!
			return '${val}'
		}
		'float' {
			val := decode_float(mut reader)!
			return '${val}'
		}
		'double' {
			val := decode_double(mut reader)!
			return '${val}'
		}
		'boolean' {
			val := decode_bool(mut reader)!
			return if val { 'true' } else { 'false' }
		}
		'record' {
			return e.decode_record(mut reader, schema)
		}
		'array' {
			return e.decode_array(mut reader, schema)
		}
		'map' {
			return e.decode_map(mut reader, schema)
		}
		'null' {
			return 'null'
		}
		else {
			// Check if it's an enum
			if field_type in schema.symbols {
				return e.decode_enum(mut reader, schema)
			}
			return error('unsupported field type: ${field_type}')
		}
	}
}

// Union encoding/decoding

fn (mut e AvroEncoder) encode_union_value(value string, union_types []string, schema AvroSchema) ![]u8 {
	if value == '' || is_json_null(value) {
		// Encode null
		null_index := union_types.index('null')
		if null_index == -1 {
			return error('null not in union')
		}
		mut result := encode_varint_zigzag(i64(null_index))
		return result
	}

	// Determine the type of the value and encode with type index
	for i, union_type in union_types {
		if union_type == 'null' {
			continue
		}

		// Try to match the value to a union type
		if e.matches_union_type(value, union_type, schema) {
			mut result := encode_varint_zigzag(i64(i))
			encoded := e.encode_value_by_type(union_type, value, schema) or { continue }
			result << encoded
			return result
		}
	}

	return error('value does not match any union type')
}

fn (mut e AvroEncoder) matches_union_type(value string, union_type string, schema AvroSchema) bool {
	match union_type {
		'string' {
			return value.starts_with('"')
		}
		'bytes' {
			return value.starts_with('[') || value.starts_with('"')
		}
		'int', 'long' {
			// Check if value can be parsed as an integer
			return value.trim_space().int() != 0
				|| value.trim_space().parse_int(10, 64) or { 0 } != 0
		}
		'float', 'double' {
			// Check if value can be parsed as a number
			return value.f64() != 0
		}
		'boolean' {
			return value == 'true' || value == 'false'
		}
		'record' {
			return value.starts_with('{')
		}
		'array' {
			return value.starts_with('[')
		}
		'map' {
			return value.starts_with('{')
		}
		else {
			// Check against enum symbols
			return value.starts_with('"') && union_type in schema.symbols
		}
	}
}

fn (mut e AvroEncoder) decode_union_value(union_types []string, mut reader AvroReader, schema AvroSchema) !string {
	if reader.pos >= reader.data.len {
		return 'null'
	}

	// Read type index
	type_index := int(decode_varint_zigzag(mut reader)!)

	if type_index < 0 || type_index >= union_types.len {
		return error('invalid union type index: ${type_index}')
	}

	union_type := union_types[type_index]

	if union_type == 'null' {
		return 'null'
	}

	return e.decode_value_by_type(union_type, mut reader, schema)
}

// Enum encoding/decoding

fn (mut e AvroEncoder) encode_enum_value(value string, schema AvroSchema) ![]u8 {
	// Parse the symbol from JSON string
	symbol := parse_json_string_value(value) or { return error('invalid enum value') }

	// Find symbol index
	symbol_index := schema.symbols.index(symbol)
	if symbol_index == -1 {
		return error('unknown enum symbol: ${symbol}')
	}

	return encode_varint_zigzag(i64(symbol_index))
}

fn (mut e AvroEncoder) decode_enum(mut reader AvroReader, schema AvroSchema) !string {
	symbol_index := int(decode_varint_zigzag(mut reader)!)

	if symbol_index < 0 || symbol_index >= schema.symbols.len {
		return error('invalid enum symbol index: ${symbol_index}')
	}

	symbol := schema.symbols[symbol_index]
	return escape_json_string(symbol)
}

// Array encoding/decoding

fn (mut e AvroEncoder) encode_array(json_str string, schema AvroSchema) ![]u8 {
	items := parse_json_array(json_str) or { return error('invalid JSON array') }

	mut result := []u8{}

	// Encode array length
	result << encode_varint_zigzag(i64(items.len))

	// Encode each item
	for item in items {
		encoded := e.encode_value_by_type(schema.items_type, item, schema) or {
			return error('failed to encode array item: ${err}')
		}
		result << encoded
	}

	return result
}

fn (mut e AvroEncoder) decode_array(mut reader AvroReader, schema AvroSchema) !string {
	// Read array length
	length := int(decode_varint_zigzag(mut reader)!)

	mut items := []string{}

	for _ in 0 .. length {
		item := e.decode_value_by_type(schema.items_type, mut reader, schema) or {
			return error('failed to decode array item: ${err}')
		}
		items << item
	}

	return '[${items.join(',')}]'
}

// Map encoding/decoding

fn (mut e AvroEncoder) encode_map(json_str string, schema AvroSchema) ![]u8 {
	data := parse_json_map(json_str) or { return error('invalid JSON object') }

	mut result := []u8{}

	// Encode pair count
	result << encode_varint_zigzag(i64(data.len))

	// Encode each key-value pair
	for key, value in data {
		// Encode key as string
		result << encode_string(key)

		// Encode value
		encoded := e.encode_value_by_type(schema.values_type, value, schema) or {
			return error('failed to encode map value: ${err}')
		}
		result << encoded
	}

	return result
}

fn (mut e AvroEncoder) decode_map(mut reader AvroReader, schema AvroSchema) !string {
	// Read pair count
	pair_count := int(decode_varint_zigzag(mut reader)!)

	mut pairs := []string{}

	for _ in 0 .. pair_count {
		// Read key
		key := decode_string(mut reader)!

		// Read value
		value := e.decode_value_by_type(schema.values_type, mut reader, schema) or {
			return error('failed to decode map value: ${err}')
		}

		pairs << '"${key}":${value}'
	}

	return '{${pairs.join(',')}}'
}

// Fixed encoding/decoding

fn (mut e AvroEncoder) encode_fixed(data []u8, schema AvroSchema) ![]u8 {
	if data.len != schema.fixed_size {
		return error('fixed size mismatch: expected ${schema.fixed_size}, got ${data.len}')
	}
	return data
}

fn (mut e AvroEncoder) decode_fixed(mut reader AvroReader, schema AvroSchema) ![]u8 {
	if reader.pos + schema.fixed_size > reader.data.len {
		return error('unexpected end of fixed data')
	}

	result := reader.data[reader.pos..reader.pos + schema.fixed_size]
	reader.pos += schema.fixed_size
	return result
}

// Low-level decoding functions using primitives

fn decode_varint_zigzag(mut reader AvroReader) !i64 {
	val, n := core.decode_varint(reader.data[reader.pos..])
	if n <= 0 {
		return error('invalid or incomplete varint')
	}
	reader.pos += n
	return val
}

fn decode_string(mut reader AvroReader) !string {
	length := int(decode_varint_zigzag(mut reader)!)

	if reader.pos + length > reader.data.len {
		return error('unexpected end of string')
	}

	str := reader.data[reader.pos..reader.pos + length].bytestr()
	reader.pos += length
	return str
}

fn decode_bytes(mut reader AvroReader) ![]u8 {
	length := int(decode_varint_zigzag(mut reader)!)

	if reader.pos + length > reader.data.len {
		return error('unexpected end of bytes')
	}

	result := reader.data[reader.pos..reader.pos + length]
	reader.pos += length
	return result
}

fn decode_float(mut reader AvroReader) !f32 {
	if reader.pos + 4 > reader.data.len {
		return error('unexpected end of float')
	}

	bits := core.read_i32_le(reader.data[reader.pos..reader.pos+4])
	reader.pos += 4

	return *unsafe { &f32(&bits) }
}

fn decode_double(mut reader AvroReader) !f64 {
	if reader.pos + 8 > reader.data.len {
		return error('unexpected end of double')
	}

	bits := core.read_i64_le(reader.data[reader.pos..reader.pos+8])
	reader.pos += 8

	return *unsafe { &f64(&bits) }
}

fn decode_bool(mut reader AvroReader) !bool {
	if reader.pos >= reader.data.len {
		return error('unexpected end of boolean')
	}

	val := reader.data[reader.pos]
	reader.pos += 1
	return val != 0
}

// AvroReader provides a reader interface for decoding Avro binary data
struct AvroReader {
mut:
	data []u8
	pos  int
}

// JSON parsing helpers

fn parse_json_bytes(json_str string) ?[]u8 {
	// Handle ["byte1", "byte2", ...] format
	if json_str.starts_with('[') {
		items := parse_json_array(json_str) or { return none }
		mut result := []u8{}
		for item in items {
			// Parse individual byte values
			val := item.int()
			if val >= 0 && val <= 255 {
				result << u8(val)
			} else {
				return none
			}
		}
		return result
	}

	// Handle base64 encoded string
	if json_str.starts_with('"') {
		str := parse_json_string_value(json_str) or { return none }
		// Simple hex or base64 handling
		if str.starts_with('0x') || str.starts_with('0X') {
			hex_str := str[2..]
			if hex_str.len % 2 != 0 {
				return none
			}
			mut result := []u8{}
			for i := 0; i < hex_str.len; i += 2 {
				high := hex_str[i]
				low := hex_str[i + 1]
				byte_val := (u8(core.hex_char_to_nibble(high)) << 4) | u8(core.hex_char_to_nibble(low))
				result << byte_val
			}
			return result
		}
		return str.bytes()
	}

	return none
}

fn format_json_bytes(bytes []u8) string {
	mut parts := []string{}
	for b in bytes {
		parts << '${b}'
	}
	return '[${parts.join(',')}]'
}

fn parse_json_long(json_str string) ?i64 {
	trimmed := json_str.trim_space()
	if trimmed == 'null' {
		return none
	}
	return trimmed.i64()
}

fn parse_json_float(json_str string) ?f64 {
	trimmed := json_str.trim_space()
	if trimmed == 'null' {
		return none
	}
	return trimmed.f64()
}

fn parse_json_double(json_str string) ?f64 {
	return parse_json_float(json_str)
}

fn parse_json_bool(json_str string) ?bool {
	trimmed := json_str.trim_space()
	if trimmed == 'true' {
		return true
	}
	if trimmed == 'false' {
		return false
	}
	return none
}

fn is_json_null(json_str string) bool {
	return json_str.trim_space() == 'null'
}
