// Service Layer - Schema Encoder
// Provides binary encoding/decoding for Avro, JSON Schema, and Protobuf
module schema

import domain

// SchemaEncoder provides encoding/decoding functionality
pub interface SchemaEncoder {
	encode(data []u8, schema domain.Schema) ![]u8
	decode(data []u8, schema domain.Schema) ![]u8
}

// ============================================================================
// Avro Binary Encoder
// Implements Apache Avro 1.11 specification binary encoding
// https://avro.apache.org/docs/current/spec.html#binary_encoding
// ============================================================================

pub struct AvroEncoder {}

pub fn new_avro_encoder() &AvroEncoder {
	return &AvroEncoder{}
}

// encode serializes data according to Avro schema
pub fn (e &AvroEncoder) encode(data []u8, schema domain.Schema) ![]u8 {
	parsed := parse_avro_schema(schema.schema_str) or {
		return error('failed to parse Avro schema: ${err}')
	}

	// Parse input JSON data
	json_str := data.bytestr()

	return e.encode_value(json_str, parsed)
}

// decode deserializes Avro binary data to JSON
pub fn (e &AvroEncoder) decode(data []u8, schema domain.Schema) ![]u8 {
	parsed := parse_avro_schema(schema.schema_str) or {
		return error('failed to parse Avro schema: ${err}')
	}

	mut reader := AvroReader{
		data: data
		pos:  0
	}
	json_result := e.decode_value(mut reader, parsed)!

	return json_result.bytes()
}

// encode_value encodes a JSON value according to schema type
fn (e &AvroEncoder) encode_value(json_str string, schema AvroSchema) ![]u8 {
	match schema.schema_type {
		'null' {
			return []u8{} // null is encoded as zero bytes
		}
		'boolean' {
			val := parse_json_bool(json_str) or { return error('invalid boolean value') }
			return if val { [u8(1)] } else { [u8(0)] }
		}
		'int' {
			val := parse_json_int(json_str) or { return error('invalid int value') }
			return encode_varint_zigzag(i64(val))
		}
		'long' {
			val := parse_json_long(json_str) or { return error('invalid long value') }
			return encode_varint_zigzag(val)
		}
		'float' {
			val := parse_json_float(json_str) or { return error('invalid float value') }
			return encode_float(val)
		}
		'double' {
			val := parse_json_double(json_str) or { return error('invalid double value') }
			return encode_double(val)
		}
		'bytes' {
			bytes := parse_json_bytes(json_str) or { return error('invalid bytes value') }
			return encode_bytes(bytes)
		}
		'string' {
			str := parse_json_string_value(json_str) or { return error('invalid string value') }
			return encode_string(str)
		}
		'array' {
			return e.encode_array(json_str, schema)
		}
		'map' {
			return e.encode_map(json_str, schema)
		}
		'record' {
			return e.encode_record(json_str, schema)
		}
		'enum' {
			return e.encode_enum(json_str, schema)
		}
		'fixed' {
			return e.encode_fixed(json_str, schema)
		}
		'union' {
			return e.encode_union(json_str, schema)
		}
		else {
			return error('unknown Avro type: ${schema.schema_type}')
		}
	}
}

// decode_value decodes Avro binary data to JSON string
fn (e &AvroEncoder) decode_value(mut reader AvroReader, schema AvroSchema) !string {
	match schema.schema_type {
		'null' {
			return 'null'
		}
		'boolean' {
			if reader.pos >= reader.data.len {
				return error('unexpected end of data')
			}
			val := reader.data[reader.pos] != 0
			reader.pos += 1
			return if val { 'true' } else { 'false' }
		}
		'int' {
			val := decode_varint_zigzag_int(mut reader)!
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
		'bytes' {
			bytes := decode_bytes(mut reader)!
			return format_json_bytes(bytes)
		}
		'string' {
			str := decode_string(mut reader)!
			return '"${escape_json_str(str)}"'
		}
		'array' {
			return e.decode_array(mut reader, schema)
		}
		'map' {
			return e.decode_map(mut reader, schema)
		}
		'record' {
			return e.decode_record(mut reader, schema)
		}
		'enum' {
			return e.decode_enum(mut reader, schema)
		}
		'fixed' {
			return e.decode_fixed(mut reader, schema)
		}
		'union' {
			return e.decode_union(mut reader, schema)
		}
		else {
			return error('unknown Avro type: ${schema.schema_type}')
		}
	}
}

// ============================================================================
// Avro Encoding Helpers
// ============================================================================

// encode_varint_zigzag encodes an i64 using variable-length zigzag encoding
fn encode_varint_zigzag(val i64) []u8 {
	// ZigZag encoding: (n << 1) ^ (n >> 63)
	zigzag := u64((val << 1) ^ (val >> 63))
	return encode_varint(zigzag)
}

// encode_varint encodes a u64 using variable-length encoding
fn encode_varint(val u64) []u8 {
	mut result := []u8{}
	mut n := val

	for {
		mut b := u8(n & 0x7F)
		n = n >> 7
		if n != 0 {
			b |= 0x80 // More bytes to follow
		}
		result << b
		if n == 0 {
			break
		}
	}

	return result
}

// encode_float encodes a f32 in little-endian IEEE 754 format
fn encode_float(val f32) []u8 {
	bits := *unsafe { &u32(&val) }
	return [
		u8(bits & 0xFF),
		u8((bits >> 8) & 0xFF),
		u8((bits >> 16) & 0xFF),
		u8((bits >> 24) & 0xFF),
	]
}

// encode_double encodes a f64 in little-endian IEEE 754 format
fn encode_double(val f64) []u8 {
	bits := *unsafe { &u64(&val) }
	return [
		u8(bits & 0xFF),
		u8((bits >> 8) & 0xFF),
		u8((bits >> 16) & 0xFF),
		u8((bits >> 24) & 0xFF),
		u8((bits >> 32) & 0xFF),
		u8((bits >> 40) & 0xFF),
		u8((bits >> 48) & 0xFF),
		u8((bits >> 56) & 0xFF),
	]
}

// encode_bytes encodes bytes with length prefix
fn encode_bytes(data []u8) []u8 {
	mut result := encode_varint_zigzag(i64(data.len))
	result << data
	return result
}

// encode_string encodes a string as bytes
fn encode_string(s string) []u8 {
	return encode_bytes(s.bytes())
}

// ============================================================================
// Avro Decoding Helpers
// ============================================================================

// AvroReader helps read binary data sequentially
struct AvroReader {
mut:
	data []u8
	pos  int
}

// decode_varint_zigzag decodes a zigzag-encoded varint to i64
fn decode_varint_zigzag(mut reader AvroReader) !i64 {
	val := decode_varint(mut reader)!
	// ZigZag decoding: (n >> 1) ^ -(n & 1)
	return i64((val >> 1) ^ u64(-i64(val & 1)))
}

// decode_varint_zigzag_int decodes a zigzag-encoded varint to i32
fn decode_varint_zigzag_int(mut reader AvroReader) !int {
	val := decode_varint_zigzag(mut reader)!
	return int(val)
}

// decode_varint decodes a variable-length u64
fn decode_varint(mut reader AvroReader) !u64 {
	mut result := u64(0)
	mut shift := 0

	for {
		if reader.pos >= reader.data.len {
			return error('unexpected end of varint')
		}

		b := reader.data[reader.pos]
		reader.pos += 1

		result |= u64(b & 0x7F) << shift

		if (b & 0x80) == 0 {
			break
		}

		shift += 7
		if shift >= 64 {
			return error('varint too long')
		}
	}

	return result
}

// decode_float decodes a f32 from little-endian IEEE 754
fn decode_float(mut reader AvroReader) !f32 {
	if reader.pos + 4 > reader.data.len {
		return error('unexpected end of float')
	}

	bits := u32(reader.data[reader.pos]) | (u32(reader.data[reader.pos + 1]) << 8) | (u32(reader.data[
		reader.pos + 2]) << 16) | (u32(reader.data[reader.pos + 3]) << 24)
	reader.pos += 4

	return *unsafe { &f32(&bits) }
}

// decode_double decodes a f64 from little-endian IEEE 754
fn decode_double(mut reader AvroReader) !f64 {
	if reader.pos + 8 > reader.data.len {
		return error('unexpected end of double')
	}

	bits := u64(reader.data[reader.pos]) | (u64(reader.data[reader.pos + 1]) << 8) | (u64(reader.data[
		reader.pos + 2]) << 16) | (u64(reader.data[reader.pos + 3]) << 24) | (u64(reader.data[
		reader.pos + 4]) << 32) | (u64(reader.data[reader.pos + 5]) << 40) | (u64(reader.data[
		reader.pos + 6]) << 48) | (u64(reader.data[reader.pos + 7]) << 56)
	reader.pos += 8

	return *unsafe { &f64(&bits) }
}

// decode_bytes decodes length-prefixed bytes
fn decode_bytes(mut reader AvroReader) ![]u8 {
	length := decode_varint_zigzag(mut reader)!
	if length < 0 {
		return error('negative bytes length')
	}

	len_int := int(length)
	if reader.pos + len_int > reader.data.len {
		return error('unexpected end of bytes')
	}

	result := reader.data[reader.pos..reader.pos + len_int].clone()
	reader.pos += len_int

	return result
}

// decode_string decodes a string from bytes
fn decode_string(mut reader AvroReader) !string {
	bytes := decode_bytes(mut reader)!
	return bytes.bytestr()
}

// ============================================================================
// Complex Type Encoding
// ============================================================================

// encode_record encodes a JSON object as an Avro record
fn (e &AvroEncoder) encode_record(json_str string, schema AvroSchema) ![]u8 {
	mut result := []u8{}

	// Fields are encoded in order defined in schema
	for field in schema.fields {
		field_value := extract_json_field(json_str, field.name) or {
			if field.has_default {
				// Use default value (simplified: assume null for nullable)
				if field.is_nullable {
					// Encode null in union
					result << encode_varint_zigzag(0) // null is index 0 in union
					continue
				}
			}
			return error('missing required field: ${field.name}')
		}

		// Handle union types (nullable fields)
		if field.is_union {
			if is_json_null(field_value) {
				// Find null index in union
				mut null_idx := 0
				for i, t in field.union_types {
					if t == 'null' {
						null_idx = i
						break
					}
				}
				result << encode_varint_zigzag(i64(null_idx))
			} else {
				// Find and encode non-null type
				mut type_idx := 0
				mut actual_type := ''
				for i, t in field.union_types {
					if t != 'null' {
						type_idx = i
						actual_type = t
						break
					}
				}
				result << encode_varint_zigzag(i64(type_idx))

				// Encode the actual value
				field_schema := AvroSchema{
					schema_type: actual_type
				}
				encoded := e.encode_value(field_value, field_schema)!
				result << encoded
			}
		} else {
			// Simple field encoding
			field_schema := AvroSchema{
				schema_type: field.field_type
			}
			encoded := e.encode_value(field_value, field_schema)!
			result << encoded
		}
	}

	return result
}

// encode_array encodes a JSON array
fn (e &AvroEncoder) encode_array(json_str string, schema AvroSchema) ![]u8 {
	items := parse_json_array(json_str) or { return error('invalid array') }

	if items.len == 0 {
		return [u8(0)] // Empty array: block count of 0
	}

	mut result := []u8{}

	// Write block count (as negative to indicate no block size follows)
	result << encode_varint_zigzag(i64(items.len))

	// Encode each item
	item_schema := AvroSchema{
		schema_type: schema.items_type
	}
	for item in items {
		encoded := e.encode_value(item, item_schema)!
		result << encoded
	}

	// End with zero block count
	result << u8(0)

	return result
}

// encode_map encodes a JSON object as Avro map
fn (e &AvroEncoder) encode_map(json_str string, schema AvroSchema) ![]u8 {
	entries := parse_json_map(json_str) or { return error('invalid map') }

	if entries.len == 0 {
		return [u8(0)] // Empty map: block count of 0
	}

	mut result := []u8{}

	// Write block count
	result << encode_varint_zigzag(i64(entries.len))

	// Encode each entry
	value_schema := AvroSchema{
		schema_type: schema.values_type
	}
	for key, value in entries {
		result << encode_string(key)
		encoded := e.encode_value(value, value_schema)!
		result << encoded
	}

	// End with zero block count
	result << u8(0)

	return result
}

// encode_enum encodes a string as enum index
fn (e &AvroEncoder) encode_enum(json_str string, schema AvroSchema) ![]u8 {
	symbol := parse_json_string_value(json_str) or { return error('invalid enum value') }

	for i, s in schema.symbols {
		if s == symbol {
			return encode_varint_zigzag(i64(i))
		}
	}

	return error('unknown enum symbol: ${symbol}')
}

// encode_fixed encodes fixed-length bytes
fn (e &AvroEncoder) encode_fixed(json_str string, schema AvroSchema) ![]u8 {
	bytes := parse_json_bytes(json_str) or { return error('invalid fixed value') }

	if bytes.len != schema.fixed_size {
		return error('fixed size mismatch: expected ${schema.fixed_size}, got ${bytes.len}')
	}

	return bytes
}

// encode_union encodes a union type
fn (e &AvroEncoder) encode_union(json_str string, schema AvroSchema) ![]u8 {
	if is_json_null(json_str) {
		// Find null index
		for i, t in schema.union_types {
			if t == 'null' {
				return encode_varint_zigzag(i64(i))
			}
		}
		return error('null not allowed in union')
	}

	// Try to match with non-null types
	for i, t in schema.union_types {
		if t != 'null' {
			type_schema := AvroSchema{
				schema_type: t
			}
			if encoded := e.encode_value(json_str, type_schema) {
				mut result := encode_varint_zigzag(i64(i))
				result << encoded
				return result
			}
		}
	}

	return error('value does not match any union type')
}

// ============================================================================
// Complex Type Decoding
// ============================================================================

// decode_record decodes an Avro record to JSON
fn (e &AvroEncoder) decode_record(mut reader AvroReader, schema AvroSchema) !string {
	mut fields := []string{}

	for field in schema.fields {
		mut value := ''

		if field.is_union {
			type_idx := decode_varint_zigzag(mut reader)!
			if type_idx < 0 || int(type_idx) >= field.union_types.len {
				return error('invalid union type index')
			}

			union_type := field.union_types[int(type_idx)]
			type_schema := AvroSchema{
				schema_type: union_type
			}
			value = e.decode_value(mut reader, type_schema)!
		} else {
			field_schema := AvroSchema{
				schema_type: field.field_type
			}
			value = e.decode_value(mut reader, field_schema)!
		}

		fields << '"${field.name}":${value}'
	}

	return '{${fields.join(',')}}'
}

// decode_array decodes an Avro array to JSON
fn (e &AvroEncoder) decode_array(mut reader AvroReader, schema AvroSchema) !string {
	mut items := []string{}
	item_schema := AvroSchema{
		schema_type: schema.items_type
	}

	for {
		block_count := decode_varint_zigzag(mut reader)!

		if block_count == 0 {
			break // End of array
		}

		count := if block_count < 0 {
			// Negative count means block size follows
			decode_varint_zigzag(mut reader)! // Skip block size
			-block_count
		} else {
			block_count
		}

		for _ in 0 .. int(count) {
			item := e.decode_value(mut reader, item_schema)!
			items << item
		}
	}

	return '[${items.join(',')}]'
}

// decode_map decodes an Avro map to JSON
fn (e &AvroEncoder) decode_map(mut reader AvroReader, schema AvroSchema) !string {
	mut entries := []string{}
	value_schema := AvroSchema{
		schema_type: schema.values_type
	}

	for {
		block_count := decode_varint_zigzag(mut reader)!

		if block_count == 0 {
			break // End of map
		}

		count := if block_count < 0 {
			decode_varint_zigzag(mut reader)! // Skip block size
			-block_count
		} else {
			block_count
		}

		for _ in 0 .. int(count) {
			key := decode_string(mut reader)!
			value := e.decode_value(mut reader, value_schema)!
			entries << '"${key}":${value}'
		}
	}

	return '{${entries.join(',')}}'
}

// decode_enum decodes an Avro enum to JSON string
fn (e &AvroEncoder) decode_enum(mut reader AvroReader, schema AvroSchema) !string {
	idx := decode_varint_zigzag(mut reader)!

	if idx < 0 || int(idx) >= schema.symbols.len {
		return error('invalid enum index: ${idx}')
	}

	return '"${schema.symbols[int(idx)]}"'
}

// decode_fixed decodes fixed-length bytes
fn (e &AvroEncoder) decode_fixed(mut reader AvroReader, schema AvroSchema) !string {
	if reader.pos + schema.fixed_size > reader.data.len {
		return error('unexpected end of fixed data')
	}

	bytes := reader.data[reader.pos..reader.pos + schema.fixed_size].clone()
	reader.pos += schema.fixed_size

	return format_json_bytes(bytes)
}

// decode_union decodes a union type
fn (e &AvroEncoder) decode_union(mut reader AvroReader, schema AvroSchema) !string {
	type_idx := decode_varint_zigzag(mut reader)!

	if type_idx < 0 || int(type_idx) >= schema.union_types.len {
		return error('invalid union type index')
	}

	union_type := schema.union_types[int(type_idx)]
	type_schema := AvroSchema{
		schema_type: union_type
	}

	return e.decode_value(mut reader, type_schema)
}

// ============================================================================
// JSON Parsing Helpers
// ============================================================================

fn parse_json_bool(s string) ?bool {
	trimmed := s.trim_space()
	if trimmed == 'true' {
		return true
	} else if trimmed == 'false' {
		return false
	}
	return none
}

fn parse_json_int(s string) ?int {
	trimmed := s.trim_space()
	return trimmed.int()
}

fn parse_json_long(s string) ?i64 {
	trimmed := s.trim_space()
	return trimmed.i64()
}

fn parse_json_float(s string) ?f32 {
	trimmed := s.trim_space()
	return f32(trimmed.f64())
}

fn parse_json_double(s string) ?f64 {
	trimmed := s.trim_space()
	return trimmed.f64()
}

fn parse_json_string_value(s string) ?string {
	trimmed := s.trim_space()
	if trimmed.starts_with('"') && trimmed.ends_with('"') {
		return trimmed[1..trimmed.len - 1]
	}
	return none
}

fn parse_json_bytes(s string) ?[]u8 {
	// Bytes can be encoded as:
	// 1. Base64 string: "\u0001\u0002..."
	// 2. JSON string with escape sequences
	str := parse_json_string_value(s)?
	return unescape_json_bytes(str)
}

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

fn is_json_null(s string) bool {
	return s.trim_space() == 'null'
}

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
		key := key_val[1..key_val.len - 1] // Remove quotes
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

fn escape_json_str(s string) string {
	return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r',
		'\\r').replace('\t', '\\t')
}

fn format_json_bytes(data []u8) string {
	// Format bytes as JSON string with unicode escapes
	mut result := '"'
	for b in data {
		if b < 32 || b > 126 {
			// Unicode escape for non-printable
			hex := b.hex()
			if hex.len == 1 {
				result += '\\u000${hex}'
			} else {
				result += '\\u00${hex}'
			}
		} else if b == `"` {
			result += '\\"'
		} else if b == `\\` {
			result += '\\\\'
		} else {
			result += rune(b).str()
		}
	}
	result += '"'
	return result
}
