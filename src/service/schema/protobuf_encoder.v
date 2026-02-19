// Service Layer - Protobuf Encoder
// Provides binary encoding/decoding for Protocol Buffers
// https://protobuf.dev/programming-guides/encoding/
module schema

// Protobuf Binary Encoder
// Implements Protocol Buffers wire format encoding
pub struct ProtobufEncoder {}

// new_protobuf_encoder creates a new Protobuf encoder
pub fn new_protobuf_encoder() !ProtobufEncoder {
	return ProtobufEncoder{}
}

// format returns the encoding format
pub fn (mut e ProtobufEncoder) format() Format {
	return Format.protobuf
}

// encode encodes JSON data to Protobuf binary format
pub fn (mut e ProtobufEncoder) encode(data []u8, schema_str string) ![]u8 {
	schema := parse_protobuf_schema(schema_str) or {
		return error('failed to parse schema: ${err}')
	}
	return encode_message(data.bytestr(), schema)
}

// decode decodes Protobuf binary data to JSON
pub fn (mut e ProtobufEncoder) decode(data []u8, schema_str string) ![]u8 {
	schema := parse_protobuf_schema(schema_str) or {
		return error('failed to parse schema: ${err}')
	}
	mut reader := ProtoReader{
		data: data
		pos:  0
	}
	result := decode_message(mut reader, schema)!
	return result.bytes()
}

// Wire types
const wire_varint = 0

const wire_64bit = 1

const wire_length_delimited = 2

const wire_32bit = 5

// encode_message encodes a JSON object as a Protobuf message
fn encode_message(json_str string, schema ProtoMessage) ![]u8 {
	mut result := []u8{}

	props := parse_json_map(json_str) or { return error('invalid JSON object') }

	for field in schema.fields {
		value := props[field.name] or {
			// Field not present - skip (proto3 default behavior)
			continue
		}

		if is_json_null(value) {
			continue
		}

		// Encode field tag (field_number << 3 | wire_type)
		wire_type := get_wire_type(field.field_type)
		tag := (u32(field.number) << 3) | u32(wire_type)
		result << proto_encode_varint(u64(tag))

		// Encode field value based on type
		encoded := encode_field(value, field)!
		result << encoded
	}

	return result
}

// encode_field encodes a single field value
fn encode_field(value string, field ProtoField) ![]u8 {
	match field.field_type {
		'int32', 'int64', 'uint32', 'uint64' {
			val := parse_json_long(value) or { return error('invalid integer') }
			return proto_encode_varint(u64(val))
		}
		'sint32' {
			val := parse_json_int(value) or { return error('invalid sint32') }
			return proto_encode_zigzag32(val)
		}
		'sint64' {
			val := parse_json_long(value) or { return error('invalid sint64') }
			return proto_encode_zigzag64(val)
		}
		'bool' {
			val := parse_json_bool(value) or { return error('invalid bool') }
			return if val { [u8(1)] } else { [u8(0)] }
		}
		'fixed32', 'sfixed32' {
			val := parse_json_int(value) or { return error('invalid fixed32') }
			return proto_encode_fixed32(u32(val))
		}
		'fixed64', 'sfixed64' {
			val := parse_json_long(value) or { return error('invalid fixed64') }
			return proto_encode_fixed64(u64(val))
		}
		'float' {
			val := parse_json_float(value) or { return error('invalid float') }
			bits := *unsafe { &u32(&val) }
			return proto_encode_fixed32(bits)
		}
		'double' {
			val := parse_json_double(value) or { return error('invalid double') }
			bits := *unsafe { &u64(&val) }
			return proto_encode_fixed64(bits)
		}
		'string' {
			str := parse_json_string_value(value) or { return error('invalid string') }
			bytes := str.bytes()
			mut result := proto_encode_varint(u64(bytes.len))
			result << bytes
			return result
		}
		'bytes' {
			bytes := parse_json_bytes(value) or { return error('invalid bytes') }
			mut result := proto_encode_varint(u64(bytes.len))
			result << bytes
			return result
		}
		else {
			// Enum or embedded message
			if field.is_enum {
				// Enum - look up index
				str := parse_json_string_value(value) or {
					// Try as integer
					val := parse_json_int(value) or { return error('invalid enum') }
					return proto_encode_varint(u64(val))
				}

				for i, name in field.enum_values {
					if name == str {
						return proto_encode_varint(u64(i))
					}
				}
				return error('unknown enum value: ${str}')
			}

			// Unknown type - treat as bytes
			return error('unsupported protobuf type: ${field.field_type}')
		}
	}
}

// decode_message decodes a Protobuf message to JSON
fn decode_message(mut reader ProtoReader, schema ProtoMessage) !string {
	mut fields := map[string]string{}

	// Create field lookup by number
	mut field_by_number := map[int]ProtoField{}
	for f in schema.fields {
		field_by_number[f.number] = f
	}

	for reader.pos < reader.data.len {
		// Read tag
		tag := proto_decode_varint(mut reader) or { break }
		field_number := int(tag >> 3)
		wire_type := int(tag & 0x07)

		field := field_by_number[field_number] or {
			// Unknown field - skip based on wire type
			skip_field(mut reader, wire_type)!
			continue
		}

		value := decode_field(mut reader, field, wire_type)!

		if field.is_repeated {
			// Append to existing array
			if existing := fields[field.name] {
				// Remove closing bracket and append
				if existing.ends_with(']') {
					fields[field.name] = '${existing[..existing.len - 1]},${value}]'
				}
			} else {
				fields[field.name] = '[${value}]'
			}
		} else {
			fields[field.name] = value
		}
	}

	// Build JSON object
	mut parts := []string{}
	for name, value in fields {
		parts << '"${name}":${value}'
	}

	return '{${parts.join(',')}}'
}

// decode_field decodes a single field
fn decode_field(mut reader ProtoReader, field ProtoField, wire_type int) !string {
	match field.field_type {
		'int32', 'int64', 'uint32', 'uint64' {
			val := proto_decode_varint(mut reader)!
			return '${val}'
		}
		'sint32' {
			val := proto_decode_zigzag32(mut reader)!
			return '${val}'
		}
		'sint64' {
			val := proto_decode_zigzag64(mut reader)!
			return '${val}'
		}
		'bool' {
			val := proto_decode_varint(mut reader)!
			return if val != 0 { 'true' } else { 'false' }
		}
		'fixed32', 'sfixed32' {
			val := proto_decode_fixed32(mut reader)!
			if field.field_type == 'sfixed32' {
				return '${int(val)}'
			}
			return '${val}'
		}
		'fixed64', 'sfixed64' {
			val := proto_decode_fixed64(mut reader)!
			if field.field_type == 'sfixed64' {
				return '${i64(val)}'
			}
			return '${val}'
		}
		'float' {
			bits := proto_decode_fixed32(mut reader)!
			val := *unsafe { &f32(&bits) }
			return '${val}'
		}
		'double' {
			bits := proto_decode_fixed64(mut reader)!
			val := *unsafe { &f64(&bits) }
			return '${val}'
		}
		'string' {
			len := proto_decode_varint(mut reader)!
			if reader.pos + int(len) > reader.data.len {
				return error('unexpected end of string')
			}
			str := reader.data[reader.pos..reader.pos + int(len)].bytestr()
			reader.pos += int(len)
			return '"${escape_json_str(str)}"'
		}
		'bytes' {
			len := proto_decode_varint(mut reader)!
			if reader.pos + int(len) > reader.data.len {
				return error('unexpected end of bytes')
			}
			bytes := reader.data[reader.pos..reader.pos + int(len)]
			reader.pos += int(len)
			return format_json_bytes(bytes)
		}
		else {
			if field.is_enum {
				val := proto_decode_varint(mut reader)!
				idx := int(val)
				if idx >= 0 && idx < field.enum_values.len {
					return '"${field.enum_values[idx]}"'
				}
				return '${val}'
			}

			// Unknown - skip based on wire type
			skip_field(mut reader, wire_type)!
			return 'null'
		}
	}
}

// skip_field skips a field based on wire type
fn skip_field(mut reader ProtoReader, wire_type int) ! {
	match wire_type {
		wire_varint {
			proto_decode_varint(mut reader)!
		}
		wire_64bit {
			if reader.pos + 8 > reader.data.len {
				return error('unexpected end')
			}
			reader.pos += 8
		}
		wire_length_delimited {
			len := proto_decode_varint(mut reader)!
			if reader.pos + int(len) > reader.data.len {
				return error('unexpected end')
			}
			reader.pos += int(len)
		}
		wire_32bit {
			if reader.pos + 4 > reader.data.len {
				return error('unexpected end')
			}
			reader.pos += 4
		}
		else {
			return error('unknown wire type: ${wire_type}')
		}
	}
}

// Protobuf Encoding Helpers

fn proto_encode_varint(val u64) []u8 {
	mut result := []u8{}
	mut n := val

	for {
		mut b := u8(n & 0x7F)
		n = n >> 7
		if n != 0 {
			b |= 0x80
		}
		result << b
		if n == 0 {
			break
		}
	}

	return result
}

fn proto_encode_zigzag32(val int) []u8 {
	// Cast to u32 first to avoid signed shift warning
	zigzag := u32(val) << 1 ^ u32(val >> 31)
	return proto_encode_varint(u64(zigzag))
}

fn proto_encode_zigzag64(val i64) []u8 {
	// Cast to u64 first to avoid signed shift warning
	zigzag := u64(val) << 1 ^ u64(val >> 63)
	return proto_encode_varint(zigzag)
}

fn proto_encode_fixed32(val u32) []u8 {
	return [
		u8(val & 0xFF),
		u8((val >> 8) & 0xFF),
		u8((val >> 16) & 0xFF),
		u8((val >> 24) & 0xFF),
	]
}

fn proto_encode_fixed64(val u64) []u8 {
	return [
		u8(val & 0xFF),
		u8((val >> 8) & 0xFF),
		u8((val >> 16) & 0xFF),
		u8((val >> 24) & 0xFF),
		u8((val >> 32) & 0xFF),
		u8((val >> 40) & 0xFF),
		u8((val >> 48) & 0xFF),
		u8((val >> 56) & 0xFF),
	]
}

// Protobuf Decoding Helpers

struct ProtoReader {
mut:
	data []u8
	pos  int
}

fn proto_decode_varint(mut reader ProtoReader) !u64 {
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

fn proto_decode_zigzag32(mut reader ProtoReader) !int {
	val := proto_decode_varint(mut reader)!
	zigzag := u32(val)
	return int((zigzag >> 1) ^ u32(-int(zigzag & 1)))
}

fn proto_decode_zigzag64(mut reader ProtoReader) !i64 {
	val := proto_decode_varint(mut reader)!
	return i64((val >> 1) ^ u64(-i64(val & 1)))
}

fn proto_decode_fixed32(mut reader ProtoReader) !u32 {
	if reader.pos + 4 > reader.data.len {
		return error('unexpected end of fixed32')
	}

	result := u32(reader.data[reader.pos]) | (u32(reader.data[reader.pos + 1]) << 8) | (u32(reader.data[
		reader.pos + 2]) << 16) | (u32(reader.data[reader.pos + 3]) << 24)
	reader.pos += 4

	return result
}

fn proto_decode_fixed64(mut reader ProtoReader) !u64 {
	if reader.pos + 8 > reader.data.len {
		return error('unexpected end of fixed64')
	}

	result := u64(reader.data[reader.pos]) | (u64(reader.data[reader.pos + 1]) << 8) | (u64(reader.data[
		reader.pos + 2]) << 16) | (u64(reader.data[reader.pos + 3]) << 24) | (u64(reader.data[
		reader.pos + 4]) << 32) | (u64(reader.data[reader.pos + 5]) << 40) | (u64(reader.data[
		reader.pos + 6]) << 48) | (u64(reader.data[reader.pos + 7]) << 56)
	reader.pos += 8

	return result
}

fn get_wire_type(field_type string) int {
	match field_type {
		'int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64', 'bool', 'enum' {
			return wire_varint
		}
		'fixed64', 'sfixed64', 'double' {
			return wire_64bit
		}
		'fixed32', 'sfixed32', 'float' {
			return wire_32bit
		}
		else {
			return wire_length_delimited
		}
	}
}

// Protobuf Schema Parsing

struct ProtoMessage {
mut:
	name   string
	fields []ProtoField
}

struct ProtoField {
mut:
	name        string
	field_type  string
	number      int
	is_repeated bool
	is_optional bool
	is_enum     bool
	enum_values []string
}

fn parse_protobuf_schema(schema_str string) !ProtoMessage {
	mut result := ProtoMessage{}

	// Find message name
	if msg_idx := schema_str.index('message') {
		// Skip "message " and read name
		mut pos := msg_idx + 7
		for pos < schema_str.len && (schema_str[pos] == ` ` || schema_str[pos] == `\t`) {
			pos += 1
		}
		mut end := pos
		for end < schema_str.len && schema_str[end] != ` ` && schema_str[end] != `{`
			&& schema_str[end] != `\n` {
			end += 1
		}
		result.name = schema_str[pos..end].trim_space()
	}

	// Extract content between { and }
	mut content := schema_str
	if brace_start := schema_str.index('{') {
		if brace_end := schema_str.last_index('}') {
			content = schema_str[brace_start + 1..brace_end]
		}
	}

	// Parse fields - handle both single-line and multi-line formats
	// Replace semicolons followed by space with semicolon+newline for uniform parsing
	normalized := content.replace('; ', ';\n').replace(';', ';\n')
	lines := normalized.split('\n')

	for line in lines {
		trimmed := line.trim_space()

		// Skip empty lines, comments, message/enum declarations
		if trimmed.len == 0 || trimmed.starts_with('//') || trimmed.starts_with('message')
			|| trimmed.starts_with('enum') || trimmed == '{' || trimmed == '}'
			|| trimmed.starts_with('syntax') || trimmed.starts_with('package')
			|| trimmed.starts_with('option') {
			continue
		}

		// Parse field: [repeated|optional] type name = number;
		if trimmed.contains('=') {
			// Remove trailing semicolon if present
			clean := trimmed.trim_right(';').trim_space()
			if clean.len > 0 {
				if field := parse_proto_field(clean + ';') {
					result.fields << field
				}
			}
		}
	}

	return result
}

fn parse_proto_field(line string) ?ProtoField {
	mut field := ProtoField{}

	// Remove trailing semicolon
	trimmed := line.trim_right(';').trim_space()

	// Split by '='
	parts := trimmed.split('=')
	if parts.len != 2 {
		return none
	}

	// Parse field number
	field.number = parts[1].trim_space().int()

	// Parse type and name
	type_name := parts[0].trim_space()
	tokens := type_name.split(' ').filter(fn (s string) bool {
		return s.len > 0
	})

	if tokens.len < 2 {
		return none
	}

	// Check for modifiers
	mut type_idx := 0
	if tokens[0] == 'repeated' {
		field.is_repeated = true
		type_idx = 1
	} else if tokens[0] == 'optional' {
		field.is_optional = true
		type_idx = 1
	}

	if type_idx + 1 >= tokens.len {
		return none
	}

	field.field_type = tokens[type_idx]
	field.name = tokens[type_idx + 1]

	return field
}
