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

// ============================================================================
// AvroReader - Binary Data Reader
// ============================================================================

// AvroReader helps read binary data sequentially
struct AvroReader {
mut:
	data []u8
	pos  int
}

// ============================================================================
// Value Encoding/Decoding - Main Dispatch
// ============================================================================

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
