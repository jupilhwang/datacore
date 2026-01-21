// Service Layer - Avro Complex Type Encoding/Decoding
// Handles record, array, map, enum, fixed, and union types
module schema

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
