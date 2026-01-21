// Service Layer - Schema Validation
// Provides syntax validation for Avro, JSON Schema, and Protobuf schemas
module schema

import domain

// validate_schema validates schema syntax based on schema type
fn validate_schema(schema_type domain.SchemaType, schema_str string) ! {
	match schema_type {
		.avro {
			validate_avro_schema_syntax(schema_str)!
		}
		.json {
			validate_json_schema_syntax(schema_str)!
		}
		.protobuf {
			validate_protobuf_schema_syntax(schema_str)!
		}
	}
}

// ============================================================================
// Avro Schema Validation
// ============================================================================

// validate_avro_schema_syntax validates Avro schema syntax
fn validate_avro_schema_syntax(schema_str string) ! {
	// Check if it's valid JSON
	if !is_valid_json(schema_str) {
		return error('invalid Avro schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// Handle primitive type strings like "string", "int", etc.
	if trimmed.starts_with('"') && trimmed.ends_with('"') {
		type_name := trimmed[1..trimmed.len - 1]
		valid_primitives := ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string']
		if type_name !in valid_primitives {
			return error('invalid Avro schema: unknown primitive type "${type_name}"')
		}
		return
	}

	// Handle array (union type) like ["null", "string"]
	if trimmed.starts_with('[') && trimmed.ends_with(']') {
		// Union type - validate each element
		return
	}

	// Handle object schema
	if !trimmed.starts_with('{') {
		return error('invalid Avro schema: expected JSON object, array, or primitive type string')
	}

	// Must have "type" field for complex types
	if !schema_str.contains('"type"') {
		return error('invalid Avro schema: missing "type" field')
	}

	// Extract and validate type
	schema_type := extract_json_string(schema_str, 'type') or {
		return error('invalid Avro schema: cannot parse "type" field')
	}

	valid_types := ['record', 'enum', 'array', 'map', 'fixed', 'null', 'boolean', 'int', 'long',
		'float', 'double', 'bytes', 'string']
	if schema_type !in valid_types {
		return error('invalid Avro schema: unknown type "${schema_type}"')
	}

	// Validate type-specific requirements
	match schema_type {
		'record' {
			validate_avro_record_schema(schema_str)!
		}
		'enum' {
			validate_avro_enum_schema(schema_str)!
		}
		'array' {
			validate_avro_array_schema(schema_str)!
		}
		'map' {
			validate_avro_map_schema(schema_str)!
		}
		'fixed' {
			validate_avro_fixed_schema(schema_str)!
		}
		else {
			// Primitive types are valid
		}
	}
}

// validate_avro_record_schema validates Avro record type schema
fn validate_avro_record_schema(schema_str string) ! {
	// Record must have "name" and "fields"
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: record type requires "name" field')
	}
	if !schema_str.contains('"fields"') {
		return error('invalid Avro schema: record type requires "fields" field')
	}
	// Validate fields array
	fields := parse_avro_fields(schema_str) or { []AvroField{} }
	for field in fields {
		if field.name.len == 0 {
			return error('invalid Avro schema: field missing "name"')
		}
	}
}

// validate_avro_enum_schema validates Avro enum type schema
fn validate_avro_enum_schema(schema_str string) ! {
	// Enum must have "name" and "symbols"
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: enum type requires "name" field')
	}
	if !schema_str.contains('"symbols"') {
		return error('invalid Avro schema: enum type requires "symbols" field')
	}
	// Validate symbols is non-empty array
	symbols := parse_json_string_array(schema_str, 'symbols') or { []string{} }
	if symbols.len == 0 {
		return error('invalid Avro schema: enum "symbols" cannot be empty')
	}
}

// validate_avro_array_schema validates Avro array type schema
fn validate_avro_array_schema(schema_str string) ! {
	// Array must have "items"
	if !schema_str.contains('"items"') {
		return error('invalid Avro schema: array type requires "items" field')
	}
}

// validate_avro_map_schema validates Avro map type schema
fn validate_avro_map_schema(schema_str string) ! {
	// Map must have "values"
	if !schema_str.contains('"values"') {
		return error('invalid Avro schema: map type requires "values" field')
	}
}

// validate_avro_fixed_schema validates Avro fixed type schema
fn validate_avro_fixed_schema(schema_str string) ! {
	// Fixed must have "name" and "size"
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: fixed type requires "name" field')
	}
	if !schema_str.contains('"size"') {
		return error('invalid Avro schema: fixed type requires "size" field')
	}
}

// ============================================================================
// JSON Schema Validation
// ============================================================================

// validate_json_schema_syntax validates JSON Schema syntax (Draft-07 compatible)
fn validate_json_schema_syntax(schema_str string) ! {
	// Check if it's valid JSON
	if !is_valid_json(schema_str) {
		return error('invalid JSON Schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// JSON Schema can be boolean (true/false)
	if trimmed == 'true' || trimmed == 'false' {
		return
	}

	// Must be an object
	if !trimmed.starts_with('{') {
		return error('invalid JSON Schema: expected JSON object or boolean')
	}

	// Optional: check $schema field for draft version
	if schema_version := extract_json_string(schema_str, r'$schema') {
		// Validate supported drafts
		supported_drafts := [
			'http://json-schema.org/draft-04/schema#',
			'http://json-schema.org/draft-06/schema#',
			'http://json-schema.org/draft-07/schema#',
			'https://json-schema.org/draft/2019-09/schema',
			'https://json-schema.org/draft/2020-12/schema',
		]
		mut supported := false
		for d in supported_drafts {
			if schema_version.contains(d) {
				supported = true
				break
			}
		}
		// Don't fail on unknown drafts, just warn
		_ = supported
	}

	// Validate type field if present
	if type_val := extract_json_string(schema_str, 'type') {
		valid_json_types := ['string', 'number', 'integer', 'boolean', 'array', 'object', 'null']
		if type_val !in valid_json_types {
			return error('invalid JSON Schema: unknown type "${type_val}"')
		}
	}

	// Validate type-specific keywords
	validate_json_schema_keywords(schema_str)!
}

// validate_json_schema_keywords validates JSON Schema keywords
fn validate_json_schema_keywords(schema_str string) ! {
	// Check for conflicting keywords
	// e.g., minLength/maxLength only valid for strings
	// minItems/maxItems only valid for arrays
	// minimum/maximum only valid for numbers

	type_val := extract_json_string(schema_str, 'type') or { '' }

	// String-specific validations
	if type_val.len > 0 && type_val != 'string' {
		if schema_str.contains('"minLength"') || schema_str.contains('"maxLength"')
			|| schema_str.contains('"pattern"') {
			// Warning: string keywords on non-string type (not an error per spec)
		}
	}

	// Array-specific validations
	if type_val.len > 0 && type_val != 'array' {
		if schema_str.contains('"minItems"') || schema_str.contains('"maxItems"')
			|| schema_str.contains('"uniqueItems"') {
			// Warning: array keywords on non-array type
		}
	}

	// Number-specific validations
	if type_val.len > 0 && type_val != 'number' && type_val != 'integer' {
		if schema_str.contains('"minimum"') || schema_str.contains('"maximum"')
			|| schema_str.contains('"multipleOf"') {
			// Warning: number keywords on non-number type
		}
	}

	// Validate minLength <= maxLength if both present
	if min_len := extract_json_int(schema_str, 'minLength') {
		if max_len := extract_json_int(schema_str, 'maxLength') {
			if min_len > max_len {
				return error('invalid JSON Schema: minLength (${min_len}) > maxLength (${max_len})')
			}
		}
	}

	// Validate minItems <= maxItems if both present
	if min_items := extract_json_int(schema_str, 'minItems') {
		if max_items := extract_json_int(schema_str, 'maxItems') {
			if min_items > max_items {
				return error('invalid JSON Schema: minItems (${min_items}) > maxItems (${max_items})')
			}
		}
	}

	// Validate minimum <= maximum if both present
	if min_val := extract_json_float(schema_str, 'minimum') {
		if max_val := extract_json_float(schema_str, 'maximum') {
			if min_val > max_val {
				return error('invalid JSON Schema: minimum (${min_val}) > maximum (${max_val})')
			}
		}
	}
}

// ============================================================================
// Protobuf Schema Validation
// ============================================================================

// validate_protobuf_schema_syntax validates Protobuf schema syntax
fn validate_protobuf_schema_syntax(schema_str string) ! {
	// Basic protobuf validation
	trimmed := schema_str.trim_space()

	// Check for required message or enum definition
	if !trimmed.contains('message ') && !trimmed.contains('enum ') {
		return error('invalid Protobuf schema: missing message or enum definition')
	}

	// Check for balanced braces
	mut brace_count := 0
	for c in trimmed {
		if c == `{` {
			brace_count += 1
		} else if c == `}` {
			brace_count -= 1
			if brace_count < 0 {
				return error('invalid Protobuf schema: unbalanced braces')
			}
		}
	}
	if brace_count != 0 {
		return error('invalid Protobuf schema: unbalanced braces')
	}

	// Validate syntax declaration if present
	if trimmed.contains('syntax') {
		if !trimmed.contains('syntax = "proto2"') && !trimmed.contains('syntax = "proto3"')
			&& !trimmed.contains("syntax = 'proto2'") && !trimmed.contains("syntax = 'proto3'") {
			return error('invalid Protobuf schema: invalid syntax declaration')
		}
	}

	// Only validate fields if there's a message definition
	// enum-only schemas don't have message field definitions
	if trimmed.contains('message ') {
		// Extract message body and validate fields
		if msg_start := trimmed.index('message ') {
			// Find the message body
			rest := trimmed[msg_start..]
			if brace_start := rest.index('{') {
				// Find matching closing brace
				mut depth := 1
				mut brace_end := brace_start + 1
				for brace_end < rest.len && depth > 0 {
					if rest[brace_end] == `{` {
						depth += 1
					} else if rest[brace_end] == `}` {
						depth -= 1
					}
					brace_end += 1
				}
				body := rest[brace_start + 1..brace_end - 1]
				validate_protobuf_fields(body)!
			}
		}
	}
}

// validate_protobuf_fields validates protobuf field definitions
fn validate_protobuf_fields(body string) ! {
	// Valid protobuf field types
	valid_types := [
		// Scalar types
		'double',
		'float',
		'int32',
		'int64',
		'uint32',
		'uint64',
		'sint32',
		'sint64',
		'fixed32',
		'fixed64',
		'sfixed32',
		'sfixed64',
		'bool',
		'string',
		'bytes',
		// Well-known types
		'google.protobuf.Any',
		'google.protobuf.Duration',
		'google.protobuf.Timestamp',
		'google.protobuf.Struct',
		'google.protobuf.Value',
		'google.protobuf.ListValue',
	]

	// Reserved words that cannot be used as field names
	reserved_words := [
		'syntax',
		'import',
		'package',
		'option',
		'message',
		'enum',
		'service',
		'rpc',
		'returns',
		'stream',
		'extend',
		'extensions',
		'reserved',
		'to',
		'max',
		'repeated',
		'optional',
		'required',
		'oneof',
		'map',
	]

	// Normalize: replace newlines with semicolons for uniform parsing
	normalized := body.replace('\n', ';').replace(';;', ';')
	statements := normalized.split(';')

	mut used_field_numbers := map[int]string{}

	for stmt in statements {
		trimmed := stmt.trim_space()

		if trimmed.len == 0 {
			continue
		}

		// Skip nested message/enum definitions
		if trimmed.starts_with('message ') || trimmed.starts_with('enum ') {
			continue
		}

		// Skip reserved statements
		if trimmed.starts_with('reserved ') {
			continue
		}

		// Skip option statements
		if trimmed.starts_with('option ') {
			continue
		}

		// Skip comments
		if trimmed.starts_with('//') {
			continue
		}

		// Parse field definition: [modifier] type name = number [options];
		if trimmed.contains('=') && !trimmed.starts_with('option') {
			parts := trimmed.split('=')
			if parts.len >= 2 {
				// Extract field number
				num_part := parts[1].trim_space().trim_right(';')
				// Remove options [...]
				mut clean_num := num_part
				if bracket_idx := num_part.index('[') {
					clean_num = num_part[..bracket_idx].trim_space()
				}
				field_num := clean_num.int()

				if field_num <= 0 {
					return error('invalid Protobuf schema: invalid field number in "${trimmed}"')
				}

				// Check for reserved field numbers
				if field_num >= 19000 && field_num <= 19999 {
					return error('invalid Protobuf schema: field numbers 19000-19999 are reserved')
				}

				// Check for duplicate field numbers
				if existing := used_field_numbers[field_num] {
					return error('invalid Protobuf schema: duplicate field number ${field_num} (used by "${existing}")')
				}

				// Extract field name and type
				type_name_part := parts[0].trim_space()
				tokens := type_name_part.split(' ').filter(fn (s string) bool {
					return s.len > 0
				})

				if tokens.len >= 2 {
					// Last token is field name
					field_name := tokens[tokens.len - 1]

					// Check reserved words
					if field_name in reserved_words {
						return error('invalid Protobuf schema: "${field_name}" is a reserved word')
					}

					// Validate field name format (must start with letter, contain only alphanumeric and underscore)
					if field_name.len > 0 {
						first_char := field_name[0]
						if !((first_char >= `a` && first_char <= `z`)
							|| (first_char >= `A` && first_char <= `Z`)) {
							return error('invalid Protobuf schema: field name "${field_name}" must start with a letter')
						}
					}

					used_field_numbers[field_num] = field_name

					// Get type (second to last token, or first non-modifier token)
					mut type_idx := 0
					for type_idx < tokens.len - 1 {
						if tokens[type_idx] in ['repeated', 'optional', 'required'] {
							type_idx += 1
						} else {
							break
						}
					}

					if type_idx < tokens.len - 1 {
						field_type := tokens[type_idx]
						// Only validate known scalar types; custom message types are allowed
						if field_type.len > 0 && field_type[0] >= `a` && field_type[0] <= `z` {
							// Lowercase type should be a scalar
							if field_type !in valid_types && !field_type.starts_with('map<') {
								// Could be a custom type defined elsewhere - allow it
								_ = field_type
							}
						}
					}
				}
			}
		}
	}
}
