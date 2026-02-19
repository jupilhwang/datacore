// Service Layer - Protobuf 스키마 호환성 검사
// Protobuf 스키마의 호환성 검사를 제공합니다
module schema

// Protobuf Schema Types

// ProtoFieldInfo represents parsed protobuf field information
struct ProtoFieldInfo {
mut:
	name        string
	field_type  string
	number      int
	is_repeated bool
	is_optional bool
	is_required bool
}

// ProtoSchemaInfo represents parsed protobuf schema information
struct ProtoSchemaInfo {
mut:
	syntax          string
	message_name    string
	fields          map[int]ProtoFieldInfo
	reserved_nums   []int
	reserved_names  []string
	nested_messages []string
	enums           []string
}

// Protobuf Schema Compatibility

fn check_protobuf_backward_compatible(old_schema string, new_schema string) bool {
	// Protobuf backward compatibility rules:
	// - Can add new fields (must use new field numbers)
	// - Can remove optional fields (proto3 all fields are optional by default)
	// - Cannot change field numbers for existing fields
	// - Cannot change field types (except compatible promotions)
	// - Reserved field numbers/names should not be reused

	old_info := parse_protobuf_schema_info(old_schema)
	new_info := parse_protobuf_schema_info(new_schema)

	// Check that no old field numbers are reused with different types
	for num, old_field in old_info.fields {
		if new_field := new_info.fields[num] {
			// Field number still exists - check type compatibility
			if !is_protobuf_type_backward_compatible(old_field.field_type, new_field.field_type) {
				return false
			}

			// Repeated modifier must match
			if old_field.is_repeated != new_field.is_repeated {
				return false
			}

			// proto2: required -> optional is OK, optional -> required is NOT OK
			if old_info.syntax == 'proto2' && !old_field.is_required && new_field.is_required {
				return false
			}
		}
		// Field removed - OK for backward compatibility (new reader ignores unknown)
	}

	// Check that new fields don't reuse reserved numbers
	for num, _ in new_info.fields {
		if num in old_info.reserved_nums {
			return false
		}
	}

	// Check that new field names don't reuse reserved names
	for _, new_field in new_info.fields {
		if new_field.name in old_info.reserved_names {
			return false
		}
	}

	return true
}

fn check_protobuf_forward_compatible(old_schema string, new_schema string) bool {
	// Protobuf forward compatibility rules:
	// - Old readers can read new data (ignore unknown fields)
	// - Can add optional fields
	// - Cannot remove required fields (proto2)
	// - Cannot change field types incompatibly

	old_info := parse_protobuf_schema_info(old_schema)
	new_info := parse_protobuf_schema_info(new_schema)

	// proto2: Required fields cannot be removed
	if old_info.syntax == 'proto2' {
		for num, old_field in old_info.fields {
			if old_field.is_required {
				if _ := new_info.fields[num] {
					// Field still exists - OK
				} else {
					// Required field removed - NOT forward compatible
					return false
				}
			}
		}
	}

	// Check type compatibility for shared fields
	for num, new_field in new_info.fields {
		if old_field := old_info.fields[num] {
			if !is_protobuf_type_forward_compatible(old_field.field_type, new_field.field_type) {
				return false
			}

			// Repeated modifier must match
			if old_field.is_repeated != new_field.is_repeated {
				return false
			}
		}
		// New field - OK, old reader will ignore
	}

	// Check that removed field numbers are properly reserved
	for num, _ in old_info.fields {
		if _ := new_info.fields[num] {
			// Field still exists
		} else {
			// Field removed - should be reserved (warning, not error)
			// For strict compatibility, uncomment:
			// if num !in new_info.reserved_nums {
			//     return false
			// }
		}
	}

	return true
}

fn parse_protobuf_schema_info(schema_str string) ProtoSchemaInfo {
	mut info := ProtoSchemaInfo{
		syntax: 'proto3' // default
		fields: map[int]ProtoFieldInfo{}
	}

	// Extract syntax
	if schema_str.contains('syntax = "proto2"') || schema_str.contains("syntax = 'proto2'") {
		info.syntax = 'proto2'
	}

	// Extract message name
	if msg_idx := schema_str.index('message ') {
		mut pos := msg_idx + 8
		for pos < schema_str.len && (schema_str[pos] == ` ` || schema_str[pos] == `\t`) {
			pos += 1
		}
		mut end := pos
		for end < schema_str.len && schema_str[end] != ` ` && schema_str[end] != `{`
			&& schema_str[end] != `\n` {
			end += 1
		}
		info.message_name = schema_str[pos..end].trim_space()
	}

	// Extract message body content
	mut body := schema_str
	if brace_start := schema_str.index('{') {
		if brace_end := schema_str.last_index('}') {
			body = schema_str[brace_start + 1..brace_end]
		}
	}

	// Normalize: replace newlines with semicolons for uniform parsing
	// Then split by semicolons to get individual statements
	normalized := body.replace('\n', ';').replace(';;', ';')
	statements := normalized.split(';')

	for stmt in statements {
		trimmed := stmt.trim_space()

		if trimmed.len == 0 {
			continue
		}

		// Parse reserved statement
		if trimmed.starts_with('reserved ') {
			reserved_part := trimmed[9..].trim_space()

			// Check if it's numbers or names
			if reserved_part.contains('"') {
				// Reserved names
				parts := reserved_part.split(',')
				for part in parts {
					p := part.trim_space().trim('"')
					if p.len > 0 {
						info.reserved_names << p
					}
				}
			} else {
				// Reserved numbers
				parts := reserved_part.split(',')
				for part in parts {
					p := part.trim_space()
					if p.contains(' to ') {
						range_parts := p.split(' to ')
						if range_parts.len == 2 {
							start := range_parts[0].trim_space().int()
							mut end_num := 0
							if range_parts[1].trim_space() == 'max' {
								end_num = 536870911
							} else {
								end_num = range_parts[1].trim_space().int()
							}
							for n := start; n <= end_num && n < start + 1000; n++ {
								info.reserved_nums << n
							}
						}
					} else if p.len > 0 && p[0] >= `0` && p[0] <= `9` {
						info.reserved_nums << p.int()
					}
				}
			}
			continue
		}

		// Parse field definition
		if trimmed.contains('=') && !trimmed.starts_with('option') && !trimmed.starts_with('//')
			&& !trimmed.starts_with('syntax') && !trimmed.starts_with('package')
			&& !trimmed.starts_with('message') && !trimmed.starts_with('enum') {
			if field := parse_protobuf_field_info(trimmed, info.syntax) {
				info.fields[field.number] = field
			}
		}
	}

	return info
}

fn parse_protobuf_field_info(line string, syntax string) ?ProtoFieldInfo {
	mut field := ProtoFieldInfo{}

	// Remove trailing semicolon and options [...]
	mut clean := line.trim_right(';')
	if bracket_idx := clean.index('[') {
		clean = clean[..bracket_idx]
	}
	clean = clean.trim_space()

	// Split by '='
	parts := clean.split('=')
	if parts.len != 2 {
		return none
	}

	// Parse field number
	field.number = parts[1].trim_space().int()
	if field.number <= 0 {
		return none
	}

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

	for type_idx < tokens.len {
		match tokens[type_idx] {
			'repeated' {
				field.is_repeated = true
				type_idx += 1
			}
			'optional' {
				field.is_optional = true
				type_idx += 1
			}
			'required' {
				field.is_required = true
				type_idx += 1
			}
			else {
				break
			}
		}
	}

	if type_idx + 1 >= tokens.len {
		return none
	}

	field.field_type = tokens[type_idx]
	field.name = tokens[type_idx + 1]

	// In proto3, all singular fields are implicitly optional
	if syntax == 'proto3' && !field.is_repeated {
		field.is_optional = true
	}

	return field
}

fn is_protobuf_type_backward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Compatible type changes:
	// int32, uint32, int64, uint64, bool are compatible (wire type 0)
	// sint32, sint64 are compatible with each other (zigzag encoding)
	// fixed32, sfixed32 are compatible (wire type 5)
	// fixed64, sfixed64 are compatible (wire type 1)
	// string, bytes are compatible (wire type 2)

	varint_types := ['int32', 'uint32', 'int64', 'uint64', 'bool']
	if old_type in varint_types && new_type in varint_types {
		return true
	}

	zigzag_types := ['sint32', 'sint64']
	if old_type in zigzag_types && new_type in zigzag_types {
		return true
	}

	fixed32_types := ['fixed32', 'sfixed32', 'float']
	if old_type in fixed32_types && new_type in fixed32_types {
		return true
	}

	fixed64_types := ['fixed64', 'sfixed64', 'double']
	if old_type in fixed64_types && new_type in fixed64_types {
		return true
	}

	length_delimited := ['string', 'bytes']
	if old_type in length_delimited && new_type in length_delimited {
		return true
	}

	return false
}

fn is_protobuf_type_forward_compatible(old_type string, new_type string) bool {
	// Forward compatibility has the same rules as backward for protobuf
	return is_protobuf_type_backward_compatible(old_type, new_type)
}

fn extract_protobuf_fields(schema_str string) map[int]string {
	// Extract field definitions from protobuf schema
	// Format: type name = number;
	mut fields := map[int]string{}

	lines := schema_str.split('\n')
	for line in lines {
		trimmed := line.trim_space()
		if trimmed.contains('=') && trimmed.ends_with(';') {
			// Parse: optional string name = 1;
			parts := trimmed.split('=')
			if parts.len >= 2 {
				// Extract field number
				num_part := parts[1].trim_space().trim_right(';')
				field_num := num_part.int()

				// Extract field name (last word before =)
				name_parts := parts[0].trim_space().split(' ')
				if name_parts.len >= 2 {
					field_name := name_parts[name_parts.len - 1]
					fields[field_num] = field_name
				}
			}
		}
	}

	return fields
}
