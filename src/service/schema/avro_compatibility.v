// Service Layer - Avro Schema Compatibility Checking
// Provides compatibility checking for Avro schemas
module schema

// Avro Schema Types

// AvroField represents a parsed Avro record field
struct AvroField {
mut:
	name        string
	field_type  string
	has_default bool
	is_nullable bool
	is_array    bool
	is_map      bool
	is_union    bool
	union_types []string
}

// AvroSchema represents a parsed Avro schema
/// AvroSchema represents a parsed Avro schema.
pub struct AvroSchema {
pub mut:
	schema_type string
	name        string
	namespace   string
	fields      []AvroField
	symbols     []string
	items_type  string
	values_type string
	union_types []string
	fixed_size  int
}

// Avro Schema Compatibility

fn check_avro_backward_compatible(old_schema string, new_schema string) bool {
	// Parse both schemas
	old_parsed := parse_avro_schema(old_schema) or { return false }
	new_parsed := parse_avro_schema(new_schema) or { return false }

	// Check schema type compatibility
	if old_parsed.schema_type != new_parsed.schema_type {
		return false
	}

	match old_parsed.schema_type {
		'record' {
			return check_avro_record_backward_compatible(old_parsed, new_parsed)
		}
		'enum' {
			return check_avro_enum_backward_compatible(old_parsed, new_parsed)
		}
		else {
			// Primitives and other types must match exactly
			return old_parsed.schema_type == new_parsed.schema_type
		}
	}
}

fn check_avro_forward_compatible(old_schema string, new_schema string) bool {
	// Parse both schemas
	old_parsed := parse_avro_schema(old_schema) or { return false }
	new_parsed := parse_avro_schema(new_schema) or { return false }

	// Check schema type compatibility
	if old_parsed.schema_type != new_parsed.schema_type {
		return false
	}

	match old_parsed.schema_type {
		'record' {
			return check_avro_record_forward_compatible(old_parsed, new_parsed)
		}
		'enum' {
			return check_avro_enum_forward_compatible(old_parsed, new_parsed)
		}
		else {
			return old_parsed.schema_type == new_parsed.schema_type
		}
	}
}

fn check_avro_record_backward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Rule 1: All fields in the OLD schema must exist in NEW schema OR have defaults in NEW
	// Rule 2: New fields added to NEW schema must have defaults (so old data can be read)

	// Create map of new schema fields for quick lookup
	mut new_fields := map[string]AvroField{}
	for f in new_schema.fields {
		new_fields[f.name] = f
	}

	// Check each old field exists in new schema with compatible type
	for old_field in old_schema.fields {
		if new_field := new_fields[old_field.name] {
			// Field exists - check type compatibility
			if !is_avro_type_compatible(old_field, new_field) {
				return false
			}
		} else {
			// Field removed in new schema - NOT backward compatible
			// (new schema cannot read old data that has this field)
			// Actually, this IS backward compatible if the reader ignores unknown fields
			// Avro readers typically ignore unknown fields, so this is allowed
			continue
		}
	}

	// Check new fields have defaults (so old data without these fields can be read)
	mut old_field_names := map[string]bool{}
	for f in old_schema.fields {
		old_field_names[f.name] = true
	}

	for new_field in new_schema.fields {
		if new_field.name !in old_field_names {
			// New field - must have default or be nullable
			if !new_field.has_default && !new_field.is_nullable {
				return false
			}
		}
	}

	return true
}

fn check_avro_record_forward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Rule 1: All fields in NEW schema must exist in OLD schema OR have defaults in OLD
	// Rule 2: Fields removed from NEW schema must have had defaults in OLD

	mut old_fields := map[string]AvroField{}
	for f in old_schema.fields {
		old_fields[f.name] = f
	}

	// Check each new field exists in old schema with compatible type
	for new_field in new_schema.fields {
		if old_field := old_fields[new_field.name] {
			if !is_avro_type_compatible(old_field, new_field) {
				return false
			}
		} else {
			// Field added in new schema - old reader won't know about it
			// Old reader will ignore unknown fields, so this is allowed
			continue
		}
	}

	// Check removed fields have defaults (so new data without these fields can be read by old)
	mut new_field_names := map[string]bool{}
	for f in new_schema.fields {
		new_field_names[f.name] = true
	}

	for old_field in old_schema.fields {
		if old_field.name !in new_field_names {
			// Field removed in new schema - old reader needs default
			if !old_field.has_default && !old_field.is_nullable {
				return false
			}
		}
	}

	return true
}

fn check_avro_enum_backward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Backward compatible: new enum can add symbols (old data still valid)
	// All old symbols must exist in new enum
	for symbol in old_schema.symbols {
		if symbol !in new_schema.symbols {
			return false
		}
	}
	return true
}

fn check_avro_enum_forward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Forward compatible: new enum can remove symbols (new data still valid for old reader)
	// All new symbols must exist in old enum
	for symbol in new_schema.symbols {
		if symbol !in old_schema.symbols {
			return false
		}
	}
	return true
}

fn is_avro_type_compatible(old_field AvroField, new_field AvroField) bool {
	// Same type is always compatible
	if old_field.field_type == new_field.field_type {
		return true
	}

	// Check type promotion rules (Avro allows certain promotions)
	// int -> long, float, double
	// long -> float, double
	// float -> double
	// string -> bytes (and vice versa)

	promotable := {
		'int':    ['long', 'float', 'double']
		'long':   ['float', 'double']
		'float':  ['double']
		'string': ['bytes']
		'bytes':  ['string']
	}

	if allowed := promotable[old_field.field_type] {
		if new_field.field_type in allowed {
			return true
		}
	}

	// Union compatibility - if new field is union containing old type
	if new_field.is_union {
		if old_field.field_type in new_field.union_types {
			return true
		}
	}

	// If old field is union, new field can be any of the union types
	if old_field.is_union {
		if new_field.field_type in old_field.union_types {
			return true
		}
	}

	return false
}

// Avro Schema Parsing

fn parse_avro_schema(schema_str string) !AvroSchema {
	// Simple JSON-based Avro schema parser
	trimmed := schema_str.trim_space()

	// Handle primitive types as strings
	if trimmed.starts_with('"') {
		type_name := trimmed.trim('"')
		return AvroSchema{
			schema_type: type_name
		}
	}

	// Handle complex types (object)
	if !trimmed.starts_with('{') {
		return error('invalid Avro schema: expected object')
	}

	// Extract type field
	schema_type := extract_json_string(trimmed, 'type') or { 'unknown' }

	mut result := AvroSchema{
		schema_type: schema_type
		name:        extract_json_string(trimmed, 'name') or { '' }
		namespace:   extract_json_string(trimmed, 'namespace') or { '' }
	}

	if schema_type == 'record' {
		// Parse fields array
		result.fields = parse_avro_fields(trimmed) or { []AvroField{} }
	} else if schema_type == 'enum' {
		// Parse symbols array
		result.symbols = parse_json_string_array(trimmed, 'symbols') or { []string{} }
	} else if schema_type == 'array' {
		result.items_type = extract_json_string(trimmed, 'items') or { '' }
	} else if schema_type == 'map' {
		result.values_type = extract_json_string(trimmed, 'values') or { '' }
	}

	return result
}

fn parse_avro_fields(schema_str string) ![]AvroField {
	// Find fields array in JSON
	fields_start := schema_str.index('"fields"') or { return []AvroField{} }

	// Find the array start after "fields":
	mut pos := fields_start + 8
	for pos < schema_str.len && schema_str[pos] != `[` {
		pos += 1
	}
	if pos >= schema_str.len {
		return []AvroField{}
	}

	// Find matching bracket
	array_start := pos
	mut depth := 1
	pos += 1
	for pos < schema_str.len && depth > 0 {
		if schema_str[pos] == `[` {
			depth += 1
		} else if schema_str[pos] == `]` {
			depth -= 1
		}
		pos += 1
	}

	fields_json := schema_str[array_start..pos]

	// Parse individual field objects
	mut fields := []AvroField{}
	mut field_start := 1
	mut brace_depth := 0

	for i := 1; i < fields_json.len - 1; i++ {
		c := fields_json[i]
		if c == `{` {
			if brace_depth == 0 {
				field_start = i
			}
			brace_depth += 1
		} else if c == `}` {
			brace_depth -= 1
			if brace_depth == 0 {
				field_json := fields_json[field_start..i + 1]
				if field := parse_single_avro_field(field_json) {
					fields << field
				}
			}
		}
	}

	return fields
}

fn parse_single_avro_field(field_json string) ?AvroField {
	name := extract_json_string(field_json, 'name') or { return none }

	mut field := AvroField{
		name:        name
		has_default: field_json.contains('"default"')
	}

	// Parse type - can be string, array (union), or object
	type_start := field_json.index('"type"') or { return field }
	mut pos := type_start + 6

	// Skip to value
	for pos < field_json.len
		&& (field_json[pos] == `:` || field_json[pos] == ` ` || field_json[pos] == `\t`) {
		pos += 1
	}

	if pos >= field_json.len {
		return field
	}

	if field_json[pos] == `"` {
		// Simple type string
		end_quote := field_json.index_after('"', pos + 1) or { return field }
		field.field_type = field_json[pos + 1..end_quote]
		field.is_nullable = field.field_type == 'null'
	} else if field_json[pos] == `[` {
		// Union type
		field.is_union = true
		// Find matching bracket
		mut bracket_depth := 1
		mut union_end := pos + 1
		for union_end < field_json.len && bracket_depth > 0 {
			if field_json[union_end] == `[` {
				bracket_depth += 1
			} else if field_json[union_end] == `]` {
				bracket_depth -= 1
			}
			union_end += 1
		}
		union_str := field_json[pos..union_end]
		field.union_types = parse_union_types(union_str)
		field.is_nullable = 'null' in field.union_types
		// Set primary type (first non-null type)
		for t in field.union_types {
			if t != 'null' {
				field.field_type = t
				break
			}
		}
	} else if field_json[pos] == `{` {
		// Complex type (nested record, array, map, etc.)
		field.field_type = 'complex'
	}

	return field
}

fn parse_union_types(union_str string) []string {
	// Parse ["null", "string"] style union
	mut types := []string{}
	mut in_string := false
	mut start := 0

	for i, c in union_str {
		if c == `"` && !in_string {
			in_string = true
			start = i + 1
		} else if c == `"` && in_string {
			in_string = false
			types << union_str[start..i]
		}
	}

	return types
}
