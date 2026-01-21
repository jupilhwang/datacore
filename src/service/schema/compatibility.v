// Service Layer - Schema Compatibility Checking
// Provides compatibility checking for Avro, JSON Schema, and Protobuf schemas
module schema

import domain

// ============================================================================
// Main Compatibility Functions
// ============================================================================

// check_backward_compatible checks if new schema can read data written with old schema
fn check_backward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_backward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_backward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_backward_compatible(old_schema, new_schema)
		}
	}
}

// check_forward_compatible checks if old schema can read data written with new schema
fn check_forward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_forward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_forward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_forward_compatible(old_schema, new_schema)
		}
	}
}

// ============================================================================
// Avro Schema Types
// ============================================================================

// AvroField represents a parsed Avro record field
struct AvroField {
mut:
	name        string
	field_type  string // primitive type or complex type name
	has_default bool
	is_nullable bool // union with null
	is_array    bool
	is_map      bool
	is_union    bool
	union_types []string // types in union
}

// AvroSchema represents a parsed Avro schema
pub struct AvroSchema {
pub mut:
	schema_type string // record, enum, array, map, union, fixed, primitive
	name        string // schema name (for record/enum)
	namespace   string
	fields      []AvroField // for record type
	symbols     []string    // for enum type
	items_type  string      // for array type
	values_type string      // for map type
	union_types []string    // for union type
	fixed_size  int         // for fixed type
}

// ============================================================================
// Avro Schema Compatibility
// ============================================================================

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

// ============================================================================
// Avro Schema Parsing
// ============================================================================

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
	mut pos := fields_start + 8 // length of '"fields"'
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
	mut field_start := 1 // skip opening [
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

// ============================================================================
// JSON Schema Types
// ============================================================================

// JsonSchemaInfo represents parsed JSON Schema information for compatibility checking
struct JsonSchemaInfo {
mut:
	schema_type           string
	properties            map[string]JsonPropertyInfo
	required              []string
	additional_properties bool = true
	has_default           bool
	enum_values           []string
	// Numeric constraints
	minimum     f64
	maximum     f64
	has_minimum bool
	has_maximum bool
	// String constraints
	min_length int
	max_length int
	pattern    string
	// Array constraints
	min_items int
	max_items int
}

struct JsonPropertyInfo {
mut:
	prop_type   string
	has_default bool
	nullable    bool
	enum_values []string
}

// ============================================================================
// JSON Schema Compatibility
// ============================================================================

fn check_json_backward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema backward compatibility:
	// - New schema can read old data
	// - All old required properties must exist in new schema
	// - New required properties must have been optional or have defaults
	// - Property types must be compatible (new type can read old data)

	old_info := parse_json_schema_info(old_schema)
	new_info := parse_json_schema_info(new_schema)

	// Type compatibility check
	if old_info.schema_type.len > 0 && new_info.schema_type.len > 0 {
		if !is_json_type_backward_compatible(old_info.schema_type, new_info.schema_type) {
			return false
		}
	}

	// All old required properties must exist in new schema with compatible types
	for prop in old_info.required {
		if new_prop := new_info.properties[prop] {
			// Property exists - check type compatibility
			if old_prop := old_info.properties[prop] {
				if !is_json_property_backward_compatible(old_prop, new_prop) {
					return false
				}
			}
		} else if !new_info.additional_properties {
			// Property removed and additionalProperties is false
			return false
		}
		// If additionalProperties is true, removed properties are OK (ignored)
	}

	// New required properties must have existed before (optional or required)
	// or have a default value
	for prop in new_info.required {
		if prop !in old_info.required {
			// New required property
			if _ := old_info.properties[prop] {
				// Property existed as optional - OK
				continue
			} else {
				// Completely new required property - check for default
				if new_prop := new_info.properties[prop] {
					if !new_prop.has_default && !new_prop.nullable {
						return false // Old data won't have this property
					}
				} else {
					return false
				}
			}
		}
	}

	// Check property type compatibility for all shared properties
	for name, old_prop in old_info.properties {
		if new_prop := new_info.properties[name] {
			if !is_json_property_backward_compatible(old_prop, new_prop) {
				return false
			}
		}
	}

	return true
}

fn check_json_forward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema forward compatibility:
	// - Old schema can read new data
	// - All new required properties must exist in old schema
	// - Old required properties that are removed must have defaults

	old_info := parse_json_schema_info(old_schema)
	new_info := parse_json_schema_info(new_schema)

	// Type compatibility check
	if old_info.schema_type.len > 0 && new_info.schema_type.len > 0 {
		if !is_json_type_forward_compatible(old_info.schema_type, new_info.schema_type) {
			return false
		}
	}

	// All new required properties must exist in old schema
	for prop in new_info.required {
		if _ := old_info.properties[prop] {
			// Property exists - OK
		} else if !old_info.additional_properties {
			// New property and old schema doesn't allow additional
			return false
		}
	}

	// Old required properties that are removed from new schema
	for prop in old_info.required {
		// Check if property is completely removed (not just made optional)
		if _ := new_info.properties[prop] {
			// Property still exists - OK
		} else {
			// Property removed - old reader needs default
			if old_prop := old_info.properties[prop] {
				if !old_prop.has_default && !old_prop.nullable {
					return false // Old reader expects this property
				}
			}
		}
	}

	return true
}

fn parse_json_schema_info(schema_str string) JsonSchemaInfo {
	mut info := JsonSchemaInfo{
		properties: map[string]JsonPropertyInfo{}
	}

	// Extract type
	info.schema_type = extract_json_string(schema_str, 'type') or { '' }

	// Extract required
	info.required = parse_json_string_array(schema_str, 'required') or { []string{} }

	// Extract additionalProperties
	if schema_str.contains('"additionalProperties":false')
		|| schema_str.contains('"additionalProperties": false') {
		info.additional_properties = false
	}

	// Extract properties
	if props_start := schema_str.index('"properties"') {
		props_json := extract_json_object_value(schema_str, props_start + 12)
		if props_json.len > 0 {
			info.properties = parse_json_properties_info(props_json)
		}
	}

	// Extract numeric constraints
	if min_val := extract_json_float(schema_str, 'minimum') {
		info.minimum = min_val
		info.has_minimum = true
	}
	if max_val := extract_json_float(schema_str, 'maximum') {
		info.maximum = max_val
		info.has_maximum = true
	}

	// Extract string/array constraints
	info.min_length = extract_json_number(schema_str, 'minLength') or { 0 }
	info.max_length = extract_json_number(schema_str, 'maxLength') or { 0 }
	info.min_items = extract_json_number(schema_str, 'minItems') or { 0 }
	info.max_items = extract_json_number(schema_str, 'maxItems') or { 0 }

	// Extract enum
	info.enum_values = parse_json_string_array(schema_str, 'enum') or { []string{} }

	// Check for default
	info.has_default = schema_str.contains('"default"')

	return info
}

fn parse_json_properties_info(props_json string) map[string]JsonPropertyInfo {
	mut result := map[string]JsonPropertyInfo{}

	mut pos := 1 // Skip opening brace

	for pos < props_json.len - 1 {
		// Skip whitespace and commas
		for pos < props_json.len && (props_json[pos] == ` `
			|| props_json[pos] == `\t` || props_json[pos] == `\n`
			|| props_json[pos] == `,`) {
			pos += 1
		}

		if pos >= props_json.len - 1 || props_json[pos] != `"` {
			break
		}

		// Read property name
		name_end := props_json.index_after('"', pos + 1) or { break }
		prop_name := props_json[pos + 1..name_end]
		pos = name_end + 1

		// Skip colon and whitespace
		for pos < props_json.len && (props_json[pos] == `:` || props_json[pos] == ` `
			|| props_json[pos] == `\t` || props_json[pos] == `\n`) {
			pos += 1
		}

		// Read property schema
		if pos < props_json.len && props_json[pos] == `{` {
			prop_schema := extract_json_object_value(props_json, pos)
			if prop_schema.len > 0 {
				mut prop_info := JsonPropertyInfo{}
				prop_info.prop_type = extract_json_string(prop_schema, 'type') or { '' }
				prop_info.has_default = prop_schema.contains('"default"')
				prop_info.nullable = prop_info.prop_type == 'null' || prop_schema.contains('"null"')
				prop_info.enum_values = parse_json_string_array(prop_schema, 'enum') or {
					[]string{}
				}

				result[prop_name] = prop_info
				pos += prop_schema.len
			} else {
				pos += 1
			}
		}
	}

	return result
}

fn is_json_type_backward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Type widening (new type accepts more) - OK for backward
	// integer -> number (number accepts integers)
	if old_type == 'integer' && new_type == 'number' {
		return true
	}

	return false
}

fn is_json_type_forward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Type narrowing (new type is subset) - OK for forward
	// number -> integer (if new data is always integers)
	if old_type == 'number' && new_type == 'integer' {
		return true
	}

	return false
}

fn is_json_property_backward_compatible(old_prop JsonPropertyInfo, new_prop JsonPropertyInfo) bool {
	// If types are specified, check compatibility
	if old_prop.prop_type.len > 0 && new_prop.prop_type.len > 0 {
		if !is_json_type_backward_compatible(old_prop.prop_type, new_prop.prop_type) {
			return false
		}
	}

	// If old property has enum, new property must be superset or same
	if old_prop.enum_values.len > 0 && new_prop.enum_values.len > 0 {
		for old_val in old_prop.enum_values {
			if old_val !in new_prop.enum_values {
				return false // Old enum value not in new enum
			}
		}
	}

	return true
}

// ============================================================================
// Protobuf Schema Types
// ============================================================================

// ProtoFieldInfo represents parsed protobuf field information
struct ProtoFieldInfo {
mut:
	name        string
	field_type  string
	number      int
	is_repeated bool
	is_optional bool
	is_required bool // proto2 only
}

// ProtoSchemaInfo represents parsed protobuf schema information
struct ProtoSchemaInfo {
mut:
	syntax          string // "proto2" or "proto3"
	message_name    string
	fields          map[int]ProtoFieldInfo // keyed by field number
	reserved_nums   []int
	reserved_names  []string
	nested_messages []string
	enums           []string
}

// ============================================================================
// Protobuf Schema Compatibility
// ============================================================================

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
								end_num = 536870911 // Protobuf max field number
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
