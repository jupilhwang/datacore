// Service Layer - JSON Schema Compatibility Checking
// Provides compatibility checking for JSON Schema
module schema

// JSON Schema Types

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

// JSON Schema Compatibility

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
						return false
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
					return false
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
	info.min_length = extract_json_int(schema_str, 'minLength') or { 0 }
	info.max_length = extract_json_int(schema_str, 'maxLength') or { 0 }
	info.min_items = extract_json_int(schema_str, 'minItems') or { 0 }
	info.max_items = extract_json_int(schema_str, 'maxItems') or { 0 }

	// Extract enum
	info.enum_values = parse_json_string_array(schema_str, 'enum') or { []string{} }

	// Check for default
	info.has_default = schema_str.contains('"default"')

	return info
}

fn parse_json_properties_info(props_json string) map[string]JsonPropertyInfo {
	mut result := map[string]JsonPropertyInfo{}

	mut pos := 1

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
				return false
			}
		}
	}

	return true
}
