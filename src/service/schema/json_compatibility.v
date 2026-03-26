// Service Layer - JSON Schema Compatibility Checking
// Provides compatibility checking for JSON Schema
module schema

import x.json2

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

	raw := json2.decode[json2.Any](schema_str) or { return info }
	obj := raw.as_map()

	if v := obj['type'] {
		info.schema_type = v.str()
	}

	if req_val := obj['required'] {
		for item in req_val.arr() {
			info.required << item.str()
		}
	}

	if ap_val := obj['additionalProperties'] {
		if ap_val.json_str() == 'false' {
			info.additional_properties = false
		}
	}

	if props_val := obj['properties'] {
		info.properties = build_properties_info_from_map(props_val.as_map())
	}

	apply_schema_info_constraints(mut info, obj)
	info.has_default = 'default' in obj

	return info
}

fn apply_schema_info_constraints(mut info JsonSchemaInfo, obj map[string]json2.Any) {
	if v := obj['minimum'] {
		info.minimum = v.f64()
		info.has_minimum = true
	}
	if v := obj['maximum'] {
		info.maximum = v.f64()
		info.has_maximum = true
	}
	if v := obj['minLength'] {
		info.min_length = v.int()
	}
	if v := obj['maxLength'] {
		info.max_length = v.int()
	}
	if v := obj['minItems'] {
		info.min_items = v.int()
	}
	if v := obj['maxItems'] {
		info.max_items = v.int()
	}
	if enum_val := obj['enum'] {
		for item in enum_val.arr() {
			info.enum_values << item.str()
		}
	}
}

fn build_properties_info_from_map(props_map map[string]json2.Any) map[string]JsonPropertyInfo {
	mut result := map[string]JsonPropertyInfo{}

	for name, val in props_map {
		prop_obj := val.as_map()
		mut prop_info := JsonPropertyInfo{}

		if v := prop_obj['type'] {
			prop_info.prop_type = v.str()
		}

		prop_info.has_default = 'default' in prop_obj
		prop_json := val.json_str()
		prop_info.nullable = prop_info.prop_type == 'null' || prop_json.contains('"null"')

		if enum_val := prop_obj['enum'] {
			for item in enum_val.arr() {
				prop_info.enum_values << item.str()
			}
		}

		result[name] = prop_info
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
