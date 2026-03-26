// Service Layer - JSON Schema compatibility regression tests
// Covers parse_json_schema, parse_json_schema_info, and compatibility checking
module schema

// parse_json_schema characterization tests

fn test_parse_json_schema_basic_type() {
	schema := parse_json_schema('{"type":"string"}') or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.schema_type == 'string'
}

fn test_parse_json_schema_object_with_properties() {
	schema_str := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.schema_type == 'object'
	assert schema.properties.len == 2
	assert schema.properties['name'].schema_type == 'string'
	assert schema.properties['age'].schema_type == 'integer'
}

fn test_parse_json_schema_required_fields() {
	schema_str := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.required.len == 1
	assert schema.required[0] == 'name'
}

fn test_parse_json_schema_items_schema() {
	schema_str := '{"type":"array","items":{"type":"string"}}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.schema_type == 'array'
	assert schema.items_schema != unsafe { nil }
	assert schema.items_schema.schema_type == 'string'
}

fn test_parse_json_schema_additional_properties_false() {
	schema_str := '{"type":"object","additionalProperties":false}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.additional_properties == false
}

fn test_parse_json_schema_numeric_constraints() {
	schema_str := '{"type":"number","minimum":0,"maximum":100,"multipleOf":5}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.has_minimum == true
	assert schema.minimum == 0.0
	assert schema.has_maximum == true
	assert schema.maximum == 100.0
	assert schema.multiple_of == 5.0
}

fn test_parse_json_schema_string_constraints() {
	schema_str := '{"type":"string","minLength":3,"maxLength":10,"format":"email"}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.min_length == 3
	assert schema.max_length == 10
	assert schema.format == 'email'
}

fn test_parse_json_schema_enum_values() {
	schema_str := '{"type":"string","enum":["a","b","c"]}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.enum_values.len == 3
	assert schema.enum_values[0] == 'a'
	assert schema.enum_values[1] == 'b'
	assert schema.enum_values[2] == 'c'
}

fn test_parse_json_schema_array_constraints() {
	schema_str := '{"type":"array","uniqueItems":true,"minItems":1,"maxItems":10}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.unique_items == true
	assert schema.min_items == 1
	assert schema.max_items == 10
}

fn test_parse_json_schema_exclusive_bounds() {
	schema_str := '{"type":"number","exclusiveMinimum":0,"exclusiveMaximum":100}'
	schema := parse_json_schema(schema_str) or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.has_exclusive_minimum == true
	assert schema.exclusive_minimum == 0.0
	assert schema.has_exclusive_maximum == true
	assert schema.exclusive_maximum == 100.0
}

fn test_parse_json_schema_empty_schema() {
	schema := parse_json_schema('{}') or {
		assert false, 'parse failed: ${err}'
		return
	}
	assert schema.schema_type == ''
	assert schema.properties.len == 0
	assert schema.required.len == 0
}

// parse_json_schema_info characterization tests

fn test_parse_json_schema_info_basic() {
	info := parse_json_schema_info('{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}')
	assert info.schema_type == 'object'
	assert info.required.len == 1
	assert info.required[0] == 'name'
	assert info.properties.len == 1
	assert info.properties['name'].prop_type == 'string'
}

fn test_parse_json_schema_info_with_default() {
	info := parse_json_schema_info('{"type":"object","properties":{"age":{"type":"integer","default":0}},"default":null}')
	assert info.has_default == true
	assert info.properties['age'].has_default == true
}

fn test_parse_json_schema_info_nullable_property() {
	info := parse_json_schema_info('{"type":"object","properties":{"val":{"type":"null"}}}')
	assert info.properties['val'].nullable == true
}

fn test_parse_json_schema_info_additional_properties_false() {
	info := parse_json_schema_info('{"type":"object","additionalProperties":false}')
	assert info.additional_properties == false
}

fn test_parse_json_schema_info_constraints() {
	info := parse_json_schema_info('{"type":"number","minimum":0,"maximum":100,"minLength":1,"maxLength":50,"minItems":1,"maxItems":10}')
	assert info.has_minimum == true
	assert info.minimum == 0.0
	assert info.has_maximum == true
	assert info.maximum == 100.0
	assert info.min_length == 1
	assert info.max_length == 50
	assert info.min_items == 1
	assert info.max_items == 10
}

fn test_parse_json_schema_info_enum() {
	info := parse_json_schema_info('{"type":"string","enum":["x","y"]}')
	assert info.enum_values.len == 2
	assert info.enum_values[0] == 'x'
	assert info.enum_values[1] == 'y'
}

// Compatibility regression tests

fn test_json_backward_compat_add_optional_property() {
	old_schema := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}'
	new_schema := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == true
}

fn test_json_backward_compat_remove_property_additional_true() {
	old_schema := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}'
	new_schema := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == true
}

fn test_json_backward_compat_add_required_no_default() {
	old_schema := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}'
	new_schema := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == false
}

fn test_json_backward_compat_type_widening() {
	old_schema := '{"type":"object","properties":{"val":{"type":"integer"}},"required":["val"]}'
	new_schema := '{"type":"object","properties":{"val":{"type":"number"}},"required":["val"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == true
}

fn test_json_backward_compat_type_narrowing() {
	old_schema := '{"type":"object","properties":{"val":{"type":"number"}},"required":["val"]}'
	new_schema := '{"type":"object","properties":{"val":{"type":"integer"}},"required":["val"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == false
}

fn test_json_forward_compat_add_property() {
	old_schema := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":true}'
	new_schema := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"],"additionalProperties":true}'
	assert check_json_forward_compatible(old_schema, new_schema) == true
}

fn test_json_forward_compat_type_narrowing() {
	old_schema := '{"type":"object","properties":{"val":{"type":"number"}},"required":["val"]}'
	new_schema := '{"type":"object","properties":{"val":{"type":"integer"}},"required":["val"]}'
	assert check_json_forward_compatible(old_schema, new_schema) == true
}

fn test_json_backward_compat_enum_superset() {
	old_schema := '{"type":"object","properties":{"status":{"type":"string","enum":["a","b"]}},"required":["status"]}'
	new_schema := '{"type":"object","properties":{"status":{"type":"string","enum":["a","b","c"]}},"required":["status"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == true
}

fn test_json_backward_compat_enum_removed_value() {
	old_schema := '{"type":"object","properties":{"status":{"type":"string","enum":["a","b","c"]}},"required":["status"]}'
	new_schema := '{"type":"object","properties":{"status":{"type":"string","enum":["a","b"]}},"required":["status"]}'
	assert check_json_backward_compatible(old_schema, new_schema) == false
}
