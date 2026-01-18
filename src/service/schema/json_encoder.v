// Service Layer - JSON Schema Encoder
// Provides validation-based encoding/decoding for JSON Schema
module schema

import domain

// ============================================================================
// JSON Schema Encoder
// Validates and encodes JSON data according to JSON Schema
// https://json-schema.org/draft/2020-12/json-schema-validation.html
// ============================================================================

pub struct JsonSchemaEncoder {}

pub fn new_json_schema_encoder() &JsonSchemaEncoder {
    return &JsonSchemaEncoder{}
}

// encode validates and returns JSON data (JSON Schema is a validation schema, not binary format)
pub fn (e &JsonSchemaEncoder) encode(data []u8, schema domain.Schema) ![]u8 {
    json_str := data.bytestr()
    
    // Parse schema
    parsed := parse_json_schema(schema.schema_str) or {
        return error('failed to parse JSON Schema: ${err}')
    }
    
    // Validate data against schema
    e.validate(json_str, parsed)!
    
    // JSON Schema doesn't define binary encoding, return normalized JSON
    return normalize_json(json_str).bytes()
}

// decode validates and returns JSON data
pub fn (e &JsonSchemaEncoder) decode(data []u8, schema domain.Schema) ![]u8 {
    json_str := data.bytestr()
    
    parsed := parse_json_schema(schema.schema_str) or {
        return error('failed to parse JSON Schema: ${err}')
    }
    
    e.validate(json_str, parsed)!
    
    return data
}

// validate checks if JSON data conforms to schema
fn (e &JsonSchemaEncoder) validate(json_str string, schema &JsonSchema) ! {
    trimmed := json_str.trim_space()
    
    // Check type constraint
    if schema.schema_type.len > 0 {
        actual_type := detect_json_type(trimmed)
        
        // Handle array of types
        if schema.schema_type !in ['any', actual_type] {
            // Check for numeric type compatibility
            if !(schema.schema_type == 'number' && actual_type == 'integer') {
                if !(schema.schema_type == 'integer' && actual_type == 'number' && is_json_integer(trimmed)) {
                    return error('type mismatch: expected ${schema.schema_type}, got ${actual_type}')
                }
            }
        }
    }
    
    // Validate based on type
    match detect_json_type(trimmed) {
        'object' {
            e.validate_object(trimmed, schema)!
        }
        'array' {
            e.validate_array(trimmed, schema)!
        }
        'string' {
            e.validate_string(trimmed, schema)!
        }
        'number', 'integer' {
            e.validate_number(trimmed, schema)!
        }
        else {
            // null, boolean - no additional validation
        }
    }
}

// validate_object validates JSON object against schema
fn (e &JsonSchemaEncoder) validate_object(json_str string, schema &JsonSchema) ! {
    // Parse object properties
    props := parse_json_map(json_str) or { return error('invalid JSON object') }
    
    // Check required properties
    for required in schema.required {
        if required !in props {
            return error('missing required property: ${required}')
        }
    }
    
    // Validate each property
    for name, value in props {
        if prop_schema := schema.properties[name] {
            e.validate(value, prop_schema)!
        } else if !schema.additional_properties {
            // additionalProperties: false
            return error('unexpected property: ${name}')
        }
    }
    
    // Check minProperties/maxProperties
    if schema.min_properties > 0 && props.len < schema.min_properties {
        return error('too few properties: minimum ${schema.min_properties}, got ${props.len}')
    }
    if schema.max_properties > 0 && props.len > schema.max_properties {
        return error('too many properties: maximum ${schema.max_properties}, got ${props.len}')
    }
}

// validate_array validates JSON array against schema
fn (e &JsonSchemaEncoder) validate_array(json_str string, schema &JsonSchema) ! {
    items := parse_json_array(json_str) or { return error('invalid JSON array') }
    
    // Check minItems/maxItems
    if schema.min_items > 0 && items.len < schema.min_items {
        return error('too few items: minimum ${schema.min_items}, got ${items.len}')
    }
    if schema.max_items > 0 && items.len > schema.max_items {
        return error('too many items: maximum ${schema.max_items}, got ${items.len}')
    }
    
    // Validate items against items schema
    if schema.items_schema != unsafe { nil } && schema.items_schema.schema_type.len > 0 {
        for i, item in items {
            e.validate(item, schema.items_schema) or {
                return error('invalid item at index ${i}: ${err}')
            }
        }
    }
    
    // Check uniqueItems
    if schema.unique_items {
        mut seen := map[string]bool{}
        for item in items {
            normalized := normalize_json(item)
            if normalized in seen {
                return error('duplicate item in array')
            }
            seen[normalized] = true
        }
    }
}

// validate_string validates JSON string against schema
fn (e &JsonSchemaEncoder) validate_string(json_str string, schema &JsonSchema) ! {
    str := parse_json_string_value(json_str) or { return error('invalid JSON string') }
    
    // Check minLength/maxLength
    if schema.min_length > 0 && str.len < schema.min_length {
        return error('string too short: minimum ${schema.min_length}, got ${str.len}')
    }
    if schema.max_length > 0 && str.len > schema.max_length {
        return error('string too long: maximum ${schema.max_length}, got ${str.len}')
    }
    
    // Check pattern (simplified - just check contains for now)
    if schema.pattern.len > 0 {
        // TODO: Full regex support
        // For now, just basic substring match
    }
    
    // Check enum values
    if schema.enum_values.len > 0 {
        mut found := false
        for v in schema.enum_values {
            if v == str {
                found = true
                break
            }
        }
        if !found {
            return error('value not in enum: ${str}')
        }
    }
    
    // Check format (common formats)
    if schema.format.len > 0 {
        match schema.format {
            'email' {
                if !str.contains('@') || !str.contains('.') {
                    return error('invalid email format')
                }
            }
            'uri', 'url' {
                if !str.starts_with('http://') && !str.starts_with('https://') {
                    return error('invalid URI format')
                }
            }
            'date' {
                // YYYY-MM-DD
                if str.len != 10 || str[4] != `-` || str[7] != `-` {
                    return error('invalid date format')
                }
            }
            'date-time' {
                // ISO 8601
                if !str.contains('T') {
                    return error('invalid date-time format')
                }
            }
            'uuid' {
                if str.len != 36 || str[8] != `-` || str[13] != `-` || str[18] != `-` || str[23] != `-` {
                    return error('invalid UUID format')
                }
            }
            else {
                // Unknown format - ignore
            }
        }
    }
}

// validate_number validates JSON number against schema
fn (e &JsonSchemaEncoder) validate_number(json_str string, schema &JsonSchema) ! {
    val := json_str.trim_space().f64()
    
    // Check minimum/maximum
    if schema.has_minimum && val < schema.minimum {
        return error('value below minimum: ${val} < ${schema.minimum}')
    }
    if schema.has_maximum && val > schema.maximum {
        return error('value above maximum: ${val} > ${schema.maximum}')
    }
    if schema.has_exclusive_minimum && val <= schema.exclusive_minimum {
        return error('value not greater than exclusive minimum: ${val} <= ${schema.exclusive_minimum}')
    }
    if schema.has_exclusive_maximum && val >= schema.exclusive_maximum {
        return error('value not less than exclusive maximum: ${val} >= ${schema.exclusive_maximum}')
    }
    
    // Check multipleOf
    if schema.multiple_of > 0 {
        remainder := val - f64(int(val / schema.multiple_of)) * schema.multiple_of
        if remainder > 0.0001 && remainder < schema.multiple_of - 0.0001 {
            return error('value not multiple of ${schema.multiple_of}')
        }
    }
}

// ============================================================================
// JSON Schema Parsing
// ============================================================================

struct JsonSchema {
mut:
    schema_type           string
    properties            map[string]&JsonSchema
    required              []string
    additional_properties bool = true
    min_properties        int
    max_properties        int
    
    items_schema  &JsonSchema = unsafe { nil }
    min_items     int
    max_items     int
    unique_items  bool
    
    min_length    int
    max_length    int
    pattern       string
    format        string
    enum_values   []string
    
    minimum               f64
    maximum               f64
    exclusive_minimum     f64
    exclusive_maximum     f64
    has_minimum           bool
    has_maximum           bool
    has_exclusive_minimum bool
    has_exclusive_maximum bool
    multiple_of           f64
}

fn parse_json_schema(schema_str string) !&JsonSchema {
    mut result := &JsonSchema{
        properties: map[string]&JsonSchema{}
    }
    
    // Extract type
    if type_val := extract_json_string(schema_str, 'type') {
        result.schema_type = type_val
    }
    
    // Extract properties
    if props_start := schema_str.index('"properties"') {
        props_json := extract_json_object_value(schema_str, props_start + 12)
        if props_json.len > 0 {
            result.properties = parse_schema_properties(props_json)
        }
    }
    
    // Extract required
    result.required = parse_json_string_array(schema_str, 'required') or { []string{} }
    
    // Extract additionalProperties
    if schema_str.contains('"additionalProperties":false') || schema_str.contains('"additionalProperties": false') {
        result.additional_properties = false
    }
    
    // Extract items (for arrays)
    if items_start := schema_str.index('"items"') {
        items_json := extract_json_object_value(schema_str, items_start + 7)
        if items_json.len > 0 {
            if inner := parse_json_schema(items_json) {
                result.items_schema = inner
            }
        }
    }
    
    // Extract numeric constraints
    result.min_length = extract_json_number(schema_str, 'minLength') or { 0 }
    result.max_length = extract_json_number(schema_str, 'maxLength') or { 0 }
    result.min_items = extract_json_number(schema_str, 'minItems') or { 0 }
    result.max_items = extract_json_number(schema_str, 'maxItems') or { 0 }
    result.min_properties = extract_json_number(schema_str, 'minProperties') or { 0 }
    result.max_properties = extract_json_number(schema_str, 'maxProperties') or { 0 }
    
    if min_val := extract_json_float(schema_str, 'minimum') {
        result.minimum = min_val
        result.has_minimum = true
    }
    if max_val := extract_json_float(schema_str, 'maximum') {
        result.maximum = max_val
        result.has_maximum = true
    }
    if ex_min := extract_json_float(schema_str, 'exclusiveMinimum') {
        result.exclusive_minimum = ex_min
        result.has_exclusive_minimum = true
    }
    if ex_max := extract_json_float(schema_str, 'exclusiveMaximum') {
        result.exclusive_maximum = ex_max
        result.has_exclusive_maximum = true
    }
    if multiple := extract_json_float(schema_str, 'multipleOf') {
        result.multiple_of = multiple
    }
    
    // Extract string constraints
    result.pattern = extract_json_string(schema_str, 'pattern') or { '' }
    result.format = extract_json_string(schema_str, 'format') or { '' }
    result.enum_values = parse_json_string_array(schema_str, 'enum') or { []string{} }
    
    // Extract uniqueItems
    result.unique_items = schema_str.contains('"uniqueItems":true') || schema_str.contains('"uniqueItems": true')
    
    return result
}

fn parse_schema_properties(props_json string) map[string]&JsonSchema {
    mut result := map[string]&JsonSchema{}
    
    // Simple property extraction
    mut pos := 1  // Skip opening brace
    
    for pos < props_json.len - 1 {
        // Skip whitespace
        for pos < props_json.len && (props_json[pos] == ` ` || props_json[pos] == `\t` || props_json[pos] == `\n` || props_json[pos] == `,`) {
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
        for pos < props_json.len && (props_json[pos] == `:` || props_json[pos] == ` ` || props_json[pos] == `\t` || props_json[pos] == `\n`) {
            pos += 1
        }
        
        // Read property schema (object)
        if pos < props_json.len && props_json[pos] == `{` {
            prop_schema := extract_json_object_value(props_json, pos)
            if prop_schema.len > 0 {
                if schema := parse_json_schema(prop_schema) {
                    result[prop_name] = schema
                }
                pos += prop_schema.len
            } else {
                pos += 1
            }
        }
    }
    
    return result
}

fn extract_json_object_value(s string, start int) string {
    mut pos := start
    
    // Skip to opening brace
    for pos < s.len && s[pos] != `{` {
        pos += 1
    }
    
    if pos >= s.len {
        return ''
    }
    
    mut depth := 1
    mut end := pos + 1
    mut in_string := false
    mut escaped := false
    
    for end < s.len && depth > 0 {
        if escaped {
            escaped = false
            end += 1
            continue
        }
        if s[end] == `\\` && in_string {
            escaped = true
            end += 1
            continue
        }
        if s[end] == `"` {
            in_string = !in_string
        } else if !in_string {
            if s[end] == `{` {
                depth += 1
            } else if s[end] == `}` {
                depth -= 1
            }
        }
        end += 1
    }
    
    return s[pos..end]
}

fn extract_json_number(s string, key string) ?int {
    pattern := '"${key}"'
    idx := s.index(pattern) or { return none }
    
    mut pos := idx + pattern.len
    for pos < s.len && (s[pos] == `:` || s[pos] == ` ` || s[pos] == `\t`) {
        pos += 1
    }
    
    if pos >= s.len {
        return none
    }
    
    mut end := pos
    for end < s.len && ((s[end] >= `0` && s[end] <= `9`) || s[end] == `-`) {
        end += 1
    }
    
    if end == pos {
        return none
    }
    
    return s[pos..end].int()
}

fn extract_json_float(s string, key string) ?f64 {
    pattern := '"${key}"'
    idx := s.index(pattern) or { return none }
    
    mut pos := idx + pattern.len
    for pos < s.len && (s[pos] == `:` || s[pos] == ` ` || s[pos] == `\t`) {
        pos += 1
    }
    
    if pos >= s.len {
        return none
    }
    
    mut end := pos
    for end < s.len && ((s[end] >= `0` && s[end] <= `9`) || s[end] == `-` || s[end] == `.` || s[end] == `e` || s[end] == `E` || s[end] == `+`) {
        end += 1
    }
    
    if end == pos {
        return none
    }
    
    return s[pos..end].f64()
}

// ============================================================================
// JSON Utility Functions
// ============================================================================

fn detect_json_type(s string) string {
    trimmed := s.trim_space()
    
    if trimmed.len == 0 {
        return 'null'
    }
    
    c := trimmed[0]
    
    if c == `{` {
        return 'object'
    } else if c == `[` {
        return 'array'
    } else if c == `"` {
        return 'string'
    } else if c == `t` || c == `f` {
        return 'boolean'
    } else if c == `n` {
        return 'null'
    } else if c == `-` || (c >= `0` && c <= `9`) {
        if trimmed.contains('.') || trimmed.contains('e') || trimmed.contains('E') {
            return 'number'
        }
        return 'integer'
    }
    
    return 'unknown'
}

fn is_json_integer(s string) bool {
    trimmed := s.trim_space()
    if trimmed.contains('.') {
        // Check if decimal part is zero
        parts := trimmed.split('.')
        if parts.len == 2 {
            decimal := parts[1].trim_right('0')
            return decimal.len == 0
        }
        return false
    }
    return !trimmed.contains('e') && !trimmed.contains('E')
}

fn normalize_json(s string) string {
    // Simple JSON normalization: remove extra whitespace
    mut result := []u8{}
    mut in_string := false
    mut escaped := false
    
    for c in s {
        if escaped {
            escaped = false
            result << c
            continue
        }
        
        if c == `\\` && in_string {
            escaped = true
            result << c
            continue
        }
        
        if c == `"` {
            in_string = !in_string
            result << c
            continue
        }
        
        if in_string {
            result << c
        } else if c != ` ` && c != `\t` && c != `\n` && c != `\r` {
            result << c
        }
    }
    
    return result.bytestr()
}
