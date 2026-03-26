// Service Layer - JSON helpers regression tests
// Covers all json_utils.v, encoder_json_helpers.v, and avro_encoder.v helper functions
module schema

// extract_json_string tests

fn test_extract_json_string_basic() {
	json_str := '{"name":"Alice","city":"Seoul"}'
	assert extract_json_string(json_str, 'name') or { '' } == 'Alice'
	assert extract_json_string(json_str, 'city') or { '' } == 'Seoul'
}

fn test_extract_json_string_missing_key() {
	json_str := '{"name":"Alice"}'
	if _ := extract_json_string(json_str, 'age') {
		assert false, 'should return none for missing key'
	}
}

fn test_extract_json_string_with_spaces() {
	json_str := '{ "name" : "Bob" }'
	assert extract_json_string(json_str, 'name') or { '' } == 'Bob'
}

fn test_extract_json_string_escaped_chars() {
	json_str := '{"msg":"hello\\"world"}'
	result := extract_json_string(json_str, 'msg') or { '' }
	// stdlib correctly unescapes JSON: \" becomes "
	assert result == 'hello"world'
}

// extract_json_int tests

fn test_extract_json_int_basic() {
	json_str := '{"id":42,"count":0}'
	assert extract_json_int(json_str, 'id') or { -1 } == 42
	assert extract_json_int(json_str, 'count') or { -1 } == 0
}

fn test_extract_json_int_negative() {
	json_str := '{"offset":-5}'
	assert extract_json_int(json_str, 'offset') or { 0 } == -5
}

fn test_extract_json_int_missing_key() {
	json_str := '{"id":42}'
	if _ := extract_json_int(json_str, 'missing') {
		assert false, 'should return none for missing key'
	}
}

// extract_json_float tests

fn test_extract_json_float_basic() {
	json_str := '{"score":3.14}'
	val := extract_json_float(json_str, 'score') or { 0.0 }
	assert val > 3.13 && val < 3.15
}

fn test_extract_json_float_integer_value() {
	json_str := '{"minimum":0,"maximum":100}'
	assert extract_json_float(json_str, 'minimum') or { -1.0 } == 0.0
	assert extract_json_float(json_str, 'maximum') or { -1.0 } == 100.0
}

fn test_extract_json_float_missing_key() {
	json_str := '{"a":1}'
	if _ := extract_json_float(json_str, 'b') {
		assert false, 'should return none for missing key'
	}
}

// parse_json_string_array tests

fn test_parse_json_string_array_basic() {
	json_str := '{"tags":["a","b","c"]}'
	arr := parse_json_string_array(json_str, 'tags') or { []string{} }
	assert arr.len == 3
	assert arr[0] == 'a'
	assert arr[1] == 'b'
	assert arr[2] == 'c'
}

fn test_parse_json_string_array_empty() {
	json_str := '{"items":[]}'
	arr := parse_json_string_array(json_str, 'items') or { ['fallback'] }
	assert arr.len == 0
}

fn test_parse_json_string_array_missing_key() {
	json_str := '{"a":1}'
	if _ := parse_json_string_array(json_str, 'items') {
		assert false, 'should return none for missing key'
	}
}

// is_valid_json tests

fn test_is_valid_json_objects() {
	assert is_valid_json('{}') == true
	assert is_valid_json('{"a":1}') == true
	assert is_valid_json('{"nested":{"b":2}}') == true
}

fn test_is_valid_json_arrays() {
	assert is_valid_json('[]') == true
	assert is_valid_json('[1,2,3]') == true
	assert is_valid_json('["a","b"]') == true
}

fn test_is_valid_json_primitives() {
	assert is_valid_json('"hello"') == true
	assert is_valid_json('42') == true
	assert is_valid_json('3.14') == true
	assert is_valid_json('true') == true
	assert is_valid_json('false') == true
	assert is_valid_json('null') == true
}

fn test_is_valid_json_invalid() {
	assert is_valid_json('') == false
	assert is_valid_json('{') == false
	assert is_valid_json('not json') == false
}

// parse_json_int tests (encoder_json_helpers.v)

fn test_parse_json_int_bare() {
	assert parse_json_int('42') or { -1 } == 42
	assert parse_json_int('0') or { -1 } == 0
	assert parse_json_int(' 7 ') or { -1 } == 7
}

// parse_json_string_value tests

fn test_parse_json_string_value_basic() {
	assert parse_json_string_value('"hello"') or { '' } == 'hello'
	assert parse_json_string_value('"test value"') or { '' } == 'test value'
}

fn test_parse_json_string_value_not_quoted() {
	if _ := parse_json_string_value('hello') {
		assert false, 'should return none for unquoted string'
	}
}

// parse_json_array tests (returns raw JSON value strings)

fn test_parse_json_array_strings() {
	arr := parse_json_array('["a","b","c"]') or { []string{} }
	assert arr.len == 3
	assert arr[0] == '"a"'
	assert arr[1] == '"b"'
	assert arr[2] == '"c"'
}

fn test_parse_json_array_numbers() {
	arr := parse_json_array('[1,2,3]') or { []string{} }
	assert arr.len == 3
	assert arr[0] == '1'
	assert arr[1] == '2'
	assert arr[2] == '3'
}

fn test_parse_json_array_mixed() {
	arr := parse_json_array('["hello",42,true]') or { []string{} }
	assert arr.len == 3
	assert arr[0] == '"hello"'
	assert arr[1] == '42'
	assert arr[2] == 'true'
}

fn test_parse_json_array_empty() {
	arr := parse_json_array('[]') or { ['fallback'] }
	assert arr.len == 0
}

fn test_parse_json_array_invalid() {
	if _ := parse_json_array('not array') {
		assert false, 'should return none for invalid input'
	}
}

// parse_json_map tests (returns raw JSON value strings)

fn test_parse_json_map_basic() {
	m := parse_json_map('{"name":"Alice","age":30}') or {
		map[string]string{}
	}
	assert m.len == 2
	assert m['name'] == '"Alice"'
	assert m['age'] == '30'
}

fn test_parse_json_map_empty() {
	m := parse_json_map('{}') or {
		map[string]string{}
	}
	assert m.len == 0
}

fn test_parse_json_map_invalid() {
	if _ := parse_json_map('not object') {
		assert false, 'should return none for invalid input'
	}
}

// parse_json_long tests (avro_encoder.v)

fn test_parse_json_long_basic() {
	assert parse_json_long('42') or { i64(-1) } == i64(42)
	assert parse_json_long('0') or { i64(-1) } == i64(0)
	assert parse_json_long(' 100 ') or { i64(-1) } == i64(100)
}

fn test_parse_json_long_null() {
	if _ := parse_json_long('null') {
		assert false, 'should return none for null'
	}
}

// parse_json_float tests (avro_encoder.v)

fn test_parse_json_float_bare() {
	val := parse_json_float('3.14') or { 0.0 }
	assert val > 3.13 && val < 3.15
}

fn test_parse_json_float_integer() {
	assert parse_json_float('42') or { 0.0 } == 42.0
}

fn test_parse_json_float_null() {
	if _ := parse_json_float('null') {
		assert false, 'should return none for null'
	}
}

// parse_json_double tests

fn test_parse_json_double_basic() {
	val := parse_json_double('2.718') or { 0.0 }
	assert val > 2.71 && val < 2.72
}

// parse_json_bool tests

fn test_parse_json_bool_true() {
	assert parse_json_bool('true') or { false } == true
}

fn test_parse_json_bool_false() {
	assert parse_json_bool('false') or { true } == false
}

fn test_parse_json_bool_invalid() {
	if _ := parse_json_bool('yes') {
		assert false, 'should return none for invalid input'
	}
}

// is_json_null tests

fn test_is_json_null_true() {
	assert is_json_null('null') == true
	assert is_json_null(' null ') == true
}

fn test_is_json_null_false() {
	assert is_json_null('42') == false
	assert is_json_null('"null"') == false
	assert is_json_null('') == false
}
