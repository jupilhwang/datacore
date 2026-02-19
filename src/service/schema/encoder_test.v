// Schema Encoder Tests
module schema

// Avro Encoder Tests

fn test_avro_encode_int() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"int"}'

	// Test positive integer
	result := encoder.encode('42'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// 42 in zigzag = 84 = 0x54
	assert result.len == 1
	assert result[0] == 0x54

	// Test negative integer
	result2 := encoder.encode('-1'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// -1 in zigzag = 1
	assert result2.len == 1
	assert result2[0] == 1
}

fn test_avro_encode_long() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"long"}'

	// Test large positive
	result := encoder.encode('300'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// 300 in zigzag = 600 = 0x258
	// varint: 0xD8 0x04
	assert result.len == 2
	assert result[0] == 0xD8
	assert result[1] == 0x04
}

fn test_avro_encode_string() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"string"}'

	result := encoder.encode('"hello"'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// Length (5) in zigzag (10) + "hello"
	assert result.len == 6
	assert result[0] == 10 // length 5 zigzag = 10
	assert result[1..6].bytestr() == 'hello'
}

fn test_avro_encode_boolean() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"boolean"}'

	result_true := encoder.encode('true'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result_true == [u8(1)]

	result_false := encoder.encode('false'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result_false == [u8(0)]
}

fn test_avro_encode_null() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"null"}'

	result := encoder.encode('null'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// null is encoded as zero bytes
	assert result.len == 0
}

fn test_avro_decode_int() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"int"}'

	// 42 in zigzag = 84 = 0x54
	result := encoder.decode([u8(0x54)], schema_str) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert result.bytestr() == '42'
}

fn test_avro_decode_string() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"string"}'

	// Length 5 (zigzag 10) + "hello"
	result := encoder.decode([u8(10), `h`, `e`, `l`, `l`, `o`], schema_str) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert result.bytestr() == '"hello"'
}

fn test_avro_roundtrip_record() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}'

	input := '{"name":"Alice","age":30}'

	encoded := encoder.encode(input.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	decoded := encoder.decode(encoded, schema_str) or {
		assert false, 'decode failed: ${err}'
		return
	}

	// Verify fields are present (order may vary in JSON)
	decoded_str := decoded.bytestr()
	assert decoded_str.contains('"name"')
	assert decoded_str.contains('"Alice"')
	assert decoded_str.contains('"age"')
	assert decoded_str.contains('30')
}

// JSON Schema Encoder Tests

fn test_json_schema_validate_object() {
	mut encoder := new_json_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}'

	// Valid object
	result := encoder.encode('{"name":"Bob","age":25}'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result.len > 0

	// Missing required field
	encoder.encode('{"age":25}'.bytes(), schema_str) or {
		// Should fail
		assert true
		return
	}
	assert false, 'should have failed for missing required field'
}

fn test_json_schema_validate_string() {
	mut encoder := new_json_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"string","minLength":3,"maxLength":10}'

	// Valid string
	result := encoder.encode('"hello"'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result.len > 0

	// Too short
	encoder.encode('"ab"'.bytes(), schema_str) or {
		assert true
		return
	}
	assert false, 'should have failed for too short string'
}

fn test_json_schema_validate_number() {
	mut encoder := new_json_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"number","minimum":0,"maximum":100}'

	// Valid number
	result := encoder.encode('50'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result.len > 0

	// Below minimum
	encoder.encode('-5'.bytes(), schema_str) or {
		assert true
		return
	}
	assert false, 'should have failed for number below minimum'
}

fn test_json_schema_validate_array() {
	mut encoder := new_json_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := '{"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":5}'

	// Valid array
	result := encoder.encode('[1,2,3]'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}
	assert result.len > 0

	// Empty array (minItems violation)
	encoder.encode('[]'.bytes(), schema_str) or {
		assert true
		return
	}
	assert false, 'should have failed for empty array'
}

// Protobuf Encoder Tests

fn test_protobuf_encode_varint() {
	mut encoder := new_protobuf_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := 'message Test { int32 value = 1; }'

	result := encoder.encode('{"value":150}'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// Field tag: (1 << 3) | 0 = 8 = 0x08
	// Value 150: varint 0x96 0x01
	assert result.len == 3
	assert result[0] == 0x08 // tag
	assert result[1] == 0x96 // 150 low byte
	assert result[2] == 0x01 // 150 high byte
}

fn test_protobuf_encode_string() {
	mut encoder := new_protobuf_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := 'message Test { string name = 1; }'

	result := encoder.encode('{"name":"test"}'.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	// Field tag: (1 << 3) | 2 = 10 = 0x0A
	// Length: 4
	// Value: "test"
	assert result.len == 6
	assert result[0] == 0x0A // tag
	assert result[1] == 4 // length
	assert result[2..6].bytestr() == 'test'
}

fn test_protobuf_decode_varint() {
	mut encoder := new_protobuf_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := 'message Test { int32 value = 1; }'

	// Tag 0x08, value 150 (0x96 0x01)
	result := encoder.decode([u8(0x08), 0x96, 0x01], schema_str) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert result.bytestr().contains('"value"')
	assert result.bytestr().contains('150')
}

fn test_protobuf_roundtrip() {
	mut encoder := new_protobuf_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}
	schema_str := 'message Person { string name = 1; int32 age = 2; }'

	input := '{"name":"Alice","age":30}'

	encoded := encoder.encode(input.bytes(), schema_str) or {
		assert false, 'encode failed: ${err}'
		return
	}

	decoded := encoder.decode(encoded, schema_str) or {
		assert false, 'decode failed: ${err}'
		return
	}

	decoded_str := decoded.bytestr()
	assert decoded_str.contains('"name"')
	assert decoded_str.contains('"Alice"')
	assert decoded_str.contains('"age"')
	assert decoded_str.contains('30')
}

// Varint Encoding Tests

fn test_zigzag_encoding() {
	// Test zigzag encoding for various values
	// 0 -> 0
	assert encode_varint_zigzag(0) == [u8(0)]

	// -1 -> 1
	assert encode_varint_zigzag(-1) == [u8(1)]

	// 1 -> 2
	assert encode_varint_zigzag(1) == [u8(2)]

	// -2 -> 3
	assert encode_varint_zigzag(-2) == [u8(3)]

	// 2147483647 -> 4294967294
	result := encode_varint_zigzag(2147483647)
	assert result.len > 0
}

fn test_varint_encoding() {
	// 1 -> 0x01
	assert encode_varint(1) == [u8(0x01)]

	// 127 -> 0x7F
	assert encode_varint(127) == [u8(0x7F)]

	// 128 -> 0x80 0x01
	assert encode_varint(128) == [u8(0x80), u8(0x01)]

	// 300 -> 0xAC 0x02
	assert encode_varint(300) == [u8(0xAC), u8(0x02)]
}

// Encoder Interface Tests

fn test_encoder_interface_avro() {
	mut encoder := new_avro_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}

	// Test that encoder implements Encoder interface
	assert encoder.format() == Format.avro
}

fn test_encoder_interface_protobuf() {
	mut encoder := new_protobuf_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}

	// Test that encoder implements Encoder interface
	assert encoder.format() == Format.protobuf
}

fn test_encoder_interface_json() {
	mut encoder := new_json_encoder() or {
		assert false, 'failed to create encoder: ${err}'
		return
	}

	// Test that encoder implements Encoder interface
	assert encoder.format() == Format.json
}
