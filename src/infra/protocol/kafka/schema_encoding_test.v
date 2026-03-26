// Tests for Confluent wire format encoding/decoding in schema_encoding.v
module kafka

import domain

// --- Confluent wire format helper tests ---

fn test_wrap_confluent_wire_format() {
	payload := [u8(0xAA), 0xBB, 0xCC]
	schema_id := 42

	result := wrap_confluent_wire_format(payload, schema_id)

	// [magic_byte(0x00)] + [4-byte big-endian schema_id] + [payload]
	assert result.len == 1 + 4 + payload.len
	assert result[0] == 0x00 // magic byte
	// schema_id 42 = 0x0000002A big-endian
	assert result[1] == 0x00
	assert result[2] == 0x00
	assert result[3] == 0x00
	assert result[4] == 0x2A
	// payload
	assert result[5] == 0xAA
	assert result[6] == 0xBB
	assert result[7] == 0xCC
}

fn test_wrap_confluent_wire_format_large_id() {
	payload := [u8(0xFF)]
	schema_id := 0x01020304 // 16909060

	result := wrap_confluent_wire_format(payload, schema_id)

	assert result[0] == 0x00
	assert result[1] == 0x01
	assert result[2] == 0x02
	assert result[3] == 0x03
	assert result[4] == 0x04
	assert result[5] == 0xFF
}

fn test_unwrap_confluent_wire_format_valid() {
	// Construct valid wire format: [0x00] + [schema_id=100 as 4-byte BE] + [payload]
	data := [u8(0x00), 0x00, 0x00, 0x00, 0x64, 0xDE, 0xAD]

	schema_id, payload := unwrap_confluent_wire_format(data)!

	assert schema_id == 100
	assert payload.len == 2
	assert payload[0] == 0xDE
	assert payload[1] == 0xAD
}

fn test_unwrap_confluent_wire_format_too_short() {
	data := [u8(0x00), 0x01, 0x02] // only 3 bytes, need at least 5

	unwrap_confluent_wire_format(data) or {
		assert err.msg().contains('too short')
		return
	}
	assert false, 'should have returned error for too-short data'
}

fn test_unwrap_confluent_wire_format_bad_magic() {
	data := [u8(0x01), 0x00, 0x00, 0x00, 0x01] // magic byte != 0x00

	unwrap_confluent_wire_format(data) or {
		assert err.msg().contains('magic byte')
		return
	}
	assert false, 'should have returned error for bad magic byte'
}

fn test_unwrap_confluent_wire_format_empty_payload() {
	data := [u8(0x00), 0x00, 0x00, 0x00, 0x01] // exactly 5 bytes, empty payload

	schema_id, payload := unwrap_confluent_wire_format(data)!

	assert schema_id == 1
	assert payload.len == 0
}

// --- encode_with_schema / decode_with_schema dispatch tests ---

fn test_encode_with_schema_avro() {
	schema_str := '{"type":"int"}'
	result := encode_with_schema('42'.bytes(), schema_str, .avro)!
	// 42 in zigzag = 84 = 0x54
	assert result.len == 1
	assert result[0] == 0x54
}

fn test_encode_with_schema_json() {
	schema_str := '{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}'
	result := encode_with_schema('{"name":"test"}'.bytes(), schema_str, .json)!
	assert result.len > 0
}

fn test_encode_with_schema_protobuf() {
	schema_str := 'message Test { int32 value = 1; }'
	result := encode_with_schema('{"value":150}'.bytes(), schema_str, .protobuf)!
	assert result.len == 3
	assert result[0] == 0x08
}

fn test_decode_with_schema_avro() {
	schema_str := '{"type":"int"}'
	result := decode_with_schema([u8(0x54)], schema_str, .avro)!
	assert result.bytestr() == '42'
}

// --- Wire format roundtrip test ---

fn test_wire_format_roundtrip() {
	original_payload := 'test data'.bytes()
	schema_id := 256

	wrapped := wrap_confluent_wire_format(original_payload, schema_id)
	recovered_id, recovered_payload := unwrap_confluent_wire_format(wrapped)!

	assert recovered_id == schema_id
	assert recovered_payload == original_payload
}

// --- Full encode -> wire wrap -> unwrap -> decode roundtrip ---

fn test_avro_full_roundtrip_with_wire_format() {
	schema_str := '{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}'
	original_json := '{"name":"Alice","age":30}'
	schema_id := 7

	// Encode
	encoded := encode_with_schema(original_json.bytes(), schema_str, .avro)!

	// Wrap in wire format
	wire_data := wrap_confluent_wire_format(encoded, schema_id)

	// Unwrap wire format
	recovered_id, payload := unwrap_confluent_wire_format(wire_data)!
	assert recovered_id == schema_id

	// Decode
	decoded := decode_with_schema(payload, schema_str, .avro)!
	decoded_str := decoded.bytestr()
	assert decoded_str.contains('"name"')
	assert decoded_str.contains('"Alice"')
	assert decoded_str.contains('"age"')
	assert decoded_str.contains('30')
}

// --- Concurrent safety tests ---

fn test_ensure_encoders_cached_concurrent_safety() {
	// Reset cache to force concurrent re-initialization
	unsafe {
		mut holder := g_encoder_cache_holder
		holder.cached = false
	}
	num_threads := 10
	ch := chan string{cap: num_threads}

	mut threads := []thread{}
	for _ in 0 .. num_threads {
		threads << spawn fn [ch] () {
			ensure_encoders_cached() or {
				ch <- 'error: ${err.msg()}'
				return
			}
			ch <- 'ok'
		}()
	}

	for t in threads {
		t.wait()
	}

	// All threads must complete without error
	mut ok_count := 0
	for _ in 0 .. num_threads {
		msg := <-ch
		if msg == 'ok' {
			ok_count++
		}
	}
	assert ok_count == num_threads, 'all ${num_threads} threads should succeed, got ${ok_count}'

	// Cache must be properly initialized after concurrent calls
	holder := unsafe { g_encoder_cache_holder }
	assert holder.cached, 'cache should be initialized after concurrent init'

	// Encoders must produce correct results after concurrent initialization
	result := encode_with_schema('42'.bytes(), '{"type":"int"}', .avro)!
	assert result.len == 1
	assert result[0] == 0x54
}

fn test_concurrent_encode_decode_with_schema() {
	// Ensure cache is initialized before concurrent encode/decode
	ensure_encoders_cached()!

	num_threads := 8
	ch := chan string{cap: num_threads}
	schema_str := '{"type":"int"}'

	mut threads := []thread{}
	for i in 0 .. num_threads {
		threads << spawn fn [ch, schema_str, i] () {
			// Even threads encode, odd threads decode
			if i % 2 == 0 {
				encoded := encode_with_schema('42'.bytes(), schema_str, .avro) or {
					ch <- 'encode error: ${err.msg()}'
					return
				}
				if encoded.len != 1 || encoded[0] != 0x54 {
					ch <- 'encode mismatch'
					return
				}
			} else {
				decoded := decode_with_schema([u8(0x54)], schema_str, .avro) or {
					ch <- 'decode error: ${err.msg()}'
					return
				}
				if decoded.bytestr() != '42' {
					ch <- 'decode mismatch'
					return
				}
			}
			ch <- 'ok'
		}()
	}

	for t in threads {
		t.wait()
	}

	mut ok_count := 0
	for _ in 0 .. num_threads {
		msg := <-ch
		if msg == 'ok' {
			ok_count++
		}
	}
	assert ok_count == num_threads, 'all ${num_threads} concurrent ops should succeed, got ${ok_count}'
}
