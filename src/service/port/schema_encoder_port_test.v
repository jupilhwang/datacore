// Unit tests for SchemaEncoderPort interface compliance.
// Verifies the port can be implemented by any conforming struct (DIP).
module port

// MockSchemaEncoder is a test double that implements SchemaEncoderPort.
struct MockSchemaEncoder {
mut:
	prefix []u8
}

fn (mut e MockSchemaEncoder) encode(data []u8, schema_str string) ![]u8 {
	mut result := e.prefix.clone()
	result << data
	return result
}

fn (mut e MockSchemaEncoder) decode(data []u8, schema_str string) ![]u8 {
	if data.len < e.prefix.len {
		return error('data too short to strip prefix')
	}
	return data[e.prefix.len..]
}

fn test_schema_encoder_port_encode_dispatches_through_interface() {
	mut enc := SchemaEncoderPort(MockSchemaEncoder{
		prefix: [u8(0xAA)]
	})
	result := enc.encode([u8(1), 2, 3], '{}')!
	assert result == [u8(0xAA), 1, 2, 3]
}

fn test_schema_encoder_port_decode_dispatches_through_interface() {
	mut enc := SchemaEncoderPort(MockSchemaEncoder{
		prefix: [u8(0xAA)]
	})
	result := enc.decode([u8(0xAA), 4, 5], '{}')!
	assert result == [u8(4), 5]
}

fn test_schema_encoder_port_encode_error_propagates() {
	mut enc := SchemaEncoderPort(MockSchemaEncoder{
		prefix: [u8(0xBB), 0xCC]
	})
	// decode with data shorter than prefix triggers error
	enc.decode([u8(0x01)], '{}') or {
		assert err.msg().contains('too short')
		return
	}
	assert false, 'expected error from decode with short data'
}
