// Confluent wire format helpers and schema encode/decode dispatch.
//
// Implements the standard Confluent Schema Registry wire format:
//   [0x00] + [4-byte big-endian schema ID] + [encoded payload]
//
// Encoder instances are created by schema_encoding_factory.v (DIP isolation).
module kafka

import domain
import service.port
import sync

// confluent_wire_magic is the magic byte for Confluent wire format.
const confluent_wire_magic = u8(0x00)
// confluent_wire_header_size is the fixed header size (1 magic + 4 ID bytes).
const confluent_wire_header_size = 5

/// wrap_confluent_wire_format wraps an encoded payload with Confluent wire format header.
fn wrap_confluent_wire_format(payload []u8, schema_id int) []u8 {
	mut buf := []u8{cap: confluent_wire_header_size + payload.len}
	buf << confluent_wire_magic
	buf << u8(u32(schema_id) >> 24)
	buf << u8(u32(schema_id) >> 16)
	buf << u8(u32(schema_id) >> 8)
	buf << u8(schema_id)
	buf << payload
	return buf
}

/// unwrap_confluent_wire_format validates and strips the Confluent wire format header.
/// Returns the extracted schema ID and the raw payload.
fn unwrap_confluent_wire_format(data []u8) !(int, []u8) {
	if data.len < confluent_wire_header_size {
		return error('confluent wire format: data too short (${data.len} bytes, need >= ${confluent_wire_header_size})')
	}
	if data[0] != confluent_wire_magic {
		return error('confluent wire format: invalid magic byte 0x${data[0]:02x}, expected 0x00')
	}
	sid := int(u32(data[1]) << 24 | u32(data[2]) << 16 | u32(data[3]) << 8 | u32(data[4]))
	return sid, data[confluent_wire_header_size..]
}

struct EncoderCacheHolder {
mut:
	mu               sync.Mutex
	avro_encoder     ?port.SchemaEncoderPort
	json_encoder     ?port.SchemaEncoderPort
	protobuf_encoder ?port.SchemaEncoderPort
	cached           bool
}

const g_encoder_cache_holder = &EncoderCacheHolder{}

/// ensure_encoders_cached initializes all encoder caches on first use.
/// Mutex guarantees only one initialization under concurrent access.
fn ensure_encoders_cached() ! {
	mut holder := unsafe { g_encoder_cache_holder }
	holder.mu.@lock()
	if holder.cached {
		holder.mu.unlock()
		return
	}
	unsafe {
		holder.avro_encoder = new_avro_encoder_port() or {
			holder.mu.unlock()
			return err
		}
		holder.json_encoder = new_json_encoder_port() or {
			holder.mu.unlock()
			return err
		}
		holder.protobuf_encoder = new_protobuf_encoder_port() or {
			holder.mu.unlock()
			return err
		}
		holder.cached = true
	}
	holder.mu.unlock()
}

/// get_or_create_avro_encoder returns the cached Avro encoder port.
fn get_or_create_avro_encoder() !port.SchemaEncoderPort {
	ensure_encoders_cached()!
	holder := unsafe { g_encoder_cache_holder }
	return holder.avro_encoder or { return error('avro encoder not initialized') }
}

/// get_or_create_json_encoder returns the cached JSON encoder port.
fn get_or_create_json_encoder() !port.SchemaEncoderPort {
	ensure_encoders_cached()!
	holder := unsafe { g_encoder_cache_holder }
	return holder.json_encoder or { return error('json encoder not initialized') }
}

/// get_or_create_protobuf_encoder returns the cached Protobuf encoder port.
fn get_or_create_protobuf_encoder() !port.SchemaEncoderPort {
	ensure_encoders_cached()!
	holder := unsafe { g_encoder_cache_holder }
	return holder.protobuf_encoder or { return error('protobuf encoder not initialized') }
}

/// encode_with_schema dispatches encode to the correct encoder via port interface.
fn encode_with_schema(data []u8, schema_str string, schema_type domain.SchemaType) ![]u8 {
	return match schema_type {
		.avro {
			mut enc := get_or_create_avro_encoder()!
			enc.encode(data, schema_str)!
		}
		.json {
			mut enc := get_or_create_json_encoder()!
			enc.encode(data, schema_str)!
		}
		.protobuf {
			mut enc := get_or_create_protobuf_encoder()!
			enc.encode(data, schema_str)!
		}
	}
}

/// decode_with_schema dispatches decode to the correct encoder via port interface.
fn decode_with_schema(data []u8, schema_str string, schema_type domain.SchemaType) ![]u8 {
	return match schema_type {
		.avro {
			mut enc := get_or_create_avro_encoder()!
			enc.decode(data, schema_str)!
		}
		.json {
			mut enc := get_or_create_json_encoder()!
			enc.decode(data, schema_str)!
		}
		.protobuf {
			mut enc := get_or_create_protobuf_encoder()!
			enc.decode(data, schema_str)!
		}
	}
}
