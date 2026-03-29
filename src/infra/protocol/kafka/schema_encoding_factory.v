// Factory functions for creating schema encoder instances.
//
// Separated from schema_encoding.v to isolate concrete service.schema imports.
// Follows the same pattern as handler_factory.v: concrete construction
// is confined here while schema_encoding.v depends only on port interfaces.
module kafka

import service.schema
import service.port

/// new_avro_encoder_port creates a new Avro encoder as SchemaEncoderPort.
fn new_avro_encoder_port() !port.SchemaEncoderPort {
	enc := schema.new_avro_encoder()!
	return enc
}

/// new_json_encoder_port creates a new JSON encoder as SchemaEncoderPort.
fn new_json_encoder_port() !port.SchemaEncoderPort {
	enc := schema.new_json_encoder()!
	return enc
}

/// new_protobuf_encoder_port creates a new Protobuf encoder as SchemaEncoderPort.
fn new_protobuf_encoder_port() !port.SchemaEncoderPort {
	enc := schema.new_protobuf_encoder()!
	return enc
}
