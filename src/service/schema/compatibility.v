// Service Layer - Schema Compatibility Checking
// Provides compatibility checking for Avro, JSON Schema, and Protobuf schemas
module schema

import domain

// ============================================================================
// Main Compatibility Functions
// ============================================================================

// check_backward_compatible checks if new schema can read data written with old schema
fn check_backward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_backward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_backward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_backward_compatible(old_schema, new_schema)
		}
	}
}

// check_forward_compatible checks if old schema can read data written with new schema
fn check_forward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_forward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_forward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_forward_compatible(old_schema, new_schema)
		}
	}
}
