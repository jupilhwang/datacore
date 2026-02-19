// Supports registration, versioning, and compatibility checking of message schemas.
module domain

import time

/// SchemaType represents the type of a schema.
/// avro: Apache Avro schema
/// json: JSON Schema
/// protobuf: Protocol Buffers schema
pub enum SchemaType {
	avro
	json
	protobuf
}

/// Schema represents a registered schema.
/// id: globally unique schema ID
/// schema_type: schema type (AVRO, JSON, PROTOBUF)
/// schema_str: raw schema definition string
/// references: references to other schemas
/// fingerprint: schema fingerprint for deduplication
pub struct Schema {
pub:
	id          int
	schema_type SchemaType
	schema_str  string
	references  []SchemaReference
	fingerprint string
}

/// SchemaReference represents a reference to another schema.
/// name: reference name
/// subject: subject of the referenced schema
/// version: version of the referenced schema
pub struct SchemaReference {
pub:
	name    string
	subject string
	version int
}

/// SchemaVersion represents a schema version under a subject.
/// version: version number (starting from 1)
/// schema_id: global schema ID
/// subject: subject name (e.g. "orders-value")
/// compatibility: compatibility level
/// created_at: creation time
pub struct SchemaVersion {
pub:
	version       int
	schema_id     int
	subject       string
	compatibility CompatibilityLevel
	created_at    time.Time
}

/// CompatibilityLevel defines schema compatibility rules.
/// none: no compatibility check
/// backward: new schema can read data written by old schema
/// forward: old schema can read data written by new schema
/// full: bidirectional compatibility
pub enum CompatibilityLevel {
	none
	backward
	backward_transitive
	forward
	forward_transitive
	full
	full_transitive
}

/// SubjectConfig represents configuration for a subject.
/// compatibility: compatibility level
/// alias: subject alias
/// normalize: whether to normalize the schema before comparison
pub struct SubjectConfig {
pub:
	compatibility CompatibilityLevel = .backward
	alias         string
	normalize     bool
}

/// SchemaInfo represents schema information for API responses.
pub struct SchemaInfo {
pub:
	id          int
	schema_type string
	schema_str  string
	subject     string
	version     int
	created_at  i64
}

/// SubjectVersion represents a subject and its list of versions.
pub struct SubjectVersion {
pub:
	subject  string
	versions []int
}

// Default compatibility level for new subjects
/// default_compatibility constant.
pub const default_compatibility = CompatibilityLevel.backward

/// str converts SchemaType to a string.
pub fn (st SchemaType) str() string {
	return match st {
		.avro { 'AVRO' }
		.json { 'JSON' }
		.protobuf { 'PROTOBUF' }
	}
}

/// schema_type_from_str converts a string to a SchemaType.
pub fn schema_type_from_str(s string) !SchemaType {
	return match s.to_upper() {
		'AVRO' { .avro }
		'JSON' { .json }
		'PROTOBUF' { .protobuf }
		else { error('unknown schema type: ${s}') }
	}
}

/// str converts CompatibilityLevel to a string.
pub fn (cl CompatibilityLevel) str() string {
	return match cl {
		.none { 'NONE' }
		.backward { 'BACKWARD' }
		.backward_transitive { 'BACKWARD_TRANSITIVE' }
		.forward { 'FORWARD' }
		.forward_transitive { 'FORWARD_TRANSITIVE' }
		.full { 'FULL' }
		.full_transitive { 'FULL_TRANSITIVE' }
	}
}

/// compatibility_from_str converts a string to a CompatibilityLevel.
pub fn compatibility_from_str(s string) !CompatibilityLevel {
	return match s.to_upper() {
		'NONE' { .none }
		'BACKWARD' { .backward }
		'BACKWARD_TRANSITIVE' { .backward_transitive }
		'FORWARD' { .forward }
		'FORWARD_TRANSITIVE' { .forward_transitive }
		'FULL' { .full }
		'FULL_TRANSITIVE' { .full_transitive }
		else { error('unknown compatibility level: ${s}') }
	}
}
