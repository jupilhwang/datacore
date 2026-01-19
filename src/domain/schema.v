// Domain Layer - Schema Domain Model
// Schema Registry domain entities for managing message schemas
module domain

import time

// SchemaType represents the type of schema
pub enum SchemaType {
	avro
	json
	protobuf
}

// Schema represents a registered schema
pub struct Schema {
pub:
	id          int               // Global unique schema ID
	schema_type SchemaType        // AVRO, JSON, PROTOBUF
	schema_str  string            // Raw schema definition
	references  []SchemaReference // References to other schemas
	fingerprint string            // Schema fingerprint for deduplication
}

// SchemaReference represents a reference to another schema
pub struct SchemaReference {
pub:
	name    string // Reference name
	subject string // Subject of the referenced schema
	version int    // Version of the referenced schema
}

// SchemaVersion represents a version of a schema under a subject
pub struct SchemaVersion {
pub:
	version       int    // Version number (1-based)
	schema_id     int    // Global schema ID
	subject       string // Subject name (e.g., "orders-value")
	compatibility CompatibilityLevel
	created_at    time.Time
}

// CompatibilityLevel defines schema compatibility rules
pub enum CompatibilityLevel {
	none     // No compatibility checks
	backward // New schema can read old data
	backward_transitive
	forward // Old schema can read new data
	forward_transitive
	full // Both backward and forward compatible
	full_transitive
}

// SubjectConfig represents configuration for a subject
pub struct SubjectConfig {
pub:
	compatibility CompatibilityLevel = .backward
	alias         string // Subject alias
	normalize     bool   // Normalize schema before comparing
}

// SchemaInfo represents schema information for API responses
pub struct SchemaInfo {
pub:
	id          int
	schema_type string
	schema_str  string
	subject     string
	version     int
	created_at  i64 // Unix timestamp
}

// SubjectVersion represents a subject with its versions
pub struct SubjectVersion {
pub:
	subject  string
	versions []int
}

// Default compatibility level for new subjects
pub const default_compatibility = CompatibilityLevel.backward

// Schema type string conversions
pub fn (st SchemaType) str() string {
	return match st {
		.avro { 'AVRO' }
		.json { 'JSON' }
		.protobuf { 'PROTOBUF' }
	}
}

pub fn schema_type_from_str(s string) !SchemaType {
	return match s.to_upper() {
		'AVRO' { .avro }
		'JSON' { .json }
		'PROTOBUF' { .protobuf }
		else { error('unknown schema type: ${s}') }
	}
}

// Compatibility level string conversions
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
