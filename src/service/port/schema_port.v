// Schema registry interface following DIP.
// Abstracts schema lookup from the concrete SchemaRegistry.
module port

import domain

/// SchemaRegistryPort abstracts schema retrieval for encode/decode operations.
/// Minimal interface for consumers that only need schema lookup (ISP).
pub interface SchemaRegistryPort {
mut:
	/// Retrieves a schema by subject name and version.
	get_schema_by_subject(subject string, version int) !domain.Schema
}

/// SchemaRegistryRestPort abstracts the full schema registry API surface
/// needed by the REST interface layer. Separated from SchemaRegistryPort
/// to follow ISP: REST handlers need the full management API,
/// while encode/decode consumers only need schema lookup.
pub interface SchemaRegistryRestPort {
mut:
	// Subject management
	list_subjects() []string
	list_versions(subject string) ![]int
	delete_subject(subject string) ![]int

	// Schema registration and retrieval
	register(subject string, schema_str string, schema_type domain.SchemaType) !int
	get_schema_by_subject(subject string, version int) !domain.Schema
	get_schema(schema_id int) !domain.Schema
	delete_version(subject string, version int) !int

	// Configuration management
	get_global_config() domain.SubjectConfig
	set_global_config(config domain.SubjectConfig)
	get_compatibility(subject string) domain.CompatibilityLevel
	set_compatibility(subject string, level domain.CompatibilityLevel)

	// Compatibility checking
	test_compatibility(subject string, schema_str string, schema_type domain.SchemaType) !bool
}
