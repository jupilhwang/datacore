// Schema registry interface following DIP.
// Abstracts schema lookup from the concrete SchemaRegistry.
module port

import domain

/// SchemaRegistryPort abstracts schema retrieval for encode/decode operations.
/// Implemented by service/schema SchemaRegistry.
pub interface SchemaRegistryPort {
mut:
	/// Retrieves a schema by subject name and version.
	get_schema_by_subject(subject string, version int) !domain.Schema
}
