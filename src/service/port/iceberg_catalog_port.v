/// Port interface and domain types for Iceberg table catalog operations.
/// These types are shared between the interface, service, and infra layers.
module port

import time
import crypto.md5

/// IcebergCatalog defines the Iceberg table catalog interface.
pub interface IcebergCatalog {
mut:
	create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata
	load_table(identifier IcebergTableIdentifier) !IcebergMetadata
	load_metadata_at(metadata_location string) !IcebergMetadata
	update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) !
	drop_table(identifier IcebergTableIdentifier) !
	list_tables(namespace []string) ![]IcebergTableIdentifier
	namespace_exists(namespace []string) bool
	create_namespace(namespace []string) !
}

/// IcebergTableIdentifier represents a table identifier.
/// Namespace: namespace (database)
/// Name: table name
pub struct IcebergTableIdentifier {
pub:
	namespace []string
	name      string
}

/// IcebergSchema represents an Iceberg table schema.
/// Fields: list of schema fields
/// SchemaId: unique schema ID
/// IdentifierFieldIds: list of identifier field IDs
pub struct IcebergSchema {
pub mut:
	schema_id            int @[json: 'schema-id']
	fields               []IcebergField
	identifier_field_ids []int @[json: 'identifier-field-ids']
}

/// IcebergField represents an Iceberg schema field.
/// Id: field ID
/// Name: field name
/// Type: field type (string, int, long, double, boolean, binary, timestamp, timestamp_ns, unknown, variant, geometry, geography)
/// Required: whether the field is required
/// DefaultValue: default value (v3 support)
pub struct IcebergField {
pub mut:
	id            int
	name          string
	typ           string @[json: 'type']
	required      bool
	default_value string @[json: 'initial-default']
}

/// IcebergPartitionSpec represents a partition specification.
/// SpecId: specification ID
/// Fields: list of partition fields
pub struct IcebergPartitionSpec {
pub mut:
	spec_id int @[json: 'spec-id']
	fields  []IcebergPartitionField
}

/// IcebergPartitionField represents a partition field.
/// SourceId: source column ID
/// FieldId: partition field ID
/// Name: partition name
/// Transform: transform function (identity, bucket[N], truncate[N], year, month, day, hour)
/// TransformArgs: list of transform arguments (v3: multi-argument transforms support)
pub struct IcebergPartitionField {
pub mut:
	source_id      int @[json: 'source-id']
	field_id       int @[json: 'field-id']
	name           string
	transform      string
	transform_args []string @[json: 'transform-args']
}

/// IcebergSnapshot represents a table snapshot.
/// SnapshotId: unique snapshot ID
/// TimestampMs: snapshot creation time (milliseconds)
/// ManifestList: manifest list file path
/// SchemaId: schema ID
/// Summary: snapshot summary information
pub struct IcebergSnapshot {
pub mut:
	snapshot_id   i64    @[json: 'snapshot-id']
	timestamp_ms  i64    @[json: 'timestamp-ms']
	manifest_list string @[json: 'manifest-list']
	schema_id     int    @[json: 'schema-id']
	summary       map[string]string
}

/// IcebergMetadata represents table metadata.
/// FormatVersion: Iceberg format version (1, 2, or 3)
/// TableUuid: table UUID
/// Location: table location (S3 path)
/// LastUpdatedMs: last update time (milliseconds)
/// Schemas: list of schemas
/// CurrentSchemaId: current schema ID
/// PartitionSpecs: list of partition specifications
/// DefaultSpecId: default partition specification ID
/// Snapshots: list of snapshots
/// CurrentSnapshotId: current snapshot ID
/// Properties: table properties
pub struct IcebergMetadata {
pub mut:
	format_version      int    @[json: 'format-version']
	table_uuid          string @[json: 'table-uuid']
	location            string
	last_updated_ms     i64 @[json: 'last-updated-ms']
	schemas             []IcebergSchema
	current_schema_id   int                    @[json: 'current-schema-id']
	partition_specs     []IcebergPartitionSpec @[json: 'partition-specs']
	default_spec_id     int                    @[json: 'default-spec-id']
	snapshots           []IcebergSnapshot
	current_snapshot_id i64 @[json: 'current-snapshot-id']
	properties          map[string]string
}

/// generate_table_uuid generates a UUID for a new table.
/// seed: deterministic input (e.g. table location or name) for reproducible UUIDs.
///       Pass '' to fall back to a timestamp-based value.
pub fn generate_table_uuid(seed string) string {
	input := if seed != '' { seed } else { time.now().str() }
	hash := md5.sum(input.bytes())
	return '${hash[0..4].hex()}-${hash[4..6].hex()}-${hash[6..8].hex()}-${hash[8..10].hex()}-${hash[10..16].hex()}'
}

/// create_default_schema creates the default Kafka record schema.
pub fn create_default_schema() IcebergSchema {
	return IcebergSchema{
		schema_id:            0
		fields:               [
			IcebergField{
				id:       1
				name:     'offset'
				typ:      'long'
				required: true
			},
			IcebergField{
				id:       2
				name:     'timestamp'
				typ:      'timestamp'
				required: true
			},
			IcebergField{
				id:       3
				name:     'topic'
				typ:      'string'
				required: true
			},
			IcebergField{
				id:       4
				name:     'partition'
				typ:      'int'
				required: true
			},
			IcebergField{
				id:       5
				name:     'key'
				typ:      'binary'
				required: false
			},
			IcebergField{
				id:       6
				name:     'value'
				typ:      'binary'
				required: false
			},
			IcebergField{
				id:       7
				name:     'headers'
				typ:      'string'
				required: false
			},
		]
		identifier_field_ids: [1]
	}
}

/// create_default_partition_spec creates the default partition specification.
pub fn create_default_partition_spec() IcebergPartitionSpec {
	return IcebergPartitionSpec{
		spec_id: 0
		fields:  [
			IcebergPartitionField{
				source_id: 2
				field_id:  1000
				name:      'timestamp_day'
				transform: 'day'
			},
			IcebergPartitionField{
				source_id: 3
				field_id:  1001
				name:      'topic'
				transform: 'identity'
			},
		]
	}
}
