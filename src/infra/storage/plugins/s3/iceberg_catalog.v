// Provides Iceberg catalog integration and table management.
// Supported catalogs: Hadoop (file-based), Glue, REST
module s3

import time
import json
import strings

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

/// HadoopCatalog is an S3-based Hadoop catalog implementation.
pub struct HadoopCatalog {
pub mut:
	adapter    &S3StorageAdapter
	warehouse  string
	properties map[string]string
}

/// new_hadoop_catalog creates a new Hadoop catalog.
pub fn new_hadoop_catalog(adapter &S3StorageAdapter, warehouse string) &HadoopCatalog {
	return &HadoopCatalog{
		adapter:    adapter
		warehouse:  normalize_s3_location(warehouse)
		properties: {}
	}
}

/// create_table creates a new Iceberg table.
pub fn (mut c HadoopCatalog) create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata {
	// Generate table path
	table_path := c.table_path(identifier)

	// Check if table already exists
	if c.table_exists(identifier) {
		return error('Table already exists: ${identifier.name}')
	}

	// Generate table UUID deterministically from location so the same table
	// recreated at the same path always yields the same UUID.
	table_uuid := generate_table_uuid(location)
	now := time.now()

	// Create initial metadata
	mut metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          table_uuid
		location:            location
		last_updated_ms:     now.unix_milli()
		schemas:             [schema]
		current_schema_id:   0
		partition_specs:     [spec]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'created_by': 'DataCore HadoopCatalog'
		}
	}

	// Metadata file path
	metadata_path := '${table_path}/metadata/00001-${table_uuid}.metadata.json'

	// Generate and save metadata JSON
	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	// Create version hint file (tracks current metadata version)
	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, '1'.bytes())!

	return metadata
}

/// load_table loads an existing Iceberg table.
pub fn (mut c HadoopCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	table_path := c.table_path(identifier)

	// Fetch version hint file (existence check; actual value unused — latest file is found by listing)
	version_hint_path := '${table_path}/metadata/version-hint.text'
	_, _ := c.adapter.get_object(version_hint_path, -1, -1) or {
		return error('Table not found: ${identifier.name}')
	}

	// Fetch latest metadata file
	// Metadata file pattern: {version:05d}-{uuid}.metadata.json
	prefix := '${table_path}/metadata/'
	objects := c.adapter.list_objects(prefix) or { return error('Failed to list metadata files') }

	// Find the latest version metadata file
	mut latest_metadata_path := ''
	mut latest_version := 0

	for obj in objects {
		filename := obj.key.split('/').last()
		if filename.ends_with('.metadata.json') {
			// Extract version from filename (e.g., 00001-uuid.metadata.json -> 1)
			version_str := filename[0..5]
			file_version := version_str.int()
			if file_version > latest_version {
				latest_version = file_version
				latest_metadata_path = obj.key
			}
		}
	}

	if latest_metadata_path == '' {
		return error('No metadata file found for table: ${identifier.name}')
	}

	// Load and parse metadata file
	metadata_data, _ := c.adapter.get_object(latest_metadata_path, -1, -1)!
	return c.decode_metadata(metadata_data.bytestr())!
}

/// update_table updates the table metadata.
pub fn (mut c HadoopCatalog) update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	table_path := c.table_path(identifier)

	// Optimistic concurrency control: create new version metadata file
	new_version := metadata.snapshots.len
	metadata_path := '${table_path}/metadata/${new_version:05d}-${metadata.table_uuid}.metadata.json'

	// Save metadata
	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	// Update version hint
	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, new_version.str().bytes())!
}

/// drop_table drops the table.
pub fn (mut c HadoopCatalog) drop_table(identifier IcebergTableIdentifier) ! {
	table_path := c.table_path(identifier)

	// Check table existence
	if !c.table_exists(identifier) {
		return error('Table not found: ${identifier.name}')
	}

	// Delete all objects in the table
	prefix := '${table_path}/'
	c.adapter.delete_objects_with_prefix(prefix)!
}

/// list_tables lists all tables in the namespace.
pub fn (mut c HadoopCatalog) list_tables(namespace []string) ![]IcebergTableIdentifier {
	mut identifiers := []IcebergTableIdentifier{}

	// Generate namespace path
	ns_path := c.namespace_path(namespace)
	prefix := '${ns_path}/'

	// Fetch list of all objects in the namespace
	objects := c.adapter.list_objects(prefix) or { return []IcebergTableIdentifier{} }

	// Collect table list (directories with a metadata folder)
	mut seen_tables := map[string]bool{}
	for obj in objects {
		parts := obj.key.split('/')
		if parts.len > namespace.len + 1 {
			// Table name is the first folder after the namespace
			table_name := parts[namespace.len]
			if table_name !in seen_tables && table_name != '' {
				seen_tables[table_name] = true
				identifiers << IcebergTableIdentifier{
					namespace: namespace
					name:      table_name
				}
			}
		}
	}

	return identifiers
}

/// namespace_exists checks whether the namespace exists.
pub fn (mut c HadoopCatalog) namespace_exists(namespace []string) bool {
	if namespace.len == 0 {
		return true
	}

	ns_path := c.namespace_path(namespace)
	marker := '${ns_path}/.namespace'

	// Check for existence of namespace marker file
	_, _ := c.adapter.get_object(marker, -1, -1) or { return false }
	return true
}

/// create_namespace creates a new namespace.
pub fn (mut c HadoopCatalog) create_namespace(namespace []string) ! {
	if namespace.len == 0 {
		return
	}

	ns_path := c.namespace_path(namespace)
	marker := '${ns_path}/.namespace'

	// Create namespace marker file
	c.adapter.put_object(marker, '{}'.bytes())!
}

/// load_metadata_at loads Iceberg metadata from a specific metadata file path.
pub fn (mut c HadoopCatalog) load_metadata_at(metadata_location string) !IcebergMetadata {
	data, _ := c.adapter.get_object(metadata_location, -1, -1) or {
		return error('Failed to read metadata at ${metadata_location}: ${err}')
	}
	return c.decode_metadata(data.bytestr())
}

/// commit_metadata writes the given metadata as a new versioned metadata JSON file to S3
/// and updates the version-hint.text pointer.
pub fn (mut c HadoopCatalog) commit_metadata(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	table_path := c.table_path(identifier)

	// Derive next version number from existing snapshot count (same convention as update_table)
	new_version := metadata.snapshots.len + 1
	metadata_path := '${table_path}/metadata/${new_version:05d}-${metadata.table_uuid}.metadata.json'

	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, new_version.str().bytes())!
}

fn (mut c HadoopCatalog) table_exists(identifier IcebergTableIdentifier) bool {
	table_path := c.table_path(identifier)
	metadata_path := '${table_path}/metadata/'

	objects := c.adapter.list_objects(metadata_path) or { return false }
	return objects.len > 0
}

/// table_path returns the S3 path for the table.
fn (mut c HadoopCatalog) table_path(identifier IcebergTableIdentifier) string {
	ns_path := c.namespace_path(identifier.namespace)
	return '${ns_path}/${identifier.name}'
}

/// namespace_path returns the S3 path for the namespace.
fn (mut c HadoopCatalog) namespace_path(namespace []string) string {
	if namespace.len == 0 {
		return c.warehouse
	}
	return '${c.warehouse}/${namespace.join('/')}'
}

/// encode_metadata encodes metadata as a JSON string.
fn encode_metadata(metadata IcebergMetadata) string {
	// Estimate initial capacity: base fields + per-schema/snapshot overhead
	estimated_size := 512 + metadata.schemas.len * 256 + metadata.partition_specs.len * 128 +
		metadata.snapshots.len * 512
	mut sb := strings.new_builder(estimated_size)

	sb.write_string('{')
	sb.write_string('"formatVersion":${metadata.format_version},')
	sb.write_string('"tableUuid":"${metadata.table_uuid}",')
	sb.write_string('"location":"${metadata.location}",')
	sb.write_string('"lastUpdatedMs":${metadata.last_updated_ms},')
	sb.write_string('"currentSchemaId":${metadata.current_schema_id},')
	sb.write_string('"defaultSpecId":${metadata.default_spec_id},')
	sb.write_string('"currentSnapshotId":${metadata.current_snapshot_id},')

	// properties
	sb.write_string('"properties":${json.encode(metadata.properties)},')

	// schemas
	sb.write_string('"schemas":[')
	for i, schema in metadata.schemas {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"schemaId":${schema.schema_id},"fields":[')
		for j, field in schema.fields {
			if j > 0 {
				sb.write_string(',')
			}
			sb.write_string('{"id":${field.id},')
			sb.write_string('"name":"${field.name}",')
			sb.write_string('"type":"${field.typ}",')
			sb.write_string('"required":${field.required}}')
		}
		sb.write_string(']}')
	}
	sb.write_string('],')

	// partitionSpecs
	sb.write_string('"partitionSpecs":[')
	for i, spec in metadata.partition_specs {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"specId":${spec.spec_id},"fields":[')
		for j, field in spec.fields {
			if j > 0 {
				sb.write_string(',')
			}
			sb.write_string('{"sourceId":${field.source_id},')
			sb.write_string('"fieldId":${field.field_id},')
			sb.write_string('"name":"${field.name}",')
			sb.write_string('"transform":"${field.transform}"}')
		}
		sb.write_string(']}')
	}
	sb.write_string('],')

	// snapshots
	sb.write_string('"snapshots":[')
	for i, snapshot in metadata.snapshots {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"snapshotId":${snapshot.snapshot_id},')
		sb.write_string('"timestampMs":${snapshot.timestamp_ms},')
		sb.write_string('"manifestList":"${snapshot.manifest_list}",')
		sb.write_string('"schemaId":${snapshot.schema_id},')
		sb.write_string('"summary":${json.encode(snapshot.summary)}}')
	}
	sb.write_string(']')

	sb.write_string('}')

	return sb.str()
}

/// decode_metadata decodes a JSON string into IcebergMetadata.
/// Parses all fields: format_version, table_uuid, location, schemas, partition_specs, snapshots.
fn (mut c HadoopCatalog) decode_metadata(json_str string) !IcebergMetadata {
	if json_str == '' {
		return error('Empty metadata JSON')
	}

	mut metadata := IcebergMetadata{}

	// format-version / formatVersion
	metadata.format_version = json_extract_int_dual(json_str, 'format-version', 'formatVersion') or {
		2
	}

	// table-uuid / tableUuid
	metadata.table_uuid = json_extract_string_dual(json_str, 'table-uuid', 'tableUuid') or {
		generate_table_uuid(json_str)
	}

	// location
	if v := json_extract_string(json_str, 'location') {
		metadata.location = v
	}

	// last-updated-ms / lastUpdatedMs
	metadata.last_updated_ms = json_extract_i64_dual(json_str, 'last-updated-ms', 'lastUpdatedMs') or {
		time.now().unix_milli()
	}

	// current-schema-id / currentSchemaId
	metadata.current_schema_id = json_extract_int_dual(json_str, 'current-schema-id',
		'currentSchemaId') or { 0 }

	// default-spec-id / defaultSpecId
	metadata.default_spec_id = json_extract_int_dual(json_str, 'default-spec-id', 'defaultSpecId') or {
		0
	}

	// current-snapshot-id / currentSnapshotId
	metadata.current_snapshot_id = json_extract_i64_dual(json_str, 'current-snapshot-id',
		'currentSnapshotId') or { i64(0) }

	// properties
	if props_str := json_extract_object(json_str, 'properties') {
		metadata.properties = json_parse_string_map(props_str)
	}

	// schemas
	if schemas_str := json_extract_array(json_str, 'schemas') {
		metadata.schemas = json_parse_schemas(schemas_str)
	}

	// partition-specs / partitionSpecs
	if specs_str := json_extract_array_dual(json_str, 'partition-specs', 'partitionSpecs') {
		metadata.partition_specs = json_parse_partition_specs(specs_str)
	}

	// snapshots
	if snaps_str := json_extract_array(json_str, 'snapshots') {
		metadata.snapshots = json_parse_snapshots(snaps_str)
	}

	return metadata
}
