// Provides Iceberg catalog integration and table management.
// Supported catalogs: Hadoop (file-based), Glue, REST
module s3

import time
import json
import strings
import net.http
import crypto.hmac
import crypto.sha256
import encoding.base64

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
		warehouse:  warehouse
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

	// Generate table UUID
	table_uuid := generate_table_uuid()
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
	metadata_json := c.encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	// Create version hint file (tracks current metadata version)
	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, '1'.bytes())!

	return metadata
}

/// load_table loads an existing Iceberg table.
pub fn (mut c HadoopCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	table_path := c.table_path(identifier)

	// Fetch version hint file
	version_hint_path := '${table_path}/metadata/version-hint.text'
	version_data, _ := c.adapter.get_object(version_hint_path, -1, -1) or {
		return error('Table not found: ${identifier.name}')
	}
	version := version_data.bytestr().int()
	_ = version

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
	metadata_json := c.encode_metadata(metadata)
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
fn (mut c HadoopCatalog) encode_metadata(metadata IcebergMetadata) string {
	// Iceberg metadata JSON format - build string directly
	mut sb := strings.new_builder(4096)

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
	if json_str.len == 0 {
		return error('Empty metadata JSON')
	}

	mut metadata := IcebergMetadata{}

	// format-version / formatVersion
	if v := json_extract_int(json_str, 'format-version') {
		metadata.format_version = v
	} else if v2 := json_extract_int(json_str, 'formatVersion') {
		metadata.format_version = v2
	} else {
		metadata.format_version = 2
	}

	// table-uuid / tableUuid
	if v := json_extract_string(json_str, 'table-uuid') {
		metadata.table_uuid = v
	} else if v2 := json_extract_string(json_str, 'tableUuid') {
		metadata.table_uuid = v2
	} else {
		metadata.table_uuid = generate_table_uuid()
	}

	// location
	if v := json_extract_string(json_str, 'location') {
		metadata.location = v
	}

	// last-updated-ms / lastUpdatedMs
	if v := json_extract_i64(json_str, 'last-updated-ms') {
		metadata.last_updated_ms = v
	} else if v2 := json_extract_i64(json_str, 'lastUpdatedMs') {
		metadata.last_updated_ms = v2
	} else {
		metadata.last_updated_ms = time.now().unix_milli()
	}

	// current-schema-id / currentSchemaId
	if v := json_extract_int(json_str, 'current-schema-id') {
		metadata.current_schema_id = v
	} else if v2 := json_extract_int(json_str, 'currentSchemaId') {
		metadata.current_schema_id = v2
	}

	// default-spec-id / defaultSpecId
	if v := json_extract_int(json_str, 'default-spec-id') {
		metadata.default_spec_id = v
	} else if v2 := json_extract_int(json_str, 'defaultSpecId') {
		metadata.default_spec_id = v2
	}

	// current-snapshot-id / currentSnapshotId
	if v := json_extract_i64(json_str, 'current-snapshot-id') {
		metadata.current_snapshot_id = v
	} else if v2 := json_extract_i64(json_str, 'currentSnapshotId') {
		metadata.current_snapshot_id = v2
	}

	// properties
	if props_str := json_extract_object(json_str, 'properties') {
		metadata.properties = json_parse_string_map(props_str)
	}

	// schemas
	if schemas_str := json_extract_array(json_str, 'schemas') {
		metadata.schemas = json_parse_schemas(schemas_str)
	}

	// partition-specs / partitionSpecs
	if specs_str := json_extract_array(json_str, 'partition-specs') {
		metadata.partition_specs = json_parse_partition_specs(specs_str)
	} else if specs_str2 := json_extract_array(json_str, 'partitionSpecs') {
		metadata.partition_specs = json_parse_partition_specs(specs_str2)
	}

	// snapshots
	if snaps_str := json_extract_array(json_str, 'snapshots') {
		metadata.snapshots = json_parse_snapshots(snaps_str)
	}

	return metadata
}

// --- JSON parsing helpers ---

/// json_extract_string extracts a string value by key from a flat JSON object.
fn json_extract_string(json_str string, key string) ?string {
	needle := '"${key}"'
	idx := json_str.index(needle) or { return none }
	rest := json_str[idx + needle.len..]
	colon_idx := rest.index(':') or { return none }
	after_colon := rest[colon_idx + 1..].trim_space()

	if after_colon.starts_with('"') {
		// String value
		end := after_colon[1..].index('"') or { return none }
		return after_colon[1..end + 1]
	}
	return none
}

/// json_extract_int extracts an integer value by key from a flat JSON object.
fn json_extract_int(json_str string, key string) ?int {
	needle := '"${key}"'
	idx := json_str.index(needle) or { return none }
	rest := json_str[idx + needle.len..]
	colon_idx := rest.index(':') or { return none }
	after_colon := rest[colon_idx + 1..].trim_space()

	// Read until non-numeric
	mut end := 0
	for end < after_colon.len && (after_colon[end] >= `0` && after_colon[end] <= `9`
		|| (end == 0 && after_colon[end] == `-`)) {
		end++
	}
	if end == 0 {
		return none
	}
	return after_colon[0..end].int()
}

/// json_extract_i64 extracts an i64 value by key from a flat JSON object.
fn json_extract_i64(json_str string, key string) ?i64 {
	needle := '"${key}"'
	idx := json_str.index(needle) or { return none }
	rest := json_str[idx + needle.len..]
	colon_idx := rest.index(':') or { return none }
	after_colon := rest[colon_idx + 1..].trim_space()

	mut end := 0
	for end < after_colon.len && (after_colon[end] >= `0` && after_colon[end] <= `9`
		|| (end == 0 && after_colon[end] == `-`)) {
		end++
	}
	if end == 0 {
		return none
	}
	return after_colon[0..end].i64()
}

/// json_extract_object extracts a JSON object {...} by key.
fn json_extract_object(json_str string, key string) ?string {
	needle := '"${key}"'
	idx := json_str.index(needle) or { return none }
	rest := json_str[idx + needle.len..]
	colon_idx := rest.index(':') or { return none }
	after_colon := rest[colon_idx + 1..].trim_space()

	if !after_colon.starts_with('{') {
		return none
	}
	return json_find_matching_brace(after_colon, `{`, `}`)
}

/// json_extract_array extracts a JSON array [...] by key.
fn json_extract_array(json_str string, key string) ?string {
	needle := '"${key}"'
	idx := json_str.index(needle) or { return none }
	rest := json_str[idx + needle.len..]
	colon_idx := rest.index(':') or { return none }
	after_colon := rest[colon_idx + 1..].trim_space()

	if !after_colon.starts_with('[') {
		return none
	}
	return json_find_matching_brace(after_colon, `[`, `]`)
}

/// json_find_matching_brace finds the matching closing brace/bracket.
fn json_find_matching_brace(s string, open u8, close u8) ?string {
	if s.len == 0 || s[0] != open {
		return none
	}
	mut depth := 0
	mut in_string := false
	mut escape := false

	for i in 0 .. s.len {
		ch := s[i]
		if escape {
			escape = false
			continue
		}
		if ch == `\\` && in_string {
			escape = true
			continue
		}
		if ch == `"` {
			in_string = !in_string
			continue
		}
		if in_string {
			continue
		}
		if ch == open {
			depth++
		} else if ch == close {
			depth--
			if depth == 0 {
				return s[0..i + 1]
			}
		}
	}
	return none
}

/// json_parse_string_map parses a JSON object into map[string]string.
fn json_parse_string_map(obj_str string) map[string]string {
	mut result := map[string]string{}
	if obj_str.len < 2 {
		return result
	}
	// Strip outer braces
	inner := obj_str[1..obj_str.len - 1].trim_space()
	if inner.len == 0 {
		return result
	}

	mut pos := 0
	for pos < inner.len {
		// Skip whitespace and commas
		for pos < inner.len && (inner[pos] == ` ` || inner[pos] == `\t`
			|| inner[pos] == `\n` || inner[pos] == `,`) {
			pos++
		}
		if pos >= inner.len {
			break
		}

		// Expect key string
		if inner[pos] != `"` {
			break
		}
		pos++ // skip opening quote
		mut key_end := pos
		for key_end < inner.len && inner[key_end] != `"` {
			key_end++
		}
		key := inner[pos..key_end]
		pos = key_end + 1 // skip closing quote

		// Skip colon
		for pos < inner.len && inner[pos] != `:` {
			pos++
		}
		pos++ // skip colon

		// Skip whitespace
		for pos < inner.len && (inner[pos] == ` ` || inner[pos] == `\t`) {
			pos++
		}

		if pos >= inner.len {
			break
		}

		// Read value
		if inner[pos] == `"` {
			pos++ // skip opening quote
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `"` {
				val_end++
			}
			result[key] = inner[pos..val_end]
			pos = val_end + 1
		} else {
			// Non-string value (number/bool)
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `,` && inner[val_end] != `}`
				&& inner[val_end] != `\n` {
				val_end++
			}
			result[key] = inner[pos..val_end].trim_space()
			pos = val_end
		}
	}
	return result
}

/// json_split_array_items splits a JSON array into individual item strings.
fn json_split_array_items(arr_str string) []string {
	mut items := []string{}
	if arr_str.len < 2 {
		return items
	}
	inner := arr_str[1..arr_str.len - 1].trim_space()
	if inner.len == 0 {
		return items
	}

	mut pos := 0
	for pos < inner.len {
		// Skip commas and whitespace
		for pos < inner.len && (inner[pos] == `,` || inner[pos] == ` ` || inner[pos] == `\n`
			|| inner[pos] == `\t`) {
			pos++
		}
		if pos >= inner.len {
			break
		}

		if inner[pos] == `{` {
			end := json_find_matching_brace(inner[pos..], `{`, `}`) or { break }
			items << end
			pos += end.len
		} else if inner[pos] == `[` {
			end := json_find_matching_brace(inner[pos..], `[`, `]`) or { break }
			items << end
			pos += end.len
		} else if inner[pos] == `"` {
			pos++
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `"` {
				val_end++
			}
			items << '"${inner[pos..val_end]}"'
			pos = val_end + 1
		} else {
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `,` {
				val_end++
			}
			items << inner[pos..val_end].trim_space()
			pos = val_end
		}
	}
	return items
}

/// json_parse_schemas parses a JSON array of schema objects.
fn json_parse_schemas(arr_str string) []IcebergSchema {
	mut schemas := []IcebergSchema{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut schema := IcebergSchema{}
		if v := json_extract_int(item, 'schema-id') {
			schema.schema_id = v
		} else if v2 := json_extract_int(item, 'schemaId') {
			schema.schema_id = v2
		}
		if fields_str := json_extract_array(item, 'fields') {
			schema.fields = json_parse_fields(fields_str)
		}
		schemas << schema
	}
	return schemas
}

/// json_parse_fields parses a JSON array of field objects.
fn json_parse_fields(arr_str string) []IcebergField {
	mut fields := []IcebergField{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut field := IcebergField{}
		if v := json_extract_int(item, 'id') {
			field.id = v
		}
		if v := json_extract_string(item, 'name') {
			field.name = v
		}
		if v := json_extract_string(item, 'type') {
			field.typ = v
		}
		// required: boolean
		if item.contains('"required":true') || item.contains('"required": true') {
			field.required = true
		}
		if v := json_extract_string(item, 'initial-default') {
			field.default_value = v
		}
		fields << field
	}
	return fields
}

/// json_parse_partition_specs parses a JSON array of partition spec objects.
fn json_parse_partition_specs(arr_str string) []IcebergPartitionSpec {
	mut specs := []IcebergPartitionSpec{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut spec := IcebergPartitionSpec{}
		if v := json_extract_int(item, 'spec-id') {
			spec.spec_id = v
		} else if v2 := json_extract_int(item, 'specId') {
			spec.spec_id = v2
		}
		if fields_str := json_extract_array(item, 'fields') {
			spec.fields = json_parse_partition_fields(fields_str)
		}
		specs << spec
	}
	return specs
}

/// json_parse_partition_fields parses a JSON array of partition field objects.
fn json_parse_partition_fields(arr_str string) []IcebergPartitionField {
	mut pfields := []IcebergPartitionField{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut pfield := IcebergPartitionField{}
		if v := json_extract_int(item, 'source-id') {
			pfield.source_id = v
		} else if v2 := json_extract_int(item, 'sourceId') {
			pfield.source_id = v2
		}
		if v := json_extract_int(item, 'field-id') {
			pfield.field_id = v
		} else if v2 := json_extract_int(item, 'fieldId') {
			pfield.field_id = v2
		}
		if v := json_extract_string(item, 'name') {
			pfield.name = v
		}
		if v := json_extract_string(item, 'transform') {
			pfield.transform = v
		}
		pfields << pfield
	}
	return pfields
}

/// json_parse_snapshots parses a JSON array of snapshot objects.
fn json_parse_snapshots(arr_str string) []IcebergSnapshot {
	mut snapshots := []IcebergSnapshot{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut snap := IcebergSnapshot{}
		if v := json_extract_i64(item, 'snapshot-id') {
			snap.snapshot_id = v
		} else if v2 := json_extract_i64(item, 'snapshotId') {
			snap.snapshot_id = v2
		}
		if v := json_extract_i64(item, 'timestamp-ms') {
			snap.timestamp_ms = v
		} else if v2 := json_extract_i64(item, 'timestampMs') {
			snap.timestamp_ms = v2
		}
		if v := json_extract_string(item, 'manifest-list') {
			snap.manifest_list = v
		} else if v2 := json_extract_string(item, 'manifestList') {
			snap.manifest_list = v2
		}
		if v := json_extract_int(item, 'schema-id') {
			snap.schema_id = v
		} else if v2 := json_extract_int(item, 'schemaId') {
			snap.schema_id = v2
		}
		if summary_str := json_extract_object(item, 'summary') {
			snap.summary = json_parse_string_map(summary_str)
		}
		snapshots << snap
	}
	return snapshots
}

/// GlueCatalog is an AWS Glue Data Catalog implementation.
/// Connects to AWS Glue via HTTP API with SigV4 authentication.
pub struct GlueCatalog {
pub mut:
	region         string
	warehouse      string
	adapter        &S3StorageAdapter
	access_key     string
	secret_key     string
	session_token  string
	glue_endpoint  string
}

/// new_glue_catalog creates a new Glue catalog.
pub fn new_glue_catalog(adapter &S3StorageAdapter, region string, warehouse string) &GlueCatalog {
	endpoint := 'https://glue.${region}.amazonaws.com'
	return &GlueCatalog{
		region:        region
		warehouse:     warehouse
		adapter:       adapter
		access_key:    ''
		secret_key:    ''
		session_token: ''
		glue_endpoint: endpoint
	}
}

/// new_glue_catalog_with_credentials creates a Glue catalog with explicit credentials.
pub fn new_glue_catalog_with_credentials(adapter &S3StorageAdapter, region string, warehouse string, access_key string, secret_key string, session_token string) &GlueCatalog {
	endpoint := 'https://glue.${region}.amazonaws.com'
	return &GlueCatalog{
		region:        region
		warehouse:     warehouse
		adapter:       adapter
		access_key:    access_key
		secret_key:    secret_key
		session_token: session_token
		glue_endpoint: endpoint
	}
}

/// create_table creates a new table in Glue.
pub fn (c &GlueCatalog) create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }
	table_uuid := generate_table_uuid()
	now := time.now()

	metadata := IcebergMetadata{
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
			'created_by': 'DataCore GlueCatalog'
		}
	}

	// Encode metadata as Iceberg-compatible JSON
	mut catalog := new_hadoop_catalog(c.adapter, c.warehouse)
	metadata_json := catalog.encode_metadata(metadata)

	// Glue CreateTable request body
	request_body := c.build_create_table_request(db_name, identifier.name, location, metadata_json,
		schema, spec)

	c.call_glue_api('CreateTable', request_body) or {
		return error('Glue CreateTable failed for ${db_name}.${identifier.name}: ${err}')
	}

	return metadata
}

/// load_table loads a table from Glue.
pub fn (c &GlueCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}","Name":"${identifier.name}"}'
	response := c.call_glue_api('GetTable', request_body) or {
		return error('Glue GetTable failed for ${db_name}.${identifier.name}: ${err}')
	}

	return c.parse_glue_table_response(response, identifier)
}

/// update_table updates a Glue table.
pub fn (c &GlueCatalog) update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	mut catalog := new_hadoop_catalog(c.adapter, c.warehouse)
	metadata_json := catalog.encode_metadata(metadata)

	request_body := c.build_update_table_request(db_name, identifier.name, metadata.location,
		metadata_json)

	c.call_glue_api('UpdateTable', request_body) or {
		return error('Glue UpdateTable failed for ${db_name}.${identifier.name}: ${err}')
	}
}

/// drop_table drops a Glue table.
pub fn (c &GlueCatalog) drop_table(identifier IcebergTableIdentifier) ! {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}","Name":"${identifier.name}"}'
	c.call_glue_api('DeleteTable', request_body) or {
		return error('Glue DeleteTable failed for ${db_name}.${identifier.name}: ${err}')
	}
}

/// list_tables retrieves the list of tables from Glue.
pub fn (c &GlueCatalog) list_tables(namespace []string) ![]IcebergTableIdentifier {
	db_name := if namespace.len > 0 { namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}"}'
	response := c.call_glue_api('GetTables', request_body) or {
		return error('Glue GetTables failed for ${db_name}: ${err}')
	}

	return c.parse_glue_tables_response(response, namespace)
}

/// namespace_exists checks whether the Glue database (namespace) exists.
pub fn (c &GlueCatalog) namespace_exists(namespace []string) bool {
	if namespace.len == 0 {
		return true
	}
	db_name := namespace[0]
	request_body := '{"Name":"${db_name}"}'
	c.call_glue_api('GetDatabase', request_body) or { return false }
	return true
}

/// create_namespace creates a new Glue database (namespace).
pub fn (c &GlueCatalog) create_namespace(namespace []string) ! {
	if namespace.len == 0 {
		return
	}
	db_name := namespace[0]
	request_body := '{"DatabaseInput":{"Name":"${db_name}","Description":"Created by DataCore GlueCatalog"}}'
	c.call_glue_api('CreateDatabase', request_body) or {
		return error('Glue CreateDatabase failed for ${db_name}: ${err}')
	}
}

// --- Glue HTTP API helpers ---

/// call_glue_api performs an authenticated HTTP POST to the Glue API.
fn (c &GlueCatalog) call_glue_api(action string, body string) !string {
	url := c.glue_endpoint
	now := time.now()
	// AWS SigV4 date formats: YYYYMMDDTHHmmssZ and YYYYMMDD
	amz_date := now.custom_format('YYYYMMDDTHHmmss') + 'Z'
	date_stamp := now.custom_format('YYYYMMDD')

	body_hash := sha256_hex(body.bytes())

	host := c.glue_endpoint.all_after('https://')

	mut req_headers := {
		'Content-Type':         'application/x-amz-json-1.1'
		'X-Amz-Target':         'AWSGlue.${action}'
		'X-Amz-Date':           amz_date
		'X-Amz-Content-Sha256': body_hash
		'Host':                 host
	}

	if c.session_token.len > 0 {
		req_headers['X-Amz-Security-Token'] = c.session_token
	}

	// Build authorization header if credentials are present
	if c.access_key.len > 0 && c.secret_key.len > 0 {
		auth := c.sigv4_authorization('POST', '/', '', req_headers, body, date_stamp, amz_date)
		req_headers['Authorization'] = auth
	}

	mut header := http.Header{}
	for key, value in req_headers {
		header.add_custom(key, value) or {}
	}

	config := http.FetchConfig{
		url:    url
		method: .post
		data:   body
		header: header
	}

	resp := http.fetch(config) or { return error('HTTP request failed: ${err}') }

	if resp.status_code >= 400 {
		return error('Glue API error ${resp.status_code}: ${resp.body}')
	}

	return resp.body
}

/// sigv4_authorization generates an AWS SigV4 Authorization header.
fn (c &GlueCatalog) sigv4_authorization(method string, uri string, query string, headers map[string]string, payload string, date_stamp string, amz_date string) string {
	service := 'glue'
	algorithm := 'AWS4-HMAC-SHA256'

	// Canonical headers (sorted)
	mut sorted_keys := headers.keys()
	sorted_keys.sort()
	mut canonical_headers := ''
	mut signed_headers_list := []string{}
	for key in sorted_keys {
		lk := key.to_lower()
		canonical_headers += '${lk}:${headers[key].trim_space()}\n'
		signed_headers_list << lk
	}
	signed_headers := signed_headers_list.join(';')

	payload_hash := sha256_hex(payload.bytes())
	canonical_request := '${method}\n${uri}\n${query}\n${canonical_headers}\n${signed_headers}\n${payload_hash}'

	credential_scope := '${date_stamp}/${c.region}/${service}/aws4_request'
	string_to_sign := '${algorithm}\n${amz_date}\n${credential_scope}\n${sha256_hex(canonical_request.bytes())}'

	signing_key := c.sigv4_signing_key(date_stamp, service)
	signature := hmac.new(signing_key, string_to_sign.bytes(), sha256.sum, sha256.block_size).hex()

	return '${algorithm} Credential=${c.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
}

/// sigv4_signing_key derives the SigV4 signing key.
fn (c &GlueCatalog) sigv4_signing_key(date_stamp string, service string) []u8 {
	k_date := hmac.new(('AWS4' + c.secret_key).bytes(), date_stamp.bytes(), sha256.sum,
		sha256.block_size)
	k_region := hmac.new(k_date, c.region.bytes(), sha256.sum, sha256.block_size)
	k_service := hmac.new(k_region, service.bytes(), sha256.sum, sha256.block_size)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, sha256.block_size)
	return k_signing
}

/// sha256_hex computes SHA-256 and returns the lowercase hex string.
fn sha256_hex(data []u8) string {
	return sha256.sum(data).hex()
}

/// build_create_table_request builds the Glue CreateTable JSON request body.
fn (c &GlueCatalog) build_create_table_request(db_name string, table_name string, location string, metadata_json string, schema IcebergSchema, spec IcebergPartitionSpec) string {
	// Encode metadata_json as base64 for Glue parameter storage
	metadata_b64 := base64.encode(metadata_json.bytes())

	mut col_defs := strings.new_builder(512)
	col_defs.write_string('[')
	for i, field in schema.fields {
		if i > 0 {
			col_defs.write_string(',')
		}
		glue_type := iceberg_type_to_glue(field.typ)
		col_defs.write_string('{"Name":"${field.name}","Type":"${glue_type}","Comment":"iceberg-field-id=${field.id}"}')
	}
	col_defs.write_string(']')

	return '{"DatabaseName":"${db_name}","TableInput":{"Name":"${table_name}",' +
		'"StorageDescriptor":{"Columns":${col_defs.str()},' +
		'"Location":"${location}",' +
		'"InputFormat":"org.apache.iceberg.mr.mapred.IcebergInputFormat",' +
		'"OutputFormat":"org.apache.iceberg.mr.mapred.IcebergOutputFormat",' +
		'"SerdeInfo":{"SerializationLibrary":"org.apache.iceberg.mr.hive.HiveIcebergSerDe"}},' +
		'"Parameters":{"table_type":"ICEBERG","metadata_location":"${location}/metadata/v1.metadata.json",' +
		'"iceberg_metadata":"${metadata_b64}"}}}'
}

/// build_update_table_request builds the Glue UpdateTable JSON request body.
fn (c &GlueCatalog) build_update_table_request(db_name string, table_name string, location string, metadata_json string) string {
	metadata_b64 := base64.encode(metadata_json.bytes())
	return '{"DatabaseName":"${db_name}","TableInput":{"Name":"${table_name}",' +
		'"Parameters":{"table_type":"ICEBERG",' +
		'"metadata_location":"${location}/metadata/latest.metadata.json",' +
		'"iceberg_metadata":"${metadata_b64}"}}}'
}

/// parse_glue_table_response parses the Glue GetTable response into IcebergMetadata.
fn (c &GlueCatalog) parse_glue_table_response(response string, identifier IcebergTableIdentifier) !IcebergMetadata {
	// Extract iceberg_metadata parameter (base64-encoded metadata JSON)
	if params_str := json_extract_object(response, 'Parameters') {
		if metadata_b64 := json_extract_string(params_str, 'iceberg_metadata') {
			metadata_json_bytes := base64.decode(metadata_b64)
			metadata_json := metadata_json_bytes.bytestr()
			mut catalog := new_hadoop_catalog(c.adapter, c.warehouse)
			return catalog.decode_metadata(metadata_json)
		}
	}

	// Fallback: reconstruct from Glue table structure
	location_str := if loc := json_extract_string(response, 'Location') { loc } else { '${c.warehouse}/${identifier.name}' }

	return IcebergMetadata{
		format_version:      2
		table_uuid:          generate_table_uuid()
		location:            location_str
		last_updated_ms:     time.now().unix_milli()
		schemas:             [create_default_schema()]
		current_schema_id:   0
		partition_specs:     [create_default_partition_spec()]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'table_type': 'ICEBERG'
		}
	}
}

/// parse_glue_tables_response parses the Glue GetTables response into table identifiers.
fn (c &GlueCatalog) parse_glue_tables_response(response string, namespace []string) []IcebergTableIdentifier {
	mut identifiers := []IcebergTableIdentifier{}

	table_list_str := json_extract_array(response, 'TableList') or { return identifiers }
	items := json_split_array_items(table_list_str)

	for item in items {
		name := json_extract_string(item, 'Name') or { continue }
		// Only return Iceberg tables
		if params_str := json_extract_object(item, 'Parameters') {
			if table_type := json_extract_string(params_str, 'table_type') {
				if table_type == 'ICEBERG' {
					identifiers << IcebergTableIdentifier{
						namespace: namespace
						name:      name
					}
				}
			}
		}
	}
	return identifiers
}

/// iceberg_type_to_glue converts an Iceberg type to a Glue/Hive column type.
fn iceberg_type_to_glue(iceberg_type string) string {
	return match iceberg_type {
		'int' { 'int' }
		'long' { 'bigint' }
		'float' { 'float' }
		'double' { 'double' }
		'boolean' { 'boolean' }
		'string' { 'string' }
		'binary' { 'binary' }
		'timestamp', 'timestamptz' { 'timestamp' }
		'date' { 'date' }
		'decimal' { 'decimal(38,10)' }
		else { 'string' }
	}
}

/// load_metadata_at loads Iceberg metadata from a specific metadata file path via S3 adapter.
pub fn (c &GlueCatalog) load_metadata_at(metadata_location string) !IcebergMetadata {
	data, _ := c.adapter.get_object(metadata_location, -1, -1) or {
		return error('Failed to read metadata at ${metadata_location}: ${err}')
	}
	mut tmp_catalog := new_hadoop_catalog(c.adapter, c.warehouse)
	return tmp_catalog.decode_metadata(data.bytestr())
}

/// create_catalog creates the appropriate catalog based on catalog type.
pub fn create_catalog(adapter &S3StorageAdapter, config IcebergCatalogConfig) IcebergCatalog {
	match config.catalog_type {
		'glue' {
			return new_glue_catalog(adapter, config.region, config.warehouse)
		}
		'hadoop', 's3', 'file' {
			return new_hadoop_catalog(adapter, config.warehouse)
		}
		else {
			// Default to Hadoop catalog
			return new_hadoop_catalog(adapter, config.warehouse)
		}
	}
}
