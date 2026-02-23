// v3 table format support
module s3

import json

// v3 table format additional types

/// IcebergDeleteFile represents a delete file (v3: Binary Deletion Vectors support).
/// ContentType: delete file type (position_deletes, equality_deletes, dv)
/// FilePath: file path
/// FileFormat: file format
/// RecordCount: number of deleted records
/// FileSizeInBytes: file size
/// ReferencedDataFile: referenced data file (used in dv type)
pub struct IcebergDeleteFile {
pub mut:
	content_type         string
	file_path            string
	file_format          string
	record_count         i64
	file_size_in_bytes   i64
	referenced_data_file string
	equality_ids         []int
	partition            map[string]string
}

/// IcebergDeletionVector represents a binary deletion vector (new in v3).
/// StorageType: storage type ('inline', 'path')
/// Data: inline data (base64 encoded)
/// Path: external file path
/// Offset: offset within the file (used in path type)
/// Length: data length
/// Cardinality: number of deleted rows
pub struct IcebergDeletionVector {
pub mut:
	storage_type string
	data         string
	path         string
	offset       i64
	length       i64
	cardinality  i64
}

// REST Catalog API request/response types

/// CatalogConfig is the response for the /v1/config endpoint.
pub struct CatalogConfig {
pub mut:
	defaults  map[string]string @[json: 'defaults']
	overrides map[string]string @[json: 'overrides']
	endpoints []string          @[json: 'endpoints']
}

/// NamespaceIdentifier is a namespace identifier.
pub struct NamespaceIdentifier {
pub mut:
	namespace []string @[json: 'namespace']
}

/// CreateNamespaceRequest is a namespace creation request.
pub struct CreateNamespaceRequest {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// CreateNamespaceResponse is a namespace creation response.
pub struct CreateNamespaceResponse {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// ListNamespacesResponse is a namespace list response.
pub struct ListNamespacesResponse {
pub mut:
	namespaces [][]string @[json: 'namespaces']
}

/// GetNamespaceResponse is a namespace retrieval response.
pub struct GetNamespaceResponse {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// UpdateNamespacePropertiesRequest is a namespace properties update request.
pub struct UpdateNamespacePropertiesRequest {
pub mut:
	removals []string          @[json: 'removals']
	updates  map[string]string @[json: 'updates']
}

/// UpdateNamespacePropertiesResponse is a namespace properties update response.
pub struct UpdateNamespacePropertiesResponse {
pub mut:
	updated []string @[json: 'updated']
	removed []string @[json: 'removed']
	missing []string @[json: 'missing']
}

/// TableIdentifierRest is a table identifier for REST API.
pub struct TableIdentifierRest {
pub mut:
	namespace []string @[json: 'namespace']
	name      string   @[json: 'name']
}

/// ListTablesResponse is a table list response.
pub struct ListTablesResponse {
pub mut:
	identifiers []TableIdentifierRest @[json: 'identifiers']
}

/// CreateTableRequest is a table creation request.
pub struct CreateTableRequest {
pub mut:
	name           string            @[json: 'name']
	location       string            @[json: 'location']
	schema         SchemaRest        @[json: 'schema']
	partition_spec PartitionSpecRest @[json: 'partition-spec']
	write_order    SortOrderRest     @[json: 'write-order']
	stage_create   bool              @[json: 'stage-create']
	properties     map[string]string @[json: 'properties']
}

/// SchemaRest is a schema for REST API.
pub struct SchemaRest {
pub mut:
	typ                  string      @[json: 'type']
	schema_id            int         @[json: 'schema-id']
	identifier_field_ids []int       @[json: 'identifier-field-ids']
	fields               []FieldRest @[json: 'fields']
}

/// FieldRest is a field for REST API.
pub struct FieldRest {
pub mut:
	id              int    @[json: 'id']
	name            string @[json: 'name']
	required        bool   @[json: 'required']
	typ             string @[json: 'type']
	doc             string @[json: 'doc']
	initial_default string @[json: 'initial-default']
	write_default   string @[json: 'write-default']
}

/// PartitionSpecRest is a partition spec for REST API.
pub struct PartitionSpecRest {
pub mut:
	spec_id int                  @[json: 'spec-id']
	fields  []PartitionFieldRest @[json: 'fields']
}

/// PartitionFieldRest is a partition field for REST API.
pub struct PartitionFieldRest {
pub mut:
	field_id       int      @[json: 'field-id']
	source_id      int      @[json: 'source-id']
	name           string   @[json: 'name']
	transform      string   @[json: 'transform']
	transform_args []string @[json: 'transform-args']
}

/// SortOrderRest is a sort order for REST API.
pub struct SortOrderRest {
pub mut:
	order_id int             @[json: 'order-id']
	fields   []SortFieldRest @[json: 'fields']
}

/// SortFieldRest is a sort field for REST API.
pub struct SortFieldRest {
pub mut:
	transform  string @[json: 'transform']
	source_id  int    @[json: 'source-id']
	direction  string @[json: 'direction']
	null_order string @[json: 'null-order']
}

/// LoadTableResponse is a table load response.
pub struct LoadTableResponse {
pub mut:
	metadata_location string            @[json: 'metadata-location']
	metadata          TableMetadataRest @[json: 'metadata']
	config            map[string]string @[json: 'config']
}

/// TableMetadataRest is table metadata for REST API.
pub struct TableMetadataRest {
pub mut:
	format_version        int                 @[json: 'format-version']
	table_uuid            string              @[json: 'table-uuid']
	location              string              @[json: 'location']
	last_sequence_number  i64                 @[json: 'last-sequence-number']
	last_updated_ms       i64                 @[json: 'last-updated-ms']
	last_column_id        int                 @[json: 'last-column-id']
	current_schema_id     int                 @[json: 'current-schema-id']
	schemas               []SchemaRest        @[json: 'schemas']
	default_spec_id       int                 @[json: 'default-spec-id']
	partition_specs       []PartitionSpecRest @[json: 'partition-specs']
	default_sort_order_id int                 @[json: 'default-sort-order-id']
	sort_orders           []SortOrderRest     @[json: 'sort-orders']
	current_snapshot_id   i64                 @[json: 'current-snapshot-id']
	snapshots             []SnapshotRest      @[json: 'snapshots']
	snapshot_log          []SnapshotLogEntry  @[json: 'snapshot-log']
	properties            map[string]string   @[json: 'properties']
}

/// SnapshotRest is a snapshot for REST API.
pub struct SnapshotRest {
pub mut:
	snapshot_id     i64               @[json: 'snapshot-id']
	parent_id       i64               @[json: 'parent-snapshot-id']
	sequence_number i64               @[json: 'sequence-number']
	timestamp_ms    i64               @[json: 'timestamp-ms']
	manifest_list   string            @[json: 'manifest-list']
	summary         map[string]string @[json: 'summary']
	schema_id       int               @[json: 'schema-id']
}

/// SnapshotLogEntry is a snapshot log entry.
pub struct SnapshotLogEntry {
pub mut:
	timestamp_ms i64 @[json: 'timestamp-ms']
	snapshot_id  i64 @[json: 'snapshot-id']
}

/// CommitTableRequest is a table commit request.
pub struct CommitTableRequest {
pub mut:
	identifier   TableIdentifierRest @[json: 'identifier']
	requirements []TableRequirement  @[json: 'requirements']
	updates      []TableUpdate       @[json: 'updates']
}

/// TableRequirement is a table commit requirement.
pub struct TableRequirement {
pub mut:
	typ         string @[json: 'type']
	ref_        string @[json: 'ref']
	uuid        string @[json: 'uuid']
	snapshot_id i64    @[json: 'snapshot-id']
	schema_id   int    @[json: 'last-assigned-field-id']
}

/// TableUpdate is a table update operation.
pub struct TableUpdate {
pub mut:
	action     string            @[json: 'action']
	schema     SchemaRest        @[json: 'schema']
	spec       PartitionSpecRest @[json: 'spec']
	sort_order SortOrderRest     @[json: 'sort-order']
	snapshot   SnapshotRest      @[json: 'snapshot']
	location   string            @[json: 'location']
	properties map[string]string @[json: 'properties']
	removals   []string          @[json: 'removals']
}

/// CommitTableResponse is a table commit response.
pub struct CommitTableResponse {
pub mut:
	metadata_location string            @[json: 'metadata-location']
	metadata          TableMetadataRest @[json: 'metadata']
}

/// RegisterTableRequest is a table registration request.
pub struct RegisterTableRequest {
pub mut:
	name              string @[json: 'name']
	metadata_location string @[json: 'metadata-location']
}

/// RenameTableRequest is a table rename request.
pub struct RenameTableRequest {
pub mut:
	source      TableIdentifierRest @[json: 'source']
	destination TableIdentifierRest @[json: 'destination']
}

/// ReportMetricsRequest is a metrics report request.
pub struct ReportMetricsRequest {
pub mut:
	report_type           string            @[json: 'report-type']
	table_name            string            @[json: 'table-name']
	snapshot_id           i64               @[json: 'snapshot-id']
	filter                string            @[json: 'filter']
	schema_id             int               @[json: 'schema-id']
	projected_field_ids   []int             @[json: 'projected-field-ids']
	projected_field_names []string          @[json: 'projected-field-names']
	metrics               map[string]string @[json: 'metrics']
}

/// IcebergErrorResponse is an error response.
pub struct IcebergErrorResponse {
pub mut:
	error_type string   @[json: 'type']
	code       int      @[json: 'code']
	message    string   @[json: 'message']
	stack      []string @[json: 'stack']
}

/// new_catalog_config creates a default catalog configuration.
pub fn new_catalog_config(warehouse string, format_version int) CatalogConfig {
	return CatalogConfig{
		defaults:  {
			'warehouse':      warehouse
			'format-version': format_version.str()
		}
		overrides: {}
		endpoints: [
			'GET /v1/{prefix}/namespaces',
			'POST /v1/{prefix}/namespaces',
			'GET /v1/{prefix}/namespaces/{namespace}',
			'DELETE /v1/{prefix}/namespaces/{namespace}',
			'POST /v1/{prefix}/namespaces/{namespace}/properties',
			'GET /v1/{prefix}/namespaces/{namespace}/tables',
			'POST /v1/{prefix}/namespaces/{namespace}/tables',
			'GET /v1/{prefix}/namespaces/{namespace}/tables/{table}',
			'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}',
			'DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}',
			'POST /v1/{prefix}/namespaces/{namespace}/register',
			'POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics',
			'POST /v1/{prefix}/tables/rename',
			'POST /v1/{prefix}/transactions/commit',
		]
	}
}

/// iceberg_error creates an Iceberg error response.
pub fn iceberg_error(code int, message string) IcebergErrorResponse {
	error_type := match code {
		400 { 'BadRequestException' }
		401 { 'NotAuthorizedException' }
		403 { 'ForbiddenException' }
		404 { 'NoSuchNamespaceException' }
		409 { 'AlreadyExistsException' }
		412 { 'CommitFailedException' }
		500 { 'ServiceFailureException' }
		503 { 'ServiceUnavailableException' }
		else { 'UnknownException' }
	}
	return IcebergErrorResponse{
		error_type: error_type
		code:       code
		message:    message
		stack:      []
	}
}

/// to_json converts the struct to a JSON string.
pub fn (c CatalogConfig) to_json() string {
	return json.encode(c)
}

/// to_json returns a JSON string.
pub fn (r ListNamespacesResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (r CreateNamespaceResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (r GetNamespaceResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (r ListTablesResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (r LoadTableResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (r CommitTableResponse) to_json() string {
	return json.encode(r)
}

/// to_json returns a JSON string.
pub fn (e IcebergErrorResponse) to_json() string {
	return json.encode(e)
}

/// to_json returns a JSON string.
pub fn (r UpdateNamespacePropertiesResponse) to_json() string {
	return json.encode(r)
}

/// schema_to_rest converts IcebergSchema to REST API format.
pub fn schema_to_rest(schema IcebergSchema) SchemaRest {
	mut fields := []FieldRest{}
	for field in schema.fields {
		fields << FieldRest{
			id:              field.id
			name:            field.name
			required:        field.required
			typ:             field.typ
			initial_default: field.default_value
			write_default:   field.default_value
		}
	}
	return SchemaRest{
		typ:                  'struct'
		schema_id:            schema.schema_id
		identifier_field_ids: schema.identifier_field_ids
		fields:               fields
	}
}

/// partition_spec_to_rest converts IcebergPartitionSpec to REST API format.
pub fn partition_spec_to_rest(spec IcebergPartitionSpec) PartitionSpecRest {
	mut fields := []PartitionFieldRest{}
	for field in spec.fields {
		fields << PartitionFieldRest{
			field_id:       field.field_id
			source_id:      field.source_id
			name:           field.name
			transform:      field.transform
			transform_args: field.transform_args
		}
	}
	return PartitionSpecRest{
		spec_id: spec.spec_id
		fields:  fields
	}
}

/// snapshot_to_rest converts IcebergSnapshot to REST API format.
pub fn snapshot_to_rest(snapshot IcebergSnapshot) SnapshotRest {
	return SnapshotRest{
		snapshot_id:   snapshot.snapshot_id
		timestamp_ms:  snapshot.timestamp_ms
		manifest_list: snapshot.manifest_list
		summary:       snapshot.summary
		schema_id:     snapshot.schema_id
	}
}

/// metadata_to_rest converts IcebergMetadata to REST API format.
pub fn metadata_to_rest(metadata IcebergMetadata, location string) TableMetadataRest {
	mut schemas := []SchemaRest{}
	for schema in metadata.schemas {
		schemas << schema_to_rest(schema)
	}

	mut specs := []PartitionSpecRest{}
	for spec in metadata.partition_specs {
		specs << partition_spec_to_rest(spec)
	}

	mut snapshots := []SnapshotRest{}
	for snapshot in metadata.snapshots {
		snapshots << snapshot_to_rest(snapshot)
	}

	mut snapshot_log := []SnapshotLogEntry{}
	for snapshot in metadata.snapshots {
		snapshot_log << SnapshotLogEntry{
			timestamp_ms: snapshot.timestamp_ms
			snapshot_id:  snapshot.snapshot_id
		}
	}

	return TableMetadataRest{
		format_version:      metadata.format_version
		table_uuid:          metadata.table_uuid
		location:            location
		last_updated_ms:     metadata.last_updated_ms
		current_schema_id:   metadata.current_schema_id
		schemas:             schemas
		default_spec_id:     metadata.default_spec_id
		partition_specs:     specs
		current_snapshot_id: metadata.current_snapshot_id
		snapshots:           snapshots
		snapshot_log:        snapshot_log
		properties:          metadata.properties
	}
}
