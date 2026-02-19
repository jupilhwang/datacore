// v3 테이블 포맷 지원
module s3

import json

// v3 테이블 포맷 추가 타입

/// IcebergDeleteFile은 삭제 파일을 나타냅니다 (v3: Binary Deletion Vectors 지원).
/// ContentType: 삭제 파일 타입 (position_deletes, equality_deletes, dv)
/// FilePath: 파일 경로
/// FileFormat: 파일 형식
/// RecordCount: 삭제 레코드 수
/// FileSizeInBytes: 파일 크기
/// ReferencedDataFile: 참조 데이터 파일 (dv 타입에서 사용)
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

/// IcebergDeletionVector는 바이너리 삭제 벡터를 나타냅니다 (v3 신규).
/// StorageType: 저장 타입 ('inline', 'path')
/// Data: 인라인 데이터 (base64 인코딩)
/// Path: 외부 파일 경로
/// Offset: 파일 내 오프셋 (path 타입에서 사용)
/// Length: 데이터 길이
/// Cardinality: 삭제된 행 수
pub struct IcebergDeletionVector {
pub mut:
	storage_type string
	data         string
	path         string
	offset       i64
	length       i64
	cardinality  i64
}

// REST Catalog API 요청/응답 타입

/// CatalogConfig는 /v1/config 엔드포인트 응답입니다.
pub struct CatalogConfig {
pub mut:
	defaults  map[string]string @[json: 'defaults']
	overrides map[string]string @[json: 'overrides']
	endpoints []string          @[json: 'endpoints']
}

/// NamespaceIdentifier는 네임스페이스 식별자입니다.
pub struct NamespaceIdentifier {
pub mut:
	namespace []string @[json: 'namespace']
}

/// CreateNamespaceRequest는 네임스페이스 생성 요청입니다.
pub struct CreateNamespaceRequest {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// CreateNamespaceResponse는 네임스페이스 생성 응답입니다.
pub struct CreateNamespaceResponse {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// ListNamespacesResponse는 네임스페이스 목록 응답입니다.
pub struct ListNamespacesResponse {
pub mut:
	namespaces [][]string @[json: 'namespaces']
}

/// GetNamespaceResponse는 네임스페이스 조회 응답입니다.
pub struct GetNamespaceResponse {
pub mut:
	namespace  []string          @[json: 'namespace']
	properties map[string]string @[json: 'properties']
}

/// UpdateNamespacePropertiesRequest는 네임스페이스 속성 업데이트 요청입니다.
pub struct UpdateNamespacePropertiesRequest {
pub mut:
	removals []string          @[json: 'removals']
	updates  map[string]string @[json: 'updates']
}

/// UpdateNamespacePropertiesResponse는 네임스페이스 속성 업데이트 응답입니다.
pub struct UpdateNamespacePropertiesResponse {
pub mut:
	updated []string @[json: 'updated']
	removed []string @[json: 'removed']
	missing []string @[json: 'missing']
}

/// TableIdentifierRest는 REST API용 테이블 식별자입니다.
pub struct TableIdentifierRest {
pub mut:
	namespace []string @[json: 'namespace']
	name      string   @[json: 'name']
}

/// ListTablesResponse는 테이블 목록 응답입니다.
pub struct ListTablesResponse {
pub mut:
	identifiers []TableIdentifierRest @[json: 'identifiers']
}

/// CreateTableRequest는 테이블 생성 요청입니다.
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

/// SchemaRest는 REST API용 스키마입니다.
pub struct SchemaRest {
pub mut:
	typ                  string      @[json: 'type']
	schema_id            int         @[json: 'schema-id']
	identifier_field_ids []int       @[json: 'identifier-field-ids']
	fields               []FieldRest @[json: 'fields']
}

/// FieldRest는 REST API용 필드입니다.
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

/// PartitionSpecRest는 REST API용 파티션 스펙입니다.
pub struct PartitionSpecRest {
pub mut:
	spec_id int                  @[json: 'spec-id']
	fields  []PartitionFieldRest @[json: 'fields']
}

/// PartitionFieldRest는 REST API용 파티션 필드입니다.
pub struct PartitionFieldRest {
pub mut:
	field_id       int      @[json: 'field-id']
	source_id      int      @[json: 'source-id']
	name           string   @[json: 'name']
	transform      string   @[json: 'transform']
	transform_args []string @[json: 'transform-args']
}

/// SortOrderRest는 REST API용 정렬 순서입니다.
pub struct SortOrderRest {
pub mut:
	order_id int             @[json: 'order-id']
	fields   []SortFieldRest @[json: 'fields']
}

/// SortFieldRest는 REST API용 정렬 필드입니다.
pub struct SortFieldRest {
pub mut:
	transform  string @[json: 'transform']
	source_id  int    @[json: 'source-id']
	direction  string @[json: 'direction']
	null_order string @[json: 'null-order']
}

/// LoadTableResponse는 테이블 로드 응답입니다.
pub struct LoadTableResponse {
pub mut:
	metadata_location string            @[json: 'metadata-location']
	metadata          TableMetadataRest @[json: 'metadata']
	config            map[string]string @[json: 'config']
}

/// TableMetadataRest는 REST API용 테이블 메타데이터입니다.
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

/// SnapshotRest는 REST API용 스냅샷입니다.
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

/// SnapshotLogEntry는 스냅샷 로그 항목입니다.
pub struct SnapshotLogEntry {
pub mut:
	timestamp_ms i64 @[json: 'timestamp-ms']
	snapshot_id  i64 @[json: 'snapshot-id']
}

/// CommitTableRequest는 테이블 커밋 요청입니다.
pub struct CommitTableRequest {
pub mut:
	identifier   TableIdentifierRest @[json: 'identifier']
	requirements []TableRequirement  @[json: 'requirements']
	updates      []TableUpdate       @[json: 'updates']
}

/// TableRequirement는 테이블 커밋 요구사항입니다.
pub struct TableRequirement {
pub mut:
	typ         string @[json: 'type']
	ref_        string @[json: 'ref']
	uuid        string @[json: 'uuid']
	snapshot_id i64    @[json: 'snapshot-id']
	schema_id   int    @[json: 'last-assigned-field-id']
}

/// TableUpdate는 테이블 업데이트 작업입니다.
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

/// CommitTableResponse는 테이블 커밋 응답입니다.
pub struct CommitTableResponse {
pub mut:
	metadata_location string            @[json: 'metadata-location']
	metadata          TableMetadataRest @[json: 'metadata']
}

/// RegisterTableRequest는 테이블 등록 요청입니다.
pub struct RegisterTableRequest {
pub mut:
	name              string @[json: 'name']
	metadata_location string @[json: 'metadata-location']
}

/// RenameTableRequest는 테이블 이름 변경 요청입니다.
pub struct RenameTableRequest {
pub mut:
	source      TableIdentifierRest @[json: 'source']
	destination TableIdentifierRest @[json: 'destination']
}

/// ReportMetricsRequest는 메트릭 보고 요청입니다.
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

/// IcebergErrorResponse는 오류 응답입니다.
pub struct IcebergErrorResponse {
pub mut:
	error_type string   @[json: 'type']
	code       int      @[json: 'code']
	message    string   @[json: 'message']
	stack      []string @[json: 'stack']
}

/// new_catalog_config는 기본 카탈로그 설정을 생성합니다.
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

/// iceberg_error는 Iceberg 오류 응답을 생성합니다.
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

/// to_json은 구조체를 JSON 문자열로 변환합니다.
pub fn (c CatalogConfig) to_json() string {
	return json.encode(c)
}

pub fn (r ListNamespacesResponse) to_json() string {
	return json.encode(r)
}

pub fn (r CreateNamespaceResponse) to_json() string {
	return json.encode(r)
}

pub fn (r GetNamespaceResponse) to_json() string {
	return json.encode(r)
}

pub fn (r ListTablesResponse) to_json() string {
	return json.encode(r)
}

pub fn (r LoadTableResponse) to_json() string {
	return json.encode(r)
}

pub fn (r CommitTableResponse) to_json() string {
	return json.encode(r)
}

pub fn (e IcebergErrorResponse) to_json() string {
	return json.encode(e)
}

pub fn (r UpdateNamespacePropertiesResponse) to_json() string {
	return json.encode(r)
}

/// schema_to_rest는 IcebergSchema를 REST API 형식으로 변환합니다.
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

/// partition_spec_to_rest는 IcebergPartitionSpec을 REST API 형식으로 변환합니다.
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

/// snapshot_to_rest는 IcebergSnapshot을 REST API 형식으로 변환합니다.
pub fn snapshot_to_rest(snapshot IcebergSnapshot) SnapshotRest {
	return SnapshotRest{
		snapshot_id:   snapshot.snapshot_id
		timestamp_ms:  snapshot.timestamp_ms
		manifest_list: snapshot.manifest_list
		summary:       snapshot.summary
		schema_id:     snapshot.schema_id
	}
}

/// metadata_to_rest는 IcebergMetadata를 REST API 형식으로 변환합니다.
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
