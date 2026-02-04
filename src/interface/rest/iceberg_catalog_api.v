// Interface Layer - Iceberg REST Catalog API
// Apache Iceberg REST Catalog API 엔드포인트를 제공합니다.
// v3 테이블 포맷 지원
module rest

import json
import infra.storage.plugins.s3

/// IcebergCatalogAPI는 Iceberg REST Catalog API 핸들러입니다.
pub struct IcebergCatalogAPI {
mut:
	catalog        s3.IcebergCatalog
	warehouse      string
	prefix         string
	format_version int
}

/// new_iceberg_catalog_api는 새로운 Iceberg Catalog API를 생성합니다.
pub fn new_iceberg_catalog_api(catalog s3.IcebergCatalog, warehouse string, format_version int) &IcebergCatalogAPI {
	return &IcebergCatalogAPI{
		catalog:        catalog
		warehouse:      warehouse
		prefix:         'iceberg'
		format_version: format_version
	}
}

/// handle_request는 Iceberg Catalog API 요청을 처리합니다.
/// 반환: (HTTP 상태 코드, 응답 본문)
pub fn (mut api IcebergCatalogAPI) handle_request(method string, path string, body string) (int, string) {
	// 경로 파싱: /v1/iceberg/... 또는 /v1/...
	parts := path.trim_left('/').split('/')

	if parts.len < 2 {
		return api.error_response(400, 'Invalid path')
	}

	// /v1/config 처리
	if parts.len == 2 && parts[1] == 'config' {
		return api.handle_config(method)
	}

	// /v1/{prefix}/... 경로 처리
	if parts.len < 3 {
		return api.error_response(404, 'Not found')
	}

	// prefix 다음 경로 파싱
	sub_parts := parts[2..]

	match sub_parts[0] {
		'namespaces' { return api.handle_namespaces(method, sub_parts[1..], body) }
		'tables' { return api.handle_tables_root(method, sub_parts[1..], body) }
		'transactions' { return api.handle_transactions(method, sub_parts[1..], body) }
		else { return api.error_response(404, 'Not found') }
	}
}

/// handle_config는 /v1/config 엔드포인트를 처리합니다.
fn (mut api IcebergCatalogAPI) handle_config(method string) (int, string) {
	if method != 'GET' {
		return api.error_response(405, 'Method not allowed')
	}

	config := s3.new_catalog_config(api.warehouse, api.format_version)
	return 200, config.to_json()
}

/// handle_namespaces는 /v1/{prefix}/namespaces 관련 요청을 처리합니다.
fn (mut api IcebergCatalogAPI) handle_namespaces(method string, parts []string, body string) (int, string) {
	// GET /namespaces - 네임스페이스 목록
	if parts.len == 0 {
		return match method {
			'GET' { api.list_namespaces() }
			'POST' { api.create_namespace(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	namespace := parts[0]

	// /namespaces/{namespace}
	if parts.len == 1 {
		return match method {
			'GET' { api.get_namespace(namespace) }
			'DELETE' { api.delete_namespace(namespace) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/properties
	if parts.len == 2 && parts[1] == 'properties' {
		return match method {
			'POST' { api.update_namespace_properties(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables
	if parts.len == 2 && parts[1] == 'tables' {
		return match method {
			'GET' { api.list_tables(namespace) }
			'POST' { api.create_table(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables/{table}
	if parts.len == 3 && parts[1] == 'tables' {
		table := parts[2]
		return match method {
			'GET' { api.load_table(namespace, table) }
			'POST' { api.commit_table(namespace, table, body) }
			'DELETE' { api.drop_table(namespace, table) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables/{table}/metrics
	if parts.len == 4 && parts[1] == 'tables' && parts[3] == 'metrics' {
		return match method {
			'POST' { api.report_metrics(parts[0], parts[2], body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/register
	if parts.len == 2 && parts[1] == 'register' {
		return match method {
			'POST' { api.register_table(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

/// handle_tables_root는 /v1/{prefix}/tables 관련 요청을 처리합니다.
fn (mut api IcebergCatalogAPI) handle_tables_root(method string, parts []string, body string) (int, string) {
	// /tables/rename
	if parts.len == 1 && parts[0] == 'rename' {
		return match method {
			'POST' { api.rename_table(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

/// handle_transactions는 /v1/{prefix}/transactions 관련 요청을 처리합니다.
fn (mut api IcebergCatalogAPI) handle_transactions(method string, parts []string, body string) (int, string) {
	// /transactions/commit
	if parts.len == 1 && parts[0] == 'commit' {
		return match method {
			'POST' { api.commit_transaction(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

// =============================================================================
// Namespace 엔드포인트 구현
// =============================================================================

fn (mut api IcebergCatalogAPI) list_namespaces() (int, string) {
	// 기본 네임스페이스 목록 반환 (실제 구현에서는 catalog에서 조회)
	resp := s3.ListNamespacesResponse{
		namespaces: [['default']]
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) create_namespace(body string) (int, string) {
	req := json.decode(s3.CreateNamespaceRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.namespace.len == 0 {
		return api.error_response(400, 'Namespace is required')
	}

	// 카탈로그에 네임스페이스 생성
	api.catalog.create_namespace(req.namespace) or {
		return api.error_response(500, 'Failed to create namespace: ${err}')
	}

	resp := s3.CreateNamespaceResponse{
		namespace:  req.namespace
		properties: req.properties
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) get_namespace(namespace string) (int, string) {
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	resp := s3.GetNamespaceResponse{
		namespace:  ns
		properties: {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) delete_namespace(namespace string) (int, string) {
	// 네임스페이스 삭제는 비어있을 때만 가능
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	// 테이블이 있으면 삭제 불가
	tables := api.catalog.list_tables(ns) or {
		return api.error_response(500, 'Failed to list tables: ${err}')
	}

	if tables.len > 0 {
		return api.error_response(409, 'Namespace not empty')
	}

	return 204, ''
}

fn (mut api IcebergCatalogAPI) update_namespace_properties(namespace string, body string) (int, string) {
	req := json.decode(s3.UpdateNamespacePropertiesRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	ns := [namespace]
	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	resp := s3.UpdateNamespacePropertiesResponse{
		updated: req.updates.keys()
		removed: req.removals
		missing: []
	}
	return 200, resp.to_json()
}

// =============================================================================
// Table 엔드포인트 구현
// =============================================================================

fn (mut api IcebergCatalogAPI) list_tables(namespace string) (int, string) {
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	tables := api.catalog.list_tables(ns) or {
		return api.error_response(500, 'Failed to list tables: ${err}')
	}

	mut identifiers := []s3.TableIdentifierRest{}
	for table in tables {
		identifiers << s3.TableIdentifierRest{
			namespace: table.namespace
			name:      table.name
		}
	}

	resp := s3.ListTablesResponse{
		identifiers: identifiers
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) create_table(namespace string, body string) (int, string) {
	req := json.decode(s3.CreateTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.name.len == 0 {
		return api.error_response(400, 'Table name is required')
	}

	ns := [namespace]
	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	identifier := s3.IcebergTableIdentifier{
		namespace: ns
		name:      req.name
	}

	// REST 스키마를 내부 형식으로 변환
	schema := api.rest_schema_to_internal(req.schema)
	spec := api.rest_partition_spec_to_internal(req.partition_spec)

	location := if req.location.len > 0 {
		req.location
	} else {
		'${api.warehouse}/${namespace}/${req.name}'
	}

	metadata := api.catalog.create_table(identifier, schema, spec, location) or {
		return api.error_response(500, 'Failed to create table: ${err}')
	}

	resp := s3.LoadTableResponse{
		metadata_location: '${location}/metadata/v1.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, location)
		config:            {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) load_table(namespace string, table string) (int, string) {
	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	metadata := api.catalog.load_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	location := metadata.location

	resp := s3.LoadTableResponse{
		metadata_location: '${location}/metadata/v${metadata.format_version}.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, location)
		config:            {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) commit_table(namespace string, table string, body string) (int, string) {
	req := json.decode(s3.CommitTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	// 현재 메타데이터 로드
	mut metadata := api.catalog.load_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	// 요구사항 검증
	for requirement in req.requirements {
		match requirement.typ {
			'assert-current-schema-id' {
				if metadata.current_schema_id != requirement.schema_id {
					return api.error_response(412, 'Schema ID mismatch')
				}
			}
			'assert-table-uuid' {
				if metadata.table_uuid != requirement.uuid {
					return api.error_response(412, 'Table UUID mismatch')
				}
			}
			else {}
		}
	}

	// 업데이트 적용
	for update in req.updates {
		match update.action {
			'add-schema' {
				schema := api.rest_schema_to_internal(update.schema)
				metadata.schemas << schema
			}
			'set-current-schema' {
				metadata.current_schema_id = update.schema.schema_id
			}
			'add-partition-spec' {
				spec := api.rest_partition_spec_to_internal(update.spec)
				metadata.partition_specs << spec
			}
			'set-default-spec' {
				metadata.default_spec_id = update.spec.spec_id
			}
			'add-snapshot' {
				snapshot := s3.IcebergSnapshot{
					snapshot_id:   update.snapshot.snapshot_id
					timestamp_ms:  update.snapshot.timestamp_ms
					manifest_list: update.snapshot.manifest_list
					schema_id:     update.snapshot.schema_id
					summary:       update.snapshot.summary
				}
				metadata.snapshots << snapshot
			}
			'set-snapshot-ref' {
				metadata.current_snapshot_id = update.snapshot.snapshot_id
			}
			'set-properties' {
				for key, value in update.properties {
					metadata.properties[key] = value
				}
			}
			'remove-properties' {
				for key in update.removals {
					metadata.properties.delete(key)
				}
			}
			else {}
		}
	}

	// 메타데이터 저장
	api.catalog.update_table(identifier, metadata) or {
		return api.error_response(500, 'Failed to update table: ${err}')
	}

	resp := s3.CommitTableResponse{
		metadata_location: '${metadata.location}/metadata/v${metadata.format_version}.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, metadata.location)
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) drop_table(namespace string, table string) (int, string) {
	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	api.catalog.drop_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	return 204, ''
}

fn (mut api IcebergCatalogAPI) register_table(namespace string, body string) (int, string) {
	_ := json.decode(s3.RegisterTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	// 등록은 현재 지원하지 않음 - 메타데이터 파일에서 직접 로드 필요
	return api.error_response(501, 'Register table not implemented')
}

fn (mut api IcebergCatalogAPI) rename_table(body string) (int, string) {
	_ := json.decode(s3.RenameTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	// 이름 변경은 현재 지원하지 않음
	return api.error_response(501, 'Rename table not implemented')
}

fn (mut api IcebergCatalogAPI) report_metrics(namespace string, table string, body string) (int, string) {
	// 메트릭 보고는 현재 무시 (로깅만)
	return 204, ''
}

fn (mut api IcebergCatalogAPI) commit_transaction(body string) (int, string) {
	// 멀티 테이블 트랜잭션은 현재 지원하지 않음
	return api.error_response(501, 'Multi-table transactions not implemented')
}

// =============================================================================
// 헬퍼 함수
// =============================================================================

fn (api &IcebergCatalogAPI) rest_schema_to_internal(schema s3.SchemaRest) s3.IcebergSchema {
	mut fields := []s3.IcebergField{}
	for field in schema.fields {
		fields << s3.IcebergField{
			id:            field.id
			name:          field.name
			typ:           field.typ
			required:      field.required
			default_value: field.initial_default
		}
	}
	return s3.IcebergSchema{
		schema_id:            schema.schema_id
		fields:               fields
		identifier_field_ids: schema.identifier_field_ids
	}
}

fn (api &IcebergCatalogAPI) rest_partition_spec_to_internal(spec s3.PartitionSpecRest) s3.IcebergPartitionSpec {
	mut fields := []s3.IcebergPartitionField{}
	for field in spec.fields {
		fields << s3.IcebergPartitionField{
			source_id:      field.source_id
			field_id:       field.field_id
			name:           field.name
			transform:      field.transform
			transform_args: field.transform_args
		}
	}
	return s3.IcebergPartitionSpec{
		spec_id: spec.spec_id
		fields:  fields
	}
}

fn (api &IcebergCatalogAPI) error_response(code int, message string) (int, string) {
	err := s3.iceberg_error(code, message)
	return code, err.to_json()
}
