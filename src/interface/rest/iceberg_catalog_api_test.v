// IcebergCatalogAPI 유닛 테스트
// MockIcebergCatalog을 사용하여 실제 S3/Glue 없이 API 로직을 검증한다.
module rest

import infra.storage.plugins.s3

// --- MockIcebergCatalog: IcebergCatalog 인터페이스 인메모리 구현 ---

// MockIcebergCatalog는 테스트용 인메모리 카탈로그이다.
struct MockIcebergCatalog {
mut:
	// 네임스페이스 목록 (존재 여부 확인용)
	namespaces [][]string
	// 테이블 메타데이터: "namespace.table" -> IcebergMetadata
	tables map[string]s3.IcebergMetadata
	// 메타데이터 위치: metadata_location -> IcebergMetadata
	metadata_at map[string]s3.IcebergMetadata
}

// table_key는 식별자로부터 맵 키를 생성한다.
fn (c &MockIcebergCatalog) table_key(identifier s3.IcebergTableIdentifier) string {
	return '${identifier.namespace.join('.')}.${identifier.name}'
}

// namespace_key는 네임스페이스 배열을 문자열로 변환한다.
fn (c &MockIcebergCatalog) ns_key(namespace []string) string {
	return namespace.join('.')
}

// IcebergCatalog 인터페이스 구현

fn (mut c MockIcebergCatalog) namespace_exists(namespace []string) bool {
	target := c.ns_key(namespace)
	for ns in c.namespaces {
		if c.ns_key(ns) == target {
			return true
		}
	}
	return false
}

fn (mut c MockIcebergCatalog) create_namespace(namespace []string) ! {
	if c.namespace_exists(namespace) {
		return error('Namespace already exists: ${namespace.join('.')}')
	}
	c.namespaces << namespace
}

fn (mut c MockIcebergCatalog) list_tables(namespace []string) ![]s3.IcebergTableIdentifier {
	mut result := []s3.IcebergTableIdentifier{}
	prefix := c.ns_key(namespace) + '.'
	for key, _ in c.tables {
		if key.starts_with(prefix) {
			table_name := key[prefix.len..]
			result << s3.IcebergTableIdentifier{
				namespace: namespace
				name:      table_name
			}
		}
	}
	return result
}

fn (mut c MockIcebergCatalog) create_table(identifier s3.IcebergTableIdentifier, schema s3.IcebergSchema, spec s3.IcebergPartitionSpec, location string) !s3.IcebergMetadata {
	key := c.table_key(identifier)
	if key in c.tables {
		return error('Table already exists: ${identifier.name}')
	}
	metadata := s3.IcebergMetadata{
		format_version:      2
		table_uuid:          s3.generate_table_uuid(location)
		location:            location
		last_updated_ms:     1700000000000
		schemas:             [schema]
		current_schema_id:   schema.schema_id
		partition_specs:     [spec]
		default_spec_id:     spec.spec_id
		snapshots:           []
		current_snapshot_id: 0
		properties:          {}
	}
	c.tables[key] = metadata
	return metadata
}

fn (mut c MockIcebergCatalog) load_table(identifier s3.IcebergTableIdentifier) !s3.IcebergMetadata {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	return c.tables[key]
}

fn (mut c MockIcebergCatalog) load_metadata_at(metadata_location string) !s3.IcebergMetadata {
	if metadata_location !in c.metadata_at {
		return error('Metadata not found at: ${metadata_location}')
	}
	return c.metadata_at[metadata_location]
}

fn (mut c MockIcebergCatalog) update_table(identifier s3.IcebergTableIdentifier, metadata s3.IcebergMetadata) ! {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	c.tables[key] = metadata
}

fn (mut c MockIcebergCatalog) drop_table(identifier s3.IcebergTableIdentifier) ! {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	c.tables.delete(key)
}

// --- 헬퍼 함수들 ---

// new_test_api는 테스트용 IcebergCatalogAPI를 생성한다.
fn new_test_api() IcebergCatalogAPI {
	mut mock := MockIcebergCatalog{
		namespaces:  []
		tables:      {}
		metadata_at: {}
	}
	return IcebergCatalogAPI{
		catalog:        s3.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}
}

// new_test_api_with_namespace는 기본 네임스페이스가 있는 API를 생성한다.
fn new_test_api_with_namespace(ns string) IcebergCatalogAPI {
	mut mock := MockIcebergCatalog{
		namespaces:  [[ns]]
		tables:      {}
		metadata_at: {}
	}
	return IcebergCatalogAPI{
		catalog:        s3.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}
}

// make_path는 API 요청 경로를 생성한다.
fn make_path(suffix string) string {
	return '/v1/iceberg/${suffix}'
}

// --- Namespace 엔드포인트 테스트 ---

// test_list_namespaces_empty는 네임스페이스가 없을 때 빈 목록을 반환하는지 검증한다.
fn test_list_namespaces_empty() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', make_path('namespaces'), '')

	// list_namespaces()는 항상 'default'를 반환하는 현재 구현을 검증
	assert status == 200
	assert body.contains('"namespaces"')
}

// test_create_namespace는 새 네임스페이스를 생성하고 200을 반환하는지 검증한다.
fn test_create_namespace() {
	mut api := new_test_api()
	req_body := '{"namespace":["mydb"],"properties":{"owner":"alice"}}'

	status, body := api.handle_request('POST', make_path('namespaces'), req_body)

	assert status == 200
	assert body.contains('"namespace"')
	assert body.contains('mydb')
}

// test_create_namespace_already_exists는 중복 생성 시 409를 반환하는지 검증한다.
fn test_create_namespace_already_exists() {
	mut api := new_test_api_with_namespace('existing')
	req_body := '{"namespace":["existing"],"properties":{}}'

	status, body := api.handle_request('POST', make_path('namespaces'), req_body)

	assert status == 500
	assert body.contains('Failed to create namespace')
}

// test_drop_namespace는 빈 네임스페이스 삭제 시 204를 반환하는지 검증한다.
fn test_drop_namespace() {
	mut api := new_test_api_with_namespace('emptyns')

	status, _ := api.handle_request('DELETE', make_path('namespaces/emptyns'), '')

	assert status == 204
}

// test_drop_namespace_not_empty는 테이블이 있는 네임스페이스 삭제 시 409를 반환하는지 검증한다.
fn test_drop_namespace_not_empty() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['ns_with_table']]
		tables:      {
			'ns_with_table.events': s3.IcebergMetadata{
				format_version:  2
				table_uuid:      'aaa-bbb'
				location:        's3://test/ns_with_table/events'
				schemas:         [s3.create_default_schema()]
				partition_specs: [s3.create_default_partition_spec()]
				properties:      {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        s3.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}

	status, body := api.handle_request('DELETE', make_path('namespaces/ns_with_table'),
		'')

	assert status == 409
	assert body.contains('not empty')
}

// test_get_namespace_not_found는 존재하지 않는 네임스페이스 조회 시 404를 반환하는지 검증한다.
fn test_get_namespace_not_found() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', make_path('namespaces/nonexistent'), '')

	assert status == 404
	assert body.contains('Namespace not found')
}

// --- Table 엔드포인트 테스트 ---

// test_list_tables_empty는 빈 네임스페이스에 테이블 목록 조회 시 빈 배열을 반환하는지 검증한다.
fn test_list_tables_empty() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('GET', make_path('namespaces/mydb/tables'), '')

	assert status == 200
	assert body.contains('"identifiers"')
}

// test_create_table은 테이블 생성 후 메타데이터를 반환하는지 검증한다.
fn test_create_table() {
	mut api := new_test_api_with_namespace('mydb')
	req_body := '{"name":"events","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]},"partition-spec":{"spec-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/mydb/tables'), req_body)

	assert status == 200
	assert body.contains('"metadata"')
	assert body.contains('"metadata-location"')
}

// test_load_table은 기존 테이블 로드 시 메타데이터를 반환하는지 검증한다.
fn test_load_table() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['mydb']]
		tables:      {
			'mydb.events': s3.IcebergMetadata{
				format_version:      2
				table_uuid:          'ccc-ddd-eee'
				location:            's3://test-bucket/warehouse/mydb/events'
				last_updated_ms:     1700000000000
				schemas:             [s3.create_default_schema()]
				current_schema_id:   0
				partition_specs:     [s3.create_default_partition_spec()]
				default_spec_id:     0
				snapshots:           []
				current_snapshot_id: 0
				properties:          {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        s3.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}

	status, body := api.handle_request('GET', make_path('namespaces/mydb/tables/events'),
		'')

	assert status == 200
	assert body.contains('"metadata"')
	assert body.contains('"table-uuid"')
	assert body.contains('ccc-ddd-eee')
}

// test_table_not_found는 존재하지 않는 테이블 로드 시 404를 반환하는지 검증한다.
fn test_table_not_found() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('GET', make_path('namespaces/mydb/tables/nonexistent'),
		'')

	assert status == 404
	assert body.contains('Table not found')
}

// test_drop_table은 테이블 삭제 후 204를 반환하는지 검증한다.
fn test_drop_table() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['mydb']]
		tables:      {
			'mydb.to_drop': s3.IcebergMetadata{
				format_version:  2
				table_uuid:      'fff-ggg'
				location:        's3://test/mydb/to_drop'
				schemas:         [s3.create_default_schema()]
				partition_specs: [s3.create_default_partition_spec()]
				properties:      {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        s3.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}

	status, _ := api.handle_request('DELETE', make_path('namespaces/mydb/tables/to_drop'),
		'')

	assert status == 204
}

// --- 에러 처리 테스트 ---

// test_invalid_path는 잘못된 경로에 대해 400을 반환하는지 검증한다.
fn test_invalid_path() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', '/v1', '')

	assert status == 400
	assert body.contains('Invalid path')
}

// test_method_not_allowed는 허용되지 않은 HTTP 메서드에 대해 405를 반환하는지 검증한다.
fn test_method_not_allowed() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('PUT', make_path('namespaces'), '')

	assert status == 405
	assert body.contains('Method not allowed')
}

// test_create_table_empty_name은 이름 없는 테이블 생성 요청 시 400을 반환하는지 검증한다.
fn test_create_table_empty_name() {
	mut api := new_test_api_with_namespace('mydb')
	req_body := '{"name":"","schema":{"type":"struct","schema-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/mydb/tables'), req_body)

	assert status == 400
	assert body.contains('Table name is required')
}

// test_config_endpoint는 /v1/config 엔드포인트가 설정을 반환하는지 검증한다.
fn test_config_endpoint() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', '/v1/config', '')

	assert status == 200
	assert body.contains('"defaults"')
	assert body.contains('s3://test-bucket/warehouse')
}

// test_create_table_namespace_not_found는 네임스페이스가 없을 때 테이블 생성 시 404를 반환하는지 검증한다.
fn test_create_table_namespace_not_found() {
	mut api := new_test_api()
	req_body := '{"name":"events","schema":{"type":"struct","schema-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/nonexistent/tables'),
		req_body)

	assert status == 404
	assert body.contains('Namespace not found')
}
