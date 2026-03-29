// IcebergCatalogAPI unit tests
// Uses MockIcebergCatalog to verify API logic without real S3/Glue.
module rest

import service.port

// --- MockIcebergCatalog: IcebergCatalog in-memory implementation ---

struct MockIcebergCatalog {
mut:
	namespaces  [][]string
	tables      map[string]port.IcebergMetadata
	metadata_at map[string]port.IcebergMetadata
}

fn (c &MockIcebergCatalog) table_key(identifier port.IcebergTableIdentifier) string {
	return '${identifier.namespace.join('.')}.${identifier.name}'
}

fn (c &MockIcebergCatalog) ns_key(namespace []string) string {
	return namespace.join('.')
}

// IcebergCatalog interface implementation

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

fn (mut c MockIcebergCatalog) list_tables(namespace []string) ![]port.IcebergTableIdentifier {
	mut result := []port.IcebergTableIdentifier{}
	prefix := c.ns_key(namespace) + '.'
	for key, _ in c.tables {
		if key.starts_with(prefix) {
			table_name := key[prefix.len..]
			result << port.IcebergTableIdentifier{
				namespace: namespace
				name:      table_name
			}
		}
	}
	return result
}

fn (mut c MockIcebergCatalog) create_table(identifier port.IcebergTableIdentifier, schema port.IcebergSchema, spec port.IcebergPartitionSpec, location string) !port.IcebergMetadata {
	key := c.table_key(identifier)
	if key in c.tables {
		return error('Table already exists: ${identifier.name}')
	}
	metadata := port.IcebergMetadata{
		format_version:      2
		table_uuid:          port.generate_table_uuid(location)
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

fn (mut c MockIcebergCatalog) load_table(identifier port.IcebergTableIdentifier) !port.IcebergMetadata {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	return c.tables[key]
}

fn (mut c MockIcebergCatalog) load_metadata_at(metadata_location string) !port.IcebergMetadata {
	if metadata_location !in c.metadata_at {
		return error('Metadata not found at: ${metadata_location}')
	}
	return c.metadata_at[metadata_location]
}

fn (mut c MockIcebergCatalog) update_table(identifier port.IcebergTableIdentifier, metadata port.IcebergMetadata) ! {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	c.tables[key] = metadata
}

fn (mut c MockIcebergCatalog) drop_table(identifier port.IcebergTableIdentifier) ! {
	key := c.table_key(identifier)
	if key !in c.tables {
		return error('Table not found: ${identifier.name}')
	}
	c.tables.delete(key)
}

// --- Helper functions ---

fn new_test_api() IcebergCatalogAPI {
	mut mock := MockIcebergCatalog{
		namespaces:  []
		tables:      {}
		metadata_at: {}
	}
	return IcebergCatalogAPI{
		catalog:        port.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}
}

fn new_test_api_with_namespace(ns string) IcebergCatalogAPI {
	mut mock := MockIcebergCatalog{
		namespaces:  [[ns]]
		tables:      {}
		metadata_at: {}
	}
	return IcebergCatalogAPI{
		catalog:        port.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}
}

fn make_path(suffix string) string {
	return '/v1/iceberg/${suffix}'
}

// --- Namespace endpoint tests ---

fn test_list_namespaces_empty() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', make_path('namespaces'), '')

	assert status == 200
	assert body.contains('"namespaces"')
}

fn test_create_namespace() {
	mut api := new_test_api()
	req_body := '{"namespace":["mydb"],"properties":{"owner":"alice"}}'

	status, body := api.handle_request('POST', make_path('namespaces'), req_body)

	assert status == 200
	assert body.contains('"namespace"')
	assert body.contains('mydb')
}

fn test_create_namespace_already_exists() {
	mut api := new_test_api_with_namespace('existing')
	req_body := '{"namespace":["existing"],"properties":{}}'

	status, body := api.handle_request('POST', make_path('namespaces'), req_body)

	assert status == 500
	assert body.contains('Failed to create namespace')
}

fn test_drop_namespace() {
	mut api := new_test_api_with_namespace('emptyns')

	status, _ := api.handle_request('DELETE', make_path('namespaces/emptyns'), '')

	assert status == 204
}

fn test_drop_namespace_not_empty() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['ns_with_table']]
		tables:      {
			'ns_with_table.events': port.IcebergMetadata{
				format_version:  2
				table_uuid:      'aaa-bbb'
				location:        's3://test/ns_with_table/events'
				schemas:         [port.create_default_schema()]
				partition_specs: [port.create_default_partition_spec()]
				properties:      {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        port.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}

	status, body := api.handle_request('DELETE', make_path('namespaces/ns_with_table'),
		'')

	assert status == 409
	assert body.contains('not empty')
}

fn test_get_namespace_not_found() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', make_path('namespaces/nonexistent'), '')

	assert status == 404
	assert body.contains('Namespace not found')
}

// --- Table endpoint tests ---

fn test_list_tables_empty() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('GET', make_path('namespaces/mydb/tables'), '')

	assert status == 200
	assert body.contains('"identifiers"')
}

fn test_create_table() {
	mut api := new_test_api_with_namespace('mydb')
	req_body := '{"name":"events","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]},"partition-spec":{"spec-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/mydb/tables'), req_body)

	assert status == 200
	assert body.contains('"metadata"')
	assert body.contains('"metadata-location"')
}

fn test_load_table() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['mydb']]
		tables:      {
			'mydb.events': port.IcebergMetadata{
				format_version:      2
				table_uuid:          'ccc-ddd-eee'
				location:            's3://test-bucket/warehouse/mydb/events'
				last_updated_ms:     1700000000000
				schemas:             [port.create_default_schema()]
				current_schema_id:   0
				partition_specs:     [port.create_default_partition_spec()]
				default_spec_id:     0
				snapshots:           []
				current_snapshot_id: 0
				properties:          {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        port.IcebergCatalog(mock)
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

fn test_table_not_found() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('GET', make_path('namespaces/mydb/tables/nonexistent'),
		'')

	assert status == 404
	assert body.contains('Table not found')
}

fn test_drop_table() {
	mut mock := MockIcebergCatalog{
		namespaces:  [['mydb']]
		tables:      {
			'mydb.to_drop': port.IcebergMetadata{
				format_version:  2
				table_uuid:      'fff-ggg'
				location:        's3://test/mydb/to_drop'
				schemas:         [port.create_default_schema()]
				partition_specs: [port.create_default_partition_spec()]
				properties:      {}
			}
		}
		metadata_at: {}
	}
	mut api := IcebergCatalogAPI{
		catalog:        port.IcebergCatalog(mock)
		warehouse:      's3://test-bucket/warehouse'
		prefix:         'iceberg'
		format_version: 2
	}

	status, _ := api.handle_request('DELETE', make_path('namespaces/mydb/tables/to_drop'),
		'')

	assert status == 204
}

// --- Error handling tests ---

fn test_invalid_path() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', '/v1', '')

	assert status == 400
	assert body.contains('Invalid path')
}

fn test_method_not_allowed() {
	mut api := new_test_api_with_namespace('mydb')

	status, body := api.handle_request('PUT', make_path('namespaces'), '')

	assert status == 405
	assert body.contains('Method not allowed')
}

fn test_create_table_empty_name() {
	mut api := new_test_api_with_namespace('mydb')
	req_body := '{"name":"","schema":{"type":"struct","schema-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/mydb/tables'), req_body)

	assert status == 400
	assert body.contains('Table name is required')
}

fn test_config_endpoint() {
	mut api := new_test_api()

	status, body := api.handle_request('GET', '/v1/config', '')

	assert status == 200
	assert body.contains('"defaults"')
	assert body.contains('s3://test-bucket/warehouse')
}

fn test_create_table_namespace_not_found() {
	mut api := new_test_api()
	req_body := '{"name":"events","schema":{"type":"struct","schema-id":0,"fields":[]}}'

	status, body := api.handle_request('POST', make_path('namespaces/nonexistent/tables'),
		req_body)

	assert status == 404
	assert body.contains('Namespace not found')
}
