// Tests for IcebergWriter catalog auto-registration feature.
// Verifies that new_iceberg_writer_with_catalog registers the table
// in the catalog if it does not already exist.
module s3

// --- MockCatalog: in-memory catalog for testing ---

struct MockCatalog {
pub mut:
	tables       map[string]IcebergMetadata
	create_count int
	load_count   int
}

fn (mut c MockCatalog) create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	if key in c.tables {
		return error('Table already exists: ${identifier.name}')
	}
	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          generate_table_uuid(location)
		location:            location
		schemas:             [schema]
		current_schema_id:   0
		partition_specs:     [spec]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {}
	}
	c.tables[key] = metadata
	c.create_count++
	return metadata
}

fn (mut c MockCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.load_count++
	if meta := c.tables[key] {
		return meta
	}
	return error('Table not found: ${identifier.name}')
}

fn (mut c MockCatalog) load_metadata_at(metadata_location string) !IcebergMetadata {
	return error('not implemented')
}

fn (mut c MockCatalog) update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.tables[key] = metadata
}

fn (mut c MockCatalog) drop_table(identifier IcebergTableIdentifier) ! {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.tables.delete(key)
}

fn (mut c MockCatalog) list_tables(namespace []string) ![]IcebergTableIdentifier {
	return []IcebergTableIdentifier{}
}

fn (mut c MockCatalog) namespace_exists(namespace []string) bool {
	return true
}

fn (mut c MockCatalog) create_namespace(namespace []string) ! {
}

// --- Tests ---

// new_iceberg_writer_with_catalog가 catalog에 테이블을 등록하는지 검증 [RED]
fn test_new_iceberg_writer_with_catalog_registers_table() {
	mut mock := MockCatalog{}
	identifier := IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'test_topic_p0'
	}
	table_location := 's3://test-bucket/warehouse/testdb/test_topic_p0'
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
		format_version:    2
	}

	_ = new_iceberg_writer_with_catalog(&S3StorageAdapter{}, config, table_location, mut
		mock, identifier) or { panic('writer 생성 실패: ${err}') }

	// catalog에 테이블이 등록되었는지 확인
	assert mock.create_count == 1, 'catalog.create_table이 1회 호출되어야 함. 실제: ${mock.create_count}'
	key := 'testdb:test_topic_p0'
	assert key in mock.tables, 'catalog에 테이블 등록이 되어 있어야 함'
}

// 이미 테이블이 존재하면 에러 없이 건너뛰는지 검증 [RED]
fn test_new_iceberg_writer_with_catalog_skips_existing_table() {
	mut mock := MockCatalog{}
	identifier := IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'existing_table'
	}
	table_location := 's3://test-bucket/warehouse/testdb/existing_table'
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}

	// 사전에 테이블 등록
	mock.tables['testdb:existing_table'] = IcebergMetadata{
		location: table_location
	}

	// 이미 존재하는 경우 에러 없이 writer가 생성되어야 함
	_ = new_iceberg_writer_with_catalog(&S3StorageAdapter{}, config, table_location, mut
		mock, identifier) or { panic('기존 테이블 존재 시 writer 생성 실패: ${err}') }

	// create_table이 호출되지 않아야 함 (이미 존재하므로 건너뜀)
	assert mock.create_count == 0, '이미 존재하는 테이블은 create_count가 0이어야 함. 실제: ${mock.create_count}'
}

// writer 생성 시 catalog 없이 기존 방식 호환성 검증
fn test_new_iceberg_writer_without_catalog_works() {
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}
	writer := new_iceberg_writer(&S3StorageAdapter{}, config, 's3://test/table') or {
		panic('writer 생성 실패: ${err}')
	}
	assert writer.table_metadata.location == 's3://test/table'
}

// catalog가 있는 writer의 catalog 필드 접근 검증 [RED]
fn test_iceberg_writer_has_catalog_identifier() {
	mut mock := MockCatalog{}
	identifier := IcebergTableIdentifier{
		namespace: ['db']
		name:      'events'
	}
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}
	writer := new_iceberg_writer_with_catalog(&S3StorageAdapter{}, config, 's3://test-bucket/warehouse/db/events', mut
		mock, identifier) or { panic('writer 생성 실패: ${err}') }
	// writer의 table_identifier가 올바르게 설정되었는지 확인
	assert writer.table_identifier.name == 'events', 'table_identifier.name이 events여야 함'
	assert writer.table_identifier.namespace == ['db'], 'table_identifier.namespace가 [db]여야 함'
}
