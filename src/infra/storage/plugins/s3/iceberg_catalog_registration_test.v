// Tests for IcebergWriter catalog auto-registration feature.
// Verifies that new_iceberg_writer_with_catalog registers the table
// in the catalog if it does not already exist.
module s3

import service.port

// --- MockCatalog: in-memory catalog for testing ---

struct MockCatalog {
pub mut:
	tables       map[string]port.IcebergMetadata
	create_count int
	load_count   int
}

fn (mut c MockCatalog) create_table(identifier port.IcebergTableIdentifier, schema port.IcebergSchema, spec port.IcebergPartitionSpec, location string) !port.IcebergMetadata {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	if key in c.tables {
		return error('Table already exists: ${identifier.name}')
	}
	metadata := port.IcebergMetadata{
		format_version:      2
		table_uuid:          port.generate_table_uuid(location)
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

fn (mut c MockCatalog) load_table(identifier port.IcebergTableIdentifier) !port.IcebergMetadata {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.load_count++
	if meta := c.tables[key] {
		return meta
	}
	return error('Table not found: ${identifier.name}')
}

fn (mut c MockCatalog) load_metadata_at(metadata_location string) !port.IcebergMetadata {
	return error('not implemented')
}

fn (mut c MockCatalog) update_table(identifier port.IcebergTableIdentifier, metadata port.IcebergMetadata) ! {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.tables[key] = metadata
}

fn (mut c MockCatalog) drop_table(identifier port.IcebergTableIdentifier) ! {
	key := '${identifier.namespace.join('.')}:${identifier.name}'
	c.tables.delete(key)
}

fn (mut c MockCatalog) list_tables(namespace []string) ![]port.IcebergTableIdentifier {
	return []port.IcebergTableIdentifier{}
}

fn (mut c MockCatalog) namespace_exists(namespace []string) bool {
	return true
}

fn (mut c MockCatalog) create_namespace(namespace []string) ! {
}

// --- Tests ---

fn test_new_iceberg_writer_with_catalog_registers_table() {
	mut mock := MockCatalog{}
	identifier := port.IcebergTableIdentifier{
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
		mock, identifier) or { panic('writer creation failed: ${err}') }

	assert mock.create_count == 1, 'catalog.create_table must be called once. actual: ${mock.create_count}'
	key := 'testdb:test_topic_p0'
	assert key in mock.tables, 'table must be registered in catalog'
}

fn test_new_iceberg_writer_with_catalog_skips_existing_table() {
	mut mock := MockCatalog{}
	identifier := port.IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'existing_table'
	}
	table_location := 's3://test-bucket/warehouse/testdb/existing_table'
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}

	mock.tables['testdb:existing_table'] = port.IcebergMetadata{
		format_version: 2
		location:       table_location
	}

	_ = new_iceberg_writer_with_catalog(&S3StorageAdapter{}, config, table_location, mut
		mock, identifier) or { panic('writer creation with existing table failed: ${err}') }

	assert mock.create_count == 0, 'existing table must not trigger create_count. actual: ${mock.create_count}'
}

fn test_new_iceberg_writer_without_catalog_works() {
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}
	writer := new_iceberg_writer(&S3StorageAdapter{}, config, 's3://test/table') or {
		panic('writer creation failed: ${err}')
	}
	assert writer.table_metadata.location == 's3://test/table'
}

fn test_iceberg_writer_has_catalog_identifier() {
	mut mock := MockCatalog{}
	identifier := port.IcebergTableIdentifier{
		namespace: ['db']
		name:      'events'
	}
	config := IcebergConfig{
		compression:       'zstd'
		max_file_size_mb:  128
		max_rows_per_file: 1000000
	}
	writer := new_iceberg_writer_with_catalog(&S3StorageAdapter{}, config, 's3://test-bucket/warehouse/db/events', mut
		mock, identifier) or { panic('writer creation failed: ${err}') }
	assert writer.table_identifier.name == 'events', 'table_identifier.name must be events'
	assert writer.table_identifier.namespace == ['db'], 'table_identifier.namespace must be [db]'
}
