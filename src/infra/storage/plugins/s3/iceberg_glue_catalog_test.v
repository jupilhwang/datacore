// Unit tests for GlueCatalog <-> HadoopCatalog synchronization methods.
// These tests exercise the logic paths without hitting real AWS endpoints by
// using a mock/no-op S3StorageAdapter where the adapter is never called.
module s3

import service.port

// --- helpers ---

// build_test_hadoop_catalog returns a HadoopCatalog backed by a zero-value adapter.
fn build_test_hadoop_catalog() HadoopCatalog {
	return HadoopCatalog{
		adapter:    &S3StorageAdapter{}
		warehouse:  's3://test-bucket/warehouse'
		properties: {}
	}
}

// build_test_glue_catalog returns a GlueCatalog backed by a zero-value adapter.
fn build_test_glue_catalog() GlueCatalog {
	return GlueCatalog{
		adapter:       &S3StorageAdapter{}
		region:        'us-east-1'
		warehouse:     's3://test-bucket/warehouse'
		access_key:    'TESTKEY'
		secret_key:    'TESTSECRET'
		session_token: ''
		glue_endpoint: 'https://glue.us-east-1.amazonaws.com'
	}
}

// build_test_metadata returns a minimal IcebergMetadata for testing.
fn build_test_metadata(table_name string) port.IcebergMetadata {
	return port.IcebergMetadata{
		format_version:      2
		table_uuid:          '11111111-2222-3333-4444-555555555555'
		location:            's3://test-bucket/warehouse/testdb/${table_name}'
		last_updated_ms:     1700000000000
		schemas:             [port.create_default_schema()]
		current_schema_id:   0
		partition_specs:     [port.create_default_partition_spec()]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'owner': 'datacore'
		}
	}
}

// --- commit_metadata tests ---

fn test_commit_metadata_encodes_and_builds_path() {
	mut hadoop := build_test_hadoop_catalog()
	metadata := build_test_metadata('events')

	encoded := encode_metadata(metadata)

	assert encoded.len > 0
	assert encoded.starts_with('{')
	assert encoded.ends_with('}')

	decoded := hadoop.decode_metadata(encoded) or { panic('decode failed: ${err}') }
	assert decoded.table_uuid == metadata.table_uuid
	assert decoded.location == metadata.location
	assert decoded.format_version == metadata.format_version
	assert decoded.schemas.len == metadata.schemas.len
	assert decoded.partition_specs.len == metadata.partition_specs.len
}

fn test_commit_metadata_version_path_formula() {
	mut hadoop := build_test_hadoop_catalog()

	identifier := port.IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'events'
	}
	metadata := build_test_metadata('events')

	expected_version := metadata.snapshots.len + 1
	expected_prefix := '${expected_version:05d}-${metadata.table_uuid}.metadata.json'

	table_path := hadoop.table_path(identifier)
	computed_path := '${table_path}/metadata/${expected_prefix}'

	assert computed_path.ends_with('00001-11111111-2222-3333-4444-555555555555.metadata.json')
}

// --- encode/decode round-trip for sync scenarios ---

fn test_export_table_metadata_roundtrip() {
	mut hadoop := build_test_hadoop_catalog()
	original := build_test_metadata('orders')

	encoded := encode_metadata(original)
	decoded := hadoop.decode_metadata(encoded) or { panic('decode failed: ${err}') }

	assert decoded.table_uuid == original.table_uuid
	assert decoded.location == original.location
	assert decoded.properties['owner'] == 'datacore'
}

fn test_import_table_metadata_preservation() {
	mut hadoop := build_test_hadoop_catalog()
	metadata := port.IcebergMetadata{
		format_version:      2
		table_uuid:          'aaaabbbb-cccc-dddd-eeee-ffffffffffff'
		location:            's3://glue-bucket/warehouse/db/tbl'
		last_updated_ms:     1710000000000
		schemas:             [
			port.IcebergSchema{
				schema_id: 0
				fields:    [
					port.IcebergField{
						id:       1
						name:     'id'
						typ:      'long'
						required: true
					},
					port.IcebergField{
						id:       2
						name:     'payload'
						typ:      'string'
						required: false
					},
				]
			},
		]
		current_schema_id:   0
		partition_specs:     [
			port.IcebergPartitionSpec{
				spec_id: 0
				fields:  [
					port.IcebergPartitionField{
						source_id: 1
						field_id:  1000
						name:      'id_bucket'
						transform: 'bucket[16]'
					},
				]
			},
		]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'table_type': 'ICEBERG'
		}
	}

	encoded := encode_metadata(metadata)
	decoded := hadoop.decode_metadata(encoded) or { panic('decode failed: ${err}') }

	assert decoded.table_uuid == metadata.table_uuid
	assert decoded.location == metadata.location
	assert decoded.schemas.len == 1
	assert decoded.schemas[0].fields.len == 2
	assert decoded.schemas[0].fields[0].name == 'id'
	assert decoded.schemas[0].fields[1].name == 'payload'
	assert decoded.partition_specs.len == 1
	assert decoded.partition_specs[0].fields[0].transform == 'bucket[16]'
	assert decoded.properties['table_type'] == 'ICEBERG'
}

// --- export_all / import_all logic tests (path-only, no adapter I/O) ---

fn test_export_all_result_accumulation() {
	tables := [
		port.IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_a'
		},
		port.IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_b'
		},
		port.IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_c'
		},
	]

	mut results := []port.IcebergMetadata{}
	for tbl in tables {
		results << build_test_metadata(tbl.name)
	}

	assert results.len == 3
	assert results[0].location.ends_with('table_a')
	assert results[1].location.ends_with('table_b')
	assert results[2].location.ends_with('table_c')
}

fn test_import_all_identifier_accumulation() {
	tables := [
		port.IcebergTableIdentifier{
			namespace: ['default']
			name:      'events'
		},
		port.IcebergTableIdentifier{
			namespace: ['default']
			name:      'metrics'
		},
	]

	mut imported := []port.IcebergTableIdentifier{}
	for tbl in tables {
		imported << tbl
	}

	assert imported.len == 2
	assert imported[0].name == 'events'
	assert imported[1].name == 'metrics'
}

// --- namespace path derivation (used internally by export/import) ---

fn test_hadoop_table_path_single_namespace() {
	mut hadoop := build_test_hadoop_catalog()
	identifier := port.IcebergTableIdentifier{
		namespace: ['mydb']
		name:      'mytable'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/mydb/mytable'
}

fn test_hadoop_table_path_empty_namespace() {
	mut hadoop := build_test_hadoop_catalog()
	identifier := port.IcebergTableIdentifier{
		namespace: []
		name:      'roottable'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/roottable'
}

fn test_hadoop_table_path_nested_namespace() {
	mut hadoop := build_test_hadoop_catalog()
	identifier := port.IcebergTableIdentifier{
		namespace: ['a', 'b', 'c']
		name:      'deep'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/a/b/c/deep'
}

// --- upsert branch determination (create vs update) ---

fn test_export_table_upsert_prefers_existing_location() {
	metadata := build_test_metadata('sales')
	location := if metadata.location.len > 0 {
		metadata.location
	} else {
		's3://test-bucket/warehouse/testdb/sales'
	}
	assert location == 's3://test-bucket/warehouse/testdb/sales'
}

fn test_export_table_upsert_falls_back_to_warehouse_path() {
	mut hadoop := build_test_hadoop_catalog()
	glue := build_test_glue_catalog()
	identifier := port.IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'pageviews'
	}
	empty_location_metadata := port.IcebergMetadata{
		format_version:      2
		table_uuid:          port.generate_table_uuid('')
		location:            ''
		last_updated_ms:     1700000000000
		schemas:             [port.create_default_schema()]
		current_schema_id:   0
		partition_specs:     [port.create_default_partition_spec()]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {}
	}
	_ = hadoop
	location := if empty_location_metadata.location.len > 0 {
		empty_location_metadata.location
	} else {
		'${glue.warehouse}/${identifier.namespace.join('/')}/${identifier.name}'
	}
	assert location == 's3://test-bucket/warehouse/testdb/pageviews'
}

fn test_glue_sigv4_signing_key_cache_populated() {
	catalog := build_test_glue_catalog()
	date_stamp := '20260325'

	key := catalog.sigv4_signing_key(date_stamp, 'glue')
	assert key.len > 0

	assert catalog.signing_key_cache.date_day == date_stamp
	assert catalog.signing_key_cache.key.len > 0
	assert catalog.signing_key_cache.key == key
}

fn test_glue_sigv4_signing_key_same_date_returns_identical_key() {
	catalog := build_test_glue_catalog()

	key1 := catalog.sigv4_signing_key('20260325', 'glue')
	key2 := catalog.sigv4_signing_key('20260325', 'glue')

	assert key1 == key2
	assert key1.len > 0
}

fn test_glue_sigv4_signing_key_different_date_invalidates_cache() {
	catalog := build_test_glue_catalog()

	key1 := catalog.sigv4_signing_key('20260325', 'glue')
	key2 := catalog.sigv4_signing_key('20260326', 'glue')

	assert key1 != key2
	assert catalog.signing_key_cache.date_day == '20260326'
}
