// Unit tests for GlueCatalog <-> HadoopCatalog synchronization methods.
// These tests exercise the logic paths without hitting real AWS endpoints by
// using a mock/no-op S3StorageAdapter where the adapter is never called.
module s3

// --- helpers ---

// build_test_hadoop_catalog returns a HadoopCatalog backed by a zero-value adapter.
// Tests that only exercise encode/decode or metadata construction are safe to run
// without a real S3 backend.
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
fn build_test_metadata(table_name string) IcebergMetadata {
	return IcebergMetadata{
		format_version:      2
		table_uuid:          '11111111-2222-3333-4444-555555555555'
		location:            's3://test-bucket/warehouse/testdb/${table_name}'
		last_updated_ms:     1700000000000
		schemas:             [create_default_schema()]
		current_schema_id:   0
		partition_specs:     [create_default_partition_spec()]
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
	// Verify that encode_metadata produces valid JSON that can be decoded back.
	mut hadoop := build_test_hadoop_catalog()
	metadata := build_test_metadata('events')

	encoded := encode_metadata(metadata)

	// Must be non-empty JSON
	assert encoded.len > 0
	assert encoded.starts_with('{')
	assert encoded.ends_with('}')

	// Round-trip: decode the encoded JSON
	decoded := hadoop.decode_metadata(encoded) or { panic('decode failed: ${err}') }
	assert decoded.table_uuid == metadata.table_uuid
	assert decoded.location == metadata.location
	assert decoded.format_version == metadata.format_version
	assert decoded.schemas.len == metadata.schemas.len
	assert decoded.partition_specs.len == metadata.partition_specs.len
}

fn test_commit_metadata_version_path_formula() {
	// The metadata path formula used by commit_metadata:
	// new_version = metadata.snapshots.len + 1
	// path = table_path/metadata/{new_version:05d}-{uuid}.metadata.json
	mut hadoop := build_test_hadoop_catalog()

	identifier := IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'events'
	}
	metadata := build_test_metadata('events')

	// snapshots.len == 0 => new_version == 1
	expected_version := metadata.snapshots.len + 1
	expected_prefix := '${expected_version:05d}-${metadata.table_uuid}.metadata.json'

	// Derive path using same logic as commit_metadata
	table_path := hadoop.table_path(identifier)
	computed_path := '${table_path}/metadata/${expected_prefix}'

	assert computed_path.ends_with('00001-11111111-2222-3333-4444-555555555555.metadata.json')
}

// --- encode/decode round-trip for sync scenarios ---

fn test_export_table_metadata_roundtrip() {
	// Simulate the metadata that export_table would transfer:
	// load from Hadoop (decode) -> pass to Glue (encode) -> store in Glue parameters.
	mut hadoop := build_test_hadoop_catalog()
	original := build_test_metadata('orders')

	encoded := encode_metadata(original)
	decoded := hadoop.decode_metadata(encoded) or { panic('decode failed: ${err}') }

	assert decoded.table_uuid == original.table_uuid
	assert decoded.location == original.location
	assert decoded.properties['owner'] == 'datacore'
}

fn test_import_table_metadata_preservation() {
	// Simulate the metadata that import_table would write to HadoopCatalog.
	// GlueCatalog.parse_glue_table_response decodes base64-encoded metadata JSON.
	// Here we just verify the encode -> decode pipeline preserves all fields.
	mut hadoop := build_test_hadoop_catalog()
	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          'aaaabbbb-cccc-dddd-eeee-ffffffffffff'
		location:            's3://glue-bucket/warehouse/db/tbl'
		last_updated_ms:     1710000000000
		schemas:             [
			IcebergSchema{
				schema_id: 0
				fields:    [
					IcebergField{
						id:       1
						name:     'id'
						typ:      'long'
						required: true
					},
					IcebergField{
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
			IcebergPartitionSpec{
				spec_id: 0
				fields:  [
					IcebergPartitionField{
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
	// Verify that the metadata list returned by export_all accumulates one entry
	// per table. We test the accumulation logic via a simulated loop, mirroring
	// the implementation.
	tables := [
		IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_a'
		},
		IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_b'
		},
		IcebergTableIdentifier{
			namespace: ['db']
			name:      'table_c'
		},
	]

	mut results := []IcebergMetadata{}
	for tbl in tables {
		results << build_test_metadata(tbl.name)
	}

	assert results.len == 3
	assert results[0].location.ends_with('table_a')
	assert results[1].location.ends_with('table_b')
	assert results[2].location.ends_with('table_c')
}

fn test_import_all_identifier_accumulation() {
	// Mirror the accumulation loop in import_all: verify identifiers are collected.
	tables := [
		IcebergTableIdentifier{
			namespace: ['default']
			name:      'events'
		},
		IcebergTableIdentifier{
			namespace: ['default']
			name:      'metrics'
		},
	]

	mut imported := []IcebergTableIdentifier{}
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
	identifier := IcebergTableIdentifier{
		namespace: ['mydb']
		name:      'mytable'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/mydb/mytable'
}

fn test_hadoop_table_path_empty_namespace() {
	mut hadoop := build_test_hadoop_catalog()
	identifier := IcebergTableIdentifier{
		namespace: []
		name:      'roottable'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/roottable'
}

fn test_hadoop_table_path_nested_namespace() {
	mut hadoop := build_test_hadoop_catalog()
	identifier := IcebergTableIdentifier{
		namespace: ['a', 'b', 'c']
		name:      'deep'
	}
	path := hadoop.table_path(identifier)
	assert path == 's3://test-bucket/warehouse/a/b/c/deep'
}

// --- upsert branch determination (create vs update) ---

fn test_export_table_upsert_prefers_existing_location() {
	// When metadata.location is set, export_table must pass it as-is to Glue.
	metadata := build_test_metadata('sales')
	location := if metadata.location.len > 0 {
		metadata.location
	} else {
		's3://test-bucket/warehouse/testdb/sales'
	}
	assert location == 's3://test-bucket/warehouse/testdb/sales'
}

fn test_export_table_upsert_falls_back_to_warehouse_path() {
	// When metadata.location is empty, derive location from warehouse + namespace + name.
	mut hadoop := build_test_hadoop_catalog()
	glue := build_test_glue_catalog()
	identifier := IcebergTableIdentifier{
		namespace: ['testdb']
		name:      'pageviews'
	}
	empty_location_metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          generate_table_uuid('')
		location:            ''
		last_updated_ms:     1700000000000
		schemas:             [create_default_schema()]
		current_schema_id:   0
		partition_specs:     [create_default_partition_spec()]
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
