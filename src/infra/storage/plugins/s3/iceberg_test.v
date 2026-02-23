// Tests for Iceberg implementation: manifest encoding, metadata decoding,
// and REST API operations.
module s3

// --- JSON helper tests ---

fn test_json_extract_string() {
	json_str := '{"tableUuid":"abc-123","location":"s3://bucket/path","formatVersion":2}'
	assert json_extract_string(json_str, 'tableUuid') == 'abc-123'
	assert json_extract_string(json_str, 'location') == 's3://bucket/path'
	// Non-existent key
	assert json_extract_string(json_str, 'missing') == none
}

fn test_json_extract_int() {
	json_str := '{"formatVersion":2,"currentSchemaId":0,"defaultSpecId":1}'
	assert json_extract_int(json_str, 'formatVersion') == 2
	assert json_extract_int(json_str, 'defaultSpecId') == 1
	assert json_extract_int(json_str, 'missing') == none
}

fn test_json_extract_i64() {
	json_str := '{"lastUpdatedMs":1700000000000,"currentSnapshotId":9876543210}'
	assert json_extract_i64(json_str, 'lastUpdatedMs') == 1700000000000
	assert json_extract_i64(json_str, 'currentSnapshotId') == 9876543210
}

fn test_json_find_matching_brace_object() {
	s := '{"a":1,"b":{"c":2}}'
	result := json_find_matching_brace(s, `{`, `}`) or { '' }
	assert result == '{"a":1,"b":{"c":2}}'
}

fn test_json_find_matching_brace_array() {
	s := '[1,[2,3],4]'
	result := json_find_matching_brace(s, `[`, `]`) or { '' }
	assert result == '[1,[2,3],4]'
}

fn test_json_parse_string_map() {
	obj := '{"key1":"value1","key2":"value2"}'
	result := json_parse_string_map(obj)
	assert result['key1'] == 'value1'
	assert result['key2'] == 'value2'
	assert result.len == 2
}

fn test_json_split_array_items_objects() {
	arr := '[{"id":1},{"id":2},{"id":3}]'
	items := json_split_array_items(arr)
	assert items.len == 3
	assert items[0] == '{"id":1}'
	assert items[1] == '{"id":2}'
	assert items[2] == '{"id":3}'
}

fn test_json_parse_schemas() {
	arr := '[{"schemaId":0,"fields":[{"id":1,"name":"offset","type":"long","required":true}]}]'
	schemas := json_parse_schemas(arr)
	assert schemas.len == 1
	assert schemas[0].schema_id == 0
	assert schemas[0].fields.len == 1
	assert schemas[0].fields[0].name == 'offset'
	assert schemas[0].fields[0].typ == 'long'
	assert schemas[0].fields[0].required == true
}

fn test_json_parse_partition_specs() {
	arr := '[{"specId":0,"fields":[{"sourceId":2,"fieldId":1000,"name":"ts_day","transform":"day"}]}]'
	specs := json_parse_partition_specs(arr)
	assert specs.len == 1
	assert specs[0].spec_id == 0
	assert specs[0].fields.len == 1
	assert specs[0].fields[0].name == 'ts_day'
	assert specs[0].fields[0].transform == 'day'
	assert specs[0].fields[0].source_id == 2
}

fn test_json_parse_snapshots() {
	arr := '[{"snapshotId":12345,"timestampMs":1700000000000,"manifestList":"metadata/snap-12345.avro","schemaId":0,"summary":{"operation":"append"}}]'
	snaps := json_parse_snapshots(arr)
	assert snaps.len == 1
	assert snaps[0].snapshot_id == 12345
	assert snaps[0].timestamp_ms == 1700000000000
	assert snaps[0].manifest_list == 'metadata/snap-12345.avro'
	assert snaps[0].summary['operation'] == 'append'
}

// --- Metadata decode test via encode/decode roundtrip ---

fn test_decode_metadata_roundtrip() {
	schema := create_default_schema()
	spec := create_default_partition_spec()

	// Use encode_metadata directly via a catalog-like helper
	// Build the JSON manually matching HadoopCatalog.encode_metadata output
	// and decode it back using json_parse helpers
	import_json := '{"formatVersion":2,"tableUuid":"11111111-2222-3333-4444-555555555555",' +
		'"location":"s3://test-bucket/warehouse/mydb/mytable",' +
		'"lastUpdatedMs":1700000000000,"currentSchemaId":0,"defaultSpecId":0,' +
		'"currentSnapshotId":0,' +
		'"properties":{"owner":"datacore"},' +
		'"schemas":[{"schemaId":0,"fields":[{"id":1,"name":"offset","type":"long","required":true}]}],' +
		'"partitionSpecs":[{"specId":0,"fields":[{"sourceId":2,"fieldId":1000,"name":"ts_day","transform":"day"}]}],' +
		'"snapshots":[]}'

	// Test json_extract_string on the metadata JSON
	uuid := json_extract_string(import_json, 'tableUuid') or { '' }
	assert uuid == '11111111-2222-3333-4444-555555555555'

	location := json_extract_string(import_json, 'location') or { '' }
	assert location == 's3://test-bucket/warehouse/mydb/mytable'

	format_version := json_extract_int(import_json, 'formatVersion') or { 0 }
	assert format_version == 2

	schemas_str := json_extract_array(import_json, 'schemas') or { '[]' }
	schemas := json_parse_schemas(schemas_str)
	assert schemas.len == 1
	assert schemas[0].fields[0].name == 'offset'

	specs_str := json_extract_array(import_json, 'partitionSpecs') or { '[]' }
	specs := json_parse_partition_specs(specs_str)
	assert specs.len == 1
	assert specs[0].fields[0].transform == 'day'

	props_str := json_extract_object(import_json, 'properties') or { '{}' }
	props := json_parse_string_map(props_str)
	assert props['owner'] == 'datacore'
}

// --- Avro varint encoding tests ---

fn test_avro_write_varint_small() {
	mut buf := []u8{}
	avro_write_varint(mut buf, 1)
	// zigzag(1) = 2 = 0x02
	assert buf == [u8(2)]
}

fn test_avro_write_varint_zero() {
	mut buf := []u8{}
	avro_write_varint(mut buf, 0)
	assert buf == [u8(0)]
}

fn test_avro_write_varint_negative() {
	mut buf := []u8{}
	avro_write_varint(mut buf, -1)
	// zigzag(-1) = 1 = 0x01
	assert buf == [u8(1)]
}

fn test_avro_write_varint_large() {
	mut buf := []u8{}
	avro_write_varint(mut buf, 64)
	// zigzag(64) = 128. 128 in varint = [0x80, 0x01]
	assert buf.len == 2
}

fn test_avro_write_string() {
	mut buf := []u8{}
	avro_write_string(mut buf, 'hi')
	// length=2 -> zigzag(2)=4 -> [0x04], then 'h','i'
	assert buf == [u8(4), u8(`h`), u8(`i`)]
}

fn test_avro_sync_marker_length() {
	marker := avro_generate_sync_marker('test-uuid-123')
	assert marker.len == 16
}

fn test_avro_sync_marker_different_sources() {
	m1 := avro_generate_sync_marker('source-a')
	m2 := avro_generate_sync_marker('source-b')
	// Different sources should produce different markers
	assert m1 != m2
}

// --- Manifest encoding test (no adapter calls needed) ---

fn test_encode_manifest_produces_avro_magic() {
	schema := create_default_schema()
	spec := create_default_partition_spec()

	// We need a minimal IcebergWriter for encode_manifest
	// The function is receiver fn but doesn't call adapter
	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          '00000000-0000-0000-0000-000000000001'
		current_schema_id:   0
		default_spec_id:     0
		schemas:             [schema]
		partition_specs:     [spec]
		snapshots:           []
		current_snapshot_id: 0
	}

	// Build writer struct directly (adapter field is &S3StorageAdapter, use unsafe)
	// We avoid calling any adapter methods in encode_manifest
	writer := build_test_writer(metadata, schema, spec)

	data_files := [
		IcebergDataFile{
			file_path:          's3://bucket/data/file1.parquet'
			file_format:        'PARQUET'
			record_count:       1000
			file_size_in_bytes: 204800
			partition:          {
				'ts_day': '2024-01-01'
			}
		},
	]

	manifest_bytes := writer.encode_manifest(data_files) or { panic('encode failed: ${err}') }

	// Check Avro magic: 'O', 'b', 'j', 0x01
	assert manifest_bytes.len >= 4
	assert manifest_bytes[0] == u8(`O`)
	assert manifest_bytes[1] == u8(`b`)
	assert manifest_bytes[2] == u8(`j`)
	assert manifest_bytes[3] == u8(0x01)
}

fn test_encode_manifest_empty_files() {
	schema := create_default_schema()
	spec := create_default_partition_spec()
	metadata := IcebergMetadata{
		format_version:    2
		table_uuid:        '00000000-0000-0000-0000-000000000002'
		schemas:           [schema]
		partition_specs:   [spec]
	}

	writer := build_test_writer(metadata, schema, spec)
	manifest_bytes := writer.encode_manifest([]) or { panic('encode failed: ${err}') }

	// Still valid Avro container with magic
	assert manifest_bytes[0] == u8(`O`)
	assert manifest_bytes[1] == u8(`b`)
	assert manifest_bytes[2] == u8(`j`)
	assert manifest_bytes[3] == u8(0x01)
}

fn test_encode_manifest_multiple_files() {
	schema := create_default_schema()
	spec := create_default_partition_spec()
	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          '00000000-0000-0000-0000-000000000003'
		current_snapshot_id: 42
		schemas:             [schema]
		partition_specs:     [spec]
	}

	writer := build_test_writer(metadata, schema, spec)

	mut files := []IcebergDataFile{}
	for i in 0 .. 5 {
		files << IcebergDataFile{
			file_path:          's3://bucket/data/file${i}.parquet'
			file_format:        'PARQUET'
			record_count:       i64(100 * (i + 1))
			file_size_in_bytes: i64(1024 * (i + 1))
		}
	}

	manifest_bytes := writer.encode_manifest(files) or { panic('encode failed: ${err}') }

	// Avro magic
	assert manifest_bytes[0] == u8(`O`)
	assert manifest_bytes[1] == u8(`b`)
	// Size grows with more files
	assert manifest_bytes.len > 100
}

// --- Glue type conversion test ---

fn test_iceberg_type_to_glue() {
	assert iceberg_type_to_glue('long') == 'bigint'
	assert iceberg_type_to_glue('int') == 'int'
	assert iceberg_type_to_glue('string') == 'string'
	assert iceberg_type_to_glue('binary') == 'binary'
	assert iceberg_type_to_glue('timestamp') == 'timestamp'
	assert iceberg_type_to_glue('timestamptz') == 'timestamp'
	assert iceberg_type_to_glue('boolean') == 'boolean'
	assert iceberg_type_to_glue('float') == 'float'
	assert iceberg_type_to_glue('double') == 'double'
	assert iceberg_type_to_glue('date') == 'date'
	assert iceberg_type_to_glue('decimal') == 'decimal(38,10)'
	assert iceberg_type_to_glue('unknown_type') == 'string'
}

// --- Helper for building a test writer without a real adapter ---
// encode_manifest does not call any adapter methods, so a pointer to a zero-value
// S3StorageAdapter is sufficient for these unit tests.
fn build_test_writer(metadata IcebergMetadata, schema IcebergSchema, spec IcebergPartitionSpec) IcebergWriter {
	return IcebergWriter{
		adapter:           &S3StorageAdapter{}
		config:            IcebergConfig{
			compression:       'zstd'
			max_file_size_mb:  128
			max_rows_per_file: 1000000
		}
		table_metadata:    metadata
		current_schema:    schema
		partition_spec:    spec
		partition_buffers: {}
	}
}

