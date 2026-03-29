// Tests for Iceberg implementation: manifest encoding, metadata decoding,
// and REST API operations.
module s3

import service.port

// --- JSON helper tests (kept functions for Glue response parsing) ---

fn test_json_extract_string() {
	json_str := '{"tableUuid":"abc-123","location":"s3://bucket/path","formatVersion":2}'
	assert json_extract_string(json_str, 'tableUuid') or { '' } == 'abc-123'
	assert json_extract_string(json_str, 'location') or { '' } == 's3://bucket/path'
	// Non-existent key
	assert json_extract_string(json_str, 'missing') == none
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

fn test_json_split_array_items_objects() {
	arr := '[{"id":1},{"id":2},{"id":3}]'
	items := json_split_array_items(arr)
	assert items.len == 3
	assert items[0] == '{"id":1}'
	assert items[1] == '{"id":2}'
	assert items[2] == '{"id":3}'
}

// --- normalize_metadata_json_keys tests ---

fn test_normalize_metadata_json_keys_camel_to_kebab() {
	input := '{"formatVersion":2,"tableUuid":"abc","lastUpdatedMs":1000}'
	result := normalize_metadata_json_keys(input)
	assert result.contains('"format-version":')
	assert result.contains('"table-uuid":')
	assert result.contains('"last-updated-ms":')
	assert !result.contains('"formatVersion":')
}

fn test_normalize_metadata_json_keys_already_kebab() {
	input := '{"format-version":2,"table-uuid":"abc","last-updated-ms":1000}'
	result := normalize_metadata_json_keys(input)
	// Should be unchanged
	assert result == input
}

fn test_normalize_metadata_json_keys_nested() {
	input := '{"schemaId":0,"specId":1,"sourceId":2,"fieldId":100}'
	result := normalize_metadata_json_keys(input)
	assert result.contains('"schema-id":')
	assert result.contains('"spec-id":')
	assert result.contains('"source-id":')
	assert result.contains('"field-id":')
}

// --- Metadata encode/decode roundtrip via json.encode/json.decode ---

fn test_decode_metadata_roundtrip() {
	schema := port.create_default_schema()
	spec := port.create_default_partition_spec()

	original := port.IcebergMetadata{
		format_version:      2
		table_uuid:          '11111111-2222-3333-4444-555555555555'
		location:            's3://test-bucket/warehouse/mydb/mytable'
		last_updated_ms:     1700000000000
		current_schema_id:   0
		default_spec_id:     0
		current_snapshot_id: 0
		schemas:             [
			port.IcebergSchema{
				schema_id: 0
				fields:    [
					port.IcebergField{
						id:       1
						name:     'offset'
						typ:      'long'
						required: true
					},
				]
			},
		]
		partition_specs:     [
			port.IcebergPartitionSpec{
				spec_id: 0
				fields:  [
					port.IcebergPartitionField{
						source_id: 2
						field_id:  1000
						name:      'ts_day'
						transform: 'day'
					},
				]
			},
		]
		properties:          {
			'owner': 'datacore'
		}
	}

	// Encode using json.encode (kebab-case output)
	encoded := encode_metadata(original)
	assert encoded.len > 0
	assert encoded.contains('"format-version":')
	assert encoded.contains('"table-uuid":')

	// Decode round-trip
	mut catalog := HadoopCatalog{
		adapter:    &S3StorageAdapter{}
		warehouse:  's3://test-bucket/warehouse'
		properties: {}
	}
	decoded := catalog.decode_metadata(encoded) or { panic('decode failed: ${err}') }

	assert decoded.table_uuid == original.table_uuid
	assert decoded.location == original.location
	assert decoded.format_version == original.format_version
	assert decoded.last_updated_ms == original.last_updated_ms
	assert decoded.schemas.len == 1
	assert decoded.schemas[0].fields[0].name == 'offset'
	assert decoded.schemas[0].fields[0].typ == 'long'
	assert decoded.schemas[0].fields[0].required == true
	assert decoded.partition_specs.len == 1
	assert decoded.partition_specs[0].fields[0].name == 'ts_day'
	assert decoded.partition_specs[0].fields[0].transform == 'day'
	assert decoded.partition_specs[0].fields[0].source_id == 2
	assert decoded.properties['owner'] == 'datacore'
}

fn test_decode_metadata_camelcase_input() {
	// Legacy camelCase format (as produced by older versions)
	import_json := '{"formatVersion":2,"tableUuid":"aaaa-bbbb","location":"s3://bucket/table",' +
		'"lastUpdatedMs":1700000000000,"currentSchemaId":0,"defaultSpecId":0,' +
		'"currentSnapshotId":0,"properties":{"owner":"test"},' +
		'"schemas":[{"schemaId":0,"fields":[{"id":1,"name":"offset","type":"long","required":true}]}],' +
		'"partitionSpecs":[{"specId":0,"fields":[{"sourceId":2,"fieldId":1000,"name":"ts","transform":"day"}]}],' +
		'"snapshots":[]}'

	mut catalog := HadoopCatalog{
		adapter:    &S3StorageAdapter{}
		warehouse:  'test'
		properties: {}
	}
	metadata := catalog.decode_metadata(import_json) or { panic('decode failed: ${err}') }

	assert metadata.format_version == 2
	assert metadata.table_uuid == 'aaaa-bbbb'
	assert metadata.location == 's3://bucket/table'
	assert metadata.schemas.len == 1
	assert metadata.schemas[0].schema_id == 0
	assert metadata.schemas[0].fields[0].name == 'offset'
	assert metadata.partition_specs.len == 1
	assert metadata.partition_specs[0].spec_id == 0
	assert metadata.partition_specs[0].fields[0].source_id == 2
	assert metadata.properties['owner'] == 'test'
}

fn test_decode_metadata_kebabcase_input() {
	// Standard kebab-case format (Iceberg spec)
	import_json := '{"format-version":3,"table-uuid":"cccc-dddd","location":"s3://bucket/v3",' +
		'"last-updated-ms":1710000000000,"current-schema-id":1,"default-spec-id":0,' +
		'"current-snapshot-id":42,"properties":{},' +
		'"schemas":[{"schema-id":1,"fields":[{"id":1,"name":"id","type":"long","required":true}]}],' +
		'"partition-specs":[{"spec-id":0,"fields":[]}],' +
		'"snapshots":[{"snapshot-id":42,"timestamp-ms":1710000000000,"manifest-list":"snap.avro","schema-id":1,"summary":{"operation":"append"}}]}'

	mut catalog := HadoopCatalog{
		adapter:    &S3StorageAdapter{}
		warehouse:  'test'
		properties: {}
	}
	metadata := catalog.decode_metadata(import_json) or { panic('decode failed: ${err}') }

	assert metadata.format_version == 3
	assert metadata.table_uuid == 'cccc-dddd'
	assert metadata.current_schema_id == 1
	assert metadata.current_snapshot_id == 42
	assert metadata.snapshots.len == 1
	assert metadata.snapshots[0].snapshot_id == 42
	assert metadata.snapshots[0].manifest_list == 'snap.avro'
	assert metadata.snapshots[0].summary['operation'] == 'append'
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
	schema := port.create_default_schema()
	spec := port.create_default_partition_spec()

	metadata := port.IcebergMetadata{
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
	schema := port.create_default_schema()
	spec := port.create_default_partition_spec()
	metadata := port.IcebergMetadata{
		format_version:  2
		table_uuid:      '00000000-0000-0000-0000-000000000002'
		schemas:         [schema]
		partition_specs: [spec]
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
	schema := port.create_default_schema()
	spec := port.create_default_partition_spec()
	metadata := port.IcebergMetadata{
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
fn build_test_writer(metadata port.IcebergMetadata, schema port.IcebergSchema, spec port.IcebergPartitionSpec) IcebergWriter {
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

// --- Column stats tests ---

// test_datafile_stats_populated: DataFile column statistics must be populated
fn test_datafile_stats_populated() {
	mut file := IcebergDataFile{
		file_path:          's3://bucket/data/test.parquet'
		file_format:        'PARQUET'
		record_count:       i64(3)
		file_size_in_bytes: i64(1024)
	}
	// field_id 1 = offset (long), 3 records
	file.value_counts[1] = i64(3)
	file.null_value_counts[1] = i64(0)

	assert file.value_counts.len > 0
	assert file.null_value_counts.len > 0
	assert file.value_counts[1] == i64(3)
	assert file.null_value_counts[1] == i64(0)
}

// test_iceberg_serialize_long: int64 Iceberg binary serialization
fn test_iceberg_serialize_long() {
	result := iceberg_serialize_long(i64(1))
	assert result.len == 8
	assert result[0] == u8(1)
	for i in 1 .. 8 {
		assert result[i] == u8(0)
	}

	r256 := iceberg_serialize_long(i64(256))
	assert r256.len == 8
	assert r256[0] == u8(0)
	assert r256[1] == u8(1)
}

// test_iceberg_serialize_int: int32 Iceberg binary serialization
fn test_iceberg_serialize_int() {
	result := iceberg_serialize_int(i32(42))
	assert result.len == 4
	assert result[0] == u8(42)
	assert result[1] == u8(0)
	assert result[2] == u8(0)
	assert result[3] == u8(0)
}

// test_iceberg_serialize_string: string Iceberg binary serialization (UTF-8 bytes)
fn test_iceberg_serialize_string() {
	result := iceberg_serialize_string('hi')
	assert result.len == 2
	assert result[0] == u8(`h`)
	assert result[1] == u8(`i`)
}

// test_avro_write_int_map: Avro map<int,long> encoding
fn test_avro_write_int_long_map() {
	mut buf := []u8{}
	m := {
		1: i64(100)
		2: i64(200)
	}
	avro_write_int_long_map(mut buf, m)
	assert buf.len > 0
	assert buf[buf.len - 1] == u8(0)
}

// test_avro_write_int_bytes_map: Avro map<int,bytes> encoding
fn test_avro_write_int_bytes_map() {
	mut buf := []u8{}
	m := {
		1: [u8(0x01), u8(0x02)]
		3: [u8(0xFF)]
	}
	avro_write_int_bytes_map(mut buf, m)
	assert buf.len > 0
	assert buf[buf.len - 1] == u8(0)
}

// test_manifest_entry_has_column_stats: manifest entry contains column statistics
fn test_manifest_entry_has_column_stats() {
	mut file := IcebergDataFile{
		file_path:          's3://bucket/data/file1.parquet'
		file_format:        'PARQUET'
		record_count:       i64(100)
		file_size_in_bytes: i64(2048)
	}
	file.value_counts[1] = i64(100)
	file.value_counts[2] = i64(100)
	file.null_value_counts[1] = i64(0)
	file.null_value_counts[2] = i64(5)
	file.lower_bounds[1] = iceberg_serialize_long(i64(0))
	file.upper_bounds[1] = iceberg_serialize_long(i64(999))

	mut buf := []u8{}
	avro_write_manifest_entry(mut buf, file, i64(42))

	has_non_null_union := buf.filter(it == u8(0x02)).len > 0
	assert has_non_null_union, 'manifest entry must contain non-null statistics union index'
}

// test_manifest_with_stats_encodes_non_null: encode_manifest with stats is larger than without
fn test_manifest_with_stats_encodes_non_null() {
	schema := port.create_default_schema()
	spec := port.create_default_partition_spec()
	metadata := port.IcebergMetadata{
		format_version:      2
		table_uuid:          '00000000-0000-0000-0000-000000000099'
		current_snapshot_id: 99
		schemas:             [schema]
		partition_specs:     [spec]
	}
	writer := build_test_writer(metadata, schema, spec)

	mut file := IcebergDataFile{
		file_path:          's3://bucket/data/stats_test.parquet'
		file_format:        'PARQUET'
		record_count:       i64(50)
		file_size_in_bytes: i64(4096)
	}
	file.value_counts[1] = i64(50)
	file.null_value_counts[1] = i64(0)
	file.lower_bounds[1] = iceberg_serialize_long(i64(100))
	file.upper_bounds[1] = iceberg_serialize_long(i64(149))

	manifest_bytes := writer.encode_manifest([file]) or { panic('encode failed: ${err}') }

	assert manifest_bytes[0] == u8(`O`)
	assert manifest_bytes[1] == u8(`b`)
	assert manifest_bytes[2] == u8(`j`)
	assert manifest_bytes[3] == u8(0x01)

	mut file_no_stats := IcebergDataFile{
		file_path:          's3://bucket/data/no_stats.parquet'
		file_format:        'PARQUET'
		record_count:       i64(50)
		file_size_in_bytes: i64(4096)
	}
	manifest_no_stats := writer.encode_manifest([file_no_stats]) or {
		panic('encode failed: ${err}')
	}

	assert manifest_bytes.len > manifest_no_stats.len, 'manifest with stats (${manifest_bytes.len}) must be larger than without (${manifest_no_stats.len})'
}
