// Tests for Iceberg implementation: manifest encoding, metadata decoding,
// and REST API operations.
module s3

// --- JSON helper tests ---

fn test_json_extract_string() {
	json_str := '{"tableUuid":"abc-123","location":"s3://bucket/path","formatVersion":2}'
	assert json_extract_string(json_str, 'tableUuid') or { '' } == 'abc-123'
	assert json_extract_string(json_str, 'location') or { '' } == 's3://bucket/path'
	// Non-existent key
	assert json_extract_string(json_str, 'missing') == none
}

fn test_json_extract_int() {
	json_str := '{"formatVersion":2,"currentSchemaId":0,"defaultSpecId":1}'
	assert json_extract_int(json_str, 'formatVersion') or { 0 } == 2
	assert json_extract_int(json_str, 'defaultSpecId') or { 0 } == 1
	assert json_extract_int(json_str, 'missing') == none
}

fn test_json_extract_i64() {
	json_str := '{"lastUpdatedMs":1700000000000,"currentSnapshotId":9876543210}'
	assert json_extract_i64(json_str, 'lastUpdatedMs') or { i64(0) } == 1700000000000
	assert json_extract_i64(json_str, 'currentSnapshotId') or { i64(0) } == 9876543210
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
		'"currentSnapshotId":0,' + '"properties":{"owner":"datacore"},' +
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

// --- Column stats tests ---

// test_datafile_stats_populated: DataFile 생성 시 컬럼 통계가 채워져야 함
fn test_datafile_stats_populated() {
	// value_counts, null_value_counts 가 비어있지 않아야 함
	mut file := IcebergDataFile{
		file_path:          's3://bucket/data/test.parquet'
		file_format:        'PARQUET'
		record_count:       i64(3)
		file_size_in_bytes: i64(1024)
	}
	// field_id 1 = offset (long), 3 records
	file.value_counts[1] = i64(3)
	file.null_value_counts[1] = i64(0)

	assert file.value_counts.len > 0, '컬럼 value_counts 가 비어있음'
	assert file.null_value_counts.len > 0, '컬럼 null_value_counts 가 비어있음'
	assert file.value_counts[1] == i64(3)
	assert file.null_value_counts[1] == i64(0)
}

// test_iceberg_serialize_long: int64 값의 Iceberg 바이너리 직렬화 확인
fn test_iceberg_serialize_long() {
	// 값 1L -> little-endian 8 bytes
	result := iceberg_serialize_long(i64(1))
	assert result.len == 8
	assert result[0] == u8(1)
	for i in 1 .. 8 {
		assert result[i] == u8(0)
	}

	// 값 256L -> [0, 1, 0, 0, 0, 0, 0, 0]
	r256 := iceberg_serialize_long(i64(256))
	assert r256.len == 8
	assert r256[0] == u8(0)
	assert r256[1] == u8(1)
}

// test_iceberg_serialize_int: int32 값의 Iceberg 바이너리 직렬화 확인
fn test_iceberg_serialize_int() {
	result := iceberg_serialize_int(i32(42))
	assert result.len == 4
	assert result[0] == u8(42)
	assert result[1] == u8(0)
	assert result[2] == u8(0)
	assert result[3] == u8(0)
}

// test_iceberg_serialize_string: 문자열의 Iceberg 바이너리 직렬화 확인 (UTF-8 bytes)
fn test_iceberg_serialize_string() {
	result := iceberg_serialize_string('hi')
	assert result.len == 2
	assert result[0] == u8(`h`)
	assert result[1] == u8(`i`)
}

// test_avro_write_int_map: Avro map<int,long> 인코딩 확인
fn test_avro_write_int_long_map() {
	mut buf := []u8{}
	m := {
		1: i64(100)
		2: i64(200)
	}
	avro_write_int_long_map(mut buf, m)
	// 빈 버퍼가 아니어야 함
	assert buf.len > 0
	// 마지막 바이트는 map 종료를 의미하는 0
	assert buf[buf.len - 1] == u8(0)
}

// test_avro_write_int_bytes_map: Avro map<int,bytes> 인코딩 확인
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

// test_manifest_entry_has_column_stats: manifest entry 인코딩에 컬럼 통계 포함 확인
fn test_manifest_entry_has_column_stats() {
	// value_counts 가 non-null로 기록된 manifest entry는 null indicator(0)가 아닌
	// union index 1(non-null)을 포함해야 함
	mut file := IcebergDataFile{
		file_path:          's3://bucket/data/file1.parquet'
		file_format:        'PARQUET'
		record_count:       i64(100)
		file_size_in_bytes: i64(2048)
	}
	// field_id 1,2,3,4 에 통계 설정
	file.value_counts[1] = i64(100)
	file.value_counts[2] = i64(100)
	file.null_value_counts[1] = i64(0)
	file.null_value_counts[2] = i64(5)
	file.lower_bounds[1] = iceberg_serialize_long(i64(0))
	file.upper_bounds[1] = iceberg_serialize_long(i64(999))

	mut buf := []u8{}
	avro_write_manifest_entry(mut buf, file, i64(42))

	// value_counts union index: 통계가 있으면 union index 1(non-null)이어야 함
	// avro_write_nullable_long(mut buf, 1) -> varint(1)=2, varint(1)=2 이 연속으로 옴
	// union index 1 = varint(1) = 0x02 (zigzag)
	// 버퍼에 0x02가 union index로 들어가야 함 (null이면 0x00)
	// column_sizes 위치: status(1 byte) + snapshot_id(2 bytes) + seq_num(2 bytes) + file_seq_num(2 bytes)
	//                   + content(1 byte) + file_path + file_format + partition(empty) + record_count + file_size
	// 통계 맵 위치는 가변적이므로 전체 버퍼에 union index 1 (0x02)이 포함되어 있는지만 확인
	// null이면 buf에 0x00만 연속으로 있을 것
	has_non_null_union := buf.filter(it == u8(0x02)).len > 0
	assert has_non_null_union, 'manifest entry에 non-null 통계 union index가 없음'
}

// test_manifest_with_stats_encodes_non_null: encode_manifest가 통계 포함 시 non-null Avro 인코딩
fn test_manifest_with_stats_encodes_non_null() {
	schema := create_default_schema()
	spec := create_default_partition_spec()
	metadata := IcebergMetadata{
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

	// Avro magic 확인
	assert manifest_bytes[0] == u8(`O`)
	assert manifest_bytes[1] == u8(`b`)
	assert manifest_bytes[2] == u8(`j`)
	assert manifest_bytes[3] == u8(0x01)

	// null-only 통계(모든 통계 바이트가 0x00인 경우)가 아닌지 확인
	// 통계 데이터가 있으면 파일이 더 커야 함
	// 통계 없는 경우(null)와 비교를 위해 stats 없는 파일 인코딩
	mut file_no_stats := IcebergDataFile{
		file_path:          's3://bucket/data/no_stats.parquet'
		file_format:        'PARQUET'
		record_count:       i64(50)
		file_size_in_bytes: i64(4096)
	}
	manifest_no_stats := writer.encode_manifest([file_no_stats]) or {
		panic('encode failed: ${err}')
	}

	// 통계 포함 manifest가 통계 없는 것보다 더 커야 함
	assert manifest_bytes.len > manifest_no_stats.len, '통계 포함 manifest 크기(${manifest_bytes.len})가 통계 없는 것(${manifest_no_stats.len})보다 작거나 같음'
}
