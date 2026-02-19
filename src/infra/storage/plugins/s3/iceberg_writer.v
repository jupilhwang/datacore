// Iceberg 테이블 형식으로 Parquet 파일을 생성하고 메타데이터를 관리합니다.
module s3

import domain
import infra.encoding as parquet
import time
import json
import crypto.md5
import sync
import strings

/// IcebergWriter는 Iceberg 테이블에 데이터를 쓰는 기능을 제공합니다.
pub struct IcebergWriter {
pub mut:
	adapter           &S3StorageAdapter
	config            IcebergConfig
	table_metadata    IcebergMetadata
	partition_buffers map[string][]parquet.ParquetRecord
	buffer_lock       sync.Mutex
	current_schema    IcebergSchema
	partition_spec    IcebergPartitionSpec
	file_counter      int
}

/// new_iceberg_writer는 새로운 Iceberg Writer를 생성합니다.
pub fn new_iceberg_writer(adapter &S3StorageAdapter, config IcebergConfig, table_location string) !&IcebergWriter {
	// 기본 스키마 생성
	schema := create_default_schema()
	partition_spec := if config.partition_by.len > 0 {
		create_partition_spec_from_config(config.partition_by)
	} else {
		create_default_partition_spec()
	}

	// 테이블 메타데이터 초기화
	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          generate_table_uuid()
		location:            table_location
		last_updated_ms:     time.now().unix_milli()
		schemas:             [schema]
		current_schema_id:   0
		partition_specs:     [partition_spec]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'write_compression':                 config.compression
			'write_metadata_compression':        'gzip'
			'write_target_data_file_size_bytes': (config.max_file_size_mb * 1024 * 1024).str()
			'commit.retry.num-retries':          '5'
		}
	}

	return &IcebergWriter{
		adapter:           adapter
		config:            config
		table_metadata:    metadata
		partition_buffers: map[string][]parquet.ParquetRecord{}
		current_schema:    schema
		partition_spec:    partition_spec
		file_counter:      0
	}
}

/// append_records은 레코드를 파티션별로 버퍼링합니다.
pub fn (mut w IcebergWriter) append_records(topic string, partition int, records []domain.Record, start_offset i64) ! {
	if records.len == 0 {
		return
	}

	w.buffer_lock.lock()
	defer {
		w.buffer_lock.unlock()
	}

	for i, record in records {
		// 레코드의 파티션 값 계산
		partition_values := w.compute_partition_values(record)
		partition_key := w.partition_values_to_key(partition_values)

		// 헤더를 JSON 문자열로 변환
		mut headers_json := '{}'
		if record.headers.len > 0 {
			mut headers_map := map[string]string{}
			for key, value in record.headers {
				headers_map[key] = value.bytestr()
			}
			headers_json = json.encode(headers_map)
		}

		prec := parquet.ParquetRecord{
			offset:    start_offset + i64(i)
			timestamp: record.timestamp.unix_milli()
			topic:     topic
			partition: partition
			key:       record.key.clone()
			value:     record.value.clone()
			headers:   headers_json
		}

		// 파티션별 버퍼에 추가
		if partition_key !in w.partition_buffers {
			w.partition_buffers[partition_key] = []
		}
		w.partition_buffers[partition_key] << prec
	}
}

/// compute_partition_values은 레코드에서 파티션 값을 계산합니다.
fn (w &IcebergWriter) compute_partition_values(record domain.Record) map[string]string {
	mut values := map[string]string{}

	for field in w.partition_spec.fields {
		match field.transform {
			'identity' {
				// 원본 값을 그대로 사용
				match field.source_id {
					2 { values[field.name] = record.timestamp.format_ss()[0..10] }
					else { values[field.name] = 'unknown' }
				}
			}
			'day' {
				// 날짜로 변환 (YYYY-MM-DD)
				values[field.name] = record.timestamp.format_ss()[0..10]
			}
			'hour' {
				// 시간으로 변환 (YYYY-MM-DD-HH)
				values[field.name] = record.timestamp.format_ss()[0..13].replace(' ',
					'-')
			}
			'month' {
				// 월로 변환 (YYYY-MM)
				values[field.name] = record.timestamp.format_ss()[0..7]
			}
			'year' {
				// 연도로 변환 (YYYY)
				values[field.name] = record.timestamp.format_ss()[0..4]
			}
			else {
				values[field.name] = 'unknown'
			}
		}
	}

	return values
}

/// partition_values_to_key은 파티션 값을 키 문자열로 변환합니다.
fn (w &IcebergWriter) partition_values_to_key(values map[string]string) string {
	mut parts := []string{}
	for field in w.partition_spec.fields {
		if value := values[field.name] {
			parts << '${field.name}=${value}'
		}
	}
	return parts.join('/')
}

/// should_flush은 플러시가 필요한지 확인합니다.
pub fn (mut w IcebergWriter) should_flush() bool {
	w.buffer_lock.lock()
	defer {
		w.buffer_lock.unlock()
	}

	for _, records in w.partition_buffers {
		if records.len >= w.config.max_rows_per_file {
			return true
		}
	}

	return false
}

/// flush_all_partitions은 모든 파티션 버퍼를 플러시합니다.
pub fn (mut w IcebergWriter) flush_all_partitions(topic string, partition int) ![]IcebergDataFile {
	w.buffer_lock.lock()
	defer {
		w.buffer_lock.unlock()
	}

	mut data_files := []IcebergDataFile{}

	for partition_key, records in w.partition_buffers {
		if records.len == 0 {
			continue
		}

		// Parquet 파일로 인코딩
		mut encoder := parquet.new_parquet_encoder(w.config.compression, w.config.max_file_size_mb)!
		data, metadata := parquet.encode_batch(records, encoder.compression)!

		// 파일 경로 생성
		file_path := w.generate_data_file_path(topic, partition, partition_key)

		// S3에 업로드
		full_path := '${w.table_metadata.location}/${file_path}'
		w.adapter.put_object(full_path, data)!

		// 파티션 값 파싱
		partition_values := w.parse_partition_key(partition_key)

		// Iceberg DataFile 메타데이터 생성
		mut data_file := IcebergDataFile{
			file_path:          full_path
			file_format:        'PARQUET'
			record_count:       metadata.num_rows
			file_size_in_bytes: i64(data.len)
			column_sizes:       map[string]i64{}
			value_counts:       map[string]i64{}
			null_value_counts:  map[string]i64{}
			lower_bounds:       map[string]string{}
			upper_bounds:       map[string]string{}
			partition:          partition_values
		}

		// 컬럼 통계 추가
		for row_group in metadata.row_groups {
			for chunk in row_group.columns {
				col_name := chunk.column_name
				data_file.column_sizes[col_name] = chunk.data_size
				data_file.value_counts[col_name] = chunk.value_count
				data_file.null_value_counts[col_name] = chunk.null_count
				data_file.lower_bounds[col_name] = chunk.min_value
				data_file.upper_bounds[col_name] = chunk.max_value
			}
		}

		data_files << data_file

		// 버퍼 비우기
		w.partition_buffers[partition_key] = []
	}

	return data_files
}

/// parse_partition_key은 파티션 키를 파티션 값 맵으로 파싱합니다.
fn (w &IcebergWriter) parse_partition_key(key string) map[string]string {
	mut values := map[string]string{}
	parts := key.split('/')
	for part in parts {
		if part.contains('=') {
			kv := part.split('=')
			if kv.len == 2 {
				values[kv[0]] = kv[1]
			}
		}
	}
	return values
}

/// generate_data_file_path은 데이터 파일 경로를 생성합니다.
fn (mut w IcebergWriter) generate_data_file_path(topic string, partition int, partition_key string) string {
	w.file_counter++

	// 데이터 파일 경로: topics/{topic}/partitions/{partition}/data/{partition_key}/{counter}-{uuid}.parquet
	partition_values := w.parse_partition_key(partition_key)
	mut partition_path := ''
	for field in w.partition_spec.fields {
		if value := partition_values[field.name] {
			partition_path += '${field.name}=${value}/'
		}
	}

	// 고유 ID 생성
	now := time.now()
	unique_id := md5.sum('${now.unix_milli()}-${w.file_counter}'.bytes()).hex()

	return 'topics/${topic}/partitions/${partition}/data/${partition_path}${w.file_counter:05d}-${unique_id[0..8]}.parquet'
}

/// create_snapshot은 새로운 스냅샷을 생성합니다.
pub fn (mut w IcebergWriter) create_snapshot(data_files []IcebergDataFile, topic string) !IcebergSnapshot {
	snapshot_id := generate_snapshot_id()
	now := time.now()

	// 매니페스트 파일 생성
	manifest_path := w.generate_manifest_path(snapshot_id)
	manifest_content := w.encode_manifest(data_files)!
	w.adapter.put_object('${w.table_metadata.location}/${manifest_path}', manifest_content)!

	// 스냅샷 요약 정보
	mut added_files := 0
	mut added_records := i64(0)
	for file in data_files {
		added_files++
		added_records += file.record_count
	}

	// 파일 크기 총합 계산
	mut total_size := i64(0)
	for file in data_files {
		total_size += file.file_size_in_bytes
	}

	summary := {
		'operation':               'append'
		'added-data-files':        added_files.str()
		'added-records':           added_records.str()
		'added-files-size':        total_size.str()
		'changed-partition-count': added_files.str()
		'topic':                   topic
	}

	snapshot := IcebergSnapshot{
		snapshot_id:   snapshot_id
		timestamp_ms:  now.unix_milli()
		manifest_list: manifest_path
		schema_id:     w.table_metadata.current_schema_id
		summary:       summary
	}

	// 테이블 메타데이터 업데이트
	w.table_metadata.snapshots << snapshot
	w.table_metadata.current_snapshot_id = snapshot_id
	w.table_metadata.last_updated_ms = now.unix_milli()

	return snapshot
}

/// generate_manifest_path은 매니페스트 파일 경로를 생성합니다.
fn (w &IcebergWriter) generate_manifest_path(snapshot_id i64) string {
	now := time.now()
	return 'metadata/snap-${snapshot_id}-${now.format_ss().replace(' ', '-').replace(':',
		'-')}.avro'
}

/// encode_manifest은 매니페스트 파일 내용을 인코딩합니다.
/// 참고: 실제 Iceberg 매니페스트는 Avro 형식이며, 여기서는 JSON으로 모킹합니다.
fn (w &IcebergWriter) encode_manifest(data_files []IcebergDataFile) ![]u8 {
	// 간략화된 매니페스트 JSON - 문자열 직접 구성
	mut sb := strings.new_builder(2048)

	sb.write_string('{')
	sb.write_string('"manifestVersion":2,')
	sb.write_string('"schemaId":${w.table_metadata.current_schema_id},')
	sb.write_string('"partitionSpecId":${w.table_metadata.default_spec_id},')
	sb.write_string('"addedFiles":${data_files.len},')
	sb.write_string('"existingFiles":0,')
	sb.write_string('"deletedFiles":0,')

	// partitions
	sb.write_string('"partitions":[')
	for i, file in data_files {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('${json.encode(file.partition)}')
	}
	sb.write_string('],')

	// files
	sb.write_string('"files":[')
	for i, file in data_files {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{')
		sb.write_string('"filePath":"${file.file_path}",')
		sb.write_string('"fileFormat":"${file.file_format}",')
		sb.write_string('"recordCount":${file.record_count},')
		sb.write_string('"fileSizeInBytes":${file.file_size_in_bytes}')
		sb.write_string('}')
	}
	sb.write_string(']')

	sb.write_string('}')

	return sb.str().bytes()
}

/// encode_metadata_json은 테이블 메타데이터를 JSON으로 인코딩합니다.
fn (w &IcebergWriter) encode_metadata_json() string {
	// Iceberg 메타데이터 JSON 형식 - 문자열 직접 구성
	mut sb := strings.new_builder(4096)

	sb.write_string('{')
	sb.write_string('"formatVersion":${w.table_metadata.format_version},')
	sb.write_string('"tableUuid":"${w.table_metadata.table_uuid}",')
	sb.write_string('"location":"${w.table_metadata.location}",')
	sb.write_string('"lastUpdatedMs":${w.table_metadata.last_updated_ms},')
	sb.write_string('"currentSchemaId":${w.table_metadata.current_schema_id},')
	sb.write_string('"defaultSpecId":${w.table_metadata.default_spec_id},')
	sb.write_string('"currentSnapshotId":${w.table_metadata.current_snapshot_id},')

	// properties
	sb.write_string('"properties":${json.encode(w.table_metadata.properties)},')

	// schemas
	sb.write_string('"schemas":[')
	for i, schema in w.table_metadata.schemas {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"schemaId":${schema.schema_id},')
		sb.write_string('"fields":[')
		for j, field in schema.fields {
			if j > 0 {
				sb.write_string(',')
			}
			sb.write_string('{"id":${field.id},')
			sb.write_string('"name":"${field.name}",')
			sb.write_string('"type":"${field.typ}",')
			sb.write_string('"required":${field.required}}')
		}
		sb.write_string(']}')
	}
	sb.write_string('],')

	// partitionSpecs
	sb.write_string('"partitionSpecs":[')
	for i, spec in w.table_metadata.partition_specs {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"specId":${spec.spec_id},')
		sb.write_string('"fields":[')
		for j, field in spec.fields {
			if j > 0 {
				sb.write_string(',')
			}
			sb.write_string('{"sourceId":${field.source_id},')
			sb.write_string('"fieldId":${field.field_id},')
			sb.write_string('"name":"${field.name}",')
			sb.write_string('"transform":"${field.transform}"}')
		}
		sb.write_string(']}')
	}
	sb.write_string('],')

	// snapshots
	sb.write_string('"snapshots":[')
	for i, snapshot in w.table_metadata.snapshots {
		if i > 0 {
			sb.write_string(',')
		}
		sb.write_string('{"snapshotId":${snapshot.snapshot_id},')
		sb.write_string('"timestampMs":${snapshot.timestamp_ms},')
		sb.write_string('"manifestList":"${snapshot.manifest_list}",')
		sb.write_string('"schemaId":${snapshot.schema_id},')
		sb.write_string('"summary":${json.encode(snapshot.summary)}}')
	}
	sb.write_string(']')

	sb.write_string('}')

	return sb.str()
}

/// write_metadata_file은 테이블 메타데이터 파일을 S3에 씁니다.
pub fn (mut w IcebergWriter) write_metadata_file() !string {
	// 메타데이터 버전 증가
	version := w.table_metadata.snapshots.len
	metadata_path := 'metadata/${version:05d}-${w.table_metadata.table_uuid}.metadata.json'

	// 메타데이터 JSON 인코딩
	metadata_json := w.encode_metadata_json()

	// S3에 메타데이터 파일 저장
	full_path := '${w.table_metadata.location}/${metadata_path}'
	w.adapter.put_object(full_path, metadata_json.bytes())!

	return metadata_path
}

/// get_table_metadata는 현재 테이블 메타데이터를 반환합니다.
pub fn (w &IcebergWriter) get_table_metadata() IcebergMetadata {
	return w.table_metadata
}

/// get_current_snapshot_id은 현재 스냅샷 ID를 반환합니다.
pub fn (w &IcebergWriter) get_current_snapshot_id() i64 {
	return w.table_metadata.current_snapshot_id
}

/// time_travel은 특정 스냅샷으로 시간 여행합니다.
pub fn (mut w IcebergWriter) time_travel(snapshot_id i64) bool {
	for snapshot in w.table_metadata.snapshots {
		if snapshot.snapshot_id == snapshot_id {
			w.table_metadata.current_snapshot_id = snapshot_id
			return true
		}
	}
	return false
}

/// list_snapshots은 모든 스냅샷을 반환합니다.
pub fn (w &IcebergWriter) list_snapshots() []IcebergSnapshot {
	return w.table_metadata.snapshots.clone()
}
