// Generates Parquet files in Iceberg table format and manages metadata.
module s3

import domain
import infra.encoding as parquet
import time
import json
import crypto.md5
import sync
import strings

/// IcebergWriter provides functionality for writing data to Iceberg tables.
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

/// new_iceberg_writer creates a new Iceberg Writer.
pub fn new_iceberg_writer(adapter &S3StorageAdapter, config IcebergConfig, table_location string) !&IcebergWriter {
	// Create default schema
	schema := create_default_schema()
	partition_spec := if config.partition_by.len > 0 {
		create_partition_spec_from_config(config.partition_by)
	} else {
		create_default_partition_spec()
	}

	// Initialize table metadata
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

/// append_records buffers records per partition.
pub fn (mut w IcebergWriter) append_records(topic string, partition int, records []domain.Record, start_offset i64) ! {
	if records.len == 0 {
		return
	}

	w.buffer_lock.lock()
	defer {
		w.buffer_lock.unlock()
	}

	for i, record in records {
		// Calculate partition values for the record
		partition_values := w.compute_partition_values(record)
		partition_key := w.partition_values_to_key(partition_values)

		// Convert headers to JSON string
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

		// Append to per-partition buffer
		if partition_key !in w.partition_buffers {
			w.partition_buffers[partition_key] = []
		}
		w.partition_buffers[partition_key] << prec
	}
}

/// compute_partition_values computes partition values from a record.
fn (w &IcebergWriter) compute_partition_values(record domain.Record) map[string]string {
	mut values := map[string]string{}

	for field in w.partition_spec.fields {
		match field.transform {
			'identity' {
				// Use the original value as-is
				match field.source_id {
					2 { values[field.name] = record.timestamp.format_ss()[0..10] }
					else { values[field.name] = 'unknown' }
				}
			}
			'day' {
				// Convert to date (YYYY-MM-DD)
				values[field.name] = record.timestamp.format_ss()[0..10]
			}
			'hour' {
				// Convert to hour (YYYY-MM-DD-HH)
				values[field.name] = record.timestamp.format_ss()[0..13].replace(' ',
					'-')
			}
			'month' {
				// Convert to month (YYYY-MM)
				values[field.name] = record.timestamp.format_ss()[0..7]
			}
			'year' {
				// Convert to year (YYYY)
				values[field.name] = record.timestamp.format_ss()[0..4]
			}
			else {
				values[field.name] = 'unknown'
			}
		}
	}

	return values
}

/// partition_values_to_key converts partition values to a key string.
fn (w &IcebergWriter) partition_values_to_key(values map[string]string) string {
	mut parts := []string{}
	for field in w.partition_spec.fields {
		if value := values[field.name] {
			parts << '${field.name}=${value}'
		}
	}
	return parts.join('/')
}

/// should_flush checks whether a flush is needed.
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

/// flush_all_partitions flushes all partition buffers.
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

		// Encode as Parquet file
		mut encoder := parquet.new_parquet_encoder(w.config.compression, w.config.max_file_size_mb)!
		data, metadata := parquet.encode_batch(records, encoder.compression)!

		// Generate file path
		file_path := w.generate_data_file_path(topic, partition, partition_key)

		// Upload to S3
		full_path := '${w.table_metadata.location}/${file_path}'
		w.adapter.put_object(full_path, data)!

		// Parse partition values
		partition_values := w.parse_partition_key(partition_key)

		// Create Iceberg DataFile metadata
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

		// Add column statistics
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

		// Clear buffer
		w.partition_buffers[partition_key] = []
	}

	return data_files
}

/// parse_partition_key parses a partition key into a partition values map.
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

/// generate_data_file_path generates a data file path.
fn (mut w IcebergWriter) generate_data_file_path(topic string, partition int, partition_key string) string {
	w.file_counter++

	// Data file path: topics/{topic}/partitions/{partition}/data/{partition_key}/{counter}-{uuid}.parquet
	partition_values := w.parse_partition_key(partition_key)
	mut partition_path := ''
	for field in w.partition_spec.fields {
		if value := partition_values[field.name] {
			partition_path += '${field.name}=${value}/'
		}
	}

	// Generate unique ID
	now := time.now()
	unique_id := md5.sum('${now.unix_milli()}-${w.file_counter}'.bytes()).hex()

	return 'topics/${topic}/partitions/${partition}/data/${partition_path}${w.file_counter:05d}-${unique_id[0..8]}.parquet'
}

/// create_snapshot creates a new snapshot.
pub fn (mut w IcebergWriter) create_snapshot(data_files []IcebergDataFile, topic string) !IcebergSnapshot {
	snapshot_id := generate_snapshot_id()
	now := time.now()

	// Create manifest file
	manifest_path := w.generate_manifest_path(snapshot_id)
	manifest_content := w.encode_manifest(data_files)!
	w.adapter.put_object('${w.table_metadata.location}/${manifest_path}', manifest_content)!

	// Snapshot summary information
	mut added_files := 0
	mut added_records := i64(0)
	for file in data_files {
		added_files++
		added_records += file.record_count
	}

	// Calculate total file size
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

	// Update table metadata
	w.table_metadata.snapshots << snapshot
	w.table_metadata.current_snapshot_id = snapshot_id
	w.table_metadata.last_updated_ms = now.unix_milli()

	return snapshot
}

/// generate_manifest_path generates a manifest file path.
fn (w &IcebergWriter) generate_manifest_path(snapshot_id i64) string {
	now := time.now()
	return 'metadata/snap-${snapshot_id}-${now.format_ss().replace(' ', '-').replace(':',
		'-')}.avro'
}

/// encode_manifest encodes manifest file content in Avro Object Container File format.
/// Implements the Iceberg Manifest File Avro schema per the Iceberg spec.
fn (w &IcebergWriter) encode_manifest(data_files []IcebergDataFile) ![]u8 {
	// Avro Object Container File format:
	// - 4-byte magic: 'O', 'b', 'j', 0x01
	// - file-level metadata (map<bytes>), including schema and codec
	// - 16-byte sync marker
	// - blocks: [count varint][size varint][data bytes][sync marker]

	// Iceberg manifest file Avro schema (manifest_entry records)
	manifest_schema := '{"type":"record","name":"manifest_entry","fields":[' +
		'{"name":"status","type":"int"},' +
		'{"name":"snapshot_id","type":["null","long"],"default":null},' +
		'{"name":"sequence_number","type":["null","long"],"default":null},' +
		'{"name":"file_sequence_number","type":["null","long"],"default":null},' +
		'{"name":"data_file","type":{"type":"record","name":"r2","fields":[' +
		'{"name":"content","type":"int"},' +
		'{"name":"file_path","type":"string"},' +
		'{"name":"file_format","type":"string"},' +
		'{"name":"partition","type":{"type":"record","name":"r102","fields":[]}},' +
		'{"name":"record_count","type":"long"},' +
		'{"name":"file_size_in_bytes","type":"long"},' +
		'{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"r104","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"r105","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"r106","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"r108","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}],"default":null},' +
		'{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"r109","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}],"default":null}' +
		']}}}]}'

	// Generate 16-byte sync marker from snapshot/schema metadata
	sync_source := '${w.table_metadata.table_uuid}-${w.table_metadata.current_snapshot_id}'
	sync_marker := avro_generate_sync_marker(sync_source)

	// Build Avro file-level metadata
	mut meta_map := map[string]string{}
	meta_map['avro.schema'] = manifest_schema
	meta_map['avro.codec'] = 'null'
	meta_map['iceberg.schema'] = manifest_schema
	meta_map['format-version'] = w.table_metadata.format_version.str()
	meta_map['partition-spec-id'] = w.table_metadata.default_spec_id.str()
	meta_map['schema-id'] = w.table_metadata.current_schema_id.str()
	meta_map['content'] = 'data'

	mut buf := []u8{}

	// Magic bytes: Obj\x01
	buf << u8(`O`)
	buf << u8(`b`)
	buf << u8(`j`)
	buf << u8(0x01)

	// File-level metadata as Avro map<bytes>
	avro_write_meta_map(mut buf, meta_map)

	// Sync marker (16 bytes)
	buf << sync_marker

	// Encode each data file as a manifest_entry record
	mut records_buf := []u8{}
	for file in data_files {
		avro_write_manifest_entry(mut records_buf, file, w.table_metadata.current_snapshot_id)
	}

	if records_buf.len > 0 {
		// Block: [count][byte_count][data][sync_marker]
		avro_write_varint(mut buf, i64(data_files.len))
		avro_write_varint(mut buf, i64(records_buf.len))
		buf << records_buf
		buf << sync_marker
	}

	// End-of-file block: count=0
	buf << u8(0)

	return buf
}

/// avro_generate_sync_marker generates a 16-byte sync marker from a string.
fn avro_generate_sync_marker(source string) []u8 {
	mut result := []u8{len: 16}
	src_bytes := source.bytes()
	for i in 0 .. 16 {
		result[i] = if i < src_bytes.len { src_bytes[i] } else { u8(i * 13 + 7) }
	}
	// XOR mixing for better distribution
	for i in 1 .. 16 {
		result[i] ^= result[i - 1]
	}
	return result
}

/// avro_write_varint writes a zigzag-encoded varint to the buffer.
fn avro_write_varint(mut buf []u8, n i64) {
	// Zigzag encoding: (n << 1) ^ (n >> 63)
	mut v := u64((n << 1) ^ (n >> 63))
	for {
		if v <= 0x7F {
			buf << u8(v)
			break
		}
		buf << u8((v & 0x7F) | 0x80)
		v >>= 7
	}
}

/// avro_write_string writes an Avro string (length-prefixed bytes).
fn avro_write_string(mut buf []u8, s string) {
	avro_write_varint(mut buf, i64(s.len))
	buf << s.bytes()
}

/// avro_write_bytes writes Avro bytes (length-prefixed).
fn avro_write_bytes(mut buf []u8, data []u8) {
	avro_write_varint(mut buf, i64(data.len))
	buf << data
}

/// avro_write_meta_map writes the Avro file-level metadata map<bytes>.
fn avro_write_meta_map(mut buf []u8, meta map[string]string) {
	if meta.len == 0 {
		buf << u8(0)
		return
	}

	// Map block: [count varint] followed by [key string][value bytes] pairs, then 0
	avro_write_varint(mut buf, i64(meta.len))
	for key, value in meta {
		avro_write_string(mut buf, key)
		avro_write_bytes(mut buf, value.bytes())
	}
	// End of map
	buf << u8(0)
}

/// avro_write_nullable_long writes an Avro union [null, long] with non-null value.
fn avro_write_nullable_long(mut buf []u8, value i64) {
	// Union index 1 = long
	avro_write_varint(mut buf, 1)
	avro_write_varint(mut buf, value)
}

/// avro_write_manifest_entry writes a single manifest_entry Avro record.
fn avro_write_manifest_entry(mut buf []u8, file IcebergDataFile, snapshot_id i64) {
	// status: 1 = ADDED (int)
	avro_write_varint(mut buf, 1)

	// snapshot_id: union [null, long] - use snapshot_id
	avro_write_nullable_long(mut buf, snapshot_id)

	// sequence_number: union [null, long]
	avro_write_nullable_long(mut buf, 0)

	// file_sequence_number: union [null, long]
	avro_write_nullable_long(mut buf, 0)

	// data_file record:
	// content: 0 = DATA
	avro_write_varint(mut buf, 0)

	// file_path: string
	avro_write_string(mut buf, file.file_path)

	// file_format: string
	avro_write_string(mut buf, file.file_format)

	// partition: empty record (no partition fields in schema for simplicity)
	// (already encoded as an empty struct - nothing to write)

	// record_count: long
	avro_write_varint(mut buf, file.record_count)

	// file_size_in_bytes: long
	avro_write_varint(mut buf, file.file_size_in_bytes)

	// column_sizes: union [null, array] - write null (union index 0)
	buf << u8(0)

	// value_counts: union [null, array] - write null
	buf << u8(0)

	// null_value_counts: union [null, array] - write null
	buf << u8(0)

	// lower_bounds: union [null, array] - write null
	buf << u8(0)

	// upper_bounds: union [null, array] - write null
	buf << u8(0)
}

/// encode_metadata_json encodes table metadata as JSON.
fn (w &IcebergWriter) encode_metadata_json() string {
	// Iceberg metadata JSON format - build string directly
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

/// write_metadata_file writes the table metadata file to S3.
pub fn (mut w IcebergWriter) write_metadata_file() !string {
	// Increment metadata version
	version := w.table_metadata.snapshots.len
	metadata_path := 'metadata/${version:05d}-${w.table_metadata.table_uuid}.metadata.json'

	// Encode metadata JSON
	metadata_json := w.encode_metadata_json()

	// Save metadata file to S3
	full_path := '${w.table_metadata.location}/${metadata_path}'
	w.adapter.put_object(full_path, metadata_json.bytes())!

	return metadata_path
}

/// get_table_metadata returns the current table metadata.
pub fn (w &IcebergWriter) get_table_metadata() IcebergMetadata {
	return w.table_metadata
}

/// get_current_snapshot_id returns the current snapshot ID.
pub fn (w &IcebergWriter) get_current_snapshot_id() i64 {
	return w.table_metadata.current_snapshot_id
}

/// time_travel time-travels to a specific snapshot.
pub fn (mut w IcebergWriter) time_travel(snapshot_id i64) bool {
	for snapshot in w.table_metadata.snapshots {
		if snapshot.snapshot_id == snapshot_id {
			w.table_metadata.current_snapshot_id = snapshot_id
			return true
		}
	}
	return false
}

/// list_snapshots returns all snapshots.
pub fn (w &IcebergWriter) list_snapshots() []IcebergSnapshot {
	return w.table_metadata.snapshots.clone()
}
