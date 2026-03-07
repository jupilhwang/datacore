// Generates Parquet files in Iceberg table format and manages metadata.
module s3

import domain
import infra.encoding as parquet
import time
import json
import crypto.md5
import sync

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
	// catalog integration (optional)
	table_identifier IcebergTableIdentifier
}

/// create_writer_metadata builds the initial IcebergMetadata, schema, and partition spec
/// for a new writer. Extracted to avoid duplication between new_iceberg_writer variants.
fn create_writer_metadata(config IcebergConfig, table_location string) (IcebergMetadata, IcebergSchema, IcebergPartitionSpec) {
	schema := create_default_schema()
	partition_spec := if config.partition_by.len > 0 {
		create_partition_spec_from_config(config.partition_by)
	} else {
		create_default_partition_spec()
	}
	format_version := if config.format_version > 0 { config.format_version } else { 2 }
	metadata := IcebergMetadata{
		format_version:      format_version
		table_uuid:          generate_table_uuid(table_location)
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
	return metadata, schema, partition_spec
}

/// new_iceberg_writer creates a new Iceberg Writer.
pub fn new_iceberg_writer(adapter &S3StorageAdapter, config IcebergConfig, table_location string) !&IcebergWriter {
	metadata, schema, partition_spec := create_writer_metadata(config, table_location)
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

/// new_iceberg_writer_with_catalog creates a new Iceberg Writer and registers the table in a catalog.
/// If the table already exists in the catalog, it skips registration without error.
pub fn new_iceberg_writer_with_catalog(adapter &S3StorageAdapter, config IcebergConfig, table_location string, mut catalog IcebergCatalog, identifier IcebergTableIdentifier) !&IcebergWriter {
	metadata, schema, partition_spec := create_writer_metadata(config, table_location)
	mut writer := &IcebergWriter{
		adapter:           adapter
		config:            config
		table_metadata:    metadata
		partition_buffers: map[string][]parquet.ParquetRecord{}
		current_schema:    schema
		partition_spec:    partition_spec
		file_counter:      0
		table_identifier:  identifier
	}
	// Try to register the table in the catalog; skip if already exists.
	catalog.create_table(identifier, schema, partition_spec, table_location) or {
		// Table already exists - silently skip.
	}
	return writer
}

/// normalize_s3_location strips the s3://bucket/ prefix from a location string,
/// returning a relative S3 key suitable for use with the adapter.
pub fn normalize_s3_location(location string) string {
	if !location.starts_with('s3://') {
		return location
	}
	// Remove 's3://'
	after_scheme := location[5..]
	// Find the first '/' after the bucket name
	slash_idx := after_scheme.index('/') or { return '' }
	relative := after_scheme[slash_idx + 1..]
	return relative.trim_right('/')
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
			// Pre-allocate with expected capacity to avoid repeated reallocations
			w.partition_buffers[partition_key] = []parquet.ParquetRecord{cap: w.config.max_rows_per_file}
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

		// Encode as Parquet file - derive compression type directly without allocating encoder
		compression := parquet.parquet_compression_from_string(w.config.compression) or {
			parquet.ParquetCompression.uncompressed
		}
		data, metadata := parquet.encode_batch(records, compression)!

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
			column_sizes:       map[int]i64{}
			value_counts:       map[int]i64{}
			null_value_counts:  map[int]i64{}
			lower_bounds:       map[int][]u8{}
			upper_bounds:       map[int][]u8{}
			partition:          partition_values
		}

		w.collect_column_stats(mut data_file, metadata)

		data_files << data_file

		// Clear buffer while preserving allocated capacity for reuse
		w.partition_buffers[partition_key] = []parquet.ParquetRecord{cap: w.config.max_rows_per_file}
	}

	return data_files
}

/// collect_column_stats populates column statistics in data_file from Parquet metadata.
fn (w &IcebergWriter) collect_column_stats(mut data_file IcebergDataFile, metadata parquet.ParquetMetadata) {
	// 컬럼 이름 -> field_id 매핑 생성
	mut name_to_field_id := map[string]int{}
	for field in w.current_schema.fields {
		name_to_field_id[field.name] = field.id
	}

	// Parquet 메타데이터에서 컬럼 통계 추출 후 DataFile에 기록
	for row_group in metadata.row_groups {
		for chunk in row_group.columns {
			field_id := name_to_field_id[chunk.column_name] or { continue }
			data_file.column_sizes[field_id] = chunk.data_size
			data_file.value_counts[field_id] = chunk.value_count
			data_file.null_value_counts[field_id] = chunk.null_count
			if chunk.min_bytes.len > 0 {
				data_file.lower_bounds[field_id] = chunk.min_bytes.clone()
			}
			if chunk.max_bytes.len > 0 {
				data_file.upper_bounds[field_id] = chunk.max_bytes.clone()
			}
		}
	}
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
