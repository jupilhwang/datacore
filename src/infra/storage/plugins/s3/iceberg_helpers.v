// Adds Iceberg table format support to S3StorageAdapter.
module s3

import os
import domain
import infra.observability
import service.port

/// is_iceberg_enabled_with_config checks whether Iceberg is enabled based on config and env var.
fn is_iceberg_enabled_with_config(config IcebergConfig) bool {
	iceberg_env := os.getenv('DATACORE_ICEBERG_ENABLED')
	if iceberg_env == 'true' || iceberg_env == '1' {
		return true
	}
	if iceberg_env == 'false' || iceberg_env == '0' {
		return false
	}
	return config.enabled
}

/// is_iceberg_enabled checks whether the Iceberg table format is enabled.
fn (a &S3StorageAdapter) is_iceberg_enabled() bool {
	return is_iceberg_enabled_with_config(a.iceberg.config)
}

/// get_or_create_iceberg_writer gets an existing Iceberg Writer or creates a new one.
fn (mut a S3StorageAdapter) get_or_create_iceberg_writer(topic string, partition int) !&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg.mu.rlock()
	if writer := a.iceberg.writers[partition_key] {
		a.iceberg.mu.runlock()
		return writer
	}
	a.iceberg.mu.runlock()

	mut config := a.iceberg.config
	if config.format_version == 0 {
		config.format_version = 2
	}

	// Build table location
	table_location := '${a.config.prefix}iceberg/${topic}/partition_${partition}'

	// Create new Iceberg Writer
	mut writer := new_iceberg_writer(&a, config, table_location)!

	a.iceberg.mu.@lock()
	a.iceberg.writers[partition_key] = writer
	a.iceberg.mu.unlock()

	return writer
}

/// append_to_iceberg appends records to an Iceberg table.
fn (mut a S3StorageAdapter) append_to_iceberg(topic string, partition int, records []domain.Record, start_offset i64) ! {
	if records.len == 0 {
		return
	}

	// Get or create Iceberg Writer
	mut writer := a.get_or_create_iceberg_writer(topic, partition)!

	// Append records
	writer.append_records(topic, partition, records, start_offset)!

	// Check and perform flush if needed
	if writer.should_flush() {
		data_files := writer.flush_all_partitions(topic, partition)!

		if data_files.len > 0 {
			// Create new snapshot
			snapshot := writer.create_snapshot(data_files, topic)!

			// Update metadata file
			writer.write_metadata_file()!

			observability.log_with_context('s3', .info, 'IcebergFlush', 'Created new Iceberg snapshot',
				{
				'topic':       topic
				'partition':   partition.str()
				'snapshot_id': snapshot.snapshot_id.str()
				'files':       data_files.len.str()
			})
		}
	}
}

/// get_iceberg_writer returns the Iceberg Writer for a specific partition.
fn (mut a S3StorageAdapter) get_iceberg_writer(topic string, partition int) ?&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg.mu.rlock()
	defer {
		a.iceberg.mu.runlock()
	}

	return a.iceberg.writers[partition_key] or { none }
}

/// list_iceberg_snapshots returns the list of Iceberg snapshots for a specific partition.
fn (mut a S3StorageAdapter) list_iceberg_snapshots(topic string, partition int) ![]port.IcebergSnapshot {
	if writer := a.get_iceberg_writer(topic, partition) {
		return writer.list_snapshots()
	}
	return []
}

/// time_travel_iceberg time-travels a specific partition to a specific snapshot.
fn (mut a S3StorageAdapter) time_travel_iceberg(topic string, partition int, snapshot_id i64) bool {
	if mut writer := a.get_iceberg_writer(topic, partition) {
		return writer.time_travel(snapshot_id)
	}
	return false
}
