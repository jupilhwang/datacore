// Adds Iceberg table format support to S3StorageAdapter.
module s3

import os
import domain
import infra.observability

/// is_iceberg_enabled_with_config checks whether Iceberg is enabled based on config and env var.
/// 우선순위: env var가 설정된 경우 env var 값 사용, 미설정 시 config.enabled 사용.
pub fn is_iceberg_enabled_with_config(config IcebergConfig) bool {
	iceberg_env := os.getenv('DATACORE_ICEBERG_ENABLED')
	// env var가 명시적으로 설정된 경우 env var 값이 우선
	if iceberg_env == 'true' || iceberg_env == '1' {
		return true
	}
	if iceberg_env == 'false' || iceberg_env == '0' {
		return false
	}
	// env var 미설정 시 config.enabled 사용
	return config.enabled
}

/// is_iceberg_enabled checks whether the Iceberg table format is enabled.
/// 어댑터에 저장된 iceberg_config를 사용하여 활성화 여부를 판단.
fn (a &S3StorageAdapter) is_iceberg_enabled() bool {
	return is_iceberg_enabled_with_config(a.iceberg_config)
}

/// get_or_create_iceberg_writer gets an existing Iceberg Writer or creates a new one.
fn (mut a S3StorageAdapter) get_or_create_iceberg_writer(topic string, partition int) !&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg_lock.rlock()
	if writer := a.iceberg_writers[partition_key] {
		a.iceberg_lock.runlock()
		return writer
	}
	a.iceberg_lock.runlock()

	// 어댑터에 저장된 iceberg_config 사용 (런타임 config와 연결)
	// format_version 미설정 시 안정 스펙 기본값 2 적용
	mut config := a.iceberg_config
	if config.format_version == 0 {
		config.format_version = 2
	}

	// Build table location
	table_location := '${a.config.prefix}iceberg/${topic}/partition_${partition}'

	// Create new Iceberg Writer
	mut writer := new_iceberg_writer(&a, config, table_location)!

	a.iceberg_lock.@lock()
	a.iceberg_writers[partition_key] = writer
	a.iceberg_lock.unlock()

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
pub fn (mut a S3StorageAdapter) get_iceberg_writer(topic string, partition int) ?&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg_lock.rlock()
	defer {
		a.iceberg_lock.runlock()
	}

	return a.iceberg_writers[partition_key] or { none }
}

/// list_iceberg_snapshots returns the list of Iceberg snapshots for a specific partition.
pub fn (mut a S3StorageAdapter) list_iceberg_snapshots(topic string, partition int) ![]IcebergSnapshot {
	if writer := a.get_iceberg_writer(topic, partition) {
		return writer.list_snapshots()
	}
	return []
}

/// time_travel_iceberg time-travels a specific partition to a specific snapshot.
pub fn (mut a S3StorageAdapter) time_travel_iceberg(topic string, partition int, snapshot_id i64) bool {
	if mut writer := a.get_iceberg_writer(topic, partition) {
		return writer.time_travel(snapshot_id)
	}
	return false
}

/// flush_all_iceberg_writers flushes all Iceberg Writers.
pub fn (mut a S3StorageAdapter) flush_all_iceberg_writers() ! {
	mut total_files := 0
	mut total_snapshots := 0

	a.iceberg_lock.@lock()
	topics_partitions := a.iceberg_writers.keys()
	a.iceberg_lock.unlock()

	for tp_key in topics_partitions {
		parts := tp_key.split(':')
		if parts.len == 2 {
			topic := parts[0]
			partition := parts[1].int()

			if mut writer := a.get_iceberg_writer(topic, partition) {
				data_files := writer.flush_all_partitions(topic, partition) or { continue }
				if data_files.len > 0 {
					writer.create_snapshot(data_files, topic) or { continue }
					writer.write_metadata_file() or { continue }
					total_files += data_files.len
					total_snapshots++
				}
			}
		}
	}

	observability.log_with_context('s3', .info, 'IcebergFlushAll', 'Flushed all Iceberg writers',
		{
		'total_files':     total_files.str()
		'total_snapshots': total_snapshots.str()
	})
}
