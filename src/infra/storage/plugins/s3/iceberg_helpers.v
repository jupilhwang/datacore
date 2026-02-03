// 인프라 레이어 - Iceberg 통합 헬퍼 메서드
// S3StorageAdapter에 Iceberg 테이블 형식 지원을 추가합니다.
module s3

import os
import domain

/// is_iceberg_enabled는 Iceberg 테이블 형식이 활성화되어 있는지 확인합니다.
/// 참고: 실제 구현에서는 환경변수나 설정 파일에서 Iceberg 활성화 여부 확인
fn (a &S3StorageAdapter) is_iceberg_enabled() bool {
	// 환경변수로부터 Iceberg 활성화 여부 확인
	iceberg_env := os.getenv('DATACORE_ICEBERG_ENABLED')
	return iceberg_env == 'true' || iceberg_env == '1'
}

/// get_or_create_iceberg_writer는 Iceberg Writer를 가져오거나 새로 생성합니다.
fn (mut a S3StorageAdapter) get_or_create_iceberg_writer(topic string, partition int) !&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg_lock.rlock()
	if writer := a.iceberg_writers[partition_key] {
		a.iceberg_lock.runlock()
		return writer
	}
	a.iceberg_lock.runlock()

	// Iceberg 설정 생성 (기본값)
	config := IcebergConfig{
		enabled:           true
		format:            'parquet'
		compression:       'zstd'
		write_mode:        'append'
		partition_by:      ['timestamp', 'topic']
		max_rows_per_file: 1000000
		max_file_size_mb:  128
		schema_evolution:  true
	}

	// 테이블 위치 생성
	table_location := '${a.config.prefix}iceberg/${topic}/partition_${partition}'

	// 새로운 Iceberg Writer 생성
	mut writer := new_iceberg_writer(&a, config, table_location)!

	a.iceberg_lock.@lock()
	a.iceberg_writers[partition_key] = writer
	a.iceberg_lock.unlock()

	return writer
}

/// append_to_iceberg는 레코드를 Iceberg 테이블에 추가합니다.
fn (mut a S3StorageAdapter) append_to_iceberg(topic string, partition int, records []domain.Record, start_offset i64) ! {
	if records.len == 0 {
		return
	}

	// Iceberg Writer 가져오기 또는 생성
	mut writer := a.get_or_create_iceberg_writer(topic, partition)!

	// 레코드 추가
	writer.append_records(topic, partition, records, start_offset)!

	// 플러시 확인 및 실행
	if writer.should_flush() {
		data_files := writer.flush_all_partitions(topic, partition)!

		if data_files.len > 0 {
			// 새 스냅샷 생성
			snapshot := writer.create_snapshot(data_files, topic)!

			// 메타데이터 파일 업데이트
			writer.write_metadata_file()!

			log_message(.info, 'IcebergFlush', 'Created new Iceberg snapshot', {
				'topic':       topic
				'partition':   partition.str()
				'snapshot_id': snapshot.snapshot_id.str()
				'files':       data_files.len.str()
			})
		}
	}
}

/// get_iceberg_writer는 특정 파티션의 Iceberg Writer를 반환합니다.
pub fn (mut a S3StorageAdapter) get_iceberg_writer(topic string, partition int) ?&IcebergWriter {
	partition_key := '${topic}:${partition}'

	a.iceberg_lock.rlock()
	defer {
		a.iceberg_lock.runlock()
	}

	return a.iceberg_writers[partition_key] or { none }
}

/// list_iceberg_snapshots은 특정 파티션의 Iceberg 스냅샷 목록을 반환합니다.
pub fn (mut a S3StorageAdapter) list_iceberg_snapshots(topic string, partition int) ![]IcebergSnapshot {
	if writer := a.get_iceberg_writer(topic, partition) {
		return writer.list_snapshots()
	}
	return []
}

/// time_travel_iceberg은 특정 파티션을 특정 스냅샷으로 시간 여행합니다.
pub fn (mut a S3StorageAdapter) time_travel_iceberg(topic string, partition int, snapshot_id i64) bool {
	if mut writer := a.get_iceberg_writer(topic, partition) {
		return writer.time_travel(snapshot_id)
	}
	return false
}

/// flush_all_iceberg_writers은 모든 Iceberg Writer를 플러시합니다.
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

	log_message(.info, 'IcebergFlushAll', 'Flushed all Iceberg writers', {
		'total_files':     total_files.str()
		'total_snapshots': total_snapshots.str()
	})
}
