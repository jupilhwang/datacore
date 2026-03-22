// Infra Layer - PostgreSQL storage adapter: share partition state operations
module postgres

import db.pg
import domain
import json

// --- Share metrics helpers ---

fn (mut a PostgresStorageAdapter) inc_share_save() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.share_save_count++
}

fn (mut a PostgresStorageAdapter) inc_share_load() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.share_load_count++
}

fn (mut a PostgresStorageAdapter) inc_share_delete() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.share_delete_count++
}

// --- Share partition state methods ---

/// save_share_partition_state saves a SharePartition state using UPSERT.
/// Serializes record_states and acquired_records as JSONB.
pub fn (mut a PostgresStorageAdapter) save_share_partition_state(state domain.SharePartitionState) ! {
	a.inc_share_save()

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// Serialize maps to JSON strings for JSONB storage
	record_states_json := json.encode(state.record_states)
	acquired_records_json := json.encode(state.acquired_records)
	delivery_counts_json := json.encode(state.delivery_counts)

	db.exec_param_many('
		INSERT INTO share_partition_states (
			group_id, topic_name, partition_id,
			start_offset, end_offset,
			record_states, acquired_records, delivery_counts,
			total_acquired, total_acknowledged, total_released, total_rejected
		) VALUES (\$1, \$2, \$3, \$4, \$5, \$6, \$7, \$8, \$9, \$10, \$11, \$12)
		ON CONFLICT (group_id, topic_name, partition_id) DO UPDATE SET
			start_offset = EXCLUDED.start_offset,
			end_offset = EXCLUDED.end_offset,
			record_states = EXCLUDED.record_states,
			acquired_records = EXCLUDED.acquired_records,
			delivery_counts = EXCLUDED.delivery_counts,
			total_acquired = EXCLUDED.total_acquired,
			total_acknowledged = EXCLUDED.total_acknowledged,
			total_released = EXCLUDED.total_released,
			total_rejected = EXCLUDED.total_rejected,
			updated_at = NOW()
	',
		[
		state.group_id,
		state.topic_name,
		state.partition.str(),
		state.start_offset.str(),
		state.end_offset.str(),
		record_states_json,
		acquired_records_json,
		delivery_counts_json,
		state.total_acquired.str(),
		state.total_acknowledged.str(),
		state.total_released.str(),
		state.total_rejected.str(),
	])!
}

/// load_share_partition_state loads a SharePartition state by composite key.
/// Returns none if not found.
pub fn (mut a PostgresStorageAdapter) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	a.inc_share_load()

	mut db := a.pool.acquire() or { return none }
	defer { a.pool.release(db) }

	rows := db.exec_param_many('
		SELECT group_id, topic_name, partition_id,
			start_offset, end_offset,
			record_states, acquired_records, delivery_counts,
			total_acquired, total_acknowledged, total_released, total_rejected
		FROM share_partition_states
		WHERE group_id = \$1 AND topic_name = \$2 AND partition_id = \$3
	',
		[group_id, topic_name, partition.str()]) or { return none }

	if rows.len == 0 {
		return none
	}

	return a.decode_share_partition_state_row(&rows[0])
}

/// delete_share_partition_state deletes a SharePartition state by composite key.
pub fn (mut a PostgresStorageAdapter) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {
	a.inc_share_delete()

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.exec_param_many('
		DELETE FROM share_partition_states
		WHERE group_id = \$1 AND topic_name = \$2 AND partition_id = \$3
	',
		[group_id, topic_name, partition.str()])!
}

/// load_all_share_partition_states loads all SharePartition states for a group.
pub fn (mut a PostgresStorageAdapter) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	a.inc_share_load()

	mut db := a.pool.acquire() or { return [] }
	defer { a.pool.release(db) }

	rows := db.exec_param('
		SELECT group_id, topic_name, partition_id,
			start_offset, end_offset,
			record_states, acquired_records, delivery_counts,
			total_acquired, total_acknowledged, total_released, total_rejected
		FROM share_partition_states
		WHERE group_id = \$1
	',
		group_id) or { return [] }

	mut result := []domain.SharePartitionState{}
	for row in rows {
		if state := a.decode_share_partition_state_row(&row) {
			result << state
		}
	}
	return result
}

/// decode_share_partition_state_row decodes a PostgreSQL row into SharePartitionState.
/// Column order: group_id(0), topic_name(1), partition_id(2),
///   start_offset(3), end_offset(4), record_states(5), acquired_records(6),
///   delivery_counts(7), total_acquired(8), total_acknowledged(9),
///   total_released(10), total_rejected(11)
fn (a &PostgresStorageAdapter) decode_share_partition_state_row(row &pg.Row) ?domain.SharePartitionState {
	g_id := get_row_str(row, 0, '')
	if g_id == '' {
		return none
	}

	t_name := get_row_str(row, 1, '')
	p_id := get_row_int(row, 2, 0)
	s_offset := get_row_i64(row, 3, 0)
	e_offset := get_row_i64(row, 4, 0)

	// Decode JSONB fields
	rs_json := get_row_str(row, 5, '{}')
	ar_json := get_row_str(row, 6, '{}')
	dc_json := get_row_str(row, 7, '{}')

	record_states := json.decode(map[i64]u8, rs_json) or {
		map[i64]u8{}
	}
	acquired_records := json.decode(map[i64]domain.AcquiredRecordState, ar_json) or {
		map[i64]domain.AcquiredRecordState{}
	}
	delivery_counts := json.decode(map[i64]i32, dc_json) or {
		map[i64]i32{}
	}

	return domain.SharePartitionState{
		group_id:           g_id
		topic_name:         t_name
		partition:          i32(p_id)
		start_offset:       s_offset
		end_offset:         e_offset
		record_states:      record_states
		acquired_records:   acquired_records
		delivery_counts:    delivery_counts
		total_acquired:     get_row_i64(row, 8, 0)
		total_acknowledged: get_row_i64(row, 9, 0)
		total_released:     get_row_i64(row, 10, 0)
		total_rejected:     get_row_i64(row, 11, 0)
	}
}
