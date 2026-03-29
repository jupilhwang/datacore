// Infra Layer - PostgreSQL storage adapter: consumer group operations
module postgres

import domain

/// save_group saves a consumer group.
pub fn (mut a PostgresStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	state_str := match group.state {
		.empty { 'empty' }
		.preparing_rebalance { 'preparing_rebalance' }
		.completing_rebalance { 'completing_rebalance' }
		.stable { 'stable' }
		.dead { 'dead' }
	}

	// Upsert group
	db.exec_param_many('
		INSERT INTO consumer_groups (group_id, protocol_type, state, generation_id, leader, protocol)
		VALUES (\$1, \$2, \$3, \$4, \$5, \$6)
		ON CONFLICT (group_id) DO UPDATE SET
			protocol_type = EXCLUDED.protocol_type,
			state = EXCLUDED.state,
			generation_id = EXCLUDED.generation_id,
			leader = EXCLUDED.leader,
			protocol = EXCLUDED.protocol,
			updated_at = NOW()
	',
		[
		group.group_id,
		group.protocol_type,
		state_str,
		group.generation_id.str(),
		group.leader,
		group.protocol,
	])!
}

/// load_group loads a consumer group.
pub fn (mut a PostgresStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec_param('
		SELECT group_id, protocol_type, state, generation_id, leader, protocol
		FROM consumer_groups WHERE group_id = \$1
	',
		group_id)!

	if rows.len == 0 {
		return error('group not found: ${group_id}')
	}

	row := rows[0]
	state_str := get_row_str(&row, 2, 'empty')
	state := match state_str {
		'preparing_rebalance' { domain.GroupState.preparing_rebalance }
		'completing_rebalance' { domain.GroupState.completing_rebalance }
		'stable' { domain.GroupState.stable }
		'dead' { domain.GroupState.dead }
		else { domain.GroupState.empty }
	}

	return domain.ConsumerGroup{
		group_id:      get_row_str(&row, 0, '')
		protocol_type: get_row_str(&row, 1, '')
		state:         state
		generation_id: get_row_int(&row, 3, 0)
		leader:        get_row_str(&row, 4, '')
		protocol:      get_row_str(&row, 5, '')
		members:       []domain.GroupMember{}
	}
}

/// delete_group deletes a consumer group.
pub fn (mut a PostgresStorageAdapter) delete_group(group_id string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!
	db.exec_param('DELETE FROM committed_offsets WHERE group_id = $1', group_id)!
	db.exec_param('DELETE FROM consumer_groups WHERE group_id = $1', group_id)!
	db.commit()!
}

/// list_groups returns a list of all consumer groups.
pub fn (mut a PostgresStorageAdapter) list_groups() ![]domain.GroupInfo {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec('SELECT group_id, protocol_type, state FROM consumer_groups')!

	mut result := []domain.GroupInfo{}
	for row in rows {
		state_str := get_row_str(&row, 2, 'Empty')
		state := match state_str {
			'preparing_rebalance' { 'PreparingRebalance' }
			'completing_rebalance' { 'CompletingRebalance' }
			'stable' { 'Stable' }
			'dead' { 'Dead' }
			else { 'Empty' }
		}

		result << domain.GroupInfo{
			group_id:      get_row_str(&row, 0, '')
			protocol_type: get_row_str(&row, 1, '')
			state:         state
		}
	}
	return result
}
