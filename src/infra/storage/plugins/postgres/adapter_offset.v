// Infra Layer - PostgreSQL storage adapter: offset operations
module postgres

import domain

// --- Offset batch helpers ---

/// build_offset_commit_params generates a parameter array for batch offset commit UPSERT.
fn (a &PostgresStorageAdapter) build_offset_commit_params(group_id string, offsets []domain.PartitionOffset) []string {
	mut all_params := []string{}
	for offset in offsets {
		all_params << group_id
		all_params << offset.topic
		all_params << offset.partition.str()
		all_params << offset.offset.str()
		all_params << offset.metadata
	}
	return all_params
}

/// build_offset_upsert_query generates a batch UPSERT query for offset commits.
fn (a &PostgresStorageAdapter) build_offset_upsert_query(offset_count int) string {
	mut values_parts := []string{}
	for i in 0 .. offset_count {
		param_idx := i * 5 + 1
		values_parts << '(\$${param_idx}, \$${param_idx + 1}, \$${param_idx + 2}, \$${param_idx + 3}, \$${
			param_idx + 4})'
	}
	values_clause := values_parts.join(', ')
	return 'INSERT INTO committed_offsets (group_id, topic_name, partition_id, committed_offset, metadata) VALUES ${values_clause} ON CONFLICT (group_id, topic_name, partition_id) DO UPDATE SET committed_offset = EXCLUDED.committed_offset, metadata = EXCLUDED.metadata, committed_at = NOW()'
}

// --- Offset methods ---

/// commit_offsets commits offsets.
pub fn (mut a PostgresStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Perform batch UPSERT
	if offsets.len > 0 {
		all_params := a.build_offset_commit_params(group_id, offsets)
		query := a.build_offset_upsert_query(offsets.len)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!
}

/// fetch_offsets retrieves committed offsets.
pub fn (mut a PostgresStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	mut results := []domain.OffsetFetchResult{}

	if partitions.len == 0 {
		return results
	}

	// Retrieve offsets for all partitions in a single query (using OR conditions)
	mut topic_partition_pairs := []string{}
	mut all_params := []string{}
	all_params << group_id

	for i, part in partitions {
		param_idx := i * 2 + 2
		topic_partition_pairs << '(topic_name = \$${param_idx} AND partition_id = \$${param_idx + 1})'
		all_params << part.topic
		all_params << part.partition.str()
	}

	where_clause := topic_partition_pairs.join(' OR ')
	query := 'SELECT topic_name, partition_id, committed_offset, metadata FROM committed_offsets WHERE group_id = \$1 AND (${where_clause})'
	rows := db.exec_param_many(query, all_params)!

	// Convert results to map
	mut offset_map := map[string]domain.OffsetFetchResult{}
	for row in rows {
		topic := get_row_str(&row, 0, '')
		partition := get_row_int(&row, 1, 0)
		key := '${topic}:${partition}'
		offset_map[key] = domain.OffsetFetchResult{
			topic:      topic
			partition:  partition
			offset:     get_row_i64(&row, 2, -1)
			metadata:   get_row_str(&row, 3, '')
			error_code: 0
		}
	}

	// Generate results for all requested partitions
	for part in partitions {
		key := '${part.topic}:${part.partition}'
		if result := offset_map[key] {
			results << result
		} else {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
	}

	return results
}
