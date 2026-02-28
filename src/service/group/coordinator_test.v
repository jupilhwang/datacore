// Unit tests for GroupCoordinator.
// Tests join_group, sync_group, heartbeat, and leave_group operations.
module group

import domain
import service.port

// CoordMockStorage is an in-memory StoragePort implementation for GroupCoordinator tests.
// It persists consumer groups in a map to enable realistic round-trip testing.
struct CoordMockStorage {
mut:
	groups map[string]domain.ConsumerGroup
}

fn new_coord_mock_storage() CoordMockStorage {
	return CoordMockStorage{
		groups: map[string]domain.ConsumerGroup{}
	}
}

fn (mut s CoordMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{
		name:            name
		topic_id:        []u8{len: 16}
		partition_count: partitions
	}
}

fn (s CoordMockStorage) delete_topic(name string) ! {}

fn (s CoordMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (s CoordMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s CoordMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s CoordMockStorage) add_partitions(name string, new_count int) ! {}

fn (mut s CoordMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{
		base_offset:     0
		log_append_time: 0
	}
}

fn (s CoordMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{
		records:        []domain.Record{}
		high_watermark: 0
	}
}

fn (s CoordMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (mut s CoordMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		high_watermark:  0
		earliest_offset: 0
	}
}

fn (mut s CoordMockStorage) save_group(group domain.ConsumerGroup) ! {
	s.groups[group.group_id] = group
}

fn (s CoordMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return s.groups[group_id] or { return error('group not found: ${group_id}') }
}

fn (mut s CoordMockStorage) delete_group(group_id string) ! {
	s.groups.delete(group_id)
}

fn (s CoordMockStorage) list_groups() ![]domain.GroupInfo {
	mut result := []domain.GroupInfo{}
	for _, g in s.groups {
		result << domain.GroupInfo{
			group_id:      g.group_id
			protocol_type: g.protocol_type
			state:         g.state.str()
		}
	}
	return result
}

fn (s CoordMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (s CoordMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s CoordMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &CoordMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s CoordMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s CoordMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s CoordMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s CoordMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn (s &CoordMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// ============================================================
// join_group tests
// ============================================================

fn test_join_group_new_member() {
	// A new member should be added to both members slice and members_map.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:             'test-group'
		member_id:            'member-1'
		protocol_type:        'consumer'
		session_timeout_ms:   30000
		rebalance_timeout_ms: 60000
		protocols:            [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}

	resp := coordinator.join_group(req)

	assert resp.error_code == 0, 'Expected no error, got ${resp.error_code}'
	assert resp.member_id == 'member-1'
	assert resp.generation_id == 1

	// Verify group was persisted with correct members and map
	group := storage.load_group('test-group') or {
		assert false, 'Group should exist after join'
		return
	}
	assert group.members.len == 1
	assert group.members[0].member_id == 'member-1'
	assert group.members_map['member-1'] == 0
}

fn test_join_group_second_member() {
	// A second member should be appended and both members_map entries must be correct.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req1 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req2 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}

	coordinator.join_group(req1)
	coordinator.join_group(req2)

	group := storage.load_group('test-group') or {
		assert false, 'Group should exist'
		return
	}

	assert group.members.len == 2
	// members_map indices must point to the correct member in the slice
	idx1 := group.members_map['member-1'] or {
		assert false, 'member-1 should be in map'
		return
	}
	idx2 := group.members_map['member-2'] or {
		assert false, 'member-2 should be in map'
		return
	}
	assert group.members[idx1].member_id == 'member-1'
	assert group.members[idx2].member_id == 'member-2'
}

fn test_join_group_existing_member_rejoins() {
	// When an existing member rejoins, it should be replaced in-place.
	// The members_map index must remain valid after the update.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req_first := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req_second := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req_rejoin := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: [u8(1), 2, 3]
		}]
	}

	coordinator.join_group(req_first)
	coordinator.join_group(req_second)
	coordinator.join_group(req_rejoin)

	group := storage.load_group('test-group') or {
		assert false, 'Group should exist'
		return
	}

	// Member count must remain at 2 (no duplicate added)
	assert group.members.len == 2

	// members_map index must correctly identify the updated member
	idx := group.members_map['member-1'] or {
		assert false, 'member-1 should still be in map'
		return
	}
	assert group.members[idx].member_id == 'member-1'
	assert group.members[idx].metadata == [u8(1), 2, 3]
}

fn test_join_group_auto_generates_member_id() {
	// When member_id is empty, coordinator should generate a unique ID.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     ''
		protocol_type: 'consumer'
		protocols:     []Protocol{}
	}

	resp := coordinator.join_group(req)

	assert resp.error_code == 0
	assert resp.member_id.len > 0
}

fn test_join_group_empty_group_id_returns_error() {
	// An empty group_id must be rejected with invalid_group_id error code.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      ''
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     []Protocol{}
	}

	resp := coordinator.join_group(req)

	assert resp.error_code == i16(domain.ErrorCode.invalid_group_id), 'Expected invalid_group_id error, got ${resp.error_code}'
}

fn test_join_group_leader_is_first_member() {
	// The first member to join must be assigned as the group leader.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'leader-member'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}

	resp := coordinator.join_group(req)

	assert resp.error_code == 0
	assert resp.leader == 'leader-member'
}

fn test_join_group_leader_receives_member_list() {
	// The leader must receive the full member list; non-leaders get an empty list.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req1 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req2 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}

	coordinator.join_group(req1)
	resp2 := coordinator.join_group(req2)

	// member-2 is not the leader, so it should receive an empty member list
	assert resp2.leader == 'member-1'
	assert resp2.members.len == 0
}

// ============================================================
// sync_group tests
// ============================================================

fn test_sync_group_leader_provides_assignment() {
	// The leader should be able to submit assignments for all members.
	// generation_id is taken from the last join so it matches the persisted group.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req1 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req2 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	coordinator.join_group(req1)
	// Use the generation_id from the last join, which is the one persisted.
	join_resp2 := coordinator.join_group(req2)

	sync_req := SyncGroupRequest{
		group_id:      'test-group'
		generation_id: join_resp2.generation_id
		member_id:     'member-1'
		assignments:   [
			MemberAssignment{
				member_id:  'member-1'
				assignment: [u8(0), 1]
			},
			MemberAssignment{
				member_id:  'member-2'
				assignment: [u8(2), 3]
			},
		]
	}

	sync_resp := coordinator.sync_group(sync_req)

	assert sync_resp.error_code == 0
	assert sync_resp.assignment == [u8(0), 1]
}

fn test_sync_group_member_receives_assignment() {
	// A non-leader member should receive the assignment that it specified in its own sync request.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req1 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	req2 := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	coordinator.join_group(req1)
	// The last join determines the persisted generation_id.
	join_resp2 := coordinator.join_group(req2)

	// member-2 sends a sync that includes its own assignment entry
	sync_req := SyncGroupRequest{
		group_id:      'test-group'
		generation_id: join_resp2.generation_id
		member_id:     'member-2'
		assignments:   [
			MemberAssignment{
				member_id:  'member-2'
				assignment: [u8(5), 6, 7]
			},
		]
	}

	sync_resp := coordinator.sync_group(sync_req)

	assert sync_resp.error_code == 0
	assert sync_resp.assignment == [u8(5), 6, 7]
}

fn test_sync_group_unknown_member_returns_error() {
	// Syncing with a member_id that never joined must return unknown_member_id.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	join_resp := coordinator.join_group(req)

	sync_req := SyncGroupRequest{
		group_id:      'test-group'
		generation_id: join_resp.generation_id
		member_id:     'ghost-member'
		assignments:   []MemberAssignment{}
	}

	sync_resp := coordinator.sync_group(sync_req)

	assert sync_resp.error_code == i16(domain.ErrorCode.unknown_member_id), 'Expected unknown_member_id, got ${sync_resp.error_code}'
}

fn test_sync_group_nonexistent_group_returns_error() {
	// Syncing on a group that does not exist must return group_id_not_found.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	sync_req := SyncGroupRequest{
		group_id:      'nonexistent-group'
		generation_id: 1
		member_id:     'member-1'
		assignments:   []MemberAssignment{}
	}

	sync_resp := coordinator.sync_group(sync_req)

	assert sync_resp.error_code == i16(domain.ErrorCode.group_id_not_found)
}

fn test_sync_group_wrong_generation_returns_error() {
	// A sync request with an outdated generation_id must be rejected.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	join_resp := coordinator.join_group(req)

	sync_req := SyncGroupRequest{
		group_id:      'test-group'
		generation_id: join_resp.generation_id + 99
		member_id:     'member-1'
		assignments:   []MemberAssignment{}
	}

	sync_resp := coordinator.sync_group(sync_req)

	assert sync_resp.error_code == i16(domain.ErrorCode.illegal_generation)
}

// ============================================================
// heartbeat tests
// ============================================================

fn test_heartbeat_valid_member_succeeds() {
	// A heartbeat from a valid member with the correct generation_id must succeed.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	join_resp := coordinator.join_group(req)

	hb_req := HeartbeatRequest{
		group_id:      'test-group'
		generation_id: join_resp.generation_id
		member_id:     'member-1'
	}

	hb_resp := coordinator.heartbeat(hb_req)

	assert hb_resp.error_code == 0
}

fn test_heartbeat_unknown_member_returns_error() {
	// A heartbeat from a member_id that has not joined must return unknown_member_id.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	join_resp := coordinator.join_group(req)

	hb_req := HeartbeatRequest{
		group_id:      'test-group'
		generation_id: join_resp.generation_id
		member_id:     'ghost-member'
	}

	hb_resp := coordinator.heartbeat(hb_req)

	assert hb_resp.error_code == i16(domain.ErrorCode.unknown_member_id)
}

fn test_heartbeat_wrong_generation_returns_error() {
	// A heartbeat with a stale generation_id must be rejected with illegal_generation.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	join_resp := coordinator.join_group(req)

	hb_req := HeartbeatRequest{
		group_id:      'test-group'
		generation_id: join_resp.generation_id - 1
		member_id:     'member-1'
	}

	hb_resp := coordinator.heartbeat(hb_req)

	assert hb_resp.error_code == i16(domain.ErrorCode.illegal_generation)
}

fn test_heartbeat_nonexistent_group_returns_error() {
	// A heartbeat for a group that does not exist must return group_id_not_found.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	hb_req := HeartbeatRequest{
		group_id:      'nonexistent-group'
		generation_id: 1
		member_id:     'member-1'
	}

	hb_resp := coordinator.heartbeat(hb_req)

	assert hb_resp.error_code == i16(domain.ErrorCode.group_id_not_found)
}

// ============================================================
// leave_group tests
// ============================================================

fn test_leave_group_member_removed_successfully() {
	// After leave_group, the member must be absent from both members slice and members_map.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	coordinator.join_group(req)

	error_code := coordinator.leave_group('test-group', 'member-1')

	assert error_code == 0

	group := storage.load_group('test-group') or {
		assert false, 'Group should still exist'
		return
	}
	assert group.members.len == 0
	assert 'member-1' !in group.members_map
}

fn test_leave_group_swap_and_pop_preserves_map_consistency() {
	// After a swap-and-pop removal, the remaining members_map indices must still
	// point to the correct members in the slice.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	// Join three members in order
	for id in ['member-1', 'member-2', 'member-3'] {
		req := JoinGroupRequest{
			group_id:      'test-group'
			member_id:     id
			protocol_type: 'consumer'
			protocols:     [Protocol{
				name:     'range'
				metadata: []u8{}
			}]
		}
		coordinator.join_group(req)
	}

	// Remove the first member; member-3 should be swapped into index 0
	error_code := coordinator.leave_group('test-group', 'member-1')
	assert error_code == 0

	group := storage.load_group('test-group') or {
		assert false, 'Group should still exist'
		return
	}

	assert group.members.len == 2
	assert 'member-1' !in group.members_map

	// Verify all remaining members_map entries are consistent
	for member_id, idx in group.members_map {
		assert idx < group.members.len, 'Index ${idx} is out of bounds for member ${member_id}'
		assert group.members[idx].member_id == member_id, 'members_map[${member_id}]=${idx} but members[${idx}].member_id=${group.members[idx].member_id}'
	}
}

fn test_leave_group_middle_member_removal_keeps_consistency() {
	// Removing a middle member (not the last) still keeps the map consistent.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	for id in ['member-1', 'member-2', 'member-3', 'member-4'] {
		req := JoinGroupRequest{
			group_id:      'test-group'
			member_id:     id
			protocol_type: 'consumer'
			protocols:     [Protocol{
				name:     'range'
				metadata: []u8{}
			}]
		}
		coordinator.join_group(req)
	}

	error_code := coordinator.leave_group('test-group', 'member-2')
	assert error_code == 0

	group := storage.load_group('test-group') or {
		assert false, 'Group should still exist'
		return
	}

	assert group.members.len == 3
	assert 'member-2' !in group.members_map

	for member_id, idx in group.members_map {
		assert idx < group.members.len
		assert group.members[idx].member_id == member_id
	}
}

fn test_leave_group_nonexistent_member_returns_success() {
	// Leaving as a member_id that was never in the group is treated as idempotent success.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	coordinator.join_group(req)

	error_code := coordinator.leave_group('test-group', 'ghost-member')

	// Idempotent leave: not-found member is treated as already gone
	assert error_code == 0
}

fn test_leave_group_nonexistent_group_returns_error() {
	// Leaving a group that does not exist must return group_id_not_found.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	error_code := coordinator.leave_group('nonexistent-group', 'member-1')

	assert error_code == i16(domain.ErrorCode.group_id_not_found)
}

fn test_leave_group_last_member_sets_empty_state() {
	// When the last member leaves, the group state should transition to empty.
	mut storage := new_coord_mock_storage()
	mut coordinator := new_group_coordinator(storage)

	req := JoinGroupRequest{
		group_id:      'test-group'
		member_id:     'solo-member'
		protocol_type: 'consumer'
		protocols:     [Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	}
	coordinator.join_group(req)

	error_code := coordinator.leave_group('test-group', 'solo-member')
	assert error_code == 0

	group := storage.load_group('test-group') or {
		assert false, 'Group should still exist'
		return
	}
	assert group.state == .empty
}
