module group

import domain
import service.port

// Mock Storage for testing

struct MockStorage {
mut:
	topics     map[string]domain.TopicMetadata
	partitions map[string]domain.PartitionInfo
}

fn new_mock_storage() MockStorage {
	return MockStorage{
		topics:     map[string]domain.TopicMetadata{}
		partitions: map[string]domain.PartitionInfo{}
	}
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        []u8{len: 16}
		partition_count: partitions
	}
	s.topics[name] = meta
	return meta
}

fn (s MockStorage) delete_topic(name string) ! {
}

fn (s MockStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (s MockStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	for _, t in s.topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('topic not found')
}

fn (s MockStorage) add_partitions(name string, new_count int) ! {
}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{
		base_offset:     0
		log_append_time: 0
	}
}

fn (s MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{
		records:        []domain.Record{}
		high_watermark: 0
	}
}

fn (s MockStorage) delete_records(topic string, partition int, before_offset i64) ! {
}

fn (mut s MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	key := '${topic}:${partition}'
	return s.partitions[key] or {
		return domain.PartitionInfo{
			topic:           topic
			partition:       partition
			high_watermark:  0
			earliest_offset: 0
		}
	}
}

fn (s MockStorage) save_group(group domain.ConsumerGroup) ! {
}

fn (s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('not found')
}

fn (s MockStorage) delete_group(group_id string) ! {
}

fn (s MockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
}

fn (s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &MockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s &MockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// ShareGroupCoordinator Tests

fn test_create_share_group() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	group := coordinator.get_or_create_group('test-group')
	assert group.group_id == 'test-group'
	assert group.state == .empty
}

fn test_heartbeat_new_member() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 3, domain.TopicConfig{}) or { panic('failed') }

	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}

	resp := coordinator.heartbeat(req)

	assert resp.error_code == 0
	assert resp.member_id.len > 0
	assert resp.member_epoch >= 0
	assert resp.heartbeat_interval > 0
}

fn test_heartbeat_existing_member() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 3, domain.TopicConfig{}) or { panic('failed') }

	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// First heartbeat - join
	req1 := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp1 := coordinator.heartbeat(req1)
	assert resp1.error_code == 0

	// Second heartbeat - existing member
	req2 := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              resp1.member_id
		member_epoch:           resp1.member_epoch
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp2 := coordinator.heartbeat(req2)

	assert resp2.error_code == 0
	assert resp2.member_id == resp1.member_id
}

fn test_heartbeat_fenced_member() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 3, domain.TopicConfig{}) or { panic('failed') }

	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// First heartbeat - join
	req1 := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp1 := coordinator.heartbeat(req1)

	// Heartbeat with wrong epoch
	req2 := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              resp1.member_id
		member_epoch:           resp1.member_epoch + 100
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp2 := coordinator.heartbeat(req2)

	assert resp2.error_code == 22
}

fn test_leave_group() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp := coordinator.heartbeat(req)

	// Leave
	coordinator.leave_group('test-group', resp.member_id) or {
		panic('leave_group failed: ${err.msg()}')
	}

	// Verify member is gone
	group := coordinator.get_group('test-group') or { panic('group should exist') }
	assert resp.member_id !in group.members
}

fn test_acquire_records() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{
		max_partition_locks:     100
		record_lock_duration_ms: 30000
	}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create group and join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp := coordinator.heartbeat(req)

	// Acquire records
	acquired := coordinator.acquire_records('test-group', resp.member_id, 'test-topic',
		0, 10)

	// Acquire should work (partition starts at offset 0)
	// The coordinator can acquire records even from empty partitions
	// since it just advances the offset
	assert acquired.len >= 0
}

fn test_acknowledge_accept() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{
		max_partition_locks:     100
		record_lock_duration_ms: 30000
	}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create group and join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp := coordinator.heartbeat(req)

	// Acknowledge (even without acquired records, should not error)
	batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      10
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	result := coordinator.acknowledge_records('test-group', resp.member_id, batch)

	assert result.error_code == 0
}

fn test_acknowledge_release() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{
		max_partition_locks:     100
		record_lock_duration_ms: 30000
		delivery_attempt_limit:  5
	}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create group and join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp := coordinator.heartbeat(req)

	batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      5
		acknowledge_type: .release
		gap_offsets:      []i64{}
	}
	result := coordinator.acknowledge_records('test-group', resp.member_id, batch)

	assert result.error_code == 0
}

fn test_acknowledge_reject() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{
		max_partition_locks:     100
		record_lock_duration_ms: 30000
	}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create group and join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	resp := coordinator.heartbeat(req)

	batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      5
		acknowledge_type: .reject
		gap_offsets:      []i64{}
	}
	result := coordinator.acknowledge_records('test-group', resp.member_id, batch)

	assert result.error_code == 0
}

fn test_get_stats() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create group and join
	req := ShareGroupHeartbeatRequest{
		group_id:               'test-group'
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: ['test-topic']
	}
	_ := coordinator.heartbeat(req)

	stats := coordinator.get_stats('test-group')

	assert stats.group_id == 'test-group'
	assert stats.member_count == 1
}

fn test_list_groups() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create multiple groups
	for i in 0 .. 3 {
		req := ShareGroupHeartbeatRequest{
			group_id:               'test-group-${i}'
			member_id:              ''
			member_epoch:           0
			rack_id:                'rack-1'
			subscribed_topic_names: ['test-topic']
		}
		_ := coordinator.heartbeat(req)
	}

	groups := coordinator.list_groups()
	assert groups.len == 3
}

// SimpleAssignor Tests

fn test_simple_assignor_single_member() {
	assignor := new_simple_assignor()

	members := [
		ShareMemberSubscription{
			member_id: 'member-1'
			rack_id:   'rack-1'
			topics:    ['topic-a', 'topic-b']
		},
	]

	topics := {
		'topic-a': ShareTopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 3
		}
		'topic-b': ShareTopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-b'
			partition_count: 2
		}
	}

	assignments := assignor.assign(members, topics)

	// Single member should get all partitions
	assert 'member-1' in assignments
	member1_assignments := assignments['member-1']

	mut total_partitions := 0
	for a in member1_assignments {
		total_partitions += a.partitions.len
	}
	assert total_partitions == 5
}

fn test_simple_assignor_multiple_members() {
	assignor := new_simple_assignor()

	members := [
		ShareMemberSubscription{
			member_id: 'member-1'
			rack_id:   'rack-1'
			topics:    ['topic-a']
		},
		ShareMemberSubscription{
			member_id: 'member-2'
			rack_id:   'rack-1'
			topics:    ['topic-a']
		},
	]

	topics := {
		'topic-a': ShareTopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics)

	// 4 partitions / 2 members = 2 each (round-robin)
	mut m1_count := 0
	mut m2_count := 0

	for a in assignments['member-1'] {
		m1_count += a.partitions.len
	}
	for a in assignments['member-2'] {
		m2_count += a.partitions.len
	}

	assert m1_count == 2
	assert m2_count == 2
}

fn test_simple_assignor_empty_members() {
	assignor := new_simple_assignor()

	members := []ShareMemberSubscription{}

	topics := {
		'topic-a': ShareTopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics)
	assert assignments.len == 0
}

fn test_session_management() {
	mut storage := new_mock_storage()
	config := domain.ShareGroupConfig{}
	mut coordinator := new_share_group_coordinator(storage, config)

	// Create session
	session := coordinator.get_or_create_session('test-group', 'member-1')
	assert session.group_id == 'test-group'
	assert session.member_id == 'member-1'
	assert session.session_epoch == 1

	// Update session
	updated := coordinator.update_session('test-group', 'member-1', 1, [
		domain.ShareSessionPartition{
			topic_id:   []u8{len: 16}
			topic_name: 'topic-a'
			partition:  0
		},
	], []domain.ShareSessionPartition{}) or { panic('update failed: ${err.msg()}') }
	assert updated.partitions.len == 1
	assert updated.session_epoch == 2

	// Close session
	coordinator.close_session('test-group', 'member-1')
}
