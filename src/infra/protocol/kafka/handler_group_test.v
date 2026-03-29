// Group 핸들러 단위 테스트 (process_* 프레임 기반 함수 중심)
// handle_* 바이트 기반 함수는 admin_test.v에서 이미 테스트됨
// process_list_groups, process_describe_groups, process_delete_groups
module kafka

import infra.compression
import service.port
import domain

// -- 테스트 전용 스토리지 (자체 완결) --

struct GroupTestStorage {
mut:
	topics map[string]domain.TopicMetadata
	groups map[string]domain.ConsumerGroup
}

fn new_group_test_storage() &GroupTestStorage {
	mut s := &GroupTestStorage{
		topics: map[string]domain.TopicMetadata{}
		groups: map[string]domain.ConsumerGroup{}
	}
	s.groups['test-group-1'] = domain.ConsumerGroup{
		group_id:      'test-group-1'
		generation_id: 5
		protocol_type: 'consumer'
		protocol:      'range'
		state:         .stable
		leader:        'member-1'
		members:       [
			domain.GroupMember{
				member_id:   'member-1'
				client_id:   'client-1'
				client_host: '/127.0.0.1'
				metadata:    []u8{}
				assignment:  []u8{}
			},
		]
	}
	s.groups['empty-group'] = domain.ConsumerGroup{
		group_id:      'empty-group'
		generation_id: 0
		protocol_type: 'consumer'
		protocol:      ''
		state:         .empty
		leader:        ''
		members:       []
	}
	return s
}

fn (mut s GroupTestStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		partition_count: partitions
		topic_id:        []u8{len: 16}
	}
	s.topics[name] = meta
	return meta
}

fn (mut s GroupTestStorage) delete_topic(name string) ! {
	s.topics.delete(name)
}

fn (s GroupTestStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (s GroupTestStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (s GroupTestStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s GroupTestStorage) add_partitions(name string, new_count int) ! {}

fn (s GroupTestStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (s GroupTestStorage) fetch(topic string, partition int, fetch_offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (s GroupTestStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (s GroupTestStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return error('topic not found')
}

fn (mut s GroupTestStorage) save_group(group domain.ConsumerGroup) ! {
	s.groups[group.group_id] = group
}

fn (mut s GroupTestStorage) load_group(group_id string) !domain.ConsumerGroup {
	return s.groups[group_id] or { return error('group not found') }
}

fn (mut s GroupTestStorage) delete_group(group_id string) ! {
	if group_id !in s.groups {
		return error('group not found')
	}
	s.groups.delete(group_id)
}

fn (s GroupTestStorage) list_groups() ![]domain.GroupInfo {
	mut result := []domain.GroupInfo{}
	for _, g in s.groups {
		state_str := match g.state {
			.empty { 'Empty' }
			.stable { 'Stable' }
			.preparing_rebalance { 'PreparingRebalance' }
			.completing_rebalance { 'CompletingRebalance' }
			.dead { 'Dead' }
		}
		result << domain.GroupInfo{
			group_id:      g.group_id
			protocol_type: g.protocol_type
			state:         state_str
		}
	}
	return result
}

fn (s GroupTestStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (s GroupTestStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s GroupTestStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &GroupTestStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s &GroupTestStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (s GroupTestStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s GroupTestStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s GroupTestStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s GroupTestStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn create_group_test_handler() Handler {
	storage := new_group_test_storage()
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	return new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
}

// -- process_list_groups 테스트 --

fn test_process_list_groups_with_groups() {
	mut handler := create_group_test_handler()
	req := ListGroupsRequest{
		states_filter: []
	}

	resp := handler.process_list_groups(req, 0)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.error_code == 0
	assert resp.groups.len == 2

	mut group_ids := []string{}
	for g in resp.groups {
		group_ids << g.group_id
	}
	assert 'test-group-1' in group_ids
	assert 'empty-group' in group_ids
}

fn test_process_list_groups_empty() {
	storage := &GroupTestStorage{
		topics: map[string]domain.TopicMetadata{}
		groups: map[string]domain.ConsumerGroup{}
	}
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	req := ListGroupsRequest{
		states_filter: []
	}

	resp := handler.process_list_groups(req, 0)!

	assert resp.error_code == 0
	assert resp.groups.len == 0
}

fn test_process_list_groups_returns_protocol_type() {
	mut handler := create_group_test_handler()
	req := ListGroupsRequest{
		states_filter: []
	}

	resp := handler.process_list_groups(req, 0)!

	for g in resp.groups {
		if g.group_id == 'test-group-1' {
			assert g.protocol_type == 'consumer'
			assert g.group_state == 'Stable'
		}
		if g.group_id == 'empty-group' {
			assert g.protocol_type == 'consumer'
			assert g.group_state == 'Empty'
		}
	}
}

// -- process_describe_groups 테스트 --

fn test_process_describe_groups_existing_group() {
	mut handler := create_group_test_handler()
	req := DescribeGroupsRequest{
		groups:                        ['test-group-1']
		include_authorized_operations: false
	}

	resp := handler.process_describe_groups(req, 0)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.groups.len == 1
	assert resp.groups[0].error_code == 0
	assert resp.groups[0].group_id == 'test-group-1'
	assert resp.groups[0].group_state == 'Stable'
	assert resp.groups[0].protocol_type == 'consumer'
	assert resp.groups[0].protocol_data == 'range'
	assert resp.groups[0].members.len == 1
	assert resp.groups[0].members[0].member_id == 'member-1'
	assert resp.groups[0].members[0].client_id == 'client-1'
	assert resp.groups[0].members[0].client_host == '/127.0.0.1'
}

fn test_process_describe_groups_nonexistent_group() {
	mut handler := create_group_test_handler()
	req := DescribeGroupsRequest{
		groups:                        ['nonexistent-group']
		include_authorized_operations: false
	}

	resp := handler.process_describe_groups(req, 0)!

	assert resp.groups.len == 1
	assert resp.groups[0].error_code == i16(ErrorCode.group_id_not_found)
	assert resp.groups[0].group_id == 'nonexistent-group'
	assert resp.groups[0].group_state == ''
	assert resp.groups[0].members.len == 0
}

fn test_process_describe_groups_mixed() {
	mut handler := create_group_test_handler()
	req := DescribeGroupsRequest{
		groups:                        ['test-group-1', 'nonexistent', 'empty-group']
		include_authorized_operations: false
	}

	resp := handler.process_describe_groups(req, 0)!

	assert resp.groups.len == 3
	assert resp.groups[0].group_id == 'test-group-1'
	assert resp.groups[0].error_code == 0
	assert resp.groups[0].group_state == 'Stable'

	assert resp.groups[1].group_id == 'nonexistent'
	assert resp.groups[1].error_code == i16(ErrorCode.group_id_not_found)

	assert resp.groups[2].group_id == 'empty-group'
	assert resp.groups[2].error_code == 0
	assert resp.groups[2].group_state == 'Empty'
	assert resp.groups[2].members.len == 0
}

fn test_process_describe_groups_empty_request() {
	mut handler := create_group_test_handler()
	req := DescribeGroupsRequest{
		groups:                        []
		include_authorized_operations: false
	}

	resp := handler.process_describe_groups(req, 0)!

	assert resp.groups.len == 0
}

fn test_process_describe_groups_state_strings() {
	mut storage := new_group_test_storage()
	storage.groups['prep-group'] = domain.ConsumerGroup{
		group_id:      'prep-group'
		generation_id: 1
		protocol_type: 'consumer'
		state:         .preparing_rebalance
		members:       []
	}
	storage.groups['completing-group'] = domain.ConsumerGroup{
		group_id:      'completing-group'
		generation_id: 1
		protocol_type: 'consumer'
		state:         .completing_rebalance
		members:       []
	}
	storage.groups['dead-group'] = domain.ConsumerGroup{
		group_id:      'dead-group'
		generation_id: 0
		protocol_type: 'consumer'
		state:         .dead
		members:       []
	}

	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	req := DescribeGroupsRequest{
		groups:                        ['prep-group', 'completing-group', 'dead-group']
		include_authorized_operations: false
	}

	resp := handler.process_describe_groups(req, 0)!

	assert resp.groups.len == 3
	assert resp.groups[0].group_state == 'PreparingRebalance'
	assert resp.groups[1].group_state == 'CompletingRebalance'
	assert resp.groups[2].group_state == 'Dead'
}

// -- process_delete_groups 테스트 --

fn test_process_delete_groups_empty_group_success() {
	mut handler := create_group_test_handler()
	req := DeleteGroupsRequest{
		groups_names: ['empty-group']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.results.len == 1
	assert resp.results[0].group_id == 'empty-group'
	assert resp.results[0].error_code == i16(ErrorCode.none)
}

fn test_process_delete_groups_non_empty_group_error() {
	mut handler := create_group_test_handler()
	req := DeleteGroupsRequest{
		groups_names: ['test-group-1']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 1
	assert resp.results[0].group_id == 'test-group-1'
	assert resp.results[0].error_code == i16(ErrorCode.non_empty_group)
}

fn test_process_delete_groups_nonexistent_group() {
	mut handler := create_group_test_handler()
	req := DeleteGroupsRequest{
		groups_names: ['does-not-exist']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 1
	assert resp.results[0].group_id == 'does-not-exist'
	assert resp.results[0].error_code == i16(ErrorCode.group_id_not_found)
}

fn test_process_delete_groups_empty_group_id() {
	mut handler := create_group_test_handler()
	req := DeleteGroupsRequest{
		groups_names: ['']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 1
	assert resp.results[0].group_id == ''
	assert resp.results[0].error_code == i16(ErrorCode.invalid_group_id)
}

fn test_process_delete_groups_dead_group_success() {
	mut storage := new_group_test_storage()
	storage.groups['dead-group'] = domain.ConsumerGroup{
		group_id:      'dead-group'
		generation_id: 0
		protocol_type: 'consumer'
		state:         .dead
		members:       []
	}
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	req := DeleteGroupsRequest{
		groups_names: ['dead-group']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 1
	assert resp.results[0].group_id == 'dead-group'
	assert resp.results[0].error_code == i16(ErrorCode.none)
}

fn test_process_delete_groups_multiple_mixed() {
	mut storage := new_group_test_storage()
	storage.groups['dead-group'] = domain.ConsumerGroup{
		group_id:      'dead-group'
		generation_id: 0
		protocol_type: 'consumer'
		state:         .dead
		members:       []
	}
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	req := DeleteGroupsRequest{
		groups_names: ['empty-group', 'test-group-1', 'nonexistent', 'dead-group', '']
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 5
	assert resp.results[0].error_code == i16(ErrorCode.none) // empty-group
	assert resp.results[1].error_code == i16(ErrorCode.non_empty_group) // test-group-1
	assert resp.results[2].error_code == i16(ErrorCode.group_id_not_found) // nonexistent
	assert resp.results[3].error_code == i16(ErrorCode.none) // dead-group
	assert resp.results[4].error_code == i16(ErrorCode.invalid_group_id) // empty string
}

fn test_process_delete_groups_empty_request() {
	mut handler := create_group_test_handler()
	req := DeleteGroupsRequest{
		groups_names: []
	}

	resp := handler.process_delete_groups(req, 0)!

	assert resp.results.len == 0
}

// -- 인코딩 테스트 --

fn test_delete_groups_response_encode_v0_from_process() {
	resp := DeleteGroupsResponse{
		throttle_time_ms: 0
		results:          [
			DeletableGroupResult{
				group_id:   'group-1'
				error_code: 0
			},
			DeletableGroupResult{
				group_id:   'group-2'
				error_code: i16(ErrorCode.group_id_not_found)
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	throttle := reader.read_i32()!
	assert throttle == 0
	results_len := reader.read_i32()!
	assert results_len == 2
	g1_name := reader.read_string()!
	assert g1_name == 'group-1'
	g1_error := reader.read_i16()!
	assert g1_error == 0
	g2_name := reader.read_string()!
	assert g2_name == 'group-2'
	g2_error := reader.read_i16()!
	assert g2_error == i16(ErrorCode.group_id_not_found)
}

fn test_list_groups_response_encode_v0_from_process() {
	resp := ListGroupsResponse{
		throttle_time_ms: 0
		error_code:       0
		groups:           [
			ListGroupsResponseGroup{
				group_id:      'group-1'
				protocol_type: 'consumer'
				group_state:   'Stable'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	error_code := reader.read_i16()!
	assert error_code == 0
	groups_len := reader.read_i32()!
	assert groups_len == 1
	gid := reader.read_string()!
	assert gid == 'group-1'
	ptype := reader.read_string()!
	assert ptype == 'consumer'
}
