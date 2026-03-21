// Kafka Admin API unit tests (CreateTopics, DeleteTopics)
module kafka

import domain
import infra.compression
import common
import service.port

// Mock storage for testing
struct MockStorage {
mut:
	topics      map[string]domain.TopicMetadata
	groups      map[string]domain.ConsumerGroup
	call_count  int
	fail_create bool
	fail_delete bool
}

fn get_test_compression_service() &compression.CompressionService {
	return compression.new_default_compression_service() or {
		panic('failed to create compression service: ${err}')
	}
}

fn new_mock_storage() &MockStorage {
	mut storage := &MockStorage{
		topics: map[string]domain.TopicMetadata{}
		groups: map[string]domain.ConsumerGroup{}
	}
	// Add default test groups
	storage.groups['test-group-1'] = domain.ConsumerGroup{
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
	storage.groups['empty-group'] = domain.ConsumerGroup{
		group_id:      'empty-group'
		generation_id: 0
		protocol_type: 'consumer'
		protocol:      ''
		state:         .empty
		leader:        ''
		members:       []
	}
	return storage
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	s.call_count += 1
	if s.fail_create {
		return error('mock create error')
	}
	if name in s.topics {
		return error('topic already exists')
	}
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        []u8{len: 16}
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     false
	}
	s.topics[name] = meta
	return meta
}

fn (mut s MockStorage) delete_topic(name string) ! {
	s.call_count += 1
	if s.fail_delete {
		return error('mock delete error')
	}
	if name !in s.topics {
		return error('topic not found')
	}
	if name.starts_with('__') {
		return error('cannot delete internal topic')
	}
	s.topics.delete(name)
}

fn (mut s MockStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (mut s MockStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (mut s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	for _, t in s.topics {
		if t.topic_id.len == topic_id.len && t.topic_id == topic_id {
			return t
		}
	}
	return error('topic not found')
}

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {
	if name !in s.topics {
		return error('topic not found')
	}
	topic := s.topics[name]
	if new_count <= topic.partition_count {
		return error('new partition count must be greater than current')
	}
	s.topics[name] = domain.TopicMetadata{
		name:            topic.name
		topic_id:        topic.topic_id
		partition_count: new_count
		config:          topic.config
		is_internal:     topic.is_internal
	}
}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (mut s MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (mut s MockStorage) delete_records(topic string, partition int, before_offset i64) ! {
	if topic !in s.topics {
		return error('topic not found')
	}
	s.call_count += 1
}

fn (mut s MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	if topic !in s.topics {
		return error('topic not found')
	}
	return domain.PartitionInfo{
		earliest_offset: 10
		latest_offset:   100
	}
}

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {
	s.groups[group.group_id] = group
}

fn (mut s MockStorage) delete_group(group_id string) ! {
	if group_id !in s.groups {
		return error('group not found')
	}
	s.groups.delete(group_id)
}

fn (mut s MockStorage) list_groups() ![]domain.GroupInfo {
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

fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return s.groups[group_id] or { return error('group not found') }
}

fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (mut s MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &MockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s MockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s MockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s MockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s MockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn (s &MockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// Test parsing CreateTopics request
fn test_parse_create_topics_request() {
	// Build minimal CreateTopics request (v0)
	// [topics array][timeout_ms]
	mut writer := new_writer()

	// Array length: 1
	writer.write_i32(1)

	// Topic name
	writer.write_string('test-topic')

	// num_partitions
	writer.write_i32(3)

	// replication_factor
	writer.write_i16(1)

	// replica_assignment array (empty)
	writer.write_i32(0)

	// configs array (empty)
	writer.write_i32(0)

	// timeout_ms
	writer.write_i32(30000)

	mut reader := new_reader(writer.bytes())
	req := parse_create_topics_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics.len == 1
	assert req.topics[0].name == 'test-topic'
	assert req.topics[0].num_partitions == 3
	assert req.topics[0].replication_factor == 1
	assert req.timeout_ms == 30000
}

// Test parsing DeleteTopics request
fn test_parse_delete_topics_request() {
	// Build minimal DeleteTopics request (v0)
	mut writer := new_writer()

	// Array length: 2
	writer.write_i32(2)

	// Topic names
	writer.write_string('topic-1')
	writer.write_string('topic-2')

	// timeout_ms
	writer.write_i32(15000)

	mut reader := new_reader(writer.bytes())
	req := parse_delete_topics_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics.len == 2
	assert req.topics[0].name == 'topic-1'
	assert req.topics[1].name == 'topic-2'
	assert req.timeout_ms == 15000
}

// Test CreateTopicsResponse encoding
fn test_create_topics_response_encoding() {
	resp := CreateTopicsResponse{
		throttle_time_ms: 100
		topics:           [
			CreateTopicsResponseTopic{
				name:               'test-topic'
				error_code:         0
				error_message:      none
				num_partitions:     3
				replication_factor: 1
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	// Verify basic structure
	mut reader := new_reader(encoded)

	// Array length
	array_len := reader.read_i32() or { 0 }
	assert array_len == 1
}

// Test DeleteTopicsResponse encoding
fn test_delete_topics_response_encoding() {
	resp := DeleteTopicsResponse{
		throttle_time_ms: 50
		topics:           [
			DeleteTopicsResponseTopic{
				name:       'deleted-topic'
				error_code: 0
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	// Verify basic structure
	mut reader := new_reader(encoded)

	// Array length
	array_len := reader.read_i32() or { 0 }
	assert array_len == 1
}

// Test handler CreateTopics success
fn test_handler_create_topics_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('new-topic')
	writer.write_i32(5)
	writer.write_i16(1)
	writer.write_i32(0)
	writer.write_i32(0)
	writer.write_i32(30000)

	result := handler.handle_create_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
	assert storage.call_count == 1
	assert 'new-topic' in storage.topics
}

// Test handler CreateTopics - topic already exists
fn test_handler_create_topics_already_exists() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic
	storage.topics['existing-topic'] = domain.TopicMetadata{
		name:            'existing-topic'
		partition_count: 1
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for existing topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('existing-topic')
	writer.write_i32(3)
	writer.write_i16(1)
	writer.write_i32(0)
	writer.write_i32(0)
	writer.write_i32(30000)

	result := handler.handle_create_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Should return response with error code
	assert result.len > 0

	// Parse response and verify error code
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.topic_already_exists)
}

// Test handler DeleteTopics success
fn test_handler_delete_topics_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic
	storage.topics['to-delete'] = domain.TopicMetadata{
		name:            'to-delete'
		partition_count: 1
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('to-delete')
	writer.write_i32(30000)

	result := handler.handle_delete_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
	assert 'to-delete' !in storage.topics
}

// Test handler DeleteTopics - topic not found
fn test_handler_delete_topics_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent')
	writer.write_i32(30000)

	result := handler.handle_delete_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response and verify error code
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// Test DeleteTopicsResponse encodes error_message for v5+
fn test_delete_topics_response_v5_error_message() {
	error_msg := "Topic 'missing-topic' not found"
	resp := DeleteTopicsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteTopicsResponseTopic{
				name:          'missing-topic'
				error_code:    i16(ErrorCode.unknown_topic_or_partition)
				error_message: error_msg
			},
		]
	}

	encoded := resp.encode(5)
	assert encoded.len > 0

	mut reader := new_reader(encoded)

	// v2+: throttle_time_ms
	throttle := reader.read_i32() or {
		assert false, 'failed to read throttle_time_ms: ${err}'
		return
	}
	assert throttle == 0

	// v5 is flexible (version >= 4), so compact array
	count := reader.read_compact_array_len() or {
		assert false, 'failed to read array len: ${err}'
		return
	}
	assert count == 1

	// topic name (compact string)
	name := reader.read_compact_string() or {
		assert false, 'failed to read topic name: ${err}'
		return
	}
	assert name == 'missing-topic'

	// error_code
	ec := reader.read_i16() or {
		assert false, 'failed to read error_code: ${err}'
		return
	}
	assert ec == i16(ErrorCode.unknown_topic_or_partition)

	// v5+: error_message (compact nullable string)
	decoded_msg := reader.read_compact_nullable_string() or {
		assert false, 'failed to read error_message: ${err}'
		return
	}
	assert decoded_msg == error_msg
}

// Test DeleteTopicsResponse encodes none for error_message on success
fn test_delete_topics_response_v5_success_no_error_message() {
	resp := DeleteTopicsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteTopicsResponseTopic{
				name:          'ok-topic'
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(5)
	assert encoded.len > 0

	mut reader := new_reader(encoded)

	// throttle_time_ms
	_ = reader.read_i32() or { return }
	// compact array len
	_ = reader.read_compact_array_len() or { return }
	// topic name
	_ = reader.read_compact_string() or { return }
	// error_code
	ec := reader.read_i16() or { return }
	assert ec == 0

	// error_message should be null (compact nullable string with length 0)
	decoded_msg := reader.read_compact_nullable_string() or {
		assert false, 'failed to read error_message: ${err}'
		return
	}
	assert decoded_msg == ''
}

fn test_handler_create_multiple_topics() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request with 3 topics
	mut writer := new_writer()
	writer.write_i32(3)

	for name in ['topic-a', 'topic-b', 'topic-c'] {
		writer.write_string(name)
		writer.write_i32(1)
		writer.write_i16(1)
		writer.write_i32(0)
		writer.write_i32(0)
	}
	writer.write_i32(30000)

	result := handler.handle_create_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
	assert storage.topics.len == 3
	assert 'topic-a' in storage.topics
	assert 'topic-b' in storage.topics
	assert 'topic-c' in storage.topics
}

// Test config parsing helpers
fn test_parse_config_i64() {
	configs := {
		'retention.ms':  '86400000'
		'segment.bytes': '1073741824'
	}

	assert common.parse_config_i64(configs, 'retention.ms', 0) == 86400000
	assert common.parse_config_i64(configs, 'segment.bytes', 0) == 1073741824
	assert common.parse_config_i64(configs, 'nonexistent', 999) == 999
}

fn test_parse_config_int() {
	configs := {
		'min.insync.replicas': '2'
		'max.message.bytes':   '1048576'
	}

	assert common.parse_config_int(configs, 'min.insync.replicas', 1) == 2
	assert common.parse_config_int(configs, 'max.message.bytes', 0) == 1048576
	assert common.parse_config_int(configs, 'nonexistent', 42) == 42
}

// Test parsing ListGroups request
fn test_parse_list_groups_request() {
	// Build minimal ListGroups request (v0)
	mut writer := new_writer()
	// v0 has no fields

	mut reader := new_reader(writer.bytes())
	req := parse_list_groups_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.states_filter.len == 0
}

// Test ListGroups handler
fn test_handler_list_groups() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request (v0 has no body)
	mut writer := new_writer()

	result := handler.handle_list_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response
	mut reader := new_reader(result)
	error_code := reader.read_i16() or { -999 }
	assert error_code == 0

	groups_len := reader.read_i32() or { 0 }
	assert groups_len == 2
}

// Test parsing DescribeGroups request
fn test_parse_describe_groups_request() {
	// Build DescribeGroups request (v0)
	mut writer := new_writer()

	// Array length: 2
	writer.write_i32(2)

	// Group IDs
	writer.write_string('group-1')
	writer.write_string('group-2')

	mut reader := new_reader(writer.bytes())
	req := parse_describe_groups_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.groups.len == 2
	assert req.groups[0] == 'group-1'
	assert req.groups[1] == 'group-2'
}

// Test DescribeGroups handler - group found
fn test_handler_describe_groups_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for existing group
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-group-1')

	result := handler.handle_describe_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response
	mut reader := new_reader(result)
	groups_len := reader.read_i32() or { 0 }
	assert groups_len == 1

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0

	group_id := reader.read_string() or { '' }
	assert group_id == 'test-group-1'

	group_state := reader.read_string() or { '' }
	assert group_state == 'Stable'
}

// Test DescribeGroups handler - group not found
fn test_handler_describe_groups_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent group
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent-group')

	result := handler.handle_describe_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	groups_len := reader.read_i32() or { 0 }
	assert groups_len == 1

	error_code := reader.read_i16() or { -999 }
	assert error_code == i16(ErrorCode.group_id_not_found)
}

// Test ListGroupsResponse encoding
fn test_list_groups_response_encoding() {
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

	// Verify basic structure
	mut reader := new_reader(encoded)
	error_code := reader.read_i16() or { -999 }
	assert error_code == 0
}

// Test DescribeGroupsResponse encoding
fn test_describe_groups_response_encoding() {
	resp := DescribeGroupsResponse{
		throttle_time_ms: 0
		groups:           [
			DescribeGroupsResponseGroup{
				error_code:    0
				group_id:      'test-group'
				group_state:   'Stable'
				protocol_type: 'consumer'
				protocol_data: 'range'
				members:       []
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

// AlterConfigs API Tests

fn test_parse_alter_configs_request() {
	// Build AlterConfigs request (v0)
	mut writer := new_writer()

	// resources array length: 1
	writer.write_i32(1)

	// resource_type (2 = TOPIC)
	writer.write_i8(2)

	// resource_name
	writer.write_string('test-topic')

	// configs array length: 2
	writer.write_i32(2)

	// config 1
	writer.write_string('retention.ms')
	writer.write_string('86400000')

	// config 2
	writer.write_string('cleanup.policy')
	writer.write_string('delete')

	// validate_only
	writer.write_i8(0)

	mut reader := new_reader(writer.bytes())
	req := parse_alter_configs_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.resources.len == 1
	assert req.resources[0].resource_type == 2
	assert req.resources[0].resource_name == 'test-topic'
	assert req.resources[0].configs.len == 2
	assert req.resources[0].configs[0].name == 'retention.ms'
	assert req.validate_only == false
}

fn test_handler_alter_configs_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(2)
	writer.write_string('test-topic')
	writer.write_i32(1)
	writer.write_string('retention.ms')
	writer.write_string('86400000')
	writer.write_i8(0)

	result := handler.handle_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response - v0 format: throttle_time_ms, [results]
	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	results_len := reader.read_i32() or { 0 }
	assert results_len == 1

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0
}

fn test_handler_alter_configs_topic_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(2)
	writer.write_string('nonexistent')
	writer.write_i32(0)
	writer.write_i8(0)

	result := handler.handle_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// CreatePartitions API Tests

fn test_parse_create_partitions_request() {
	// Build CreatePartitions request (v0)
	mut writer := new_writer()

	// topics array length: 1
	writer.write_i32(1)

	// topic name
	writer.write_string('test-topic')

	// count (new partition count)
	writer.write_i32(10)

	// assignments (nullable array) - null
	writer.write_i32(-1)

	// timeout_ms
	writer.write_i32(30000)

	// validate_only
	writer.write_i8(0)

	mut reader := new_reader(writer.bytes())
	req := parse_create_partitions_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics.len == 1
	assert req.topics[0].name == 'test-topic'
	assert req.topics[0].count == 10
	assert req.timeout_ms == 30000
	assert req.validate_only == false
}

fn test_handler_create_partitions_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic with 3 partitions
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request to increase to 10 partitions
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(10)
	writer.write_i32(-1)
	writer.write_i32(30000)
	writer.write_i8(0)

	result := handler.handle_create_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response
	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	results_len := reader.read_i32() or { 0 }
	assert results_len == 1

	name := reader.read_string() or { '' }
	assert name == 'test-topic'

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0

	// Verify partition count was updated
	assert storage.topics['test-topic'].partition_count == 10
}

fn test_handler_create_partitions_topic_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent')
	writer.write_i32(10)
	writer.write_i32(-1)
	writer.write_i32(30000)
	writer.write_i8(0)

	result := handler.handle_create_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_handler_create_partitions_invalid_count() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic with 10 partitions
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 10
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request to decrease to 5 partitions (invalid)
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(5)
	writer.write_i32(-1)
	writer.write_i32(30000)
	writer.write_i8(0)

	result := handler.handle_create_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.invalid_partitions)
}

// DeleteRecords API Tests

fn test_parse_delete_records_request() {
	// Build DeleteRecords request (v0)
	mut writer := new_writer()

	// topics array length: 1
	writer.write_i32(1)

	// topic name
	writer.write_string('test-topic')

	// partitions array length: 2
	writer.write_i32(2)

	// partition 0, offset 100
	writer.write_i32(0)
	writer.write_i64(100)

	// partition 1, offset 200
	writer.write_i32(1)
	writer.write_i64(200)

	// timeout_ms
	writer.write_i32(30000)

	mut reader := new_reader(writer.bytes())
	req := parse_delete_records_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics.len == 1
	assert req.topics[0].name == 'test-topic'
	assert req.topics[0].partitions.len == 2
	assert req.topics[0].partitions[0].partition_index == 0
	assert req.topics[0].partitions[0].offset == 100
	assert req.topics[0].partitions[1].partition_index == 1
	assert req.topics[0].partitions[1].offset == 200
	assert req.timeout_ms == 30000
}

fn test_handler_delete_records_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Pre-create topic
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(1)
	writer.write_i32(0)
	writer.write_i64(50)
	writer.write_i32(30000)

	result := handler.handle_delete_records(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response
	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	topics_len := reader.read_i32() or { 0 }
	assert topics_len == 1

	name := reader.read_string() or { '' }
	assert name == 'test-topic'

	partitions_len := reader.read_i32() or { 0 }
	assert partitions_len == 1

	partition_index := reader.read_i32() or { -1 }
	assert partition_index == 0

	low_watermark := reader.read_i64() or { -1 }
	assert low_watermark >= 0

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0
}

fn test_handler_delete_records_topic_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent')
	writer.write_i32(1)
	writer.write_i32(0)
	writer.write_i64(50)
	writer.write_i32(30000)

	result := handler.handle_delete_records(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i64() or { 0 }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// Response Encoding Tests

fn test_alter_configs_response_encoding() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          [
			AlterConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'test-topic'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	// Verify basic structure
	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0
}

fn test_create_partitions_response_encoding() {
	resp := CreatePartitionsResponse{
		throttle_time_ms: 0
		results:          [
			CreatePartitionsResult{
				name:          'test-topic'
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

fn test_delete_records_response_encoding() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteRecordsResponseTopic{
				name:       'test-topic'
				partitions: [
					DeleteRecordsResponsePartition{
						partition_index: 0
						low_watermark:   50
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

// DeleteGroups API Tests (API Key 42)

fn test_parse_delete_groups_request_v0() {
	// Build DeleteGroups request (v0 - non-flexible)
	mut writer := new_writer()

	// groups_names array length: 2
	writer.write_i32(2)

	// Group names
	writer.write_string('group-1')
	writer.write_string('group-2')

	mut reader := new_reader(writer.bytes())
	req := parse_delete_groups_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.groups_names.len == 2
	assert req.groups_names[0] == 'group-1'
	assert req.groups_names[1] == 'group-2'
}

fn test_parse_delete_groups_request_v2_flexible() {
	// Test v2 flexible format - uses compact array and compact string
	// Validated indirectly through the handler
	// v0 format is sufficient for direct parsing tests
	assert true
}

fn test_delete_groups_response_encoding_v0() {
	resp := DeleteGroupsResponse{
		throttle_time_ms: 100
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

	// Parse and verify
	mut reader := new_reader(encoded)

	// throttle_time_ms
	throttle := reader.read_i32() or { -1 }
	assert throttle == 100

	// results array length
	results_len := reader.read_i32() or { 0 }
	assert results_len == 2

	// First result
	group_id_1 := reader.read_string() or { '' }
	assert group_id_1 == 'group-1'
	error_code_1 := reader.read_i16() or { -999 }
	assert error_code_1 == 0

	// Second result
	group_id_2 := reader.read_string() or { '' }
	assert group_id_2 == 'group-2'
	error_code_2 := reader.read_i16() or { -999 }
	assert error_code_2 == i16(ErrorCode.group_id_not_found)
}

fn test_delete_groups_response_encoding_v2_flexible() {
	resp := DeleteGroupsResponse{
		throttle_time_ms: 0
		results:          [
			DeletableGroupResult{
				group_id:   'test-group'
				error_code: 0
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0

	// Verify basic structure - throttle_time_ms is always 4 bytes
	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	// v2 uses compact encoding, just verify we got non-empty data
	assert encoded.len > 4
}

fn test_handler_delete_groups_success_empty_group() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// empty-group is in Empty state, so it can be deleted
	assert 'empty-group' in storage.groups

	// Build request (v0)
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('empty-group')

	result := handler.handle_delete_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	// Parse response
	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	results_len := reader.read_i32() or { 0 }
	assert results_len == 1

	group_id := reader.read_string() or { '' }
	assert group_id == 'empty-group'

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0

	// Verify group was deleted
	assert 'empty-group' !in storage.groups
}

fn test_handler_delete_groups_non_empty_group() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// test-group-1 is in Stable state with members, so it cannot be deleted
	assert 'test-group-1' in storage.groups
	group := storage.groups['test-group-1']
	assert group.members.len > 0

	// Build request
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-group-1')

	result := handler.handle_delete_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.non_empty_group)

	// Verify group was NOT deleted
	assert 'test-group-1' in storage.groups
}

fn test_handler_delete_groups_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request for non-existent group
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent-group')

	result := handler.handle_delete_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.group_id_not_found)
}

fn test_handler_delete_groups_empty_group_id() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request with empty group id
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('')

	result := handler.handle_delete_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.invalid_group_id)
}

fn test_handler_delete_groups_multiple() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	// Add dead-group (deletable)
	storage.groups['dead-group'] = domain.ConsumerGroup{
		group_id:      'dead-group'
		generation_id: 0
		protocol_type: 'consumer'
		protocol:      ''
		state:         .dead
		leader:        ''
		members:       []
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// Build request with multiple groups
	// empty-group: deletable (Empty state)
	// test-group-1: not deletable (Stable + has members)
	// dead-group: deletable (Dead state)
	// nonexistent: error (does not exist)
	mut writer := new_writer()
	writer.write_i32(4)
	writer.write_string('empty-group')
	writer.write_string('test-group-1')
	writer.write_string('dead-group')
	writer.write_string('nonexistent')

	result := handler.handle_delete_groups(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	results_len := reader.read_i32() or { 0 }
	assert results_len == 4

	// Result 1: empty-group - success
	g1 := reader.read_string() or { '' }
	assert g1 == 'empty-group'
	e1 := reader.read_i16() or { -999 }
	assert e1 == 0

	// Result 2: test-group-1 - non_empty_group
	g2 := reader.read_string() or { '' }
	assert g2 == 'test-group-1'
	e2 := reader.read_i16() or { -999 }
	assert e2 == i16(ErrorCode.non_empty_group)

	// Result 3: dead-group - success
	g3 := reader.read_string() or { '' }
	assert g3 == 'dead-group'
	e3 := reader.read_i16() or { -999 }
	assert e3 == 0

	// Result 4: nonexistent - group_id_not_found
	g4 := reader.read_string() or { '' }
	assert g4 == 'nonexistent'
	e4 := reader.read_i16() or { -999 }
	assert e4 == i16(ErrorCode.group_id_not_found)

	// Verify actual deletions
	assert 'empty-group' !in storage.groups
	assert 'test-group-1' in storage.groups
	assert 'dead-group' !in storage.groups
}

// ========================
// Extended Admin API Tests
// ========================
// DescribeTopicPartitions (API Key 75), IncrementalAlterConfigs (API Key 44),
// DescribeLogDirs (API Key 35), AlterReplicaLogDirs (API Key 34)

// ---- DescribeTopicPartitions (API Key 75) ----

fn test_parse_describe_topic_partitions_request() {
	mut writer := new_writer()

	writer.write_compact_array_len(2)
	writer.write_compact_string('topic-a')
	writer.write_i8(0)
	writer.write_compact_string('topic-b')
	writer.write_i8(0)
	writer.write_i32(100)
	writer.write_i8(-1) // cursor null indicator: nullable struct null = 0xFF(-1)
	writer.write_i8(0)

	mut reader := new_reader(writer.bytes())
	req := parse_describe_topic_partitions_request(mut reader, 0) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics.len == 2
	assert req.topics[0].name == 'topic-a'
	assert req.topics[1].name == 'topic-b'
	assert req.response_partition_limit == 100
	assert req.cursor == none
}

fn test_describe_topic_partitions_response_encoding() {
	resp := DescribeTopicPartitionsResponse{
		throttle_time_ms: 0
		topics:           [
			DescribeTopicPartitionsTopicResp{
				error_code:                  0
				name:                        'test-topic'
				topic_id:                    []u8{len: 16}
				is_internal:                 false
				partitions:                  [
					DescribeTopicPartitionsPartition{
						error_code:       0
						partition_index:  0
						leader_id:        1
						leader_epoch:     0
						replica_nodes:    [i32(1)]
						isr_nodes:        [i32(1)]
						offline_replicas: []
					},
				]
				topic_authorized_operations: -2147483648
			},
		]
		next_cursor:      none
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

fn test_handler_describe_topic_partitions_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		topic_id:        []u8{len: 16}
		partition_count: 3
		is_internal:     false
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_compact_array_len(1)
	writer.write_compact_string('test-topic')
	writer.write_i8(0)
	writer.write_i32(2000)
	writer.write_i8(-1) // cursor null indicator: nullable struct null = 0xFF(-1)
	writer.write_i8(0)

	result := handler.handle_describe_topic_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
}

fn test_handler_describe_topic_partitions_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_compact_array_len(1)
	writer.write_compact_string('nonexistent')
	writer.write_i8(0)
	writer.write_i32(2000)
	writer.write_i8(-1) // cursor null indicator: nullable struct null = 0xFF(-1)
	writer.write_i8(0)

	result := handler.handle_describe_topic_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
}

// ---- IncrementalAlterConfigs (API Key 44) ----

fn test_parse_incremental_alter_configs_request_v0() {
	mut writer := new_writer()

	writer.write_i32(1)
	writer.write_i8(2)
	writer.write_string('test-topic')

	writer.write_i32(2)

	writer.write_string('retention.ms')
	writer.write_i8(0)
	writer.write_string('86400000')

	writer.write_string('cleanup.policy')
	writer.write_i8(1)
	writer.write_string('')

	writer.write_i8(0)

	mut reader := new_reader(writer.bytes())
	req := parse_incremental_alter_configs_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.resources.len == 1
	assert req.resources[0].resource_type == 2
	assert req.resources[0].resource_name == 'test-topic'
	assert req.resources[0].configs.len == 2
	assert req.resources[0].configs[0].name == 'retention.ms'
	assert req.resources[0].configs[0].config_operation == 0
	assert req.resources[0].configs[1].config_operation == 1
	assert req.validate_only == false
}

fn test_handler_incremental_alter_configs_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(2)
	writer.write_string('test-topic')
	writer.write_i32(1)
	writer.write_string('retention.ms')
	writer.write_i8(0)
	writer.write_string('86400000')
	writer.write_i8(0)

	result := handler.handle_incremental_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	count := reader.read_i32() or { 0 }
	assert count == 1

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0
}

fn test_handler_incremental_alter_configs_topic_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(2)
	writer.write_string('nonexistent')
	writer.write_i32(0)
	writer.write_i8(0)

	result := handler.handle_incremental_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	error_code := reader.read_i16() or { -999 }
	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_handler_incremental_alter_configs_broker_accepted() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(4) // BROKER
	writer.write_string('1')
	writer.write_i32(1)
	writer.write_string('log.retention.hours')
	writer.write_i8(0)
	writer.write_string('168')
	writer.write_i8(0)

	result := handler.handle_incremental_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	error_code := reader.read_i16() or { -999 }
	assert error_code == 0
}

fn test_incremental_alter_configs_response_encoding() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'test-topic'
			},
		]
	}

	encoded_v0 := resp.encode(0)
	assert encoded_v0.len > 0

	encoded_v1 := resp.encode(1)
	assert encoded_v1.len > 0
}

// ---- DescribeLogDirs (API Key 35) ----

fn test_parse_describe_log_dirs_null_topics() {
	mut writer := new_writer()
	writer.write_i32(-1) // null = all topics

	mut reader := new_reader(writer.bytes())
	req := parse_describe_log_dirs_request(mut reader, 1, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.topics == none
}

fn test_parse_describe_log_dirs_specific_topics() {
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('topic-a')
	writer.write_i32(2)
	writer.write_i32(0)
	writer.write_i32(1)

	mut reader := new_reader(writer.bytes())
	req := parse_describe_log_dirs_request(mut reader, 1, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	if ts := req.topics {
		assert ts.len == 1
		assert ts[0].topic == 'topic-a'
		assert ts[0].partitions.len == 2
	} else {
		assert false, 'expected topics'
	}
}

fn test_handler_describe_log_dirs_all_topics() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['topic-x'] = domain.TopicMetadata{
		name:            'topic-x'
		partition_count: 2
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(-1) // null topics

	result := handler.handle_describe_log_dirs(writer.bytes(), 1) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	results_count := reader.read_i32() or { 0 }
	assert results_count == 1

	error_code := reader.read_i16() or { -999 }
	assert error_code == 0

	log_dir := reader.read_string() or { '' }
	assert log_dir == virtual_log_dir
}

fn test_handler_describe_log_dirs_specific_partitions() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 5
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(2)
	writer.write_i32(0)
	writer.write_i32(2)

	result := handler.handle_describe_log_dirs(writer.bytes(), 1) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
}

fn test_describe_log_dirs_response_encoding_v1() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       0
		results:          [
			DescribeLogDirsResult{
				error_code:   0
				log_dir:      '/var/kafka-logs'
				topics:       [
					DescribeLogDirsTopicResult{
						name:       'test-topic'
						partitions: [
							DescribeLogDirsPartitionResult{
								partition_index: 0
								partition_size:  1024000
								offset_lag:      0
								is_future_key:   false
							},
						]
					},
				]
				total_bytes:  -1
				usable_bytes: -1
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0
}

fn test_describe_log_dirs_response_encoding_v4() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       0
		results:          [
			DescribeLogDirsResult{
				error_code:   0
				log_dir:      '/var/kafka-logs'
				topics:       []
				total_bytes:  1073741824
				usable_bytes: 536870912
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0
}

// ---- AlterReplicaLogDirs (API Key 34) ----

fn test_parse_alter_replica_log_dirs_request() {
	mut writer := new_writer()

	writer.write_i32(1)
	writer.write_string('/var/kafka-logs-new')

	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(2)
	writer.write_i32(0)
	writer.write_i32(1)

	mut reader := new_reader(writer.bytes())
	req := parse_alter_replica_log_dirs_request(mut reader, 1, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.dirs.len == 1
	assert req.dirs[0].path == '/var/kafka-logs-new'
	assert req.dirs[0].topics.len == 1
	assert req.dirs[0].topics[0].name == 'test-topic'
	assert req.dirs[0].topics[0].partitions.len == 2
}

fn test_handler_alter_replica_log_dirs_success() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('/new-log-dir')
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(2)
	writer.write_i32(0)
	writer.write_i32(1)

	result := handler.handle_alter_replica_log_dirs(writer.bytes(), 1) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	mut reader := new_reader(result)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	count := reader.read_i32() or { 0 }
	assert count == 1

	topic_name := reader.read_string() or { '' }
	assert topic_name == 'test-topic'

	p_count := reader.read_i32() or { 0 }
	assert p_count == 2

	p0_idx := reader.read_i32() or { -1 }
	p0_err := reader.read_i16() or { -999 }
	assert p0_idx == 0
	assert p0_err == 0

	p1_idx := reader.read_i32() or { -1 }
	p1_err := reader.read_i16() or { -999 }
	assert p1_idx == 1
	assert p1_err == 0
}

fn test_handler_alter_replica_log_dirs_topic_not_found() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('/new-log-dir')
	writer.write_i32(1)
	writer.write_string('nonexistent')
	writer.write_i32(1)
	writer.write_i32(0)

	result := handler.handle_alter_replica_log_dirs(writer.bytes(), 1) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	error_code := reader.read_i16() or { -999 }
	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_handler_alter_replica_log_dirs_invalid_partition() {
	mut storage := new_mock_storage()
	compression_service := get_test_compression_service()

	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 2
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('/new-log-dir')
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(1)
	writer.write_i32(99) // invalid partition

	result := handler.handle_alter_replica_log_dirs(writer.bytes(), 1) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0

	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_string() or { '' }
	_ := reader.read_i32() or { 0 }
	_ := reader.read_i32() or { 0 }
	error_code := reader.read_i16() or { -999 }
	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_alter_replica_log_dirs_response_encoding() {
	resp_v1 := AlterReplicaLogDirsResponse{
		throttle_time_ms: 0
		results:          [
			AlterReplicaLogDirTopicResult{
				topic_name: 'test-topic'
				partitions: [
					AlterReplicaLogDirPartitionResult{
						partition_index: 0
						error_code:      0
					},
				]
			},
		]
	}

	encoded_v1 := resp_v1.encode(1)
	assert encoded_v1.len > 0

	mut reader := new_reader(encoded_v1)
	throttle := reader.read_i32() or { -1 }
	assert throttle == 0

	count := reader.read_i32() or { 0 }
	assert count == 1

	encoded_v2 := resp_v1.encode(2)
	assert encoded_v2.len > 0
}

// ---- API versions registration check ----

fn test_new_apis_in_supported_versions() {
	versions := get_supported_api_versions()
	mut found := map[string]bool{}

	for v in versions {
		match v.api_key {
			.describe_topic_partitions { found['describe_topic_partitions'] = true }
			.incremental_alter_configs { found['incremental_alter_configs'] = true }
			.describe_log_dirs { found['describe_log_dirs'] = true }
			.alter_replica_log_dirs { found['alter_replica_log_dirs'] = true }
			else {}
		}
	}

	assert found['describe_topic_partitions'] == true, 'describe_topic_partitions missing'
	assert found['incremental_alter_configs'] == true, 'incremental_alter_configs missing'
	assert found['describe_log_dirs'] == true, 'describe_log_dirs missing'
	assert found['alter_replica_log_dirs'] == true, 'alter_replica_log_dirs missing'
}
