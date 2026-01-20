// Unit tests for Kafka Admin API (CreateTopics, DeleteTopics)
module kafka

import domain
import service.port

// Mock Storage for testing
struct MockStorage {
mut:
	topics      map[string]domain.TopicMetadata
	call_count  int
	fail_create bool
	fail_delete bool
}

fn new_mock_storage() &MockStorage {
	return &MockStorage{
		topics: map[string]domain.TopicMetadata{}
	}
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

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
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

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (mut s MockStorage) delete_group(group_id string) ! {}

fn (mut s MockStorage) list_groups() ![]domain.GroupInfo {
	return [
		domain.GroupInfo{
			group_id:      'test-group-1'
			protocol_type: 'consumer'
			state:         'Stable'
		},
		domain.GroupInfo{
			group_id:      'test-group-2'
			protocol_type: 'consumer'
			state:         'Empty'
		},
	]
}

fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	if group_id == 'test-group-1' {
		return domain.ConsumerGroup{
			group_id:      group_id
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
	}
	return error('group not found')
}

fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (mut s MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

// Test CreateTopics Request Parsing
fn test_parse_create_topics_request() {
	// Build a minimal CreateTopics request (v0)
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

// Test DeleteTopics Request Parsing
fn test_parse_delete_topics_request() {
	// Build a minimal DeleteTopics request (v0)
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

// Test CreateTopicsResponse Encoding
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

	// Verify we can read back the basics
	mut reader := new_reader(encoded)

	// Array length
	array_len := reader.read_i32() or { 0 }
	assert array_len == 1
}

// Test DeleteTopicsResponse Encoding
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

	// Verify we can read back the basics
	mut reader := new_reader(encoded)

	// Array length
	array_len := reader.read_i32() or { 0 }
	assert array_len == 1
}

// Test Handler CreateTopics Success
fn test_handler_create_topics_success() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1) // 1 topic
	writer.write_string('new-topic')
	writer.write_i32(5) // 5 partitions
	writer.write_i16(1) // replication factor
	writer.write_i32(0) // no replica assignment
	writer.write_i32(0) // no configs
	writer.write_i32(30000) // timeout

	result := handler.handle_create_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
	assert storage.call_count == 1
	assert 'new-topic' in storage.topics
}

// Test Handler CreateTopics - Topic Already Exists
fn test_handler_create_topics_already_exists() {
	mut storage := new_mock_storage()

	// Pre-create topic
	storage.topics['existing-topic'] = domain.TopicMetadata{
		name:            'existing-topic'
		partition_count: 1
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

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

	// Parse response to check error code
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 } // array len
	_ := reader.read_string() or { '' } // topic name
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.topic_already_exists)
}

// Test Handler DeleteTopics Success
fn test_handler_delete_topics_success() {
	mut storage := new_mock_storage()

	// Pre-create topic
	storage.topics['to-delete'] = domain.TopicMetadata{
		name:            'to-delete'
		partition_count: 1
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1) // 1 topic
	writer.write_string('to-delete')
	writer.write_i32(30000) // timeout

	result := handler.handle_delete_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	assert result.len > 0
	assert 'to-delete' !in storage.topics
}

// Test Handler DeleteTopics - Topic Not Found
fn test_handler_delete_topics_not_found() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('nonexistent')
	writer.write_i32(30000)

	result := handler.handle_delete_topics(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response to check error code
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 } // array len
	_ := reader.read_string() or { '' } // topic name
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// Test Multiple Topics in Single Request
fn test_handler_create_multiple_topics() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request with 3 topics
	mut writer := new_writer()
	writer.write_i32(3) // 3 topics

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

// Test Config Parsing Helper
fn test_parse_config_i64() {
	configs := {
		'retention.ms':  '86400000'
		'segment.bytes': '1073741824'
	}

	assert parse_config_i64(configs, 'retention.ms', 0) == 86400000
	assert parse_config_i64(configs, 'segment.bytes', 0) == 1073741824
	assert parse_config_i64(configs, 'nonexistent', 999) == 999
}

fn test_parse_config_int() {
	configs := {
		'min.insync.replicas': '2'
		'max.message.bytes':   '1048576'
	}

	assert parse_config_int(configs, 'min.insync.replicas', 1) == 2
	assert parse_config_int(configs, 'max.message.bytes', 0) == 1048576
	assert parse_config_int(configs, 'nonexistent', 42) == 42
}

// Test ListGroups Request Parsing
fn test_parse_list_groups_request() {
	// Build a minimal ListGroups request (v0)
	mut writer := new_writer()
	// v0 has no fields

	mut reader := new_reader(writer.bytes())
	req := parse_list_groups_request(mut reader, 0, false) or {
		assert false, 'parse failed: ${err}'
		return
	}

	assert req.states_filter.len == 0
}

// Test ListGroups Handler
fn test_handler_list_groups() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

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

// Test DescribeGroups Request Parsing
fn test_parse_describe_groups_request() {
	// Build a DescribeGroups request (v0)
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

// Test DescribeGroups Handler - Group Found
fn test_handler_describe_groups_found() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request for existing group
	mut writer := new_writer()
	writer.write_i32(1) // 1 group
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

// Test DescribeGroups Handler - Group Not Found
fn test_handler_describe_groups_not_found() {
	mut storage := new_mock_storage()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

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

// Test ListGroupsResponse Encoding
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

// Test DescribeGroupsResponse Encoding
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

// ============================================================================
// AlterConfigs API Tests
// ============================================================================

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

	// Pre-create topic
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1) // 1 resource
	writer.write_i8(2) // TOPIC
	writer.write_string('test-topic')
	writer.write_i32(1) // 1 config
	writer.write_string('retention.ms')
	writer.write_string('86400000')
	writer.write_i8(0) // validate_only = false

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
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request for non-existent topic
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_i8(2) // TOPIC
	writer.write_string('nonexistent')
	writer.write_i32(0) // no configs
	writer.write_i8(0)

	result := handler.handle_alter_configs(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 } // throttle
	_ := reader.read_i32() or { 0 } // results len
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// ============================================================================
// CreatePartitions API Tests
// ============================================================================

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

	// Pre-create topic with 3 partitions
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request to increase to 10 partitions
	mut writer := new_writer()
	writer.write_i32(1) // 1 topic
	writer.write_string('test-topic')
	writer.write_i32(10) // new count
	writer.write_i32(-1) // no assignments
	writer.write_i32(30000) // timeout
	writer.write_i8(0) // validate_only = false

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
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

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
	_ := reader.read_i32() or { 0 } // throttle
	_ := reader.read_i32() or { 0 } // results len
	_ := reader.read_string() or { '' } // name
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_handler_create_partitions_invalid_count() {
	mut storage := new_mock_storage()

	// Pre-create topic with 10 partitions
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 10
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request to decrease to 5 partitions (invalid)
	mut writer := new_writer()
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(5) // less than current (invalid)
	writer.write_i32(-1)
	writer.write_i32(30000)
	writer.write_i8(0)

	result := handler.handle_create_partitions(writer.bytes(), 0) or {
		assert false, 'handler failed: ${err}'
		return
	}

	// Parse response
	mut reader := new_reader(result)
	_ := reader.read_i32() or { 0 } // throttle
	_ := reader.read_i32() or { 0 } // results len
	_ := reader.read_string() or { '' } // name
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.invalid_partitions)
}

// ============================================================================
// DeleteRecords API Tests
// ============================================================================

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

	// Pre-create topic
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

	// Build request
	mut writer := new_writer()
	writer.write_i32(1) // 1 topic
	writer.write_string('test-topic')
	writer.write_i32(1) // 1 partition
	writer.write_i32(0) // partition index
	writer.write_i64(50) // delete before offset 50
	writer.write_i32(30000) // timeout

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
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)

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
	_ := reader.read_i32() or { 0 } // throttle
	_ := reader.read_i32() or { 0 } // topics len
	_ := reader.read_string() or { '' } // name
	_ := reader.read_i32() or { 0 } // partitions len
	_ := reader.read_i32() or { 0 } // partition index
	_ := reader.read_i64() or { 0 } // low watermark
	error_code := reader.read_i16() or { -999 }

	assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// ============================================================================
// Response Encoding Tests
// ============================================================================

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
