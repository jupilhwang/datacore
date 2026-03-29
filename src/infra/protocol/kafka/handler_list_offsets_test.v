// ListOffsets 핸들러 단위 테스트
// process_list_offsets, parse_list_offsets_request, ListOffsetsResponse.encode 등
module kafka

import infra.compression
import service.port
import domain

// -- 테스트 전용 스토리지 (자체 완결) --

struct ListOffsetsTestStorage {
mut:
	topics map[string]domain.TopicMetadata
}

fn new_list_offsets_test_storage() &ListOffsetsTestStorage {
	mut s := &ListOffsetsTestStorage{
		topics: map[string]domain.TopicMetadata{}
	}
	s.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
		topic_id:        []u8{len: 16}
	}
	s.topics['multi-topic'] = domain.TopicMetadata{
		name:            'multi-topic'
		partition_count: 2
		topic_id:        []u8{len: 16}
	}
	return s
}

fn (mut s ListOffsetsTestStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		partition_count: partitions
		topic_id:        []u8{len: 16}
	}
	s.topics[name] = meta
	return meta
}

fn (mut s ListOffsetsTestStorage) delete_topic(name string) ! {
	s.topics.delete(name)
}

fn (mut s ListOffsetsTestStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (mut s ListOffsetsTestStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (s ListOffsetsTestStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s ListOffsetsTestStorage) add_partitions(name string, new_count int) ! {}

fn (s ListOffsetsTestStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (s ListOffsetsTestStorage) fetch(topic string, partition int, fetch_offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (s ListOffsetsTestStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (mut s ListOffsetsTestStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	if topic !in s.topics {
		return error('topic not found')
	}
	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: 10
		latest_offset:   100
		high_watermark:  100
	}
}

fn (s ListOffsetsTestStorage) save_group(group domain.ConsumerGroup) ! {}

fn (s ListOffsetsTestStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (s ListOffsetsTestStorage) delete_group(group_id string) ! {}

fn (s ListOffsetsTestStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (s ListOffsetsTestStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (s ListOffsetsTestStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s ListOffsetsTestStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &ListOffsetsTestStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s &ListOffsetsTestStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (s ListOffsetsTestStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s ListOffsetsTestStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s ListOffsetsTestStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s ListOffsetsTestStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn create_list_offsets_handler() Handler {
	storage := new_list_offsets_test_storage()
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	return new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
}

// -- process_list_offsets 기본 테스트 --

fn test_process_list_offsets_latest_offset() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.topics.len == 1
	assert resp.topics[0].name == 'test-topic'
	assert resp.topics[0].partitions.len == 1
	assert resp.topics[0].partitions[0].partition_index == 0
	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[0].partitions[0].offset == 100
	assert resp.topics[0].partitions[0].timestamp == -1
}

fn test_process_list_offsets_earliest_offset() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -2
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics.len == 1
	assert resp.topics[0].partitions.len == 1
	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[0].partitions[0].offset == 10
}

fn test_process_list_offsets_specific_timestamp() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       1000000
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics[0].partitions[0].error_code == 0
	// NOTE: 기존 코드에서 타임스탬프 기반 조회가 미구현되어 latest_offset을 반환
	assert resp.topics[0].partitions[0].offset == 100
}

// -- 존재하지 않는 토픽/파티션 --

fn test_process_list_offsets_unknown_topic() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'nonexistent-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics.len == 1
	assert resp.topics[0].name == 'nonexistent-topic'
	assert resp.topics[0].partitions.len == 1
	assert resp.topics[0].partitions[0].error_code == i16(ErrorCode.unknown_topic_or_partition)
	assert resp.topics[0].partitions[0].offset == -1
	assert resp.topics[0].partitions[0].timestamp == -1
}

// -- 여러 토픽/파티션 --

fn test_process_list_offsets_multiple_topics() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
					ListOffsetsRequestPartition{
						partition_index: 1
						timestamp:       -2
					},
				]
			},
			ListOffsetsRequestTopic{
				name:       'multi-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics.len == 2
	assert resp.topics[0].name == 'test-topic'
	assert resp.topics[0].partitions.len == 2
	assert resp.topics[0].partitions[0].offset == 100
	assert resp.topics[0].partitions[1].offset == 10
	assert resp.topics[1].name == 'multi-topic'
	assert resp.topics[1].partitions.len == 1
	assert resp.topics[1].partitions[0].offset == 100
}

fn test_process_list_offsets_mixed_existing_and_unknown() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          [
			ListOffsetsRequestTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
				]
			},
			ListOffsetsRequestTopic{
				name:       'no-such-topic'
				partitions: [
					ListOffsetsRequestPartition{
						partition_index: 0
						timestamp:       -1
					},
				]
			},
		]
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics.len == 2
	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[0].partitions[0].offset == 100
	assert resp.topics[1].partitions[0].error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// -- 빈 요청 --

fn test_process_list_offsets_empty_request() {
	mut handler := create_list_offsets_handler()

	req := ListOffsetsRequest{
		replica_id:      -1
		isolation_level: 0
		topics:          []
	}

	resp := handler.process_list_offsets(req, 1)!

	assert resp.topics.len == 0
	assert resp.throttle_time_ms == default_throttle_time_ms
}

// -- ListOffsetsResponse 인코딩 테스트 --

fn test_list_offsets_response_encode_v1() {
	resp := ListOffsetsResponse{
		throttle_time_ms: 0
		topics:           [
			ListOffsetsResponseTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsResponsePartition{
						partition_index: 0
						error_code:      0
						timestamp:       -1
						offset:          100
						leader_epoch:    0
					},
				]
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	topics_len := reader.read_i32()!
	assert topics_len == 1
	name := reader.read_string()!
	assert name == 'test-topic'
	parts_len := reader.read_i32()!
	assert parts_len == 1
	pi := reader.read_i32()!
	assert pi == 0
	ec := reader.read_i16()!
	assert ec == 0
	ts := reader.read_i64()!
	assert ts == -1
	off := reader.read_i64()!
	assert off == 100
}

fn test_list_offsets_response_encode_v2_with_throttle() {
	resp := ListOffsetsResponse{
		throttle_time_ms: 50
		topics:           [
			ListOffsetsResponseTopic{
				name:       'topic-1'
				partitions: [
					ListOffsetsResponsePartition{
						partition_index: 0
						error_code:      0
						timestamp:       -2
						offset:          0
						leader_epoch:    0
					},
				]
			},
		]
	}

	encoded := resp.encode(2)
	mut reader := new_reader(encoded)

	throttle := reader.read_i32()!
	assert throttle == 50
}

fn test_list_offsets_response_encode_v4_with_leader_epoch() {
	resp := ListOffsetsResponse{
		throttle_time_ms: 0
		topics:           [
			ListOffsetsResponseTopic{
				name:       'test-topic'
				partitions: [
					ListOffsetsResponsePartition{
						partition_index: 0
						error_code:      0
						timestamp:       -1
						offset:          200
						leader_epoch:    5
					},
				]
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ = reader.read_i32()! // throttle_time_ms
	_ = reader.read_i32()! // topics array len
	_ = reader.read_string()! // topic name
	_ = reader.read_i32()! // partitions array len
	_ = reader.read_i32()! // partition_index
	_ = reader.read_i16()! // error_code
	_ = reader.read_i64()! // timestamp
	_ = reader.read_i64()! // offset
	epoch := reader.read_i32()!
	assert epoch == 5
}

// -- handle_list_offsets 바이트 수준 테스트 --

fn test_handle_list_offsets_v1_bytes() {
	mut handler := create_list_offsets_handler()

	mut writer := new_writer()
	writer.write_i32(-1) // replica_id
	writer.write_i32(1) // topics array
	writer.write_string('test-topic')
	writer.write_i32(1) // partitions array
	writer.write_i32(0) // partition_index
	writer.write_i64(-1) // timestamp (latest)

	result := handler.handle_list_offsets(writer.bytes(), 1)!
	assert result.len > 0

	mut reader := new_reader(result)
	topics_len := reader.read_i32()!
	assert topics_len == 1
	topic_name := reader.read_string()!
	assert topic_name == 'test-topic'
	parts_len := reader.read_i32()!
	assert parts_len == 1
	pi := reader.read_i32()!
	assert pi == 0
	ec := reader.read_i16()!
	assert ec == 0
}

// -- 파싱 테스트 --

fn test_parse_list_offsets_request_v1() {
	mut writer := new_writer()
	writer.write_i32(-1)
	writer.write_i32(1) // topics array
	writer.write_string('my-topic')
	writer.write_i32(2) // partitions array
	writer.write_i32(0)
	writer.write_i64(-1)
	writer.write_i32(1)
	writer.write_i64(-2)

	mut reader := new_reader(writer.bytes())
	req := parse_list_offsets_request(mut reader, 1, false)!

	assert req.replica_id == -1
	assert req.isolation_level == 0
	assert req.topics.len == 1
	assert req.topics[0].name == 'my-topic'
	assert req.topics[0].partitions.len == 2
	assert req.topics[0].partitions[0].partition_index == 0
	assert req.topics[0].partitions[0].timestamp == -1
	assert req.topics[0].partitions[1].partition_index == 1
	assert req.topics[0].partitions[1].timestamp == -2
}

fn test_parse_list_offsets_request_v2_with_isolation_level() {
	mut writer := new_writer()
	writer.write_i32(-1)
	writer.write_i8(1) // isolation_level = READ_COMMITTED
	writer.write_i32(1)
	writer.write_string('test-topic')
	writer.write_i32(1)
	writer.write_i32(0)
	writer.write_i64(-1)

	mut reader := new_reader(writer.bytes())
	req := parse_list_offsets_request(mut reader, 2, false)!

	assert req.isolation_level == 1
	assert req.topics.len == 1
}
