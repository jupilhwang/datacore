// FindCoordinator 핸들러 단위 테스트
// process_find_coordinator, compute_coordinator_broker, coordinator_key_type_str 등
module kafka

import infra.compression
import service.port
import domain

// -- 테스트 전용 스토리지 (자체 완결) --

struct FindCoordTestStorage {}

fn (s FindCoordTestStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{
		name:            name
		partition_count: partitions
		topic_id:        []u8{len: 16}
	}
}

fn (s FindCoordTestStorage) delete_topic(name string) ! {}

fn (s FindCoordTestStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (s FindCoordTestStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s FindCoordTestStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (s FindCoordTestStorage) add_partitions(name string, new_count int) ! {}

fn (s FindCoordTestStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (s FindCoordTestStorage) fetch(topic string, partition int, fetch_offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (s FindCoordTestStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (s FindCoordTestStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return error('topic not found')
}

fn (s FindCoordTestStorage) save_group(group domain.ConsumerGroup) ! {}

fn (s FindCoordTestStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (s FindCoordTestStorage) delete_group(group_id string) ! {}

fn (s FindCoordTestStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (s FindCoordTestStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (s FindCoordTestStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s FindCoordTestStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &FindCoordTestStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s &FindCoordTestStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (s FindCoordTestStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s FindCoordTestStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s FindCoordTestStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s FindCoordTestStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

fn create_find_coordinator_handler() Handler {
	storage := FindCoordTestStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
	return new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
}

// -- coordinator_key_type_str 테스트 --

fn test_coordinator_key_type_str_group() {
	assert coordinator_key_type_str(0) == 'GROUP'
}

fn test_coordinator_key_type_str_transaction() {
	assert coordinator_key_type_str(1) == 'TRANSACTION'
}

fn test_coordinator_key_type_str_share() {
	assert coordinator_key_type_str(2) == 'SHARE'
}

fn test_coordinator_key_type_str_unknown() {
	assert coordinator_key_type_str(99) == 'UNKNOWN'
}

// -- process_find_coordinator v0-v3 (단일 키 응답) --

fn test_process_find_coordinator_v0_single_key() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              'test-group'
		key_type:         0
		coordinator_keys: []
	}

	resp := handler.process_find_coordinator(req, 0)!

	// 단일 브로커 모드에서는 자기 자신을 반환
	assert resp.error_code == 0
	assert resp.node_id == 1
	assert resp.host == 'localhost'
	assert resp.port == 9092
	assert resp.coordinators.len == 0
}

fn test_process_find_coordinator_v1_with_group_type() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              'consumer-group-1'
		key_type:         i8(CoordinatorKeyType.group)
		coordinator_keys: []
	}

	resp := handler.process_find_coordinator(req, 1)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.error_code == 0
	assert resp.node_id == 1
	assert resp.host == 'localhost'
	assert resp.port == 9092
}

fn test_process_find_coordinator_v1_with_transaction_type() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              'txn-id-1'
		key_type:         i8(CoordinatorKeyType.transaction)
		coordinator_keys: []
	}

	resp := handler.process_find_coordinator(req, 1)!

	assert resp.error_code == 0
	assert resp.node_id == 1
}

fn test_process_find_coordinator_v3_single_key() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              'group-v3'
		key_type:         i8(CoordinatorKeyType.group)
		coordinator_keys: []
	}

	resp := handler.process_find_coordinator(req, 3)!

	assert resp.error_code == 0
	assert resp.node_id == 1
	assert resp.host == 'localhost'
	assert resp.port == 9092
	assert resp.coordinators.len == 0
}

// -- process_find_coordinator v4+ (배치 응답) --

fn test_process_find_coordinator_v4_batch_keys() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.group)
		coordinator_keys: ['group-a', 'group-b', 'group-c']
	}

	resp := handler.process_find_coordinator(req, 4)!

	assert resp.coordinators.len == 3
	for node in resp.coordinators {
		assert node.error_code == 0
		assert node.node_id == 1
		assert node.host == 'localhost'
		assert node.port == 9092
	}
	assert resp.coordinators[0].key == 'group-a'
	assert resp.coordinators[1].key == 'group-b'
	assert resp.coordinators[2].key == 'group-c'
}

fn test_process_find_coordinator_v4_empty_keys_fallback_to_key() {
	mut handler := create_find_coordinator_handler()
	// coordinator_keys가 비어있으면 key 필드를 사용
	req := FindCoordinatorRequest{
		key:              'fallback-group'
		key_type:         i8(CoordinatorKeyType.group)
		coordinator_keys: []
	}

	resp := handler.process_find_coordinator(req, 4)!

	assert resp.coordinators.len == 1
	assert resp.coordinators[0].key == 'fallback-group'
	assert resp.coordinators[0].error_code == 0
	assert resp.coordinators[0].node_id == 1
}

fn test_process_find_coordinator_v4_single_batch_key() {
	mut handler := create_find_coordinator_handler()
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.transaction)
		coordinator_keys: ['txn-1']
	}

	resp := handler.process_find_coordinator(req, 4)!

	assert resp.coordinators.len == 1
	assert resp.coordinators[0].key == 'txn-1'
	assert resp.coordinators[0].error_code == 0
}

// -- v6 Share Group 키 형식 검증 --

fn test_process_find_coordinator_v6_share_group_valid_key() {
	mut handler := create_find_coordinator_handler()
	// Share Group 키 형식: "groupId:topicId:partition"
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.share)
		coordinator_keys: ['my-group:my-topic:0']
	}

	resp := handler.process_find_coordinator(req, 6)!

	assert resp.coordinators.len == 1
	assert resp.coordinators[0].error_code == 0
	assert resp.coordinators[0].node_id == 1
}

fn test_process_find_coordinator_v6_share_group_invalid_key() {
	mut handler := create_find_coordinator_handler()
	// 잘못된 Share Group 키 형식 (콜론 2개 필요하지만 1개만 존재)
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.share)
		coordinator_keys: ['invalid-format']
	}

	resp := handler.process_find_coordinator(req, 6)!

	assert resp.coordinators.len == 1
	assert resp.coordinators[0].error_code == i16(ErrorCode.invalid_request)
	assert resp.coordinators[0].node_id == -1
	assert resp.coordinators[0].host == ''
	assert resp.coordinators[0].port == 0
}

fn test_process_find_coordinator_v6_share_group_mixed_keys() {
	mut handler := create_find_coordinator_handler()
	// 유효한 키와 잘못된 키가 혼합된 경우
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.share)
		coordinator_keys: ['valid:topic:0', 'bad-key', 'also:valid:1']
	}

	resp := handler.process_find_coordinator(req, 6)!

	assert resp.coordinators.len == 3
	assert resp.coordinators[0].error_code == 0
	assert resp.coordinators[0].key == 'valid:topic:0'
	assert resp.coordinators[1].error_code == i16(ErrorCode.invalid_request)
	assert resp.coordinators[1].key == 'bad-key'
	assert resp.coordinators[2].error_code == 0
	assert resp.coordinators[2].key == 'also:valid:1'
}

fn test_process_find_coordinator_v5_share_key_not_validated() {
	mut handler := create_find_coordinator_handler()
	// v5에서는 Share Group 키 형식 검증을 하지 않음 (v6부터 적용)
	req := FindCoordinatorRequest{
		key:              ''
		key_type:         i8(CoordinatorKeyType.share)
		coordinator_keys: ['no-colons-here']
	}

	resp := handler.process_find_coordinator(req, 5)!

	assert resp.coordinators.len == 1
	assert resp.coordinators[0].error_code == 0
}

// -- compute_coordinator_broker 테스트 --

fn test_compute_coordinator_broker_single_broker_mode() {
	mut handler := create_find_coordinator_handler()
	node_id, host, broker_port := handler.compute_coordinator_broker('any-key', 0)

	assert node_id == 1
	assert host == 'localhost'
	assert broker_port == 9092
}

fn test_compute_coordinator_broker_empty_key() {
	mut handler := create_find_coordinator_handler()
	node_id, host, broker_port := handler.compute_coordinator_broker('', 0)

	assert node_id == 1
	assert host == 'localhost'
	assert broker_port == 9092
}

// -- FindCoordinatorResponse 인코딩 테스트 --

fn test_find_coordinator_response_encode_v0() {
	resp := FindCoordinatorResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    none
		node_id:          1
		host:             'localhost'
		port:             9092
		coordinators:     []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	error_code := reader.read_i16()!
	assert error_code == 0
	node_id := reader.read_i32()!
	assert node_id == 1
	host := reader.read_string()!
	assert host == 'localhost'
	broker_port := reader.read_i32()!
	assert broker_port == 9092
}

fn test_find_coordinator_response_encode_v1() {
	resp := FindCoordinatorResponse{
		throttle_time_ms: 100
		error_code:       0
		error_message:    'test message'
		node_id:          2
		host:             '192.168.1.1'
		port:             9093
		coordinators:     []
	}

	encoded := resp.encode(1)
	mut reader := new_reader(encoded)

	throttle := reader.read_i32()!
	assert throttle == 100
	error_code := reader.read_i16()!
	assert error_code == 0
	error_msg := reader.read_nullable_string()!
	assert error_msg == 'test message'
	node_id := reader.read_i32()!
	assert node_id == 2
}

fn test_find_coordinator_response_encode_v4_batch() {
	resp := FindCoordinatorResponse{
		throttle_time_ms: 0
		coordinators:     [
			FindCoordinatorResponseNode{
				key:           'group-1'
				node_id:       1
				host:          'host-1'
				port:          9092
				error_code:    0
				error_message: none
			},
			FindCoordinatorResponseNode{
				key:           'group-2'
				node_id:       2
				host:          'host-2'
				port:          9093
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	throttle := reader.read_i32()!
	assert throttle == 0
	count := reader.read_compact_array_len()!
	assert count == 2
}

// -- 파싱 라운드트립 테스트 --

fn test_parse_find_coordinator_request_v0() {
	mut writer := new_writer()
	writer.write_string('my-group')

	mut reader := new_reader(writer.bytes())
	req := parse_find_coordinator_request(mut reader, 0, false)!

	assert req.key == 'my-group'
	assert req.key_type == 0
	assert req.coordinator_keys.len == 0
}

fn test_parse_find_coordinator_request_v1() {
	mut writer := new_writer()
	writer.write_string('my-group')
	writer.write_i8(1) // key_type = TRANSACTION

	mut reader := new_reader(writer.bytes())
	req := parse_find_coordinator_request(mut reader, 1, false)!

	assert req.key == 'my-group'
	assert req.key_type == 1
}

fn test_parse_find_coordinator_request_v4_flexible() {
	mut writer := new_writer()
	// v4 flexible: key_type + coordinator_keys array
	writer.write_i8(0) // key_type = GROUP
	// compact array len (2 keys)
	writer.write_compact_array_len(2)
	writer.write_compact_string('key-a')
	writer.write_compact_string('key-b')
	writer.write_tagged_fields()

	mut reader := new_reader(writer.bytes())
	req := parse_find_coordinator_request(mut reader, 4, true)!

	assert req.key_type == 0
	assert req.coordinator_keys.len == 2
	assert req.coordinator_keys[0] == 'key-a'
	assert req.coordinator_keys[1] == 'key-b'
}
