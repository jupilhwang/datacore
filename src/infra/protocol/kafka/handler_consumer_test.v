// Unit tests - Consumer handler 테스트
// ConsumerGroupHeartbeat (KIP-848) 핸들러 동작을 검증한다.
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import service.port

// -- 핸들러 생성 헬퍼 --

fn create_consumer_handler() kafka.Handler {
	storage := ConsumerHandlerMockStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, kafka.new_compression_port_adapter(cs))
}

fn create_consumer_handler_with_topics() kafka.Handler {
	storage := ConsumerHandlerMockStorageWithTopics{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, kafka.new_compression_port_adapter(cs))
}

// ConsumerGroupHeartbeat 요청 빌더 (API Key 68, v0, always flexible)
fn build_consumer_group_heartbeat_request(correlation_id i32, group_id string, member_id string, member_epoch i32, subscribed_topics []string) []u8 {
	mut w := kafka.new_writer()
	w.write_i32(0) // size placeholder
	w.write_i16(68) // api_key = ConsumerGroupHeartbeat
	w.write_i16(0) // version 0
	w.write_i32(correlation_id)
	w.write_nullable_string('test-client')
	w.write_tagged_fields() // flexible header tagged fields

	// Body (always flexible v0+)
	w.write_compact_string(group_id)
	w.write_compact_string(member_id)
	w.write_i32(member_epoch)
	w.write_compact_nullable_string(none) // instance_id
	w.write_compact_nullable_string(none) // rack_id
	w.write_i32(300000) // rebalance_timeout_ms

	// subscribed_topic_names
	w.write_compact_array_len(subscribed_topics.len)
	for t in subscribed_topics {
		w.write_compact_string(t)
	}

	w.write_compact_nullable_string(none) // server_assignor

	// topic_partitions (empty for new member)
	w.write_compact_array_len(0)

	w.write_tagged_fields() // body tagged fields

	return w.bytes()[4..]
}

// -- ConsumerGroupHeartbeat 핸들러 테스트 --

fn test_consumer_group_heartbeat_new_member() {
	// epoch=0, member_id 비어있음 -> 새 멤버 등록
	mut handler := create_consumer_handler()
	req := build_consumer_group_heartbeat_request(1, 'test-group', '', 0, ['topic-1'])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	cid := reader.read_i32()! // correlation_id
	assert cid == 1
	_ := reader.read_uvarint()! // flexible header tagged fields

	// Body
	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0

	// error_message
	error_msg := reader.read_compact_nullable_string()!
	assert error_msg == ''

	// member_id: 새로 생성된 ID가 있어야 한다
	member_id := reader.read_compact_nullable_string()!
	assert member_id.len > 0
	assert member_id.starts_with('member-')

	// member_epoch: 1이어야 한다 (새 멤버)
	member_epoch := reader.read_i32()!
	assert member_epoch == 1

	// heartbeat_interval_ms
	heartbeat_ms := reader.read_i32()!
	assert heartbeat_ms == 3000
}

fn test_consumer_group_heartbeat_empty_group_id() {
	// 빈 group_id -> INVALID_GROUP_ID
	mut handler := create_consumer_handler()
	req := build_consumer_group_heartbeat_request(2, '', '', 0, [])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 24 // INVALID_GROUP_ID
}

fn test_consumer_group_heartbeat_leave_group() {
	// epoch=-1 -> 그룹 탈퇴 요청
	mut handler := create_consumer_handler()
	req := build_consumer_group_heartbeat_request(3, 'leave-group', 'member-1-12345',
		-1, [])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_compact_nullable_string()! // error_message
	_ := reader.read_compact_nullable_string()! // member_id

	member_epoch := reader.read_i32()!
	assert member_epoch == -1 // 탈퇴 시 epoch은 -1
}

fn test_consumer_group_heartbeat_regular_heartbeat() {
	// epoch > 0 -> 일반 하트비트 (상태 변경 없음)
	mut handler := create_consumer_handler()
	req := build_consumer_group_heartbeat_request(4, 'hb-group', 'member-1-99', 5, [])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_compact_nullable_string()! // error_message
	_ := reader.read_compact_nullable_string()! // member_id

	member_epoch := reader.read_i32()!
	assert member_epoch == 5 // 하트비트 시 epoch은 변경되지 않는다
}

fn test_consumer_group_heartbeat_new_member_with_topic_assignment() {
	// 토픽이 존재할 때 새 멤버 등록 시 파티션 할당이 이루어져야 한다
	mut handler := create_consumer_handler_with_topics()
	req := build_consumer_group_heartbeat_request(5, 'assign-group', '', 0, [
		'test-topic',
	])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_compact_nullable_string()! // error_message

	member_id := reader.read_compact_nullable_string()!
	assert member_id.len > 0

	member_epoch := reader.read_i32()!
	assert member_epoch == 1

	heartbeat_ms := reader.read_i32()!
	assert heartbeat_ms == 3000

	// assignment가 존재해야 한다 (topic_partitions compact array)
	tp_count_raw := reader.read_uvarint()!
	tp_count := int(tp_count_raw) - 1
	assert tp_count == 1 // test-topic 1개
}

fn test_consumer_group_heartbeat_new_member_nonexistent_topic() {
	// 존재하지 않는 토픽을 구독하면 할당이 비어있어야 한다
	mut handler := create_consumer_handler()
	req := build_consumer_group_heartbeat_request(6, 'no-topic-group', '', 0, [
		'nonexistent',
	])

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!
	_ := reader.read_uvarint()!

	_ := reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_compact_nullable_string()! // error_message

	member_id := reader.read_compact_nullable_string()!
	assert member_id.len > 0

	member_epoch := reader.read_i32()!
	assert member_epoch == 1

	_ := reader.read_i32()! // heartbeat_interval_ms

	// 존재하지 않는 토픽이므로 assignment는 null(0)이어야 한다
	assignment_indicator := reader.read_uvarint()!
	assert assignment_indicator == 0
}

// -- ConsumerGroupHeartbeatResponse 인코딩 테스트 --

fn test_consumer_group_heartbeat_response_encode_no_assignment() {
	resp := kafka.ConsumerGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            0
		error_message:         none
		member_id:             'member-1'
		member_epoch:          1
		heartbeat_interval_ms: 3000
		assignment:            none
	}

	bytes := resp.encode(0)
	mut reader := kafka.new_reader(bytes)

	throttle := reader.read_i32()!
	assert throttle == 0

	ec := reader.read_i16()!
	assert ec == 0

	_ := reader.read_compact_nullable_string()! // error_message

	mid := reader.read_compact_nullable_string()!
	assert mid == 'member-1'

	epoch := reader.read_i32()!
	assert epoch == 1

	hb := reader.read_i32()!
	assert hb == 3000
}

fn test_consumer_group_heartbeat_response_encode_with_error() {
	resp := kafka.ConsumerGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            i16(kafka.ErrorCode.invalid_group_id)
		error_message:         'Group ID cannot be empty'
		member_id:             none
		member_epoch:          -1
		heartbeat_interval_ms: 0
		assignment:            none
	}

	bytes := resp.encode(0)
	mut reader := kafka.new_reader(bytes)

	_ := reader.read_i32()! // throttle
	ec := reader.read_i16()!
	assert ec == 24 // INVALID_GROUP_ID

	error_msg := reader.read_compact_nullable_string()!
	assert error_msg == 'Group ID cannot be empty'
}

// -- ConsumerHandlerMockStorage: get_topic이 항상 실패하는 버전 --

struct ConsumerHandlerMockStorage {}

fn (m ConsumerHandlerMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m ConsumerHandlerMockStorage) delete_topic(name string) ! {}

fn (m ConsumerHandlerMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m ConsumerHandlerMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m ConsumerHandlerMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m ConsumerHandlerMockStorage) add_partitions(name string, new_count int) ! {}

fn (m ConsumerHandlerMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m ConsumerHandlerMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m ConsumerHandlerMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m ConsumerHandlerMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m ConsumerHandlerMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m ConsumerHandlerMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m ConsumerHandlerMockStorage) delete_group(group_id string) ! {}

fn (m ConsumerHandlerMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m ConsumerHandlerMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m ConsumerHandlerMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m ConsumerHandlerMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &ConsumerHandlerMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &ConsumerHandlerMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m ConsumerHandlerMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m ConsumerHandlerMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m ConsumerHandlerMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m ConsumerHandlerMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

// -- ConsumerHandlerMockStorageWithTopics: 토픽이 존재하는 버전 --

struct ConsumerHandlerMockStorageWithTopics {}

fn (m ConsumerHandlerMockStorageWithTopics) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m ConsumerHandlerMockStorageWithTopics) delete_topic(name string) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m ConsumerHandlerMockStorageWithTopics) get_topic(name string) !domain.TopicMetadata {
	if name == 'test-topic' {
		return domain.TopicMetadata{
			name:            'test-topic'
			partition_count: 3
			topic_id:        [u8(0), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
		}
	}
	return error('topic not found')
}

fn (m ConsumerHandlerMockStorageWithTopics) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m ConsumerHandlerMockStorageWithTopics) add_partitions(name string, new_count int) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m ConsumerHandlerMockStorageWithTopics) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m ConsumerHandlerMockStorageWithTopics) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m ConsumerHandlerMockStorageWithTopics) save_group(group domain.ConsumerGroup) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m ConsumerHandlerMockStorageWithTopics) delete_group(group_id string) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m ConsumerHandlerMockStorageWithTopics) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m ConsumerHandlerMockStorageWithTopics) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &ConsumerHandlerMockStorageWithTopics) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &ConsumerHandlerMockStorageWithTopics) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m ConsumerHandlerMockStorageWithTopics) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m ConsumerHandlerMockStorageWithTopics) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m ConsumerHandlerMockStorageWithTopics) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
