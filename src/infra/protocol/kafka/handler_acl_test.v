// Unit tests - ACL handler 테스트
// DescribeAcls, CreateAcls, DeleteAcls 핸들러의 동작을 검증한다.
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.auth as infra_auth
import service.port

// -- 핸들러 생성 헬퍼 --

fn create_acl_handler_with_manager() kafka.Handler {
	storage := AclHandlerMockStorage{}
	acl_manager := infra_auth.new_memory_acl_manager()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none,
		acl_manager, none, kafka.new_compression_port_adapter(cs))
}

fn create_acl_handler_without_manager() kafka.Handler {
	storage := AclHandlerMockStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, kafka.new_compression_port_adapter(cs))
}

// -- DescribeAcls 핸들러 테스트 --

fn test_describe_acls_handler_security_disabled() {
	// ACL manager가 없으면 SECURITY_DISABLED를 반환해야 한다
	mut handler := create_acl_handler_without_manager()

	// v1 (non-flexible) DescribeAcls 요청 구성
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(29) // DescribeAcls
	request.write_i16(1) // version 1
	request.write_i32(1) // correlation_id
	request.write_nullable_string('test-client')

	// Filter: ANY/ANY
	request.write_i8(1) // resource_type = ANY
	request.write_nullable_string(none) // resource_name
	request.write_i8(1) // pattern_type = ANY
	request.write_nullable_string(none) // principal
	request.write_nullable_string(none) // host
	request.write_i8(1) // operation = ANY
	request.write_i8(1) // permission_type = ANY

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	_ := reader.read_i32()! // correlation_id

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 54 // SECURITY_DISABLED
}

fn test_describe_acls_handler_empty_result() {
	mut handler := create_acl_handler_with_manager()

	// ACL이 없는 상태에서 조회
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(29)
	request.write_i16(1)
	request.write_i32(2)
	request.write_nullable_string('test-client')

	request.write_i8(1) // ANY
	request.write_nullable_string(none)
	request.write_i8(1) // ANY
	request.write_nullable_string(none)
	request.write_nullable_string(none)
	request.write_i8(1) // ANY
	request.write_i8(1) // ANY

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0
	_ := reader.read_nullable_string()!

	resources := reader.read_array_len()!
	assert resources == 0
}

// -- CreateAcls 핸들러 테스트 --

fn test_create_acls_handler_success() {
	mut handler := create_acl_handler_with_manager()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(30) // CreateAcls
	request.write_i16(1)
	request.write_i32(3)
	request.write_nullable_string('test-client')

	// 1개의 ACL 생성
	request.write_array_len(1)
	request.write_i8(2) // resource_type = TOPIC
	request.write_string('my-topic')
	request.write_i8(3) // pattern_type = LITERAL
	request.write_string('User:bob')
	request.write_string('*')
	request.write_i8(2) // operation = WRITE
	request.write_i8(2) // permission_type = ALLOW

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()! // throttle_time_ms
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 0
}

fn test_create_acls_handler_security_disabled() {
	mut handler := create_acl_handler_without_manager()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(30)
	request.write_i16(1)
	request.write_i32(4)
	request.write_nullable_string('test-client')

	request.write_array_len(1)
	request.write_i8(2)
	request.write_string('topic-a')
	request.write_i8(3)
	request.write_string('User:alice')
	request.write_string('*')
	request.write_i8(3) // READ
	request.write_i8(2) // ALLOW

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 54 // SECURITY_DISABLED
}

fn test_create_acls_handler_multiple_entries() {
	mut handler := create_acl_handler_with_manager()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(30)
	request.write_i16(1)
	request.write_i32(5)
	request.write_nullable_string('test-client')

	// 2개의 ACL 생성
	request.write_array_len(2)

	// ACL 1
	request.write_i8(2) // TOPIC
	request.write_string('topic-a')
	request.write_i8(3) // LITERAL
	request.write_string('User:alice')
	request.write_string('*')
	request.write_i8(3) // READ
	request.write_i8(2) // ALLOW

	// ACL 2
	request.write_i8(2) // TOPIC
	request.write_string('topic-b')
	request.write_i8(3) // LITERAL
	request.write_string('User:bob')
	request.write_string('192.168.1.1')
	request.write_i8(2) // WRITE
	request.write_i8(2) // ALLOW

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 2

	// 두 결과 모두 성공이어야 한다
	e1 := reader.read_i16()!
	assert e1 == 0
	_ := reader.read_nullable_string()!

	e2 := reader.read_i16()!
	assert e2 == 0
}

// -- DeleteAcls 핸들러 테스트 --

fn test_delete_acls_handler_security_disabled() {
	mut handler := create_acl_handler_without_manager()

	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(31) // DeleteAcls
	request.write_i16(1)
	request.write_i32(6)
	request.write_nullable_string('test-client')

	request.write_array_len(1)
	request.write_i8(1) // ANY
	request.write_nullable_string(none)
	request.write_i8(1) // ANY
	request.write_nullable_string(none)
	request.write_nullable_string(none)
	request.write_i8(1)
	request.write_i8(1)

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 54 // SECURITY_DISABLED
}

fn test_delete_acls_handler_create_then_delete() {
	mut handler := create_acl_handler_with_manager()

	// 1. ACL 생성
	mut create_req := kafka.new_writer()
	create_req.write_i32(0)
	create_req.write_i16(30)
	create_req.write_i16(1)
	create_req.write_i32(7)
	create_req.write_nullable_string('test-client')

	create_req.write_array_len(1)
	create_req.write_i8(2) // TOPIC
	create_req.write_string('del-topic')
	create_req.write_i8(3) // LITERAL
	create_req.write_string('User:charlie')
	create_req.write_string('*')
	create_req.write_i8(3) // READ
	create_req.write_i8(2) // ALLOW

	mut conn := ?&domain.AuthConnection(none)
	_ := handler.handle_request(create_req.bytes()[4..], mut conn) or {
		panic('생성 요청 실패: ${err}')
	}

	// 2. ACL 삭제
	mut del_req := kafka.new_writer()
	del_req.write_i32(0)
	del_req.write_i16(31)
	del_req.write_i16(1)
	del_req.write_i32(8)
	del_req.write_nullable_string('test-client')

	del_req.write_array_len(1)
	del_req.write_i8(2) // TOPIC
	del_req.write_nullable_string('del-topic')
	del_req.write_i8(3) // LITERAL
	del_req.write_nullable_string(none)
	del_req.write_nullable_string(none)
	del_req.write_i8(1) // ANY
	del_req.write_i8(1) // ANY

	response := handler.handle_request(del_req.bytes()[4..], mut conn) or {
		panic('삭제 요청 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	_ := reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 0
	_ := reader.read_nullable_string()!

	// 삭제된 ACL 수 확인
	match_count := reader.read_array_len()!
	assert match_count == 1
}

// -- AclHandlerMockStorage --

struct AclHandlerMockStorage {}

fn (m AclHandlerMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m AclHandlerMockStorage) delete_topic(name string) ! {}

fn (m AclHandlerMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m AclHandlerMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m AclHandlerMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m AclHandlerMockStorage) add_partitions(name string, new_count int) ! {}

fn (m AclHandlerMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m AclHandlerMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m AclHandlerMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m AclHandlerMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m AclHandlerMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m AclHandlerMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m AclHandlerMockStorage) delete_group(group_id string) ! {}

fn (m AclHandlerMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m AclHandlerMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m AclHandlerMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m AclHandlerMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &AclHandlerMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &AclHandlerMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m AclHandlerMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m AclHandlerMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m AclHandlerMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m AclHandlerMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
