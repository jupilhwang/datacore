module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.auth as infra_auth
import service.port

// Helper: Create ACL handler
fn create_test_handler_with_acl() kafka.Handler {
	storage := AclMockStorage{}
	acl_manager := infra_auth.new_memory_acl_manager()
	compression_service := compression.new_default_compression_service() or {
		panic('failed to create compression service: ${err}')
	}

	// Use new_handler_full (9 args: broker_id, host, port, cluster_id, storage, auth_manager, acl_manager, txn_coordinator, compression_service)
	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage, none,
		acl_manager, none, compression_service)
}

struct AclMockStorage {}

fn (m AclMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m AclMockStorage) delete_topic(name string) ! {}

fn (m AclMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m AclMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m AclMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m AclMockStorage) add_partitions(name string, new_count int) ! {}

fn (m AclMockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (m AclMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m AclMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m AclMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m AclMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m AclMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m AclMockStorage) delete_group(group_id string) ! {}

fn (m AclMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m AclMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m AclMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m AclMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &AclMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &AclMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn test_handler_create_acls() {
	mut handler := create_test_handler_with_acl()

	// Build CreateAcls request
	mut request := kafka.new_writer()
	request.write_i32(0) // size
	request.write_i16(30) // api_key: CreateAcls
	request.write_i16(1) // version: 1 (supports pattern_type)
	request.write_i32(1) // correlation_id
	request.write_nullable_string('test-client')

	// Body: count(1) + creation
	request.write_array_len(1)
	request.write_i8(2) // resource_type: TOPIC
	request.write_string('test-topic')
	request.write_i8(3) // pattern_type: LITERAL
	request.write_string('User:alice')
	request.write_string('*')
	request.write_i8(3) // operation: READ
	request.write_i8(2) // permission_type: ALLOW

	response := handler.handle_request(request.bytes()[4..]) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()! // size
	_ = reader.read_i32()! // correlation_id

	// Body: throttle_time_ms + results array
	_ = reader.read_i32()! // throttle_time_ms
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 0
	error_message := reader.read_nullable_string()!
	assert error_message == ''
}

fn test_handler_describe_acls() {
	mut handler := create_test_handler_with_acl()

	// First create an ACL
	mut create_req := kafka.new_writer()
	create_req.write_i32(0)
	create_req.write_i16(30)
	create_req.write_i16(1) // version 1
	create_req.write_i32(1)
	create_req.write_nullable_string('test-client')
	create_req.write_array_len(1)
	create_req.write_i8(2) // TOPIC
	create_req.write_string('test-topic')
	create_req.write_i8(3) // LITERAL
	create_req.write_string('User:alice')
	create_req.write_string('*')
	create_req.write_i8(3) // READ
	create_req.write_i8(2) // ALLOW
	_ = handler.handle_request(create_req.bytes()[4..]) or { panic(err) }

	// Now describe ACLs
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(29) // api_key: DescribeAcls
	request.write_i16(1) // version 1
	request.write_i32(2) // correlation_id
	request.write_nullable_string('test-client')

	// Filter: match everything
	request.write_i8(1) // resource_type: ANY
	request.write_nullable_string(none) // resource_name: null
	request.write_i8(1) // pattern_type: ANY
	request.write_nullable_string(none) // principal: null
	request.write_nullable_string(none) // host: null
	request.write_i8(1) // operation: ANY
	request.write_i8(1) // permission_type: ANY

	response := handler.handle_request(request.bytes()[4..]) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!

	_ = reader.read_i32()! // throttle_time_ms
	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()! // error_message

	// Resources array
	res_count := reader.read_array_len()!
	assert res_count == 1

	res_type := reader.read_i8()!
	assert res_type == 2 // TOPIC
	res_name := reader.read_string()!
	assert res_name == 'test-topic'

	// Read pattern_type (v1+)
	pattern_type := reader.read_i8()!
	assert pattern_type == 3 // LITERAL

	// ACLs array
	acl_count := reader.read_array_len()!
	assert acl_count == 1

	principal := reader.read_string()!
	assert principal == 'User:alice'
	host := reader.read_string()!
	assert host == '*'
	op := reader.read_i8()!
	assert op == 3 // READ
	perm := reader.read_i8()!
	assert perm == 2 // ALLOW
}

fn test_handler_delete_acls() {
	mut handler := create_test_handler_with_acl()

	// Create ACL
	mut create_req := kafka.new_writer()
	create_req.write_i32(0)
	create_req.write_i16(30)
	create_req.write_i16(1) // version 1
	create_req.write_i32(1)
	create_req.write_nullable_string('test-client')
	create_req.write_array_len(1)
	create_req.write_i8(2) // TOPIC
	create_req.write_string('test-topic')
	create_req.write_i8(3) // LITERAL
	create_req.write_string('User:alice')
	create_req.write_string('*')
	create_req.write_i8(3) // READ
	create_req.write_i8(2) // ALLOW
	_ = handler.handle_request(create_req.bytes()[4..]) or { panic(err) }

	// Delete ACL
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(31) // api_key: DeleteAcls
	request.write_i16(1) // version 1
	request.write_i32(3) // correlation_id
	request.write_nullable_string('test-client')

	// Filter array
	request.write_array_len(1)
	request.write_i8(2) // TOPIC
	request.write_nullable_string('test-topic')
	request.write_i8(3) // LITERAL
	request.write_nullable_string(none)
	request.write_nullable_string(none)
	request.write_i8(1) // ANY
	request.write_i8(1) // ANY

	response := handler.handle_request(request.bytes()[4..]) or { panic(err) }

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!

	_ = reader.read_i32()! // throttle_time_ms

	// Results array
	count := reader.read_array_len()!
	assert count == 1

	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()!

	// Matching ACLs array
	match_count := reader.read_array_len()!
	assert match_count == 1

	match_error := reader.read_i16()!
	assert match_error == 0
	_ = reader.read_nullable_string()!

	res_type := reader.read_i8()!
	assert res_type == 2
	res_name := reader.read_string()!
	assert res_name == 'test-topic'

	// Read pattern_type (v1+)
	pattern_type := reader.read_i8()!
	assert pattern_type == 3 // LITERAL

	principal := reader.read_string()!
	assert principal == 'User:alice'
}
