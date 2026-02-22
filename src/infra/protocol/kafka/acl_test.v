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

fn (m AclMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
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

fn (m AclMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m AclMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m AclMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m AclMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []domain.SharePartitionState{}
}

fn test_handler_create_acls() {
	mut handler := create_test_handler_with_acl()

	// Build CreateAcls request
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(30)
	request.write_i16(1)
	request.write_i32(1)
	request.write_nullable_string('test-client')

	// Body: count(1) + creation
	request.write_array_len(1)
	request.write_i8(2)
	request.write_string('test-topic')
	request.write_i8(3)
	request.write_string('User:alice')
	request.write_string('*')
	request.write_i8(3)
	request.write_i8(2)

	response := handler.handle_request(request.bytes()[4..], mut ?&domain.AuthConnection(none)) or {
		panic(err)
	}

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!

	// Body: throttle_time_ms + results array
	_ = reader.read_i32()!
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
	create_req.write_i16(1)
	create_req.write_i32(1)
	create_req.write_nullable_string('test-client')
	create_req.write_array_len(1)
	create_req.write_i8(2)
	create_req.write_string('test-topic')
	create_req.write_i8(3)
	create_req.write_string('User:alice')
	create_req.write_string('*')
	create_req.write_i8(3)
	create_req.write_i8(2)
	_ = handler.handle_request(create_req.bytes()[4..], mut ?&domain.AuthConnection(none)) or {
		panic(err)
	}

	// Now describe ACLs
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(29)
	request.write_i16(1)
	request.write_i32(2)
	request.write_nullable_string('test-client')

	// Filter: match everything
	request.write_i8(1)
	request.write_nullable_string(none)
	request.write_i8(1)
	request.write_nullable_string(none)
	request.write_nullable_string(none)
	request.write_i8(1)
	request.write_i8(1)

	response := handler.handle_request(request.bytes()[4..], mut ?&domain.AuthConnection(none)) or {
		panic(err)
	}

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!

	_ = reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()!

	// Resources array
	res_count := reader.read_array_len()!
	assert res_count == 1

	res_type := reader.read_i8()!
	assert res_type == 2
	res_name := reader.read_string()!
	assert res_name == 'test-topic'

	// Read pattern_type (v1+)
	pattern_type := reader.read_i8()!
	assert pattern_type == 3

	// ACLs array
	acl_count := reader.read_array_len()!
	assert acl_count == 1

	principal := reader.read_string()!
	assert principal == 'User:alice'
	host := reader.read_string()!
	assert host == '*'
	op := reader.read_i8()!
	assert op == 3
	perm := reader.read_i8()!
	assert perm == 2
}

fn test_handler_delete_acls() {
	mut handler := create_test_handler_with_acl()

	// Create ACL
	mut create_req := kafka.new_writer()
	create_req.write_i32(0)
	create_req.write_i16(30)
	create_req.write_i16(1)
	create_req.write_i32(1)
	create_req.write_nullable_string('test-client')
	create_req.write_array_len(1)
	create_req.write_i8(2)
	create_req.write_string('test-topic')
	create_req.write_i8(3)
	create_req.write_string('User:alice')
	create_req.write_string('*')
	create_req.write_i8(3)
	create_req.write_i8(2)
	_ = handler.handle_request(create_req.bytes()[4..], mut ?&domain.AuthConnection(none)) or {
		panic(err)
	}

	// Delete ACL
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(31)
	request.write_i16(1)
	request.write_i32(3)
	request.write_nullable_string('test-client')

	// Filter array
	request.write_array_len(1)
	request.write_i8(2)
	request.write_nullable_string('test-topic')
	request.write_i8(3)
	request.write_nullable_string(none)
	request.write_nullable_string(none)
	request.write_i8(1)
	request.write_i8(1)

	response := handler.handle_request(request.bytes()[4..], mut ?&domain.AuthConnection(none)) or {
		panic(err)
	}

	mut reader := kafka.new_reader(response)
	_ = reader.read_i32()!
	_ = reader.read_i32()!

	_ = reader.read_i32()!

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
	assert pattern_type == 3

	principal := reader.read_string()!
	assert principal == 'User:alice'
}
