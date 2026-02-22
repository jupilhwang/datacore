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

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or { panic(err) }

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
	mut create_conn := ?&domain.AuthConnection(none)
	_ = handler.handle_request(create_req.bytes()[4..], mut create_conn) or { panic(err) }

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

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or { panic(err) }

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
	mut create_conn2 := ?&domain.AuthConnection(none)
	_ = handler.handle_request(create_req.bytes()[4..], mut create_conn2) or { panic(err) }

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

	mut conn2 := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn2) or { panic(err) }

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

// --- Additional test scenarios ---

// create_acl_helper builds a v1 non-flexible CreateAcls request.
fn create_acl_helper(resource_type i8, resource_name string, pattern_type i8, principal string, host string, operation i8, permission_type i8) []u8 {
	mut w := kafka.new_writer()
	w.write_i32(0)
	w.write_i16(30)
	w.write_i16(1)
	w.write_i32(1)
	w.write_nullable_string('test-client')
	w.write_array_len(1)
	w.write_i8(resource_type)
	w.write_string(resource_name)
	w.write_i8(pattern_type)
	w.write_string(principal)
	w.write_string(host)
	w.write_i8(operation)
	w.write_i8(permission_type)
	return w.bytes()
}

// describe_all_acls_helper builds a v1 DescribeAcls request matching all ACLs.
fn describe_all_acls_helper() []u8 {
	mut w := kafka.new_writer()
	w.write_i32(0)
	w.write_i16(29)
	w.write_i16(1)
	w.write_i32(2)
	w.write_nullable_string('test-client')
	w.write_i8(1)
	w.write_nullable_string(none)
	w.write_i8(1)
	w.write_nullable_string(none)
	w.write_nullable_string(none)
	w.write_i8(1)
	w.write_i8(1)
	return w.bytes()
}

fn test_create_acls_duplicate_is_idempotent() {
	mut handler := create_test_handler_with_acl()

	// Create same ACL twice
	create_body := create_acl_helper(2, 'dup-topic', 3, 'User:bob', '*', 3, 2)
	for _ in 0 .. 2 {
		mut c := ?&domain.AuthConnection(none)
		resp := handler.handle_request(create_body[4..], mut c) or { panic(err) }
		mut reader := kafka.new_reader(resp)
		_ = reader.read_i32()!
		_ = reader.read_i32()!
		_ = reader.read_i32()!
		count := reader.read_array_len()!
		assert count == 1
		error_code := reader.read_i16()!
		assert error_code == 0
	}

	// Only one resource entry should exist in describe response
	desc_body := describe_all_acls_helper()
	mut desc_conn := ?&domain.AuthConnection(none)
	desc_resp := handler.handle_request(desc_body[4..], mut desc_conn) or { panic(err) }
	mut dr := kafka.new_reader(desc_resp)
	_ = dr.read_i32()!
	_ = dr.read_i32()!
	_ = dr.read_i32()!
	_ = dr.read_i16()!
	_ = dr.read_nullable_string()!
	res_count := dr.read_array_len()!
	assert res_count == 1
}

fn test_delete_acls_multiple_matches() {
	mut handler := create_test_handler_with_acl()

	// Create two ACLs on the same topic with different principals
	for principal in ['User:charlie', 'User:dave'] {
		mut c := ?&domain.AuthConnection(none)
		body := create_acl_helper(2, 'multi-topic', 3, principal, '*', 3, 2)
		_ = handler.handle_request(body[4..], mut c) or { panic(err) }
	}

	// Delete all ACLs on multi-topic
	mut del_req := kafka.new_writer()
	del_req.write_i32(0)
	del_req.write_i16(31)
	del_req.write_i16(1)
	del_req.write_i32(3)
	del_req.write_nullable_string('test-client')
	del_req.write_array_len(1)
	del_req.write_i8(2)
	del_req.write_nullable_string('multi-topic')
	del_req.write_i8(3)
	del_req.write_nullable_string(none)
	del_req.write_nullable_string(none)
	del_req.write_i8(1)
	del_req.write_i8(1)

	mut del_conn := ?&domain.AuthConnection(none)
	del_resp := handler.handle_request(del_req.bytes()[4..], mut del_conn) or { panic(err) }

	mut reader := kafka.new_reader(del_resp)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 1
	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()!
	match_count := reader.read_array_len()!
	assert match_count == 2
}

fn test_describe_acls_empty_when_none_exist() {
	mut handler := create_test_handler_with_acl()

	mut conn := ?&domain.AuthConnection(none)
	desc_body := describe_all_acls_helper()
	desc_resp := handler.handle_request(desc_body[4..], mut conn) or { panic(err) }

	mut reader := kafka.new_reader(desc_resp)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()!
	res_count := reader.read_array_len()!
	assert res_count == 0
}

fn test_acls_security_disabled_when_no_acl_manager() {
	storage := AclMockStorage{}
	compression_service := compression.new_default_compression_service() or {
		panic('failed to create compression service: ${err}')
	}
	// Handler with no ACL manager
	mut handler := kafka.new_handler_full(1, '127.0.0.1', 9092, 'test-cluster', storage,
		none, none, none, compression_service)

	mut conn := ?&domain.AuthConnection(none)
	desc_body := describe_all_acls_helper()
	desc_resp := handler.handle_request(desc_body[4..], mut conn) or { panic(err) }

	mut reader := kafka.new_reader(desc_resp)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	error_code := reader.read_i16()!
	// 54 = SECURITY_DISABLED
	assert error_code == 54
}

fn test_delete_acls_no_matches_returns_zero() {
	mut handler := create_test_handler_with_acl()

	mut del_req := kafka.new_writer()
	del_req.write_i32(0)
	del_req.write_i16(31)
	del_req.write_i16(1)
	del_req.write_i32(3)
	del_req.write_nullable_string('test-client')
	del_req.write_array_len(1)
	del_req.write_i8(2)
	del_req.write_nullable_string('nonexistent-topic')
	del_req.write_i8(3)
	del_req.write_nullable_string(none)
	del_req.write_nullable_string(none)
	del_req.write_i8(1)
	del_req.write_i8(1)

	mut del_conn := ?&domain.AuthConnection(none)
	del_resp := handler.handle_request(del_req.bytes()[4..], mut del_conn) or { panic(err) }

	mut reader := kafka.new_reader(del_resp)
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	_ = reader.read_i32()!
	count := reader.read_array_len()!
	assert count == 1
	error_code := reader.read_i16()!
	assert error_code == 0
	_ = reader.read_nullable_string()!
	match_count := reader.read_array_len()!
	assert match_count == 0
}
