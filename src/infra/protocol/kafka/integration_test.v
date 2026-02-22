// Integration tests - Combined SASL/ACL/Transaction tests
// Task #48: Security and transaction feature integration tests
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.auth as infra_auth
import infra.transaction as infra_txn
import service.auth
import service.transaction
import service.port

// Full Integration Handler (SASL + ACL + Transaction)

fn create_full_integration_handler() kafka.Handler {
	storage := IntegrationMockStorage{}

	// SASL: User Store + Auth Service
	mut user_store := infra_auth.new_memory_user_store()
	user_store.create_user('admin', 'adminpass', .plain) or {}
	user_store.create_user('producer', 'producerpass', .plain) or {}
	user_store.create_user('consumer', 'consumerpass', .plain) or {}
	user_store.create_user('txn-user', 'txnpass', .plain) or {}
	auth_service := auth.new_auth_service(user_store, [.plain])

	// ACL: ACL Manager
	acl_manager := infra_auth.new_memory_acl_manager()

	// Transaction: Transaction Coordinator
	txn_store := infra_txn.new_memory_transaction_store()
	txn_coordinator := transaction.new_transaction_coordinator(txn_store)

	// Compression Service
	compression_service := compression.new_default_compression_service() or {
		panic('failed to create compression service: ${err}')
	}

	return kafka.new_handler_full(1, '127.0.0.1', 9092, 'integration-test-cluster', storage,
		auth_service, acl_manager, *txn_coordinator, compression_service)
}

// Mock Storage for integration tests
struct IntegrationMockStorage {}

fn (m IntegrationMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{
		name:            name
		partition_count: partitions
		topic_id:        [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
	}
}

fn (m IntegrationMockStorage) delete_topic(name string) ! {}

fn (m IntegrationMockStorage) list_topics() ![]domain.TopicMetadata {
	return [
		domain.TopicMetadata{
			name:            'test-topic'
			partition_count: 3
			topic_id:        [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
		},
	]
}

fn (m IntegrationMockStorage) get_topic(name string) !domain.TopicMetadata {
	if name == 'test-topic' {
		return domain.TopicMetadata{
			name:            name
			partition_count: 3
			topic_id:        [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
		}
	}
	return error('topic not found')
}

fn (m IntegrationMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m IntegrationMockStorage) add_partitions(name string, new_count int) ! {}

fn (m IntegrationMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{
		base_offset:      0
		log_append_time:  1000
		log_start_offset: 0
		record_count:     records.len
	}
}

fn (m IntegrationMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{
		records:          []
		high_watermark:   10
		log_start_offset: 0
	}
}

fn (m IntegrationMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m IntegrationMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		latest_offset:   10
		high_watermark:  10
	}
}

fn (m IntegrationMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m IntegrationMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m IntegrationMockStorage) delete_group(group_id string) ! {}

fn (m IntegrationMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m IntegrationMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m IntegrationMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m IntegrationMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &IntegrationMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &IntegrationMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m IntegrationMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m IntegrationMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m IntegrationMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m IntegrationMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []domain.SharePartitionState{}
}

// MockAuthConnection implements domain.AuthConnection for testing
struct MockAuthConnection {
mut:
	authenticated bool
	principal     domain.Principal
}

fn (m MockAuthConnection) is_authenticated() bool {
	return m.authenticated
}

fn (mut m MockAuthConnection) set_authenticated(principal domain.Principal) {
	m.authenticated = true
	m.principal = principal
}

fn new_mock_auth_conn() &MockAuthConnection {
	return &MockAuthConnection{}
}

fn new_authenticated_mock_conn(principal_name string) &MockAuthConnection {
	return &MockAuthConnection{
		authenticated: true
		principal:     domain.new_user_principal(principal_name)
	}
}

// Helper Functions

fn make_plain_auth_bytes(authzid string, username string, password string) []u8 {
	mut data := []u8{}
	data << authzid.bytes()
	data << u8(0)
	data << username.bytes()
	data << u8(0)
	data << password.bytes()
	return data
}

// Test 1: SASL Authentication + ACL Authorization Flow

fn test_sasl_auth_then_acl_check() {
	mut handler := create_full_integration_handler()

	// Step 1: SASL Handshake
	mut handshake_req := kafka.new_writer()
	handshake_req.write_i32(0)
	handshake_req.write_i16(17)
	handshake_req.write_i16(0)
	handshake_req.write_i32(1)
	handshake_req.write_nullable_string('integration-client')
	handshake_req.write_string('PLAIN')

	mut mock_conn := new_mock_auth_conn()
	mut auth_conn := ?&domain.AuthConnection(mock_conn)
	handshake_resp := handler.handle_request(handshake_req.bytes()[4..], mut auth_conn) or {
		panic(err)
	}
	mut hs_reader := kafka.new_reader(handshake_resp)
	_ = hs_reader.read_i32()!
	_ = hs_reader.read_i32()!
	hs_error := hs_reader.read_i16()!
	assert hs_error == 0, 'SASL Handshake should succeed'

	// Step 2: SASL Authenticate as 'producer'
	auth_bytes := make_plain_auth_bytes('', 'producer', 'producerpass')
	mut auth_req := kafka.new_writer()
	auth_req.write_i32(0)
	auth_req.write_i16(36)
	auth_req.write_i16(0)
	auth_req.write_i32(2)
	auth_req.write_nullable_string('integration-client')
	auth_req.write_bytes(auth_bytes)

	auth_resp := handler.handle_request(auth_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut auth_reader := kafka.new_reader(auth_resp)
	_ = auth_reader.read_i32()!
	_ = auth_reader.read_i32()!
	auth_error := auth_reader.read_i16()!
	assert auth_error == 0, 'SASL Authentication should succeed'

	// Step 3: Create ACL for producer user
	mut create_acl_req := kafka.new_writer()
	create_acl_req.write_i32(0)
	create_acl_req.write_i16(30)
	create_acl_req.write_i16(1)
	create_acl_req.write_i32(3)
	create_acl_req.write_nullable_string('integration-client')
	create_acl_req.write_array_len(1)
	create_acl_req.write_i8(2)
	create_acl_req.write_string('test-topic')
	create_acl_req.write_i8(3)
	create_acl_req.write_string('User:producer')
	create_acl_req.write_string('*')
	create_acl_req.write_i8(4)
	create_acl_req.write_i8(2)

	create_acl_resp := handler.handle_request(create_acl_req.bytes()[4..], mut auth_conn) or {
		panic(err)
	}
	mut acl_reader := kafka.new_reader(create_acl_resp)
	_ = acl_reader.read_i32()!
	_ = acl_reader.read_i32()!
	_ = acl_reader.read_i32()!
	acl_count := acl_reader.read_array_len()!
	assert acl_count == 1
	acl_error := acl_reader.read_i16()!
	assert acl_error == 0, 'ACL creation should succeed'

	// Step 4: Describe ACLs to verify
	mut describe_acl_req := kafka.new_writer()
	describe_acl_req.write_i32(0)
	describe_acl_req.write_i16(29)
	describe_acl_req.write_i16(1)
	describe_acl_req.write_i32(4)
	describe_acl_req.write_nullable_string('integration-client')
	describe_acl_req.write_i8(2)
	describe_acl_req.write_nullable_string('test-topic')
	describe_acl_req.write_i8(3)
	describe_acl_req.write_nullable_string('User:producer')
	describe_acl_req.write_nullable_string(none)
	describe_acl_req.write_i8(1)
	describe_acl_req.write_i8(1)

	describe_resp := handler.handle_request(describe_acl_req.bytes()[4..], mut auth_conn) or {
		panic(err)
	}
	mut desc_reader := kafka.new_reader(describe_resp)
	_ = desc_reader.read_i32()!
	_ = desc_reader.read_i32()!
	_ = desc_reader.read_i32()!
	desc_error := desc_reader.read_i16()!
	assert desc_error == 0
	_ = desc_reader.read_nullable_string()!
	res_count := desc_reader.read_array_len()!
	assert res_count == 1, 'Should find 1 ACL resource'
}

// Test 2: Transaction Flow with ACL Checks

fn test_transaction_with_acl() {
	mut handler := create_full_integration_handler()

	// Use pre-authenticated connection (bypasses SASL for this test)
	mut mock_conn := new_authenticated_mock_conn('txn-user')
	mut auth_conn := ?&domain.AuthConnection(mock_conn)

	// Step 1: Create ACL for transactional user
	mut create_acl_req := kafka.new_writer()
	create_acl_req.write_i32(0)
	create_acl_req.write_i16(30)
	create_acl_req.write_i16(1)
	create_acl_req.write_i32(1)
	create_acl_req.write_nullable_string('txn-client')

	// Create multiple ACLs: WRITE on topic + IDEMPOTENT_WRITE on cluster
	create_acl_req.write_array_len(2)

	// ACL 1: WRITE on test-topic
	create_acl_req.write_i8(2)
	create_acl_req.write_string('test-topic')
	create_acl_req.write_i8(3)
	create_acl_req.write_string('User:txn-user')
	create_acl_req.write_string('*')
	create_acl_req.write_i8(4)
	create_acl_req.write_i8(2)

	// ACL 2: IDEMPOTENT_WRITE on cluster
	create_acl_req.write_i8(4)
	create_acl_req.write_string('kafka-cluster')
	create_acl_req.write_i8(3)
	create_acl_req.write_string('User:txn-user')
	create_acl_req.write_string('*')
	create_acl_req.write_i8(11)
	create_acl_req.write_i8(2)

	_ = handler.handle_request(create_acl_req.bytes()[4..], mut auth_conn) or { panic(err) }

	// Step 2: InitProducerId for transactional producer
	mut init_req := kafka.new_writer()
	init_req.write_i32(0)
	init_req.write_i16(22)
	init_req.write_i16(3)
	init_req.write_i32(2)
	init_req.write_nullable_string('txn-client')
	init_req.write_tagged_fields()
	init_req.write_compact_nullable_string('txn-integration-test')
	init_req.write_i32(60000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()

	init_resp := handler.handle_request(init_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut init_reader := kafka.new_reader(init_resp)
	_ = init_reader.read_i32()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_uvarint()!
	_ = init_reader.read_i32()!
	init_error := init_reader.read_i16()!
	assert init_error == 0, 'InitProducerId should succeed'

	pid := init_reader.read_i64()!
	epoch := init_reader.read_i16()!
	assert pid > 0, 'Producer ID should be positive'
	assert epoch == 0, 'Initial epoch should be 0'

	// Step 3: AddPartitionsToTxn
	mut add_req := kafka.new_writer()
	add_req.write_i32(0)
	add_req.write_i16(24)
	add_req.write_i16(3)
	add_req.write_i32(3)
	add_req.write_nullable_string('txn-client')
	add_req.write_tagged_fields()
	add_req.write_compact_string('txn-integration-test')
	add_req.write_i64(pid)
	add_req.write_i16(epoch)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('test-topic')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()

	add_resp := handler.handle_request(add_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut add_reader := kafka.new_reader(add_resp)
	_ = add_reader.read_i32()!
	_ = add_reader.read_i32()!
	_ = add_reader.read_uvarint()!
	_ = add_reader.read_i32()!

	add_count := add_reader.read_compact_array_len()!
	assert add_count == 1
	_ = add_reader.read_compact_string()!
	p_count := add_reader.read_compact_array_len()!
	assert p_count == 1
	_ = add_reader.read_i32()!
	add_error := add_reader.read_i16()!
	assert add_error == 0, 'AddPartitionsToTxn should succeed'

	// Step 4: EndTxn (Commit)
	mut end_req := kafka.new_writer()
	end_req.write_i32(0)
	end_req.write_i16(26)
	end_req.write_i16(3)
	end_req.write_i32(4)
	end_req.write_nullable_string('txn-client')
	end_req.write_tagged_fields()
	end_req.write_compact_string('txn-integration-test')
	end_req.write_i64(pid)
	end_req.write_i16(epoch)
	end_req.write_i8(1)
	end_req.write_tagged_fields()

	end_resp := handler.handle_request(end_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut end_reader := kafka.new_reader(end_resp)
	_ = end_reader.read_i32()!
	_ = end_reader.read_i32()!
	_ = end_reader.read_uvarint()!
	_ = end_reader.read_i32()!
	end_error := end_reader.read_i16()!
	assert end_error == 0, 'EndTxn (Commit) should succeed'
}

// Test 3: Full SASL + ACL + Transaction Integration

fn test_full_sasl_acl_transaction_flow() {
	mut handler := create_full_integration_handler()

	// Step 1: SASL Authentication
	mut handshake_req := kafka.new_writer()
	handshake_req.write_i32(0)
	handshake_req.write_i16(17)
	handshake_req.write_i16(0)
	handshake_req.write_i32(1)
	handshake_req.write_nullable_string('full-test-client')
	handshake_req.write_string('PLAIN')
	mut mock_conn := new_mock_auth_conn()
	mut auth_conn := ?&domain.AuthConnection(mock_conn)
	_ = handler.handle_request(handshake_req.bytes()[4..], mut auth_conn) or { panic(err) }

	auth_bytes := make_plain_auth_bytes('', 'txn-user', 'txnpass')
	mut auth_req := kafka.new_writer()
	auth_req.write_i32(0)
	auth_req.write_i16(36)
	auth_req.write_i16(0)
	auth_req.write_i32(2)
	auth_req.write_nullable_string('full-test-client')
	auth_req.write_bytes(auth_bytes)

	auth_resp := handler.handle_request(auth_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut auth_reader := kafka.new_reader(auth_resp)
	_ = auth_reader.read_i32()!
	_ = auth_reader.read_i32()!
	auth_error := auth_reader.read_i16()!
	assert auth_error == 0, 'SASL auth should succeed for txn-user'

	// Step 2: Setup ACLs
	mut create_acl_req := kafka.new_writer()
	create_acl_req.write_i32(0)
	create_acl_req.write_i16(30)
	create_acl_req.write_i16(1)
	create_acl_req.write_i32(3)
	create_acl_req.write_nullable_string('full-test-client')
	create_acl_req.write_array_len(1)
	create_acl_req.write_i8(2)
	create_acl_req.write_string('txn-topic')
	create_acl_req.write_i8(3)
	create_acl_req.write_string('User:txn-user')
	create_acl_req.write_string('*')
	create_acl_req.write_i8(5)
	create_acl_req.write_i8(2)
	_ = handler.handle_request(create_acl_req.bytes()[4..], mut auth_conn) or { panic(err) }

	// Step 3: Transaction Flow
	mut init_req := kafka.new_writer()
	init_req.write_i32(0)
	init_req.write_i16(22)
	init_req.write_i16(3)
	init_req.write_i32(4)
	init_req.write_nullable_string('full-test-client')
	init_req.write_tagged_fields()
	init_req.write_compact_nullable_string('full-integration-txn')
	init_req.write_i32(30000)
	init_req.write_i64(-1)
	init_req.write_i16(-1)
	init_req.write_tagged_fields()

	init_resp := handler.handle_request(init_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut init_reader := kafka.new_reader(init_resp)
	_ = init_reader.read_i32()!
	_ = init_reader.read_i32()!
	_ = init_reader.read_uvarint()!
	_ = init_reader.read_i32()!
	init_error := init_reader.read_i16()!
	assert init_error == 0

	pid := init_reader.read_i64()!
	epoch := init_reader.read_i16()!

	// AddOffsetsToTxn
	mut add_offsets_req := kafka.new_writer()
	add_offsets_req.write_i32(0)
	add_offsets_req.write_i16(25)
	add_offsets_req.write_i16(3)
	add_offsets_req.write_i32(5)
	add_offsets_req.write_nullable_string('full-test-client')
	add_offsets_req.write_tagged_fields()
	add_offsets_req.write_compact_string('full-integration-txn')
	add_offsets_req.write_i64(pid)
	add_offsets_req.write_i16(epoch)
	add_offsets_req.write_compact_string('test-consumer-group')
	add_offsets_req.write_tagged_fields()

	add_offsets_resp := handler.handle_request(add_offsets_req.bytes()[4..], mut auth_conn) or {
		panic(err)
	}
	mut ao_reader := kafka.new_reader(add_offsets_resp)
	_ = ao_reader.read_i32()!
	_ = ao_reader.read_i32()!
	_ = ao_reader.read_uvarint()!
	_ = ao_reader.read_i32()!
	ao_error := ao_reader.read_i16()!
	assert ao_error == 0, 'AddOffsetsToTxn should succeed'

	// EndTxn (Abort)
	mut end_req := kafka.new_writer()
	end_req.write_i32(0)
	end_req.write_i16(26)
	end_req.write_i16(3)
	end_req.write_i32(6)
	end_req.write_nullable_string('full-test-client')
	end_req.write_tagged_fields()
	end_req.write_compact_string('full-integration-txn')
	end_req.write_i64(pid)
	end_req.write_i16(epoch)
	end_req.write_i8(0)
	end_req.write_tagged_fields()

	end_resp := handler.handle_request(end_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut end_reader := kafka.new_reader(end_resp)
	_ = end_reader.read_i32()!
	_ = end_reader.read_i32()!
	_ = end_reader.read_uvarint()!
	_ = end_reader.read_i32()!
	end_error := end_reader.read_i16()!
	assert end_error == 0, 'EndTxn (Abort) should succeed'
}

// Test 4: Authentication Failure Scenarios

fn test_sasl_auth_failure_wrong_password() {
	mut handler := create_full_integration_handler()

	// SASL Handshake
	mut handshake_req := kafka.new_writer()
	handshake_req.write_i32(0)
	handshake_req.write_i16(17)
	handshake_req.write_i16(0)
	handshake_req.write_i32(1)
	handshake_req.write_nullable_string('test-client')
	handshake_req.write_string('PLAIN')
	mut mock_conn := new_mock_auth_conn()
	mut auth_conn := ?&domain.AuthConnection(mock_conn)
	_ = handler.handle_request(handshake_req.bytes()[4..], mut auth_conn) or { panic(err) }

	// SASL Authenticate with wrong password
	auth_bytes := make_plain_auth_bytes('', 'admin', 'wrongpassword')
	mut auth_req := kafka.new_writer()
	auth_req.write_i32(0)
	auth_req.write_i16(36)
	auth_req.write_i16(0)
	auth_req.write_i32(2)
	auth_req.write_nullable_string('test-client')
	auth_req.write_bytes(auth_bytes)

	auth_resp := handler.handle_request(auth_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut auth_reader := kafka.new_reader(auth_resp)
	_ = auth_reader.read_i32()!
	_ = auth_reader.read_i32()!
	auth_error := auth_reader.read_i16()!
	assert auth_error == 58, 'Should return SASL_AUTHENTICATION_FAILED (58)'
}

fn test_sasl_auth_failure_unknown_user() {
	mut handler := create_full_integration_handler()

	// SASL Handshake
	mut handshake_req := kafka.new_writer()
	handshake_req.write_i32(0)
	handshake_req.write_i16(17)
	handshake_req.write_i16(0)
	handshake_req.write_i32(1)
	handshake_req.write_nullable_string('test-client')
	handshake_req.write_string('PLAIN')
	mut mock_conn := new_mock_auth_conn()
	mut auth_conn := ?&domain.AuthConnection(mock_conn)
	_ = handler.handle_request(handshake_req.bytes()[4..], mut auth_conn) or { panic(err) }

	// SASL Authenticate with unknown user
	auth_bytes := make_plain_auth_bytes('', 'unknownuser', 'anypassword')
	mut auth_req := kafka.new_writer()
	auth_req.write_i32(0)
	auth_req.write_i16(36)
	auth_req.write_i16(0)
	auth_req.write_i32(2)
	auth_req.write_nullable_string('test-client')
	auth_req.write_bytes(auth_bytes)

	auth_resp := handler.handle_request(auth_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut auth_reader := kafka.new_reader(auth_resp)
	_ = auth_reader.read_i32()!
	_ = auth_reader.read_i32()!
	auth_error := auth_reader.read_i16()!
	assert auth_error == 58, 'Should return SASL_AUTHENTICATION_FAILED (58)'
}

// Test 5: Transaction Error Scenarios

fn test_transaction_invalid_producer_id() {
	mut handler := create_full_integration_handler()

	// Try AddPartitionsToTxn with invalid producer ID
	mut mock_conn := new_authenticated_mock_conn('txn-user')
	mut auth_conn := ?&domain.AuthConnection(mock_conn)
	mut add_req := kafka.new_writer()
	add_req.write_i32(0)
	add_req.write_i16(24)
	add_req.write_i16(3)
	add_req.write_i32(1)
	add_req.write_nullable_string('test-client')
	add_req.write_tagged_fields()
	add_req.write_compact_string('non-existent-txn')
	add_req.write_i64(999999)
	add_req.write_i16(0)
	add_req.write_compact_array_len(1)
	add_req.write_compact_string('test-topic')
	add_req.write_compact_array_len(1)
	add_req.write_i32(0)
	add_req.write_tagged_fields()
	add_req.write_tagged_fields()

	add_resp := handler.handle_request(add_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut add_reader := kafka.new_reader(add_resp)
	_ = add_reader.read_i32()!
	_ = add_reader.read_i32()!
	_ = add_reader.read_uvarint()!
	_ = add_reader.read_i32()!

	// The response should contain an error
	count := add_reader.read_compact_array_len()!
	assert count == 1
	_ = add_reader.read_compact_string()!
	p_count := add_reader.read_compact_array_len()!
	assert p_count == 1
	_ = add_reader.read_i32()!
	error_code := add_reader.read_i16()!
	// Should be non-zero error (INVALID_PRODUCER_ID_MAPPING or similar)
	assert error_code != 0, 'Should return error for invalid producer ID'
}

// Test 6: ACL Delete and Re-check

fn test_acl_lifecycle() {
	mut handler := create_full_integration_handler()

	// Use pre-authenticated connection
	mut mock_conn := new_authenticated_mock_conn('admin')
	mut auth_conn := ?&domain.AuthConnection(mock_conn)

	// Create ACL
	mut create_req := kafka.new_writer()
	create_req.write_i32(0)
	create_req.write_i16(30)
	create_req.write_i16(1)
	create_req.write_i32(1)
	create_req.write_nullable_string('test-client')
	create_req.write_array_len(1)
	create_req.write_i8(2)
	create_req.write_string('lifecycle-topic')
	create_req.write_i8(3)
	create_req.write_string('User:lifecycle-user')
	create_req.write_string('*')
	create_req.write_i8(3)
	create_req.write_i8(2)
	_ = handler.handle_request(create_req.bytes()[4..], mut auth_conn) or { panic(err) }

	// Verify ACL exists
	mut describe_req := kafka.new_writer()
	describe_req.write_i32(0)
	describe_req.write_i16(29)
	describe_req.write_i16(1)
	describe_req.write_i32(2)
	describe_req.write_nullable_string('test-client')
	describe_req.write_i8(2)
	describe_req.write_nullable_string('lifecycle-topic')
	describe_req.write_i8(3)
	describe_req.write_nullable_string(none)
	describe_req.write_nullable_string(none)
	describe_req.write_i8(1)
	describe_req.write_i8(1)

	describe_resp := handler.handle_request(describe_req.bytes()[4..], mut auth_conn) or {
		panic(err)
	}
	mut desc_reader := kafka.new_reader(describe_resp)
	_ = desc_reader.read_i32()!
	_ = desc_reader.read_i32()!
	_ = desc_reader.read_i32()!
	_ = desc_reader.read_i16()!
	_ = desc_reader.read_nullable_string()!
	res_count := desc_reader.read_array_len()!
	assert res_count == 1, 'ACL should exist'

	// Delete ACL
	mut delete_req := kafka.new_writer()
	delete_req.write_i32(0)
	delete_req.write_i16(31)
	delete_req.write_i16(1)
	delete_req.write_i32(3)
	delete_req.write_nullable_string('test-client')
	delete_req.write_array_len(1)
	delete_req.write_i8(2)
	delete_req.write_nullable_string('lifecycle-topic')
	delete_req.write_i8(3)
	delete_req.write_nullable_string(none)
	delete_req.write_nullable_string(none)
	delete_req.write_i8(1)
	delete_req.write_i8(1)

	delete_resp := handler.handle_request(delete_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut del_reader := kafka.new_reader(delete_resp)
	_ = del_reader.read_i32()!
	_ = del_reader.read_i32()!
	_ = del_reader.read_i32()!
	del_count := del_reader.read_array_len()!
	assert del_count == 1
	del_error := del_reader.read_i16()!
	assert del_error == 0

	// Verify ACL is deleted
	mut verify_req := kafka.new_writer()
	verify_req.write_i32(0)
	verify_req.write_i16(29)
	verify_req.write_i16(1)
	verify_req.write_i32(4)
	verify_req.write_nullable_string('test-client')
	verify_req.write_i8(2)
	verify_req.write_nullable_string('lifecycle-topic')
	verify_req.write_i8(3)
	verify_req.write_nullable_string(none)
	verify_req.write_nullable_string(none)
	verify_req.write_i8(1)
	verify_req.write_i8(1)

	verify_resp := handler.handle_request(verify_req.bytes()[4..], mut auth_conn) or { panic(err) }
	mut ver_reader := kafka.new_reader(verify_resp)
	_ = ver_reader.read_i32()!
	_ = ver_reader.read_i32()!
	_ = ver_reader.read_i32()!
	_ = ver_reader.read_i16()!
	_ = ver_reader.read_nullable_string()!
	final_count := ver_reader.read_array_len()!
	assert final_count == 0, 'ACL should be deleted'
}
