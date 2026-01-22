// 단위 테스트 - Infra Layer: SASL 프로토콜
module kafka_test

import domain
import infra.protocol.kafka
import infra.auth as infra_auth
import service.auth
import service.port

// 헬퍼: PLAIN 인증 바이트 생성
fn make_plain_auth(authzid string, username string, password string) []u8 {
	mut data := []u8{}
	data << authzid.bytes()
	data << u8(0)
	data << username.bytes()
	data << u8(0)
	data << password.bytes()
	return data
}

// ============================================================================
// SaslHandshake Request/Response Tests
// ============================================================================

fn test_sasl_handshake_request_parse_v0() {
	// Build a SaslHandshake request with mechanism "PLAIN"
	mut writer := kafka.new_writer()
	writer.write_string('PLAIN')

	mut reader := kafka.new_reader(writer.bytes())

	// Parse - v0 is never flexible
	mechanism := reader.read_string()!

	assert mechanism == 'PLAIN'
}

fn test_sasl_handshake_request_parse_v1() {
	// Build a SaslHandshake request with mechanism "SCRAM-SHA-256"
	mut writer := kafka.new_writer()
	writer.write_string('SCRAM-SHA-256')

	mut reader := kafka.new_reader(writer.bytes())

	mechanism := reader.read_string()!

	assert mechanism == 'SCRAM-SHA-256'
}

fn test_sasl_handshake_response_encode_v0_success() {
	response := kafka.SaslHandshakeResponse{
		error_code: 0
		mechanisms: ['PLAIN', 'SCRAM-SHA-256']
	}

	bytes := response.encode(0)

	mut reader := kafka.new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// mechanisms: ARRAY[STRING]
	array_len := reader.read_array_len()!
	assert array_len == 2

	m1 := reader.read_string()!
	assert m1 == 'PLAIN'

	m2 := reader.read_string()!
	assert m2 == 'SCRAM-SHA-256'
}

fn test_sasl_handshake_response_encode_v0_unsupported() {
	response := kafka.SaslHandshakeResponse{
		error_code: i16(kafka.ErrorCode.unsupported_sasl_mechanism)
		mechanisms: ['PLAIN']
	}

	bytes := response.encode(0)

	mut reader := kafka.new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 33 // UNSUPPORTED_SASL_MECHANISM

	array_len := reader.read_array_len()!
	assert array_len == 1
}

// ============================================================================
// SaslAuthenticate Request/Response Tests
// ============================================================================

fn test_sasl_authenticate_request_parse_v0() {
	// Build PLAIN auth bytes
	auth_bytes := make_plain_auth('', 'testuser', 'testpass')

	// Build request with BYTES field
	mut writer := kafka.new_writer()
	writer.write_bytes(auth_bytes)

	mut reader := kafka.new_reader(writer.bytes())

	// Parse auth bytes (non-flexible, v0)
	parsed := reader.read_bytes()!

	assert parsed == auth_bytes
}

fn test_sasl_authenticate_request_parse_v2_flexible() {
	// Build PLAIN auth bytes
	auth_bytes := make_plain_auth('', 'user', 'pass')

	// Build request with COMPACT_BYTES (flexible version)
	mut writer := kafka.new_writer()
	writer.write_compact_bytes(auth_bytes)
	writer.write_tagged_fields()

	mut reader := kafka.new_reader(writer.bytes())

	// Parse auth bytes (flexible, v2)
	parsed := reader.read_compact_bytes()!

	assert parsed == auth_bytes
}

fn test_sasl_authenticate_response_encode_v0_success() {
	response := kafka.SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          []u8{}
		session_lifetime_ms: 0
	}

	bytes := response.encode(0)

	mut reader := kafka.new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// error_message: NULLABLE_STRING
	error_msg := reader.read_nullable_string()!
	assert error_msg == '' // null/empty

	// auth_bytes: BYTES
	auth_bytes := reader.read_bytes()!
	assert auth_bytes.len == 0
}

fn test_sasl_authenticate_response_encode_v1_with_lifetime() {
	response := kafka.SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          []u8{}
		session_lifetime_ms: 3600000 // 1 hour
	}

	bytes := response.encode(1)

	mut reader := kafka.new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_nullable_string()!
	_ := reader.read_bytes()!

	// session_lifetime_ms: INT64 (v1+)
	lifetime := reader.read_i64()!
	assert lifetime == 3600000
}

fn test_sasl_authenticate_response_encode_v0_failure() {
	response := kafka.SaslAuthenticateResponse{
		error_code:          i16(kafka.ErrorCode.sasl_authentication_failed)
		error_message:       'Invalid credentials'
		auth_bytes:          []u8{}
		session_lifetime_ms: 0
	}

	bytes := response.encode(0)

	mut reader := kafka.new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 58 // SASL_AUTHENTICATION_FAILED

	error_msg := reader.read_nullable_string()!
	assert error_msg == 'Invalid credentials'
}

fn test_sasl_authenticate_response_encode_v2_flexible() {
	response := kafka.SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          [u8(1), 2, 3]
		session_lifetime_ms: 7200000
	}

	bytes := response.encode(2)

	mut reader := kafka.new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// error_message: COMPACT_NULLABLE_STRING
	error_msg := reader.read_compact_nullable_string()!
	assert error_msg == ''

	// auth_bytes: COMPACT_BYTES
	auth_bytes := reader.read_compact_bytes()!
	assert auth_bytes == [u8(1), 2, 3]

	// session_lifetime_ms: INT64
	lifetime := reader.read_i64()!
	assert lifetime == 7200000
}

// ============================================================================
// Handler Integration Tests
// ============================================================================

fn create_test_handler_with_auth() kafka.Handler {
	// Create storage mock
	storage := create_mock_storage()

	// Create user store and auth service
	mut user_store := infra_auth.new_memory_user_store()
	user_store.create_user('admin', 'adminpass', .plain) or {}
	user_store.create_user('user1', 'user1pass', .plain) or {}

	auth_service := auth.new_auth_service(user_store, [.plain])

	return kafka.new_handler_with_auth(1, // broker_id
	 '127.0.0.1', // host
	 9092, // port
	 'test-cluster', // cluster_id
	 storage, auth_service)
}

fn create_mock_storage() port.StoragePort {
	return MockStorage{}
}

// 테스트용 MockStorage - StoragePort 구현
struct MockStorage {}

fn (m MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{
		name:            name
		partition_count: partitions
	}
}

fn (m MockStorage) delete_topic(name string) ! {}

fn (m MockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m MockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m MockStorage) add_partitions(name string, new_count int) ! {}

fn (m MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	return domain.AppendResult{
		base_offset:      0
		log_append_time:  0
		log_start_offset: 0
		record_count:     records.len
	}
}

fn (m MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{
		records:          []
		high_watermark:   0
		log_start_offset: 0
	}
}

fn (m MockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m MockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m MockStorage) delete_group(group_id string) ! {}

fn (m MockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &MockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &MockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn test_handler_sasl_handshake_success() {
	mut handler := create_test_handler_with_auth()

	// Build SaslHandshake request: [size][header][body]
	// Request header v0: api_key(2) + version(2) + correlation_id(4) + client_id(nullable string)
	mut request := kafka.new_writer()
	request.write_i32(0) // size (ignored by parse_request but required)
	request.write_i16(17) // api_key: SaslHandshake
	request.write_i16(0) // version
	request.write_i32(1) // correlation_id
	request.write_nullable_string('test-client') // client_id
	request.write_string('PLAIN') // mechanism

	// Handle request
	response := handler.handle_request(request.bytes()[4..]) or {
		panic('handle_request failed: ${err}')
	}

	// Parse response
	mut reader := kafka.new_reader(response)

	// Response: [size(4)][correlation_id(4)][body]
	_ := reader.read_i32()! // size
	correlation_id := reader.read_i32()!
	assert correlation_id == 1

	// Body: error_code(2) + mechanisms_array
	error_code := reader.read_i16()!
	assert error_code == 0

	array_len := reader.read_array_len()!
	assert array_len == 1

	mechanism := reader.read_string()!
	assert mechanism == 'PLAIN'
}

fn test_handler_sasl_handshake_unsupported_mechanism() {
	mut handler := create_test_handler_with_auth()

	mut request := kafka.new_writer()
	request.write_i32(0) // size
	request.write_i16(17) // api_key: SaslHandshake
	request.write_i16(0) // version
	request.write_i32(2) // correlation_id
	request.write_nullable_string('test-client')
	request.write_string('SCRAM-SHA-256') // unsupported mechanism

	response := handler.handle_request(request.bytes()[4..]) or {
		panic('handle_request failed: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	_ := reader.read_i32()! // correlation_id

	error_code := reader.read_i16()!
	assert error_code == 33 // UNSUPPORTED_SASL_MECHANISM
}

fn test_handler_sasl_authenticate_success() {
	mut handler := create_test_handler_with_auth()

	// Build SaslAuthenticate request
	auth_bytes := make_plain_auth('', 'admin', 'adminpass')

	mut request := kafka.new_writer()
	request.write_i32(0) // size
	request.write_i16(36) // api_key: SaslAuthenticate
	request.write_i16(0) // version
	request.write_i32(3) // correlation_id
	request.write_nullable_string('test-client')
	request.write_bytes(auth_bytes)

	response := handler.handle_request(request.bytes()[4..]) or {
		panic('handle_request failed: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	correlation_id := reader.read_i32()!
	assert correlation_id == 3

	// Body: error_code(2) + error_message + auth_bytes
	error_code := reader.read_i16()!
	assert error_code == 0 // SUCCESS
}

fn test_handler_sasl_authenticate_failure() {
	mut handler := create_test_handler_with_auth()

	// Build SaslAuthenticate request with wrong password
	auth_bytes := make_plain_auth('', 'admin', 'wrongpassword')

	mut request := kafka.new_writer()
	request.write_i32(0) // size
	request.write_i16(36) // api_key: SaslAuthenticate
	request.write_i16(0) // version
	request.write_i32(4) // correlation_id
	request.write_nullable_string('test-client')
	request.write_bytes(auth_bytes)

	response := handler.handle_request(request.bytes()[4..]) or {
		panic('handle_request failed: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	_ := reader.read_i32()! // correlation_id

	error_code := reader.read_i16()!
	assert error_code == 58 // SASL_AUTHENTICATION_FAILED
}
