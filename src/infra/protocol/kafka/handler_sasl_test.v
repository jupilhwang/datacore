// Unit tests - SASL handler 테스트
// SaslHandshake, SaslAuthenticate 핸들러 함수의 동작을 검증한다.
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import infra.auth as infra_auth
import service.auth
import service.port

// -- 핸들러 생성 헬퍼 --

fn create_sasl_handler_plain_only() kafka.Handler {
	storage := SaslMockStorage{}
	mut user_store := infra_auth.new_memory_user_store()
	user_store.create_user('admin', 'adminpass', .plain) or {}
	user_store.create_user('user1', 'user1pass', .plain) or {}
	auth_service := auth.new_auth_service(user_store, [.plain])
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler_with_auth(1, '127.0.0.1', 9092, 'test-cluster', storage,
		auth_service, cs)
}

fn create_sasl_handler_multi_mechanism() kafka.Handler {
	storage := SaslMockStorage{}
	mut user_store := infra_auth.new_memory_user_store()
	user_store.create_user('admin', 'adminpass', .plain) or {}
	user_store.create_user('scramuser', 'scrampass', .scram_sha_256) or {}
	auth_service := auth.new_auth_service(user_store, [.plain, .scram_sha_256])
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler_with_auth(1, '127.0.0.1', 9092, 'test-cluster', storage,
		auth_service, cs)
}

fn create_sasl_handler_no_auth() kafka.Handler {
	storage := SaslMockStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, cs)
}

// PLAIN 인증 바이트 생성 헬퍼
fn build_plain_auth(authzid string, username string, password string) []u8 {
	mut data := []u8{}
	data << authzid.bytes()
	data << u8(0)
	data << username.bytes()
	data << u8(0)
	data << password.bytes()
	return data
}

// 요청 빌더 헬퍼: SaslHandshake (API Key 17, non-flexible)
fn build_sasl_handshake_request(correlation_id i32, version i16, mechanism string) []u8 {
	mut w := kafka.new_writer()
	w.write_i32(0) // size placeholder
	w.write_i16(17) // api_key
	w.write_i16(version)
	w.write_i32(correlation_id)
	w.write_nullable_string('test-client')
	w.write_string(mechanism)
	return w.bytes()[4..]
}

// 요청 빌더 헬퍼: SaslAuthenticate (API Key 36)
fn build_sasl_authenticate_request(correlation_id i32, version i16, auth_bytes []u8) []u8 {
	mut w := kafka.new_writer()
	w.write_i32(0) // size placeholder
	w.write_i16(36)
	w.write_i16(version)
	w.write_i32(correlation_id)

	if version >= 2 {
		// v2+: flexible header
		w.write_nullable_string('test-client')
		w.write_tagged_fields()
		w.write_compact_bytes(auth_bytes)
		w.write_tagged_fields()
	} else {
		w.write_nullable_string('test-client')
		w.write_bytes(auth_bytes)
	}

	return w.bytes()[4..]
}

// -- SaslHandshake 핸들러 테스트 --

fn test_sasl_handshake_handler_plain_mechanism_success() {
	mut handler := create_sasl_handler_plain_only()
	req := build_sasl_handshake_request(100, 0, 'PLAIN')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	cid := reader.read_i32()! // correlation_id
	assert cid == 100

	error_code := reader.read_i16()!
	assert error_code == 0 // 성공

	array_len := reader.read_array_len()!
	assert array_len == 1
	m := reader.read_string()!
	assert m == 'PLAIN'
}

fn test_sasl_handshake_handler_unsupported_mechanism() {
	mut handler := create_sasl_handler_plain_only()
	req := build_sasl_handshake_request(101, 0, 'GSSAPI')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 33 // UNSUPPORTED_SASL_MECHANISM
}

fn test_sasl_handshake_handler_multi_mechanism() {
	mut handler := create_sasl_handler_multi_mechanism()
	req := build_sasl_handshake_request(102, 1, 'SCRAM-SHA-256')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 0

	// 지원 메커니즘 목록에 PLAIN과 SCRAM-SHA-256이 포함되어야 한다
	array_len := reader.read_array_len()!
	assert array_len == 2
}

fn test_sasl_handshake_handler_no_auth_manager_plain_default() {
	// auth_manager가 없는 핸들러에서는 기본적으로 PLAIN만 지원한다
	mut handler := create_sasl_handler_no_auth()
	req := build_sasl_handshake_request(103, 0, 'PLAIN')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 0

	array_len := reader.read_array_len()!
	assert array_len == 1
	m := reader.read_string()!
	assert m == 'PLAIN'
}

fn test_sasl_handshake_handler_no_auth_manager_unsupported() {
	// auth_manager가 없을 때 PLAIN 외의 메커니즘은 거부된다
	mut handler := create_sasl_handler_no_auth()
	req := build_sasl_handshake_request(104, 0, 'SCRAM-SHA-256')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 33 // UNSUPPORTED_SASL_MECHANISM
}

// -- SaslAuthenticate 핸들러 테스트 --

fn test_sasl_authenticate_handler_v0_success() {
	mut handler := create_sasl_handler_plain_only()
	auth_bytes := build_plain_auth('', 'admin', 'adminpass')
	req := build_sasl_authenticate_request(200, 0, auth_bytes)

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	cid := reader.read_i32()!
	assert cid == 200

	error_code := reader.read_i16()!
	assert error_code == 0 // 인증 성공
}

fn test_sasl_authenticate_handler_v0_wrong_password() {
	mut handler := create_sasl_handler_plain_only()
	auth_bytes := build_plain_auth('', 'admin', 'wrongpass')
	req := build_sasl_authenticate_request(201, 0, auth_bytes)

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 58 // SASL_AUTHENTICATION_FAILED
}

fn test_sasl_authenticate_handler_v0_unknown_user() {
	mut handler := create_sasl_handler_plain_only()
	auth_bytes := build_plain_auth('', 'nonexistent', 'somepass')
	req := build_sasl_authenticate_request(202, 0, auth_bytes)

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 58 // SASL_AUTHENTICATION_FAILED
}

fn test_sasl_authenticate_handler_no_auth_manager() {
	// auth_manager가 없으면 ILLEGAL_SASL_STATE를 반환해야 한다
	mut handler := create_sasl_handler_no_auth()
	auth_bytes := build_plain_auth('', 'admin', 'adminpass')
	req := build_sasl_authenticate_request(203, 0, auth_bytes)

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(req, mut conn) or { panic('요청 처리 실패: ${err}') }

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()!
	_ := reader.read_i32()!

	error_code := reader.read_i16()!
	assert error_code == 34 // ILLEGAL_SASL_STATE
}

// -- detect_sasl_mechanism 테스트 --

fn test_detect_sasl_mechanism_plain() {
	plain_bytes := build_plain_auth('', 'user', 'pass')
	result := kafka.detect_sasl_mechanism(plain_bytes)
	assert result == domain.SaslMechanism.plain
}

fn test_detect_sasl_mechanism_scram() {
	scram_bytes := 'n,,n=user,r=nonce123'.bytes()
	result := kafka.detect_sasl_mechanism(scram_bytes)
	assert result == domain.SaslMechanism.scram_sha_256
}

fn test_detect_sasl_mechanism_oauthbearer() {
	oauth_bytes := 'n,,\x01auth=Bearer token123\x01\x01'.bytes()
	result := kafka.detect_sasl_mechanism(oauth_bytes)
	assert result == domain.SaslMechanism.oauthbearer
}

fn test_detect_sasl_mechanism_empty() {
	result := kafka.detect_sasl_mechanism([]u8{})
	assert result == domain.SaslMechanism.plain
}

// -- SaslMockStorage: 최소한의 StoragePort 구현 --

struct SaslMockStorage {}

fn (m SaslMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m SaslMockStorage) delete_topic(name string) ! {}

fn (m SaslMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m SaslMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m SaslMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m SaslMockStorage) add_partitions(name string, new_count int) ! {}

fn (m SaslMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m SaslMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m SaslMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m SaslMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m SaslMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m SaslMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m SaslMockStorage) delete_group(group_id string) ! {}

fn (m SaslMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m SaslMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m SaslMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m SaslMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &SaslMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &SaslMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m SaslMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m SaslMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m SaslMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m SaslMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
