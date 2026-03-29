// Unit tests - ApiVersions handler 테스트
// handle_api_versions 핸들러와 new_api_versions_response를 검증한다.
module kafka_test

import domain
import infra.compression
import infra.protocol.kafka
import service.port

// -- 핸들러 생성 헬퍼 --

fn create_api_versions_handler() kafka.Handler {
	storage := ApiVersionsMockStorage{}
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	return kafka.new_handler(1, '127.0.0.1', 9092, 'test-cluster', storage, kafka.new_compression_port_adapter(cs))
}

// -- ApiVersions 핸들러 테스트 --

fn test_api_versions_handler_v0() {
	mut handler := create_api_versions_handler()

	// ApiVersions v0 (non-flexible)
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(18) // api_key = ApiVersions
	request.write_i16(0)
	request.write_i32(1)
	request.write_nullable_string('test-client')

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	cid := reader.read_i32()! // correlation_id
	assert cid == 1

	error_code := reader.read_i16()!
	assert error_code == 0

	// api_versions 배열
	count := reader.read_array_len()!
	assert count > 0
}

fn test_api_versions_handler_v3_flexible() {
	mut handler := create_api_versions_handler()

	// ApiVersions v3 (flexible) - 그러나 ApiVersions는 항상 non-flexible 헤더 사용
	mut request := kafka.new_writer()
	request.write_i32(0)
	request.write_i16(18)
	request.write_i16(3)
	request.write_i32(2)
	request.write_nullable_string('test-client')
	request.write_tagged_fields() // flexible header tagged fields

	// v3 body: client_software_name, client_software_version
	request.write_compact_string('datacore-test')
	request.write_compact_string('0.1.0')
	request.write_tagged_fields()

	mut conn := ?&domain.AuthConnection(none)
	response := handler.handle_request(request.bytes()[4..], mut conn) or {
		panic('요청 처리 실패: ${err}')
	}

	// ApiVersions 응답은 항상 non-flexible 헤더를 사용한다
	mut reader := kafka.new_reader(response)
	_ := reader.read_i32()! // size
	cid := reader.read_i32()!
	assert cid == 2

	error_code := reader.read_i16()!
	assert error_code == 0

	// v3는 compact array를 사용
	count := reader.read_uvarint()!
	real_count := int(count) - 1
	assert real_count > 0
}

// -- new_api_versions_response 테스트 --

fn test_new_api_versions_response_contains_all_apis() {
	resp := kafka.new_api_versions_response()
	assert resp.error_code == 0
	assert resp.throttle_time_ms == 0
	assert resp.api_versions.len > 0

	supported := kafka.get_supported_api_versions()
	assert resp.api_versions.len == supported.len
}

fn test_api_versions_response_includes_core_apis() {
	resp := kafka.new_api_versions_response()

	// Produce (API Key 0)가 포함되어야 한다
	mut has_produce := false
	// Fetch (API Key 1)가 포함되어야 한다
	mut has_fetch := false
	// Metadata (API Key 3)가 포함되어야 한다
	mut has_metadata := false
	// ApiVersions (API Key 18) 자체가 포함되어야 한다
	mut has_api_versions := false
	// InitProducerId (API Key 22)
	mut has_init_producer_id := false

	for v in resp.api_versions {
		match v.api_key {
			0 { has_produce = true }
			1 { has_fetch = true }
			3 { has_metadata = true }
			18 { has_api_versions = true }
			22 { has_init_producer_id = true }
			else {}
		}
	}

	assert has_produce
	assert has_fetch
	assert has_metadata
	assert has_api_versions
	assert has_init_producer_id
}

fn test_api_versions_response_version_ranges() {
	resp := kafka.new_api_versions_response()

	for v in resp.api_versions {
		// min_version은 항상 max_version 이하여야 한다
		assert v.min_version <= v.max_version
		// min_version은 0 이상이어야 한다
		assert v.min_version >= 0
	}
}

fn test_api_versions_response_encode_v0() {
	resp := kafka.ApiVersionsResponse{
		error_code:               0
		api_versions:             [
			kafka.ApiVersionsResponseKey{
				api_key:     0
				min_version: 0
				max_version: 9
			},
		]
		throttle_time_ms:         0
		supported_features:       []
		finalized_features_epoch: -1
		finalized_features:       []
	}

	bytes := resp.encode(0)
	mut reader := kafka.new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 0

	// v0은 non-flexible이므로 regular array
	count := reader.read_array_len()!
	assert count == 1

	api_key := reader.read_i16()!
	assert api_key == 0
	min_ver := reader.read_i16()!
	assert min_ver == 0
	max_ver := reader.read_i16()!
	assert max_ver == 9
}

fn test_api_versions_response_encode_v1_with_throttle() {
	resp := kafka.ApiVersionsResponse{
		error_code:               0
		api_versions:             [
			kafka.ApiVersionsResponseKey{
				api_key:     18
				min_version: 0
				max_version: 3
			},
		]
		throttle_time_ms:         100
		supported_features:       []
		finalized_features_epoch: -1
		finalized_features:       []
	}

	bytes := resp.encode(1)
	mut reader := kafka.new_reader(bytes)

	_ := reader.read_i16()! // error_code
	count := reader.read_array_len()!
	assert count == 1

	_ := reader.read_i16()! // api_key
	_ := reader.read_i16()! // min
	_ := reader.read_i16()! // max

	// v1+ 에서는 throttle_time_ms가 포함된다
	throttle := reader.read_i32()!
	assert throttle == 100
}

fn test_api_versions_response_encode_v3_flexible() {
	resp := kafka.ApiVersionsResponse{
		error_code:               0
		api_versions:             [
			kafka.ApiVersionsResponseKey{
				api_key:     18
				min_version: 0
				max_version: 3
			},
		]
		throttle_time_ms:         0
		supported_features:       []
		finalized_features_epoch: -1
		finalized_features:       []
	}

	bytes := resp.encode(3)
	mut reader := kafka.new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 0

	// v3은 compact array
	compact_len := reader.read_uvarint()!
	assert compact_len == 2 // 1개의 항목 + 1

	api_key := reader.read_i16()!
	assert api_key == 18
	min_ver := reader.read_i16()!
	assert min_ver == 0
	max_ver := reader.read_i16()!
	assert max_ver == 3

	// tagged fields (per entry)
	tags := reader.read_uvarint()!
	assert tags == 0

	throttle := reader.read_i32()!
	assert throttle == 0

	// tagged fields (body)
	body_tags := reader.read_uvarint()!
	assert body_tags == 0
}

// -- ApiVersionsMockStorage --

struct ApiVersionsMockStorage {}

fn (m ApiVersionsMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return domain.TopicMetadata{}
}

fn (m ApiVersionsMockStorage) delete_topic(name string) ! {}

fn (m ApiVersionsMockStorage) list_topics() ![]domain.TopicMetadata {
	return []domain.TopicMetadata{}
}

fn (m ApiVersionsMockStorage) get_topic(name string) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m ApiVersionsMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (m ApiVersionsMockStorage) add_partitions(name string, new_count int) ! {}

fn (m ApiVersionsMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	return domain.AppendResult{}
}

fn (m ApiVersionsMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (m ApiVersionsMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (m ApiVersionsMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	return domain.PartitionInfo{}
}

fn (m ApiVersionsMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (m ApiVersionsMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('group not found')
}

fn (m ApiVersionsMockStorage) delete_group(group_id string) ! {}

fn (m ApiVersionsMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (m ApiVersionsMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (m ApiVersionsMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (m ApiVersionsMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (m &ApiVersionsMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (m &ApiVersionsMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (m ApiVersionsMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (m ApiVersionsMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (m ApiVersionsMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (m ApiVersionsMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}
