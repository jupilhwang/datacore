// gRPC Service 테스트
module streaming

import domain
import service.port

// 테스트용 Mock storage
struct MockStorage {
mut:
	topics     map[string]domain.TopicMetadata
	partitions map[string][]domain.Record
	offsets    map[string]i64
	groups     map[string]domain.ConsumerGroup
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	if name in s.topics {
		return error('topic already exists')
	}
	// Generate a proper 16-byte topic_id
	mut topic_id := []u8{len: 16}
	name_bytes := name.bytes()
	for i in 0 .. 16 {
		if i < name_bytes.len {
			topic_id[i] = name_bytes[i]
		} else {
			topic_id[i] = u8(i)
		}
	}
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
	}
	s.topics[name] = meta
	for i in 0 .. partitions {
		s.partitions['${name}:${i}'] = []domain.Record{}
	}
	return meta
}

fn (mut s MockStorage) delete_topic(name string) ! {
	if name !in s.topics {
		return error('topic not found')
	}
	s.topics.delete(name)
}

fn (mut s MockStorage) list_topics() ![]domain.TopicMetadata {
	return s.topics.values()
}

fn (mut s MockStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (mut s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	for _, t in s.topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('topic not found')
}

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {
	return error('not implemented')
}

fn (mut s MockStorage) append(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
	key := '${topic_name}:${partition}'
	if key !in s.partitions {
		return error('partition not found')
	}
	base_offset := s.offsets[key] or { 0 }
	s.partitions[key] << records
	s.offsets[key] = base_offset + records.len
	return domain.AppendResult{
		base_offset:  base_offset
		record_count: records.len
	}
}

fn (mut s MockStorage) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	key := '${topic_name}:${partition}'
	records := s.partitions[key] or { return error('partition not found') }
	high_watermark := s.offsets[key] or { 0 }

	start := int(offset)
	if start >= records.len {
		return domain.FetchResult{
			records:        []
			high_watermark: high_watermark
		}
	}

	end := if start + 100 > records.len { records.len } else { start + 100 }
	return domain.FetchResult{
		records:        records[start..end]
		high_watermark: high_watermark
	}
}

fn (mut s MockStorage) delete_records(topic_name string, partition int, before_offset i64) ! {
	return error('not implemented')
}

fn (mut s MockStorage) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	key := '${topic_name}:${partition}'
	if key !in s.partitions {
		return error('partition not found')
	}
	hwm := s.offsets[key] or { 0 }
	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: 0
		latest_offset:   hwm
		high_watermark:  hwm
	}
}

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {
	s.groups[group.group_id] = group
}

fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return s.groups[group_id] or { return error('group not found') }
}

fn (mut s MockStorage) delete_group(group_id string) ! {
	s.groups.delete(group_id)
}

fn (mut s MockStorage) list_groups() ![]domain.GroupInfo {
	mut result := []domain.GroupInfo{}
	for _, g in s.groups {
		result << domain.GroupInfo{
			group_id: g.group_id
		}
	}
	return result
}

fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	for o in offsets {
		s.offsets['${group_id}:${o.topic}:${o.partition}'] = o.offset
	}
}

fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut results := []domain.OffsetFetchResult{}
	for p in partitions {
		key := '${group_id}:${p.topic}:${p.partition}'
		offset := s.offsets[key] or { -1 }
		results << domain.OffsetFetchResult{
			topic:     p.topic
			partition: p.partition
			offset:    offset
		}
	}
	return results
}

fn (mut s MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &MockStorage) get_storage_capability() domain.StorageCapability {
	return domain.StorageCapability{
		name: 'mock'
	}
}

fn (s &MockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn new_mock_storage() &MockStorage {
	return &MockStorage{
		topics:     map[string]domain.TopicMetadata{}
		partitions: map[string][]domain.Record{}
		offsets:    map[string]i64{}
		groups:     map[string]domain.ConsumerGroup{}
	}
}

// ============================================================================
// Tests
// ============================================================================

fn test_grpc_service_create() {
	mut storage := new_mock_storage()
	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	assert service.running == true
	stats := service.get_stats()
	assert stats.active_connections == 0
}

fn test_grpc_service_register_connection() {
	mut storage := new_mock_storage()
	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	assert conn_id == conn.id

	stats := service.get_stats()
	assert stats.active_connections == 1
	assert stats.connections_created == 1
}

fn test_grpc_service_produce() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 4, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic'
		return
	}

	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	// Register connection
	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	// Produce records
	req := domain.GrpcProduceRequest{
		topic:     'test-topic'
		partition: 0
		records:   [
			domain.GrpcRecord{
				key:   'key1'.bytes()
				value: 'value1'.bytes()
			},
			domain.GrpcRecord{
				key:   'key2'.bytes()
				value: 'value2'.bytes()
			},
		]
	}

	result := service.produce(conn_id, req)

	assert result.error_code == 0
	assert result.topic == 'test-topic'
	assert result.partition == 0
	assert result.base_offset == 0
	assert result.record_count == 2

	stats := service.get_stats()
	assert stats.produce_requests == 1
	assert stats.messages_produced == 2
}

fn test_grpc_service_produce_unknown_topic() {
	mut storage := new_mock_storage()
	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	req := domain.GrpcProduceRequest{
		topic:     'nonexistent-topic'
		partition: 0
		records:   [
			domain.GrpcRecord{
				value: 'test'.bytes()
			},
		]
	}

	result := service.produce(conn_id, req)

	assert result.error_code == domain.grpc_error_unknown_topic
}

fn test_grpc_service_subscribe() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 4, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic'
		return
	}

	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .server_streaming)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	req := domain.GrpcConsumeRequest{
		topic:       'test-topic'
		partition:   0
		offset:      0
		max_records: 100
		max_bytes:   1048576
	}

	result := service.subscribe(conn_id, req) or {
		assert false, 'Failed to subscribe: ${err}'
		return
	}

	assert result.error_code == 0
	assert result.topic == 'test-topic'
	assert result.partition == 0

	stats := service.get_stats()
	assert stats.total_subscriptions == 1
	assert stats.consume_requests == 1
}

fn test_grpc_service_fetch_messages() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 4, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic'
		return
	}

	// Add some records
	storage.append('test-topic', 0, [
		domain.Record{
			key:   'k1'.bytes()
			value: 'v1'.bytes()
		},
		domain.Record{
			key:   'k2'.bytes()
			value: 'v2'.bytes()
		},
	]) or {
		assert false, 'Failed to append records'
		return
	}

	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .server_streaming)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	req := domain.GrpcConsumeRequest{
		topic:       'test-topic'
		partition:   0
		offset:      0
		max_records: 100
		max_bytes:   1048576
	}

	result := service.fetch_messages(conn_id, req)

	assert result.error_code == 0
	assert result.records.len == 2
	assert result.records[0].key == 'k1'.bytes()
	assert result.records[0].value == 'v1'.bytes()
	assert result.next_offset == 2
}

fn test_grpc_service_bidirectional_stream() {
	mut storage := new_mock_storage()
	storage.create_topic('test-topic', 4, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic'
		return
	}

	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	// Test produce request
	produce_req := domain.GrpcStreamRequest{
		request_type: .produce
		produce:      domain.GrpcProduceRequest{
			topic:     'test-topic'
			partition: 0
			records:   [
				domain.GrpcRecord{
					value: 'test-message'.bytes()
				},
			]
		}
	}

	response := service.handle_stream_request(conn_id, produce_req)
	assert response.response_type == .produce_ack

	if produce_resp := response.produce {
		assert produce_resp.error_code == 0
		assert produce_resp.record_count == 1
	} else {
		assert false, 'Missing produce response'
	}
}

fn test_grpc_service_ping() {
	mut storage := new_mock_storage()
	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	ping_req := domain.GrpcStreamRequest{
		request_type: .ping
	}

	response := service.handle_stream_request(conn_id, ping_req)
	assert response.response_type == .pong

	if pong := response.pong {
		assert pong.timestamp > 0
	} else {
		assert false, 'Missing pong response'
	}
}

fn test_grpc_service_unregister_connection() {
	mut storage := new_mock_storage()
	config := domain.default_grpc_config()
	mut service := new_grpc_service(storage, config)

	conn := domain.new_grpc_connection('127.0.0.1', .bidirectional)
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	assert service.get_stats().active_connections == 1

	service.unregister_connection(conn_id) or {
		assert false, 'Failed to unregister connection'
		return
	}

	assert service.get_stats().active_connections == 0
	assert service.get_stats().connections_closed == 1
}

fn test_grpc_domain_record_encode_decode() {
	record := domain.GrpcRecord{
		key:       'test-key'.bytes()
		value:     'test-value'.bytes()
		timestamp: 1234567890123
		headers:   {
			'header1': 'value1'.bytes()
			'header2': 'value2'.bytes()
		}
	}

	encoded := record.encode()
	decoded := domain.decode_grpc_record(encoded) or {
		assert false, 'Failed to decode record: ${err}'
		return
	}

	assert decoded.key == record.key
	assert decoded.value == record.value
	assert decoded.timestamp == record.timestamp
	assert decoded.headers.len == record.headers.len
}

fn test_grpc_error_messages() {
	assert domain.grpc_error_message(domain.grpc_error_none) == 'No error'
	assert domain.grpc_error_message(domain.grpc_error_unknown_topic) == 'Unknown topic'
	assert domain.grpc_error_message(domain.grpc_error_invalid_partition) == 'Invalid partition'
	assert domain.grpc_error_message(domain.grpc_error_message_too_large) == 'Message too large'
}
