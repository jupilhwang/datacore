// SSE Service 테스트
module streaming

import domain
import service.port

// ============================================================================
// 테스트용 Mock Storage
// ============================================================================

struct MockStorage {
mut:
	topics     map[string]domain.TopicMetadata
	partitions map[string]domain.PartitionInfo
	records    map[string][]domain.Record
}

fn new_mock_storage() &MockStorage {
	mut storage := &MockStorage{
		topics:     map[string]domain.TopicMetadata{}
		partitions: map[string]domain.PartitionInfo{}
		records:    map[string][]domain.Record{}
	}

	// Add test topic
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 1
	}

	storage.partitions['test-topic:0'] = domain.PartitionInfo{
		topic:           'test-topic'
		partition:       0
		earliest_offset: 0
		latest_offset:   0
	}

	return storage
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		partition_count: partitions
	}
	s.topics[name] = meta
	return meta
}

fn (mut s MockStorage) delete_topic(name string) ! {
	s.topics.delete(name)
}

fn (mut s MockStorage) list_topics() ![]domain.TopicMetadata {
	return s.topics.values()
}

fn (mut s MockStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('Topic not found') }
}

fn (mut s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('Not implemented')
}

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {
}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	key := '${topic}:${partition}'
	if key !in s.records {
		s.records[key] = []domain.Record{}
	}
	s.records[key] << records

	// Update partition info
	if info := s.partitions[key] {
		s.partitions[key] = domain.PartitionInfo{
			...info
			latest_offset: info.latest_offset + records.len
		}
	}

	return domain.AppendResult{
		base_offset:  0
		record_count: records.len
	}
}

fn (mut s MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	key := '${topic}:${partition}'
	records := s.records[key] or { []domain.Record{} }

	// Filter records by offset
	mut result := []domain.Record{}
	for i, record in records {
		if i64(i) >= offset {
			result << record
		}
	}

	return domain.FetchResult{
		records:        result
		high_watermark: i64(records.len)
	}
}

fn (mut s MockStorage) delete_records(topic string, partition int, before_offset i64) ! {
}

fn (mut s MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	key := '${topic}:${partition}'
	return s.partitions[key] or { return error('Partition not found') }
}

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {
}

fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('Not implemented')
}

fn (mut s MockStorage) delete_group(group_id string) ! {
}

fn (mut s MockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
}

fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (mut s MockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (mut s MockStorage) get_storage_capability() domain.StorageCapability {
	return domain.StorageCapability{}
}

fn (mut s MockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// ============================================================================
// Tests
// ============================================================================

fn test_new_sse_service() {
	storage := new_mock_storage()
	config := domain.default_sse_config()

	mut service := new_sse_service(storage, config)

	assert service.running == true
	stats := service.get_stats()
	assert stats.active_connections == 0
}

fn test_register_connection() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	assert conn_id == conn.id

	stats := service.get_stats()
	assert stats.active_connections == 1
	assert stats.connections_created == 1
}

fn test_unregister_connection() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	service.unregister_connection(conn_id) or {
		assert false, 'Failed to unregister connection'
		return
	}

	stats := service.get_stats()
	assert stats.active_connections == 0
	assert stats.connections_closed == 1
}

fn test_get_connection() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	retrieved := service.get_connection(conn_id) or {
		assert false, 'Failed to get connection'
		return
	}

	assert retrieved.id == conn_id
	assert retrieved.client_ip == '192.168.1.1'
	assert retrieved.state == .connected
}

fn test_list_connections() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	// Register multiple connections
	conn1 := domain.new_sse_connection('192.168.1.1', 'Client 1')
	conn2 := domain.new_sse_connection('192.168.1.2', 'Client 2')

	service.register_connection(conn1) or {}
	service.register_connection(conn2) or {}

	connections := service.list_connections()
	assert connections.len == 2
}

fn test_subscribe() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	sub := domain.new_subscription('test-topic', i32(0), .earliest, 0, none, 'test-client')
	service.subscribe(conn_id, sub) or {
		assert false, 'Failed to subscribe: ${err}'
		return
	}

	subs := service.get_subscriptions(conn_id)
	assert subs.len == 1
	assert subs[0].topic == 'test-topic'
}

fn test_subscribe_invalid_topic() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	sub := domain.new_subscription('non-existent-topic', i32(0), .earliest, 0, none, 'test-client')
	service.subscribe(conn_id, sub) or {
		// Expected error
		assert err.msg().contains('Topic not found')
		return
	}

	assert false, 'Should have failed for non-existent topic'
}

fn test_unsubscribe() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	sub := domain.new_subscription('test-topic', i32(0), .earliest, 0, none, 'test-client')
	service.subscribe(conn_id, sub) or {
		assert false, 'Failed to subscribe'
		return
	}

	service.unsubscribe(conn_id, 'test-topic', i32(0)) or {
		assert false, 'Failed to unsubscribe'
		return
	}

	subs := service.get_subscriptions(conn_id)
	assert subs.len == 0
}

fn test_max_connections_limit() {
	storage := new_mock_storage()
	config := domain.SSEConfig{
		...domain.default_sse_config()
		max_connections: 2
	}
	mut service := new_sse_service(storage, config)

	// Register max connections
	conn1 := domain.new_sse_connection('192.168.1.1', 'Client 1')
	conn2 := domain.new_sse_connection('192.168.1.2', 'Client 2')
	conn3 := domain.new_sse_connection('192.168.1.3', 'Client 3')

	service.register_connection(conn1) or {}
	service.register_connection(conn2) or {}

	// Third connection should fail
	service.register_connection(conn3) or {
		assert err.msg().contains('Maximum connections')
		return
	}

	assert false, 'Should have failed for max connections'
}

fn test_max_subscriptions_limit() {
	storage := new_mock_storage()
	config := domain.SSEConfig{
		...domain.default_sse_config()
		max_subscriptions: 1
	}
	mut service := new_sse_service(storage, config)

	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	conn_id := service.register_connection(conn) or {
		assert false, 'Failed to register connection'
		return
	}

	sub1 := domain.new_subscription('test-topic', i32(0), .earliest, 0, none, 'test-client')
	service.subscribe(conn_id, sub1) or {
		assert false, 'Failed to subscribe'
		return
	}

	// Second subscription should fail
	sub2 := domain.new_subscription('test-topic', i32(0), .latest, 0, none, 'test-client')
	service.subscribe(conn_id, sub2) or {
		assert err.msg().contains('Maximum subscriptions')
		return
	}

	assert false, 'Should have failed for max subscriptions'
}

fn test_get_stats() {
	storage := new_mock_storage()
	config := domain.default_sse_config()
	mut service := new_sse_service(storage, config)

	// Initial stats
	stats := service.get_stats()
	assert stats.active_connections == 0
	assert stats.total_subscriptions == 0
	assert stats.messages_sent == 0
	assert stats.connections_created == 0
	assert stats.connections_closed == 0

	// After registering connection
	conn := domain.new_sse_connection('192.168.1.1', 'Test Client')
	service.register_connection(conn) or {}

	stats2 := service.get_stats()
	assert stats2.active_connections == 1
	assert stats2.connections_created == 1
}
