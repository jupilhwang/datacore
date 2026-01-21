// Infra Layer - PostgreSQL Storage Adapter Tests
// Integration tests requiring a running PostgreSQL instance
module postgres

import domain
import os
import time

// Test configuration - set environment variables to run tests
// DATACORE_PG_HOST, DATACORE_PG_PORT, DATACORE_PG_USER, DATACORE_PG_PASSWORD, DATACORE_PG_DATABASE
fn get_test_config() ?PostgresConfig {
	// Skip tests if PostgreSQL is not configured
	host := os.getenv_opt('DATACORE_PG_HOST') or { return none }
	port_str := os.getenv_opt('DATACORE_PG_PORT') or { '5432' }
	user := os.getenv_opt('DATACORE_PG_USER') or { return none }
	password := os.getenv_opt('DATACORE_PG_PASSWORD') or { '' }
	database := os.getenv_opt('DATACORE_PG_DATABASE') or { 'datacore_test' }

	return PostgresConfig{
		host:      host
		port:      port_str.int()
		user:      user
		password:  password
		database:  database
		pool_size: 5
	}
}

fn test_postgres_adapter_creation() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests - environment not configured')
		return
	}

	adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}

	assert adapter.initialized == true
	adapter.close()
}

fn test_topic_lifecycle() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	topic_name := 'test-topic-${time.now().unix_milli()}'

	// Create topic
	metadata := adapter.create_topic(topic_name, 3, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic: ${err}'
		return
	}

	assert metadata.name == topic_name
	assert metadata.partition_count == 3
	assert metadata.topic_id.len == 16

	// Get topic
	retrieved := adapter.get_topic(topic_name) or {
		assert false, 'Failed to get topic: ${err}'
		return
	}

	assert retrieved.name == topic_name
	assert retrieved.partition_count == 3

	// Get topic by ID
	by_id := adapter.get_topic_by_id(metadata.topic_id) or {
		assert false, 'Failed to get topic by ID: ${err}'
		return
	}

	assert by_id.name == topic_name

	// List topics
	topics := adapter.list_topics() or {
		assert false, 'Failed to list topics: ${err}'
		return
	}

	mut found := false
	for t in topics {
		if t.name == topic_name {
			found = true
			break
		}
	}
	assert found == true

	// Add partitions
	adapter.add_partitions(topic_name, 5) or {
		assert false, 'Failed to add partitions: ${err}'
		return
	}

	updated := adapter.get_topic(topic_name) or {
		assert false, 'Failed to get updated topic: ${err}'
		return
	}
	assert updated.partition_count == 5

	// Delete topic
	adapter.delete_topic(topic_name) or {
		assert false, 'Failed to delete topic: ${err}'
		return
	}

	// Verify deletion
	_ := adapter.get_topic(topic_name) or {
		assert err.msg() == 'topic not found'
		return
	}
	assert false, 'Topic should have been deleted'
}

fn test_record_operations() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	topic_name := 'test-records-${time.now().unix_milli()}'

	// Create topic
	adapter.create_topic(topic_name, 2, domain.TopicConfig{}) or {
		assert false, 'Failed to create topic: ${err}'
		return
	}
	defer { adapter.delete_topic(topic_name) or {} }

	// Append records
	records := [
		domain.Record{
			key:     'key1'.bytes()
			value:   'value1'.bytes()
			headers: map[string][]u8{}
		},
		domain.Record{
			key:     'key2'.bytes()
			value:   'value2'.bytes()
			headers: map[string][]u8{}
		},
	]

	result := adapter.append(topic_name, 0, records) or {
		assert false, 'Failed to append records: ${err}'
		return
	}

	assert result.base_offset == 0
	assert result.record_count == 2

	// Fetch records
	fetch_result := adapter.fetch(topic_name, 0, 0, 1024 * 1024) or {
		assert false, 'Failed to fetch records: ${err}'
		return
	}

	assert fetch_result.records.len == 2
	assert fetch_result.high_watermark == 2
	assert fetch_result.records[0].key == 'key1'.bytes()
	assert fetch_result.records[1].value == 'value2'.bytes()

	// Get partition info
	info := adapter.get_partition_info(topic_name, 0) or {
		assert false, 'Failed to get partition info: ${err}'
		return
	}

	assert info.earliest_offset == 0
	assert info.latest_offset == 2
	assert info.high_watermark == 2

	// Delete records
	adapter.delete_records(topic_name, 0, 1) or {
		assert false, 'Failed to delete records: ${err}'
		return
	}

	// Verify deletion
	info_after := adapter.get_partition_info(topic_name, 0) or {
		assert false, 'Failed to get partition info after delete: ${err}'
		return
	}

	assert info_after.earliest_offset == 1
}

fn test_consumer_group_operations() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	group_id := 'test-group-${time.now().unix_milli()}'

	// Save group
	group := domain.ConsumerGroup{
		group_id:      group_id
		protocol_type: 'consumer'
		state:         .stable
		generation_id: 1
		leader:        'member-1'
		protocol:      'range'
		members:       []domain.GroupMember{}
	}

	adapter.save_group(group) or {
		assert false, 'Failed to save group: ${err}'
		return
	}

	// Load group
	loaded := adapter.load_group(group_id) or {
		assert false, 'Failed to load group: ${err}'
		return
	}

	assert loaded.group_id == group_id
	assert loaded.protocol_type == 'consumer'
	assert loaded.state == .stable

	// List groups
	groups := adapter.list_groups() or {
		assert false, 'Failed to list groups: ${err}'
		return
	}

	mut found := false
	for g in groups {
		if g.group_id == group_id {
			found = true
			assert g.state == 'Stable'
			break
		}
	}
	assert found == true

	// Delete group
	adapter.delete_group(group_id) or {
		assert false, 'Failed to delete group: ${err}'
		return
	}

	// Verify deletion
	_ := adapter.load_group(group_id) or {
		assert err.msg() == 'group not found'
		return
	}
	assert false, 'Group should have been deleted'
}

fn test_offset_commit_fetch() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	group_id := 'test-offsets-${time.now().unix_milli()}'
	topic_name := 'test-topic-offsets'

	// Create group first
	group := domain.ConsumerGroup{
		group_id:      group_id
		protocol_type: 'consumer'
		state:         .stable
		generation_id: 1
		leader:        'member-1'
		protocol:      'range'
		members:       []domain.GroupMember{}
	}
	adapter.save_group(group) or {}
	defer { adapter.delete_group(group_id) or {} }

	// Commit offsets
	offsets := [
		domain.PartitionOffset{
			topic:     topic_name
			partition: 0
			offset:    100
			metadata:  'test-metadata'
		},
		domain.PartitionOffset{
			topic:     topic_name
			partition: 1
			offset:    200
			metadata:  ''
		},
	]

	adapter.commit_offsets(group_id, offsets) or {
		assert false, 'Failed to commit offsets: ${err}'
		return
	}

	// Fetch offsets
	partitions := [
		domain.TopicPartition{
			topic:     topic_name
			partition: 0
		},
		domain.TopicPartition{
			topic:     topic_name
			partition: 1
		},
		domain.TopicPartition{
			topic:     topic_name
			partition: 2
		},
	]

	results := adapter.fetch_offsets(group_id, partitions) or {
		assert false, 'Failed to fetch offsets: ${err}'
		return
	}

	assert results.len == 3
	assert results[0].offset == 100
	assert results[0].metadata == 'test-metadata'
	assert results[1].offset == 200
	assert results[2].offset == -1 // Not committed
}

fn test_health_check() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	status := adapter.health_check() or {
		assert false, 'Health check failed: ${err}'
		return
	}

	assert status == .healthy
}

fn test_storage_capability() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	capability := adapter.get_storage_capability()

	assert capability.name == 'postgresql'
	assert capability.supports_multi_broker == true
	assert capability.supports_transactions == true
	assert capability.is_persistent == true
	assert capability.is_distributed == true
}

fn test_cluster_metadata_port() {
	config := get_test_config() or {
		println('Skipping PostgreSQL tests')
		return
	}

	mut adapter := new_postgres_adapter(config) or {
		assert false, 'Failed to create adapter: ${err}'
		return
	}
	defer { adapter.close() }

	_ := adapter.get_cluster_metadata_port() or {
		assert false, 'ClusterMetadataPort should be available for PostgreSQL'
		return
	}

	// ClusterMetadataPort is available
	assert true
}
