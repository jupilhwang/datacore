// Unit Tests - Memory Storage Adapter
module memory

import domain
import service.port
import time

// ============================================================
// Topic Tests
// ============================================================

fn test_create_topic() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('test-topic', 3, domain.TopicConfig{})!
    
    metadata := adapter.get_topic('test-topic')!
    assert metadata.name == 'test-topic'
    assert metadata.partition_count == 3
    assert metadata.is_internal == false
}

fn test_create_internal_topic() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('__schemas', 1, domain.TopicConfig{})!
    
    metadata := adapter.get_topic('__schemas')!
    assert metadata.is_internal == true
}

fn test_create_duplicate_topic() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('test-topic', 3, domain.TopicConfig{})!
    
    // Should fail
    if _ := adapter.create_topic('test-topic', 3, domain.TopicConfig{}) {
        assert false, 'Expected error for duplicate topic'
    }
}

fn test_delete_topic() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('test-topic', 3, domain.TopicConfig{})!
    adapter.delete_topic('test-topic')!
    
    // Should not find topic
    if _ := adapter.get_topic('test-topic') {
        assert false, 'Expected error for deleted topic'
    }
}

fn test_list_topics() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('topic-1', 1, domain.TopicConfig{})!
    adapter.create_topic('topic-2', 2, domain.TopicConfig{})!
    adapter.create_topic('topic-3', 3, domain.TopicConfig{})!
    
    topics := adapter.list_topics()!
    assert topics.len == 3
}

fn test_add_partitions() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('test-topic', 2, domain.TopicConfig{})!
    adapter.add_partitions('test-topic', 5)!
    
    metadata := adapter.get_topic('test-topic')!
    assert metadata.partition_count == 5
}

// ============================================================
// Record Tests
// ============================================================

fn test_append_records() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 3, domain.TopicConfig{})!
    
    records := [
        domain.Record{
            key: 'key1'.bytes()
            value: 'value1'.bytes()
            timestamp: time.now()
        },
        domain.Record{
            key: 'key2'.bytes()
            value: 'value2'.bytes()
            timestamp: time.now()
        },
    ]
    
    result := adapter.append('test-topic', 0, records)!
    
    assert result.base_offset == 0
    assert result.record_count == 2
}

fn test_fetch_records() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 1, domain.TopicConfig{})!
    
    // Append some records
    for i in 0 .. 10 {
        adapter.append('test-topic', 0, [
            domain.Record{
                key: 'key${i}'.bytes()
                value: 'value${i}'.bytes()
                timestamp: time.now()
            },
        ])!
    }
    
    // Fetch from offset 5
    result := adapter.fetch('test-topic', 0, 5, 1048576)!
    
    assert result.records.len == 5
    assert result.high_watermark == 10
    assert result.log_start_offset == 0
}

fn test_fetch_empty_partition() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 1, domain.TopicConfig{})!
    
    result := adapter.fetch('test-topic', 0, 0, 1048576)!
    
    assert result.records.len == 0
    assert result.high_watermark == 0
}

fn test_fetch_out_of_range() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 1, domain.TopicConfig{})!
    
    adapter.append('test-topic', 0, [
        domain.Record{ key: 'key'.bytes(), value: 'value'.bytes(), timestamp: time.now() },
    ])!
    
    // Fetch from offset beyond high watermark
    result := adapter.fetch('test-topic', 0, 100, 1048576)!
    assert result.records.len == 0
}

fn test_delete_records() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 1, domain.TopicConfig{})!
    
    // Append 10 records
    for i in 0 .. 10 {
        adapter.append('test-topic', 0, [
            domain.Record{ key: 'key${i}'.bytes(), value: 'value${i}'.bytes(), timestamp: time.now() },
        ])!
    }
    
    // Delete first 5 records
    adapter.delete_records('test-topic', 0, 5)!
    
    info := adapter.get_partition_info('test-topic', 0)!
    assert info.earliest_offset == 5
    assert info.high_watermark == 10
}

// ============================================================
// Partition Info Tests
// ============================================================

fn test_get_partition_info() {
    mut adapter := new_memory_adapter()
    adapter.create_topic('test-topic', 3, domain.TopicConfig{})!
    
    // Append to partition 1
    for i in 0 .. 5 {
        adapter.append('test-topic', 1, [
            domain.Record{ key: 'key${i}'.bytes(), value: 'value${i}'.bytes(), timestamp: time.now() },
        ])!
    }
    
    info := adapter.get_partition_info('test-topic', 1)!
    
    assert info.topic == 'test-topic'
    assert info.partition == 1
    assert info.earliest_offset == 0
    assert info.high_watermark == 5
}

// ============================================================
// Consumer Group Tests
// ============================================================

fn test_save_and_load_group() {
    mut adapter := new_memory_adapter()
    
    group := domain.ConsumerGroup{
        group_id: 'test-group'
        generation_id: 1
        protocol_type: 'consumer'
        protocol: 'range'
        leader: 'member-1'
        state: .stable
        members: []
    }
    
    adapter.save_group(group)!
    
    loaded := adapter.load_group('test-group')!
    assert loaded.group_id == 'test-group'
    assert loaded.generation_id == 1
    assert loaded.state == .stable
}

fn test_list_groups() {
    mut adapter := new_memory_adapter()
    
    adapter.save_group(domain.ConsumerGroup{
        group_id: 'group-1'
        state: .stable
    })!
    adapter.save_group(domain.ConsumerGroup{
        group_id: 'group-2'
        state: .empty
    })!
    
    groups := adapter.list_groups()!
    assert groups.len == 2
}

fn test_delete_group() {
    mut adapter := new_memory_adapter()
    
    adapter.save_group(domain.ConsumerGroup{
        group_id: 'test-group'
        state: .stable
    })!
    
    adapter.delete_group('test-group')!
    
    if _ := adapter.load_group('test-group') {
        assert false, 'Expected error for deleted group'
    }
}

// ============================================================
// Offset Tests
// ============================================================

fn test_commit_and_fetch_offsets() {
    mut adapter := new_memory_adapter()
    
    offsets := [
        domain.PartitionOffset{
            topic: 'topic-1'
            partition: 0
            offset: 100
        },
        domain.PartitionOffset{
            topic: 'topic-1'
            partition: 1
            offset: 200
        },
    ]
    
    adapter.commit_offsets('test-group', offsets)!
    
    partitions := [
        domain.TopicPartition{ topic: 'topic-1', partition: 0 },
        domain.TopicPartition{ topic: 'topic-1', partition: 1 },
        domain.TopicPartition{ topic: 'topic-1', partition: 2 },  // Not committed
    ]
    
    results := adapter.fetch_offsets('test-group', partitions)!
    
    assert results.len == 3
    assert results[0].offset == 100
    assert results[1].offset == 200
    assert results[2].offset == -1  // Not committed
}

fn test_fetch_offsets_unknown_group() {
    mut adapter := new_memory_adapter()
    
    partitions := [
        domain.TopicPartition{ topic: 'topic-1', partition: 0 },
    ]
    
    results := adapter.fetch_offsets('unknown-group', partitions)!
    
    assert results.len == 1
    assert results[0].offset == -1
}

// ============================================================
// Retention Tests
// ============================================================

fn test_max_messages_retention() {
    config := MemoryConfig{
        max_messages_per_partition: 5
    }
    mut adapter := new_memory_adapter_with_config(config)
    adapter.create_topic('test-topic', 1, domain.TopicConfig{})!
    
    // Append 10 records
    for i in 0 .. 10 {
        adapter.append('test-topic', 0, [
            domain.Record{ key: 'key${i}'.bytes(), value: 'value${i}'.bytes(), timestamp: time.now() },
        ])!
    }
    
    // Should only have last 5 records
    info := adapter.get_partition_info('test-topic', 0)!
    assert info.earliest_offset == 5
    assert info.high_watermark == 10
    
    // Fetch should start from offset 5
    result := adapter.fetch('test-topic', 0, 0, 1048576)!
    assert result.records.len == 0  // Offset 0-4 deleted
    
    result2 := adapter.fetch('test-topic', 0, 5, 1048576)!
    assert result2.records.len == 5
}

// ============================================================
// Stats Tests
// ============================================================

fn test_get_stats() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('topic-1', 2, domain.TopicConfig{})!
    adapter.create_topic('topic-2', 3, domain.TopicConfig{})!
    
    for i in 0 .. 5 {
        adapter.append('topic-1', 0, [
            domain.Record{ key: 'key'.bytes(), value: 'value'.bytes(), timestamp: time.now() },
        ])!
    }
    
    adapter.save_group(domain.ConsumerGroup{ group_id: 'group-1' })!
    
    stats := adapter.get_stats()
    
    assert stats.topic_count == 2
    assert stats.total_partitions == 5
    assert stats.total_records == 5
    assert stats.group_count == 1
}

// ============================================================
// Clear Tests
// ============================================================

fn test_clear() {
    mut adapter := new_memory_adapter()
    
    adapter.create_topic('topic-1', 1, domain.TopicConfig{})!
    adapter.save_group(domain.ConsumerGroup{ group_id: 'group-1' })!
    
    adapter.clear()
    
    topics := adapter.list_topics()!
    groups := adapter.list_groups()!
    
    assert topics.len == 0
    assert groups.len == 0
}

// ============================================================
// Health Check
// ============================================================

fn test_health_check() {
    mut adapter := new_memory_adapter()
    
    status := adapter.health_check()!
    assert status == .healthy
}
