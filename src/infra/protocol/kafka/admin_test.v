// Unit tests for Kafka Admin API (CreateTopics, DeleteTopics)
module kafka

import domain
import service.port

// Mock Storage for testing
struct MockStorage {
mut:
    topics       map[string]domain.TopicMetadata
    call_count   int
    fail_create  bool
    fail_delete  bool
}

fn new_mock_storage() &MockStorage {
    return &MockStorage{
        topics: map[string]domain.TopicMetadata{}
    }
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
    s.call_count += 1
    if s.fail_create {
        return error('mock create error')
    }
    if name in s.topics {
        return error('topic already exists')
    }
    meta := domain.TopicMetadata{
        name: name
        partition_count: partitions
        config: map[string]string{}
        is_internal: false
    }
    s.topics[name] = meta
    return meta
}

fn (mut s MockStorage) delete_topic(name string) ! {
    s.call_count += 1
    if s.fail_delete {
        return error('mock delete error')
    }
    if name !in s.topics {
        return error('topic not found')
    }
    if name.starts_with('__') {
        return error('cannot delete internal topic')
    }
    s.topics.delete(name)
}

fn (mut s MockStorage) list_topics() ![]domain.TopicMetadata {
    mut result := []domain.TopicMetadata{}
    for _, t in s.topics {
        result << t
    }
    return result
}

fn (mut s MockStorage) get_topic(name string) !domain.TopicMetadata {
    return s.topics[name] or { return error('topic not found') }
}

fn (mut s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
    return error('topic not found')
}

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {
    return error('not implemented')
}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
    return domain.AppendResult{}
}

fn (mut s MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
    return domain.FetchResult{}
}

fn (mut s MockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (mut s MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
    return domain.PartitionInfo{}
}

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup {
    return error('not found')
}

fn (mut s MockStorage) delete_group(group_id string) ! {}

fn (mut s MockStorage) list_groups() ![]domain.GroupInfo {
    return []domain.GroupInfo{}
}

fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
    return []domain.OffsetFetchResult{}
}

fn (mut s MockStorage) health_check() !port.HealthStatus {
    return .healthy
}

// Test CreateTopics Request Parsing
fn test_parse_create_topics_request() {
    // Build a minimal CreateTopics request (v0)
    // [topics array][timeout_ms]
    mut writer := new_writer()
    
    // Array length: 1
    writer.write_i32(1)
    
    // Topic name
    writer.write_string('test-topic')
    
    // num_partitions
    writer.write_i32(3)
    
    // replication_factor
    writer.write_i16(1)
    
    // replica_assignment array (empty)
    writer.write_i32(0)
    
    // configs array (empty)
    writer.write_i32(0)
    
    // timeout_ms
    writer.write_i32(30000)
    
    mut reader := new_reader(writer.bytes())
    req := parse_create_topics_request(mut reader, 0, false) or {
        assert false, 'parse failed: ${err}'
        return
    }
    
    assert req.topics.len == 1
    assert req.topics[0].name == 'test-topic'
    assert req.topics[0].num_partitions == 3
    assert req.topics[0].replication_factor == 1
    assert req.timeout_ms == 30000
}

// Test DeleteTopics Request Parsing
fn test_parse_delete_topics_request() {
    // Build a minimal DeleteTopics request (v0)
    mut writer := new_writer()
    
    // Array length: 2
    writer.write_i32(2)
    
    // Topic names
    writer.write_string('topic-1')
    writer.write_string('topic-2')
    
    // timeout_ms
    writer.write_i32(15000)
    
    mut reader := new_reader(writer.bytes())
    req := parse_delete_topics_request(mut reader, 0, false) or {
        assert false, 'parse failed: ${err}'
        return
    }
    
    assert req.topics.len == 2
    assert req.topics[0].name == 'topic-1'
    assert req.topics[1].name == 'topic-2'
    assert req.timeout_ms == 15000
}

// Test CreateTopicsResponse Encoding
fn test_create_topics_response_encoding() {
    resp := CreateTopicsResponse{
        throttle_time_ms: 100
        topics: [
            CreateTopicsResponseTopic{
                name: 'test-topic'
                error_code: 0
                error_message: none
                num_partitions: 3
                replication_factor: 1
            },
        ]
    }
    
    encoded := resp.encode(0)
    assert encoded.len > 0
    
    // Verify we can read back the basics
    mut reader := new_reader(encoded)
    
    // Array length
    array_len := reader.read_i32() or { 0 }
    assert array_len == 1
}

// Test DeleteTopicsResponse Encoding
fn test_delete_topics_response_encoding() {
    resp := DeleteTopicsResponse{
        throttle_time_ms: 50
        topics: [
            DeleteTopicsResponseTopic{
                name: 'deleted-topic'
                error_code: 0
            },
        ]
    }
    
    encoded := resp.encode(0)
    assert encoded.len > 0
    
    // Verify we can read back the basics
    mut reader := new_reader(encoded)
    
    // Array length
    array_len := reader.read_i32() or { 0 }
    assert array_len == 1
}

// Test Handler CreateTopics Success
fn test_handler_create_topics_success() {
    mut storage := new_mock_storage()
    mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)
    
    // Build request
    mut writer := new_writer()
    writer.write_i32(1)  // 1 topic
    writer.write_string('new-topic')
    writer.write_i32(5)  // 5 partitions
    writer.write_i16(1)  // replication factor
    writer.write_i32(0)  // no replica assignment
    writer.write_i32(0)  // no configs
    writer.write_i32(30000)  // timeout
    
    result := handler.handle_create_topics(writer.bytes(), 0) or {
        assert false, 'handler failed: ${err}'
        return
    }
    
    assert result.len > 0
    assert storage.call_count == 1
    assert 'new-topic' in storage.topics
}

// Test Handler CreateTopics - Topic Already Exists
fn test_handler_create_topics_already_exists() {
    mut storage := new_mock_storage()
    
    // Pre-create topic
    storage.topics['existing-topic'] = domain.TopicMetadata{
        name: 'existing-topic'
        partition_count: 1
    }
    
    mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)
    
    // Build request for existing topic
    mut writer := new_writer()
    writer.write_i32(1)
    writer.write_string('existing-topic')
    writer.write_i32(3)
    writer.write_i16(1)
    writer.write_i32(0)
    writer.write_i32(0)
    writer.write_i32(30000)
    
    result := handler.handle_create_topics(writer.bytes(), 0) or {
        assert false, 'handler failed: ${err}'
        return
    }
    
    // Should return response with error code
    assert result.len > 0
    
    // Parse response to check error code
    mut reader := new_reader(result)
    _ := reader.read_i32() or { 0 }  // array len
    _ := reader.read_string() or { '' }  // topic name
    error_code := reader.read_i16() or { -999 }
    
    assert error_code == i16(ErrorCode.topic_already_exists)
}

// Test Handler DeleteTopics Success
fn test_handler_delete_topics_success() {
    mut storage := new_mock_storage()
    
    // Pre-create topic
    storage.topics['to-delete'] = domain.TopicMetadata{
        name: 'to-delete'
        partition_count: 1
    }
    
    mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)
    
    // Build request
    mut writer := new_writer()
    writer.write_i32(1)  // 1 topic
    writer.write_string('to-delete')
    writer.write_i32(30000)  // timeout
    
    result := handler.handle_delete_topics(writer.bytes(), 0) or {
        assert false, 'handler failed: ${err}'
        return
    }
    
    assert result.len > 0
    assert 'to-delete' !in storage.topics
}

// Test Handler DeleteTopics - Topic Not Found
fn test_handler_delete_topics_not_found() {
    mut storage := new_mock_storage()
    mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)
    
    // Build request for non-existent topic
    mut writer := new_writer()
    writer.write_i32(1)
    writer.write_string('nonexistent')
    writer.write_i32(30000)
    
    result := handler.handle_delete_topics(writer.bytes(), 0) or {
        assert false, 'handler failed: ${err}'
        return
    }
    
    // Parse response to check error code
    mut reader := new_reader(result)
    _ := reader.read_i32() or { 0 }  // array len
    _ := reader.read_string() or { '' }  // topic name
    error_code := reader.read_i16() or { -999 }
    
    assert error_code == i16(ErrorCode.unknown_topic_or_partition)
}

// Test Multiple Topics in Single Request
fn test_handler_create_multiple_topics() {
    mut storage := new_mock_storage()
    mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage)
    
    // Build request with 3 topics
    mut writer := new_writer()
    writer.write_i32(3)  // 3 topics
    
    for name in ['topic-a', 'topic-b', 'topic-c'] {
        writer.write_string(name)
        writer.write_i32(1)
        writer.write_i16(1)
        writer.write_i32(0)
        writer.write_i32(0)
    }
    writer.write_i32(30000)
    
    result := handler.handle_create_topics(writer.bytes(), 0) or {
        assert false, 'handler failed: ${err}'
        return
    }
    
    assert result.len > 0
    assert storage.topics.len == 3
    assert 'topic-a' in storage.topics
    assert 'topic-b' in storage.topics
    assert 'topic-c' in storage.topics
}

// Test Config Parsing Helper
fn test_parse_config_i64() {
    configs := {
        'retention.ms': '86400000'
        'segment.bytes': '1073741824'
    }
    
    assert parse_config_i64(configs, 'retention.ms', 0) == 86400000
    assert parse_config_i64(configs, 'segment.bytes', 0) == 1073741824
    assert parse_config_i64(configs, 'nonexistent', 999) == 999
}

fn test_parse_config_int() {
    configs := {
        'min.insync.replicas': '2'
        'max.message.bytes': '1048576'
    }
    
    assert parse_config_int(configs, 'min.insync.replicas', 1) == 2
    assert parse_config_int(configs, 'max.message.bytes', 0) == 1048576
    assert parse_config_int(configs, 'nonexistent', 42) == 42
}
