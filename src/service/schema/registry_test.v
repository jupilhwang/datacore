// Unit tests for Schema Registry
module schema

import domain
import service.port

// Mock Storage for testing
struct MockStorage {
mut:
    topics       map[string]domain.TopicMetadata
    records      map[string][]domain.Record
}

fn new_mock_storage() &MockStorage {
    return &MockStorage{
        topics: map[string]domain.TopicMetadata{}
        records: map[string][]domain.Record{}
    }
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) ! {
    if name in s.topics {
        return error('topic already exists')
    }
    s.topics[name] = domain.TopicMetadata{
        name: name
        partition_count: partitions
    }
}

fn (mut s MockStorage) delete_topic(name string) ! {
    if name !in s.topics {
        return error('topic not found')
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

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
    key := '${topic}-${partition}'
    if key !in s.records {
        s.records[key] = []domain.Record{}
    }
    s.records[key] << records
    return domain.AppendResult{ base_offset: 0 }
}

fn (mut s MockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
    return domain.FetchResult{}
}

fn (mut s MockStorage) delete_records(topic string, partition int, before_offset i64) ! {}
fn (mut s MockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
    return domain.PartitionInfo{}
}

fn (mut s MockStorage) save_group(group domain.ConsumerGroup) ! {}
fn (mut s MockStorage) load_group(group_id string) !domain.ConsumerGroup { return error('not found') }
fn (mut s MockStorage) delete_group(group_id string) ! {}
fn (mut s MockStorage) list_groups() ![]domain.GroupInfo { return []domain.GroupInfo{} }
fn (mut s MockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}
fn (mut s MockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
    return []domain.OffsetFetchResult{}
}

fn (mut s MockStorage) health_check() !port.HealthStatus { return .healthy }

// Test Schema Registry Creation
fn test_registry_creation() {
    storage := new_mock_storage()
    config := RegistryConfig{}
    mut registry := new_registry(storage, config)
    
    stats := registry.get_stats()
    assert stats.total_schemas == 0
    assert stats.total_subjects == 0
    assert stats.next_id == 1
}

// Test Schema Registration
fn test_register_schema() {
    storage := new_mock_storage()
    config := RegistryConfig{}
    mut registry := new_registry(storage, config)
    
    avro_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
    
    schema_id := registry.register('user-value', avro_schema, .avro) or {
        assert false, 'registration failed: ${err}'
        return
    }
    
    assert schema_id == 1
    
    stats := registry.get_stats()
    assert stats.total_schemas == 1
    assert stats.total_subjects == 1
}

// Test Duplicate Schema Returns Same ID
fn test_register_duplicate_schema() {
    storage := new_mock_storage()
    config := RegistryConfig{}
    mut registry := new_registry(storage, config)
    
    avro_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
    
    id1 := registry.register('user-value', avro_schema, .avro) or {
        assert false, 'first registration failed'
        return
    }
    
    id2 := registry.register('user-value', avro_schema, .avro) or {
        assert false, 'second registration failed'
        return
    }
    
    assert id1 == id2
    
    stats := registry.get_stats()
    assert stats.total_schemas == 1  // Same schema, not duplicated
}

// Test Multiple Schema Versions
fn test_register_multiple_versions() {
    storage := new_mock_storage()
    config := RegistryConfig{
        default_compatibility: .none  // Disable compatibility for this test
    }
    mut registry := new_registry(storage, config)
    
    schema_v1 := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
    schema_v2 := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
    
    id1 := registry.register('user-value', schema_v1, .avro) or {
        assert false, 'v1 registration failed'
        return
    }
    
    id2 := registry.register('user-value', schema_v2, .avro) or {
        assert false, 'v2 registration failed'
        return
    }
    
    assert id1 != id2  // Different schemas get different IDs
    
    versions := registry.list_versions('user-value') or { []int{} }
    assert versions.len == 2
    assert versions[0] == 1
    assert versions[1] == 2
}

// Test Get Schema by ID
fn test_get_schema_by_id() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    avro_schema := '{"type":"record","name":"Order","fields":[]}'
    
    schema_id := registry.register('order-value', avro_schema, .avro) or {
        assert false, 'registration failed'
        return
    }
    
    schema := registry.get_schema(schema_id) or {
        assert false, 'get schema failed'
        return
    }
    
    assert schema.id == schema_id
    assert schema.schema_type == .avro
}

// Test Get Schema by Subject and Version
fn test_get_schema_by_subject() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    avro_schema := '{"type":"record","name":"Product","fields":[]}'
    
    _ := registry.register('product-value', avro_schema, .avro) or {
        assert false, 'registration failed'
        return
    }
    
    // Get version 1
    schema := registry.get_schema_by_subject('product-value', 1) or {
        assert false, 'get by subject failed'
        return
    }
    
    assert schema.schema_type == .avro
    
    // Get latest version (-1)
    latest := registry.get_schema_by_subject('product-value', -1) or {
        assert false, 'get latest failed'
        return
    }
    
    assert latest.id == schema.id
}

// Test List Subjects
fn test_list_subjects() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    registry.register('user-value', '{"type":"string"}', .avro) or {}
    registry.register('order-value', '{"type":"int"}', .avro) or {}
    registry.register('product-value', '{"type":"long"}', .avro) or {}
    
    subjects := registry.list_subjects()
    assert subjects.len == 3
}

// Test Delete Subject
fn test_delete_subject() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    registry.register('temp-value', '{"type":"string"}', .avro) or {}
    
    deleted := registry.delete_subject('temp-value') or {
        assert false, 'delete failed'
        return
    }
    
    assert deleted.len == 1
    assert deleted[0] == 1
    
    // Subject should no longer exist
    registry.list_versions('temp-value') or {
        return  // Expected error
    }
    assert false, 'subject should not exist'
}

// Test Delete Version
fn test_delete_version() {
    storage := new_mock_storage()
    config := RegistryConfig{ default_compatibility: .none }
    mut registry := new_registry(storage, config)
    
    registry.register('multi-value', '{"type":"string"}', .avro) or {}
    registry.register('multi-value', '{"type":"int"}', .avro) or {}
    registry.register('multi-value', '{"type":"long"}', .avro) or {}
    
    // Delete version 2
    registry.delete_version('multi-value', 2) or {
        assert false, 'delete version failed'
        return
    }
    
    // Should have 2 versions left
    versions := registry.list_versions('multi-value') or { []int{} }
    assert versions.len == 2
}

// Test Compatibility Get/Set
fn test_compatibility_config() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{
        default_compatibility: .backward
    })
    
    // Get default
    compat := registry.get_compatibility('new-subject')
    assert compat == .backward
    
    // Set to FULL
    registry.set_compatibility('new-subject', .full)
    
    new_compat := registry.get_compatibility('new-subject')
    assert new_compat == .full
}

// Test Schema Not Found
fn test_schema_not_found() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    registry.get_schema(999) or {
        assert err.str().contains('not found')
        return
    }
    assert false, 'should return error for non-existent schema'
}

// Test Subject Not Found
fn test_subject_not_found() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    registry.list_versions('nonexistent') or {
        assert err.str().contains('not found')
        return
    }
    assert false, 'should return error for non-existent subject'
}

// Test Invalid Avro Schema
fn test_invalid_avro_schema() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    // Missing "type" field
    registry.register('bad-value', '{"name":"Bad"}', .avro) or {
        assert err.str().contains('invalid')
        return
    }
    assert false, 'should reject invalid Avro schema'
}

// Test JSON Schema Type
fn test_json_schema() {
    storage := new_mock_storage()
    mut registry := new_registry(storage, RegistryConfig{})
    
    json_schema := '{"type":"object","properties":{"id":{"type":"integer"}}}'
    
    schema_id := registry.register('json-value', json_schema, .json) or {
        assert false, 'JSON schema registration failed'
        return
    }
    
    schema := registry.get_schema(schema_id) or {
        assert false, 'get schema failed'
        return
    }
    
    assert schema.schema_type == .json
}

// Test Schema Type Conversion
fn test_schema_type_str() {
    assert domain.SchemaType.avro.str() == 'AVRO'
    assert domain.SchemaType.json.str() == 'JSON'
    assert domain.SchemaType.protobuf.str() == 'PROTOBUF'
}

// Test Compatibility Level Conversion
fn test_compatibility_level_str() {
    assert domain.CompatibilityLevel.none.str() == 'NONE'
    assert domain.CompatibilityLevel.backward.str() == 'BACKWARD'
    assert domain.CompatibilityLevel.forward.str() == 'FORWARD'
    assert domain.CompatibilityLevel.full.str() == 'FULL'
}

// Test Schema Type from String
fn test_schema_type_from_str() {
    st := domain.schema_type_from_str('AVRO') or {
        assert false
        return
    }
    assert st == .avro
    
    domain.schema_type_from_str('INVALID') or {
        assert err.str().contains('unknown')
        return
    }
    assert false, 'should error on invalid type'
}

// Test Compatibility Level from String
fn test_compatibility_from_str() {
    cl := domain.compatibility_from_str('BACKWARD') or {
        assert false
        return
    }
    assert cl == .backward
    
    domain.compatibility_from_str('INVALID') or {
        assert err.str().contains('unknown')
        return
    }
    assert false, 'should error on invalid level'
}

// Test JSON Validation Helper
fn test_is_valid_json() {
    assert is_valid_json('{"key":"value"}') == true
    assert is_valid_json('[1,2,3]') == true
    assert is_valid_json('{"nested":{"a":1}}') == true
    
    assert is_valid_json('') == false
    assert is_valid_json('{') == false
    assert is_valid_json('not json') == false
    assert is_valid_json('{"unclosed":') == false
}

// Test Normalize Schema
fn test_normalize_schema() {
    schema1 := '{ "type" : "string" }'
    schema2 := '{"type":"string"}'
    
    assert normalize_schema(schema1) == normalize_schema(schema2)
}

// Test Fingerprint Computation
fn test_compute_fingerprint() {
    schema := '{"type":"record","name":"Test"}'
    fp1 := compute_fingerprint(schema)
    fp2 := compute_fingerprint(schema)
    
    assert fp1 == fp2  // Same schema = same fingerprint
    assert fp1.len == 32  // MD5 hex is 32 chars
    
    fp3 := compute_fingerprint('{"type":"string"}')
    assert fp1 != fp3  // Different schema = different fingerprint
}

// Test Test Compatibility
fn test_test_compatibility() {
    storage := new_mock_storage()
    config := RegistryConfig{ default_compatibility: .backward }
    mut registry := new_registry(storage, config)
    
    // Register first schema
    registry.register('compat-test', '{"type":"string"}', .avro) or {}
    
    // Test compatibility of new schema
    is_compat := registry.test_compatibility('compat-test', '{"type":"int"}', .avro) or {
        false
    }
    
    // Simplified compatibility always returns true for P0
    assert is_compat == true
}
