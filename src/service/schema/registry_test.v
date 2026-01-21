// Unit tests for Schema Registry
module schema

import domain
import service.port

// Mock Storage for testing
struct MockStorage {
mut:
	topics  map[string]domain.TopicMetadata
	records map[string][]domain.Record
}

fn new_mock_storage() &MockStorage {
	return &MockStorage{
		topics:  map[string]domain.TopicMetadata{}
		records: map[string][]domain.Record{}
	}
}

fn (mut s MockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	if name in s.topics {
		return error('topic already exists')
	}
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        []u8{len: 16}
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     false
	}
	s.topics[name] = meta
	return meta
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

fn (mut s MockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	for _, t in s.topics {
		if t.topic_id.len == topic_id.len && t.topic_id == topic_id {
			return t
		}
	}
	return error('topic not found')
}

fn (mut s MockStorage) add_partitions(name string, new_count int) ! {}

fn (mut s MockStorage) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	key := '${topic}-${partition}'
	if key !in s.records {
		s.records[key] = []domain.Record{}
	}
	s.records[key] << records
	return domain.AppendResult{
		base_offset: 0
	}
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
	assert stats.total_schemas == 1 // Same schema, not duplicated
}

// Test Multiple Schema Versions
fn test_register_multiple_versions() {
	storage := new_mock_storage()
	config := RegistryConfig{
		default_compatibility: .none // Disable compatibility for this test
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

	assert id1 != id2 // Different schemas get different IDs

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
	registry.list_versions('temp-value') or { return }
	assert false, 'subject should not exist'
}

// Test Delete Version
fn test_delete_version() {
	storage := new_mock_storage()
	config := RegistryConfig{
		default_compatibility: .none
	}
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

	assert fp1 == fp2 // Same schema = same fingerprint
	assert fp1.len == 32 // MD5 hex is 32 chars

	fp3 := compute_fingerprint('{"type":"string"}')
	assert fp1 != fp3 // Different schema = different fingerprint
}

// Test Test Compatibility
fn test_test_compatibility() {
	storage := new_mock_storage()
	config := RegistryConfig{
		default_compatibility: .backward
	}
	mut registry := new_registry(storage, config)

	// Register first schema (record type)
	registry.register('compat-test', '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
		.avro) or {}

	// Test compatibility of new schema with added nullable field (backward compatible)
	is_compat := registry.test_compatibility('compat-test', '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":["null","string"]}]}',
		.avro) or { false }

	assert is_compat == true, 'Adding nullable field should be backward compatible'
}

// ============================================================================
// Avro Compatibility Tests
// ============================================================================

// Test Avro backward compatibility - adding field with default
fn test_avro_backward_compat_add_field_with_default() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":"unknown"}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'Adding field with default should be backward compatible'
}

// Test Avro backward compatibility - adding field without default (nullable)
fn test_avro_backward_compat_add_nullable_field() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"email","type":["null","string"]}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'Adding nullable field should be backward compatible'
}

// Test Avro backward compatibility - removing field
fn test_avro_backward_compat_remove_field() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	// Removing a field IS backward compatible (new reader ignores old field)
	assert result == true, 'Removing field should be backward compatible (reader ignores unknown fields)'
}

// Test Avro backward compatibility - adding required field without default
fn test_avro_backward_compat_add_required_field() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	// Adding required field without default is NOT backward compatible
	assert result == false, 'Adding required field without default should NOT be backward compatible'
}

// Test Avro forward compatibility - removing field with default
fn test_avro_forward_compat_remove_field_with_default() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'

	result := check_forward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'Removing field with default should be forward compatible'
}

// Test Avro forward compatibility - removing required field
fn test_avro_forward_compat_remove_required_field() {
	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'

	result := check_forward_compatible(old_schema, new_schema, .avro)
	// Removing required field without default is NOT forward compatible
	assert result == false, 'Removing required field without default should NOT be forward compatible'
}

// Test Avro type promotion - int to long
fn test_avro_type_promotion_int_to_long() {
	old_schema := '{"type":"record","name":"Data","fields":[{"name":"value","type":"int"}]}'
	new_schema := '{"type":"record","name":"Data","fields":[{"name":"value","type":"long"}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'int to long promotion should be compatible'
}

// Test Avro type promotion - string to bytes
fn test_avro_type_promotion_string_to_bytes() {
	old_schema := '{"type":"record","name":"Data","fields":[{"name":"value","type":"string"}]}'
	new_schema := '{"type":"record","name":"Data","fields":[{"name":"value","type":"bytes"}]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'string to bytes should be compatible'
}

// Test Avro enum backward compatibility - adding symbol
fn test_avro_enum_backward_add_symbol() {
	old_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE"]}'
	new_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE","DELETED"]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'Adding enum symbol should be backward compatible'
}

// Test Avro enum backward compatibility - removing symbol
fn test_avro_enum_backward_remove_symbol() {
	old_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE","DELETED"]}'
	new_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE"]}'

	result := check_backward_compatible(old_schema, new_schema, .avro)
	assert result == false, 'Removing enum symbol should NOT be backward compatible'
}

// Test Avro enum forward compatibility - adding symbol
fn test_avro_enum_forward_add_symbol() {
	old_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE"]}'
	new_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE","DELETED"]}'

	result := check_forward_compatible(old_schema, new_schema, .avro)
	assert result == false, 'Adding enum symbol should NOT be forward compatible'
}

// Test Avro enum forward compatibility - removing symbol
fn test_avro_enum_forward_remove_symbol() {
	old_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE","DELETED"]}'
	new_schema := '{"type":"enum","name":"Status","symbols":["PENDING","ACTIVE"]}'

	result := check_forward_compatible(old_schema, new_schema, .avro)
	assert result == true, 'Removing enum symbol should be forward compatible'
}

// Test Avro schema parsing
fn test_parse_avro_schema_record() {
	schema_str := '{"type":"record","name":"User","namespace":"com.example","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}'

	schema := parse_avro_schema(schema_str) or {
		assert false, 'parsing failed: ${err}'
		return
	}

	assert schema.schema_type == 'record'
	assert schema.name == 'User'
	assert schema.namespace == 'com.example'
	assert schema.fields.len == 2

	// Check first field
	assert schema.fields[0].name == 'id'
	assert schema.fields[0].field_type == 'int'
	assert schema.fields[0].has_default == false

	// Check second field
	assert schema.fields[1].name == 'name'
	assert schema.fields[1].field_type == 'string'
	assert schema.fields[1].has_default == true
}

// Test Avro schema parsing - nullable union
fn test_parse_avro_schema_nullable_field() {
	schema_str := '{"type":"record","name":"Data","fields":[{"name":"value","type":["null","string"]}]}'

	schema := parse_avro_schema(schema_str) or {
		assert false, 'parsing failed'
		return
	}

	assert schema.fields.len == 1
	assert schema.fields[0].is_nullable == true
	assert schema.fields[0].is_union == true
	assert 'null' in schema.fields[0].union_types
	assert 'string' in schema.fields[0].union_types
}

// ============================================================================
// Global Config Tests
// ============================================================================

fn test_global_config_get_set() {
	storage := new_mock_storage()
	config := RegistryConfig{
		default_compatibility: .backward
	}
	mut registry := new_registry(storage, config)

	// Get default
	global := registry.get_global_config()
	assert global.compatibility == .backward

	// Set to FULL
	registry.set_global_config(domain.SubjectConfig{
		compatibility: .full
	})

	new_global := registry.get_global_config()
	assert new_global.compatibility == .full
}

// ============================================================================
// Boot Recovery Tests
// ============================================================================

fn test_load_from_storage_empty() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// First call should work
	registry.load_from_storage() or {
		assert false, 'load_from_storage failed: ${err}'
		return
	}

	// Registry should be empty but recovered
	stats := registry.get_stats()
	assert stats.total_schemas == 0
	assert stats.total_subjects == 0
}

fn test_extract_json_int() {
	json_str := '{"id":42,"name":"test"}'

	id := extract_json_int(json_str, 'id') or {
		assert false, 'extract_json_int failed'
		return
	}

	assert id == 42
}

fn test_extract_json_string() {
	json_str := '{"id":42,"name":"test"}'

	name := extract_json_string(json_str, 'name') or {
		assert false, 'extract_json_string failed'
		return
	}

	assert name == 'test'
}

// ============================================================================
// Full Compatibility (Backward + Forward)
// ============================================================================

fn test_full_compatibility() {
	// Full compatibility requires both backward AND forward compatibility
	// This means:
	// - Cannot add required fields without defaults
	// - Cannot remove required fields without defaults
	// - Only adding optional/nullable fields is allowed

	old_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}'
	new_schema := '{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}'

	backward := check_backward_compatible(old_schema, new_schema, .avro)
	forward := check_forward_compatible(old_schema, new_schema, .avro)

	assert backward == true, 'Should be backward compatible (new field has default)'
	assert forward == true, 'Should be forward compatible (old reader ignores new field)'
}

// ============================================================================
// JSON Schema Validation Tests
// ============================================================================

fn test_json_schema_validation_basic() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Valid JSON Schema
	valid_schema := '{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}'

	schema_id := registry.register('json-test', valid_schema, .json) or {
		assert false, 'Valid JSON Schema should be accepted: ${err}'
		return
	}

	assert schema_id > 0
}

fn test_json_schema_validation_invalid_json() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid JSON - unclosed brace
	invalid_schema := '{"type":"object", "properties":'

	registry.register('json-invalid', invalid_schema, .json) or {
		assert err.str().contains('invalid') || err.str().contains('JSON')
		return
	}
	assert false, 'Invalid JSON should be rejected'
}

fn test_json_schema_validation_invalid_type() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid type
	invalid_type := '{"type":"unknown_type"}'

	registry.register('json-bad-type', invalid_type, .json) or {
		assert err.str().contains('unknown type')
		return
	}
	assert false, 'Invalid type should be rejected'
}

fn test_json_schema_validation_min_max_constraints() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid: minimum > maximum
	invalid_minmax := '{"type":"number","minimum":100,"maximum":50}'

	registry.register('json-bad-minmax', invalid_minmax, .json) or {
		assert err.str().contains('minimum')
		return
	}
	assert false, 'Invalid min/max should be rejected'
}

fn test_json_schema_validation_length_constraints() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid: minLength > maxLength
	invalid_len := '{"type":"string","minLength":10,"maxLength":5}'

	registry.register('json-bad-len', invalid_len, .json) or {
		assert err.str().contains('minLength')
		return
	}
	assert false, 'Invalid min/max length should be rejected'
}

fn test_json_schema_boolean_schema() {
	// JSON Schema can be boolean (true = allow all, false = deny all)
	validate_json_schema_syntax('true') or {
		assert false, 'Boolean true schema should be valid'
		return
	}

	validate_json_schema_syntax('false') or {
		assert false, 'Boolean false schema should be valid'
		return
	}
}

// ============================================================================
// JSON Schema Compatibility Tests
// ============================================================================

fn test_json_schema_backward_compat_add_optional_property() {
	old_schema := '{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}'
	new_schema := '{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}'

	result := check_json_backward_compatible(old_schema, new_schema)
	assert result == true, 'Adding optional property should be backward compatible'
}

fn test_json_schema_backward_compat_add_required_property_with_default() {
	old_schema := '{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}'
	new_schema := '{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string","default":"unknown"}},"required":["id","name"]}'

	result := check_json_backward_compatible(old_schema, new_schema)
	assert result == true, 'Adding required property with default should be backward compatible'
}

fn test_json_schema_backward_compat_type_widening() {
	// integer -> number is OK (number accepts integers)
	old_schema := '{"type":"object","properties":{"value":{"type":"integer"}}}'
	new_schema := '{"type":"object","properties":{"value":{"type":"number"}}}'

	result := check_json_backward_compatible(old_schema, new_schema)
	assert result == true, 'Widening integer to number should be backward compatible'
}

fn test_json_schema_forward_compat_remove_optional_property() {
	old_schema := '{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}'
	new_schema := '{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}'

	result := check_json_forward_compatible(old_schema, new_schema)
	assert result == true, 'Removing optional property should be forward compatible'
}

fn test_json_schema_forward_compat_type_narrowing() {
	// number -> integer is OK for forward (if new data is always integers)
	old_schema := '{"type":"object","properties":{"value":{"type":"number"}}}'
	new_schema := '{"type":"object","properties":{"value":{"type":"integer"}}}'

	result := check_json_forward_compatible(old_schema, new_schema)
	assert result == true, 'Narrowing number to integer should be forward compatible'
}

// ============================================================================
// Protobuf Schema Validation Tests
// ============================================================================

fn test_protobuf_schema_validation_basic() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	proto_schema := 'syntax = "proto3";
message User {
  int32 id = 1;
  string name = 2;
}'

	schema_id := registry.register('proto-test', proto_schema, .protobuf) or {
		assert false, 'Valid Protobuf schema should be accepted: ${err}'
		return
	}

	assert schema_id > 0
}

fn test_protobuf_schema_validation_no_message() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid: no message or enum
	invalid_schema := 'syntax = "proto3";'

	registry.register('proto-invalid', invalid_schema, .protobuf) or {
		assert err.str().contains('message') || err.str().contains('enum')
		return
	}
	assert false, 'Protobuf without message/enum should be rejected'
}

fn test_protobuf_schema_validation_unmatched_braces() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid: unmatched braces
	invalid_schema := 'message User { int32 id = 1;'

	registry.register('proto-bad-braces', invalid_schema, .protobuf) or {
		assert err.str().contains('brace')
		return
	}
	assert false, 'Unmatched braces should be rejected'
}

fn test_protobuf_schema_validation_invalid_field_number() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Invalid: reserved field number range
	invalid_schema := 'message User { int32 id = 19000; }'

	registry.register('proto-reserved-num', invalid_schema, .protobuf) or {
		assert err.str().contains('reserved') || err.str().contains('19000')
		return
	}
	assert false, 'Reserved field numbers should be rejected'
}

fn test_protobuf_schema_validation_enum() {
	storage := new_mock_storage()
	mut registry := new_registry(storage, RegistryConfig{})

	// Valid enum
	proto_schema := 'enum Status { UNKNOWN = 0; ACTIVE = 1; DELETED = 2; }'

	schema_id := registry.register('proto-enum', proto_schema, .protobuf) or {
		assert false, 'Valid Protobuf enum should be accepted: ${err}'
		return
	}

	assert schema_id > 0
}

// ============================================================================
// Protobuf Schema Compatibility Tests
// ============================================================================

fn test_protobuf_backward_compat_add_field() {
	old_schema := 'message User { int32 id = 1; }'
	new_schema := 'message User { int32 id = 1; string name = 2; }'

	result := check_protobuf_backward_compatible(old_schema, new_schema)
	assert result == true, 'Adding new field should be backward compatible'
}

fn test_protobuf_backward_compat_remove_field() {
	old_schema := 'message User { int32 id = 1; string name = 2; }'
	new_schema := 'message User { int32 id = 1; }'

	result := check_protobuf_backward_compatible(old_schema, new_schema)
	assert result == true, 'Removing field should be backward compatible'
}

fn test_protobuf_backward_compat_type_change_compatible() {
	// int32 -> int64 is compatible (same wire type)
	old_schema := 'message Data { int32 value = 1; }'
	new_schema := 'message Data { int64 value = 1; }'

	result := check_protobuf_backward_compatible(old_schema, new_schema)
	assert result == true, 'int32 to int64 should be compatible'
}

fn test_protobuf_forward_compat_add_field() {
	old_schema := 'message User { int32 id = 1; }'
	new_schema := 'message User { int32 id = 1; string name = 2; }'

	result := check_protobuf_forward_compatible(old_schema, new_schema)
	assert result == true, 'Adding field should be forward compatible (old reader ignores)'
}

fn test_protobuf_reserved_field_check() {
	old_schema := 'message User { int32 id = 1; string name = 2; reserved 3; }'
	new_schema := 'message User { int32 id = 1; string name = 2; string email = 3; }'

	result := check_protobuf_backward_compatible(old_schema, new_schema)
	assert result == false, 'Reusing reserved field number should NOT be compatible'
}

fn test_protobuf_compatible_wire_types() {
	// Test various wire type compatible changes

	// sint32 to sint64 (both zigzag)
	old1 := 'message Data { sint32 value = 1; }'
	new1 := 'message Data { sint64 value = 1; }'
	assert check_protobuf_backward_compatible(old1, new1) == true

	// fixed32 to sfixed32 (both 32-bit wire type)
	old2 := 'message Data { fixed32 value = 1; }'
	new2 := 'message Data { sfixed32 value = 1; }'
	assert check_protobuf_backward_compatible(old2, new2) == true

	// string to bytes (both length-delimited)
	old3 := 'message Data { string value = 1; }'
	new3 := 'message Data { bytes value = 1; }'
	assert check_protobuf_backward_compatible(old3, new3) == true
}

fn test_protobuf_schema_info_parsing() {
	schema := 'syntax = "proto3";
message User {
  int32 id = 1;
  string name = 2;
  repeated string tags = 3;
  reserved 10, 11, 12;
  reserved "old_field";
}'

	info := parse_protobuf_schema_info(schema)

	assert info.syntax == 'proto3'
	assert info.message_name == 'User'
	assert info.fields.len == 3

	// Check field 1
	field1 := info.fields[1] or {
		assert false, 'Field 1 should exist'
		return
	}
	assert field1.name == 'id'
	assert field1.field_type == 'int32'
	assert field1.is_optional == true // proto3 default

	// Check field 3 (repeated)
	field3 := info.fields[3] or {
		assert false, 'Field 3 should exist'
		return
	}
	assert field3.is_repeated == true

	// Check reserved
	assert 10 in info.reserved_nums
	assert 11 in info.reserved_nums
	assert 12 in info.reserved_nums
	assert 'old_field' in info.reserved_names
}

// ============================================================================
// Avro Schema Enhanced Validation Tests
// ============================================================================

fn test_avro_schema_validation_primitive_string() {
	// Primitive type as string
	validate_avro_schema_syntax('"string"') or {
		assert false, 'Primitive string should be valid'
		return
	}
}

fn test_avro_schema_validation_invalid_primitive() {
	validate_avro_schema_syntax('"invalid_type"') or {
		assert err.str().contains('unknown primitive')
		return
	}
	assert false, 'Invalid primitive should be rejected'
}

fn test_avro_schema_validation_record_missing_name() {
	schema := '{"type":"record","fields":[]}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('name')
		return
	}
	assert false, 'Record without name should be rejected'
}

fn test_avro_schema_validation_record_missing_fields() {
	schema := '{"type":"record","name":"Test"}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('fields')
		return
	}
	assert false, 'Record without fields should be rejected'
}

fn test_avro_schema_validation_enum_empty_symbols() {
	schema := '{"type":"enum","name":"Status","symbols":[]}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('empty')
		return
	}
	assert false, 'Enum with empty symbols should be rejected'
}

fn test_avro_schema_validation_array_missing_items() {
	schema := '{"type":"array"}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('items')
		return
	}
	assert false, 'Array without items should be rejected'
}

fn test_avro_schema_validation_map_missing_values() {
	schema := '{"type":"map"}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('values')
		return
	}
	assert false, 'Map without values should be rejected'
}

fn test_avro_schema_validation_fixed_missing_size() {
	schema := '{"type":"fixed","name":"MD5"}'

	validate_avro_schema_syntax(schema) or {
		assert err.str().contains('size')
		return
	}
	assert false, 'Fixed without size should be rejected'
}
