// Tests for persistent topic_id reverse index (S3-backed)
module s3

import json

// test_topic_id_index_key verifies the S3 key for the persistent index file.
fn test_topic_id_index_key() {
	config := S3Config{
		prefix: 'datacore/'
	}
	adapter := S3StorageAdapter{
		config: config
	}

	assert adapter.topic_id_index_key() == 'datacore/__cluster/topic_id_index.json'
}

// test_encode_decode_topic_id_index verifies roundtrip serialization.
fn test_encode_decode_topic_id_index() {
	idx := {
		'aabbccdd': 'my-topic'
		'11223344': 'other-topic'
	}

	data := encode_topic_id_index(idx)
	assert data.len > 0

	decoded := decode_topic_id_index(data)
	assert decoded.len == 2
	assert decoded['aabbccdd'] == 'my-topic'
	assert decoded['11223344'] == 'other-topic'
}

// test_decode_topic_id_index_empty_data returns empty map for empty input.
fn test_decode_topic_id_index_empty_data() {
	decoded := decode_topic_id_index([]u8{})
	assert decoded.len == 0
}

// test_decode_topic_id_index_invalid_json returns empty map for bad JSON.
fn test_decode_topic_id_index_invalid_json() {
	decoded := decode_topic_id_index('not json'.bytes())
	assert decoded.len == 0
}

// test_merge_topic_id_index_adds_new_entry preserves existing + adds new.
fn test_merge_topic_id_index_adds_new_entry() {
	existing := {
		'aabb': 'topic-a'
	}

	merged := merge_topic_id_index(existing, 'ccdd', 'topic-b')
	assert merged.len == 2
	assert merged['aabb'] == 'topic-a'
	assert merged['ccdd'] == 'topic-b'
}

// test_merge_topic_id_index_overwrites_existing_key updates value for same key.
fn test_merge_topic_id_index_overwrites_existing_key() {
	existing := {
		'aabb': 'old-name'
	}

	merged := merge_topic_id_index(existing, 'aabb', 'new-name')
	assert merged.len == 1
	assert merged['aabb'] == 'new-name'
}

// test_remove_from_topic_id_index removes the specified entry.
fn test_remove_from_topic_id_index() {
	existing := {
		'aabb': 'topic-a'
		'ccdd': 'topic-b'
	}

	result := remove_from_topic_id_index(existing, 'aabb')
	assert result.len == 1
	assert 'aabb' !in result
	assert result['ccdd'] == 'topic-b'
}

// test_remove_from_topic_id_index_nonexistent is a no-op for missing key.
fn test_remove_from_topic_id_index_nonexistent() {
	existing := {
		'aabb': 'topic-a'
	}

	result := remove_from_topic_id_index(existing, 'xxxx')
	assert result.len == 1
	assert result['aabb'] == 'topic-a'
}

// test_merge_topic_id_index_from_empty_map starts from empty.
fn test_merge_topic_id_index_from_empty_map() {
	existing := map[string]string{}

	merged := merge_topic_id_index(existing, 'aabb', 'topic-a')
	assert merged.len == 1
	assert merged['aabb'] == 'topic-a'
}

// test_remove_from_topic_id_index_last_entry results in empty map.
fn test_remove_from_topic_id_index_last_entry() {
	existing := {
		'aabb': 'topic-a'
	}

	result := remove_from_topic_id_index(existing, 'aabb')
	assert result.len == 0
}
