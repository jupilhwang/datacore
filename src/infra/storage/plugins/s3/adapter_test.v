// S3 Storage Adapter Tests
// Uses mock S3 client for unit testing
module s3

import domain
import time
import json

// MockS3Client simulates S3 operations for testing
struct MockS3Client {
mut:
	objects map[string]MockObject
}

struct MockObject {
	data []u8
	etag string
}

fn new_mock_s3_client() &MockS3Client {
	return &MockS3Client{
		objects: map[string]MockObject{}
	}
}

fn (mut c MockS3Client) put(key string, data []u8) {
	c.objects[key] = MockObject{
		data: data.clone()
		etag: '"mock-etag-${key.hash()}"'
	}
}

fn (mut c MockS3Client) get(key string) !([]u8, string) {
	if obj := c.objects[key] {
		return obj.data, obj.etag
	}
	return error('not found')
}

fn (mut c MockS3Client) delete(key string) {
	c.objects.delete(key)
}

fn (c &MockS3Client) list_prefix(prefix string) []string {
	mut keys := []string{}
	for key, _ in c.objects {
		if key.starts_with(prefix) {
			keys << key
		}
	}
	return keys
}

// ============================================================
// Unit Tests
// ============================================================

fn test_s3_config_defaults() {
	config := S3Config{}
	assert config.bucket_name == 'datacore-storage'
	assert config.region == 'us-east-1'
	assert config.prefix == 'datacore/'
	assert config.max_retries == 3
}

fn test_generate_topic_id() {
	id1 := generate_topic_id('test-topic')
	id2 := generate_topic_id('test-topic')
	id3 := generate_topic_id('different-topic')
	
	assert id1.len == 16
	assert id1 == id2  // Same name = same ID
	assert id1 != id3  // Different name = different ID
}

fn test_encode_decode_records() {
	original := [
		StoredRecord{
			offset: 100
			timestamp: time.unix(1700000000)
			key: 'key1'.bytes()
			value: 'value1'.bytes()
			headers: {'h1': 'v1'.bytes()}
		},
		StoredRecord{
			offset: 101
			timestamp: time.unix(1700000001)
			key: 'key2'.bytes()
			value: 'value2'.bytes()
			headers: map[string][]u8{}
		},
	]
	
	encoded := encode_stored_records(original)
	assert encoded.len > 0
	
	decoded := decode_stored_records(encoded)
	assert decoded.len == 2
	assert decoded[0].offset == 100
	assert decoded[0].key == 'key1'.bytes()
	assert decoded[0].value == 'value1'.bytes()
	assert decoded[0].headers.len == 1
	assert decoded[0].headers['h1'] == 'v1'.bytes()
	
	assert decoded[1].offset == 101
	assert decoded[1].key == 'key2'.bytes()
	assert decoded[1].headers.len == 0
}

fn test_encode_decode_empty_records() {
	original := []StoredRecord{}
	
	encoded := encode_stored_records(original)
	decoded := decode_stored_records(encoded)
	
	assert decoded.len == 0
}

fn test_encode_decode_large_record() {
	large_value := []u8{len: 1000000, init: u8(65)} // 1MB of 'A'
	
	original := [
		StoredRecord{
			offset: 0
			timestamp: time.now()
			key: 'large-key'.bytes()
			value: large_value
			headers: map[string][]u8{}
		},
	]
	
	encoded := encode_stored_records(original)
	decoded := decode_stored_records(encoded)
	
	assert decoded.len == 1
	assert decoded[0].value.len == 1000000
}

fn test_s3_key_helpers() {
	config := S3Config{
		prefix: 'test-prefix/'
		bucket_name: 'my-bucket'
	}
	
	adapter := S3StorageAdapter{
		config: config
	}
	
	// Topic metadata key
	assert adapter.topic_metadata_key('my-topic') == 'test-prefix/topics/my-topic/metadata.json'
	
	// Partition index key
	assert adapter.partition_index_key('my-topic', 0) == 'test-prefix/topics/my-topic/partitions/0/index.json'
	
	// Log segment key
	segment_key := adapter.log_segment_key('my-topic', 0, 100, 199)
	assert segment_key.contains('my-topic')
	assert segment_key.contains('partitions/0')
	assert segment_key.ends_with('.bin')
	
	// Group key
	assert adapter.group_key('my-group') == 'test-prefix/groups/my-group/state.json'
	
	// Offset key
	assert adapter.offset_key('my-group', 'my-topic', 0) == 'test-prefix/offsets/my-group/my-topic:0.json'
}

fn test_partition_index_serialization() {
	index := PartitionIndex{
		topic: 'test-topic'
		partition: 0
		earliest_offset: 0
		high_watermark: 100
		log_segments: [
			LogSegment{
				start_offset: 0
				end_offset: 49
				key: 'segment-0-49.bin'
				size_bytes: 1024
				created_at: time.now()
			},
			LogSegment{
				start_offset: 50
				end_offset: 99
				key: 'segment-50-99.bin'
				size_bytes: 2048
				created_at: time.now()
			},
		]
	}
	
	// Serialize
	data := json.encode(index)
	assert data.len > 0
	
	// Deserialize
	decoded := json.decode(PartitionIndex, data) or {
		assert false, 'Failed to decode partition index'
		return
	}
	
	assert decoded.topic == 'test-topic'
	assert decoded.partition == 0
	assert decoded.high_watermark == 100
	assert decoded.log_segments.len == 2
}

fn test_s3_endpoint_generation() {
	// Default AWS endpoint
	config1 := S3Config{
		region: 'us-west-2'
	}
	adapter1 := S3StorageAdapter{
		config: config1
	}
	assert adapter1.get_endpoint() == 'https://s3.us-west-2.amazonaws.com'
	
	// Custom endpoint (MinIO)
	config2 := S3Config{
		endpoint: 'http://localhost:9000'
	}
	adapter2 := S3StorageAdapter{
		config: config2
	}
	assert adapter2.get_endpoint() == 'http://localhost:9000'
}

fn test_parse_list_objects_response() {
	xml_response := '<?xml version="1.0"?>
<ListBucketResult>
  <Contents>
    <Key>datacore/topics/topic1/metadata.json</Key>
    <Size>256</Size>
  </Contents>
  <Contents>
    <Key>datacore/topics/topic2/metadata.json</Key>
    <Size>128</Size>
  </Contents>
</ListBucketResult>'
	
	objects := parse_list_objects_response(xml_response)
	
	assert objects.len == 2
	assert objects[0].key == 'datacore/topics/topic1/metadata.json'
	assert objects[1].key == 'datacore/topics/topic2/metadata.json'
}

fn test_cached_topic_structure() {
	meta := domain.TopicMetadata{
		name: 'test'
		topic_id: [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
		partition_count: 3
		config: {}
		is_internal: false
	}
	
	cached := CachedTopic{
		meta: meta
		etag: '"abc123"'
		cached_at: time.now()
	}
	
	assert cached.meta.name == 'test'
	assert cached.meta.partition_count == 3
}

fn test_log_segment_structure() {
	segment := LogSegment{
		start_offset: 1000
		end_offset: 1999
		key: 'path/to/segment.bin'
		size_bytes: 1048576 // 1MB
		created_at: time.now()
	}
	
	assert segment.end_offset - segment.start_offset == 999
	assert segment.size_bytes == 1048576
}
