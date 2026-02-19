module s3

import time
import json
import domain

struct MockS3Client {
mut:
	objects map[string]MockObject
}

struct MockObject {
	data []u8
	etag string
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

fn test_encode_decode_empty_records() {
	original := []StoredRecord{}

	encoded := encode_stored_records(original)
	decoded := decode_stored_records(encoded)

	assert decoded.len == 0
}

fn test_encode_decode_large_record() {
	large_value := []u8{len: 1000000, init: u8(65)}

	original := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'large-key'.bytes()
			value:     large_value
			headers:   map[string][]u8{}
		},
	]

	encoded := encode_stored_records(original)
	decoded := decode_stored_records(encoded)

	assert decoded.len == 1
	assert decoded[0].value.len == 1000000
}

fn test_s3_key_helpers() {
	config := S3Config{
		prefix:      'test-prefix/'
		bucket_name: 'my-bucket'
		region:      'us-east-1'
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
		topic:           'test-topic'
		partition:       0
		earliest_offset: 0
		high_watermark:  100
		log_segments:    [
			LogSegment{
				start_offset: 0
				end_offset:   49
				key:          'segment-0-49.bin'
				size_bytes:   1024
				created_at:   time.now()
			},
			LogSegment{
				start_offset: 50
				end_offset:   99
				key:          'segment-50-99.bin'
				size_bytes:   2048
				created_at:   time.now()
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
	// Test AWS S3 endpoint
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
		name:            'test'
		topic_id:        [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
		partition_count: 3
		config:          {}
		is_internal:     false
	}

	cached := CachedTopic{
		meta:      meta
		etag:      '"abc123"'
		cached_at: time.now()
	}

	assert cached.meta.name == 'test'
	assert cached.meta.partition_count == 3
}

fn test_log_segment_structure() {
	segment := LogSegment{
		start_offset: 1000
		end_offset:   1999
		key:          'path/to/segment.bin'
		size_bytes:   1048576
		created_at:   time.now()
	}

	assert segment.end_offset - segment.start_offset == 999
	assert segment.size_bytes == 1048576
}

// Real S3 integration test (create/read/delete objects in a real bucket)
// Environment: uses ~/.aws/credentials, ap-northeast-2, jhwang-s3
// Note: Objects may remain after the test
// fn test_s3_real_object_lifecycle() {
// 	// config.toml path
// 	config_path := '../../../../../config.toml'
// 	app_config := config.load_config(config_path) or { panic('Failed to read config.toml: ${err}') }
// 	s3 := app_config.storage.s3

// 	// Debug: print all S3 fields loaded from config.toml
// 	println('Loaded S3 config from TOML:')
// 	println('  region   : ${s3.region}')
// 	println('  endpoint : ${s3.endpoint}')
// 	println('  bucket   : ${s3.bucket}')
// 	println('  access_key: ${s3.access_key}')
// 	println('  secret_key: ${s3.secret_key}')
// 	println('  prefix   : ${s3.prefix}')

// 	s3_config := S3Config{
// 		bucket_name:    s3.bucket
// 		region:         s3.region
// 		endpoint:       s3.endpoint
// 		prefix:         s3.prefix
// 		access_key:     s3.access_key
// 		secret_key:     s3.secret_key
// 		use_path_style: false
// 	}
// 	mut adapter := S3StorageAdapter{
// 		config: s3_config
// 	}
// 	key := 'test-adapter-object.txt'
// 	value := 'hello s3 from test'.bytes()

// 	adapter.put_object(key, value) or { panic('S3 put_object failed: ${err}') }

// 	// get_object
// 	data, _ := adapter.get_object(key) or { panic('S3 get_object failed: ${err}') }
// 	assert data == value

// 	// delete_object
// 	adapter.delete_object(key) or { panic('S3 delete_object failed: ${err}') }
// }
