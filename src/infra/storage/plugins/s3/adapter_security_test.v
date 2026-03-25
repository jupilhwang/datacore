// Tests for path traversal prevention at public API entry points.
// Verifies that validate_identifier is called before any S3 key construction.
module s3

import domain

fn test_append_rejects_traversal_topic() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.append('../../evil', 0, [], 0) or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}

fn test_append_rejects_null_byte_topic() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.append('evil\x00topic', 0, [], 0) or {
		assert err.msg().contains('null byte'), 'expected null byte error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected null byte in topic name'
}

fn test_fetch_rejects_traversal_topic() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.fetch('../evil', 0, 0, 1024) or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}

fn test_delete_records_rejects_traversal_topic() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.delete_records('foo..bar', 0, 10) or {
		assert err.msg().contains('path traversal'), 'expected path traversal error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected double dot in topic name'
}

fn test_delete_topic_rejects_traversal() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.delete_topic('../../evil') or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}

fn test_get_topic_rejects_traversal() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.get_topic('../../evil') or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}

fn test_get_partition_info_rejects_traversal() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.get_partition_info('../evil', 0) or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}

fn test_create_topic_rejects_double_dot() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.create_topic('foo..bar', 1, domain.TopicConfig{}) or {
		assert err.msg().contains('path traversal'), 'expected path traversal error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected double dot in topic name'
}

fn test_add_partitions_rejects_traversal() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}
	adapter.add_partitions('../../evil', 5) or {
		assert err.msg().contains('path traversal') || err.msg().contains('invalid character'), 'expected validation error, got: ${err.msg()}'

		return
	}
	assert false, 'should have rejected path traversal in topic name'
}
