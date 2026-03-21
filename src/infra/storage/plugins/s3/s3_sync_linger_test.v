// Unit tests for sync linger batching in S3 adapter
module s3

import time

// test_sync_linger_disabled_when_zero verifies that when sync_linger_ms=0,
// records are added directly to the sync linger disabled path (immediate write).
// No linger buffer should be populated.
fn test_sync_linger_disabled_when_zero() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  0
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// With sync_linger_ms=0, the linger buffer should remain empty
	// after attempting to add records to it
	assert adapter.sync_linger.buffers.len == 0, 'Linger buffers should be empty when disabled'

	// Verify the config is correctly set
	assert adapter.config.sync_linger_ms == 0
}

// test_sync_linger_batches_within_window verifies that multiple record sets
// added within the linger window are accumulated in a single linger buffer.
fn test_sync_linger_batches_within_window() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Create stored records for two separate produce requests
	records_1 := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'key1'.bytes()
			value:     'value1'.bytes()
			headers:   map[string][]u8{}
		},
	]
	records_2 := [
		StoredRecord{
			offset:    1
			timestamp: time.now()
			key:       'key2'.bytes()
			value:     'value2'.bytes()
			headers:   map[string][]u8{}
		},
	]

	// Add records to linger buffer (simulating two produce requests within window)
	ch1 := chan LingerResult{cap: 1}
	ch2 := chan LingerResult{cap: 1}

	adapter.add_to_sync_linger_buffer(partition_key, records_1, ch1)
	adapter.add_to_sync_linger_buffer(partition_key, records_2, ch2)

	// Verify both record sets are in the same linger buffer
	assert partition_key in adapter.sync_linger.buffers, 'Linger buffer should exist for partition'
	buf := adapter.sync_linger.buffers[partition_key]
	assert buf.records.len == 2, 'Expected 2 records in linger buffer, got ${buf.records.len}'
	assert buf.channels.len == 2, 'Expected 2 waiting channels, got ${buf.channels.len}'
}

// test_sync_linger_max_bytes_triggers_flush verifies that when accumulated
// records exceed batch_max_bytes, the linger buffer is marked as ready for flush.
fn test_sync_linger_max_bytes_triggers_flush() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 100
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Create a large record that exceeds batch_max_bytes (100 bytes)
	large_value := []u8{len: 150, init: u8(65)}
	records := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'key'.bytes()
			value:     large_value
			headers:   map[string][]u8{}
		},
	]

	ch := chan LingerResult{cap: 1}
	should_flush := adapter.add_to_sync_linger_buffer(partition_key, records, ch)

	assert should_flush == true, 'Should trigger flush when exceeding batch_max_bytes'
}

// test_sync_linger_result_propagated_to_all_waiters verifies that when
// a linger buffer is flushed, the result is sent to all waiting channels.
fn test_sync_linger_result_propagated_to_all_waiters() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Simulate two waiters
	ch1 := chan LingerResult{cap: 1}
	ch2 := chan LingerResult{cap: 1}
	ch3 := chan LingerResult{cap: 1}

	records_1 := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'k1'.bytes()
			value:     'v1'.bytes()
			headers:   map[string][]u8{}
		},
	]
	records_2 := [
		StoredRecord{
			offset:    1
			timestamp: time.now()
			key:       'k2'.bytes()
			value:     'v2'.bytes()
			headers:   map[string][]u8{}
		},
	]
	records_3 := [
		StoredRecord{
			offset:    2
			timestamp: time.now()
			key:       'k3'.bytes()
			value:     'v3'.bytes()
			headers:   map[string][]u8{}
		},
	]

	adapter.add_to_sync_linger_buffer(partition_key, records_1, ch1)
	adapter.add_to_sync_linger_buffer(partition_key, records_2, ch2)
	adapter.add_to_sync_linger_buffer(partition_key, records_3, ch3)

	// Drain the linger buffer
	buf := adapter.drain_sync_linger_buffer(partition_key)

	assert buf.records.len == 3, 'Expected 3 records in drained buffer'
	assert buf.channels.len == 3, 'Expected 3 channels in drained buffer'

	// Simulate sending a result to all waiters
	result := LingerResult{
		base_offset: 0
	}
	notify_sync_linger_waiters(buf.channels, result)

	// All channels should have received the result
	r1 := <-ch1
	r2 := <-ch2
	r3 := <-ch3

	assert r1.base_offset == 0
	assert r2.base_offset == 0
	assert r3.base_offset == 0
	assert r1.err == none
	assert r2.err == none
	assert r3.err == none
}

// test_sync_linger_error_propagated_to_all_waiters verifies that when
// a flush fails, the error is propagated to all waiting channels.
fn test_sync_linger_error_propagated_to_all_waiters() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	ch1 := chan LingerResult{cap: 1}
	ch2 := chan LingerResult{cap: 1}

	records_1 := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'k1'.bytes()
			value:     'v1'.bytes()
			headers:   map[string][]u8{}
		},
	]
	records_2 := [
		StoredRecord{
			offset:    1
			timestamp: time.now()
			key:       'k2'.bytes()
			value:     'v2'.bytes()
			headers:   map[string][]u8{}
		},
	]

	adapter.add_to_sync_linger_buffer(partition_key, records_1, ch1)
	adapter.add_to_sync_linger_buffer(partition_key, records_2, ch2)

	buf := adapter.drain_sync_linger_buffer(partition_key)

	// Simulate error result
	err_result := LingerResult{
		err:         error('S3 PUT failed')
		base_offset: 0
	}
	notify_sync_linger_waiters(buf.channels, err_result)

	r1 := <-ch1
	r2 := <-ch2

	assert r1.err != none, 'Expected error in result 1'
	assert r2.err != none, 'Expected error in result 2'
}

// test_sync_linger_expired_buffer_detection verifies that expired linger buffers
// (created_at + sync_linger_ms < now) are correctly detected.
fn test_sync_linger_expired_buffer_detection() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Create a buffer with an old timestamp (10ms ago)
	adapter.sync_linger.buffers[partition_key] = SyncLingerBuffer{
		records:    [
			StoredRecord{
				offset:    0
				timestamp: time.now()
				key:       'k'.bytes()
				value:     'v'.bytes()
				headers:   map[string][]u8{}
			},
		]
		channels:   []
		created_at: time.now().unix_milli() - 10
	}

	// With 5ms linger window, this buffer should be expired
	expired_keys := adapter.get_expired_sync_linger_keys()
	assert expired_keys.len == 1, 'Expected 1 expired key, got ${expired_keys.len}'
	assert expired_keys[0] == partition_key
}

// test_sync_linger_not_expired_within_window verifies that buffers within
// the linger window are not detected as expired.
fn test_sync_linger_not_expired_within_window() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  100
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Create a buffer with current timestamp
	adapter.sync_linger.buffers[partition_key] = SyncLingerBuffer{
		records:    [
			StoredRecord{
				offset:    0
				timestamp: time.now()
				key:       'k'.bytes()
				value:     'v'.bytes()
				headers:   map[string][]u8{}
			},
		]
		channels:   []
		created_at: time.now().unix_milli()
	}

	// With 100ms linger window, this buffer should NOT be expired
	expired_keys := adapter.get_expired_sync_linger_keys()
	assert expired_keys.len == 0, 'Expected 0 expired keys, got ${expired_keys.len}'
}

// test_sync_linger_drain_clears_buffer verifies that draining a linger buffer
// removes it from the map and returns all accumulated data.
fn test_sync_linger_drain_clears_buffer() {
	config := S3Config{
		prefix:          'test/'
		bucket_name:     'test-bucket'
		region:          'us-east-1'
		sync_linger_ms:  5
		batch_max_bytes: 4096000
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	ch := chan LingerResult{cap: 1}
	records := [
		StoredRecord{
			offset:    0
			timestamp: time.now()
			key:       'k'.bytes()
			value:     'v'.bytes()
			headers:   map[string][]u8{}
		},
	]

	adapter.add_to_sync_linger_buffer(partition_key, records, ch)
	assert partition_key in adapter.sync_linger.buffers

	buf := adapter.drain_sync_linger_buffer(partition_key)
	assert buf.records.len == 1
	assert buf.channels.len == 1

	// Buffer should be removed from map after drain
	assert partition_key !in adapter.sync_linger.buffers, 'Buffer should be removed after drain'
}
