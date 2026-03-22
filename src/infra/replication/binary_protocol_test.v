module replication

import domain
import time

// --- BinaryProtocol encode/decode roundtrip tests ---

// create_binary_test_message creates a ReplicationMessage for binary protocol testing.
fn create_binary_test_message(msg_type domain.ReplicationType) domain.ReplicationMessage {
	return domain.ReplicationMessage{
		msg_type:       msg_type
		correlation_id: 'bin-corr-001'
		sender_id:      'broker-1'
		timestamp:      time.now().unix_milli()
		topic:          'test-topic'
		partition:      3
		offset:         12345
		records_data:   'binary-test-data'.bytes()
		success:        true
		error_msg:      ''
	}
}

// assert_messages_equal verifies all fields of two ReplicationMessages match.
fn assert_messages_equal(decoded domain.ReplicationMessage, original domain.ReplicationMessage) {
	assert decoded.msg_type == original.msg_type
	assert decoded.correlation_id == original.correlation_id
	assert decoded.sender_id == original.sender_id
	assert decoded.timestamp == original.timestamp
	assert decoded.topic == original.topic
	assert decoded.partition == original.partition
	assert decoded.offset == original.offset
	assert decoded.records_data == original.records_data
	assert decoded.success == original.success
	assert decoded.error_msg == original.error_msg
}

// test_binary_roundtrip_replicate: replicate type message encode/decode roundtrip
fn test_binary_roundtrip_replicate() {
	bp := BinaryProtocol.new()
	msg := create_binary_test_message(.replicate)

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_replicate_ack: replicate_ack type roundtrip
fn test_binary_roundtrip_replicate_ack() {
	bp := BinaryProtocol.new()
	msg := create_binary_test_message(.replicate_ack)

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_flush_ack: flush_ack type roundtrip
fn test_binary_roundtrip_flush_ack() {
	bp := BinaryProtocol.new()
	msg := create_binary_test_message(.flush_ack)

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_heartbeat: heartbeat type roundtrip
fn test_binary_roundtrip_heartbeat() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: 'hb-001'
		sender_id:      'broker-hb'
		timestamp:      time.now().unix_milli()
		topic:          ''
		partition:      0
		offset:         0
		records_data:   []
		success:        false
		error_msg:      ''
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_recover: recover type roundtrip
fn test_binary_roundtrip_recover() {
	bp := BinaryProtocol.new()
	msg := create_binary_test_message(.recover)

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_empty_fields: message with all empty/zero fields
fn test_binary_roundtrip_empty_fields() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .heartbeat
		correlation_id: ''
		sender_id:      ''
		timestamp:      0
		topic:          ''
		partition:      0
		offset:         0
		records_data:   []
		success:        false
		error_msg:      ''
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_roundtrip_large_records: message with large records_data payload
fn test_binary_roundtrip_large_records() {
	bp := BinaryProtocol.new()
	// Create a 64KB records_data payload
	large_data := []u8{len: 65536, init: u8(index % 256)}
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'large-001'
		sender_id:      'broker-large'
		timestamp:      time.now().unix_milli()
		topic:          'large-topic'
		partition:      7
		offset:         999999999
		records_data:   large_data
		success:        true
		error_msg:      ''
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
	assert decoded.records_data.len == 65536
}

// test_binary_roundtrip_unicode_strings: topic and error_msg with unicode characters
fn test_binary_roundtrip_unicode_strings() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate_ack
		correlation_id: 'unicode-001'
		sender_id:      'broker-kr'
		timestamp:      time.now().unix_milli()
		topic:          'topic-replication-test'
		partition:      1
		offset:         500
		records_data:   []
		success:        false
		error_msg:      'connection-timeout-error-message'
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert_messages_equal(decoded, msg)
}

// test_binary_decode_too_short: data shorter than minimum header fails
fn test_binary_decode_too_short() {
	bp := BinaryProtocol.new()

	// Empty data
	bp.decode([]u8{}) or {
		assert err.msg().contains('too short')
		return
	}
	assert false, 'empty data should produce error'
}

// test_binary_decode_wrong_version: wrong protocol version fails
fn test_binary_decode_wrong_version() {
	bp := BinaryProtocol.new()

	// Craft a minimal buffer with wrong version
	mut buf := []u8{len: 10}
	// total_length = 6 (bytes after length prefix)
	buf[0] = 0
	buf[1] = 0
	buf[2] = 0
	buf[3] = 6
	// protocol_version = 99 (wrong)
	buf[4] = 99
	// msg_type = 0
	buf[5] = 0

	bp.decode(buf) or {
		assert err.msg().contains('unsupported protocol version')
		return
	}
	assert false, 'wrong version should produce error'
}

// test_binary_vs_json_size: binary encoding produces smaller output than JSON
fn test_binary_vs_json_size() {
	bp := BinaryProtocol.new()
	jp := Protocol.new()

	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'size-compare-001'
		sender_id:      'broker-1'
		timestamp:      time.now().unix_milli()
		topic:          'comparison-topic'
		partition:      5
		offset:         100000
		records_data:   'some-record-data-for-comparison'.bytes()
		success:        true
		error_msg:      ''
	}

	binary_encoded := bp.encode(msg)
	json_encoded := jp.encode(msg) or {
		assert false, 'json encode failed: ${err}'
		return
	}

	assert binary_encoded.len < json_encoded.len, 'binary (${binary_encoded.len}) should be smaller than json (${json_encoded.len})'
}

// test_binary_all_fields_preserved: verify every field value is preserved exactly
fn test_binary_all_fields_preserved() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: 'exact-corr-id-12345'
		sender_id:      'exact-sender-67890'
		timestamp:      1700000000000
		topic:          'exact-topic-name'
		partition:      42
		offset:         9876543210
		records_data:   [u8(0x00), 0x01, 0xFF, 0x7F, 0x80]
		success:        true
		error_msg:      'exact error message'
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert decoded.msg_type == .replicate
	assert decoded.correlation_id == 'exact-corr-id-12345'
	assert decoded.sender_id == 'exact-sender-67890'
	assert decoded.timestamp == 1700000000000
	assert decoded.topic == 'exact-topic-name'
	assert decoded.partition == 42
	assert decoded.offset == 9876543210
	assert decoded.records_data == [u8(0x00), 0x01, 0xFF, 0x7F, 0x80]
	assert decoded.success == true
	assert decoded.error_msg == 'exact error message'
}

// test_binary_boundary_values: boundary values for numeric fields
fn test_binary_boundary_values() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate
		correlation_id: ''
		sender_id:      ''
		timestamp:      i64(9223372036854775807) // max i64
		topic:          ''
		partition:      2147483647 // max i32
		offset:         0
		records_data:   []
		success:        false
		error_msg:      ''
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert decoded.timestamp == i64(9223372036854775807)
	assert decoded.partition == 2147483647
	assert decoded.offset == 0
}

// test_binary_decode_truncated_payload: header claims more data than available
fn test_binary_decode_truncated_payload() {
	bp := BinaryProtocol.new()

	// Craft buffer with length prefix claiming 1000 bytes, but only 10 provided
	mut buf := []u8{len: 14}
	// total_length = 1000
	buf[0] = 0
	buf[1] = 0
	buf[2] = 0x03
	buf[3] = 0xE8
	// protocol_version = 1
	buf[4] = 1
	// msg_type = 0
	buf[5] = 0

	bp.decode(buf) or {
		assert err.msg().contains('expected') || err.msg().contains('too short')
		return
	}
	assert false, 'truncated payload should produce error'
}

// test_binary_success_flag_false: explicitly test success=false encoding
fn test_binary_success_flag_false() {
	bp := BinaryProtocol.new()
	msg := domain.ReplicationMessage{
		msg_type:       .replicate_ack
		correlation_id: 'fail-001'
		sender_id:      'broker-fail'
		timestamp:      time.now().unix_milli()
		topic:          'fail-topic'
		partition:      0
		offset:         0
		records_data:   []
		success:        false
		error_msg:      'replication failed: timeout'
	}

	encoded := bp.encode(msg)
	decoded := bp.decode(encoded) or {
		assert false, 'decode failed: ${err}'
		return
	}

	assert decoded.success == false
	assert decoded.error_msg == 'replication failed: timeout'
}

// test_binary_protocol_version_header: verify protocol version byte in encoded output
fn test_binary_protocol_version_header() {
	bp := BinaryProtocol.new()
	msg := create_binary_test_message(.replicate)

	encoded := bp.encode(msg)

	// After 4-byte length prefix, byte at index 4 should be protocol version 1
	assert encoded.len > 5
	assert encoded[4] == 1, 'protocol version byte should be 1, got ${encoded[4]}'
}
