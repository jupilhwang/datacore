module replication

import domain
import encoding.binary
import net
import time

// Binary protocol version. Increment on breaking wire-format changes.
const binary_protocol_version = u8(1)

// Minimum binary frame size: 4 (length) + 1 (version) + 1 (msg_type)
const binary_min_frame_size = 6

// Maximum allowed binary payload size (10 MiB).
const max_binary_payload_size = 10 * 1024 * 1024

/// BinaryProtocol implements a compact binary wire format for ReplicationMessage.
/// This is an alternative to the JSON-based Protocol, offering lower overhead
/// and smaller payload sizes.
///
/// Wire format (all multi-byte integers are big-endian):
///   [4 bytes: total_length (of everything after these 4 bytes)]
///   [1 byte:  protocol_version]
///   [1 byte:  msg_type enum ordinal]
///   [2 bytes: correlation_id_length][N bytes: correlation_id]
///   [2 bytes: sender_id_length][N bytes: sender_id]
///   [8 bytes: timestamp]
///   [2 bytes: topic_length][N bytes: topic]
///   [4 bytes: partition]
///   [8 bytes: offset]
///   [1 byte:  success (0 or 1)]
///   [2 bytes: error_msg_length][N bytes: error_msg]
///   [4 bytes: records_data_length][N bytes: records_data]
///
/// Integration note (not yet implemented):
///   To switch to binary protocol, modify client.v send() and server.v message
///   handler to call binary_encode/decode instead of encode/decode. This could
///   be driven by config: replication.protocol = "binary" | "json".
pub struct BinaryProtocol {}

// BinaryProtocol.new creates a new BinaryProtocol instance.
/// BinaryProtocol.new creates a new BinaryProtocol instance.
pub fn BinaryProtocol.new() BinaryProtocol {
	return BinaryProtocol{}
}

// encode converts a ReplicationMessage into compact binary wire format.
/// encode converts a ReplicationMessage into compact binary wire format.
pub fn (p BinaryProtocol) encode(msg domain.ReplicationMessage) ![]u8 {
	payload_size := calc_payload_size(msg)
	mut buf := []u8{len: 0, cap: 4 + payload_size}

	// Total length (excludes itself)
	write_i32(mut buf, i32(payload_size))
	// Protocol version
	buf << binary_protocol_version
	// Message type ordinal
	buf << u8(msg.msg_type)
	// String fields
	write_string(mut buf, msg.correlation_id)!
	write_string(mut buf, msg.sender_id)!
	// Timestamp
	write_i64(mut buf, msg.timestamp)
	// Topic
	write_string(mut buf, msg.topic)!
	// Partition
	write_i32(mut buf, msg.partition)
	// Offset
	write_i64(mut buf, msg.offset)
	// Success flag
	buf << if msg.success { u8(1) } else { u8(0) }
	// Error message
	write_string(mut buf, msg.error_msg)!
	// Records data (4-byte length prefix)
	write_bytes(mut buf, msg.records_data)

	return buf
}

// decode parses binary wire-format data into a ReplicationMessage.
/// decode parses binary wire-format data into a ReplicationMessage.
pub fn (p BinaryProtocol) decode(data []u8) !domain.ReplicationMessage {
	if data.len < binary_min_frame_size {
		return error('invalid binary data: too short (${data.len} bytes, need >= ${binary_min_frame_size})')
	}

	total_len := read_i32(data, 0)
	if data.len < 4 + int(total_len) {
		return error('invalid binary data: expected ${4 + total_len} bytes, got ${data.len}')
	}

	mut pos := 4
	version := data[pos]
	pos++
	if version != binary_protocol_version {
		return error('unsupported protocol version: ${version} (expected ${binary_protocol_version})')
	}

	payload := data[..4 + int(total_len)]
	return decode_fields(payload, pos)
}

// read_message reads a complete binary-encoded message from a TCP connection.
/// read_message reads a complete binary-encoded message from a TCP connection.
pub fn (p BinaryProtocol) read_message(mut conn net.TcpConn, timeout_ms i64) !domain.ReplicationMessage {
	conn.set_read_timeout(time.Duration(timeout_ms * time.millisecond))
	mut len_buf := []u8{len: 4}
	read_exact(mut conn, mut len_buf)!

	payload_len := int(binary.big_endian_u32(len_buf))
	if payload_len <= 0 || payload_len > max_binary_payload_size {
		return error('invalid binary payload length: ${payload_len}')
	}

	mut payload := []u8{len: payload_len}
	read_exact(mut conn, mut payload)!

	mut full := []u8{len: 0, cap: 4 + payload_len}
	full << len_buf
	full << payload
	return p.decode(full)
}

// write_message writes a binary-encoded message to a TCP connection.
/// write_message writes a binary-encoded message to a TCP connection.
pub fn (p BinaryProtocol) write_message(mut conn net.TcpConn, msg domain.ReplicationMessage) ! {
	wire_data := p.encode(msg)!
	conn.write(wire_data) or { return error('binary write failed: ${err}') }
}

// --- Size calculation ---

// calc_payload_size computes the total payload byte count (excluding the 4-byte length prefix).
fn calc_payload_size(msg domain.ReplicationMessage) int {
	mut size := 0
	size += 1 // protocol version
	size += 1 // msg_type
	size += 2 + msg.correlation_id.len // correlation_id
	size += 2 + msg.sender_id.len // sender_id
	size += 8 // timestamp
	size += 2 + msg.topic.len // topic
	size += 4 // partition
	size += 8 // offset
	size += 1 // success
	size += 2 + msg.error_msg.len // error_msg
	size += 4 + msg.records_data.len // records_data
	return size
}
