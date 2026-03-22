module replication

import domain
import encoding.binary
import net
import time

// Binary protocol version. Increment on breaking wire-format changes.
const binary_protocol_version = u8(1)

// Minimum binary frame size: 4 (length) + 1 (version) + 1 (msg_type)
const binary_min_frame_size = 6

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
pub fn (p BinaryProtocol) encode(msg domain.ReplicationMessage) []u8 {
	payload_size := calc_payload_size(msg)
	mut buf := []u8{len: 0, cap: 4 + payload_size}

	// Total length (excludes itself)
	write_i32(mut buf, i32(payload_size))
	// Protocol version
	buf << binary_protocol_version
	// Message type ordinal
	buf << u8(msg.msg_type)
	// String fields
	write_string(mut buf, msg.correlation_id)
	write_string(mut buf, msg.sender_id)
	// Timestamp
	write_i64(mut buf, msg.timestamp)
	// Topic
	write_string(mut buf, msg.topic)
	// Partition
	write_i32(mut buf, msg.partition)
	// Offset
	write_i64(mut buf, msg.offset)
	// Success flag
	buf << if msg.success { u8(1) } else { u8(0) }
	// Error message
	write_string(mut buf, msg.error_msg)
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
	if payload_len <= 0 || payload_len > 10 * 1024 * 1024 {
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
	wire_data := p.encode(msg)
	conn.write(wire_data) or { return error('binary write failed: ${err}') }
}

// --- Helper: size calculation ---

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

// --- Helper: write primitives ---

// write_i16 appends a big-endian i16 to the buffer.
fn write_i16(mut buf []u8, val i16) {
	mut tmp := []u8{len: 2}
	binary.big_endian_put_u16(mut tmp, u16(val))
	buf << tmp
}

// write_i32 appends a big-endian i32 to the buffer.
fn write_i32(mut buf []u8, val i32) {
	mut tmp := []u8{len: 4}
	binary.big_endian_put_u32(mut tmp, u32(val))
	buf << tmp
}

// write_i64 appends a big-endian i64 to the buffer.
fn write_i64(mut buf []u8, val i64) {
	mut tmp := []u8{len: 8}
	binary.big_endian_put_u64(mut tmp, u64(val))
	buf << tmp
}

// write_string appends a 2-byte length-prefixed UTF-8 string to the buffer.
fn write_string(mut buf []u8, s string) {
	write_i16(mut buf, i16(s.len))
	if s != '' {
		buf << s.bytes()
	}
}

// write_bytes appends a 4-byte length-prefixed byte slice to the buffer.
fn write_bytes(mut buf []u8, data []u8) {
	write_i32(mut buf, i32(data.len))
	if data.len > 0 {
		buf << data
	}
}

// --- Helper: read primitives ---

// read_i16 reads a big-endian i16 at the given offset.
fn read_i16(data []u8, offset int) i16 {
	return i16(binary.big_endian_u16(data[offset..offset + 2]))
}

// read_i32 reads a big-endian i32 at the given offset.
fn read_i32(data []u8, offset int) i32 {
	return i32(binary.big_endian_u32(data[offset..offset + 4]))
}

// read_i64 reads a big-endian i64 at the given offset.
fn read_i64(data []u8, offset int) i64 {
	return i64(binary.big_endian_u64(data[offset..offset + 8]))
}

// read_string reads a 2-byte length-prefixed string at the given offset.
// Returns the string and the number of bytes consumed (2 + string length).
fn read_string(data []u8, offset int) !(string, int) {
	if offset + 2 > data.len {
		return error('read_string: not enough data for length at offset ${offset}')
	}
	str_len := int(read_i16(data, offset))
	end := offset + 2 + str_len
	if end > data.len {
		return error('read_string: not enough data for string at offset ${offset}, need ${str_len} bytes')
	}
	if str_len == 0 {
		return '', 2
	}
	return data[offset + 2..end].bytestr(), 2 + str_len
}

// read_bytes reads a 4-byte length-prefixed byte slice at the given offset.
// Returns the byte slice and the number of bytes consumed (4 + data length).
fn read_bytes(data []u8, offset int) !([]u8, int) {
	if offset + 4 > data.len {
		return error('read_bytes: not enough data for length at offset ${offset}')
	}
	data_len := int(read_i32(data, offset))
	end := offset + 4 + data_len
	if end > data.len {
		return error('read_bytes: not enough data at offset ${offset}, need ${data_len} bytes')
	}
	if data_len == 0 {
		return []u8{}, 4
	}
	return data[offset + 4..end].clone(), 4 + data_len
}

// --- Helper: field decoding ---

// decode_fields reads all message fields starting from the given position.
fn decode_fields(data []u8, start_pos int) !domain.ReplicationMessage {
	mut pos := start_pos

	msg_type_byte := data[pos]
	pos++
	msg_type := decode_msg_type(msg_type_byte)!

	corr_id, corr_len := read_string(data, pos)!
	pos += corr_len

	sender, sender_len := read_string(data, pos)!
	pos += sender_len

	ts := read_i64(data, pos)
	pos += 8

	topic, topic_len := read_string(data, pos)!
	pos += topic_len

	partition := read_i32(data, pos)
	pos += 4

	offset := read_i64(data, pos)
	pos += 8

	success := data[pos] == 1
	pos++

	error_msg, err_len := read_string(data, pos)!
	pos += err_len

	records, _ := read_bytes(data, pos)!

	return domain.ReplicationMessage{
		msg_type:       msg_type
		correlation_id: corr_id
		sender_id:      sender
		timestamp:      ts
		topic:          topic
		partition:      partition
		offset:         offset
		records_data:   records
		success:        success
		error_msg:      error_msg
	}
}

// decode_msg_type converts a byte ordinal to a ReplicationType enum value.
fn decode_msg_type(b u8) !domain.ReplicationType {
	return match b {
		0 { domain.ReplicationType.replicate }
		1 { domain.ReplicationType.replicate_ack }
		2 { domain.ReplicationType.flush_ack }
		3 { domain.ReplicationType.heartbeat }
		4 { domain.ReplicationType.recover }
		else { error('unknown binary msg_type ordinal: ${b}') }
	}
}

// --- Helper: TCP read ---

// read_exact reads exactly buf.len bytes from the connection.
fn read_exact(mut conn net.TcpConn, mut buf []u8) ! {
	mut total := 0
	for total < buf.len {
		n := conn.read(mut buf[total..]) or { return error('read_exact: ${err}') }
		if n == 0 {
			return error('read_exact: connection closed after ${total}/${buf.len} bytes')
		}
		total += n
	}
}
