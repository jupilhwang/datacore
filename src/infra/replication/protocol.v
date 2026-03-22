module replication

import domain
import json
import encoding.binary
import net

// Protocol implements length-prefixed binary protocol
// Format: [4 bytes length][JSON payload]
/// Protocol implements a length-prefixed binary protocol. Format: [4 bytes length][payload].
pub struct Protocol {}

// Protocol.new creates a new Protocol instance for encoding/decoding replication messages.
/// Protocol.
pub fn Protocol.new() Protocol {
	return Protocol{}
}

// encode converts ReplicationMessage to wire format
/// encode converts a ReplicationMessage to wire format.
pub fn (p Protocol) encode(msg domain.ReplicationMessage) ![]u8 {
	// Convert message to JSON
	json_str := msg.to_json()
	json_bytes := json_str.bytes()

	// Create buffer: 4 bytes for length + JSON payload
	mut buf := []u8{len: 4 + json_bytes.len}

	// Write length (big-endian 32-bit)
	binary.big_endian_put_u32(mut buf[0..4], u32(json_bytes.len))

	// Write JSON payload
	copy(mut buf[4..], json_bytes)

	return buf
}

// decode parses wire format to ReplicationMessage
/// decode parses wire format into a ReplicationMessage.
pub fn (p Protocol) decode(data []u8) !domain.ReplicationMessage {
	if data.len < 4 {
		return error('invalid data: too short (${data.len} bytes)')
	}

	// Read length
	payload_len := int(binary.big_endian_u32(data[0..4]))

	if data.len < 4 + payload_len {
		return error('invalid data: expected ${4 + payload_len} bytes, got ${data.len}')
	}

	// Extract JSON payload
	json_bytes := data[4..4 + payload_len]
	json_str := json_bytes.bytestr()

	// Parse JSON
	msg := parse_message(json_str)!

	return msg
}

// WireMessage - JSON 디코딩용 중간 표현
// to_json()가 map[string]string으로 직렬화하므로 모든 필드를 string으로 선언
struct WireMessage {
pub:
	msg_type       string
	correlation_id string
	sender_id      string
	timestamp      string
	topic          string
	partition      string
	offset         string
	success        string
	error_msg      string
}

// parse_message는 JSON 문자열을 ReplicationMessage로 변환한다.
fn parse_message(json_str string) !domain.ReplicationMessage {
	wire := json.decode(WireMessage, json_str)!

	msg_type_val := match wire.msg_type {
		'replicate' { domain.ReplicationType.replicate }
		'replicate_ack' { domain.ReplicationType.replicate_ack }
		'flush_ack' { domain.ReplicationType.flush_ack }
		'heartbeat' { domain.ReplicationType.heartbeat }
		else { return error('unknown msg_type: ${wire.msg_type}') }
	}

	return domain.ReplicationMessage{
		msg_type:       msg_type_val
		correlation_id: wire.correlation_id
		sender_id:      wire.sender_id
		timestamp:      wire.timestamp.i64()
		topic:          wire.topic
		partition:      i32(wire.partition.int())
		offset:         wire.offset.i64()
		records_data:   []
		success:        wire.success == 'true'
		error_msg:      wire.error_msg
	}
}

// read_message reads a complete message from TCP connection
/// read_message reads a complete message from a TCP connection.
pub fn (p Protocol) read_message(mut conn net.TcpConn) !domain.ReplicationMessage {
	// Read 4-byte length header
	mut len_buf := []u8{len: 4}
	bytes_read := conn.read(mut len_buf)!

	if bytes_read != 4 {
		return error('failed to read length header: got ${bytes_read} bytes')
	}

	payload_len := int(binary.big_endian_u32(len_buf))

	if payload_len <= 0 || payload_len > 10 * 1024 * 1024 {
		return error('invalid payload length: ${payload_len}')
	}

	// Read payload
	mut payload := []u8{len: payload_len}
	mut total_read := 0

	for total_read < payload_len {
		n := conn.read(mut payload[total_read..])!
		if n == 0 {
			return error('connection closed while reading payload')
		}
		total_read += n
	}

	// Decode
	msg := p.decode_payload(payload)!
	return msg
}

// decode_payload decodes just the payload (without length prefix)
fn (p Protocol) decode_payload(data []u8) !domain.ReplicationMessage {
	json_str := data.bytestr()
	msg := parse_message(json_str)!
	return msg
}

// write_message writes a complete message to TCP connection
/// write_message writes a complete message to a TCP connection.
pub fn (p Protocol) write_message(mut conn net.TcpConn, msg domain.ReplicationMessage) ! {
	wire_data := p.encode(msg)!
	conn.write(wire_data)!
}
