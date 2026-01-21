// Infra Layer - gRPC Handler
// HTTP/2 based gRPC protocol handler for streaming
module grpc

import domain
import net
import service.port
import service.streaming
import time

// ============================================================================
// gRPC Handler
// ============================================================================

// GrpcHandler handles gRPC connections over HTTP/2
pub struct GrpcHandler {
	config domain.GrpcConfig
pub mut:
	grpc_service &streaming.GrpcService
	storage      port.StoragePort
}

// new_grpc_handler creates a new gRPC handler
pub fn new_grpc_handler(storage port.StoragePort, config domain.GrpcConfig) &GrpcHandler {
	grpc_service := streaming.new_grpc_service(storage, config)
	return &GrpcHandler{
		config:       config
		grpc_service: grpc_service
		storage:      storage
	}
}

// ============================================================================
// Connection Handling
// ============================================================================

// handle_connection handles a new gRPC connection
pub fn (mut h GrpcHandler) handle_connection(mut conn net.TcpConn, client_ip string) {
	// Create gRPC connection
	grpc_conn := domain.new_grpc_connection(client_ip, .bidirectional)

	// Register connection
	conn_id := h.grpc_service.register_connection(grpc_conn) or {
		h.send_error(mut conn, domain.grpc_error_unknown, 'Failed to register connection')
		conn.close() or {}
		return
	}

	defer {
		h.grpc_service.unregister_connection(conn_id) or {}
		conn.close() or {}
	}

	// Get send channel
	send_chan := h.grpc_service.get_send_channel(conn_id) or { return }

	// Start sender goroutine
	spawn h.sender_loop(conn_id, mut conn, send_chan)

	// Start polling goroutine for subscriptions
	spawn h.poll_loop(conn_id)

	// Receiver loop (main loop)
	h.receiver_loop(conn_id, mut conn)
}

// receiver_loop handles incoming gRPC messages
fn (mut h GrpcHandler) receiver_loop(conn_id string, mut conn net.TcpConn) {
	for {
		// Read frame header
		frame := h.read_frame(mut conn) or { break }

		// Parse and handle request
		response := h.handle_frame(conn_id, frame)

		// Send response if needed
		if response.response_type != .pong {
			h.send_response(mut conn, response) or { break }
		}
	}
}

// sender_loop handles outgoing messages from the channel
fn (mut h GrpcHandler) sender_loop(conn_id string, mut conn net.TcpConn, recv_chan chan domain.GrpcStreamResponse) {
	for {
		// Blocking receive from channel
		response := <-recv_chan or { break }
		h.send_response(mut conn, response) or { break }
	}
}

// poll_loop periodically polls for new messages for subscriptions
fn (mut h GrpcHandler) poll_loop(conn_id string) {
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// Check if connection still exists
		_ = h.grpc_service.get_connection(conn_id) or { break }

		// Poll for new messages every 100ms
		if now - last_poll >= 100 {
			h.grpc_service.poll_and_send()
			last_poll = now
		}

		time.sleep(50 * time.millisecond)
	}
}

// ============================================================================
// Frame Reading/Writing
// ============================================================================

// GrpcFrame represents a gRPC frame
struct GrpcFrame {
	compressed bool
	length     u32
	data       []u8
}

// read_frame reads a gRPC frame from the connection
fn (mut h GrpcHandler) read_frame(mut conn net.TcpConn) !GrpcFrame {
	// Read 5-byte header: compressed (1) + length (4)
	mut header := []u8{len: 5}
	total_read := conn.read(mut header) or { return error('Failed to read frame header') }
	if total_read < 5 {
		return error('Incomplete frame header')
	}

	compressed := header[0] == 1
	length := u32(header[1]) << 24 | u32(header[2]) << 16 | u32(header[3]) << 8 | u32(header[4])

	// Check max message size
	if length > u32(h.config.max_message_size) {
		return error('Message too large: ${length} > ${h.config.max_message_size}')
	}

	// Read payload
	mut data := []u8{len: int(length)}
	if length > 0 {
		mut bytes_read := 0
		for bytes_read < int(length) {
			n := conn.read(mut data[bytes_read..]) or { return error('Failed to read payload') }
			if n == 0 {
				break
			}
			bytes_read += n
		}
	}

	return GrpcFrame{
		compressed: compressed
		length:     length
		data:       data
	}
}

// send_frame sends a gRPC frame to the connection
fn (mut h GrpcHandler) send_frame(mut conn net.TcpConn, data []u8, compressed bool) ! {
	// Build 5-byte header
	length := u32(data.len)
	mut frame := []u8{cap: 5 + data.len}

	// Compressed flag
	frame << if compressed { u8(1) } else { u8(0) }

	// Length (big endian)
	frame << u8(length >> 24)
	frame << u8(length >> 16)
	frame << u8(length >> 8)
	frame << u8(length)

	// Payload
	frame << data

	conn.write(frame) or { return error('Failed to write frame') }
}

// ============================================================================
// Request Handling
// ============================================================================

// handle_frame parses and handles a gRPC frame
fn (mut h GrpcHandler) handle_frame(conn_id string, frame GrpcFrame) domain.GrpcStreamResponse {
	// Parse the frame data as a stream request
	req := h.parse_stream_request(frame.data) or {
		return domain.GrpcStreamResponse{
			response_type: .error
			error:         domain.GrpcErrorResponse{
				code:    domain.grpc_error_invalid_message
				message: 'Failed to parse request: ${err}'
			}
		}
	}

	// Handle the request
	return h.grpc_service.handle_stream_request(conn_id, req)
}

// parse_stream_request parses binary data to GrpcStreamRequest
fn (h &GrpcHandler) parse_stream_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 1 {
		return error('Empty request')
	}

	// First byte is request type
	request_type := domain.grpc_stream_request_type_from_int(int(data[0]))

	return match request_type {
		.produce {
			h.parse_produce_request(data[1..])
		}
		.subscribe {
			h.parse_consume_request(data[1..])
		}
		.commit {
			h.parse_commit_request(data[1..])
		}
		.ack {
			h.parse_ack_request(data[1..])
		}
		.ping {
			domain.GrpcStreamRequest{
				request_type: .ping
			}
		}
	}
}

// parse_produce_request parses a produce request from binary data
fn (h &GrpcHandler) parse_produce_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for produce request')
	}

	mut pos := 0

	// Topic length (2 bytes) + topic
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// Partition (4 bytes, -1 for auto)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition_val := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4
	partition := if partition_val >= 0 { i32(partition_val) } else { none }

	// Record count (4 bytes)
	if pos + 4 > data.len {
		return error('Missing record count')
	}
	record_count := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// Parse records
	mut records := []domain.GrpcRecord{cap: record_count}
	for _ in 0 .. record_count {
		record := domain.decode_grpc_record(data[pos..]) or {
			return error('Failed to decode record: ${err}')
		}
		records << record
		pos += record.encode().len
	}

	return domain.GrpcStreamRequest{
		request_type: .produce
		produce:      domain.GrpcProduceRequest{
			topic:     topic
			partition: partition
			records:   records
		}
	}
}

// parse_consume_request parses a consume request from binary data
fn (h &GrpcHandler) parse_consume_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 18 {
		return error('Data too short for consume request')
	}

	mut pos := 0

	// Topic length (2 bytes) + topic
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// Partition (4 bytes)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
		pos + 3])
	pos += 4

	// Offset (8 bytes)
	if pos + 8 > data.len {
		return error('Missing offset')
	}
	offset := i64(data[pos]) << 56 | i64(data[pos + 1]) << 48 | i64(data[pos + 2]) << 40 | i64(data[
		pos + 3]) << 32 | i64(data[pos + 4]) << 24 | i64(data[pos + 5]) << 16 | i64(data[pos + 6]) << 8 | i64(data[
		pos + 7])
	pos += 8

	// Max records (4 bytes)
	if pos + 4 > data.len {
		return error('Missing max_records')
	}
	max_records := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// Max bytes (4 bytes)
	max_bytes := if pos + 4 <= data.len {
		int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[pos + 3])
	} else {
		1048576 // Default 1MB
	}

	return domain.GrpcStreamRequest{
		request_type: .subscribe
		consume:      domain.GrpcConsumeRequest{
			topic:       topic
			partition:   partition
			offset:      offset
			max_records: max_records
			max_bytes:   max_bytes
			group_id:    none
		}
	}
}

// parse_commit_request parses a commit request from binary data
fn (h &GrpcHandler) parse_commit_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for commit request')
	}

	mut pos := 0

	// Group ID length (2 bytes) + group_id
	group_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + group_len > data.len {
		return error('Invalid group_id length')
	}
	group_id := data[pos..pos + group_len].bytestr()
	pos += group_len

	// Offset count (4 bytes)
	if pos + 4 > data.len {
		return error('Missing offset count')
	}
	offset_count := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// Parse offsets
	mut offsets := []domain.GrpcPartitionOffset{cap: offset_count}
	for _ in 0 .. offset_count {
		// Topic length + topic
		if pos + 2 > data.len {
			return error('Missing topic length')
		}
		topic_len := int(data[pos]) << 8 | int(data[pos + 1])
		pos += 2
		if pos + topic_len > data.len {
			return error('Invalid topic length')
		}
		topic := data[pos..pos + topic_len].bytestr()
		pos += topic_len

		// Partition (4 bytes)
		if pos + 4 > data.len {
			return error('Missing partition')
		}
		partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
			pos + 3])
		pos += 4

		// Offset (8 bytes)
		if pos + 8 > data.len {
			return error('Missing offset')
		}
		offset := i64(data[pos]) << 56 | i64(data[pos + 1]) << 48 | i64(data[pos + 2]) << 40 | i64(data[
			pos + 3]) << 32 | i64(data[pos + 4]) << 24 | i64(data[pos + 5]) << 16 | i64(data[pos + 6]) << 8 | i64(data[
			pos + 7])
		pos += 8

		offsets << domain.GrpcPartitionOffset{
			topic:     topic
			partition: partition
			offset:    offset
			metadata:  ''
		}
	}

	return domain.GrpcStreamRequest{
		request_type: .commit
		commit:       domain.GrpcCommitRequest{
			group_id: group_id
			offsets:  offsets
		}
	}
}

// parse_ack_request parses an ack request from binary data
fn (h &GrpcHandler) parse_ack_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 14 {
		return error('Data too short for ack request')
	}

	mut pos := 0

	// Topic length (2 bytes) + topic
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// Partition (4 bytes)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
		pos + 3])
	pos += 4

	// Offset (8 bytes)
	if pos + 8 > data.len {
		return error('Missing offset')
	}
	offset := i64(data[pos]) << 56 | i64(data[pos + 1]) << 48 | i64(data[pos + 2]) << 40 | i64(data[
		pos + 3]) << 32 | i64(data[pos + 4]) << 24 | i64(data[pos + 5]) << 16 | i64(data[pos + 6]) << 8 | i64(data[
		pos + 7])

	return domain.GrpcStreamRequest{
		request_type: .ack
		ack:          domain.GrpcAckRequest{
			topic:     topic
			partition: partition
			offset:    offset
		}
	}
}

// ============================================================================
// Response Encoding
// ============================================================================

// send_response encodes and sends a response
fn (mut h GrpcHandler) send_response(mut conn net.TcpConn, response domain.GrpcStreamResponse) ! {
	data := h.encode_stream_response(response)
	h.send_frame(mut conn, data, false)!
}

// encode_stream_response encodes a stream response to binary
fn (h &GrpcHandler) encode_stream_response(response domain.GrpcStreamResponse) []u8 {
	return match response.response_type {
		.produce_ack {
			if produce := response.produce {
				h.encode_produce_response(produce)
			} else {
				h.encode_error_response(domain.grpc_error_unknown, 'Missing produce response')
			}
		}
		.message {
			if msg := response.message {
				h.encode_message_response(msg)
			} else {
				h.encode_error_response(domain.grpc_error_unknown, 'Missing message response')
			}
		}
		.commit_ack {
			if commit := response.commit {
				h.encode_commit_response(commit)
			} else {
				h.encode_error_response(domain.grpc_error_unknown, 'Missing commit response')
			}
		}
		.error {
			if err := response.error {
				h.encode_error_response(err.code, err.message)
			} else {
				h.encode_error_response(domain.grpc_error_unknown, 'Unknown error')
			}
		}
		.pong {
			if pong := response.pong {
				h.encode_pong_response(pong)
			} else {
				h.encode_pong_response(domain.GrpcPongResponse{
					timestamp: time.now().unix_milli()
				})
			}
		}
	}
}

// encode_produce_response encodes a produce response
fn (h &GrpcHandler) encode_produce_response(resp domain.GrpcProduceResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.error_msg.len}

	// Response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.produce_ack)

	// Topic length + topic
	buf << u8(resp.topic.len >> 8)
	buf << u8(resp.topic.len)
	buf << resp.topic.bytes()

	// Partition (4 bytes)
	buf << u8(resp.partition >> 24)
	buf << u8(resp.partition >> 16)
	buf << u8(resp.partition >> 8)
	buf << u8(resp.partition)

	// Base offset (8 bytes)
	buf << u8(resp.base_offset >> 56)
	buf << u8(resp.base_offset >> 48)
	buf << u8(resp.base_offset >> 40)
	buf << u8(resp.base_offset >> 32)
	buf << u8(resp.base_offset >> 24)
	buf << u8(resp.base_offset >> 16)
	buf << u8(resp.base_offset >> 8)
	buf << u8(resp.base_offset)

	// Record count (4 bytes)
	buf << u8(resp.record_count >> 24)
	buf << u8(resp.record_count >> 16)
	buf << u8(resp.record_count >> 8)
	buf << u8(resp.record_count)

	// Timestamp (8 bytes)
	buf << u8(resp.timestamp >> 56)
	buf << u8(resp.timestamp >> 48)
	buf << u8(resp.timestamp >> 40)
	buf << u8(resp.timestamp >> 32)
	buf << u8(resp.timestamp >> 24)
	buf << u8(resp.timestamp >> 16)
	buf << u8(resp.timestamp >> 8)
	buf << u8(resp.timestamp)

	// Error code (4 bytes)
	buf << u8(resp.error_code >> 24)
	buf << u8(resp.error_code >> 16)
	buf << u8(resp.error_code >> 8)
	buf << u8(resp.error_code)

	// Error message length + message
	buf << u8(resp.error_msg.len >> 8)
	buf << u8(resp.error_msg.len)
	buf << resp.error_msg.bytes()

	return buf
}

// encode_message_response encodes a message response
fn (h &GrpcHandler) encode_message_response(resp domain.GrpcMessageResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.key.len + resp.value.len}

	// Response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.message)

	// Topic length + topic
	buf << u8(resp.topic.len >> 8)
	buf << u8(resp.topic.len)
	buf << resp.topic.bytes()

	// Partition (4 bytes)
	buf << u8(resp.partition >> 24)
	buf << u8(resp.partition >> 16)
	buf << u8(resp.partition >> 8)
	buf << u8(resp.partition)

	// Offset (8 bytes)
	buf << u8(resp.offset >> 56)
	buf << u8(resp.offset >> 48)
	buf << u8(resp.offset >> 40)
	buf << u8(resp.offset >> 32)
	buf << u8(resp.offset >> 24)
	buf << u8(resp.offset >> 16)
	buf << u8(resp.offset >> 8)
	buf << u8(resp.offset)

	// Timestamp (8 bytes)
	buf << u8(resp.timestamp >> 56)
	buf << u8(resp.timestamp >> 48)
	buf << u8(resp.timestamp >> 40)
	buf << u8(resp.timestamp >> 32)
	buf << u8(resp.timestamp >> 24)
	buf << u8(resp.timestamp >> 16)
	buf << u8(resp.timestamp >> 8)
	buf << u8(resp.timestamp)

	// Key length + key
	buf << u8(resp.key.len >> 24)
	buf << u8(resp.key.len >> 16)
	buf << u8(resp.key.len >> 8)
	buf << u8(resp.key.len)
	buf << resp.key

	// Value length + value
	buf << u8(resp.value.len >> 24)
	buf << u8(resp.value.len >> 16)
	buf << u8(resp.value.len >> 8)
	buf << u8(resp.value.len)
	buf << resp.value

	// Header count + headers
	buf << u8(resp.headers.len >> 24)
	buf << u8(resp.headers.len >> 16)
	buf << u8(resp.headers.len >> 8)
	buf << u8(resp.headers.len)

	for k, v in resp.headers {
		// Key length + key
		buf << u8(k.len >> 8)
		buf << u8(k.len)
		buf << k.bytes()

		// Value length + value
		buf << u8(v.len >> 8)
		buf << u8(v.len)
		buf << v
	}

	return buf
}

// encode_commit_response encodes a commit response
fn (h &GrpcHandler) encode_commit_response(resp domain.GrpcCommitResponse) []u8 {
	mut buf := []u8{cap: 4 + resp.message.len}

	// Response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.commit_ack)

	// Success (1 byte)
	buf << if resp.success { u8(1) } else { u8(0) }

	// Message length + message
	buf << u8(resp.message.len >> 8)
	buf << u8(resp.message.len)
	buf << resp.message.bytes()

	return buf
}

// encode_error_response encodes an error response
fn (h &GrpcHandler) encode_error_response(code i32, message string) []u8 {
	mut buf := []u8{cap: 8 + message.len}

	// Response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.error)

	// Error code (4 bytes)
	buf << u8(code >> 24)
	buf << u8(code >> 16)
	buf << u8(code >> 8)
	buf << u8(code)

	// Message length + message
	buf << u8(message.len >> 8)
	buf << u8(message.len)
	buf << message.bytes()

	return buf
}

// encode_pong_response encodes a pong response
fn (h &GrpcHandler) encode_pong_response(resp domain.GrpcPongResponse) []u8 {
	mut buf := []u8{cap: 9}

	// Response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.pong)

	// Timestamp (8 bytes)
	buf << u8(resp.timestamp >> 56)
	buf << u8(resp.timestamp >> 48)
	buf << u8(resp.timestamp >> 40)
	buf << u8(resp.timestamp >> 32)
	buf << u8(resp.timestamp >> 24)
	buf << u8(resp.timestamp >> 16)
	buf << u8(resp.timestamp >> 8)
	buf << u8(resp.timestamp)

	return buf
}

// send_error sends an error frame
fn (mut h GrpcHandler) send_error(mut conn net.TcpConn, code i32, message string) {
	data := h.encode_error_response(code, message)
	h.send_frame(mut conn, data, false) or {}
}

// ============================================================================
// Statistics
// ============================================================================

// get_stats returns gRPC service statistics
pub fn (mut h GrpcHandler) get_stats() streaming.GrpcStats {
	return h.grpc_service.get_stats()
}

// get_connections returns all active gRPC connections
pub fn (mut h GrpcHandler) get_connections() []domain.GrpcConnection {
	return h.grpc_service.list_connections()
}
