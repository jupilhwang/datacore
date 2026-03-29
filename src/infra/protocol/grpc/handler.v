// HTTP/2-based gRPC protocol handler (streaming support)
module grpc

import domain
import net
import service.port
import time
import infra.observability
import infra.performance.core

const default_grpc_max_bytes = 1048576
const grpc_send_poll_interval = 50 * time.millisecond

// GrpcHandler handles gRPC connections over HTTP/2.
/// GrpcHandler handles gRPC connections over HTTP/2.
pub struct GrpcHandler {
	config domain.GrpcConfig
pub mut:
	grpc_service port.GrpcServicePort
	storage      port.StoragePort
	metrics      &observability.ProtocolMetrics
}

/// new_grpc_handler creates a new gRPC handler.
pub fn new_grpc_handler(grpc_service port.GrpcServicePort, storage port.StoragePort, config domain.GrpcConfig) &GrpcHandler {
	metrics := observability.new_protocol_metrics()
	return &GrpcHandler{
		config:       config
		grpc_service: grpc_service
		storage:      storage
		metrics:      metrics
	}
}

// Connection handling

/// handle_connection handles a new gRPC connection.
pub fn (mut h GrpcHandler) handle_connection(mut conn net.TcpConn, client_ip string) {
	// create gRPC connection
	grpc_conn := domain.new_grpc_connection(client_ip, .bidirectional)

	// register connection
	conn_id := h.grpc_service.register_connection(grpc_conn) or {
		h.send_error(mut conn, domain.grpc_error_unknown, 'Failed to register connection')
		conn.close() or {}
		return
	}

	defer {
		h.grpc_service.unregister_connection(conn_id) or {}
		conn.close() or {}
	}

	// acquire send channel
	send_chan := h.grpc_service.get_send_channel(conn_id) or { return }

	// start sender goroutine
	spawn h.sender_loop(conn_id, mut conn, send_chan)

	// start polling goroutine for subscriptions
	spawn h.poll_loop(conn_id)

	// receiver loop (main loop)
	h.receiver_loop(conn_id, mut conn)
}

/// receiver_loop processes incoming gRPC messages.
fn (mut h GrpcHandler) receiver_loop(conn_id string, mut conn net.TcpConn) {
	for {
		// read frame header
		frame := h.read_frame(mut conn) or { break }

		// parse and handle the request
		response := h.handle_frame(conn_id, frame)

		// send response if needed
		if response.response_type != .pong {
			h.send_response(mut conn, response) or { break }
		}
	}
}

/// sender_loop processes outgoing messages from the channel.
fn (mut h GrpcHandler) sender_loop(conn_id string, mut conn net.TcpConn, recv_chan chan domain.GrpcStreamResponse) {
	for {
		// blocking receive from channel
		response := <-recv_chan or { break }
		h.send_response(mut conn, response) or { break }
	}
}

/// poll_loop periodically polls for new messages for subscriptions.
fn (mut h GrpcHandler) poll_loop(conn_id string) {
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// verify connection still exists
		_ = h.grpc_service.get_connection(conn_id) or { break }

		// poll for new messages every 100ms
		if now - last_poll >= 100 {
			h.grpc_service.poll_and_send()
			last_poll = now
		}

		time.sleep(grpc_send_poll_interval)
	}
}

// Frame read/write

/// GrpcFrame represents a gRPC frame.
struct GrpcFrame {
	compressed bool
	length     u32
	data       []u8
}

/// read_frame reads a gRPC frame from the connection.
fn (mut h GrpcHandler) read_frame(mut conn net.TcpConn) !GrpcFrame {
	// read 5-byte header: compressed (1) + length (4)
	mut header := []u8{len: 5}
	total_read := conn.read(mut header) or { return error('Failed to read frame header') }
	if total_read < 5 {
		return error('Incomplete frame header')
	}

	compressed := header[0] == 1
	length := u32(core.read_i32_be(header[1..5]))

	// check maximum message size
	if length > u32(h.config.max_message_size) {
		return error('Message too large: ${length} > ${h.config.max_message_size}')
	}

	// read payload
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

/// send_frame sends a gRPC frame to the connection.
fn (mut h GrpcHandler) send_frame(mut conn net.TcpConn, data []u8, compressed bool) ! {
	// build 5-byte header
	length := u32(data.len)
	mut frame := []u8{cap: 5 + data.len}

	// compression flag
	frame << if compressed { u8(1) } else { u8(0) }

	// length (big-endian)
	core.write_u32_be(mut frame, length)

	// payload
	frame << data

	conn.write(frame) or { return error('Failed to write frame') }
}

// Request handling

/// handle_frame parses and handles a gRPC frame.
fn (mut h GrpcHandler) handle_frame(conn_id string, frame GrpcFrame) domain.GrpcStreamResponse {
	start_time := time.now()
	mut success := true
	mut api_name := 'unknown'

	// parse frame data into a stream request
	req := h.parse_stream_request(frame.data) or {
		success = false
		return domain.GrpcStreamResponse{
			response_type: .error
			error:         domain.GrpcErrorResponse{
				code:    domain.grpc_error_invalid_message
				message: 'Failed to parse request: ${err}'
			}
		}
	}

	// set API name
	api_name = match req.request_type {
		.produce { 'produce' }
		.subscribe { 'subscribe' }
		.commit { 'commit' }
		.ack { 'ack' }
		.ping { 'ping' }
	}

	// Request handling
	response := h.grpc_service.handle_stream_request(conn_id, req)

	// check for error
	if response.response_type == .error {
		success = false
	}

	// record metrics
	elapsed_ms := time.since(start_time).milliseconds()
	h.metrics.record_request('grpc_${api_name}', elapsed_ms, success, frame.data.len,
		h.encode_stream_response(response).len)

	return response
}

/// parse_stream_request parses binary data into a GrpcStreamRequest.
fn (h &GrpcHandler) parse_stream_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 1 {
		return error('Empty request')
	}

	// first byte is the request type
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

/// parse_produce_request parses a produce request from binary data.
fn (h &GrpcHandler) parse_produce_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for produce request')
	}

	mut pos := 0

	// topic length (2 bytes) + topic
	topic_len := int(core.read_i16_be(data[pos..pos + 2]))
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// partition (4 bytes, -1 means auto)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition_val := core.read_i32_be(data[pos..pos + 4])
	pos += 4
	partition := if partition_val >= 0 { i32(partition_val) } else { none }

	// record count (4 bytes)
	if pos + 4 > data.len {
		return error('Missing record count')
	}
	record_count := int(core.read_i32_be(data[pos..pos + 4]))
	pos += 4

	// parse records
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

/// parse_consume_request parses a consume request from binary data.
fn (h &GrpcHandler) parse_consume_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 18 {
		return error('Data too short for consume request')
	}

	mut pos := 0

	// topic length (2 bytes) + topic
	topic_len := int(core.read_i16_be(data[pos..pos + 2]))
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	partition := core.read_i32_be(data[pos..pos + 4])
	pos += 4

	// offset (8 bytes)
	if pos + 8 > data.len {
		return error('Missing offset')
	}
	offset := core.read_i64_be(data[pos..pos + 8])
	pos += 8

	// max record count (4 bytes)
	if pos + 4 > data.len {
		return error('Missing max_records')
	}
	max_records := int(core.read_i32_be(data[pos..pos + 4]))
	pos += 4

	// max byte count (4 bytes)
	max_bytes := if pos + 4 <= data.len {
		int(core.read_i32_be(data[pos..pos + 4]))
	} else {
		default_grpc_max_bytes
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

/// parse_commit_request parses a commit request from binary data.
fn (h &GrpcHandler) parse_commit_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for commit request')
	}

	mut pos := 0

	// group ID length (2 bytes) + group_id
	group_len := int(core.read_i16_be(data[pos..pos + 2]))
	pos += 2
	if pos + group_len > data.len {
		return error('Invalid group_id length')
	}
	group_id := data[pos..pos + group_len].bytestr()
	pos += group_len

	// offset count (4 bytes)
	if pos + 4 > data.len {
		return error('Missing offset count')
	}
	offset_count := int(core.read_i32_be(data[pos..pos + 4]))
	pos += 4

	// parse offsets
	mut offsets := []domain.GrpcPartitionOffset{cap: offset_count}
	for _ in 0 .. offset_count {
		// topic length + topic
		if pos + 2 > data.len {
			return error('Missing topic length')
		}
		topic_len := int(core.read_i16_be(data[pos..pos + 2]))
		pos += 2
		if pos + topic_len > data.len {
			return error('Invalid topic length')
		}
		topic := data[pos..pos + topic_len].bytestr()
		pos += topic_len

		// partition (4 bytes)
		if pos + 4 > data.len {
			return error('Missing partition')
		}
		partition := core.read_i32_be(data[pos..pos + 4])
		pos += 4

		// offset (8 bytes)
		if pos + 8 > data.len {
			return error('Missing offset')
		}
		offset := core.read_i64_be(data[pos..pos + 8])
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

/// parse_ack_request parses an ack request from binary data.
fn (h &GrpcHandler) parse_ack_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 14 {
		return error('Data too short for ack request')
	}

	mut pos := 0

	// topic length (2 bytes) + topic
	topic_len := int(core.read_i16_be(data[pos..pos + 2]))
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// partition (4 bytes)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition := core.read_i32_be(data[pos..pos + 4])
	pos += 4

	// offset (8 bytes)
	if pos + 8 > data.len {
		return error('Missing offset')
	}
	offset := core.read_i64_be(data[pos..pos + 8])

	return domain.GrpcStreamRequest{
		request_type: .ack
		ack:          domain.GrpcAckRequest{
			topic:     topic
			partition: partition
			offset:    offset
		}
	}
}

// Response encoding

/// send_response encodes and sends a response.
fn (mut h GrpcHandler) send_response(mut conn net.TcpConn, response domain.GrpcStreamResponse) ! {
	data := h.encode_stream_response(response)
	h.send_frame(mut conn, data, false)!
}

/// encode_stream_response encodes a stream response to binary.
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

/// encode_produce_response encodes a produce response.
fn (h &GrpcHandler) encode_produce_response(resp domain.GrpcProduceResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.error_msg.len}

	// response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.produce_ack)

	// topic length + topic
	core.write_i16_be(mut buf, i16(resp.topic.len))
	buf << resp.topic.bytes()

	// partition (4 bytes)
	core.write_i32_be(mut buf, resp.partition)

	// base offset (8 bytes)
	core.write_i64_be(mut buf, resp.base_offset)

	// record count (4 bytes)
	core.write_i32_be(mut buf, i32(resp.record_count))

	// timestamp (8 bytes)
	core.write_i64_be(mut buf, resp.timestamp)

	// error code (4 bytes)
	core.write_i32_be(mut buf, resp.error_code)

	// error message length + message
	core.write_i16_be(mut buf, i16(resp.error_msg.len))
	buf << resp.error_msg.bytes()

	return buf
}

/// encode_message_response encodes a message response.
fn (h &GrpcHandler) encode_message_response(resp domain.GrpcMessageResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.key.len + resp.value.len}

	// response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.message)

	// topic length + topic
	core.write_i16_be(mut buf, i16(resp.topic.len))
	buf << resp.topic.bytes()

	// partition (4 bytes)
	core.write_i32_be(mut buf, resp.partition)

	// offset (8 bytes)
	core.write_i64_be(mut buf, resp.offset)

	// timestamp (8 bytes)
	core.write_i64_be(mut buf, resp.timestamp)

	// key length + key (4 bytes)
	core.write_i32_be(mut buf, i32(resp.key.len))
	buf << resp.key

	// value length + value (4 bytes)
	core.write_i32_be(mut buf, i32(resp.value.len))
	buf << resp.value

	// header count + headers (4 bytes)
	core.write_i32_be(mut buf, i32(resp.headers.len))

	for k, v in resp.headers {
		core.write_i16_be(mut buf, i16(k.len))
		buf << k.bytes()

		core.write_i16_be(mut buf, i16(v.len))
		buf << v
	}

	return buf
}

/// encode_commit_response encodes a commit response.
fn (h &GrpcHandler) encode_commit_response(resp domain.GrpcCommitResponse) []u8 {
	mut buf := []u8{cap: 4 + resp.message.len}

	// response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.commit_ack)

	// success flag (1 byte)
	buf << if resp.success { u8(1) } else { u8(0) }

	// message length + message
	core.write_i16_be(mut buf, i16(resp.message.len))
	buf << resp.message.bytes()

	return buf
}

/// encode_error_response encodes an error response.
fn (h &GrpcHandler) encode_error_response(code i32, message string) []u8 {
	mut buf := []u8{cap: 8 + message.len}

	// response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.error)

	// error code (4 bytes)
	core.write_i32_be(mut buf, code)

	// message length + message
	core.write_i16_be(mut buf, i16(message.len))
	buf << message.bytes()

	return buf
}

/// encode_pong_response encodes a pong response.
fn (h &GrpcHandler) encode_pong_response(resp domain.GrpcPongResponse) []u8 {
	mut buf := []u8{cap: 9}

	// response type (1 byte)
	buf << u8(domain.GrpcStreamResponseType.pong)

	core.write_i64_be(mut buf, resp.timestamp)

	return buf
}

/// send_error sends an error frame.
fn (mut h GrpcHandler) send_error(mut conn net.TcpConn, code i32, message string) {
	data := h.encode_error_response(code, message)
	h.send_frame(mut conn, data, false) or {
		observability.log_with_context('grpc', .error, 'Send', 'Failed to send error frame: ${err}',
			{
			'code':    code.str()
			'message': message
		})
	}
}

// Statistics

/// get_stats returns gRPC service statistics.
pub fn (mut h GrpcHandler) get_stats() port.GrpcServiceStats {
	return h.grpc_service.get_stats()
}

/// get_connections returns all active gRPC connections.
pub fn (mut h GrpcHandler) get_connections() []domain.GrpcConnection {
	return h.grpc_service.list_connections()
}

// Metrics query

/// get_metrics_summary returns a summary of gRPC protocol metrics.
pub fn (mut h GrpcHandler) get_metrics_summary() string {
	return h.metrics.get_summary()
}

/// get_metrics returns the gRPC protocol metrics struct.
pub fn (mut h GrpcHandler) get_metrics() &observability.ProtocolMetrics {
	return h.metrics
}

/// reset_metrics resets all gRPC protocol metrics.
pub fn (mut h GrpcHandler) reset_metrics() {
	h.metrics.reset()
}
