// HTTP handler for WebSocket connections
module http

import crypto.sha1
import encoding.base64
import domain
import net
import service.streaming
import service.port
import time
import infra.observability

// Logging

/// log_message prints a structured log message.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('websocket.${component}')
	match level {
		.debug { logger.debug(message) }
		.info { logger.info(message) }
		.warn { logger.warn(message) }
		.error { logger.error(message) }
		else {}
	}
}

// WebSocket handler

/// WebSocketHandler handles WebSocket HTTP requests.
pub struct WebSocketHandler {
	config domain.WebSocketConfig
pub mut:
	ws_service &streaming.WebSocketService
	storage    port.StoragePort
	metrics    &observability.ProtocolMetrics
}

/// new_websocket_handler creates a new WebSocket handler.
pub fn new_websocket_handler(storage port.StoragePort, config domain.WebSocketConfig) &WebSocketHandler {
	ws_service := streaming.new_websocket_service(storage, config)
	metrics := observability.new_protocol_metrics()
	return &WebSocketHandler{
		config:     config
		ws_service: ws_service
		storage:    storage
		metrics:    metrics
	}
}

// WebSocket upgrade handling

/// handle_upgrade handles a WebSocket upgrade request.
pub fn (mut h WebSocketHandler) handle_upgrade(mut conn net.TcpConn, headers map[string]string, client_ip string) !string {
	start_time := time.now()
	mut success := true

	// validate WebSocket upgrade request
	upgrade := headers['Upgrade'] or { headers['upgrade'] or { '' } }
	if upgrade.to_lower() != 'websocket' {
		success = false
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Invalid Upgrade header', {
			'client_ip': client_ip
		})
		return error('Invalid Upgrade header')
	}

	connection := headers['Connection'] or { headers['connection'] or { '' } }
	if !connection.to_lower().contains('upgrade') {
		success = false
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Invalid Connection header', {
			'client_ip': client_ip
		})
		return error('Invalid Connection header')
	}

	ws_key := headers['Sec-WebSocket-Key'] or { headers['sec-websocket-key'] or { '' } }
	if ws_key.len == 0 {
		success = false
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Missing Sec-WebSocket-Key header', {
			'client_ip': client_ip
		})
		return error('Missing Sec-WebSocket-Key header')
	}

	ws_version := headers['Sec-WebSocket-Version'] or { headers['sec-websocket-version'] or { '' } }
	if ws_version != '13' {
		success = false
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Unsupported WebSocket version', {
			'client_ip': client_ip
			'version':   ws_version
		})
		return error('Unsupported WebSocket version')
	}

	// generate Accept key
	accept_key := generate_accept_key(ws_key)

	// create WebSocket connection
	user_agent := headers['User-Agent'] or { headers['user-agent'] or { 'unknown' } }
	ws_conn := domain.new_ws_connection(client_ip, user_agent)

	// register connection
	conn_id := h.ws_service.register_connection(ws_conn) or {
		success = false
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Failed to register connection', {
			'client_ip': client_ip
			'error':     err.msg()
		})
		return error('Failed to register connection: ${err}')
	}

	// send upgrade response
	response := 'HTTP/1.1 101 Switching Protocols\r\n' + 'Upgrade: websocket\r\n' +
		'Connection: Upgrade\r\n' + 'Sec-WebSocket-Accept: ${accept_key}\r\n' +
		'X-WebSocket-Connection-Id: ${conn_id}\r\n' + '\r\n'

	conn.write_string(response) or {
		success = false
		h.ws_service.unregister_connection(conn_id) or {}
		h.metrics.record_request('ws_upgrade', time.since(start_time).milliseconds(),
			success, 0, 0)
		log_message(.error, 'Upgrade', 'Failed to send upgrade response', {
			'client_ip': client_ip
			'conn_id':   conn_id
		})
		return error('Failed to send upgrade response')
	}

	// record metrics
	elapsed_ms := time.since(start_time).milliseconds()
	h.metrics.record_request('ws_upgrade', elapsed_ms, success, 0, response.len)

	log_message(.info, 'Upgrade', 'WebSocket upgrade successful', {
		'client_ip': client_ip
		'conn_id':   conn_id
	})

	return conn_id
}

/// generate_accept_key generates the Sec-WebSocket-Accept key.
fn generate_accept_key(key string) string {
	magic := '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
	combined := key + magic
	hash := sha1.sum(combined.bytes())
	return base64.encode(hash)
}

// WebSocket frame handling

/// WebSocketOpcode represents a WebSocket frame opcode.
enum WebSocketOpcode {
	continuation = 0x0
	text         = 0x1
	binary       = 0x2
	close        = 0x8
	ping         = 0x9
	pong         = 0xa
}

/// WebSocketFrame represents a WebSocket frame.
struct WebSocketFrame {
	fin     bool
	opcode  WebSocketOpcode
	masked  bool
	payload []u8
}

/// start_connection starts WebSocket connection processing.
pub fn (mut h WebSocketHandler) start_connection(conn_id string, mut conn net.TcpConn) {
	defer {
		h.ws_service.unregister_connection(conn_id) or {}
		conn.close() or {}
	}

	// acquire send channel
	send_chan := h.ws_service.get_send_channel(conn_id) or { return }

	// start sender goroutine
	spawn h.sender_loop(conn_id, mut conn, send_chan)

	// start polling goroutine
	spawn h.poll_loop(conn_id)

	// receiver loop (main loop)
	h.receiver_loop(conn_id, mut conn)
}

/// receiver_loop processes incoming WebSocket frames.
fn (mut h WebSocketHandler) receiver_loop(conn_id string, mut conn net.TcpConn) {
	for {
		frame := h.read_frame(mut conn) or { break }

		match frame.opcode {
			.text {
				h.handle_text_message(conn_id, frame.payload)
			}
			.binary {
				h.handle_binary_message(conn_id, frame.payload)
			}
			.ping {
				h.send_pong(mut conn, frame.payload) or { break }
			}
			.pong {
				h.handle_pong(conn_id)
			}
			.close {
				h.handle_close(conn_id, mut conn, frame.payload)
				break
			}
			.continuation {
				// TODO: implement fragmented frame message handling
			}
		}
	}
}

/// sender_loop processes outgoing messages.
fn (mut h WebSocketHandler) sender_loop(conn_id string, mut conn net.TcpConn, recv_chan chan string) {
	for {
		// blocking receive from channel - exit when channel closes
		msg := <-recv_chan or { break }
		h.send_text_frame(mut conn, msg) or { break }
	}
}

/// poll_loop periodically polls for new messages.
fn (mut h WebSocketHandler) poll_loop(conn_id string) {
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// verify connection still exists
		_ = h.ws_service.get_connection(conn_id) or { break }

		// poll for new messages every 100ms
		if now - last_poll >= 100 {
			h.ws_service.poll_and_send()
			last_poll = now
		}

		time.sleep(50 * time.millisecond)
	}
}

// Frame reading

/// read_frame reads a WebSocket frame from the connection.
fn (mut h WebSocketHandler) read_frame(mut conn net.TcpConn) !WebSocketFrame {
	// read first 2 bytes (FIN, RSV, Opcode, MASK, Payload len)
	mut header := []u8{len: 2}
	conn.read(mut header) or { return error('Failed to read frame header') }

	fin := (header[0] & 0x80) != 0
	opcode := unsafe { WebSocketOpcode(header[0] & 0x0F) }
	masked := (header[1] & 0x80) != 0
	mut payload_len := u64(header[1] & 0x7F)

	// extended payload length
	if payload_len == 126 {
		mut ext := []u8{len: 2}
		conn.read(mut ext) or { return error('Failed to read extended length') }
		payload_len = u64(ext[0]) << 8 | u64(ext[1])
	} else if payload_len == 127 {
		mut ext := []u8{len: 8}
		conn.read(mut ext) or { return error('Failed to read extended length') }
		payload_len = u64(ext[0]) << 56 | u64(ext[1]) << 48 | u64(ext[2]) << 40 | u64(ext[3]) << 32 | u64(ext[4]) << 24 | u64(ext[5]) << 16 | u64(ext[6]) << 8 | u64(ext[7])
	}

	// check maximum message size
	if payload_len > u64(h.config.max_message_size) {
		return error('Message too large')
	}

	// read masking key (if masked)
	mut mask_key := []u8{}
	if masked {
		mask_key = []u8{len: 4}
		conn.read(mut mask_key) or { return error('Failed to read mask key') }
	}

	// read payload
	mut payload := []u8{len: int(payload_len)}
	if payload_len > 0 {
		mut total_read := 0
		for total_read < int(payload_len) {
			n := conn.read(mut payload[total_read..]) or { return error('Failed to read payload') }
			if n == 0 {
				break
			}
			total_read += n
		}

		// unmask payload
		if masked {
			for i := 0; i < payload.len; i++ {
				payload[i] ^= mask_key[i % 4]
			}
		}
	}

	return WebSocketFrame{
		fin:     fin
		opcode:  opcode
		masked:  masked
		payload: payload
	}
}

// Frame writing

/// send_text_frame sends a text frame.
fn (mut h WebSocketHandler) send_text_frame(mut conn net.TcpConn, message string) ! {
	h.send_frame(mut conn, .text, message.bytes())!
}

/// send_pong sends a pong frame.
fn (mut h WebSocketHandler) send_pong(mut conn net.TcpConn, payload []u8) ! {
	h.send_frame(mut conn, .pong, payload)!
}

/// send_close sends a close frame.
fn (mut h WebSocketHandler) send_close(mut conn net.TcpConn, code u16, reason string) ! {
	mut payload := []u8{len: 2 + reason.len}
	payload[0] = u8(code >> 8)
	payload[1] = u8(code & 0xFF)
	for i, c in reason.bytes() {
		payload[2 + i] = c
	}
	h.send_frame(mut conn, .close, payload)!
}

/// send_frame sends a WebSocket frame.
fn (mut h WebSocketHandler) send_frame(mut conn net.TcpConn, opcode WebSocketOpcode, payload []u8) ! {
	mut frame := []u8{}

	// first byte: FIN + Opcode
	frame << u8(0x80 | u8(opcode))

	// second byte: payload length (server frames are not masked)
	if payload.len < 126 {
		frame << u8(payload.len)
	} else if payload.len < 65536 {
		frame << u8(126)
		frame << u8(payload.len >> 8)
		frame << u8(payload.len & 0xFF)
	} else {
		frame << u8(127)
		for i := 7; i >= 0; i-- {
			frame << u8((payload.len >> (i * 8)) & 0xFF)
		}
	}

	// payload
	frame << payload

	conn.write(frame) or { return error('Failed to write frame') }
}

// Message handling

/// handle_text_message handles a text message.
fn (mut h WebSocketHandler) handle_text_message(conn_id string, payload []u8) {
	message := payload.bytestr()

	// parse JSON message
	msg := parse_ws_message(message) or {
		response := domain.new_ws_error_response('INVALID_MESSAGE', 'Failed to parse message: ${err}')
		h.ws_service.send_message(conn_id, response) or {}
		return
	}

	// Message handling
	response := h.ws_service.handle_message(conn_id, msg) or {
		err_response := domain.new_ws_error_response('INTERNAL_ERROR', 'Failed to handle message: ${err}')
		h.ws_service.send_message(conn_id, err_response) or {}
		return
	}

	// send response
	h.ws_service.send_message(conn_id, response) or {}
}

/// handle_binary_message handles a binary message.
fn (mut h WebSocketHandler) handle_binary_message(conn_id string, payload []u8) {
	// currently treat binary as text
	h.handle_text_message(conn_id, payload)
}

/// handle_pong handles a pong frame.
fn (mut h WebSocketHandler) handle_pong(conn_id string) {
	// update last pong timestamp
	h.ws_service.handle_message(conn_id, domain.WebSocketMessage{
		action: .ping
	}) or {}
}

/// handle_close handles a close frame.
fn (mut h WebSocketHandler) handle_close(conn_id string, mut conn net.TcpConn, payload []u8) {
	// parse close code
	code := if payload.len >= 2 {
		u16(payload[0]) << 8 | u16(payload[1])
	} else {
		u16(1000)
	}

	// send close response
	h.send_close(mut conn, code, '') or {}
}

// JSON parsing

/// parse_ws_message parses a WebSocket message from JSON.
fn parse_ws_message(json_str string) !domain.WebSocketMessage {
	// simple JSON parsing (TODO: replace with a proper JSON library)
	action_str := extract_json_string(json_str, 'action') or { return error('Missing action') }
	action := domain.websocket_action_from_str(action_str) or {
		return error('Invalid action: ${action_str}')
	}

	topic := extract_json_string(json_str, 'topic') or { '' }

	partition := if p := extract_json_int(json_str, 'partition') {
		i32(p)
	} else {
		none
	}

	offset := extract_json_string(json_str, 'offset')
	key := extract_json_string(json_str, 'key')
	value := extract_json_string(json_str, 'value')
	group_id := extract_json_string(json_str, 'group_id')

	// header parsing (simplified)
	headers := extract_json_object(json_str, 'headers')

	return domain.WebSocketMessage{
		action:    action
		topic:     topic
		partition: partition
		offset:    offset
		key:       key
		value:     value
		headers:   headers
		group_id:  group_id
	}
}

/// extract_json_string extracts a string value from JSON.
fn extract_json_string(json_str string, key string) ?string {
	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return none }
	start := idx + pattern.len

	// skip whitespace
	mut pos := start
	for pos < json_str.len && json_str[pos] in [` `, `\t`, `\n`, `\r`] {
		pos++
	}

	if pos >= json_str.len {
		return none
	}

	// check string value
	if json_str[pos] == `"` {
		pos++
		mut end := pos
		for end < json_str.len && json_str[end] != `"` {
			if json_str[end] == `\\` {
				end++
			}
			end++
		}
		return unescape_json_string(json_str[pos..end])
	}

	// check for null
	if json_str[pos..].starts_with('null') {
		return none
	}

	return none
}

/// extract_json_int extracts an integer value from JSON.
fn extract_json_int(json_str string, key string) ?int {
	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return none }
	start := idx + pattern.len

	// skip whitespace
	mut pos := start
	for pos < json_str.len && json_str[pos] in [` `, `\t`, `\n`, `\r`] {
		pos++
	}

	if pos >= json_str.len {
		return none
	}

	// parse number
	mut end := pos
	if json_str[end] == `-` {
		end++
	}
	for end < json_str.len && json_str[end] >= `0` && json_str[end] <= `9` {
		end++
	}

	if end == pos {
		return none
	}

	return json_str[pos..end].int()
}

/// extract_json_object extracts an object value from JSON (simplified - returns single-level map).
fn extract_json_object(json_str string, key string) map[string]string {
	mut result := map[string]string{}

	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return result }
	start := idx + pattern.len

	// find opening brace
	mut pos := start
	for pos < json_str.len && json_str[pos] != `{` {
		pos++
	}
	if pos >= json_str.len {
		return result
	}

	// find matching closing brace
	mut depth := 1
	mut obj_start := pos + 1
	pos++
	for pos < json_str.len && depth > 0 {
		if json_str[pos] == `{` {
			depth++
		} else if json_str[pos] == `}` {
			depth--
		}
		pos++
	}

	if depth != 0 {
		return result
	}

	obj_str := json_str[obj_start..pos - 1]

	// parse key-value pairs (simplified)
	mut in_key := true
	mut current_key := ''
	mut i := 0
	for i < obj_str.len {
		if obj_str[i] == `"` {
			i++
			mut end := i
			for end < obj_str.len && obj_str[end] != `"` {
				if obj_str[end] == `\\` {
					end++
				}
				end++
			}
			str_val := obj_str[i..end]
			if in_key {
				current_key = str_val
			} else {
				result[current_key] = str_val
			}
			i = end + 1
		} else if obj_str[i] == `:` {
			in_key = false
			i++
		} else if obj_str[i] == `,` {
			in_key = true
			i++
		} else {
			i++
		}
	}

	return result
}

/// unescape_json_string unescapes JSON string escape sequences.
fn unescape_json_string(s string) string {
	mut result := ''
	mut i := 0
	for i < s.len {
		if s[i] == `\\` && i + 1 < s.len {
			result += match s[i + 1] {
				`"` { '"' }
				`\\` { '\\' }
				`n` { '\n' }
				`r` { '\r' }
				`t` { '\t' }
				else { s[i + 1].ascii_str() }
			}
			i += 2
		} else {
			result += s[i].ascii_str()
			i++
		}
	}
	return result
}

// Statistics

/// get_stats returns WebSocket service statistics.
pub fn (mut h WebSocketHandler) get_stats() streaming.WebSocketStats {
	return h.ws_service.get_stats()
}

/// get_connections returns all active WebSocket connections.
pub fn (mut h WebSocketHandler) get_connections() []domain.WebSocketConnection {
	return h.ws_service.list_connections()
}
