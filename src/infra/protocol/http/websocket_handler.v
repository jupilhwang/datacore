// 인프라 레이어 - WebSocket 핸들러
// WebSocket 연결을 위한 HTTP 핸들러
module http

import crypto.sha1
import encoding.base64
import domain
import net
import service.streaming
import service.port
import time

// ============================================================================
// WebSocket 핸들러
// ============================================================================

/// WebSocketHandler는 WebSocket HTTP 요청을 처리합니다.
pub struct WebSocketHandler {
	config domain.WebSocketConfig
pub mut:
	ws_service &streaming.WebSocketService
	storage    port.StoragePort
}

/// new_websocket_handler는 새로운 WebSocket 핸들러를 생성합니다.
pub fn new_websocket_handler(storage port.StoragePort, config domain.WebSocketConfig) &WebSocketHandler {
	ws_service := streaming.new_websocket_service(storage, config)
	return &WebSocketHandler{
		config:     config
		ws_service: ws_service
		storage:    storage
	}
}

// ============================================================================
// WebSocket 업그레이드 처리
// ============================================================================

/// handle_upgrade는 WebSocket 업그레이드 요청을 처리합니다.
pub fn (mut h WebSocketHandler) handle_upgrade(mut conn net.TcpConn, headers map[string]string, client_ip string) !string {
	// WebSocket 업그레이드 요청 검증
	upgrade := headers['Upgrade'] or { headers['upgrade'] or { '' } }
	if upgrade.to_lower() != 'websocket' {
		return error('Invalid Upgrade header')
	}

	connection := headers['Connection'] or { headers['connection'] or { '' } }
	if !connection.to_lower().contains('upgrade') {
		return error('Invalid Connection header')
	}

	ws_key := headers['Sec-WebSocket-Key'] or { headers['sec-websocket-key'] or { '' } }
	if ws_key.len == 0 {
		return error('Missing Sec-WebSocket-Key header')
	}

	ws_version := headers['Sec-WebSocket-Version'] or { headers['sec-websocket-version'] or { '' } }
	if ws_version != '13' {
		return error('Unsupported WebSocket version')
	}

	// Accept 키 생성
	accept_key := generate_accept_key(ws_key)

	// WebSocket 연결 생성
	user_agent := headers['User-Agent'] or { headers['user-agent'] or { 'unknown' } }
	ws_conn := domain.new_ws_connection(client_ip, user_agent)

	// 연결 등록
	conn_id := h.ws_service.register_connection(ws_conn) or {
		return error('Failed to register connection: ${err}')
	}

	// 업그레이드 응답 전송
	response := 'HTTP/1.1 101 Switching Protocols\r\n' + 'Upgrade: websocket\r\n' +
		'Connection: Upgrade\r\n' + 'Sec-WebSocket-Accept: ${accept_key}\r\n' +
		'X-WebSocket-Connection-Id: ${conn_id}\r\n' + '\r\n'

	conn.write_string(response) or {
		h.ws_service.unregister_connection(conn_id) or {}
		return error('Failed to send upgrade response')
	}

	return conn_id
}

/// generate_accept_key는 Sec-WebSocket-Accept 키를 생성합니다.
fn generate_accept_key(key string) string {
	magic := '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
	combined := key + magic
	hash := sha1.sum(combined.bytes())
	return base64.encode(hash)
}

// ============================================================================
// WebSocket 프레임 처리
// ============================================================================

/// WebSocketOpcode는 WebSocket 프레임 opcode를 나타냅니다.
enum WebSocketOpcode {
	continuation = 0x0
	text         = 0x1
	binary       = 0x2
	close        = 0x8
	ping         = 0x9
	pong         = 0xa
}

/// WebSocketFrame은 WebSocket 프레임을 나타냅니다.
struct WebSocketFrame {
	fin     bool
	opcode  WebSocketOpcode
	masked  bool
	payload []u8
}

/// start_connection은 WebSocket 연결 처리를 시작합니다.
pub fn (mut h WebSocketHandler) start_connection(conn_id string, mut conn net.TcpConn) {
	defer {
		h.ws_service.unregister_connection(conn_id) or {}
		conn.close() or {}
	}

	// 송신 채널 획득
	send_chan := h.ws_service.get_send_channel(conn_id) or { return }

	// 송신 고루틴 시작
	spawn h.sender_loop(conn_id, mut conn, send_chan)

	// 폴링 고루틴 시작
	spawn h.poll_loop(conn_id)

	// 수신 루프 (메인 루프)
	h.receiver_loop(conn_id, mut conn)
}

/// receiver_loop는 수신되는 WebSocket 프레임을 처리합니다.
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
				// TODO: 분할된 프레임 메시지 처리 구현 필요
			}
		}
	}
}

/// sender_loop는 나가는 메시지를 처리합니다.
fn (mut h WebSocketHandler) sender_loop(conn_id string, mut conn net.TcpConn, recv_chan chan string) {
	for {
		// 채널에서 블로킹 수신 - 채널이 닫히면 종료
		msg := <-recv_chan or { break }
		h.send_text_frame(mut conn, msg) or { break }
	}
}

/// poll_loop는 새 메시지를 주기적으로 폴링합니다.
fn (mut h WebSocketHandler) poll_loop(conn_id string) {
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// 연결이 여전히 존재하는지 확인
		_ = h.ws_service.get_connection(conn_id) or { break }

		// 100ms마다 새 메시지 폴링
		if now - last_poll >= 100 {
			h.ws_service.poll_and_send()
			last_poll = now
		}

		time.sleep(50 * time.millisecond)
	}
}

// ============================================================================
// 프레임 읽기
// ============================================================================

/// read_frame은 연결에서 WebSocket 프레임을 읽습니다.
fn (mut h WebSocketHandler) read_frame(mut conn net.TcpConn) !WebSocketFrame {
	// 처음 2바이트 읽기 (FIN, RSV, Opcode, MASK, Payload len)
	mut header := []u8{len: 2}
	conn.read(mut header) or { return error('Failed to read frame header') }

	fin := (header[0] & 0x80) != 0
	opcode := unsafe { WebSocketOpcode(header[0] & 0x0F) }
	masked := (header[1] & 0x80) != 0
	mut payload_len := u64(header[1] & 0x7F)

	// 확장 페이로드 길이
	if payload_len == 126 {
		mut ext := []u8{len: 2}
		conn.read(mut ext) or { return error('Failed to read extended length') }
		payload_len = u64(ext[0]) << 8 | u64(ext[1])
	} else if payload_len == 127 {
		mut ext := []u8{len: 8}
		conn.read(mut ext) or { return error('Failed to read extended length') }
		payload_len = u64(ext[0]) << 56 | u64(ext[1]) << 48 | u64(ext[2]) << 40 | u64(ext[3]) << 32 | u64(ext[4]) << 24 | u64(ext[5]) << 16 | u64(ext[6]) << 8 | u64(ext[7])
	}

	// 최대 메시지 크기 확인
	if payload_len > u64(h.config.max_message_size) {
		return error('Message too large')
	}

	// 마스킹 키 읽기 (마스킹된 경우)
	mut mask_key := []u8{}
	if masked {
		mask_key = []u8{len: 4}
		conn.read(mut mask_key) or { return error('Failed to read mask key') }
	}

	// 페이로드 읽기
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

		// 페이로드 언마스킹
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

// ============================================================================
// 프레임 쓰기
// ============================================================================

/// send_text_frame은 텍스트 프레임을 전송합니다.
fn (mut h WebSocketHandler) send_text_frame(mut conn net.TcpConn, message string) ! {
	h.send_frame(mut conn, .text, message.bytes())!
}

/// send_pong은 pong 프레임을 전송합니다.
fn (mut h WebSocketHandler) send_pong(mut conn net.TcpConn, payload []u8) ! {
	h.send_frame(mut conn, .pong, payload)!
}

/// send_close는 close 프레임을 전송합니다.
fn (mut h WebSocketHandler) send_close(mut conn net.TcpConn, code u16, reason string) ! {
	mut payload := []u8{len: 2 + reason.len}
	payload[0] = u8(code >> 8)
	payload[1] = u8(code & 0xFF)
	for i, c in reason.bytes() {
		payload[2 + i] = c
	}
	h.send_frame(mut conn, .close, payload)!
}

/// send_frame은 WebSocket 프레임을 전송합니다.
fn (mut h WebSocketHandler) send_frame(mut conn net.TcpConn, opcode WebSocketOpcode, payload []u8) ! {
	mut frame := []u8{}

	// 첫 번째 바이트: FIN + Opcode
	frame << u8(0x80 | u8(opcode))

	// 두 번째 바이트: 페이로드 길이 (서버 프레임은 마스킹되지 않음)
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

	// 페이로드
	frame << payload

	conn.write(frame) or { return error('Failed to write frame') }
}

// ============================================================================
// 메시지 처리
// ============================================================================

/// handle_text_message는 텍스트 메시지를 처리합니다.
fn (mut h WebSocketHandler) handle_text_message(conn_id string, payload []u8) {
	message := payload.bytestr()

	// JSON 메시지 파싱
	msg := parse_ws_message(message) or {
		response := domain.new_ws_error_response('INVALID_MESSAGE', 'Failed to parse message: ${err}')
		h.ws_service.send_message(conn_id, response) or {}
		return
	}

	// 메시지 처리
	response := h.ws_service.handle_message(conn_id, msg) or {
		err_response := domain.new_ws_error_response('INTERNAL_ERROR', 'Failed to handle message: ${err}')
		h.ws_service.send_message(conn_id, err_response) or {}
		return
	}

	// 응답 전송
	h.ws_service.send_message(conn_id, response) or {}
}

/// handle_binary_message는 바이너리 메시지를 처리합니다.
fn (mut h WebSocketHandler) handle_binary_message(conn_id string, payload []u8) {
	// 현재는 바이너리를 텍스트로 처리
	h.handle_text_message(conn_id, payload)
}

/// handle_pong은 pong 프레임을 처리합니다.
fn (mut h WebSocketHandler) handle_pong(conn_id string) {
	// 마지막 pong 타임스탬프 업데이트
	h.ws_service.handle_message(conn_id, domain.WebSocketMessage{
		action: .ping
	}) or {}
}

/// handle_close는 close 프레임을 처리합니다.
fn (mut h WebSocketHandler) handle_close(conn_id string, mut conn net.TcpConn, payload []u8) {
	// close 코드 파싱
	code := if payload.len >= 2 {
		u16(payload[0]) << 8 | u16(payload[1])
	} else {
		u16(1000)
	}

	// close 응답 전송
	h.send_close(mut conn, code, '') or {}
}

// ============================================================================
// JSON 파싱
// ============================================================================

/// parse_ws_message는 JSON에서 WebSocket 메시지를 파싱합니다.
fn parse_ws_message(json_str string) !domain.WebSocketMessage {
	// 간단한 JSON 파싱 (TODO: 적절한 JSON 라이브러리로 교체 필요)
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

	// 헤더 파싱 (간소화)
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

/// extract_json_string은 JSON에서 문자열 값을 추출합니다.
fn extract_json_string(json_str string, key string) ?string {
	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return none }
	start := idx + pattern.len

	// 공백 건너뛰기
	mut pos := start
	for pos < json_str.len && json_str[pos] in [` `, `\t`, `\n`, `\r`] {
		pos++
	}

	if pos >= json_str.len {
		return none
	}

	// 문자열 값 확인
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

	// null 확인
	if json_str[pos..].starts_with('null') {
		return none
	}

	return none
}

/// extract_json_int는 JSON에서 정수 값을 추출합니다.
fn extract_json_int(json_str string, key string) ?int {
	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return none }
	start := idx + pattern.len

	// 공백 건너뛰기
	mut pos := start
	for pos < json_str.len && json_str[pos] in [` `, `\t`, `\n`, `\r`] {
		pos++
	}

	if pos >= json_str.len {
		return none
	}

	// 숫자 파싱
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

/// extract_json_object는 JSON에서 객체 값을 추출합니다 (간소화 - 단일 레벨 맵 반환).
fn extract_json_object(json_str string, key string) map[string]string {
	mut result := map[string]string{}

	pattern := '"${key}":'
	idx := json_str.index(pattern) or { return result }
	start := idx + pattern.len

	// 여는 중괄호 찾기
	mut pos := start
	for pos < json_str.len && json_str[pos] != `{` {
		pos++
	}
	if pos >= json_str.len {
		return result
	}

	// 매칭되는 닫는 중괄호 찾기
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

	// 키-값 쌍 파싱 (간소화)
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

/// unescape_json_string은 JSON 문자열 이스케이프 시퀀스를 언이스케이프합니다.
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

// ============================================================================
// 통계
// ============================================================================

/// get_stats는 WebSocket 서비스 통계를 반환합니다.
pub fn (mut h WebSocketHandler) get_stats() streaming.WebSocketStats {
	return h.ws_service.get_stats()
}

/// get_connections는 모든 활성 WebSocket 연결을 반환합니다.
pub fn (mut h WebSocketHandler) get_connections() []domain.WebSocketConnection {
	return h.ws_service.list_connections()
}
