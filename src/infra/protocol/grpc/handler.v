// 인프라 레이어 - gRPC 핸들러
// HTTP/2 기반 gRPC 프로토콜 핸들러 (스트리밍 지원)
module grpc

import domain
import net
import service.port
import service.streaming
import time
import infra.observability

// ============================================================
// 로깅 (Logging)
// ============================================================

/// log_message는 구조화된 로그 메시지를 출력합니다.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	logger := observability.get_named_logger('grpc.${component}')
	match level {
		.debug { logger.debug(message, context) }
		.info { logger.info(message, context) }
		.warn { logger.warn(message, context) }
		.error { logger.error(message, context) }
	}
}

// ============================================================================
// gRPC 핸들러
// ============================================================================

// GrpcHandler는 HTTP/2를 통한 gRPC 연결을 처리합니다.
pub struct GrpcHandler {
	config domain.GrpcConfig
pub mut:
	grpc_service &streaming.GrpcService
	storage      port.StoragePort
	metrics      &observability.ProtocolMetrics // gRPC 메트릭 수집
}

/// new_grpc_handler는 새로운 gRPC 핸들러를 생성합니다.
pub fn new_grpc_handler(storage port.StoragePort, config domain.GrpcConfig) &GrpcHandler {
	grpc_service := streaming.new_grpc_service(storage, config)
	metrics := observability.new_protocol_metrics()
	return &GrpcHandler{
		config:       config
		grpc_service: grpc_service
		storage:      storage
		metrics:      metrics
	}
}

// ============================================================================
// 연결 처리
// ============================================================================

/// handle_connection은 새로운 gRPC 연결을 처리합니다.
pub fn (mut h GrpcHandler) handle_connection(mut conn net.TcpConn, client_ip string) {
	// gRPC 연결 생성
	grpc_conn := domain.new_grpc_connection(client_ip, .bidirectional)

	// 연결 등록
	conn_id := h.grpc_service.register_connection(grpc_conn) or {
		h.send_error(mut conn, domain.grpc_error_unknown, 'Failed to register connection')
		conn.close() or {}
		return
	}

	defer {
		h.grpc_service.unregister_connection(conn_id) or {}
		conn.close() or {}
	}

	// 송신 채널 획득
	send_chan := h.grpc_service.get_send_channel(conn_id) or { return }

	// 송신 고루틴 시작
	spawn h.sender_loop(conn_id, mut conn, send_chan)

	// 구독을 위한 폴링 고루틴 시작
	spawn h.poll_loop(conn_id)

	// 수신 루프 (메인 루프)
	h.receiver_loop(conn_id, mut conn)
}

/// receiver_loop는 수신되는 gRPC 메시지를 처리합니다.
fn (mut h GrpcHandler) receiver_loop(conn_id string, mut conn net.TcpConn) {
	for {
		// 프레임 헤더 읽기
		frame := h.read_frame(mut conn) or { break }

		// 요청 파싱 및 처리
		response := h.handle_frame(conn_id, frame)

		// 필요시 응답 전송
		if response.response_type != .pong {
			h.send_response(mut conn, response) or { break }
		}
	}
}

/// sender_loop는 채널에서 나가는 메시지를 처리합니다.
fn (mut h GrpcHandler) sender_loop(conn_id string, mut conn net.TcpConn, recv_chan chan domain.GrpcStreamResponse) {
	for {
		// 채널에서 블로킹 수신
		response := <-recv_chan or { break }
		h.send_response(mut conn, response) or { break }
	}
}

/// poll_loop는 구독에 대한 새 메시지를 주기적으로 폴링합니다.
fn (mut h GrpcHandler) poll_loop(conn_id string) {
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// 연결이 여전히 존재하는지 확인
		_ = h.grpc_service.get_connection(conn_id) or { break }

		// 100ms마다 새 메시지 폴링
		if now - last_poll >= 100 {
			h.grpc_service.poll_and_send()
			last_poll = now
		}

		time.sleep(50 * time.millisecond)
	}
}

// ============================================================================
// 프레임 읽기/쓰기
// ============================================================================

/// GrpcFrame은 gRPC 프레임을 나타냅니다.
struct GrpcFrame {
	compressed bool
	length     u32
	data       []u8
}

/// read_frame은 연결에서 gRPC 프레임을 읽습니다.
fn (mut h GrpcHandler) read_frame(mut conn net.TcpConn) !GrpcFrame {
	// 5바이트 헤더 읽기: compressed (1) + length (4)
	mut header := []u8{len: 5}
	total_read := conn.read(mut header) or { return error('Failed to read frame header') }
	if total_read < 5 {
		return error('Incomplete frame header')
	}

	compressed := header[0] == 1
	length := u32(header[1]) << 24 | u32(header[2]) << 16 | u32(header[3]) << 8 | u32(header[4])

	// 최대 메시지 크기 확인
	if length > u32(h.config.max_message_size) {
		return error('Message too large: ${length} > ${h.config.max_message_size}')
	}

	// 페이로드 읽기
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

/// send_frame은 연결에 gRPC 프레임을 전송합니다.
fn (mut h GrpcHandler) send_frame(mut conn net.TcpConn, data []u8, compressed bool) ! {
	// 5바이트 헤더 생성
	length := u32(data.len)
	mut frame := []u8{cap: 5 + data.len}

	// 압축 플래그
	frame << if compressed { u8(1) } else { u8(0) }

	// 길이 (빅 엔디안)
	frame << u8(length >> 24)
	frame << u8(length >> 16)
	frame << u8(length >> 8)
	frame << u8(length)

	// 페이로드
	frame << data

	conn.write(frame) or { return error('Failed to write frame') }
}

// ============================================================================
// 요청 처리
// ============================================================================

/// handle_frame은 gRPC 프레임을 파싱하고 처리합니다.
fn (mut h GrpcHandler) handle_frame(conn_id string, frame GrpcFrame) domain.GrpcStreamResponse {
	start_time := time.now()
	mut success := true
	mut api_name := 'unknown'

	// 프레임 데이터를 스트림 요청으로 파싱
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

	// API 이름 설정
	api_name = match req.request_type {
		.produce { 'produce' }
		.subscribe { 'subscribe' }
		.commit { 'commit' }
		.ack { 'ack' }
		.ping { 'ping' }
	}

	// 요청 처리
	response := h.grpc_service.handle_stream_request(conn_id, req)

	// 에러 확인
	if response.response_type == .error {
		success = false
	}

	// 메트릭 기록
	elapsed_ms := time.since(start_time).milliseconds()
	h.metrics.record_request('grpc_${api_name}', elapsed_ms, success, frame.data.len,
		response.encode().len)

	return response
}

/// parse_stream_request는 바이너리 데이터를 GrpcStreamRequest로 파싱합니다.
fn (h &GrpcHandler) parse_stream_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 1 {
		return error('Empty request')
	}

	// 첫 번째 바이트는 요청 타입
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

/// parse_produce_request는 바이너리 데이터에서 produce 요청을 파싱합니다.
fn (h &GrpcHandler) parse_produce_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for produce request')
	}

	mut pos := 0

	// 토픽 길이 (2바이트) + 토픽
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// 파티션 (4바이트, -1은 자동)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition_val := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4
	partition := if partition_val >= 0 { i32(partition_val) } else { none }

	// 레코드 개수 (4바이트)
	if pos + 4 > data.len {
		return error('Missing record count')
	}
	record_count := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// 레코드 파싱
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

/// parse_consume_request는 바이너리 데이터에서 consume 요청을 파싱합니다.
fn (h &GrpcHandler) parse_consume_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 18 {
		return error('Data too short for consume request')
	}

	mut pos := 0

	// 토픽 길이 (2바이트) + 토픽
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// 파티션 (4바이트)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
		pos + 3])
	pos += 4

	// 오프셋 (8바이트)
	if pos + 8 > data.len {
		return error('Missing offset')
	}
	offset := i64(data[pos]) << 56 | i64(data[pos + 1]) << 48 | i64(data[pos + 2]) << 40 | i64(data[
		pos + 3]) << 32 | i64(data[pos + 4]) << 24 | i64(data[pos + 5]) << 16 | i64(data[pos + 6]) << 8 | i64(data[
		pos + 7])
	pos += 8

	// 최대 레코드 수 (4바이트)
	if pos + 4 > data.len {
		return error('Missing max_records')
	}
	max_records := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// 최대 바이트 수 (4바이트)
	max_bytes := if pos + 4 <= data.len {
		int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[pos + 3])
	} else {
		1048576 // 기본값 1MB
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

/// parse_commit_request는 바이너리 데이터에서 commit 요청을 파싱합니다.
fn (h &GrpcHandler) parse_commit_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 6 {
		return error('Data too short for commit request')
	}

	mut pos := 0

	// 그룹 ID 길이 (2바이트) + group_id
	group_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + group_len > data.len {
		return error('Invalid group_id length')
	}
	group_id := data[pos..pos + group_len].bytestr()
	pos += group_len

	// 오프셋 개수 (4바이트)
	if pos + 4 > data.len {
		return error('Missing offset count')
	}
	offset_count := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// 오프셋 파싱
	mut offsets := []domain.GrpcPartitionOffset{cap: offset_count}
	for _ in 0 .. offset_count {
		// 토픽 길이 + 토픽
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

		// 파티션 (4바이트)
		if pos + 4 > data.len {
			return error('Missing partition')
		}
		partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
			pos + 3])
		pos += 4

		// 오프셋 (8바이트)
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

/// parse_ack_request는 바이너리 데이터에서 ack 요청을 파싱합니다.
fn (h &GrpcHandler) parse_ack_request(data []u8) !domain.GrpcStreamRequest {
	if data.len < 14 {
		return error('Data too short for ack request')
	}

	mut pos := 0

	// 토픽 길이 (2바이트) + 토픽
	topic_len := int(data[pos]) << 8 | int(data[pos + 1])
	pos += 2
	if pos + topic_len > data.len {
		return error('Invalid topic length')
	}
	topic := data[pos..pos + topic_len].bytestr()
	pos += topic_len

	// 파티션 (4바이트)
	if pos + 4 > data.len {
		return error('Missing partition')
	}
	partition := i32(data[pos]) << 24 | i32(data[pos + 1]) << 16 | i32(data[pos + 2]) << 8 | i32(data[
		pos + 3])
	pos += 4

	// 오프셋 (8바이트)
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
// 응답 인코딩
// ============================================================================

/// send_response는 응답을 인코딩하여 전송합니다.
fn (mut h GrpcHandler) send_response(mut conn net.TcpConn, response domain.GrpcStreamResponse) ! {
	data := h.encode_stream_response(response)
	h.send_frame(mut conn, data, false)!
}

/// encode_stream_response는 스트림 응답을 바이너리로 인코딩합니다.
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

/// encode_produce_response는 produce 응답을 인코딩합니다.
fn (h &GrpcHandler) encode_produce_response(resp domain.GrpcProduceResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.error_msg.len}

	// 응답 타입 (1바이트)
	buf << u8(domain.GrpcStreamResponseType.produce_ack)

	// 토픽 길이 + 토픽
	buf << u8(resp.topic.len >> 8)
	buf << u8(resp.topic.len)
	buf << resp.topic.bytes()

	// 파티션 (4바이트)
	buf << u8(resp.partition >> 24)
	buf << u8(resp.partition >> 16)
	buf << u8(resp.partition >> 8)
	buf << u8(resp.partition)

	// 기본 오프셋 (8바이트)
	buf << u8(resp.base_offset >> 56)
	buf << u8(resp.base_offset >> 48)
	buf << u8(resp.base_offset >> 40)
	buf << u8(resp.base_offset >> 32)
	buf << u8(resp.base_offset >> 24)
	buf << u8(resp.base_offset >> 16)
	buf << u8(resp.base_offset >> 8)
	buf << u8(resp.base_offset)

	// 레코드 개수 (4바이트)
	buf << u8(resp.record_count >> 24)
	buf << u8(resp.record_count >> 16)
	buf << u8(resp.record_count >> 8)
	buf << u8(resp.record_count)

	// 타임스탬프 (8바이트)
	buf << u8(resp.timestamp >> 56)
	buf << u8(resp.timestamp >> 48)
	buf << u8(resp.timestamp >> 40)
	buf << u8(resp.timestamp >> 32)
	buf << u8(resp.timestamp >> 24)
	buf << u8(resp.timestamp >> 16)
	buf << u8(resp.timestamp >> 8)
	buf << u8(resp.timestamp)

	// 에러 코드 (4바이트)
	buf << u8(resp.error_code >> 24)
	buf << u8(resp.error_code >> 16)
	buf << u8(resp.error_code >> 8)
	buf << u8(resp.error_code)

	// 에러 메시지 길이 + 메시지
	buf << u8(resp.error_msg.len >> 8)
	buf << u8(resp.error_msg.len)
	buf << resp.error_msg.bytes()

	return buf
}

/// encode_message_response는 메시지 응답을 인코딩합니다.
fn (h &GrpcHandler) encode_message_response(resp domain.GrpcMessageResponse) []u8 {
	mut buf := []u8{cap: 64 + resp.topic.len + resp.key.len + resp.value.len}

	// 응답 타입 (1바이트)
	buf << u8(domain.GrpcStreamResponseType.message)

	// 토픽 길이 + 토픽
	buf << u8(resp.topic.len >> 8)
	buf << u8(resp.topic.len)
	buf << resp.topic.bytes()

	// 파티션 (4바이트)
	buf << u8(resp.partition >> 24)
	buf << u8(resp.partition >> 16)
	buf << u8(resp.partition >> 8)
	buf << u8(resp.partition)

	// 오프셋 (8바이트)
	buf << u8(resp.offset >> 56)
	buf << u8(resp.offset >> 48)
	buf << u8(resp.offset >> 40)
	buf << u8(resp.offset >> 32)
	buf << u8(resp.offset >> 24)
	buf << u8(resp.offset >> 16)
	buf << u8(resp.offset >> 8)
	buf << u8(resp.offset)

	// 타임스탬프 (8바이트)
	buf << u8(resp.timestamp >> 56)
	buf << u8(resp.timestamp >> 48)
	buf << u8(resp.timestamp >> 40)
	buf << u8(resp.timestamp >> 32)
	buf << u8(resp.timestamp >> 24)
	buf << u8(resp.timestamp >> 16)
	buf << u8(resp.timestamp >> 8)
	buf << u8(resp.timestamp)

	// 키 길이 + 키
	buf << u8(resp.key.len >> 24)
	buf << u8(resp.key.len >> 16)
	buf << u8(resp.key.len >> 8)
	buf << u8(resp.key.len)
	buf << resp.key

	// 값 길이 + 값
	buf << u8(resp.value.len >> 24)
	buf << u8(resp.value.len >> 16)
	buf << u8(resp.value.len >> 8)
	buf << u8(resp.value.len)
	buf << resp.value

	// 헤더 개수 + 헤더
	buf << u8(resp.headers.len >> 24)
	buf << u8(resp.headers.len >> 16)
	buf << u8(resp.headers.len >> 8)
	buf << u8(resp.headers.len)

	for k, v in resp.headers {
		// 키 길이 + 키
		buf << u8(k.len >> 8)
		buf << u8(k.len)
		buf << k.bytes()

		// 값 길이 + 값
		buf << u8(v.len >> 8)
		buf << u8(v.len)
		buf << v
	}

	return buf
}

/// encode_commit_response는 commit 응답을 인코딩합니다.
fn (h &GrpcHandler) encode_commit_response(resp domain.GrpcCommitResponse) []u8 {
	mut buf := []u8{cap: 4 + resp.message.len}

	// 응답 타입 (1바이트)
	buf << u8(domain.GrpcStreamResponseType.commit_ack)

	// 성공 여부 (1바이트)
	buf << if resp.success { u8(1) } else { u8(0) }

	// 메시지 길이 + 메시지
	buf << u8(resp.message.len >> 8)
	buf << u8(resp.message.len)
	buf << resp.message.bytes()

	return buf
}

/// encode_error_response는 에러 응답을 인코딩합니다.
fn (h &GrpcHandler) encode_error_response(code i32, message string) []u8 {
	mut buf := []u8{cap: 8 + message.len}

	// 응답 타입 (1바이트)
	buf << u8(domain.GrpcStreamResponseType.error)

	// 에러 코드 (4바이트)
	buf << u8(code >> 24)
	buf << u8(code >> 16)
	buf << u8(code >> 8)
	buf << u8(code)

	// 메시지 길이 + 메시지
	buf << u8(message.len >> 8)
	buf << u8(message.len)
	buf << message.bytes()

	return buf
}

/// encode_pong_response는 pong 응답을 인코딩합니다.
fn (h &GrpcHandler) encode_pong_response(resp domain.GrpcPongResponse) []u8 {
	mut buf := []u8{cap: 9}

	// 응답 타입 (1바이트)
	buf << u8(domain.GrpcStreamResponseType.pong)

	// 타임스탬프 (8바이트)
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

/// send_error는 에러 프레임을 전송합니다.
fn (mut h GrpcHandler) send_error(mut conn net.TcpConn, code i32, message string) {
	data := h.encode_error_response(code, message)
	h.send_frame(mut conn, data, false) or {}
}

// ============================================================================
// 통계
// ============================================================================

/// get_stats는 gRPC 서비스 통계를 반환합니다.
pub fn (mut h GrpcHandler) get_stats() streaming.GrpcStats {
	return h.grpc_service.get_stats()
}

/// get_connections는 모든 활성 gRPC 연결을 반환합니다.
pub fn (mut h GrpcHandler) get_connections() []domain.GrpcConnection {
	return h.grpc_service.list_connections()
}

// ============================================================================
// 메트릭 조회 (Metrics Query)
// ============================================================================

/// get_metrics_summary는 gRPC 프로토콜 메트릭 요약을 반환합니다.
pub fn (mut h GrpcHandler) get_metrics_summary() string {
	return h.metrics.get_summary()
}

/// get_metrics는 gRPC 프로토콜 메트릭 구조체를 반환합니다.
pub fn (mut h GrpcHandler) get_metrics() &observability.ProtocolMetrics {
	return h.metrics
}

/// reset_metrics는 모든 gRPC 프로토콜 메트릭을 초기화합니다.
pub fn (mut h GrpcHandler) reset_metrics() {
	h.metrics.reset()
}
