// 인프라 레이어 - SSE 핸들러
// Server-Sent Events 스트리밍을 위한 HTTP 핸들러
module http

import domain
import service.port
import service.streaming
import net
import time
import infra.observability

// ============================================================================
// 로깅 (Logging)
// ============================================================================

/// log_message는 구조화된 로그 메시지를 출력합니다.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('sse.${component}')
	fields := observability.fields_from_map(context)
	match level {
		.debug { logger.debug(message, fields) }
		.info { logger.info(message, fields) }
		.warn { logger.warn(message, fields) }
		.error { logger.error(message, fields) }
	}
}

// ============================================================================
// SSE 핸들러
// ============================================================================

/// SSEHandler는 SSE HTTP 요청을 처리합니다.
pub struct SSEHandler {
	config domain.SSEConfig
pub mut:
	sse_service &streaming.SSEService
	storage     port.StoragePort
	metrics     &observability.ProtocolMetrics // SSE 메트릭 수집
}

/// new_sse_handler는 새로운 SSE 핸들러를 생성합니다.
pub fn new_sse_handler(storage port.StoragePort, config domain.SSEConfig) &SSEHandler {
	sse_service := streaming.new_sse_service(storage, config)
	metrics := observability.new_protocol_metrics()
	return &SSEHandler{
		config:      config
		sse_service: sse_service
		storage:     storage
		metrics:     metrics
	}
}

// ============================================================================
// HTTP 요청 처리
// ============================================================================

/// handle_sse_request는 SSE HTTP 요청을 처리합니다.
/// 반환값: (상태 코드, 헤더, 스트리밍 여부)
pub fn (mut h SSEHandler) handle_sse_request(request SSERequest) !(int, map[string]string, bool) {
	start_time := time.now()
	mut success := true
	mut status_code := 200

	// 토픽 존재 여부 확인
	_ = h.storage.get_topic(request.topic) or {
		success = false
		status_code = 404
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		log_message(.error, 'Request', 'Topic not found', {
			'topic':     request.topic
			'client_ip': request.client_ip
		})
		return 404, map[string]string{}, false
	}

	// 파티션이 지정된 경우 유효성 검사
	if partition := request.partition {
		topic_meta := h.storage.get_topic(request.topic)!
		if partition < 0 || partition >= topic_meta.partition_count {
			success = false
			status_code = 400
			elapsed_ms := time.since(start_time).milliseconds()
			h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
			log_message(.error, 'Request', 'Invalid partition', {
				'topic':     request.topic
				'partition': partition.str()
				'client_ip': request.client_ip
			})
			return 400, map[string]string{}, false
		}
	}

	// SSE 연결 생성
	conn := domain.new_sse_connection(request.client_ip, request.user_agent)

	// 연결 등록
	conn_id := h.sse_service.register_connection(conn) or {
		success = false
		status_code = 503
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		log_message(.error, 'Request', 'Failed to register connection', {
			'client_ip': request.client_ip
			'error':     err.msg()
		})
		return 503, map[string]string{}, false
	}

	// 구독 생성
	offset_type := domain.subscription_offset_from_str(request.offset_str)
	offset := if offset_type == .specific { request.offset_str.i64() } else { i64(0) }

	sub := domain.new_subscription(request.topic, request.partition, offset_type, offset,
		request.group_id, request.client_id)

	// 구독 추가
	h.sse_service.subscribe(conn_id, sub) or {
		success = false
		status_code = 400
		h.sse_service.unregister_connection(conn_id) or {}
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		log_message(.error, 'Request', 'Failed to subscribe', {
			'conn_id': conn_id
			'topic':   request.topic
			'error':   err.msg()
		})
		return 400, map[string]string{}, false
	}

	// SSE 헤더 반환
	headers := {
		'Content-Type':                'text/event-stream'
		'Cache-Control':               'no-cache'
		'Connection':                  'keep-alive'
		'X-Accel-Buffering':           'no'
		'Access-Control-Allow-Origin': '*'
		'X-SSE-Connection-Id':         conn_id
	}

	// 메트릭 기록
	elapsed_ms := time.since(start_time).milliseconds()
	h.metrics.record_request('sse_request', elapsed_ms, success, 0, headers.len)

	log_message(.info, 'Request', 'SSE request successful', {
		'conn_id':   conn_id
		'topic':     request.topic
		'client_ip': request.client_ip
	})

	return 200, headers, true
}

/// start_streaming은 연결에 대한 SSE 스트리밍 루프를 시작합니다.
pub fn (mut h SSEHandler) start_streaming(conn_id string, mut writer SSEResponseWriter) {
	// 연결에 writer 설정
	h.sse_service.set_writer(conn_id, writer) or { return }

	// 구독 조회
	subs := h.sse_service.get_subscriptions(conn_id)
	if subs.len == 0 {
		return
	}

	// 각 구독에 대해 스트리밍 시작
	for sub in subs {
		h.sse_service.stream_messages(conn_id, sub.id) or { continue }
	}

	// 메인 스트리밍 루프
	mut last_heartbeat := time.now().unix_milli()
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// 연결이 여전히 활성 상태인지 확인
		if !writer.is_alive() {
			break
		}

		// 필요시 하트비트 전송
		if now - last_heartbeat >= h.config.heartbeat_interval_ms {
			heartbeat := domain.new_sse_heartbeat_event()
			writer.write_event(heartbeat) or { break }
			writer.flush() or { break }
			last_heartbeat = now
		}

		// 서비스 메서드를 사용하여 새 메시지 폴링 (오프셋 업데이트를 올바르게 처리)
		if now - last_poll >= 100 { // 100ms마다 폴링
			h.sse_service.poll_messages_for_connection(conn_id) or { break }
			writer.flush() or { break }
			last_poll = now
		}

		// busy loop 방지를 위한 짧은 sleep
		time.sleep(10 * time.millisecond)
	}

	// 정리
	h.sse_service.unregister_connection(conn_id) or {}
}

// ============================================================================
// SSE 요청/응답 타입
// ============================================================================

/// SSERequest는 SSE HTTP 요청을 나타냅니다.
pub struct SSERequest {
pub:
	topic         string  // 토픽 이름
	partition     ?i32    // 파티션 (선택사항)
	offset_str    string  // 오프셋 문자열 (earliest, latest, 또는 숫자)
	group_id      ?string // 컨슈머 그룹 ID (선택사항)
	client_id     string  // 클라이언트 식별자
	client_ip     string  // 클라이언트 IP 주소
	user_agent    string  // User agent
	last_event_id string  // 재연결을 위한 Last-Event-ID 헤더
}

/// parse_sse_request는 HTTP 요청 데이터에서 SSE 요청을 파싱합니다.
pub fn parse_sse_request(path string, query map[string]string, headers map[string]string, client_ip string) !SSERequest {
	// 경로 파싱: /v1/topics/{topic}/sse 또는 /v1/topics/{topic}/partitions/{partition}/sse
	parts := path.trim_left('/').split('/')

	if parts.len < 4 {
		return error('Invalid path')
	}

	// 경로 구조 검증
	if parts[0] != 'v1' || parts[1] != 'topics' {
		return error('Invalid path')
	}

	topic := parts[2]
	mut partition := ?i32(none)

	// 경로에서 파티션 확인
	if parts.len >= 6 && parts[3] == 'partitions' && parts[5] == 'sse' {
		partition = i32(parts[4].int())
	} else if parts.len >= 4 && parts[3] == 'sse' {
		// 파티션 미지정
	} else {
		return error('Invalid path')
	}

	// 쿼리 파라미터 파싱
	offset_str := query['offset'] or { query['from'] or { 'latest' } }
	group_id := if gid := query['group_id'] { gid } else { none }
	client_id := query['client_id'] or { 'sse-client' }

	// 헤더 파싱
	user_agent := headers['User-Agent'] or { headers['user-agent'] or { 'unknown' } }
	last_event_id := headers['Last-Event-ID'] or { headers['last-event-id'] or { '' } }

	// 재연결을 위한 Last-Event-ID 처리
	mut final_offset := offset_str
	if last_event_id.len > 0 {
		// 마지막 이벤트 ID 파싱: topic:partition:offset
		id_parts := last_event_id.split(':')
		if id_parts.len >= 3 {
			final_offset = (id_parts[2].i64() + 1).str()
		}
	}

	return SSERequest{
		topic:         topic
		partition:     partition
		offset_str:    final_offset
		group_id:      group_id
		client_id:     client_id
		client_ip:     client_ip
		user_agent:    user_agent
		last_event_id: last_event_id
	}
}

// ============================================================================
// SSE 응답 Writer
// ============================================================================

/// SSEResponseWriter는 SSE 쓰기를 위해 연결을 래핑합니다.
pub struct SSEResponseWriter {
mut:
	conn   &net.TcpConn
	alive  bool
	buffer []u8
}

/// new_sse_response_writer는 새로운 SSE 응답 writer를 생성합니다.
pub fn new_sse_response_writer(conn &net.TcpConn) &SSEResponseWriter {
	return &SSEResponseWriter{
		conn:   conn
		alive:  true
		buffer: []u8{}
	}
}

/// write_event는 SSE 이벤트를 씁니다.
pub fn (mut w SSEResponseWriter) write_event(event domain.SSEEvent) ! {
	if !w.alive {
		return error('Connection closed')
	}

	data := event.encode()
	w.buffer << data.bytes()
}

/// flush는 버퍼링된 데이터를 전송합니다.
pub fn (mut w SSEResponseWriter) flush() ! {
	if !w.alive {
		return error('Connection closed')
	}

	if w.buffer.len == 0 {
		return
	}

	w.conn.write(w.buffer) or {
		w.alive = false
		return error('Write failed')
	}

	w.buffer.clear()
}

/// is_alive는 연결이 여전히 활성 상태인지 확인합니다.
pub fn (w &SSEResponseWriter) is_alive() bool {
	return w.alive
}

/// close는 연결을 닫습니다.
pub fn (mut w SSEResponseWriter) close() ! {
	w.alive = false
	// 닫기 전에 close 이벤트 전송
	close_event := domain.new_sse_close_event('server shutdown')
	w.conn.write(close_event.encode().bytes()) or {}
	w.conn.close() or {}
}

// ============================================================================
// 통계
// ============================================================================

/// get_stats는 SSE 서비스 통계를 반환합니다.
pub fn (mut h SSEHandler) get_stats() port.StreamingStats {
	return h.sse_service.get_stats()
}

/// get_connections는 모든 활성 SSE 연결을 반환합니다.
pub fn (mut h SSEHandler) get_connections() []domain.SSEConnection {
	return h.sse_service.list_connections()
}
