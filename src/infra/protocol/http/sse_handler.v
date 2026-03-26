// HTTP handler for Server-Sent Events streaming
module http

import domain
import service.port
import service.streaming
import net
import time
import infra.observability

// SSE handler

/// SSEHandler handles SSE HTTP requests.
pub struct SSEHandler {
	config          domain.SSEConfig
	allowed_origins []string
pub mut:
	sse_service &streaming.SSEService
	storage     port.StoragePort
	metrics     &observability.ProtocolMetrics
}

/// new_sse_handler creates a new SSE handler.
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

// HTTP request handling

/// handle_sse_request handles an SSE HTTP request.
/// Returns: (status code, headers, is_streaming)
pub fn (mut h SSEHandler) handle_sse_request(request SSERequest) !(int, map[string]string, bool) {
	start_time := time.now()
	mut success := true
	mut status_code := 200

	// check if topic exists
	_ = h.storage.get_topic(request.topic) or {
		success = false
		status_code = 404
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		observability.log_with_context('sse', .error, 'Request', 'Topic not found', {
			'topic':     request.topic
			'client_ip': request.client_ip
		})
		return 404, map[string]string{}, false
	}

	// validate partition if specified
	if partition := request.partition {
		topic_meta := h.storage.get_topic(request.topic)!
		if partition < 0 || partition >= topic_meta.partition_count {
			success = false
			status_code = 400
			elapsed_ms := time.since(start_time).milliseconds()
			h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
			observability.log_with_context('sse', .error, 'Request', 'Invalid partition',
				{
				'topic':     request.topic
				'partition': partition.str()
				'client_ip': request.client_ip
			})
			return 400, map[string]string{}, false
		}
	}

	// create SSE connection
	conn := domain.new_sse_connection(request.client_ip, request.user_agent)

	// register connection
	conn_id := h.sse_service.register_connection(conn) or {
		success = false
		status_code = 503
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		observability.log_with_context('sse', .error, 'Request', 'Failed to register connection',
			{
			'client_ip': request.client_ip
			'error':     err.msg()
		})
		return 503, map[string]string{}, false
	}

	// create subscription
	offset_type := domain.subscription_offset_from_str(request.offset_str)
	offset := if offset_type == .specific { request.offset_str.i64() } else { i64(0) }

	sub := domain.new_subscription(request.topic, request.partition, offset_type, offset,
		request.group_id, request.client_id)

	// add subscription
	h.sse_service.subscribe(conn_id, sub) or {
		success = false
		h.sse_service.unregister_connection(conn_id) or {}
		elapsed_ms := time.since(start_time).milliseconds()
		h.metrics.record_request('sse_request', elapsed_ms, success, 0, 0)
		observability.log_with_context('sse', .error, 'Request', 'Failed to subscribe',
			{
			'conn_id': conn_id
			'topic':   request.topic
			'error':   err.msg()
		})
		return 400, map[string]string{}, false
	}

	// return SSE headers
	mut headers := {
		'Content-Type':        'text/event-stream'
		'Cache-Control':       'no-cache'
		'Connection':          'keep-alive'
		'X-Accel-Buffering':   'no'
		'X-SSE-Connection-Id': conn_id
	}

	if cors_origin := resolve_cors_origin(h.allowed_origins, request.origin) {
		headers['Access-Control-Allow-Origin'] = cors_origin
	}

	// record metrics
	elapsed_ms := time.since(start_time).milliseconds()
	h.metrics.record_request('sse_request', elapsed_ms, success, 0, headers.len)

	observability.log_with_context('sse', .info, 'Request', 'SSE request successful',
		{
		'conn_id':   conn_id
		'topic':     request.topic
		'client_ip': request.client_ip
	})

	return status_code, headers, true
}

/// start_streaming starts the SSE streaming loop for a connection.
pub fn (mut h SSEHandler) start_streaming(conn_id string, mut writer SSEResponseWriter) {
	// set writer on connection
	h.sse_service.set_writer(conn_id, writer) or { return }

	// look up subscription
	subs := h.sse_service.get_subscriptions(conn_id)
	if subs.len == 0 {
		return
	}

	// start streaming for each subscription
	for sub in subs {
		h.sse_service.stream_messages(conn_id, sub.id) or { continue }
	}

	// main streaming loop
	mut last_heartbeat := time.now().unix_milli()
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// check if connection is still active
		if !writer.is_alive() {
			break
		}

		// send heartbeat if needed
		if now - last_heartbeat >= h.config.heartbeat_interval_ms {
			heartbeat := domain.new_sse_heartbeat_event()
			writer.write_event(heartbeat) or { break }
			writer.flush() or { break }
			last_heartbeat = now
		}

		// poll for new messages using service method (correctly handles offset updates)
		if now - last_poll >= 100 {
			h.sse_service.poll_messages_for_connection(conn_id) or { break }
			writer.flush() or { break }
			last_poll = now
		}

		// short sleep to prevent busy loop
		time.sleep(10 * time.millisecond)
	}

	// cleanup
	h.sse_service.unregister_connection(conn_id) or {}
}

// SSE request/response types

/// SSERequest represents an SSE HTTP request.
pub struct SSERequest {
pub:
	topic         string
	partition     ?i32
	offset_str    string
	group_id      ?string
	client_id     string
	client_ip     string
	user_agent    string
	last_event_id string
	origin        string
}

/// parse_sse_request parses an SSE request from HTTP request data.
pub fn parse_sse_request(path string, query map[string]string, headers map[string]string, client_ip string) !SSERequest {
	// parse path: /v1/topics/{topic}/sse or /v1/topics/{topic}/partitions/{partition}/sse
	parts := path.trim_left('/').split('/')

	if parts.len < 4 {
		return error('Invalid path')
	}

	// validate path structure
	if parts[0] != 'v1' || parts[1] != 'topics' {
		return error('Invalid path')
	}

	topic := parts[2]
	mut partition := ?i32(none)

	// check partition from path
	if parts.len >= 6 && parts[3] == 'partitions' && parts[5] == 'sse' {
		partition = i32(parts[4].int())
	} else if parts.len >= 4 && parts[3] == 'sse' {
		// partition not specified
	} else {
		return error('Invalid path')
	}

	// parse query parameters
	offset_str := query['offset'] or { query['from'] or { 'latest' } }
	group_id := if gid := query['group_id'] { gid } else { none }
	client_id := query['client_id'] or { 'sse-client' }

	// parse headers
	user_agent := headers['User-Agent'] or { headers['user-agent'] or { 'unknown' } }
	last_event_id := headers['Last-Event-ID'] or { headers['last-event-id'] or { '' } }
	origin := headers['Origin'] or { headers['origin'] or { '' } }

	// handle Last-Event-ID for reconnection
	mut final_offset := offset_str
	if last_event_id.len > 0 {
		// parse last event ID: topic:partition:offset
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
		origin:        origin
	}
}

// SSE response writer

/// SSEResponseWriter wraps a connection for SSE writing.
pub struct SSEResponseWriter {
mut:
	conn   &net.TcpConn
	alive  bool
	buffer []u8
}

/// new_sse_response_writer creates a new SSE response writer.
pub fn new_sse_response_writer(conn &net.TcpConn) &SSEResponseWriter {
	return &SSEResponseWriter{
		conn:   conn
		alive:  true
		buffer: []u8{}
	}
}

/// write_event writes an SSE event.
pub fn (mut w SSEResponseWriter) write_event(event domain.SSEEvent) ! {
	if !w.alive {
		return error('Connection closed')
	}

	data := event.encode()
	w.buffer << data.bytes()
}

/// flush sends buffered data.
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

/// is_alive checks if the connection is still active.
pub fn (w &SSEResponseWriter) is_alive() bool {
	return w.alive
}

/// close closes the connection.
pub fn (mut w SSEResponseWriter) close() ! {
	w.alive = false
	// send close event before closing
	close_event := domain.new_sse_close_event('server shutdown')
	w.conn.write(close_event.encode().bytes()) or {}
	w.conn.close() or {}
}

// Statistics

/// resolve_cors_origin determines the Access-Control-Allow-Origin header value.
/// Returns the origin string to set, or none if the header should not be set.
/// Empty allowed_origins or presence of '*' means allow all (backward compatible).
fn resolve_cors_origin(allowed_origins []string, request_origin string) ?string {
	if allowed_origins.len == 0 || '*' in allowed_origins {
		return '*'
	}
	if request_origin.len > 0 && request_origin in allowed_origins {
		return request_origin
	}
	return none
}

/// get_stats returns SSE service statistics.
pub fn (mut h SSEHandler) get_stats() port.StreamingStats {
	return h.sse_service.get_stats()
}

/// get_connections returns all active SSE connections.
pub fn (mut h SSEHandler) get_connections() []domain.SSEConnection {
	return h.sse_service.list_connections()
}
