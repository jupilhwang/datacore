// Infra Layer - SSE Handler
// HTTP handler for Server-Sent Events streaming
module http

import domain
import service.port
import service.streaming
import net
import time

// ============================================================================
// SSE Handler
// ============================================================================

// SSEHandler handles SSE HTTP requests
pub struct SSEHandler {
	config domain.SSEConfig
pub mut:
	sse_service &streaming.SSEService
	storage     port.StoragePort
}

// new_sse_handler creates a new SSE handler
pub fn new_sse_handler(storage port.StoragePort, config domain.SSEConfig) &SSEHandler {
	sse_service := streaming.new_sse_service(storage, config)
	return &SSEHandler{
		config:      config
		sse_service: sse_service
		storage:     storage
	}
}

// ============================================================================
// HTTP Request Handling
// ============================================================================

// handle_sse_request handles an SSE HTTP request
// Returns (status_code, headers, should_stream)
pub fn (mut h SSEHandler) handle_sse_request(request SSERequest) !(int, map[string]string, bool) {
	// Validate topic exists
	_ = h.storage.get_topic(request.topic) or { return 404, map[string]string{}, false }

	// Validate partition if specified
	if partition := request.partition {
		topic_meta := h.storage.get_topic(request.topic)!
		if partition < 0 || partition >= topic_meta.partition_count {
			return 400, map[string]string{}, false
		}
	}

	// Create SSE connection
	conn := domain.new_sse_connection(request.client_ip, request.user_agent)

	// Register connection
	conn_id := h.sse_service.register_connection(conn) or { return 503, map[string]string{}, false }

	// Create subscription
	offset_type := domain.subscription_offset_from_str(request.offset_str)
	offset := if offset_type == .specific { request.offset_str.i64() } else { i64(0) }

	sub := domain.new_subscription(request.topic, request.partition, offset_type, offset,
		request.group_id, request.client_id)

	// Add subscription
	h.sse_service.subscribe(conn_id, sub) or {
		h.sse_service.unregister_connection(conn_id) or {}
		return 400, map[string]string{}, false
	}

	// Return SSE headers
	headers := {
		'Content-Type':                'text/event-stream'
		'Cache-Control':               'no-cache'
		'Connection':                  'keep-alive'
		'X-Accel-Buffering':           'no'
		'Access-Control-Allow-Origin': '*'
		'X-SSE-Connection-Id':         conn_id
	}

	return 200, headers, true
}

// start_streaming starts the SSE streaming loop for a connection
pub fn (mut h SSEHandler) start_streaming(conn_id string, mut writer SSEResponseWriter) {
	// Set writer on connection
	h.sse_service.set_writer(conn_id, writer) or { return }

	// Get subscriptions
	subs := h.sse_service.get_subscriptions(conn_id)
	if subs.len == 0 {
		return
	}

	// Start streaming for each subscription
	for sub in subs {
		h.sse_service.stream_messages(conn_id, sub.id) or { continue }
	}

	// Main streaming loop
	mut last_heartbeat := time.now().unix_milli()
	mut last_poll := time.now().unix_milli()

	for {
		now := time.now().unix_milli()

		// Check if connection is still alive
		if !writer.is_alive() {
			break
		}

		// Send heartbeat if needed
		if now - last_heartbeat >= h.config.heartbeat_interval_ms {
			heartbeat := domain.new_sse_heartbeat_event()
			writer.write_event(heartbeat) or { break }
			writer.flush() or { break }
			last_heartbeat = now
		}

		// Poll for new messages using service method (handles offset updates correctly)
		if now - last_poll >= 100 { // Poll every 100ms
			h.sse_service.poll_messages_for_connection(conn_id) or { break }
			writer.flush() or { break }
			last_poll = now
		}

		// Small sleep to prevent busy loop
		time.sleep(10 * time.millisecond)
	}

	// Cleanup
	h.sse_service.unregister_connection(conn_id) or {}
}

// ============================================================================
// SSE Request/Response Types
// ============================================================================

// SSERequest represents an SSE HTTP request
pub struct SSERequest {
pub:
	topic         string  // Topic name
	partition     ?i32    // Partition (optional)
	offset_str    string  // Offset string (earliest, latest, or number)
	group_id      ?string // Consumer group ID (optional)
	client_id     string  // Client identifier
	client_ip     string  // Client IP address
	user_agent    string  // User agent
	last_event_id string  // Last-Event-ID header for reconnection
}

// parse_sse_request parses an SSE request from HTTP request data
pub fn parse_sse_request(path string, query map[string]string, headers map[string]string, client_ip string) !SSERequest {
	// Parse path: /v1/topics/{topic}/sse or /v1/topics/{topic}/partitions/{partition}/sse
	parts := path.trim_left('/').split('/')

	if parts.len < 4 {
		return error('Invalid path')
	}

	// Validate path structure
	if parts[0] != 'v1' || parts[1] != 'topics' {
		return error('Invalid path')
	}

	topic := parts[2]
	mut partition := ?i32(none)

	// Check for partition in path
	if parts.len >= 6 && parts[3] == 'partitions' && parts[5] == 'sse' {
		partition = i32(parts[4].int())
	} else if parts.len >= 4 && parts[3] == 'sse' {
		// No partition specified
	} else {
		return error('Invalid path')
	}

	// Parse query parameters
	offset_str := query['offset'] or { query['from'] or { 'latest' } }
	group_id := if gid := query['group_id'] { gid } else { none }
	client_id := query['client_id'] or { 'sse-client' }

	// Parse headers
	user_agent := headers['User-Agent'] or { headers['user-agent'] or { 'unknown' } }
	last_event_id := headers['Last-Event-ID'] or { headers['last-event-id'] or { '' } }

	// Handle Last-Event-ID for reconnection
	mut final_offset := offset_str
	if last_event_id.len > 0 {
		// Parse last event ID: topic:partition:offset
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
// SSE Response Writer
// ============================================================================

// SSEResponseWriter wraps a connection for SSE writing
pub struct SSEResponseWriter {
mut:
	conn   &net.TcpConn
	alive  bool
	buffer []u8
}

// new_sse_response_writer creates a new SSE response writer
pub fn new_sse_response_writer(conn &net.TcpConn) &SSEResponseWriter {
	return &SSEResponseWriter{
		conn:   conn
		alive:  true
		buffer: []u8{}
	}
}

// write_event writes an SSE event
pub fn (mut w SSEResponseWriter) write_event(event domain.SSEEvent) ! {
	if !w.alive {
		return error('Connection closed')
	}

	data := event.encode()
	w.buffer << data.bytes()
}

// flush sends buffered data
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

// is_alive checks if connection is still alive
pub fn (w &SSEResponseWriter) is_alive() bool {
	return w.alive
}

// close closes the connection
pub fn (mut w SSEResponseWriter) close() ! {
	w.alive = false
	// Send close event before closing
	close_event := domain.new_sse_close_event('server shutdown')
	w.conn.write(close_event.encode().bytes()) or {}
	w.conn.close() or {}
}

// ============================================================================
// Statistics
// ============================================================================

// get_stats returns SSE service statistics
pub fn (mut h SSEHandler) get_stats() port.StreamingStats {
	return h.sse_service.get_stats()
}

// get_connections returns all active SSE connections
pub fn (mut h SSEHandler) get_connections() []domain.SSEConnection {
	return h.sse_service.list_connections()
}
