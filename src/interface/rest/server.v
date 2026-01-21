// Interface Layer - REST API Server
// HTTP server for REST API, SSE and WebSocket streaming
module rest

import domain
import infra.observability
import infra.protocol.http as proto_http
import io
import net
import os
import service.port
import time

// ============================================================================
// REST API Server
// ============================================================================

// RestServerConfig holds REST server configuration
pub struct RestServerConfig {
pub:
	host            string = '0.0.0.0'
	port            int    = 8080
	max_connections int    = 1000
	request_timeout int    = 30000       // 30 seconds
	static_dir      string = 'tests/web' // Static files directory
	sse_config      domain.SSEConfig
	ws_config       domain.WebSocketConfig
}

// default_rest_config returns default REST server configuration
pub fn default_rest_config() RestServerConfig {
	return RestServerConfig{
		sse_config: domain.default_sse_config()
		ws_config:  domain.default_ws_config()
	}
}

// RestServer is the HTTP REST API server
pub struct RestServer {
	config RestServerConfig
mut:
	storage     port.StoragePort
	sse_handler &proto_http.SSEHandler
	ws_handler  &proto_http.WebSocketHandler
	metrics     observability.DataCoreMetrics
	start_time  time.Time
	running     bool
	ready       bool // Indicates if server is ready to serve traffic
}

// new_rest_server creates a new REST API server
pub fn new_rest_server(config RestServerConfig, storage port.StoragePort) &RestServer {
	return &RestServer{
		config:      config
		storage:     storage
		sse_handler: proto_http.new_sse_handler(storage, config.sse_config)
		ws_handler:  proto_http.new_websocket_handler(storage, config.ws_config)
		metrics:     observability.new_datacore_metrics()
		start_time:  time.now()
		running:     false
		ready:       false
	}
}

// start starts the REST API server (blocking)
pub fn (mut s RestServer) start() ! {
	mut listener := net.listen_tcp(.ip, '${s.config.host}:${s.config.port}')!
	s.running = true
	s.ready = true
	s.start_time = time.now()

	println('╔═══════════════════════════════════════════════════════════╗')
	println('║             DataCore REST API Server                      ║')
	println('╠═══════════════════════════════════════════════════════════╣')
	println('║  HTTP Listening: ${s.config.host}:${s.config.port}                          ║')
	println('║  Test Client: http://localhost:${s.config.port}/                   ║')
	println('║  Health: /health, /ready, /live                          ║')
	println('║  Metrics: /metrics                                       ║')
	println('║  SSE Endpoint: /v1/topics/{topic}/sse                     ║')
	println('║  WebSocket Endpoint: /v1/ws                               ║')
	println('║  Max Connections: ${s.config.max_connections}                                  ║')
	println('╚═══════════════════════════════════════════════════════════╝')

	for s.running {
		mut conn := listener.accept() or {
			if !s.running {
				break
			}
			continue
		}

		spawn s.handle_connection(mut conn)
	}

	listener.close() or {}
}

// start_background starts the REST API server in background
pub fn (mut s RestServer) start_background() {
	spawn fn [mut s] () {
		s.start() or { eprintln('[REST] Failed to start server: ${err}') }
	}()
}

// stop stops the REST API server
pub fn (mut s RestServer) stop() {
	s.running = false
	println('[REST] Server stopped')
}

// handle_connection handles a single HTTP connection
fn (mut s RestServer) handle_connection(mut conn net.TcpConn) {
	// Read HTTP request
	mut reader := io.new_buffered_reader(reader: conn)

	// Read request line
	request_line := reader.read_line() or {
		conn.close() or {}
		return
	}
	parts := request_line.trim_right('\r\n').split(' ')
	if parts.len < 2 {
		s.send_error(mut conn, 400, 'Bad Request')
		conn.close() or {}
		return
	}

	method := parts[0]
	full_path := parts[1]

	// Parse path and query string
	path_parts := full_path.split('?')
	path := path_parts[0]
	query := if path_parts.len > 1 {
		parse_query_string(path_parts[1])
	} else {
		map[string]string{}
	}

	// Read headers
	mut headers := map[string]string{}
	for {
		header_line := reader.read_line() or { break }
		trimmed := header_line.trim_right('\r\n')
		if trimmed.len == 0 {
			break
		}
		if idx := trimmed.index(':') {
			key := trimmed[..idx].trim_space()
			value := trimmed[idx + 1..].trim_space()
			headers[key] = value
		}
	}

	// Get client IP
	client_ip := conn.peer_ip() or { '0.0.0.0' }

	// Route request
	s.route_request(method, path, query, headers, client_ip, mut conn)
}

// route_request routes HTTP requests to appropriate handlers
fn (mut s RestServer) route_request(method string, path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// WebSocket upgrade
	upgrade := headers['Upgrade'] or { headers['upgrade'] or { '' } }
	if upgrade.to_lower() == 'websocket' && (path == '/v1/ws' || path == '/ws') {
		s.handle_websocket(headers, client_ip, mut conn)
		return
	}

	// Stats API (check before SSE to avoid path conflict)
	if path == '/v1/sse/stats' && method == 'GET' {
		s.handle_sse_stats(mut conn)
		return
	}

	if path == '/v1/ws/stats' && method == 'GET' {
		s.handle_ws_stats(mut conn)
		return
	}

	// SSE endpoints
	if path.contains('/sse') && method == 'GET' {
		s.handle_sse(path, query, headers, client_ip, mut conn)
		return
	}

	// Topics API
	if path.starts_with('/v1/topics') {
		s.handle_topics_api(method, path, query, mut conn)
		return
	}

	// Health endpoints (Kubernetes compatible)
	if path == '/health' || path == '/healthz' {
		s.handle_health(mut conn)
		return
	}

	if path == '/ready' || path == '/readyz' {
		s.handle_ready(mut conn)
		return
	}

	if path == '/live' || path == '/livez' {
		s.handle_live(mut conn)
		return
	}

	// Metrics endpoint (Prometheus format)
	if path == '/metrics' {
		s.handle_metrics(mut conn)
		return
	}

	// Static files (test client)
	if path == '/' || path == '/index.html' || path.starts_with('/static/') {
		s.serve_static_file(path, mut conn)
		return
	}

	// Not found
	s.send_error(mut conn, 404, 'Not Found')
	conn.close() or {}
}

// ============================================================================
// WebSocket Handlers
// ============================================================================

// handle_websocket handles WebSocket upgrade requests
fn (mut s RestServer) handle_websocket(headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// Handle upgrade
	conn_id := s.ws_handler.handle_upgrade(mut conn, headers, client_ip) or {
		s.send_error(mut conn, 400, 'WebSocket upgrade failed: ${err}')
		conn.close() or {}
		return
	}

	// Start WebSocket connection (this blocks until connection closes)
	s.ws_handler.start_connection(conn_id, mut conn)
}

// handle_ws_stats handles WebSocket statistics requests
fn (mut s RestServer) handle_ws_stats(mut conn net.TcpConn) {
	stats := s.ws_handler.get_stats()
	json := '{' + '"active_connections":${stats.active_connections}' +
		',"total_subscriptions":${stats.total_subscriptions}' +
		',"messages_sent":${stats.messages_sent}' +
		',"messages_received":${stats.messages_received}' + ',"bytes_sent":${stats.bytes_sent}' +
		',"bytes_received":${stats.bytes_received}' +
		',"connections_created":${stats.connections_created}' +
		',"connections_closed":${stats.connections_closed}' + '}'
	s.send_json(mut conn, 200, json)
	conn.close() or {}
}

// ============================================================================
// SSE Handlers
// ============================================================================

// handle_sse handles SSE streaming requests
fn (mut s RestServer) handle_sse(path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// Parse SSE request
	request := proto_http.parse_sse_request(path, query, headers, client_ip) or {
		s.send_error(mut conn, 400, 'Invalid SSE request: ${err}')
		conn.close() or {}
		return
	}

	// Handle SSE request
	status, resp_headers, should_stream := s.sse_handler.handle_sse_request(request) or {
		s.send_error(mut conn, 500, 'Internal server error')
		conn.close() or {}
		return
	}

	if !should_stream {
		s.send_error(mut conn, status, 'SSE request failed')
		conn.close() or {}
		return
	}

	// Get connection ID from headers
	conn_id := resp_headers['X-SSE-Connection-Id'] or { '' }
	if conn_id.len == 0 {
		s.send_error(mut conn, 500, 'Failed to create SSE connection')
		conn.close() or {}
		return
	}

	// Send SSE headers
	mut header_str := 'HTTP/1.1 200 OK\r\n'
	for key, value in resp_headers {
		header_str += '${key}: ${value}\r\n'
	}
	header_str += '\r\n'

	conn.write_string(header_str) or {
		s.sse_handler.sse_service.unregister_connection(conn_id) or {}
		conn.close() or {}
		return
	}

	// Send initial connected event
	connected_event := domain.SSEEvent{
		id:         conn_id
		event_type: .subscribed
		data:       '{"connection_id":"${conn_id}","topic":"${request.topic}"}'
	}
	conn.write_string(connected_event.encode()) or {
		s.sse_handler.sse_service.unregister_connection(conn_id) or {}
		conn.close() or {}
		return
	}

	// Create SSE writer and start streaming
	mut writer := proto_http.new_sse_response_writer(conn)
	s.sse_handler.start_streaming(conn_id, mut writer)
}

// handle_sse_stats handles SSE statistics requests
fn (mut s RestServer) handle_sse_stats(mut conn net.TcpConn) {
	stats := s.sse_handler.get_stats()
	json := '{' + '"active_connections":${stats.active_connections}' +
		',"total_subscriptions":${stats.total_subscriptions}' +
		',"messages_sent":${stats.messages_sent}' + ',"bytes_sent":${stats.bytes_sent}' +
		',"connections_created":${stats.connections_created}' +
		',"connections_closed":${stats.connections_closed}' + '}'
	s.send_json(mut conn, 200, json)
	conn.close() or {}
}

// ============================================================================
// Health & Metrics Handlers
// ============================================================================

// handle_health handles the /health endpoint
// Returns overall health status including storage health
fn (mut s RestServer) handle_health(mut conn net.TcpConn) {
	// Check storage health
	storage_status := s.storage.health_check() or {
		s.send_json(mut conn, 503, '{"status":"unhealthy","storage":"error","error":"${err}"}')
		conn.close() or {}
		return
	}

	status := match storage_status {
		.healthy { 'healthy' }
		.degraded { 'degraded' }
		.unhealthy { 'unhealthy' }
	}

	http_status := match storage_status {
		.healthy { 200 }
		.degraded { 200 }
		.unhealthy { 503 }
	}

	uptime_seconds := time.since(s.start_time) / time.second

	json := '{' + '"status":"${status}"' + ',"storage":"${status}"' +
		',"uptime_seconds":${uptime_seconds}' + ',"version":"0.26.0"' + '}'

	s.send_json(mut conn, http_status, json)
	conn.close() or {}
}

// handle_ready handles the /ready endpoint
// Returns whether the server is ready to accept traffic
fn (mut s RestServer) handle_ready(mut conn net.TcpConn) {
	if s.ready {
		// Additional readiness checks can be added here
		storage_status := s.storage.health_check() or {
			s.send_json(mut conn, 503, '{"ready":false,"reason":"storage_unavailable"}')
			conn.close() or {}
			return
		}

		if storage_status == .unhealthy {
			s.send_json(mut conn, 503, '{"ready":false,"reason":"storage_unhealthy"}')
			conn.close() or {}
			return
		}

		s.send_json(mut conn, 200, '{"ready":true}')
	} else {
		s.send_json(mut conn, 503, '{"ready":false,"reason":"server_not_ready"}')
	}
	conn.close() or {}
}

// handle_live handles the /live endpoint
// Returns whether the server process is alive
fn (mut s RestServer) handle_live(mut conn net.TcpConn) {
	// Liveness check - just verify the server is running
	if s.running {
		s.send_json(mut conn, 200, '{"alive":true}')
	} else {
		s.send_json(mut conn, 503, '{"alive":false}')
	}
	conn.close() or {}
}

// handle_metrics handles the /metrics endpoint (Prometheus format)
fn (s &RestServer) handle_metrics(mut conn net.TcpConn) {
	registry := observability.get_registry()
	metrics_output := registry.export_prometheus()

	response := 'HTTP/1.1 200 OK\r\n' +
		'Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n' +
		'Content-Length: ${metrics_output.len}\r\n' + 'Connection: close\r\n' + '\r\n' +
		metrics_output

	conn.write_string(response) or {}
	conn.close() or {}
}

// ============================================================================
// Topics API Handlers
// ============================================================================

// handle_topics_api handles topics REST API requests
fn (mut s RestServer) handle_topics_api(method string, path string, query map[string]string, mut conn net.TcpConn) {
	parts := path.trim_left('/').split('/')

	// GET /v1/topics - List topics
	if parts.len == 2 && parts[1] == 'topics' && method == 'GET' {
		s.list_topics(mut conn)
		return
	}

	// GET /v1/topics/{topic} - Get topic info
	if parts.len == 3 && parts[1] == 'topics' && method == 'GET' {
		s.get_topic(parts[2], mut conn)
		return
	}

	s.send_error(mut conn, 404, 'Not Found')
	conn.close() or {}
}

// list_topics returns list of all topics
fn (mut s RestServer) list_topics(mut conn net.TcpConn) {
	topics := s.storage.list_topics() or {
		s.send_error(mut conn, 500, 'Failed to list topics')
		conn.close() or {}
		return
	}

	mut json := '{"topics":['
	for i, topic in topics {
		if i > 0 {
			json += ','
		}
		json += '{"name":"${topic.name}","partitions":${topic.partition_count}}'
	}
	json += ']}'

	s.send_json(mut conn, 200, json)
	conn.close() or {}
}

// get_topic returns topic information
fn (mut s RestServer) get_topic(name string, mut conn net.TcpConn) {
	topic := s.storage.get_topic(name) or {
		s.send_error(mut conn, 404, 'Topic not found')
		conn.close() or {}
		return
	}

	json := '{"name":"${topic.name}","partitions":${topic.partition_count}}'
	s.send_json(mut conn, 200, json)
	conn.close() or {}
}

// ============================================================================
// Response Helpers
// ============================================================================

// send_json sends a JSON response
fn (s &RestServer) send_json(mut conn net.TcpConn, status int, body string) {
	status_text := match status {
		200 { 'OK' }
		201 { 'Created' }
		400 { 'Bad Request' }
		404 { 'Not Found' }
		500 { 'Internal Server Error' }
		503 { 'Service Unavailable' }
		else { 'Unknown' }
	}

	response := 'HTTP/1.1 ${status} ${status_text}\r\n' + 'Content-Type: application/json\r\n' +
		'Content-Length: ${body.len}\r\n' + 'Connection: close\r\n' + '\r\n' + body

	conn.write_string(response) or {}
}

// send_error sends an error response
fn (s &RestServer) send_error(mut conn net.TcpConn, status int, message string) {
	body := '{"error":"${message}"}'
	s.send_json(mut conn, status, body)
}

// ============================================================================
// Helper Functions
// ============================================================================

// serve_static_file serves static files from the static directory
fn (s &RestServer) serve_static_file(path string, mut conn net.TcpConn) {
	// Determine file path
	file_path := if path == '/' || path == '/index.html' {
		os.join_path(s.config.static_dir, 'test_client.html')
	} else if path.starts_with('/static/') {
		os.join_path(s.config.static_dir, path[8..])
	} else {
		os.join_path(s.config.static_dir, path.trim_left('/'))
	}

	// Security: prevent directory traversal
	abs_static_dir := os.real_path(s.config.static_dir)
	abs_file_path := os.real_path(file_path)
	if !abs_file_path.starts_with(abs_static_dir) {
		s.send_error(mut conn, 403, 'Forbidden')
		conn.close() or {}
		return
	}

	// Read file
	content := os.read_file(file_path) or {
		s.send_error(mut conn, 404, 'File not found')
		conn.close() or {}
		return
	}

	// Determine content type
	content_type := get_content_type(file_path)

	// Send response
	response := 'HTTP/1.1 200 OK\r\n' + 'Content-Type: ${content_type}\r\n' +
		'Content-Length: ${content.len}\r\n' + 'Cache-Control: no-cache\r\n' +
		'Connection: close\r\n' + '\r\n' + content

	conn.write_string(response) or {}
	conn.close() or {}
}

// get_content_type returns the content type for a file
fn get_content_type(path string) string {
	if path.ends_with('.html') {
		return 'text/html; charset=utf-8'
	} else if path.ends_with('.css') {
		return 'text/css; charset=utf-8'
	} else if path.ends_with('.js') {
		return 'application/javascript; charset=utf-8'
	} else if path.ends_with('.json') {
		return 'application/json; charset=utf-8'
	} else if path.ends_with('.png') {
		return 'image/png'
	} else if path.ends_with('.jpg') || path.ends_with('.jpeg') {
		return 'image/jpeg'
	} else if path.ends_with('.svg') {
		return 'image/svg+xml'
	} else if path.ends_with('.ico') {
		return 'image/x-icon'
	}
	return 'application/octet-stream'
}

// parse_query_string parses URL query string into map
fn parse_query_string(query string) map[string]string {
	mut result := map[string]string{}
	if query.len == 0 {
		return result
	}

	pairs := query.split('&')
	for pair in pairs {
		if idx := pair.index('=') {
			key := pair[..idx]
			value := pair[idx + 1..]
			result[key] = value
		}
	}
	return result
}
