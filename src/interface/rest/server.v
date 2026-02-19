// Interface Layer - REST API Server
//
// Server providing HTTP-based REST API, SSE (Server-Sent Events),
// and WebSocket streaming. Supports Kubernetes-compatible health checks
// and Prometheus metrics.
//
// Key features:
// - REST API endpoints (topic management)
// - SSE streaming (real-time message subscription)
// - WebSocket streaming (bidirectional communication)
// - Health checks (/health, /ready, /live)
// - Prometheus metrics (/metrics)
// - Static file serving (test client)
module rest

import domain
import infra.observability
import infra.protocol.http as proto_http
import io
import json
import net
import os
import regex
import service.port
import time

// REST API server

/// RestServerConfig is a struct holding REST server configuration.
pub struct RestServerConfig {
pub:
	host            string
	port            int
	max_connections int
	static_dir      string
pub mut:
	sse_config domain.SSEConfig
	ws_config  domain.WebSocketConfig
}

/// RestServer provides an HTTP-based REST API server.
/// Supports SSE and WebSocket streaming.
pub struct RestServer {
	config RestServerConfig
mut:
	storage             port.StoragePort
	sse_handler         &proto_http.SSEHandler
	ws_handler          &proto_http.WebSocketHandler
	schema_api          &SchemaAPI
	iceberg_catalog_api &IcebergCatalogAPI
	metrics             observability.DataCoreMetrics
	start_time          time.Time
	running             bool
	ready               bool
}

// ParsedRequest holds the parsed HTTP request from handle_connection.
struct ParsedRequest {
	method  string
	path    string
	query   map[string]string
	headers map[string]string
	body    string
}

// Response structs

// TopicResponse is the response struct representing a single topic.
struct TopicResponse {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// TopicsListResponse is the response struct for listing topics.
struct TopicsListResponse {
	topics []TopicResponse @[json: 'topics']
}

// CreateTopicRequest is the request struct for POST /v1/topics.
struct CreateTopicRequest {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// RestErrorResponse is the REST API error response struct.
struct RestErrorResponse {
	error_code int    @[json: 'error_code']
	message    string @[json: 'message']
}

// SSEStatsResponse is the SSE statistics response struct.
struct SSEStatsResponse {
	active_connections  int @[json: 'active_connections']
	total_subscriptions int @[json: 'total_subscriptions']
	messages_sent       i64 @[json: 'messages_sent']
	bytes_sent          i64 @[json: 'bytes_sent']
	connections_created i64 @[json: 'connections_created']
	connections_closed  i64 @[json: 'connections_closed']
}

// WSStatsResponse is the WebSocket statistics response struct.
struct WSStatsResponse {
	active_connections  int @[json: 'active_connections']
	total_subscriptions int @[json: 'total_subscriptions']
	messages_sent       i64 @[json: 'messages_sent']
	messages_received   i64 @[json: 'messages_received']
	bytes_sent          i64 @[json: 'bytes_sent']
	bytes_received      i64 @[json: 'bytes_received']
	connections_created i64 @[json: 'connections_created']
	connections_closed  i64 @[json: 'connections_closed']
}

// HealthResponse is the health check response struct.
struct HealthResponse {
	status         string @[json: 'status']
	storage        string @[json: 'storage']
	uptime_seconds i64    @[json: 'uptime_seconds']
	version        string @[json: 'version']
}

// ReadyResponse is the response struct for the /ready endpoint.
struct ReadyResponse {
	ready  bool   @[json: 'ready']
	reason string @[json: 'reason'; omitempty]
}

// LiveResponse is the response struct for the /live endpoint.
struct LiveResponse {
	alive bool @[json: 'alive']
}

/// new_rest_server - creates a new REST API server
pub fn new_rest_server(config RestServerConfig, storage port.StoragePort) &RestServer {
	return &RestServer{
		config:              config
		storage:             storage
		sse_handler:         proto_http.new_sse_handler(storage, config.sse_config)
		ws_handler:          proto_http.new_websocket_handler(storage, config.ws_config)
		schema_api:          &SchemaAPI(unsafe { nil })
		iceberg_catalog_api: &IcebergCatalogAPI(unsafe { nil })
		metrics:             observability.new_datacore_metrics()
		start_time:          time.now()
		running:             false
		ready:               false
	}
}

/// default_rest_config - returns default REST server configuration
pub fn default_rest_config() RestServerConfig {
	return RestServerConfig{
		host:            '0.0.0.0'
		port:            8080
		max_connections: 1000
		static_dir:      ''
		sse_config:      domain.default_sse_config()
		ws_config:       domain.default_ws_config()
	}
}

/// set_schema_api - sets the Schema API handler
pub fn (mut s RestServer) set_schema_api(api &SchemaAPI) {
	unsafe {
		s.schema_api = api
	}
}

/// set_iceberg_catalog_api - sets the Iceberg Catalog API handler
pub fn (mut s RestServer) set_iceberg_catalog_api(api &IcebergCatalogAPI) {
	unsafe {
		s.iceberg_catalog_api = api
	}
}

/// start - starts the REST API server (blocking)
pub fn (mut s RestServer) start() ! {
	mut listener := net.listen_tcp(.ip, '${s.config.host}:${s.config.port}')!
	s.running = true
	s.ready = true
	s.start_time = time.now()

	println('╔═══════════════════════════════════════════════════════════╗')
	println('║             DataCore REST API Server                      ║')
	println('╠═════════════════════════════════════════════════════════════╣')
	println('║  HTTP Listening: ${s.config.host}:${s.config.port}                          ║')
	println('║  Test Client: http://localhost:${s.config.port}/                   ║')
	println('║  Health: /health, /ready, /live                          ║')
	println('║  Metrics: /metrics                                       ║')
	println('║  SSE Endpoint: /v1/topics/{topic}/sse                     ║')
	println('║  WebSocket Endpoint: /v1/ws                               ║')
	println('║  Iceberg Catalog API: /v1/iceberg/*                       ║')
	println('║  Max Connections: ${s.config.max_connections}                                  ║')
	println('╚═════════════════════════════════════════════════════════════╝')

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

/// start_background - starts the REST API server in background
pub fn (mut s RestServer) start_background() {
	spawn fn [mut s] () {
		s.start() or { eprintln('[REST] Failed to start server: ${err}') }
	}()
}

/// stop - stops the REST API server
pub fn (mut s RestServer) stop() {
	s.running = false
	println('[REST] Server stopped')
}

// handle_connection handles a single HTTP connection.
fn (mut s RestServer) handle_connection(mut conn net.TcpConn) {
	// Read HTTP request - use buffered reader to read headers and body to prevent data loss
	mut reader := io.new_buffered_reader(reader: conn)

	// Read request line
	request_line := reader.read_line() or {
		conn.close() or {}
		return
	}
	parts := request_line.trim_right('\r\n').split(' ')
	if parts.len < 2 {
		s.send_error(mut conn, 400, 40001, 'Bad Request')
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
		if trimmed == '' {
			break
		}
		if idx := trimmed.index(':') {
			key := trimmed[..idx].trim_space()
			value := trimmed[idx + 1..].trim_space()
			headers[key] = value
		}
	}

	// Read body: since headers were read from buffered reader, body must be read from the same reader
	mut body := ''
	content_length_str := headers['Content-Length'] or { headers['content-length'] or { '0' } }
	content_length := content_length_str.int()
	if content_length > 0 && content_length <= 1024 * 1024 {
		mut body_buf := []u8{len: content_length}
		reader.read(mut body_buf) or {}
		body = body_buf.bytestr()
	}

	// Get client IP
	client_ip := conn.peer_ip() or { '0.0.0.0' }

	parsed := ParsedRequest{
		method:  method
		path:    path
		query:   query
		headers: headers
		body:    body
	}

	// Route request
	s.route_request(parsed, client_ip, mut conn)
}

// route_request routes an HTTP request to the appropriate handler.
fn (mut s RestServer) route_request(parsed ParsedRequest, client_ip string, mut conn net.TcpConn) {
	// WebSocket upgrade
	upgrade := parsed.headers['Upgrade'] or { parsed.headers['upgrade'] or { '' } }
	if upgrade.to_lower() == 'websocket' && (parsed.path == '/v1/ws' || parsed.path == '/ws') {
		s.handle_websocket(parsed.headers, client_ip, mut conn)
		return
	}

	// Stats API (check before SSE to avoid path conflicts)
	if parsed.path == '/v1/sse/stats' && parsed.method == 'GET' {
		s.handle_sse_stats(mut conn)
		return
	}

	if parsed.path == '/v1/ws/stats' && parsed.method == 'GET' {
		s.handle_ws_stats(mut conn)
		return
	}

	// SSE endpoint
	if parsed.path.contains('/sse') && parsed.method == 'GET' {
		s.handle_sse(parsed.path, parsed.query, parsed.headers, client_ip, mut conn)
		return
	}

	// Topics API
	if parsed.path.starts_with('/v1/topics') {
		s.handle_topics_api(parsed.method, parsed.path, parsed.query, parsed.headers,
			parsed.body, mut conn)
		return
	}

	// Iceberg Catalog API
	if parsed.path.starts_with('/v1/iceberg') || parsed.path.starts_with('/v1/config') {
		s.handle_iceberg_catalog_api(parsed.method, parsed.path, parsed.headers, parsed.body, mut
			conn)
		return
	}

	// Schema Registry API
	if parsed.path.starts_with('/subjects') || parsed.path.starts_with('/schemas')
		|| parsed.path.starts_with('/config') || parsed.path.starts_with('/compatibility') {
		s.handle_schema_api(parsed.method, parsed.path, parsed.headers, parsed.body, mut
			conn)
		return
	}

	// Health endpoints (Kubernetes compatible)
	if parsed.path == '/health' || parsed.path == '/healthz' {
		s.handle_health(mut conn)
		return
	}

	if parsed.path == '/ready' || parsed.path == '/readyz' {
		s.handle_ready(mut conn)
		return
	}

	if parsed.path == '/live' || parsed.path == '/livez' {
		s.handle_live(mut conn)
		return
	}

	// Metrics endpoint (Prometheus format)
	if parsed.path == '/metrics' {
		s.handle_metrics(mut conn)
		return
	}

	// Static files (test client)
	if parsed.path == '/' || parsed.path == '/index.html' || parsed.path.starts_with('/static/') {
		s.serve_static_file(parsed.path, mut conn)
		return
	}

	// Not found
	s.send_error(mut conn, 404, 40401, 'Not Found')
	conn.close() or {}
}

// WebSocket handler

// handle_websocket handles a WebSocket upgrade request.
fn (mut s RestServer) handle_websocket(headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// Handle upgrade
	conn_id := s.ws_handler.handle_upgrade(mut conn, headers, client_ip) or {
		s.send_error(mut conn, 400, 40001, 'WebSocket upgrade failed: ${err}')
		conn.close() or {}
		return
	}

	// Start WebSocket connection (blocks until connection is closed)
	s.ws_handler.start_connection(conn_id, mut conn)
}

// handle_ws_stats handles a WebSocket statistics request.
fn (mut s RestServer) handle_ws_stats(mut conn net.TcpConn) {
	stats := s.ws_handler.get_stats()
	resp := WSStatsResponse{
		active_connections:  stats.active_connections
		total_subscriptions: stats.total_subscriptions
		messages_sent:       stats.messages_sent
		messages_received:   stats.messages_received
		bytes_sent:          stats.bytes_sent
		bytes_received:      stats.bytes_received
		connections_created: stats.connections_created
		connections_closed:  stats.connections_closed
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// SSE handler

// handle_sse handles an SSE streaming request.
fn (mut s RestServer) handle_sse(path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// Parse SSE request
	request := proto_http.parse_sse_request(path, query, headers, client_ip) or {
		s.send_error(mut conn, 400, 40001, 'Invalid SSE request: ${err}')
		conn.close() or {}
		return
	}

	// Handle SSE request
	status, resp_headers, should_stream := s.sse_handler.handle_sse_request(request) or {
		s.send_error(mut conn, 500, 50001, 'Internal server error')
		conn.close() or {}
		return
	}

	if !should_stream {
		s.send_error(mut conn, status, 50001, 'SSE request failed')
		conn.close() or {}
		return
	}

	// Get connection ID from headers
	conn_id := resp_headers['X-SSE-Connection-Id'] or { '' }
	if conn_id == '' {
		s.send_error(mut conn, 500, 50001, 'Failed to create SSE connection')
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

	// Send initial connection event
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

// handle_sse_stats handles an SSE statistics request.
fn (mut s RestServer) handle_sse_stats(mut conn net.TcpConn) {
	stats := s.sse_handler.get_stats()
	resp := SSEStatsResponse{
		active_connections:  stats.active_connections
		total_subscriptions: stats.total_subscriptions
		messages_sent:       stats.messages_sent
		bytes_sent:          stats.bytes_sent
		connections_created: stats.connections_created
		connections_closed:  stats.connections_closed
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// Health & metrics handlers

// handle_health handles the /health endpoint.
// Returns full health status including storage state.
fn (mut s RestServer) handle_health(mut conn net.TcpConn) {
	// Check storage health
	storage_status := s.storage.health_check() or {
		resp := HealthResponse{
			status:  'unhealthy'
			storage: 'error'
		}
		s.send_json(mut conn, 503, json.encode(resp))
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

	resp := HealthResponse{
		status:         status
		storage:        status
		uptime_seconds: uptime_seconds
		version:        '0.44.1'
	}
	s.send_json(mut conn, http_status, json.encode(resp))
	conn.close() or {}
}

// handle_ready handles the /ready endpoint.
// Returns whether the server is ready to receive traffic.
fn (mut s RestServer) handle_ready(mut conn net.TcpConn) {
	if s.ready {
		// Additional readiness checks possible
		storage_status := s.storage.health_check() or {
			resp := ReadyResponse{
				ready:  false
				reason: 'storage_unavailable'
			}
			s.send_json(mut conn, 503, json.encode(resp))
			conn.close() or {}
			return
		}

		if storage_status == .unhealthy {
			resp := ReadyResponse{
				ready:  false
				reason: 'storage_unhealthy'
			}
			s.send_json(mut conn, 503, json.encode(resp))
			conn.close() or {}
			return
		}

		resp := ReadyResponse{
			ready: true
		}
		s.send_json(mut conn, 200, json.encode(resp))
	} else {
		resp := ReadyResponse{
			ready:  false
			reason: 'server_not_ready'
		}
		s.send_json(mut conn, 503, json.encode(resp))
	}
	conn.close() or {}
}

// handle_live handles the /live endpoint.
// Returns whether the server process is alive.
fn (mut s RestServer) handle_live(mut conn net.TcpConn) {
	// Liveness check - only verify that the server is running
	if s.running {
		s.send_json(mut conn, 200, json.encode(LiveResponse{ alive: true }))
	} else {
		s.send_json(mut conn, 503, json.encode(LiveResponse{ alive: false }))
	}
	conn.close() or {}
}

// handle_metrics handles the /metrics endpoint (Prometheus format).
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

// Topics API handlers

// handle_topics_api handles topic REST API requests.
fn (mut s RestServer) handle_topics_api(method string, path string, query map[string]string, headers map[string]string, body string, mut conn net.TcpConn) {
	parts := path.trim_left('/').split('/')

	// GET /v1/topics - list topics
	// POST /v1/topics - create topic
	if parts.len == 2 && parts[1] == 'topics' {
		match method {
			'GET' {
				s.list_topics(mut conn)
			}
			'POST' {
				s.create_topic(body, mut conn)
			}
			else {
				s.send_error(mut conn, 405, 40501, 'Method Not Allowed')
				conn.close() or {}
			}
		}
		return
	}

	// GET /v1/topics/{topic} - topic info
	// DELETE /v1/topics/{topic} - delete topic
	if parts.len == 3 && parts[1] == 'topics' {
		match method {
			'GET' {
				s.get_topic(parts[2], mut conn)
			}
			'DELETE' {
				s.delete_topic(parts[2], mut conn)
			}
			else {
				s.send_error(mut conn, 405, 40501, 'Method Not Allowed')
				conn.close() or {}
			}
		}
		return
	}

	s.send_error(mut conn, 404, 40401, 'Not Found')
	conn.close() or {}
}

// list_topics returns a list of all topics.
fn (mut s RestServer) list_topics(mut conn net.TcpConn) {
	topics := s.storage.list_topics() or {
		s.send_error(mut conn, 500, 50001, 'Failed to list topics')
		conn.close() or {}
		return
	}

	mut topic_list := []TopicResponse{}
	for topic in topics {
		topic_list << TopicResponse{
			name:       topic.name
			partitions: topic.partition_count
		}
	}

	resp := TopicsListResponse{
		topics: topic_list
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// get_topic returns topic information.
fn (mut s RestServer) get_topic(name string, mut conn net.TcpConn) {
	topic := s.storage.get_topic(name) or {
		s.send_error(mut conn, 404, 40401, 'Topic not found')
		conn.close() or {}
		return
	}

	resp := TopicResponse{
		name:       topic.name
		partitions: topic.partition_count
	}
	s.send_json(mut conn, 200, json.encode(resp))
	conn.close() or {}
}

// create_topic creates a new topic.
// body is pre-read by handle_connection using a buffered reader.
fn (mut s RestServer) create_topic(body string, mut conn net.TcpConn) {
	if body == '' {
		s.send_error(mut conn, 400, 40001, 'Request body is required')
		conn.close() or {}
		return
	}

	req := json.decode(CreateTopicRequest, body) or {
		s.send_error(mut conn, 400, 40001, 'Invalid request body: ${err}')
		conn.close() or {}
		return
	}

	if req.name == '' {
		s.send_error(mut conn, 400, 40001, 'Topic name is required')
		conn.close() or {}
		return
	}

	// Validate topic name: ^[a-zA-Z0-9._-]{1,249}$
	if req.name.len > 249 || !is_valid_topic_name(req.name) {
		s.send_error(mut conn, 400, 40002, 'Invalid topic name. Use alphanumeric, dots, underscores, hyphens (max 249 chars)')
		conn.close() or {}
		return
	}

	partitions := if req.partitions > 0 { req.partitions } else { 1 }

	topic_meta := s.storage.create_topic(req.name, partitions, domain.TopicConfig{}) or {
		// Return 409 Conflict if topic already exists
		if err.msg().contains('already exists') {
			s.send_error(mut conn, 409, 40901, 'Topic already exists: ${req.name}')
		} else {
			s.send_error(mut conn, 500, 50001, 'Failed to create topic: ${err}')
		}
		conn.close() or {}
		return
	}

	resp := TopicResponse{
		name:       topic_meta.name
		partitions: topic_meta.partition_count
	}
	s.send_json(mut conn, 201, json.encode(resp))
	conn.close() or {}
}

// delete_topic deletes a topic.
fn (mut s RestServer) delete_topic(name string, mut conn net.TcpConn) {
	s.storage.delete_topic(name) or {
		if err.msg().contains('not found') || err.msg().contains('does not exist') {
			s.send_error(mut conn, 404, 40401, 'Topic not found: ${name}')
		} else {
			s.send_error(mut conn, 500, 50001, 'Failed to delete topic: ${err}')
		}
		conn.close() or {}
		return
	}

	// 204 No Content - successfully deleted
	s.send_json(mut conn, 204, '')
	conn.close() or {}
}

// Iceberg Catalog API handler

// handle_iceberg_catalog_api handles Iceberg REST Catalog API requests.
// body is pre-read by handle_connection using a buffered reader.
fn (mut s RestServer) handle_iceberg_catalog_api(method string, path string, headers map[string]string, body string, mut conn net.TcpConn) {
	unsafe {
		if s.iceberg_catalog_api == 0 {
			s.send_error(mut conn, 503, 50301, 'Iceberg Catalog not available')
			conn.close() or {}
			return
		}
	}
	// Handle Iceberg Catalog API request
	status, resp_body := s.iceberg_catalog_api.handle_request(method, path, body)
	s.send_json(mut conn, status, resp_body)
	conn.close() or {}
}

// Schema Registry API handler

// handle_schema_api handles Schema Registry REST API requests.
// body is pre-read by handle_connection using a buffered reader.
fn (mut s RestServer) handle_schema_api(method string, path string, headers map[string]string, body string, mut conn net.TcpConn) {
	unsafe {
		if s.schema_api == 0 {
			s.send_error(mut conn, 503, 50301, 'Schema Registry not available')
			conn.close() or {}
			return
		}
	}
	status, resp_body := s.schema_api.handle_request(method, path, body)
	s.send_json(mut conn, status, resp_body)
	conn.close() or {}
}

// get_content_length returns the Content-Length header value.
fn (s &RestServer) get_content_length(headers map[string]string) !int {
	content_length := headers['Content-Length'] or { headers['content-length'] or { '' } }
	if content_length == '' {
		return error('Content-Length header not found')
	}
	return content_length.int()
}

// Response helpers

// send_json sends a JSON response.
// For 204 No Content, sends only headers without a body.
fn (s &RestServer) send_json(mut conn net.TcpConn, status int, body string) {
	// 204 No Content - send only headers without body
	if status == 204 {
		conn.write_string('HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n') or {}
		return
	}

	status_text := match status {
		200 { 'OK' }
		201 { 'Created' }
		400 { 'Bad Request' }
		403 { 'Forbidden' }
		404 { 'Not Found' }
		405 { 'Method Not Allowed' }
		409 { 'Conflict' }
		412 { 'Precondition Failed' }
		500 { 'Internal Server Error' }
		501 { 'Not Implemented' }
		503 { 'Service Unavailable' }
		else { 'Unknown' }
	}

	response := 'HTTP/1.1 ${status} ${status_text}\r\n' + 'Content-Type: application/json\r\n' +
		'Content-Length: ${body.len}\r\n' + 'Connection: close\r\n' + '\r\n' + body

	conn.write_string(response) or {}
}

// send_error sends an error response.
fn (s &RestServer) send_error(mut conn net.TcpConn, status int, error_code int, message string) {
	resp := RestErrorResponse{
		error_code: error_code
		message:    message
	}
	s.send_json(mut conn, status, json.encode(resp))
}

// serve_static_file serves static files from the static directory.
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
		s.send_error(mut conn, 403, 40301, 'Forbidden')
		conn.close() or {}
		return
	}

	// Read file
	content := os.read_file(file_path) or {
		s.send_error(mut conn, 404, 40401, 'File not found')
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

// get_content_type returns the content type for a file.
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

// is_valid_topic_name validates that a topic name matches the pattern ^[a-zA-Z0-9._-]{1,249}$.
fn is_valid_topic_name(name string) bool {
	if name == '' || name.len > 249 {
		return false
	}
	mut re := regex.regex_opt(r'^[a-zA-Z0-9._\-]+$') or { return false }
	return re.matches_string(name)
}

// parse_query_string parses a URL query string into a map.
fn parse_query_string(query string) map[string]string {
	mut result := map[string]string{}
	if query == '' {
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
