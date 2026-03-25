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

// RestErrorResponse is the REST API error response struct.
struct RestErrorResponse {
	error_code int    @[json: 'error_code']
	message    string @[json: 'message']
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
		reader.read(mut body_buf) or {
			observability.log_with_context('rest', .warn, 'Server', 'failed to read request body',
				{
				'content_length': content_length.str()
				'error':          err.str()
			})
		}
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

// Response helpers

// get_content_length returns the Content-Length header value.
fn (s &RestServer) get_content_length(headers map[string]string) !int {
	content_length := headers['Content-Length'] or { headers['content-length'] or { '' } }
	if content_length == '' {
		return error('Content-Length header not found')
	}
	return content_length.int()
}

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
