// Message and streaming handlers for REST API server
//
// Handles SSE, WebSocket, API delegation, and static file serving:
// - SSE streaming (/v1/topics/{topic}/sse)
// - WebSocket (/v1/ws)
// - SSE/WS statistics
// - Iceberg Catalog API delegation
// - Schema Registry API delegation
// - Static file serving (test client)
module rest

import domain
import infra.protocol.http as proto_http
import json
import net
import os

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

// Static file handler

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
