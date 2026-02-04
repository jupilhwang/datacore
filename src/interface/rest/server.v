// Interface Layer - REST API Server
// 인터페이스 레이어 - REST API 서버
//
// HTTP 기반 REST API, SSE(Server-Sent Events), WebSocket 스트리밍을
// 제공하는 서버입니다. Kubernetes 호환 헬스체크 및 Prometheus 메트릭을
// 지원합니다.
//
// 주요 기능:
// - REST API 엔드포인트 (토픽 관리)
// - SSE 스트리밍 (실시간 메시지 구독)
// - WebSocket 스트리밍 (양방향 통신)
// - 헬스체크 (/health, /ready, /live)
// - Prometheus 메트릭 (/metrics)
// - 정적 파일 서빙 (테스트 클라이언트)
module rest

import domain
import infra.observability
import infra.protocol.http as proto_http
import io
import net
import os
import service.port
import time

// REST API 서버

/// RestServerConfig는 REST 서버 설정을 담는 구조체입니다.
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

/// RestServer는 HTTP 기반 REST API 서버를 제공합니다.
/// SSE와 WebSocket 스트리밍을 지원합니다.
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
	ready               bool // 트래픽 수신 준비 상태
}

/// new_rest_server - creates a new REST API server
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
/// start_background - starts the REST API server in background
pub fn (mut s RestServer) start_background() {
	spawn fn [mut s] () {
		s.start() or { eprintln('[REST] Failed to start server: ${err}') }
	}()
}

/// stop - stops the REST API server
/// stop - stops the REST API server
pub fn (mut s RestServer) stop() {
	s.running = false
	println('[REST] Server stopped')
}

// handle_connection은 단일 HTTP 연결을 처리합니다.
fn (mut s RestServer) handle_connection(mut conn net.TcpConn) {
	// HTTP 요청 읽기
	mut reader := io.new_buffered_reader(reader: conn)

	// 요청 라인 읽기
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

	// 경로와 쿼리 문자열 파싱
	path_parts := full_path.split('?')
	path := path_parts[0]
	query := if path_parts.len > 1 {
		parse_query_string(path_parts[1])
	} else {
		map[string]string{}
	}

	// 헤더 읽기
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

	// 클라이언트 IP 가져오기
	client_ip := conn.peer_ip() or { '0.0.0.0' }

	// 요청 라우팅
	s.route_request(method, path, query, headers, client_ip, mut conn)
}

// route_request는 HTTP 요청을 적절한 핸들러로 라우팅합니다.
fn (mut s RestServer) route_request(method string, path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// WebSocket 업그레이드
	upgrade := headers['Upgrade'] or { headers['upgrade'] or { '' } }
	if upgrade.to_lower() == 'websocket' && (path == '/v1/ws' || path == '/ws') {
		s.handle_websocket(headers, client_ip, mut conn)
		return
	}

	// Stats API (경로 충돌 방지를 위해 SSE보다 먼저 확인)
	if path == '/v1/sse/stats' && method == 'GET' {
		s.handle_sse_stats(mut conn)
		return
	}

	if path == '/v1/ws/stats' && method == 'GET' {
		s.handle_ws_stats(mut conn)
		return
	}

	// SSE 엔드포인트
	if path.contains('/sse') && method == 'GET' {
		s.handle_sse(path, query, headers, client_ip, mut conn)
		return
	}

	// Topics API
	if path.starts_with('/v1/topics') {
		s.handle_topics_api(method, path, query, mut conn)
		return
	}

	// Iceberg Catalog API
	if path.starts_with('/v1/iceberg') || path.starts_with('/v1/config') {
		s.handle_iceberg_catalog_api(method, path, headers, mut conn)
		return
	}

	// Schema Registry API
	if path.starts_with('/subjects') || path.starts_with('/schemas') || path.starts_with('/config')
		|| path.starts_with('/compatibility') {
		s.handle_schema_api(method, path, headers, mut conn)
		return
	}

	// 헬스 엔드포인트 (Kubernetes 호환)
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

	// 메트릭 엔드포인트 (Prometheus 형식)
	if path == '/metrics' {
		s.handle_metrics(mut conn)
		return
	}

	// 정적 파일 (테스트 클라이언트)
	if path == '/' || path == '/index.html' || path.starts_with('/static/') {
		s.serve_static_file(path, mut conn)
		return
	}

	// 찾을 수 없음
	s.send_error(mut conn, 404, 'Not Found')
	conn.close() or {}
}

// WebSocket 핸들러

// handle_websocket은 WebSocket 업그레이드 요청을 처리합니다.
fn (mut s RestServer) handle_websocket(headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// 업그레이드 처리
	conn_id := s.ws_handler.handle_upgrade(mut conn, headers, client_ip) or {
		s.send_error(mut conn, 400, 'WebSocket upgrade failed: ${err}')
		conn.close() or {}
		return
	}

	// WebSocket 연결 시작 (연결이 닫힐 때까지 블로킹)
	s.ws_handler.start_connection(conn_id, mut conn)
}

// handle_ws_stats는 WebSocket 통계 요청을 처리합니다.
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

// SSE 핸들러

// handle_sse는 SSE 스트리밍 요청을 처리합니다.
fn (mut s RestServer) handle_sse(path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// SSE 요청 파싱
	request := proto_http.parse_sse_request(path, query, headers, client_ip) or {
		s.send_error(mut conn, 400, 'Invalid SSE request: ${err}')
		conn.close() or {}
		return
	}

	// SSE 요청 처리
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

	// 헤더에서 연결 ID 가져오기
	conn_id := resp_headers['X-SSE-Connection-Id'] or { '' }
	if conn_id == '' {
		s.send_error(mut conn, 500, 'Failed to create SSE connection')
		conn.close() or {}
		return
	}

	// SSE 헤더 전송
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

	// 초기 연결 이벤트 전송
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

	// SSE 라이터 생성 및 스트리밍 시작
	mut writer := proto_http.new_sse_response_writer(conn)
	s.sse_handler.start_streaming(conn_id, mut writer)
}

// handle_sse_stats는 SSE 통계 요청을 처리합니다.
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

// 헬스 & 메트릭 핸들러

// handle_health는 /health 엔드포인트를 처리합니다.
// 스토리지 상태를 포함한 전체 헬스 상태를 반환합니다.
fn (mut s RestServer) handle_health(mut conn net.TcpConn) {
	// 스토리지 헬스 확인
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

// handle_ready는 /ready 엔드포인트를 처리합니다.
// 서버가 트래픽을 받을 준비가 되었는지 반환합니다.
fn (mut s RestServer) handle_ready(mut conn net.TcpConn) {
	if s.ready {
		// 추가 준비 상태 확인 가능
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

// handle_live는 /live 엔드포인트를 처리합니다.
// 서버 프로세스가 살아있는지 반환합니다.
fn (mut s RestServer) handle_live(mut conn net.TcpConn) {
	// 라이브니스 확인 - 서버가 실행 중인지만 확인
	if s.running {
		s.send_json(mut conn, 200, '{"alive":true}')
	} else {
		s.send_json(mut conn, 503, '{"alive":false}')
	}
	conn.close() or {}
}

// handle_metrics는 /metrics 엔드포인트를 처리합니다 (Prometheus 형식).
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

// Topics API 핸들러

// handle_topics_api는 토픽 REST API 요청을 처리합니다.
fn (mut s RestServer) handle_topics_api(method string, path string, query map[string]string, mut conn net.TcpConn) {
	parts := path.trim_left('/').split('/')

	// GET /v1/topics - 토픽 목록
	if parts.len == 2 && parts[1] == 'topics' && method == 'GET' {
		s.list_topics(mut conn)
		return
	}

	// GET /v1/topics/{topic} - 토픽 정보
	if parts.len == 3 && parts[1] == 'topics' && method == 'GET' {
		s.get_topic(parts[2], mut conn)
		return
	}

	s.send_error(mut conn, 404, 'Not Found')
	conn.close() or {}
}

// list_topics는 모든 토픽 목록을 반환합니다.
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

// get_topic은 토픽 정보를 반환합니다.
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

// Iceberg Catalog API 핸들러

// handle_iceberg_catalog_api는 Iceberg REST Catalog API 요청을 처리합니다.
fn (mut s RestServer) handle_iceberg_catalog_api(method string, path string, headers map[string]string, mut conn net.TcpConn) {
	unsafe {
		if s.iceberg_catalog_api == 0 {
			s.send_error(mut conn, 503, 'Iceberg Catalog not available')
			conn.close() or {}
			return
		}
	}
	mut body := ''
	// POST/PUT 요청의 본문 읽기
	if method == 'POST' || method == 'PUT' {
		content_length := s.get_content_length(headers) or { 0 }
		if content_length > 0 && content_length < 10 * 1024 * 1024 {
			mut buf := []u8{len: content_length}
			conn.read(mut buf) or {}
			body = buf.bytestr()
		}
	}

	// Iceberg Catalog API 요청 처리
	status, resp_body := s.iceberg_catalog_api.handle_request(method, path, body)
	s.send_json(mut conn, status, resp_body)
	conn.close() or {}
}

// Schema Registry API 핸들러

// handle_schema_api는 스키마 레지스트리 REST API 요청을 처리합니다.
fn (mut s RestServer) handle_schema_api(method string, path string, headers map[string]string, mut conn net.TcpConn) {
	unsafe {
		if s.schema_api == 0 {
			s.send_error(mut conn, 503, 'Schema Registry not available')
			conn.close() or {}
			return
		}
	}
	mut body := ''
	// Read request body for POST/PUT requests
	if method == 'POST' || method == 'PUT' {
		content_length := s.get_content_length(headers) or { 0 }
		if content_length > 0 && content_length < 1024 * 1024 {
			mut buf := []u8{len: content_length}
			conn.read(mut buf) or {}
			body = buf.bytestr()
		}
	}

	status, resp_body := s.schema_api.handle_request(method, path, body)
	s.send_json(mut conn, status, resp_body)
	conn.close() or {}
}

// get_content_length는 Content-Length 헤더 값을 반환합니다.
fn (s &RestServer) get_content_length(headers map[string]string) !int {
	content_length := headers['Content-Length'] or { headers['content-length'] or { '' } }
	if content_length == '' {
		return error('Content-Length header not found')
	}
	return content_length.int()
}

// 응답 헬퍼

// send_json은 JSON 응답을 전송합니다.
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

// send_error는 에러 응답을 전송합니다.
fn (s &RestServer) send_error(mut conn net.TcpConn, status int, message string) {
	body := '{"error":"${message}"}'
	s.send_json(mut conn, status, body)
}

// 헬퍼 함수

// serve_static_file은 정적 디렉토리에서 정적 파일을 서빙합니다.
fn (s &RestServer) serve_static_file(path string, mut conn net.TcpConn) {
	// 파일 경로 결정
	file_path := if path == '/' || path == '/index.html' {
		os.join_path(s.config.static_dir, 'test_client.html')
	} else if path.starts_with('/static/') {
		os.join_path(s.config.static_dir, path[8..])
	} else {
		os.join_path(s.config.static_dir, path.trim_left('/'))
	}

	// 보안: 디렉토리 탐색 방지
	abs_static_dir := os.real_path(s.config.static_dir)
	abs_file_path := os.real_path(file_path)
	if !abs_file_path.starts_with(abs_static_dir) {
		s.send_error(mut conn, 403, 'Forbidden')
		conn.close() or {}
		return
	}

	// 파일 읽기
	content := os.read_file(file_path) or {
		s.send_error(mut conn, 404, 'File not found')
		conn.close() or {}
		return
	}

	// 콘텐츠 타입 결정
	content_type := get_content_type(file_path)

	// 응답 전송
	response := 'HTTP/1.1 200 OK\r\n' + 'Content-Type: ${content_type}\r\n' +
		'Content-Length: ${content.len}\r\n' + 'Cache-Control: no-cache\r\n' +
		'Connection: close\r\n' + '\r\n' + content

	conn.write_string(response) or {}
	conn.close() or {}
}

// get_content_type은 파일의 콘텐츠 타입을 반환합니다.
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

// parse_query_string은 URL 쿼리 문자열을 맵으로 파싱합니다.
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
