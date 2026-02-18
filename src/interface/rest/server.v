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
import json
import net
import os
import regex
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

// ParsedRequest는 handle_connection에서 파싱된 HTTP 요청을 담는 구조체입니다.
struct ParsedRequest {
	method  string
	path    string
	query   map[string]string
	headers map[string]string
	body    string
}

// 응답 구조체

// TopicResponse는 단일 토픽 정보를 나타내는 응답 구조체입니다.
struct TopicResponse {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// TopicsListResponse는 토픽 목록 응답 구조체입니다.
struct TopicsListResponse {
	topics []TopicResponse @[json: 'topics']
}

// CreateTopicRequest는 POST /v1/topics 요청 구조체입니다.
struct CreateTopicRequest {
	name       string @[json: 'name']
	partitions int    @[json: 'partitions']
}

// RestErrorResponse는 REST API 에러 응답 구조체입니다.
struct RestErrorResponse {
	error_code int    @[json: 'error_code']
	message    string @[json: 'message']
}

// SSEStatsResponse는 SSE 통계 응답 구조체입니다.
struct SSEStatsResponse {
	active_connections  int @[json: 'active_connections']
	total_subscriptions int @[json: 'total_subscriptions']
	messages_sent       i64 @[json: 'messages_sent']
	bytes_sent          i64 @[json: 'bytes_sent']
	connections_created i64 @[json: 'connections_created']
	connections_closed  i64 @[json: 'connections_closed']
}

// WSStatsResponse는 WebSocket 통계 응답 구조체입니다.
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

// HealthResponse는 헬스체크 응답 구조체입니다.
struct HealthResponse {
	status         string @[json: 'status']
	storage        string @[json: 'storage']
	uptime_seconds i64    @[json: 'uptime_seconds']
	version        string @[json: 'version']
}

// ReadyResponse는 /ready 엔드포인트 응답 구조체입니다.
struct ReadyResponse {
	ready  bool   @[json: 'ready']
	reason string @[json: 'reason'; omitempty]
}

// LiveResponse는 /live 엔드포인트 응답 구조체입니다.
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

// handle_connection은 단일 HTTP 연결을 처리합니다.
fn (mut s RestServer) handle_connection(mut conn net.TcpConn) {
	// HTTP 요청 읽기 - buffered reader로 헤더와 body를 모두 읽어 데이터 유실 방지
	mut reader := io.new_buffered_reader(reader: conn)

	// 요청 라인 읽기
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

	// body 읽기: buffered reader로 헤더를 읽었으므로 body도 동일한 reader에서 읽어야 함
	mut body := ''
	content_length_str := headers['Content-Length'] or { headers['content-length'] or { '0' } }
	content_length := content_length_str.int()
	if content_length > 0 && content_length <= 1024 * 1024 {
		mut body_buf := []u8{len: content_length}
		reader.read(mut body_buf) or {}
		body = body_buf.bytestr()
	}

	// 클라이언트 IP 가져오기
	client_ip := conn.peer_ip() or { '0.0.0.0' }

	parsed := ParsedRequest{
		method:  method
		path:    path
		query:   query
		headers: headers
		body:    body
	}

	// 요청 라우팅
	s.route_request(parsed, client_ip, mut conn)
}

// route_request는 HTTP 요청을 적절한 핸들러로 라우팅합니다.
fn (mut s RestServer) route_request(parsed ParsedRequest, client_ip string, mut conn net.TcpConn) {
	// WebSocket 업그레이드
	upgrade := parsed.headers['Upgrade'] or { parsed.headers['upgrade'] or { '' } }
	if upgrade.to_lower() == 'websocket' && (parsed.path == '/v1/ws' || parsed.path == '/ws') {
		s.handle_websocket(parsed.headers, client_ip, mut conn)
		return
	}

	// Stats API (경로 충돌 방지를 위해 SSE보다 먼저 확인)
	if parsed.path == '/v1/sse/stats' && parsed.method == 'GET' {
		s.handle_sse_stats(mut conn)
		return
	}

	if parsed.path == '/v1/ws/stats' && parsed.method == 'GET' {
		s.handle_ws_stats(mut conn)
		return
	}

	// SSE 엔드포인트
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

	// 헬스 엔드포인트 (Kubernetes 호환)
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

	// 메트릭 엔드포인트 (Prometheus 형식)
	if parsed.path == '/metrics' {
		s.handle_metrics(mut conn)
		return
	}

	// 정적 파일 (테스트 클라이언트)
	if parsed.path == '/' || parsed.path == '/index.html' || parsed.path.starts_with('/static/') {
		s.serve_static_file(parsed.path, mut conn)
		return
	}

	// 찾을 수 없음
	s.send_error(mut conn, 404, 40401, 'Not Found')
	conn.close() or {}
}

// WebSocket 핸들러

// handle_websocket은 WebSocket 업그레이드 요청을 처리합니다.
fn (mut s RestServer) handle_websocket(headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// 업그레이드 처리
	conn_id := s.ws_handler.handle_upgrade(mut conn, headers, client_ip) or {
		s.send_error(mut conn, 400, 40001, 'WebSocket upgrade failed: ${err}')
		conn.close() or {}
		return
	}

	// WebSocket 연결 시작 (연결이 닫힐 때까지 블로킹)
	s.ws_handler.start_connection(conn_id, mut conn)
}

// handle_ws_stats는 WebSocket 통계 요청을 처리합니다.
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

// SSE 핸들러

// handle_sse는 SSE 스트리밍 요청을 처리합니다.
fn (mut s RestServer) handle_sse(path string, query map[string]string, headers map[string]string, client_ip string, mut conn net.TcpConn) {
	// SSE 요청 파싱
	request := proto_http.parse_sse_request(path, query, headers, client_ip) or {
		s.send_error(mut conn, 400, 40001, 'Invalid SSE request: ${err}')
		conn.close() or {}
		return
	}

	// SSE 요청 처리
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

	// 헤더에서 연결 ID 가져오기
	conn_id := resp_headers['X-SSE-Connection-Id'] or { '' }
	if conn_id == '' {
		s.send_error(mut conn, 500, 50001, 'Failed to create SSE connection')
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

// 헬스 & 메트릭 핸들러

// handle_health는 /health 엔드포인트를 처리합니다.
// 스토리지 상태를 포함한 전체 헬스 상태를 반환합니다.
fn (mut s RestServer) handle_health(mut conn net.TcpConn) {
	// 스토리지 헬스 확인
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

// handle_ready는 /ready 엔드포인트를 처리합니다.
// 서버가 트래픽을 받을 준비가 되었는지 반환합니다.
fn (mut s RestServer) handle_ready(mut conn net.TcpConn) {
	if s.ready {
		// 추가 준비 상태 확인 가능
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

// handle_live는 /live 엔드포인트를 처리합니다.
// 서버 프로세스가 살아있는지 반환합니다.
fn (mut s RestServer) handle_live(mut conn net.TcpConn) {
	// 라이브니스 확인 - 서버가 실행 중인지만 확인
	if s.running {
		s.send_json(mut conn, 200, json.encode(LiveResponse{ alive: true }))
	} else {
		s.send_json(mut conn, 503, json.encode(LiveResponse{ alive: false }))
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
fn (mut s RestServer) handle_topics_api(method string, path string, query map[string]string, headers map[string]string, body string, mut conn net.TcpConn) {
	parts := path.trim_left('/').split('/')

	// GET /v1/topics - 토픽 목록
	// POST /v1/topics - 토픽 생성
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

	// GET /v1/topics/{topic} - 토픽 정보
	// DELETE /v1/topics/{topic} - 토픽 삭제
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

// list_topics는 모든 토픽 목록을 반환합니다.
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

// get_topic은 토픽 정보를 반환합니다.
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

// create_topic은 새 토픽을 생성합니다.
// body는 handle_connection에서 buffered reader로 미리 읽어 전달됩니다.
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

	// 토픽 이름 검증: ^[a-zA-Z0-9._-]{1,249}$
	if req.name.len > 249 || !is_valid_topic_name(req.name) {
		s.send_error(mut conn, 400, 40002, 'Invalid topic name. Use alphanumeric, dots, underscores, hyphens (max 249 chars)')
		conn.close() or {}
		return
	}

	partitions := if req.partitions > 0 { req.partitions } else { 1 }

	topic_meta := s.storage.create_topic(req.name, partitions, domain.TopicConfig{}) or {
		// 이미 존재하는 토픽인 경우 409 Conflict
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

// delete_topic은 토픽을 삭제합니다.
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

	// 204 No Content - 성공적으로 삭제됨
	s.send_json(mut conn, 204, '')
	conn.close() or {}
}

// Iceberg Catalog API 핸들러

// handle_iceberg_catalog_api는 Iceberg REST Catalog API 요청을 처리합니다.
// body는 handle_connection에서 buffered reader로 미리 읽어 전달됩니다.
fn (mut s RestServer) handle_iceberg_catalog_api(method string, path string, headers map[string]string, body string, mut conn net.TcpConn) {
	unsafe {
		if s.iceberg_catalog_api == 0 {
			s.send_error(mut conn, 503, 50301, 'Iceberg Catalog not available')
			conn.close() or {}
			return
		}
	}
	// Iceberg Catalog API 요청 처리
	status, resp_body := s.iceberg_catalog_api.handle_request(method, path, body)
	s.send_json(mut conn, status, resp_body)
	conn.close() or {}
}

// Schema Registry API 핸들러

// handle_schema_api는 스키마 레지스트리 REST API 요청을 처리합니다.
// body는 handle_connection에서 buffered reader로 미리 읽어 전달됩니다.
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
// 204 No Content인 경우 본문 없이 헤더만 전송합니다.
fn (s &RestServer) send_json(mut conn net.TcpConn, status int, body string) {
	// 204 No Content는 본문 없이 헤더만 전송
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

// send_error는 에러 응답을 전송합니다.
fn (s &RestServer) send_error(mut conn net.TcpConn, status int, error_code int, message string) {
	resp := RestErrorResponse{
		error_code: error_code
		message:    message
	}
	s.send_json(mut conn, status, json.encode(resp))
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
		s.send_error(mut conn, 403, 40301, 'Forbidden')
		conn.close() or {}
		return
	}

	// 파일 읽기
	content := os.read_file(file_path) or {
		s.send_error(mut conn, 404, 40401, 'File not found')
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

// is_valid_topic_name은 토픽 이름이 ^[a-zA-Z0-9._-]{1,249}$ 패턴에 맞는지 검증합니다.
fn is_valid_topic_name(name string) bool {
	if name == '' || name.len > 249 {
		return false
	}
	mut re := regex.regex_opt(r'^[a-zA-Z0-9._\-]+$') or { return false }
	return re.matches_string(name)
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
