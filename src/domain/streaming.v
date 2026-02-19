module domain

import time

/// SSEEventType은 SSE 이벤트의 유형을 나타냅니다.
pub enum SSEEventType {
	message
	heartbeat
	error
	close
	subscribed
}

/// str은 SSEEventType을 문자열로 변환합니다.
pub fn (t SSEEventType) str() string {
	return match t {
		.message { 'message' }
		.heartbeat { 'heartbeat' }
		.error { 'error' }
		.close { 'close' }
		.subscribed { 'subscribed' }
	}
}

/// SSEEvent는 Server-Sent Event를 나타냅니다.
pub struct SSEEvent {
pub:
	id         string
	event_type SSEEventType
	data       string
	retry      int
}

/// new_sse_message_event는 새로운 메시지 이벤트를 생성합니다.
pub fn new_sse_message_event(topic string, partition i32, offset i64, data string) SSEEvent {
	return SSEEvent{
		id:         '${topic}:${partition}:${offset}'
		event_type: .message
		data:       data
	}
}

/// new_sse_heartbeat_event는 새로운 heartbeat 이벤트를 생성합니다.
pub fn new_sse_heartbeat_event() SSEEvent {
	return SSEEvent{
		id:         'heartbeat'
		event_type: .heartbeat
		data:       '{"timestamp":${time.now().unix_milli()}}'
	}
}

/// new_sse_error_event는 새로운 에러 이벤트를 생성합니다.
pub fn new_sse_error_event(code string, message string) SSEEvent {
	return SSEEvent{
		id:         'error'
		event_type: .error
		data:       '{"code":"${code}","message":"${message}"}'
	}
}

/// new_sse_close_event는 새로운 종료 이벤트를 생성합니다.
pub fn new_sse_close_event(reason string) SSEEvent {
	return SSEEvent{
		id:         'close'
		event_type: .close
		data:       '{"reason":"${reason}"}'
	}
}

/// encode는 SSE 이벤트를 HTTP 스트리밍용으로 포맷합니다.
pub fn (e &SSEEvent) encode() string {
	mut result := ''

	if e.id.len > 0 {
		result += 'id: ${e.id}\n'
	}

	result += 'event: ${e.event_type.str()}\n'
	result += 'data: ${e.data}\n'

	if e.retry > 0 {
		result += 'retry: ${e.retry}\n'
	}

	result += '\n'
	return result
}

// SSE 메시지 데이터

/// SSEMessageData는 SSE 메시지 이벤트의 데이터 페이로드를 나타냅니다.
pub struct SSEMessageData {
pub:
	topic     string
	partition i32
	offset    i64
	timestamp i64
	key       ?string
	value     string
	headers   map[string]string
}

// 구독 모델

/// SubscriptionOffset은 소비 시작 위치를 나타냅니다.
pub enum SubscriptionOffset {
	earliest
	latest
	specific
}

/// subscription_offset_from_str은 오프셋 문자열을 파싱합니다.
pub fn subscription_offset_from_str(s string) SubscriptionOffset {
	return match s.to_lower() {
		'earliest', 'beginning', '0' { .earliest }
		'latest', 'end', '-1' { .latest }
		else { .specific }
	}
}

/// Subscription은 클라이언트의 토픽/파티션 구독을 나타냅니다.
pub struct Subscription {
pub:
	id          string
	topic       string
	partition   ?i32
	offset_type SubscriptionOffset
	offset      i64
	group_id    ?string
	client_id   string
	created_at  i64
pub mut:
	current_offset i64
	last_activity  i64
}

/// new_subscription은 새로운 구독을 생성합니다.
pub fn new_subscription(topic string, partition ?i32, offset_type SubscriptionOffset, offset i64, group_id ?string, client_id string) Subscription {
	now := time.now().unix_milli()
	return Subscription{
		id:             generate_subscription_id()
		topic:          topic
		partition:      partition
		offset_type:    offset_type
		offset:         offset
		group_id:       group_id
		client_id:      client_id
		created_at:     now
		current_offset: offset
		last_activity:  now
	}
}

/// generate_subscription_id는 고유한 구독 ID를 생성합니다.
fn generate_subscription_id() string {
	return 'sub-${time.now().unix_nano()}'
}

// SSE 연결 상태

/// SSEConnectionState는 SSE 연결의 상태를 나타냅니다.
pub enum SSEConnectionState {
	connecting
	connected
	paused
	closing
	closed
}

/// SSEConnection은 활성 SSE 연결을 나타냅니다.
pub struct SSEConnection {
pub:
	id         string
	client_ip  string
	user_agent string
	created_at i64
pub mut:
	state         SSEConnectionState
	subscriptions []Subscription
	last_event_id string
	messages_sent i64
	bytes_sent    i64
	last_activity i64
}

/// new_sse_connection은 새로운 SSE 연결을 생성합니다.
pub fn new_sse_connection(client_ip string, user_agent string) SSEConnection {
	now := time.now().unix_milli()
	return SSEConnection{
		id:            'sse-${time.now().unix_nano()}'
		client_ip:     client_ip
		user_agent:    user_agent
		created_at:    now
		state:         .connecting
		subscriptions: []Subscription{}
		last_event_id: ''
		messages_sent: 0
		bytes_sent:    0
		last_activity: now
	}
}

// SSE 설정

/// SSEConfig는 SSE 서버 설정을 보관합니다.
pub struct SSEConfig {
pub:
	heartbeat_interval_ms int = 30000
	connection_timeout_ms int = 300000
	max_connections       int = 10000
	max_subscriptions     int = 100
	buffer_size           int = 1000
	retry_interval_ms     int = 3000
}

/// default_sse_config는 기본 SSE 설정을 반환합니다.
pub fn default_sse_config() SSEConfig {
	return SSEConfig{}
}

// WebSocket 모델

/// WebSocketAction은 WebSocket을 통한 클라이언트 액션을 나타냅니다.
pub enum WebSocketAction {
	subscribe
	unsubscribe
	produce
	commit
	ping
}

/// websocket_action_from_str은 액션 문자열을 파싱합니다.
pub fn websocket_action_from_str(s string) ?WebSocketAction {
	return match s.to_lower() {
		'subscribe' { .subscribe }
		'unsubscribe' { .unsubscribe }
		'produce' { .produce }
		'commit' { .commit }
		'ping' { .ping }
		else { none }
	}
}

/// WebSocketMessage는 WebSocket 메시지 (클라이언트 -> 서버)를 나타냅니다.
pub struct WebSocketMessage {
pub:
	action    WebSocketAction
	topic     string
	partition ?i32
	offset    ?string
	key       ?string
	value     ?string
	headers   map[string]string
	group_id  ?string
}

/// WebSocketResponse는 WebSocket 메시지 (서버 -> 클라이언트)를 나타냅니다.
pub struct WebSocketResponse {
pub:
	response_type string
	topic         string
	partition     i32
	offset        i64
	timestamp     i64
	key           ?string
	value         string
	headers       map[string]string
	error_code    ?string
	error_message ?string
}

/// new_ws_message_response는 메시지 응답을 생성합니다.
pub fn new_ws_message_response(topic string, partition i32, offset i64, timestamp i64, key ?string, value string, headers map[string]string) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'message'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     timestamp
		key:           key
		value:         value
		headers:       headers
	}
}

/// new_ws_subscribed_response는 구독 확인 응답을 생성합니다.
pub fn new_ws_subscribed_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'subscribed'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_produced_response는 produce 확인 응답을 생성합니다.
pub fn new_ws_produced_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'produced'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_error_response는 에러 응답을 생성합니다.
pub fn new_ws_error_response(code string, message string) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'error'
		error_code:    code
		error_message: message
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_pong_response는 pong 응답을 생성합니다.
pub fn new_ws_pong_response() WebSocketResponse {
	return WebSocketResponse{
		response_type: 'pong'
		timestamp:     time.now().unix_milli()
	}
}

/// to_json은 WebSocketResponse를 JSON 문자열로 변환합니다.
pub fn (r &WebSocketResponse) to_json() string {
	mut json := '{"type":"${r.response_type}"'

	if r.topic.len > 0 {
		json += ',"topic":"${r.topic}"'
	}
	if r.partition >= 0 {
		json += ',"partition":${r.partition}'
	}
	if r.offset > 0 || r.response_type in ['message', 'subscribed', 'produced'] {
		json += ',"offset":${r.offset}'
	}
	json += ',"timestamp":${r.timestamp}'

	if key := r.key {
		json += ',"key":"${escape_json_str(key)}"'
	}
	if r.value.len > 0 {
		json += ',"value":"${escape_json_str(r.value)}"'
	}
	if r.headers.len > 0 {
		json += ',"headers":{'
		mut first := true
		for k, v in r.headers {
			if !first {
				json += ','
			}
			json += '"${escape_json_str(k)}":"${escape_json_str(v)}"'
			first = false
		}
		json += '}'
	}
	if code := r.error_code {
		json += ',"code":"${code}"'
	}
	if msg := r.error_message {
		json += ',"message":"${escape_json_str(msg)}"'
	}

	json += '}'
	return json
}

/// escape_json_str은 JSON용 특수 문자를 이스케이프합니다.
fn escape_json_str(s string) string {
	mut result := ''
	for c in s {
		result += match c {
			`"` { '\\"' }
			`\\` { '\\\\' }
			`\n` { '\\n' }
			`\r` { '\\r' }
			`\t` { '\\t' }
			else { c.ascii_str() }
		}
	}
	return result
}

// WebSocket 연결 상태

/// WebSocketConnectionState는 WebSocket 연결의 상태를 나타냅니다.
pub enum WebSocketConnectionState {
	connecting
	open
	closing
	closed
}

/// WebSocketConnection은 활성 WebSocket 연결을 나타냅니다.
pub struct WebSocketConnection {
pub:
	id         string
	client_ip  string
	user_agent string
	created_at i64
pub mut:
	state         WebSocketConnectionState
	subscriptions []Subscription
	messages_sent i64
	messages_recv i64
	bytes_sent    i64
	bytes_recv    i64
	last_activity i64
	last_ping     i64
	last_pong     i64
}

/// new_ws_connection은 새로운 WebSocket 연결을 생성합니다.
pub fn new_ws_connection(client_ip string, user_agent string) WebSocketConnection {
	now := time.now().unix_milli()
	return WebSocketConnection{
		id:            'ws-${time.now().unix_nano()}'
		client_ip:     client_ip
		user_agent:    user_agent
		created_at:    now
		state:         .connecting
		subscriptions: []Subscription{}
		messages_sent: 0
		messages_recv: 0
		bytes_sent:    0
		bytes_recv:    0
		last_activity: now
		last_ping:     0
		last_pong:     0
	}
}

// WebSocket 설정

/// WebSocketConfig는 WebSocket 서버 설정을 보관합니다.
pub struct WebSocketConfig {
pub:
	ping_interval_ms      int = 30000
	pong_timeout_ms       int = 10000
	connection_timeout_ms int = 300000
	max_connections       int = 10000
	max_subscriptions     int = 100
	max_message_size      int = 1048576
}

/// default_ws_config는 기본 WebSocket 설정을 반환합니다.
pub fn default_ws_config() WebSocketConfig {
	return WebSocketConfig{}
}
