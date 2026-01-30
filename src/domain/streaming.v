// 도메인 레이어 - 스트리밍 모델
// SSE (Server-Sent Events) 및 WebSocket 스트리밍 도메인 모델
module domain

import time

// SSE 이벤트 타입

/// SSEEventType은 SSE 이벤트의 유형을 나타냅니다.
pub enum SSEEventType {
	message    // 토픽에서 온 일반 메시지
	heartbeat  // Keep-alive ping
	error      // 에러 알림
	close      // 스트림 종료
	subscribed // 구독 확인
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
	id         string       // 이벤트 ID (topic:partition:offset)
	event_type SSEEventType // 이벤트 유형
	data       string       // JSON 인코딩된 데이터
	retry      int          // 재시도 간격 (밀리초, 선택사항)
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
	topic     string            // 토픽 이름
	partition i32               // 파티션 번호
	offset    i64               // 메시지 오프셋
	timestamp i64               // 메시지 타임스탬프 (Unix 밀리초)
	key       ?string           // 메시지 키 (선택사항)
	value     string            // 메시지 값
	headers   map[string]string // 메시지 헤더
}

// 구독 모델

/// SubscriptionOffset은 소비 시작 위치를 나타냅니다.
pub enum SubscriptionOffset {
	earliest // 처음부터 시작
	latest   // 끝에서 시작 (새 메시지만)
	specific // 특정 오프셋에서 시작
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
	id          string             // 고유 구독 ID
	topic       string             // 토픽 이름
	partition   ?i32               // 파티션 (none = 모든 파티션)
	offset_type SubscriptionOffset // 시작 위치
	offset      i64                // 특정 오프셋 (offset_type이 specific인 경우)
	group_id    ?string            // 컨슈머 그룹 ID (선택사항)
	client_id   string             // 클라이언트 식별자
	created_at  i64                // 구독 생성 시간
pub mut:
	current_offset i64 // 현재 위치
	last_activity  i64 // 마지막 활동 타임스탬프
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
	connecting // 초기 연결 중
	connected  // 활성 및 스트리밍 중
	paused     // 일시 중지됨
	closing    // 정상 종료 중
	closed     // 연결 종료됨
}

/// SSEConnection은 활성 SSE 연결을 나타냅니다.
pub struct SSEConnection {
pub:
	id         string // 연결 ID
	client_ip  string // 클라이언트 IP 주소
	user_agent string // 클라이언트 User Agent
	created_at i64    // 연결 생성 시간
pub mut:
	state         SSEConnectionState // 현재 상태
	subscriptions []Subscription     // 활성 구독 목록
	last_event_id string             // 마지막 전송 이벤트 ID
	messages_sent i64                // 총 전송 메시지 수
	bytes_sent    i64                // 총 전송 바이트
	last_activity i64                // 마지막 활동 타임스탬프
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
	heartbeat_interval_ms int = 30000  // Heartbeat 간격 (기본 30초)
	connection_timeout_ms int = 300000 // 연결 타임아웃 (기본 5분)
	max_connections       int = 10000  // 최대 동시 연결 수
	max_subscriptions     int = 100    // 연결당 최대 구독 수
	buffer_size           int = 1000   // 구독당 메시지 버퍼 크기
	retry_interval_ms     int = 3000   // 클라이언트 재시도 간격 힌트
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
	action    WebSocketAction   // 액션 유형
	topic     string            // 토픽 이름
	partition ?i32              // 파티션 (선택사항)
	offset    ?string           // 오프셋 (선택사항, 구독용)
	key       ?string           // 메시지 키 (produce용)
	value     ?string           // 메시지 값 (produce용)
	headers   map[string]string // 메시지 헤더 (produce용)
	group_id  ?string           // 컨슈머 그룹 ID (선택사항)
}

/// WebSocketResponse는 WebSocket 메시지 (서버 -> 클라이언트)를 나타냅니다.
pub struct WebSocketResponse {
pub:
	response_type string            // 응답 유형 (message, subscribed, produced, error, pong)
	topic         string            // 토픽 이름
	partition     i32               // 파티션 번호
	offset        i64               // 오프셋
	timestamp     i64               // 타임스탬프
	key           ?string           // 메시지 키
	value         string            // 메시지 값
	headers       map[string]string // 메시지 헤더
	error_code    ?string           // 에러 코드 (에러 응답용)
	error_message ?string           // 에러 메시지 (에러 응답용)
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
	connecting // 초기 핸드셰이크
	open       // 활성 및 준비됨
	closing    // 정상 종료 중
	closed     // 연결 종료됨
}

/// WebSocketConnection은 활성 WebSocket 연결을 나타냅니다.
pub struct WebSocketConnection {
pub:
	id         string // 연결 ID
	client_ip  string // 클라이언트 IP 주소
	user_agent string // 클라이언트 User Agent
	created_at i64    // 연결 생성 시간
pub mut:
	state         WebSocketConnectionState // 현재 상태
	subscriptions []Subscription           // 활성 구독 목록
	messages_sent i64                      // 총 전송 메시지 수
	messages_recv i64                      // 총 수신 메시지 수
	bytes_sent    i64                      // 총 전송 바이트
	bytes_recv    i64                      // 총 수신 바이트
	last_activity i64                      // 마지막 활동 타임스탬프
	last_ping     i64                      // 마지막 ping 전송 시간
	last_pong     i64                      // 마지막 pong 수신 시간
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
	ping_interval_ms      int = 30000   // Ping 간격 (기본 30초)
	pong_timeout_ms       int = 10000   // Pong 타임아웃 (기본 10초)
	connection_timeout_ms int = 300000  // 연결 타임아웃 (기본 5분)
	max_connections       int = 10000   // 최대 동시 연결 수
	max_subscriptions     int = 100     // 연결당 최대 구독 수
	max_message_size      int = 1048576 // 최대 메시지 크기 (1MB)
}

/// default_ws_config는 기본 WebSocket 설정을 반환합니다.
pub fn default_ws_config() WebSocketConfig {
	return WebSocketConfig{}
}
