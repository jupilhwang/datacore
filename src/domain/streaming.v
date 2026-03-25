module domain

import time

/// SSEEventType represents the type of an SSE event.
pub enum SSEEventType {
	message
	heartbeat
	error
	close
	subscribed
}

/// str converts SSEEventType to a string.
pub fn (t SSEEventType) str() string {
	return match t {
		.message { 'message' }
		.heartbeat { 'heartbeat' }
		.error { 'error' }
		.close { 'close' }
		.subscribed { 'subscribed' }
	}
}

/// SSEEvent represents a Server-Sent Event.
pub struct SSEEvent {
pub:
	id         string
	event_type SSEEventType
	data       string
	retry      int
}

/// new_sse_message_event creates a new message event.
pub fn new_sse_message_event(topic string, partition i32, offset i64, data string) SSEEvent {
	return SSEEvent{
		id:         '${topic}:${partition}:${offset}'
		event_type: .message
		data:       data
	}
}

/// new_sse_heartbeat_event creates a new heartbeat event.
pub fn new_sse_heartbeat_event() SSEEvent {
	return SSEEvent{
		id:         'heartbeat'
		event_type: .heartbeat
		data:       '{"timestamp":${time.now().unix_milli()}}'
	}
}

/// new_sse_error_event creates a new error event.
pub fn new_sse_error_event(code string, message string) SSEEvent {
	return SSEEvent{
		id:         'error'
		event_type: .error
		data:       '{"code":"${code}","message":"${message}"}'
	}
}

/// new_sse_close_event creates a new close event.
pub fn new_sse_close_event(reason string) SSEEvent {
	return SSEEvent{
		id:         'close'
		event_type: .close
		data:       '{"reason":"${reason}"}'
	}
}

/// encode formats an SSE event for HTTP streaming.
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

// SSE message data

/// SSEMessageData represents the data payload of an SSE message event.
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

// Subscription model

/// SubscriptionOffset represents the position from which consumption starts.
pub enum SubscriptionOffset {
	earliest
	latest
	specific
}

/// subscription_offset_from_str parses an offset string.
pub fn subscription_offset_from_str(s string) SubscriptionOffset {
	return match s.to_lower() {
		'earliest', 'beginning', '0' { .earliest }
		'latest', 'end', '-1' { .latest }
		else { .specific }
	}
}

/// Subscription represents a client's subscription to a topic/partition.
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

/// new_subscription creates a new subscription.
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

/// generate_subscription_id generates a unique subscription ID.
fn generate_subscription_id() string {
	return 'sub-${time.now().unix_nano()}'
}

// SSE connection state

/// SSEConnectionState represents the state of an SSE connection.
pub enum SSEConnectionState {
	connecting
	connected
	paused
	closing
	closed
}

/// SSEConnection represents an active SSE connection.
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

/// new_sse_connection creates a new SSE connection.
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

// SSE configuration

/// SSEConfig holds SSE server configuration.
pub struct SSEConfig {
pub:
	heartbeat_interval_ms int = 30000
	connection_timeout_ms int = 300000
	max_connections       int = 10000
	max_subscriptions     int = 100
	buffer_size           int = 1000
	retry_interval_ms     int = 3000
}

/// default_sse_config returns the default SSE configuration.
pub fn default_sse_config() SSEConfig {
	return SSEConfig{}
}

// WebSocket model

/// WebSocketAction represents a client action via WebSocket.
pub enum WebSocketAction {
	subscribe
	unsubscribe
	produce
	commit
	ping
}

/// websocket_action_from_str parses an action string.
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

/// WebSocketMessage represents a WebSocket message (client -> server).
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

/// WebSocketResponse represents a WebSocket message (server -> client).
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

/// new_ws_message_response creates a message response.
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

/// new_ws_subscribed_response creates a subscription confirmation response.
pub fn new_ws_subscribed_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'subscribed'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_produced_response creates a produce confirmation response.
pub fn new_ws_produced_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'produced'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_error_response creates an error response.
pub fn new_ws_error_response(code string, message string) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'error'
		error_code:    code
		error_message: message
		timestamp:     time.now().unix_milli()
	}
}

/// new_ws_pong_response creates a pong response.
pub fn new_ws_pong_response() WebSocketResponse {
	return WebSocketResponse{
		response_type: 'pong'
		timestamp:     time.now().unix_milli()
	}
}

/// to_json converts a WebSocketResponse to a JSON string.
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
		json += ',"key":"${streaming_escape_json(key)}"'
	}
	if r.value.len > 0 {
		json += ',"value":"${streaming_escape_json(r.value)}"'
	}
	if r.headers.len > 0 {
		json += ',"headers":{'
		mut first := true
		for k, v in r.headers {
			if !first {
				json += ','
			}
			json += '"${streaming_escape_json(k)}":"${streaming_escape_json(v)}"'
			first = false
		}
		json += '}'
	}
	if code := r.error_code {
		json += ',"code":"${code}"'
	}
	if msg := r.error_message {
		json += ',"message":"${streaming_escape_json(msg)}"'
	}

	json += '}'
	return json
}

// WebSocket connection state

/// WebSocketConnectionState represents the state of a WebSocket connection.
pub enum WebSocketConnectionState {
	connecting
	open
	closing
	closed
}

/// WebSocketConnection represents an active WebSocket connection.
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

/// new_ws_connection creates a new WebSocket connection.
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

// WebSocket configuration

/// WebSocketConfig holds WebSocket server configuration.
pub struct WebSocketConfig {
pub:
	ping_interval_ms      int = 30000
	pong_timeout_ms       int = 10000
	connection_timeout_ms int = 300000
	max_connections       int = 10000
	max_subscriptions     int = 100
	max_message_size      int = 1048576
}

/// default_ws_config returns the default WebSocket configuration.
pub fn default_ws_config() WebSocketConfig {
	return WebSocketConfig{}
}

// JSON escape helper (domain-private, replacing common module dependency)

fn streaming_escape_json(s string) string {
	mut result := []u8{cap: s.len + 10}
	for c in s.bytes() {
		match c {
			`"` {
				result << [u8(`\\`), u8(`"`)]
			}
			`\\` {
				result << [u8(`\\`), u8(`\\`)]
			}
			`\n` {
				result << [u8(`\\`), u8(`n`)]
			}
			`\r` {
				result << [u8(`\\`), u8(`r`)]
			}
			`\t` {
				result << [u8(`\\`), u8(`t`)]
			}
			else {
				result << c
			}
		}
	}
	return result.bytestr()
}
