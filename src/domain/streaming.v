// Domain Layer - Streaming Models
// SSE (Server-Sent Events) and WebSocket streaming domain models
module domain

import time

// ============================================================================
// SSE Event Types
// ============================================================================

// SSEEventType represents the type of SSE event
pub enum SSEEventType {
	message    // Regular message from topic
	heartbeat  // Keep-alive ping
	error      // Error notification
	close      // Stream closing
	subscribed // Subscription confirmed
}

// sse_event_type_str converts SSEEventType to string
pub fn (t SSEEventType) str() string {
	return match t {
		.message { 'message' }
		.heartbeat { 'heartbeat' }
		.error { 'error' }
		.close { 'close' }
		.subscribed { 'subscribed' }
	}
}

// SSEEvent represents a Server-Sent Event
pub struct SSEEvent {
pub:
	id         string       // Event ID (topic:partition:offset)
	event_type SSEEventType // Event type
	data       string       // JSON encoded data
	retry      int          // Retry interval in ms (optional)
}

// new_sse_message_event creates a new message event
pub fn new_sse_message_event(topic string, partition i32, offset i64, data string) SSEEvent {
	return SSEEvent{
		id:         '${topic}:${partition}:${offset}'
		event_type: .message
		data:       data
	}
}

// new_sse_heartbeat_event creates a new heartbeat event
pub fn new_sse_heartbeat_event() SSEEvent {
	return SSEEvent{
		id:         'heartbeat'
		event_type: .heartbeat
		data:       '{"timestamp":${time.now().unix_milli()}}'
	}
}

// new_sse_error_event creates a new error event
pub fn new_sse_error_event(code string, message string) SSEEvent {
	return SSEEvent{
		id:         'error'
		event_type: .error
		data:       '{"code":"${code}","message":"${message}"}'
	}
}

// new_sse_close_event creates a new close event
pub fn new_sse_close_event(reason string) SSEEvent {
	return SSEEvent{
		id:         'close'
		event_type: .close
		data:       '{"reason":"${reason}"}'
	}
}

// encode formats the SSE event for HTTP streaming
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

// ============================================================================
// SSE Message Data
// ============================================================================

// SSEMessageData represents the data payload of an SSE message event
pub struct SSEMessageData {
pub:
	topic     string            // Topic name
	partition i32               // Partition number
	offset    i64               // Message offset
	timestamp i64               // Message timestamp (unix millis)
	key       ?string           // Message key (optional)
	value     string            // Message value
	headers   map[string]string // Message headers
}

// ============================================================================
// Subscription Models
// ============================================================================

// SubscriptionOffset represents where to start consuming
pub enum SubscriptionOffset {
	earliest // Start from the beginning
	latest   // Start from the end (new messages only)
	specific // Start from a specific offset
}

// subscription_offset_from_str parses offset string
pub fn subscription_offset_from_str(s string) SubscriptionOffset {
	return match s.to_lower() {
		'earliest', 'beginning', '0' { .earliest }
		'latest', 'end', '-1' { .latest }
		else { .specific }
	}
}

// Subscription represents a client's subscription to a topic/partition
pub struct Subscription {
pub:
	id          string             // Unique subscription ID
	topic       string             // Topic name
	partition   ?i32               // Partition (none = all partitions)
	offset_type SubscriptionOffset // Where to start
	offset      i64                // Specific offset (if offset_type is specific)
	group_id    ?string            // Consumer group ID (optional)
	client_id   string             // Client identifier
	created_at  i64                // Subscription creation time
pub mut:
	current_offset i64 // Current position
	last_activity  i64 // Last activity timestamp
}

// new_subscription creates a new subscription
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

// generate_subscription_id generates a unique subscription ID
fn generate_subscription_id() string {
	return 'sub-${time.now().unix_nano()}'
}

// ============================================================================
// SSE Connection State
// ============================================================================

// SSEConnectionState represents the state of an SSE connection
pub enum SSEConnectionState {
	connecting // Initial connection
	connected  // Active and streaming
	paused     // Temporarily paused
	closing    // Graceful shutdown
	closed     // Connection closed
}

// SSEConnection represents an active SSE connection
pub struct SSEConnection {
pub:
	id         string // Connection ID
	client_ip  string // Client IP address
	user_agent string // Client user agent
	created_at i64    // Connection creation time
pub mut:
	state         SSEConnectionState // Current state
	subscriptions []Subscription     // Active subscriptions
	last_event_id string             // Last sent event ID
	messages_sent i64                // Total messages sent
	bytes_sent    i64                // Total bytes sent
	last_activity i64                // Last activity timestamp
}

// new_sse_connection creates a new SSE connection
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

// ============================================================================
// SSE Configuration
// ============================================================================

// SSEConfig holds SSE server configuration
pub struct SSEConfig {
pub:
	heartbeat_interval_ms int = 30000  // Heartbeat interval (default 30s)
	connection_timeout_ms int = 300000 // Connection timeout (default 5min)
	max_connections       int = 10000  // Maximum concurrent connections
	max_subscriptions     int = 100    // Max subscriptions per connection
	buffer_size           int = 1000   // Message buffer size per subscription
	retry_interval_ms     int = 3000   // Client retry interval hint
}

// default_sse_config returns default SSE configuration
pub fn default_sse_config() SSEConfig {
	return SSEConfig{}
}

// ============================================================================
// WebSocket Models
// ============================================================================

// WebSocketAction represents client actions over WebSocket
pub enum WebSocketAction {
	subscribe
	unsubscribe
	produce
	commit
	ping
}

// websocket_action_from_str parses action string
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

// WebSocketMessage represents a WebSocket message (client -> server)
pub struct WebSocketMessage {
pub:
	action    WebSocketAction   // Action type
	topic     string            // Topic name
	partition ?i32              // Partition (optional)
	offset    ?string           // Offset (optional, for subscribe)
	key       ?string           // Message key (for produce)
	value     ?string           // Message value (for produce)
	headers   map[string]string // Message headers (for produce)
	group_id  ?string           // Consumer group ID (optional)
}

// WebSocketResponse represents a WebSocket message (server -> client)
pub struct WebSocketResponse {
pub:
	response_type string            // Response type (message, subscribed, produced, error, pong)
	topic         string            // Topic name
	partition     i32               // Partition number
	offset        i64               // Offset
	timestamp     i64               // Timestamp
	key           ?string           // Message key
	value         string            // Message value
	headers       map[string]string // Message headers
	error_code    ?string           // Error code (for error responses)
	error_message ?string           // Error message (for error responses)
}

// new_ws_message_response creates a message response
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

// new_ws_subscribed_response creates a subscription confirmation response
pub fn new_ws_subscribed_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'subscribed'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

// new_ws_produced_response creates a produce confirmation response
pub fn new_ws_produced_response(topic string, partition i32, offset i64) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'produced'
		topic:         topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

// new_ws_error_response creates an error response
pub fn new_ws_error_response(code string, message string) WebSocketResponse {
	return WebSocketResponse{
		response_type: 'error'
		error_code:    code
		error_message: message
		timestamp:     time.now().unix_milli()
	}
}

// new_ws_pong_response creates a pong response
pub fn new_ws_pong_response() WebSocketResponse {
	return WebSocketResponse{
		response_type: 'pong'
		timestamp:     time.now().unix_milli()
	}
}

// to_json converts WebSocketResponse to JSON string
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

// escape_json_str escapes special characters for JSON
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

// ============================================================================
// WebSocket Connection State
// ============================================================================

// WebSocketConnectionState represents the state of a WebSocket connection
pub enum WebSocketConnectionState {
	connecting // Initial handshake
	open       // Active and ready
	closing    // Graceful shutdown
	closed     // Connection closed
}

// WebSocketConnection represents an active WebSocket connection
pub struct WebSocketConnection {
pub:
	id         string // Connection ID
	client_ip  string // Client IP address
	user_agent string // Client user agent
	created_at i64    // Connection creation time
pub mut:
	state         WebSocketConnectionState // Current state
	subscriptions []Subscription           // Active subscriptions
	messages_sent i64                      // Total messages sent
	messages_recv i64                      // Total messages received
	bytes_sent    i64                      // Total bytes sent
	bytes_recv    i64                      // Total bytes received
	last_activity i64                      // Last activity timestamp
	last_ping     i64                      // Last ping sent
	last_pong     i64                      // Last pong received
}

// new_ws_connection creates a new WebSocket connection
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

// ============================================================================
// WebSocket Configuration
// ============================================================================

// WebSocketConfig holds WebSocket server configuration
pub struct WebSocketConfig {
pub:
	ping_interval_ms      int = 30000   // Ping interval (default 30s)
	pong_timeout_ms       int = 10000   // Pong timeout (default 10s)
	connection_timeout_ms int = 300000  // Connection timeout (default 5min)
	max_connections       int = 10000   // Maximum concurrent connections
	max_subscriptions     int = 100     // Max subscriptions per connection
	max_message_size      int = 1048576 // Max message size (1MB)
}

// default_ws_config returns default WebSocket configuration
pub fn default_ws_config() WebSocketConfig {
	return WebSocketConfig{}
}
