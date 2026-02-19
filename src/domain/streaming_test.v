// SSE 도메인 모델 테스트
module domain

fn test_sse_event_type_str() {
	assert SSEEventType.message.str() == 'message'
	assert SSEEventType.heartbeat.str() == 'heartbeat'
	assert SSEEventType.error.str() == 'error'
	assert SSEEventType.close.str() == 'close'
	assert SSEEventType.subscribed.str() == 'subscribed'
}

fn test_new_sse_message_event() {
	event := new_sse_message_event('test-topic', 0, 100, '{"key":"value"}')

	assert event.id == 'test-topic:0:100'
	assert event.event_type == .message
	assert event.data == '{"key":"value"}'
	assert event.retry == 0
}

fn test_new_sse_heartbeat_event() {
	event := new_sse_heartbeat_event()

	assert event.id == 'heartbeat'
	assert event.event_type == .heartbeat
	assert event.data.contains('timestamp')
}

fn test_new_sse_error_event() {
	event := new_sse_error_event('TOPIC_NOT_FOUND', 'Topic does not exist')

	assert event.id == 'error'
	assert event.event_type == .error
	assert event.data.contains('TOPIC_NOT_FOUND')
	assert event.data.contains('Topic does not exist')
}

fn test_new_sse_close_event() {
	event := new_sse_close_event('server shutdown')

	assert event.id == 'close'
	assert event.event_type == .close
	assert event.data.contains('server shutdown')
}

fn test_sse_event_encode() {
	event := SSEEvent{
		id:         'test-id'
		event_type: .message
		data:       '{"test":"data"}'
		retry:      3000
	}

	encoded := event.encode()

	assert encoded.contains('id: test-id')
	assert encoded.contains('event: message')
	assert encoded.contains('data: {"test":"data"}')
	assert encoded.contains('retry: 3000')
	assert encoded.ends_with('\n\n')
}

fn test_sse_event_encode_no_retry() {
	event := SSEEvent{
		id:         'test-id'
		event_type: .heartbeat
		data:       '{}'
	}

	encoded := event.encode()

	assert !encoded.contains('retry:')
}

fn test_subscription_offset_from_str() {
	assert subscription_offset_from_str('earliest') == .earliest
	assert subscription_offset_from_str('beginning') == .earliest
	assert subscription_offset_from_str('0') == .earliest
	assert subscription_offset_from_str('latest') == .latest
	assert subscription_offset_from_str('end') == .latest
	assert subscription_offset_from_str('-1') == .latest
	assert subscription_offset_from_str('100') == .specific
	assert subscription_offset_from_str('unknown') == .specific
}

fn test_new_subscription() {
	sub := new_subscription('test-topic', i32(0), .earliest, 0, 'test-group', 'test-client')

	assert sub.topic == 'test-topic'
	assert sub.partition or { -1 } == 0
	assert sub.offset_type == .earliest
	assert sub.group_id or { '' } == 'test-group'
	assert sub.client_id == 'test-client'
	assert sub.id.starts_with('sub-')
	assert sub.created_at > 0
	assert sub.current_offset == 0
}

fn test_new_subscription_no_partition() {
	sub := new_subscription('test-topic', none, .latest, 0, none, 'test-client')

	assert sub.topic == 'test-topic'
	assert sub.partition == none
	assert sub.offset_type == .latest
	assert sub.group_id == none
}

fn test_new_sse_connection() {
	conn := new_sse_connection('192.168.1.1', 'Mozilla/5.0')

	assert conn.client_ip == '192.168.1.1'
	assert conn.user_agent == 'Mozilla/5.0'
	assert conn.state == .connecting
	assert conn.id.starts_with('sse-')
	assert conn.subscriptions.len == 0
	assert conn.messages_sent == 0
	assert conn.bytes_sent == 0
}

fn test_default_sse_config() {
	config := default_sse_config()

	assert config.heartbeat_interval_ms == 30000
	assert config.connection_timeout_ms == 300000
	assert config.max_connections == 10000
	assert config.max_subscriptions == 100
	assert config.buffer_size == 1000
	assert config.retry_interval_ms == 3000
}

fn test_websocket_action_from_str() {
	assert websocket_action_from_str('subscribe') or { WebSocketAction.ping } == .subscribe
	assert websocket_action_from_str('unsubscribe') or { WebSocketAction.ping } == .unsubscribe
	assert websocket_action_from_str('produce') or { WebSocketAction.ping } == .produce
	assert websocket_action_from_str('commit') or { WebSocketAction.ping } == .commit
	assert websocket_action_from_str('ping') or { WebSocketAction.subscribe } == .ping
	assert websocket_action_from_str('invalid') == none
}

// WebSocket 도메인 테스트

fn test_new_ws_message_response() {
	resp := new_ws_message_response('test-topic', 0, 100, 1234567890, 'key1', 'value1',
		{
		'header1': 'val1'
	})

	assert resp.response_type == 'message'
	assert resp.topic == 'test-topic'
	assert resp.partition == 0
	assert resp.offset == 100
	assert resp.timestamp == 1234567890
	assert resp.key or { '' } == 'key1'
	assert resp.value == 'value1'
	assert resp.headers['header1'] == 'val1'
}

fn test_new_ws_subscribed_response() {
	resp := new_ws_subscribed_response('test-topic', 0, 50)

	assert resp.response_type == 'subscribed'
	assert resp.topic == 'test-topic'
	assert resp.partition == 0
	assert resp.offset == 50
	assert resp.timestamp > 0
}

fn test_new_ws_produced_response() {
	resp := new_ws_produced_response('test-topic', 1, 200)

	assert resp.response_type == 'produced'
	assert resp.topic == 'test-topic'
	assert resp.partition == 1
	assert resp.offset == 200
	assert resp.timestamp > 0
}

fn test_new_ws_error_response() {
	resp := new_ws_error_response('TOPIC_NOT_FOUND', 'Topic does not exist')

	assert resp.response_type == 'error'
	assert resp.error_code or { '' } == 'TOPIC_NOT_FOUND'
	assert resp.error_message or { '' } == 'Topic does not exist'
	assert resp.timestamp > 0
}

fn test_new_ws_pong_response() {
	resp := new_ws_pong_response()

	assert resp.response_type == 'pong'
	assert resp.timestamp > 0
}

fn test_ws_response_to_json_message() {
	resp := new_ws_message_response('test-topic', 0, 100, 1234567890, 'key1', 'value1',
		{
		'h1': 'v1'
	})

	json := resp.to_json()

	assert json.contains('"type":"message"')
	assert json.contains('"topic":"test-topic"')
	assert json.contains('"partition":0')
	assert json.contains('"offset":100')
	assert json.contains('"key":"key1"')
	assert json.contains('"value":"value1"')
	assert json.contains('"headers":{')
}

fn test_ws_response_to_json_error() {
	resp := new_ws_error_response('ERR_CODE', 'Error message')

	json := resp.to_json()

	assert json.contains('"type":"error"')
	assert json.contains('"code":"ERR_CODE"')
	assert json.contains('"message":"Error message"')
}

fn test_new_ws_connection() {
	conn := new_ws_connection('192.168.1.1', 'Mozilla/5.0')

	assert conn.client_ip == '192.168.1.1'
	assert conn.user_agent == 'Mozilla/5.0'
	assert conn.state == .connecting
	assert conn.id.starts_with('ws-')
	assert conn.subscriptions.len == 0
	assert conn.messages_sent == 0
	assert conn.messages_recv == 0
	assert conn.bytes_sent == 0
	assert conn.bytes_recv == 0
	assert conn.last_ping == 0
	assert conn.last_pong == 0
}

fn test_default_ws_config() {
	config := default_ws_config()

	assert config.ping_interval_ms == 30000
	assert config.pong_timeout_ms == 10000
	assert config.connection_timeout_ms == 300000
	assert config.max_connections == 10000
	assert config.max_subscriptions == 100
	assert config.max_message_size == 1048576
}
