// Tests for WebSocket JSON message parsing
module http

import domain

fn test_parse_ws_message_subscribe_with_all_fields() {
	json_str := '{"action":"subscribe","topic":"test-topic","partition":0,"offset":"latest","group_id":"my-group","headers":{"key1":"val1"}}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse, got: ${err}'
		return
	}
	assert msg.action == .subscribe
	assert msg.topic == 'test-topic'
	assert msg.partition or { -1 } == 0
	assert (msg.offset or { '' }) == 'latest'
	assert (msg.group_id or { '' }) == 'my-group'
	assert msg.headers['key1'] == 'val1'
}

fn test_parse_ws_message_produce_with_key_value() {
	json_str := '{"action":"produce","topic":"events","key":"user-1","value":"hello world"}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse, got: ${err}'
		return
	}
	assert msg.action == .produce
	assert msg.topic == 'events'
	assert (msg.key or { '' }) == 'user-1'
	assert (msg.value or { '' }) == 'hello world'
}

fn test_parse_ws_message_minimal_action_only() {
	json_str := '{"action":"ping"}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse, got: ${err}'
		return
	}
	assert msg.action == .ping
	assert msg.topic == ''
}

fn test_parse_ws_message_missing_action_returns_error() {
	json_str := '{"topic":"test"}'
	if _ := parse_ws_message(json_str) {
		assert false, 'Expected error for missing action'
	}
}

fn test_parse_ws_message_invalid_action_returns_error() {
	json_str := '{"action":"invalid_action"}'
	if _ := parse_ws_message(json_str) {
		assert false, 'Expected error for invalid action'
	}
}

fn test_parse_ws_message_malformed_json_returns_error() {
	json_str := '{not valid json}'
	if _ := parse_ws_message(json_str) {
		assert false, 'Expected error for malformed JSON'
	}
}

fn test_parse_ws_message_empty_string_returns_error() {
	if _ := parse_ws_message('') {
		assert false, 'Expected error for empty string'
	}
}

fn test_parse_ws_message_extra_fields_ignored() {
	json_str := '{"action":"commit","topic":"test","extra_field":"should_be_ignored","unknown":42}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse with extra fields, got: ${err}'
		return
	}
	assert msg.action == .commit
	assert msg.topic == 'test'
}

fn test_parse_ws_message_all_actions() {
	actions := ['subscribe', 'unsubscribe', 'produce', 'commit', 'ping']
	expected := [domain.WebSocketAction.subscribe, .unsubscribe, .produce, .commit, .ping]

	for i, action_str in actions {
		json_str := '{"action":"${action_str}"}'
		msg := parse_ws_message(json_str) or {
			assert false, 'Failed to parse action ${action_str}: ${err}'
			return
		}
		assert msg.action == expected[i], 'Mismatch for action: ${action_str}'
	}
}

fn test_parse_ws_message_unsubscribe_with_topic() {
	json_str := '{"action":"unsubscribe","topic":"my-topic","partition":2}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse, got: ${err}'
		return
	}
	assert msg.action == .unsubscribe
	assert msg.topic == 'my-topic'
	assert msg.partition or { -1 } == 2
}

fn test_parse_ws_message_null_optional_fields() {
	json_str := '{"action":"subscribe","topic":"test","partition":null,"offset":null}'
	msg := parse_ws_message(json_str) or {
		assert false, 'Expected successful parse, got: ${err}'
		return
	}
	assert msg.action == .subscribe
	assert msg.topic == 'test'
	assert msg.partition == none
	assert msg.offset == none
}
