/// Unit tests - Infrastructure layer: Audit logger
module auth

import time

// AuditEventType and AuditEvent Tests

fn test_audit_event_type_string_representation() {
	assert AuditEventType.authentication_success.str() == 'authentication_success'
	assert AuditEventType.authentication_failure.str() == 'authentication_failure'
	assert AuditEventType.authorization_success.str() == 'authorization_success'
	assert AuditEventType.authorization_failure.str() == 'authorization_failure'
	assert AuditEventType.topic_created.str() == 'topic_created'
	assert AuditEventType.topic_deleted.str() == 'topic_deleted'
	assert AuditEventType.group_joined.str() == 'group_joined'
	assert AuditEventType.group_left.str() == 'group_left'
}

// Logger Creation Tests

fn test_new_audit_logger_enabled() {
	mut logger := new_audit_logger(true)

	assert logger.enabled == true
}

fn test_new_audit_logger_disabled() {
	mut logger := new_audit_logger(false)

	assert logger.enabled == false
}

// Log Auth Success Tests

fn test_log_auth_success_event() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'SCRAM-SHA-512')

	events := logger.get_recent_events(10)
	assert events.len == 1
	assert events[0].event_type == .authentication_success
	assert events[0].client_ip == '192.168.1.1'
	assert events[0].principal == 'alice'
	assert events[0].operation == 'SCRAM-SHA-512'
	assert events[0].authorized == true
	assert events[0].timestamp > 0
}

// Log Auth Failure Tests

fn test_log_auth_failure_event() {
	mut logger := new_audit_logger(true)

	logger.log_auth_failure('10.0.0.1', 'invalid credentials')

	events := logger.get_recent_events(10)
	assert events.len == 1
	assert events[0].event_type == .authentication_failure
	assert events[0].client_ip == '10.0.0.1'
	assert events[0].principal == 'anonymous'
	assert events[0].authorized == false
	assert events[0].error_message == 'invalid credentials'
}

// Log Authorization Tests

fn test_log_authorization_success() {
	mut logger := new_audit_logger(true)

	logger.log_authorization('192.168.1.1', 'alice', 'topic', 'my-topic', 'produce', true)

	events := logger.get_recent_events(10)
	assert events.len == 1
	assert events[0].event_type == .authorization_success
	assert events[0].client_ip == '192.168.1.1'
	assert events[0].principal == 'alice'
	assert events[0].resource_type == 'topic'
	assert events[0].resource_name == 'my-topic'
	assert events[0].operation == 'produce'
	assert events[0].authorized == true
}

fn test_log_authorization_failure() {
	mut logger := new_audit_logger(true)

	logger.log_authorization('192.168.1.1', 'bob', 'topic', 'restricted-topic', 'produce',
		false)

	events := logger.get_recent_events(10)
	assert events.len == 1
	assert events[0].event_type == .authorization_failure
	assert events[0].authorized == false
}

// Log Generic Event Tests

fn test_log_event_adds_to_buffer() {
	mut logger := new_audit_logger(true)

	event := AuditEvent{
		timestamp:     time.now().unix()
		event_type:    .topic_created
		client_ip:     '192.168.1.1'
		principal:     'admin'
		resource_type: 'topic'
		resource_name: 'new-topic'
		operation:     'create_topic'
		authorized:    true
	}

	logger.log_event(event)

	events := logger.get_recent_events(10)
	assert events.len == 1
	assert events[0].event_type == .topic_created
	assert events[0].resource_name == 'new-topic'
}

// Buffer Size Limit Tests

fn test_buffer_drops_oldest_when_full() {
	mut logger := new_audit_logger_with_buffer_size(true, 5)

	// Add 7 events, buffer should keep only last 5
	for i in 0 .. 7 {
		logger.log_auth_success('192.168.1.${i}', 'user${i}', 'PLAIN')
	}

	events := logger.get_recent_events(10)
	assert events.len == 5

	// Oldest events (indices 0,1) should be dropped
	// Remaining should be indices 2-6
	assert events[0].principal == 'user2'
	assert events[4].principal == 'user6'
}

fn test_buffer_default_max_size() {
	mut logger := new_audit_logger(true)

	// Default max buffer is 10000
	assert logger.max_buffer_size == 10000
}

// Get Recent Events Tests

fn test_get_recent_events_respects_count() {
	mut logger := new_audit_logger(true)

	for i in 0 .. 10 {
		logger.log_auth_success('192.168.1.${i}', 'user${i}', 'PLAIN')
	}

	// Request only 3 most recent
	events := logger.get_recent_events(3)
	assert events.len == 3
	// Should be the 3 most recent events
	assert events[0].principal == 'user7'
	assert events[1].principal == 'user8'
	assert events[2].principal == 'user9'
}

fn test_get_recent_events_returns_all_when_count_exceeds_buffer() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')
	logger.log_auth_success('192.168.1.2', 'bob', 'PLAIN')

	events := logger.get_recent_events(100)
	assert events.len == 2
}

fn test_get_recent_events_empty_buffer() {
	mut logger := new_audit_logger(true)

	events := logger.get_recent_events(10)
	assert events.len == 0
}

// Filter by Event Type Tests

fn test_get_events_by_type() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')
	logger.log_auth_failure('10.0.0.1', 'bad password')
	logger.log_auth_success('192.168.1.2', 'bob', 'SCRAM-SHA-256')
	logger.log_authorization('192.168.1.1', 'alice', 'topic', 'test', 'produce', true)
	logger.log_auth_failure('10.0.0.2', 'unknown user')

	// Filter only authentication failures
	failures := logger.get_events_by_type(.authentication_failure, 10)
	assert failures.len == 2
	assert failures[0].client_ip == '10.0.0.1'
	assert failures[1].client_ip == '10.0.0.2'

	// Filter only authentication successes
	successes := logger.get_events_by_type(.authentication_success, 10)
	assert successes.len == 2

	// Filter authorization
	auth_events := logger.get_events_by_type(.authorization_success, 10)
	assert auth_events.len == 1
}

fn test_get_events_by_type_with_count_limit() {
	mut logger := new_audit_logger(true)

	for _ in 0 .. 5 {
		logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')
	}

	// Request only 2
	events := logger.get_events_by_type(.authentication_success, 2)
	assert events.len == 2
}

fn test_get_events_by_type_no_matches() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')

	events := logger.get_events_by_type(.topic_created, 10)
	assert events.len == 0
}

// Disabled Logger Tests

fn test_disabled_logger_does_not_buffer_events() {
	mut logger := new_audit_logger(false)

	logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')
	logger.log_auth_failure('10.0.0.1', 'bad password')
	logger.log_authorization('192.168.1.1', 'alice', 'topic', 'test', 'produce', true)

	events := logger.get_recent_events(10)
	assert events.len == 0
}

fn test_disabled_logger_log_event_is_noop() {
	mut logger := new_audit_logger(false)

	event := AuditEvent{
		timestamp:  time.now().unix()
		event_type: .topic_created
		client_ip:  '192.168.1.1'
		principal:  'admin'
		operation:  'create_topic'
		authorized: true
	}

	logger.log_event(event)

	events := logger.get_recent_events(10)
	assert events.len == 0
}

// Clear Buffer Tests

fn test_clear_buffer() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'PLAIN')
	logger.log_auth_success('192.168.1.2', 'bob', 'PLAIN')
	logger.log_auth_success('192.168.1.3', 'charlie', 'PLAIN')

	assert logger.get_recent_events(10).len == 3

	logger.clear_buffer()

	assert logger.get_recent_events(10).len == 0
}

fn test_clear_buffer_on_empty_logger() {
	mut logger := new_audit_logger(true)

	// Should not panic on empty buffer
	logger.clear_buffer()

	assert logger.get_recent_events(10).len == 0
}

// Multiple Event Types Mixed

fn test_mixed_event_types() {
	mut logger := new_audit_logger(true)

	logger.log_auth_success('192.168.1.1', 'alice', 'SCRAM-SHA-512')
	logger.log_authorization('192.168.1.1', 'alice', 'topic', 'test', 'produce', true)
	logger.log_auth_failure('10.0.0.1', 'connection refused')

	event := AuditEvent{
		timestamp:     time.now().unix()
		event_type:    .group_joined
		client_ip:     '192.168.1.1'
		principal:     'alice'
		resource_type: 'group'
		resource_name: 'my-group'
		operation:     'join_group'
		authorized:    true
	}
	logger.log_event(event)

	all_events := logger.get_recent_events(10)
	assert all_events.len == 4

	group_events := logger.get_events_by_type(.group_joined, 10)
	assert group_events.len == 1
	assert group_events[0].resource_name == 'my-group'
}
