/// Infrastructure layer - Audit logging for authentication and authorization events
/// Thread-safe event buffer with structured logging support.
module auth

import sync
import time

/// AuditEventType represents the type of audit event.
pub enum AuditEventType {
	authentication_success
	authentication_failure
	authorization_success
	authorization_failure
	topic_created
	topic_deleted
	group_joined
	group_left
}

/// str converts an AuditEventType to its string representation.
pub fn (t AuditEventType) str() string {
	return match t {
		.authentication_success { 'authentication_success' }
		.authentication_failure { 'authentication_failure' }
		.authorization_success { 'authorization_success' }
		.authorization_failure { 'authorization_failure' }
		.topic_created { 'topic_created' }
		.topic_deleted { 'topic_deleted' }
		.group_joined { 'group_joined' }
		.group_left { 'group_left' }
	}
}

/// AuditEvent represents a single audit log entry.
pub struct AuditEvent {
pub:
	timestamp     i64
	event_type    AuditEventType
	client_ip     string
	principal     string
	resource_type string
	resource_name string
	operation     string
	authorized    bool
	error_message string
}

/// AuditLogger provides thread-safe audit event logging with an in-memory buffer.
pub struct AuditLogger {
pub:
	enabled         bool
	max_buffer_size int
mut:
	events []AuditEvent
	lock   sync.Mutex
}

/// new_audit_logger creates a new audit logger with the default buffer size (10000).
pub fn new_audit_logger(enabled bool) &AuditLogger {
	return &AuditLogger{
		enabled:         enabled
		max_buffer_size: 10000
		events:          []AuditEvent{}
	}
}

/// new_audit_logger_with_buffer_size creates a new audit logger with a custom buffer size.
fn new_audit_logger_with_buffer_size(enabled bool, max_buffer_size int) &AuditLogger {
	return &AuditLogger{
		enabled:         enabled
		max_buffer_size: max_buffer_size
		events:          []AuditEvent{}
	}
}

/// log_event logs a generic audit event to the buffer.
fn (mut l AuditLogger) log_event(event AuditEvent) {
	if !l.enabled {
		return
	}

	l.lock.@lock()
	defer { l.lock.unlock() }

	l.events << event
	l.trim_buffer()
}

/// log_auth_success logs a successful authentication event.
pub fn (mut l AuditLogger) log_auth_success(client_ip string, principal string, mechanism string) {
	l.log_event(AuditEvent{
		timestamp:  time.now().unix()
		event_type: .authentication_success
		client_ip:  client_ip
		principal:  principal
		operation:  mechanism
		authorized: true
	})
}

/// log_auth_failure logs a failed authentication event.
pub fn (mut l AuditLogger) log_auth_failure(client_ip string, reason string) {
	l.log_event(AuditEvent{
		timestamp:     time.now().unix()
		event_type:    .authentication_failure
		client_ip:     client_ip
		principal:     'anonymous'
		authorized:    false
		error_message: reason
	})
}

/// log_authorization logs an authorization check event.
fn (mut l AuditLogger) log_authorization(client_ip string, principal string, resource_type string, resource_name string, operation string, authorized bool) {
	event_type := if authorized {
		AuditEventType.authorization_success
	} else {
		AuditEventType.authorization_failure
	}

	l.log_event(AuditEvent{
		timestamp:     time.now().unix()
		event_type:    event_type
		client_ip:     client_ip
		principal:     principal
		resource_type: resource_type
		resource_name: resource_name
		operation:     operation
		authorized:    authorized
	})
}

/// get_recent_events returns the most recent events from the buffer.
/// Returns at most `count` events, ordered from oldest to newest.
pub fn (mut l AuditLogger) get_recent_events(count int) []AuditEvent {
	l.lock.@lock()
	defer { l.lock.unlock() }

	if l.events.len == 0 {
		return []AuditEvent{}
	}

	if count >= l.events.len {
		return l.events.clone()
	}

	start := l.events.len - count
	return l.events[start..].clone()
}

/// get_events_by_type returns events matching the specified type.
/// Returns at most `count` matching events, ordered from oldest to newest.
fn (mut l AuditLogger) get_events_by_type(event_type AuditEventType, count int) []AuditEvent {
	l.lock.@lock()
	defer { l.lock.unlock() }

	mut matched := []AuditEvent{}
	for event in l.events {
		if event.event_type == event_type {
			matched << event
			if matched.len >= count {
				break
			}
		}
	}
	return matched
}

/// clear_buffer removes all events from the buffer.
fn (mut l AuditLogger) clear_buffer() {
	l.lock.@lock()
	defer { l.lock.unlock() }

	l.events.clear()
}

/// trim_buffer removes oldest events when buffer exceeds max size.
/// Must be called while holding the lock.
fn (mut l AuditLogger) trim_buffer() {
	if l.events.len > l.max_buffer_size {
		excess := l.events.len - l.max_buffer_size
		l.events = l.events[excess..].clone()
	}
}
