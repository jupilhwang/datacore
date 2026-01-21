// Service Layer - Streaming Port Interface
// Defines the interface for streaming operations (SSE, WebSocket)
module port

import domain

// ============================================================================
// Streaming Port Interface
// ============================================================================

// StreamingPort defines operations for message streaming
// This interface abstracts the streaming mechanism (SSE, WebSocket, gRPC)
pub interface StreamingPort {
mut:
	// Connection management
	register_connection(conn domain.SSEConnection) !string
	unregister_connection(conn_id string) !
	get_connection(conn_id string) !domain.SSEConnection
	list_connections() []domain.SSEConnection

	// Subscription management
	subscribe(conn_id string, sub domain.Subscription) !
	unsubscribe(conn_id string, topic string, partition ?i32) !
	get_subscriptions(conn_id string) []domain.Subscription

	// Message streaming
	send_event(conn_id string, event domain.SSEEvent) !
	broadcast_event(topic string, partition i32, event domain.SSEEvent) !

	// Statistics
	get_stats() StreamingStats
}

// StreamingStats holds streaming statistics
pub struct StreamingStats {
pub:
	active_connections  int // Number of active connections
	total_subscriptions int // Total active subscriptions
	messages_sent       i64 // Total messages sent
	bytes_sent          i64 // Total bytes sent
	connections_created i64 // Total connections created
	connections_closed  i64 // Total connections closed
}

// ============================================================================
// Message Consumer Port (for fetching messages)
// ============================================================================

// MessageConsumerPort defines operations for consuming messages
// Used by streaming services to fetch messages from storage
pub interface MessageConsumerPort {
mut:
	// Fetch messages from a topic/partition starting at offset
	consume(topic string, partition i32, offset i64, max_messages int) ![]domain.Record

	// Get the earliest available offset
	get_earliest_offset(topic string, partition i32) !i64

	// Get the latest offset (next offset to be written)
	get_latest_offset(topic string, partition i32) !i64

	// Commit offset for a consumer group
	commit_offset(group_id string, topic string, partition i32, offset i64) !

	// Get committed offset for a consumer group
	get_committed_offset(group_id string, topic string, partition i32) !i64
}

// ============================================================================
// SSE Writer Port
// ============================================================================

// SSEWriterPort defines the interface for writing SSE events to HTTP response
pub interface SSEWriterPort {
mut:
	// Write an SSE event to the response
	write_event(event domain.SSEEvent) !

	// Flush the response buffer
	flush() !

	// Check if the connection is still alive
	is_alive() bool

	// Close the connection
	close() !
}

// ============================================================================
// Subscription Filter
// ============================================================================

// SubscriptionFilter defines criteria for filtering messages
pub struct SubscriptionFilter {
pub:
	key_pattern    ?string           // Key pattern (glob or regex)
	header_filters map[string]string // Header key-value filters
	value_contains ?string           // Value contains substring
}

// matches checks if a record matches the filter
pub fn (f &SubscriptionFilter) matches(record domain.Record) bool {
	// Key pattern matching
	if pattern := f.key_pattern {
		if record.key.len == 0 {
			return false
		}
		// Simple glob matching (TODO: implement proper glob/regex)
		if !simple_match(pattern, record.key.bytestr()) {
			return false
		}
	}

	// Header filters (record.headers is map[string][]u8)
	for key, expected_value in f.header_filters {
		if header_value := record.headers[key] {
			if header_value.bytestr() != expected_value {
				return false
			}
		} else {
			return false
		}
	}

	// Value contains
	if contains := f.value_contains {
		if !record.value.bytestr().contains(contains) {
			return false
		}
	}

	return true
}

// simple_match performs simple wildcard matching
fn simple_match(pattern string, value string) bool {
	if pattern == '*' {
		return true
	}
	if pattern.starts_with('*') && pattern.ends_with('*') {
		return value.contains(pattern[1..pattern.len - 1])
	}
	if pattern.starts_with('*') {
		return value.ends_with(pattern[1..])
	}
	if pattern.ends_with('*') {
		return value.starts_with(pattern[..pattern.len - 1])
	}
	return pattern == value
}

// ============================================================================
// Streaming Errors
// ============================================================================

// StreamingError represents streaming-related errors
pub enum StreamingError {
	connection_not_found
	subscription_not_found
	max_connections_reached
	max_subscriptions_reached
	topic_not_found
	partition_not_found
	invalid_offset
	connection_closed
	write_failed
}

// streaming_error_message returns error message for StreamingError
pub fn streaming_error_message(err StreamingError) string {
	return match err {
		.connection_not_found { 'Connection not found' }
		.subscription_not_found { 'Subscription not found' }
		.max_connections_reached { 'Maximum connections reached' }
		.max_subscriptions_reached { 'Maximum subscriptions reached' }
		.topic_not_found { 'Topic not found' }
		.partition_not_found { 'Partition not found' }
		.invalid_offset { 'Invalid offset' }
		.connection_closed { 'Connection closed' }
		.write_failed { 'Write failed' }
	}
}
