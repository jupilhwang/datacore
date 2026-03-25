// Abstracts real-time message streaming functionality.
module port

import domain

// Streaming port interfaces

/// StreamingPort defines message streaming operations.
/// Abstracts various streaming mechanisms such as SSE, WebSocket, and gRPC.
pub interface StreamingPort {
mut:
	/// Registers a new connection.
	/// Returns the connection ID.
	register_connection(conn domain.SSEConnection) !string

	/// Unregisters a connection.
	unregister_connection(conn_id string) !

	/// Retrieves connection information.
	get_connection(conn_id string) !domain.SSEConnection

	/// Returns a list of all active connections.
	list_connections() []domain.SSEConnection
	/// Subscribes to a topic/partition.
	subscribe(conn_id string, sub domain.Subscription) !

	/// Unsubscribes from a topic.
	unsubscribe(conn_id string, topic string, partition ?i32) !

	/// Returns all subscriptions for a connection.
	get_subscriptions(conn_id string) []domain.Subscription
	/// Sends an event to a specific connection.
	send_event(conn_id string, event domain.SSEEvent) !

	/// Broadcasts an event to all connections subscribed to a topic/partition.
	broadcast_event(topic string, partition i32, event domain.SSEEvent) !
	/// Returns streaming statistics.
	get_stats() StreamingStats
}

/// StreamingStats holds streaming statistics.
pub struct StreamingStats {
pub:
	active_connections  int
	total_subscriptions int
	messages_sent       i64
	bytes_sent          i64
	connections_created i64
	connections_closed  i64
}

// SSE writer port

/// SSEWriterPort is an interface for writing SSE events to an HTTP response.
pub interface SSEWriterPort {
mut:
	/// Writes an SSE event to the response.
	write_event(event domain.SSEEvent) !

	/// Flushes the response buffer.
	flush() !

	/// Checks whether the connection is still alive.
	is_alive() bool

	/// Closes the connection.
	close() !
}

// Subscription filter

/// SubscriptionFilter defines message filtering conditions.
pub struct SubscriptionFilter {
pub:
	key_pattern    ?string
	header_filters map[string]string
	value_contains ?string
}

/// matches checks whether a record satisfies the filter conditions.
pub fn (f &SubscriptionFilter) matches(record domain.Record) bool {
	// Key pattern matching
	if pattern := f.key_pattern {
		if record.key.len == 0 {
			return false
		}
		if !glob_match(pattern, record.key.bytestr()) {
			return false
		}
	}

	// Header filter (record.headers is of type map[string][]u8)
	for key, expected_value in f.header_filters {
		if header_value := record.headers[key] {
			if header_value.bytestr() != expected_value {
				return false
			}
		} else {
			return false
		}
	}

	// Value contains check
	if contains := f.value_contains {
		if !record.value.bytestr().contains(contains) {
			return false
		}
	}

	return true
}

// Streaming errors

/// StreamingError represents streaming-related errors.
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

/// streaming_error_message returns the error message for a StreamingError.
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
