// Service Layer - SSE Service
// Manages SSE connections and message streaming
module streaming

import domain
import service.port
import sync
import time

// ============================================================================
// SSE Service
// ============================================================================

// SSEService manages SSE connections and subscriptions
pub struct SSEService {
	config domain.SSEConfig
mut:
	storage     port.StoragePort               // Storage for fetching messages
	connections map[string]&SSEConnectionState // Active connections
	topic_subs  map[string][]string            // topic -> connection IDs
	mutex       sync.RwMutex                   // Thread safety
	stats       port.StreamingStats            // Statistics
	running     bool // Service running flag
}

// SSEConnectionState holds the state of an SSE connection
@[heap]
struct SSEConnectionState {
pub mut:
	connection    domain.SSEConnection
	subscriptions map[string]domain.Subscription // sub_id -> Subscription
	writer        ?&port.SSEWriterPort           // Writer for sending events
	last_ping     i64 // Last heartbeat sent
}

// new_sse_service creates a new SSE service
pub fn new_sse_service(storage port.StoragePort, config domain.SSEConfig) &SSEService {
	return &SSEService{
		config:      config
		storage:     storage
		connections: map[string]&SSEConnectionState{}
		topic_subs:  map[string][]string{}
		stats:       port.StreamingStats{}
		running:     true
	}
}

// ============================================================================
// Connection Management
// ============================================================================

// register_connection registers a new SSE connection
pub fn (mut s SSEService) register_connection(conn domain.SSEConnection) !string {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	// Check max connections
	if s.connections.len >= s.config.max_connections {
		return error('Maximum connections reached')
	}

	// Create connection state
	mut state := &SSEConnectionState{
		connection:    conn
		subscriptions: map[string]domain.Subscription{}
		writer:        none
		last_ping:     time.now().unix_milli()
	}
	state.connection.state = .connected

	s.connections[conn.id] = state
	s.stats = port.StreamingStats{
		...s.stats
		active_connections:  s.connections.len
		connections_created: s.stats.connections_created + 1
	}

	return conn.id
}

// unregister_connection removes an SSE connection
pub fn (mut s SSEService) unregister_connection(conn_id string) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }

	// Remove from topic subscriptions
	for _, sub in state.subscriptions {
		s.remove_topic_subscription(sub.topic, conn_id)
	}

	s.connections.delete(conn_id)
	s.stats = port.StreamingStats{
		...s.stats
		active_connections: s.connections.len
		connections_closed: s.stats.connections_closed + 1
	}
}

// get_connection returns connection info
pub fn (mut s SSEService) get_connection(conn_id string) !domain.SSEConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }
	return state.connection
}

// list_connections returns all active connections
pub fn (mut s SSEService) list_connections() []domain.SSEConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	mut result := []domain.SSEConnection{}
	for _, state in s.connections {
		result << state.connection
	}
	return result
}

// set_writer sets the SSE writer for a connection
pub fn (mut s SSEService) set_writer(conn_id string, writer &port.SSEWriterPort) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }
	unsafe {
		state.writer = writer
	}
}

// ============================================================================
// Subscription Management
// ============================================================================

// subscribe adds a subscription to a connection
pub fn (mut s SSEService) subscribe(conn_id string, sub domain.Subscription) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Check max subscriptions
	if state.subscriptions.len >= s.config.max_subscriptions {
		return error('Maximum subscriptions reached')
	}

	// Verify topic exists
	_ = s.storage.get_topic(sub.topic) or { return error('Topic not found: ${sub.topic}') }

	// Add subscription
	state.subscriptions[sub.id] = sub
	state.connection.subscriptions << sub

	// Add to topic index
	s.add_topic_subscription(sub.topic, conn_id)

	s.stats = port.StreamingStats{
		...s.stats
		total_subscriptions: s.count_subscriptions()
	}
}

// unsubscribe removes a subscription from a connection
pub fn (mut s SSEService) unsubscribe(conn_id string, topic string, partition ?i32) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Find and remove matching subscriptions
	mut to_remove := []string{}
	for sub_id, sub in state.subscriptions {
		if sub.topic == topic {
			if part := partition {
				if sub_part := sub.partition {
					if sub_part == part {
						to_remove << sub_id
					}
				}
			} else {
				to_remove << sub_id
			}
		}
	}

	for sub_id in to_remove {
		state.subscriptions.delete(sub_id)
	}

	// Update connection subscriptions
	state.connection.subscriptions = state.connection.subscriptions.filter(it.topic != topic)

	// Remove from topic index if no more subscriptions
	has_topic_sub := state.subscriptions.values().any(it.topic == topic)
	if !has_topic_sub {
		s.remove_topic_subscription(topic, conn_id)
	}

	s.stats = port.StreamingStats{
		...s.stats
		total_subscriptions: s.count_subscriptions()
	}
}

// get_subscriptions returns all subscriptions for a connection
pub fn (mut s SSEService) get_subscriptions(conn_id string) []domain.Subscription {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return []domain.Subscription{} }
	return state.subscriptions.values()
}

// ============================================================================
// Message Streaming
// ============================================================================

// send_event sends an SSE event to a specific connection
pub fn (mut s SSEService) send_event(conn_id string, event domain.SSEEvent) ! {
	s.mutex.rlock()
	state := s.connections[conn_id] or {
		s.mutex.runlock()
		return error('Connection not found')
	}
	s.mutex.runlock()

	if mut writer := state.writer {
		writer.write_event(event)!
		writer.flush()!

		// Update stats
		s.mutex.@lock()
		s.stats = port.StreamingStats{
			...s.stats
			messages_sent: s.stats.messages_sent + 1
			bytes_sent:    s.stats.bytes_sent + event.encode().len
		}
		s.mutex.unlock()
	}
}

// broadcast_event sends an SSE event to all connections subscribed to a topic/partition
pub fn (mut s SSEService) broadcast_event(topic string, partition i32, event domain.SSEEvent) ! {
	s.mutex.rlock()
	conn_ids := s.topic_subs[topic] or {
		s.mutex.runlock()
		return
	}
	s.mutex.runlock()

	for conn_id in conn_ids {
		s.send_event(conn_id, event) or { continue }
	}
}

// stream_messages starts streaming messages for a subscription
pub fn (mut s SSEService) stream_messages(conn_id string, sub_id string) ! {
	// Get subscription
	s.mutex.rlock()
	state := s.connections[conn_id] or {
		s.mutex.runlock()
		return error('Connection not found')
	}
	mut sub := state.subscriptions[sub_id] or {
		s.mutex.runlock()
		return error('Subscription not found')
	}
	s.mutex.runlock()

	// Determine starting offset
	start_offset := s.resolve_offset(sub)!

	// Update subscription with resolved offset
	s.mutex.@lock()
	if mut conn_state := s.connections[conn_id] {
		if mut subscription := conn_state.subscriptions[sub_id] {
			subscription.current_offset = start_offset
		}
	}
	s.mutex.unlock()

	// Send subscribed event
	subscribed_event := domain.SSEEvent{
		id:         sub.id
		event_type: .subscribed
		data:       '{"topic":"${sub.topic}","partition":${sub.partition or { -1 }},"offset":${start_offset}}'
	}
	s.send_event(conn_id, subscribed_event)!

	// Start streaming loop (this would typically be in a separate goroutine)
	// For now, we just set up the subscription - actual streaming happens in poll_messages
}

// poll_messages fetches and sends new messages for all subscriptions
pub fn (mut s SSEService) poll_messages() ! {
	s.mutex.rlock()
	connections := s.connections.clone()
	s.mutex.runlock()

	for conn_id, state in connections {
		for sub_id, sub in state.subscriptions {
			s.poll_subscription(conn_id, sub_id, sub) or { continue }
		}
	}
}

// poll_subscription fetches messages for a single subscription
fn (mut s SSEService) poll_subscription(conn_id string, sub_id string, sub domain.Subscription) ! {
	partition := sub.partition or { 0 }
	start_offset := sub.current_offset

	// Fetch messages from storage
	result := s.storage.fetch(sub.topic, partition, start_offset, 1024 * 1024) or {
		return error('Failed to fetch messages: ${err}')
	}

	// Send each message as an SSE event
	for i, record in result.records {
		offset := start_offset + i64(i)
		msg_data := domain.SSEMessageData{
			topic:     sub.topic
			partition: partition
			offset:    offset
			timestamp: record.timestamp.unix_milli()
			key:       if record.key.len > 0 { record.key.bytestr() } else { none }
			value:     record.value.bytestr()
			headers:   s.headers_to_map(record.headers)
		}

		// Encode message data as JSON
		data := encode_message_data(msg_data)
		event := domain.new_sse_message_event(sub.topic, partition, offset, data)

		s.send_event(conn_id, event) or { continue }

		// Update current offset
		s.mutex.@lock()
		if mut conn_state := s.connections[conn_id] {
			if mut subscription := conn_state.subscriptions[sub_id] {
				subscription.current_offset = offset + 1
				subscription.last_activity = time.now().unix_milli()
			}
		}
		s.mutex.unlock()
	}
}

// poll_messages_for_connection fetches and sends new messages for a specific connection
// 통계를 위해 (messages_sent, bytes_sent) 반환
pub fn (mut s SSEService) poll_messages_for_connection(conn_id string) !(int, i64) {
	s.mutex.rlock()
	state := s.connections[conn_id] or {
		s.mutex.runlock()
		return error('Connection not found')
	}
	subs := state.subscriptions.clone()
	s.mutex.runlock()

	mut total_messages := 0
	mut total_bytes := i64(0)

	for sub_id, sub in subs {
		partition := sub.partition or { 0 }
		start_offset := sub.current_offset

		// Fetch messages from storage
		result := s.storage.fetch(sub.topic, partition, start_offset, 64 * 1024) or { continue }

		if result.records.len == 0 {
			continue
		}

		// Send each message as an SSE event
		for i, record in result.records {
			offset := start_offset + i64(i)
			msg_data := domain.SSEMessageData{
				topic:     sub.topic
				partition: partition
				offset:    offset
				timestamp: record.timestamp.unix_milli()
				key:       if record.key.len > 0 { record.key.bytestr() } else { none }
				value:     record.value.bytestr()
				headers:   s.headers_to_map(record.headers)
			}

			data := encode_message_data(msg_data)
			event := domain.new_sse_message_event(sub.topic, partition, offset, data)

			s.send_event(conn_id, event) or { continue }

			total_messages += 1
			total_bytes += event.encode().len
		}

		// Update current offset after sending all messages in batch
		new_offset := start_offset + i64(result.records.len)
		s.mutex.@lock()
		if mut conn_state := s.connections[conn_id] {
			if mut subscription := conn_state.subscriptions[sub_id] {
				subscription.current_offset = new_offset
				subscription.last_activity = time.now().unix_milli()
			}
		}
		s.mutex.unlock()
	}

	return total_messages, total_bytes
}

// ============================================================================
// Heartbeat Management
// ============================================================================

// send_heartbeats sends heartbeat events to all connections
pub fn (mut s SSEService) send_heartbeats() {
	s.mutex.rlock()
	connections := s.connections.clone()
	s.mutex.runlock()

	now := time.now().unix_milli()
	heartbeat := domain.new_sse_heartbeat_event()

	for conn_id, state in connections {
		// Check if heartbeat is needed
		if now - state.last_ping >= s.config.heartbeat_interval_ms {
			s.send_event(conn_id, heartbeat) or { continue }

			// Update last ping
			s.mutex.@lock()
			if mut conn_state := s.connections[conn_id] {
				conn_state.last_ping = now
			}
			s.mutex.unlock()
		}
	}
}

// cleanup_stale_connections removes connections that have timed out
pub fn (mut s SSEService) cleanup_stale_connections() {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	now := time.now().unix_milli()
	mut to_remove := []string{}

	for conn_id, state in s.connections {
		// Check connection timeout
		if now - state.connection.last_activity > s.config.connection_timeout_ms {
			to_remove << conn_id
		}

		// Check if writer is still alive
		if mut writer := state.writer {
			if !writer.is_alive() {
				to_remove << conn_id
			}
		}
	}

	for conn_id in to_remove {
		// Remove from topic subscriptions
		if state := s.connections[conn_id] {
			for _, sub in state.subscriptions {
				s.remove_topic_subscription(sub.topic, conn_id)
			}
		}
		s.connections.delete(conn_id)
	}

	s.stats = port.StreamingStats{
		...s.stats
		active_connections:  s.connections.len
		connections_closed:  s.stats.connections_closed + to_remove.len
		total_subscriptions: s.count_subscriptions()
	}
}

// ============================================================================
// Statistics
// ============================================================================

// get_stats returns streaming statistics
pub fn (mut s SSEService) get_stats() port.StreamingStats {
	s.mutex.rlock()
	defer { s.mutex.runlock() }
	return s.stats
}

// ============================================================================
// Helper Functions
// ============================================================================

fn (mut s SSEService) add_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		if conn_id !in s.topic_subs[topic] {
			s.topic_subs[topic] << conn_id
		}
	} else {
		s.topic_subs[topic] = [conn_id]
	}
}

fn (mut s SSEService) remove_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		s.topic_subs[topic] = s.topic_subs[topic].filter(it != conn_id)
		if s.topic_subs[topic].len == 0 {
			s.topic_subs.delete(topic)
		}
	}
}

fn (s &SSEService) count_subscriptions() int {
	mut count := 0
	for _, state in s.connections {
		count += state.subscriptions.len
	}
	return count
}

fn (mut s SSEService) resolve_offset(sub domain.Subscription) !i64 {
	partition := sub.partition or { 0 }

	return match sub.offset_type {
		.earliest {
			info := s.storage.get_partition_info(sub.topic, partition)!
			info.earliest_offset
		}
		.latest {
			info := s.storage.get_partition_info(sub.topic, partition)!
			info.latest_offset
		}
		.specific {
			sub.offset
		}
	}
}

fn (s &SSEService) headers_to_map(headers map[string][]u8) map[string]string {
	mut result := map[string]string{}
	for k, v in headers {
		result[k] = v.bytestr()
	}
	return result
}

// encode_message_data encodes SSEMessageData to JSON string
fn encode_message_data(data domain.SSEMessageData) string {
	mut json := '{'
	json += '"topic":"${data.topic}"'
	json += ',"partition":${data.partition}'
	json += ',"offset":${data.offset}'
	json += ',"timestamp":${data.timestamp}'

	if key := data.key {
		json += ',"key":"${escape_json_string(key)}"'
	} else {
		json += ',"key":null'
	}

	json += ',"value":"${escape_json_string(data.value)}"'

	// Headers
	json += ',"headers":{'
	mut first := true
	for k, v in data.headers {
		if !first {
			json += ','
		}
		json += '"${escape_json_string(k)}":"${escape_json_string(v)}"'
		first = false
	}
	json += '}'

	json += '}'
	return json
}

// escape_json_string escapes special characters in JSON string
fn escape_json_string(s string) string {
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
