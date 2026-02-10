// Service Layer - WebSocket Service
// Manages WebSocket connections and message handling
module streaming

import domain
import service.port
import sync
import time

// WebSocket Service

// WebSocketService manages WebSocket connections and subscriptions
pub struct WebSocketService {
	config domain.WebSocketConfig
mut:
	storage     port.StoragePort                     // Storage for message operations
	connections map[string]&WebSocketConnectionState // Active connections
	topic_subs  map[string][]string                  // topic -> connection IDs
	mutex       sync.RwMutex                         // Thread safety
	stats       WebSocketStats                       // Statistics
	running     bool // Service running flag
}

// WebSocketConnectionState는 WebSocket 연결 상태를 보관합니다
@[heap]
struct WebSocketConnectionState {
pub mut:
	connection    domain.WebSocketConnection
	subscriptions map[string]domain.Subscription // sub_id -> Subscription
	send_chan     chan string                    // Channel for sending messages
	last_ping     i64 // Last ping sent timestamp
}

// WebSocketStats holds WebSocket service statistics
pub struct WebSocketStats {
pub mut:
	active_connections  int // Number of active connections
	total_subscriptions int // Total active subscriptions
	messages_sent       i64 // Total messages sent
	messages_received   i64 // Total messages received
	bytes_sent          i64 // Total bytes sent
	bytes_received      i64 // Total bytes received
	connections_created i64 // Total connections created
	connections_closed  i64 // Total connections closed
}

// new_websocket_service creates a new WebSocket service
pub fn new_websocket_service(storage port.StoragePort, config domain.WebSocketConfig) &WebSocketService {
	return &WebSocketService{
		config:      config
		storage:     storage
		connections: map[string]&WebSocketConnectionState{}
		topic_subs:  map[string][]string{}
		stats:       WebSocketStats{}
		running:     true
	}
}

// Connection Management

// register_connection registers a new WebSocket connection
pub fn (mut s WebSocketService) register_connection(conn domain.WebSocketConnection) !string {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	// Check max connections
	if s.connections.len >= s.config.max_connections {
		return error('Maximum connections reached')
	}

	// Create connection state
	mut state := &WebSocketConnectionState{
		connection:    conn
		subscriptions: map[string]domain.Subscription{}
		send_chan:     chan string{cap: 100}
		last_ping:     time.now().unix_milli()
	}
	state.connection.state = .open

	s.connections[conn.id] = state
	s.stats.active_connections = s.connections.len
	s.stats.connections_created += 1

	return conn.id
}

// unregister_connection removes a WebSocket connection
pub fn (mut s WebSocketService) unregister_connection(conn_id string) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }

	// Remove from topic subscriptions
	for _, sub in state.subscriptions {
		s.remove_topic_subscription(sub.topic, conn_id)
	}

	// Close send channel
	state.send_chan.close()

	s.connections.delete(conn_id)
	s.stats.active_connections = s.connections.len
	s.stats.connections_closed += 1
}

// get_connection returns connection info
pub fn (mut s WebSocketService) get_connection(conn_id string) !domain.WebSocketConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }
	return state.connection
}

// list_connections returns all active connections
pub fn (mut s WebSocketService) list_connections() []domain.WebSocketConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	mut result := []domain.WebSocketConnection{}
	for _, state in s.connections {
		result << state.connection
	}
	return result
}

// get_send_channel returns the send channel for a connection
pub fn (mut s WebSocketService) get_send_channel(conn_id string) !chan string {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }
	return state.send_chan
}

// Message Handling

// handle_message processes an incoming WebSocket message
pub fn (mut s WebSocketService) handle_message(conn_id string, msg domain.WebSocketMessage) !domain.WebSocketResponse {
	// Update last activity
	s.mutex.@lock()
	if mut state := s.connections[conn_id] {
		state.connection.last_activity = time.now().unix_milli()
		state.connection.messages_recv += 1
	}
	s.stats.messages_received += 1
	s.mutex.unlock()

	return match msg.action {
		.subscribe { s.handle_subscribe(conn_id, msg) }
		.unsubscribe { s.handle_unsubscribe(conn_id, msg) }
		.produce { s.handle_produce(conn_id, msg) }
		.commit { s.handle_commit(conn_id, msg) }
		.ping { s.handle_ping(conn_id) }
	}
}

// handle_subscribe handles a subscribe action
fn (mut s WebSocketService) handle_subscribe(conn_id string, msg domain.WebSocketMessage) !domain.WebSocketResponse {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Check max subscriptions
	if state.subscriptions.len >= s.config.max_subscriptions {
		return domain.new_ws_error_response('MAX_SUBSCRIPTIONS', 'Maximum subscriptions reached')
	}

	// Verify topic exists
	_ = s.storage.get_topic(msg.topic) or {
		return domain.new_ws_error_response('TOPIC_NOT_FOUND', 'Topic not found: ${msg.topic}')
	}

	// Parse offset
	offset_type := domain.subscription_offset_from_str(msg.offset or { 'latest' })
	offset := if offset_type == .specific { (msg.offset or { '0' }).i64() } else { i64(0) }

	// Create subscription
	sub := domain.new_subscription(msg.topic, msg.partition, offset_type, offset, msg.group_id,
		conn_id)

	// Add subscription
	state.subscriptions[sub.id] = sub
	state.connection.subscriptions << sub

	// Add to topic index
	s.add_topic_subscription(msg.topic, conn_id)

	s.stats.total_subscriptions = s.count_subscriptions()

	// Resolve starting offset
	start_offset := s.resolve_offset_unlocked(sub)!

	return domain.new_ws_subscribed_response(msg.topic, msg.partition or { 0 }, start_offset)
}

// handle_unsubscribe handles an unsubscribe action
fn (mut s WebSocketService) handle_unsubscribe(conn_id string, msg domain.WebSocketMessage) !domain.WebSocketResponse {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Find and remove matching subscriptions
	mut to_remove := []string{}
	for sub_id, sub in state.subscriptions {
		if sub.topic == msg.topic {
			if part := msg.partition {
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
	state.connection.subscriptions = state.connection.subscriptions.filter(it.topic != msg.topic)

	// Remove from topic index if no more subscriptions
	has_topic_sub := state.subscriptions.values().any(it.topic == msg.topic)
	if !has_topic_sub {
		s.remove_topic_subscription(msg.topic, conn_id)
	}

	s.stats.total_subscriptions = s.count_subscriptions()

	return domain.WebSocketResponse{
		response_type: 'unsubscribed'
		topic:         msg.topic
		partition:     msg.partition or { -1 }
		timestamp:     time.now().unix_milli()
	}
}

// handle_produce handles a produce action
fn (mut s WebSocketService) handle_produce(conn_id string, msg domain.WebSocketMessage) !domain.WebSocketResponse {
	// Verify topic exists
	topic := s.storage.get_topic(msg.topic) or {
		return domain.new_ws_error_response('TOPIC_NOT_FOUND', 'Topic not found: ${msg.topic}')
	}

	// Determine partition
	partition := msg.partition or { 0 }

	if partition < 0 || partition >= topic.partition_count {
		return domain.new_ws_error_response('INVALID_PARTITION', 'Invalid partition: ${partition}')
	}

	// Build record
	key := if k := msg.key { k.bytes() } else { []u8{} }
	value := if v := msg.value { v.bytes() } else { []u8{} }

	mut headers := map[string][]u8{}
	for k, v in msg.headers {
		headers[k] = v.bytes()
	}

	record := domain.Record{
		key:       key
		value:     value
		headers:   headers
		timestamp: time.now()
	}

	// Append to storage
	result := s.storage.append(msg.topic, partition, [record], i16(1)) or {
		return domain.new_ws_error_response('PRODUCE_FAILED', 'Failed to produce: ${err}')
	}

	return domain.new_ws_produced_response(msg.topic, partition, result.base_offset)
}

// handle_commit handles a commit action
fn (mut s WebSocketService) handle_commit(conn_id string, msg domain.WebSocketMessage) !domain.WebSocketResponse {
	group_id := msg.group_id or {
		return domain.new_ws_error_response('MISSING_GROUP_ID', 'group_id is required for commit')
	}

	partition := msg.partition or { 0 }
	offset := (msg.offset or { '0' }).i64()

	// Commit offset
	s.storage.commit_offsets(group_id, [
		domain.PartitionOffset{
			topic:     msg.topic
			partition: partition
			offset:    offset
		},
	]) or { return domain.new_ws_error_response('COMMIT_FAILED', 'Failed to commit: ${err}') }

	return domain.WebSocketResponse{
		response_type: 'committed'
		topic:         msg.topic
		partition:     partition
		offset:        offset
		timestamp:     time.now().unix_milli()
	}
}

// handle_ping handles a ping action
fn (mut s WebSocketService) handle_ping(conn_id string) !domain.WebSocketResponse {
	s.mutex.@lock()
	if mut state := s.connections[conn_id] {
		state.connection.last_pong = time.now().unix_milli()
	}
	s.mutex.unlock()

	return domain.new_ws_pong_response()
}

// Message Broadcasting

// send_message sends a message to a specific connection
pub fn (mut s WebSocketService) send_message(conn_id string, response domain.WebSocketResponse) ! {
	s.mutex.rlock()
	state := s.connections[conn_id] or {
		s.mutex.runlock()
		return error('Connection not found')
	}
	s.mutex.runlock()

	json := response.to_json()

	select {
		state.send_chan <- json {
			s.mutex.@lock()
			s.stats.messages_sent += 1
			s.stats.bytes_sent += json.len
			if mut conn_state := s.connections[conn_id] {
				conn_state.connection.messages_sent += 1
				conn_state.connection.bytes_sent += json.len
			}
			s.mutex.unlock()
		}
		else {
			return error('Send channel full')
		}
	}
}

// broadcast_to_topic sends a message to all connections subscribed to a topic
pub fn (mut s WebSocketService) broadcast_to_topic(topic string, partition i32, response domain.WebSocketResponse) {
	s.mutex.rlock()
	conn_ids := s.topic_subs[topic] or {
		s.mutex.runlock()
		return
	}
	s.mutex.runlock()

	for conn_id in conn_ids {
		s.send_message(conn_id, response) or { continue }
	}
}

// poll_and_send polls for new messages and sends them to subscribers
pub fn (mut s WebSocketService) poll_and_send() {
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
fn (mut s WebSocketService) poll_subscription(conn_id string, sub_id string, sub domain.Subscription) ! {
	partition := sub.partition or { 0 }
	start_offset := sub.current_offset

	// Fetch messages from storage
	result := s.storage.fetch(sub.topic, partition, start_offset, 64 * 1024) or {
		return error('Failed to fetch messages: ${err}')
	}

	// Send each message
	for i, record in result.records {
		offset := start_offset + i64(i)

		response := domain.new_ws_message_response(sub.topic, partition, offset, record.timestamp.unix_milli(),
			if record.key.len > 0 { record.key.bytestr() } else { none }, record.value.bytestr(),
			s.headers_to_map(record.headers))

		s.send_message(conn_id, response) or { continue }

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

// Ping/Pong Management

// send_pings sends ping messages to all connections
pub fn (mut s WebSocketService) send_pings() {
	s.mutex.rlock()
	connections := s.connections.clone()
	s.mutex.runlock()

	now := time.now().unix_milli()

	for conn_id, state in connections {
		// Check if ping is needed
		if now - state.last_ping >= s.config.ping_interval_ms {
			// Send ping frame (handled by WebSocket layer)
			s.mutex.@lock()
			if mut conn_state := s.connections[conn_id] {
				conn_state.last_ping = now
				conn_state.connection.last_ping = now
			}
			s.mutex.unlock()
		}
	}
}

// cleanup_stale_connections removes connections that have timed out
pub fn (mut s WebSocketService) cleanup_stale_connections() []string {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	now := time.now().unix_milli()
	mut to_remove := []string{}

	for conn_id, state in s.connections {
		// Check connection timeout
		if now - state.connection.last_activity > s.config.connection_timeout_ms {
			to_remove << conn_id
			continue
		}

		// Check pong timeout (if ping was sent but no pong received)
		if state.last_ping > 0 && state.connection.last_pong < state.last_ping {
			if now - state.last_ping > s.config.pong_timeout_ms {
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
			state.send_chan.close()
		}
		s.connections.delete(conn_id)
	}

	s.stats.active_connections = s.connections.len
	s.stats.connections_closed += to_remove.len
	s.stats.total_subscriptions = s.count_subscriptions()

	return to_remove
}

// Statistics

// get_stats returns WebSocket service statistics
pub fn (mut s WebSocketService) get_stats() WebSocketStats {
	s.mutex.rlock()
	defer { s.mutex.runlock() }
	return s.stats
}

// Helper Functions

fn (mut s WebSocketService) add_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		if conn_id !in s.topic_subs[topic] {
			s.topic_subs[topic] << conn_id
		}
	} else {
		s.topic_subs[topic] = [conn_id]
	}
}

fn (mut s WebSocketService) remove_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		s.topic_subs[topic] = s.topic_subs[topic].filter(it != conn_id)
		if s.topic_subs[topic].len == 0 {
			s.topic_subs.delete(topic)
		}
	}
}

fn (s &WebSocketService) count_subscriptions() int {
	mut count := 0
	for _, state in s.connections {
		count += state.subscriptions.len
	}
	return count
}

fn (mut s WebSocketService) resolve_offset_unlocked(sub domain.Subscription) !i64 {
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

fn (s &WebSocketService) headers_to_map(headers map[string][]u8) map[string]string {
	mut result := map[string]string{}
	for k, v in headers {
		result[k] = v.bytestr()
	}
	return result
}
