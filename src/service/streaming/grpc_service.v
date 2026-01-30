// Service Layer - gRPC Service
// Manages gRPC connections and message streaming
module streaming

import domain
import service.port
import sync
import time

// gRPC Service

// GrpcService manages gRPC connections and operations
pub struct GrpcService {
	config domain.GrpcConfig
mut:
	storage     port.StoragePort                // Storage for message operations
	connections map[string]&GrpcConnectionState // Active connections
	topic_subs  map[string][]string             // topic -> connection IDs
	mutex       sync.RwMutex                    // Thread safety
	stats       GrpcStats                       // Statistics
	running     bool // Service running flag
}

// GrpcConnectionState는 gRPC 연결 상태를 보관합니다
@[heap]
struct GrpcConnectionState {
pub mut:
	connection    domain.GrpcConnection
	subscriptions map[string]domain.Subscription // sub_id -> Subscription
	send_chan     chan domain.GrpcStreamResponse // Channel for sending responses
	last_ping     i64 // Last ping timestamp
}

// GrpcStats holds gRPC service statistics
pub struct GrpcStats {
pub mut:
	active_connections  int // Number of active connections
	total_subscriptions int // Total active subscriptions
	produce_requests    i64 // Total produce requests
	consume_requests    i64 // Total consume requests
	messages_produced   i64 // Total messages produced
	messages_consumed   i64 // Total messages consumed
	bytes_produced      i64 // Total bytes produced
	bytes_consumed      i64 // Total bytes consumed
	connections_created i64 // Total connections created
	connections_closed  i64 // Total connections closed
	errors              i64 // Total errors
}

// new_grpc_service creates a new gRPC service
pub fn new_grpc_service(storage port.StoragePort, config domain.GrpcConfig) &GrpcService {
	return &GrpcService{
		config:      config
		storage:     storage
		connections: map[string]&GrpcConnectionState{}
		topic_subs:  map[string][]string{}
		stats:       GrpcStats{}
		running:     true
	}
}

// Connection Management

// register_connection registers a new gRPC connection
pub fn (mut s GrpcService) register_connection(conn domain.GrpcConnection) !string {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	// Check max connections
	if s.connections.len >= s.config.max_connections {
		return error('Maximum connections reached')
	}

	// Create connection state
	mut state := &GrpcConnectionState{
		connection:    conn
		subscriptions: map[string]domain.Subscription{}
		send_chan:     chan domain.GrpcStreamResponse{cap: 100}
		last_ping:     time.now().unix_milli()
	}
	state.connection.state = .ready

	s.connections[conn.id] = state
	s.stats.active_connections = s.connections.len
	s.stats.connections_created += 1

	return conn.id
}

// unregister_connection removes a gRPC connection
pub fn (mut s GrpcService) unregister_connection(conn_id string) ! {
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
pub fn (mut s GrpcService) get_connection(conn_id string) !domain.GrpcConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }
	return state.connection
}

// list_connections returns all active connections
pub fn (mut s GrpcService) list_connections() []domain.GrpcConnection {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	mut result := []domain.GrpcConnection{}
	for _, state in s.connections {
		result << state.connection
	}
	return result
}

// get_send_channel returns the send channel for a connection
pub fn (mut s GrpcService) get_send_channel(conn_id string) !chan domain.GrpcStreamResponse {
	s.mutex.rlock()
	defer { s.mutex.runlock() }

	state := s.connections[conn_id] or { return error('Connection not found') }
	return state.send_chan
}

// Produce Operations

// produce handles a produce request
pub fn (mut s GrpcService) produce(conn_id string, req domain.GrpcProduceRequest) domain.GrpcProduceResponse {
	// Verify topic exists
	topic := s.storage.get_topic(req.topic) or {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_produce_error(req.topic, req.partition or { -1 }, domain.grpc_error_unknown_topic,
			'Topic not found: ${req.topic}')
	}

	// Determine partition
	partition := req.partition or { 0 }

	if partition < 0 || partition >= topic.partition_count {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_produce_error(req.topic, partition, domain.grpc_error_invalid_partition,
			'Invalid partition: ${partition}')
	}

	// Check batch size
	if req.records.len > s.config.max_batch_size {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_produce_error(req.topic, partition, domain.grpc_error_record_list_too_large,
			'Batch size exceeds limit: ${req.records.len} > ${s.config.max_batch_size}')
	}

	// Convert records to domain records
	mut domain_records := []domain.Record{cap: req.records.len}
	mut total_bytes := i64(0)
	for rec in req.records {
		domain_records << rec.to_domain_record()
		total_bytes += rec.key.len + rec.value.len
	}

	// Check message size
	if total_bytes > s.config.max_message_size {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_produce_error(req.topic, partition, domain.grpc_error_message_too_large,
			'Message too large: ${total_bytes} > ${s.config.max_message_size}')
	}

	// Append to storage
	result := s.storage.append(req.topic, partition, domain_records) or {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_produce_error(req.topic, partition, domain.grpc_error_unknown,
			'Failed to produce: ${err}')
	}

	// Update stats
	s.mutex.@lock()
	s.stats.produce_requests += 1
	s.stats.messages_produced += req.records.len
	s.stats.bytes_produced += total_bytes
	if mut state := s.connections[conn_id] {
		state.connection.requests_recv += 1
		state.connection.last_activity = time.now().unix_milli()
	}
	s.mutex.unlock()

	return domain.new_grpc_produce_response(req.topic, partition, result.base_offset,
		req.records.len)
}

// Consume Operations (Server Streaming)

// subscribe adds a subscription for server streaming
pub fn (mut s GrpcService) subscribe(conn_id string, req domain.GrpcConsumeRequest) !domain.GrpcConsumeResponse {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Verify topic exists
	topic := s.storage.get_topic(req.topic) or { return error('Topic not found: ${req.topic}') }

	// Validate partition
	if req.partition < 0 || req.partition >= topic.partition_count {
		return error('Invalid partition: ${req.partition}')
	}

	// Create subscription
	offset_type := if req.offset < 0 {
		domain.SubscriptionOffset.latest
	} else {
		domain.SubscriptionOffset.specific
	}
	sub := domain.new_subscription(req.topic, i32(req.partition), offset_type, req.offset,
		req.group_id, conn_id)

	// Add subscription
	state.subscriptions[sub.id] = sub
	state.connection.subscriptions << sub
	state.connection.state = .streaming

	// Add to topic index
	s.add_topic_subscription(req.topic, conn_id)
	s.stats.total_subscriptions = s.count_subscriptions()
	s.stats.consume_requests += 1

	// Resolve starting offset
	start_offset := s.resolve_offset_unlocked(sub) or { req.offset }

	return domain.new_grpc_consume_response(req.topic, req.partition, []domain.GrpcRecord{},
		0, start_offset)
}

// unsubscribe removes a subscription
pub fn (mut s GrpcService) unsubscribe(conn_id string, topic string, partition i32) ! {
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	mut state := s.connections[conn_id] or { return error('Connection not found') }

	// Find and remove matching subscriptions
	mut to_remove := []string{}
	for sub_id, sub in state.subscriptions {
		if sub.topic == topic {
			if sub_part := sub.partition {
				if sub_part == partition {
					to_remove << sub_id
				}
			}
		}
	}

	for sub_id in to_remove {
		state.subscriptions.delete(sub_id)
	}

	// Update connection subscriptions
	state.connection.subscriptions = state.connection.subscriptions.filter(!(it.topic == topic && (it.partition or {
		-1
	}) == partition))

	// Remove from topic index if no more subscriptions
	has_topic_sub := state.subscriptions.values().any(it.topic == topic)
	if !has_topic_sub {
		s.remove_topic_subscription(topic, conn_id)
	}

	s.stats.total_subscriptions = s.count_subscriptions()
}

// fetch_messages fetches messages for a consume request
pub fn (mut s GrpcService) fetch_messages(conn_id string, req domain.GrpcConsumeRequest) domain.GrpcConsumeResponse {
	// Fetch messages from storage
	result := s.storage.fetch(req.topic, req.partition, req.offset, req.max_bytes) or {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.new_grpc_consume_error(req.topic, req.partition, domain.grpc_error_unknown,
			'Failed to fetch: ${err}')
	}

	// Convert to gRPC records
	mut grpc_records := []domain.GrpcRecord{cap: result.records.len}
	mut total_bytes := i64(0)
	for record in result.records {
		grpc_records << domain.grpc_record_from_domain(&record)
		total_bytes += record.key.len + record.value.len
	}

	// Update stats
	s.mutex.@lock()
	s.stats.messages_consumed += grpc_records.len
	s.stats.bytes_consumed += total_bytes
	if mut state := s.connections[conn_id] {
		state.connection.responses_sent += 1
		state.connection.bytes_sent += total_bytes
		state.connection.last_activity = time.now().unix_milli()
	}
	s.mutex.unlock()

	next_offset := req.offset + i64(grpc_records.len)
	return domain.new_grpc_consume_response(req.topic, req.partition, grpc_records, result.high_watermark,
		next_offset)
}

// poll_and_send polls for new messages and sends them to subscribers
pub fn (mut s GrpcService) poll_and_send() {
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
fn (mut s GrpcService) poll_subscription(conn_id string, sub_id string, sub domain.Subscription) ! {
	partition := sub.partition or { 0 }
	start_offset := sub.current_offset

	// Fetch messages from storage
	result := s.storage.fetch(sub.topic, partition, start_offset, 64 * 1024) or {
		return error('Failed to fetch messages: ${err}')
	}

	if result.records.len == 0 {
		return
	}

	// Convert to gRPC records
	mut grpc_records := []domain.GrpcRecord{cap: result.records.len}
	mut total_bytes := i64(0)
	for record in result.records {
		grpc_records << domain.grpc_record_from_domain(&record)
		total_bytes += record.key.len + record.value.len
	}

	// Create response
	next_offset := start_offset + i64(grpc_records.len)
	response := domain.GrpcStreamResponse{
		response_type: .message
		message:       domain.GrpcMessageResponse{
			topic:     sub.topic
			partition: partition
			offset:    start_offset
			timestamp: time.now().unix_milli()
			key:       if grpc_records.len > 0 { grpc_records[0].key } else { []u8{} }
			value:     if grpc_records.len > 0 { grpc_records[0].value } else { []u8{} }
			headers:   if grpc_records.len > 0 {
				grpc_records[0].headers
			} else {
				map[string][]u8{}
			}
		}
	}

	// Send to connection channel
	s.mutex.rlock()
	if state := s.connections[conn_id] {
		select {
			state.send_chan <- response {}
			else {}
		}
	}
	s.mutex.runlock()

	// Update current offset
	s.mutex.@lock()
	if mut conn_state := s.connections[conn_id] {
		if mut subscription := conn_state.subscriptions[sub_id] {
			subscription.current_offset = next_offset
			subscription.last_activity = time.now().unix_milli()
		}
		conn_state.connection.responses_sent += 1
		conn_state.connection.bytes_sent += total_bytes
	}
	s.stats.messages_consumed += grpc_records.len
	s.stats.bytes_consumed += total_bytes
	s.mutex.unlock()
}

// Bidirectional Streaming

// handle_stream_request handles a bidirectional stream request
pub fn (mut s GrpcService) handle_stream_request(conn_id string, req domain.GrpcStreamRequest) domain.GrpcStreamResponse {
	match req.request_type {
		.produce {
			if produce_req := req.produce {
				result := s.produce(conn_id, produce_req)
				return domain.GrpcStreamResponse{
					response_type: .produce_ack
					produce:       result
				}
			}
			return s.create_error_response(domain.grpc_error_invalid_message, 'Missing produce request')
		}
		.subscribe {
			if consume_req := req.consume {
				result := s.subscribe(conn_id, consume_req) or {
					return s.create_error_response(domain.grpc_error_unknown, err.str())
				}
				// Return initial response, messages will be streamed via poll_and_send
				return domain.GrpcStreamResponse{
					response_type: .message
					message:       domain.GrpcMessageResponse{
						topic:     result.topic
						partition: result.partition
						offset:    result.next_offset
					}
				}
			}
			return s.create_error_response(domain.grpc_error_invalid_message, 'Missing consume request')
		}
		.commit {
			if commit_req := req.commit {
				result := s.handle_commit(conn_id, commit_req)
				return domain.GrpcStreamResponse{
					response_type: .commit_ack
					commit:        result
				}
			}
			return s.create_error_response(domain.grpc_error_invalid_message, 'Missing commit request')
		}
		.ack {
			if ack_req := req.ack {
				s.handle_ack(conn_id, ack_req)
				return domain.GrpcStreamResponse{
					response_type: .pong
					pong:          domain.GrpcPongResponse{
						timestamp: time.now().unix_milli()
					}
				}
			}
			return s.create_error_response(domain.grpc_error_invalid_message, 'Missing ack request')
		}
		.ping {
			return domain.GrpcStreamResponse{
				response_type: .pong
				pong:          domain.GrpcPongResponse{
					timestamp: time.now().unix_milli()
				}
			}
		}
	}
}

// handle_commit handles an offset commit request
fn (mut s GrpcService) handle_commit(conn_id string, req domain.GrpcCommitRequest) domain.GrpcCommitResponse {
	// Convert to domain offset format
	mut offsets := []domain.PartitionOffset{cap: req.offsets.len}
	for po in req.offsets {
		offsets << domain.PartitionOffset{
			topic:     po.topic
			partition: po.partition
			offset:    po.offset
			metadata:  po.metadata
		}
	}

	// Commit offsets
	s.storage.commit_offsets(req.group_id, offsets) or {
		s.mutex.@lock()
		s.stats.errors += 1
		s.mutex.unlock()
		return domain.GrpcCommitResponse{
			success: false
			message: 'Commit failed: ${err}'
		}
	}

	return domain.GrpcCommitResponse{
		success: true
		message: 'Committed ${offsets.len} offsets'
	}
}

// handle_ack handles a message acknowledgment
fn (mut s GrpcService) handle_ack(conn_id string, req domain.GrpcAckRequest) {
	// Update subscription offset
	s.mutex.@lock()
	defer { s.mutex.unlock() }

	if mut state := s.connections[conn_id] {
		for _, mut sub in state.subscriptions {
			if sub.topic == req.topic {
				if sub_part := sub.partition {
					if sub_part == req.partition {
						sub.current_offset = req.offset + 1
						sub.last_activity = time.now().unix_milli()
					}
				}
			}
		}
	}
}

// create_error_response creates an error stream response
fn (s &GrpcService) create_error_response(code i32, msg string) domain.GrpcStreamResponse {
	return domain.GrpcStreamResponse{
		response_type: .error
		error:         domain.GrpcErrorResponse{
			code:    code
			message: msg
		}
	}
}

// Keepalive Management

// send_keepalives sends keepalive pings to all connections
pub fn (mut s GrpcService) send_keepalives() {
	s.mutex.rlock()
	connections := s.connections.clone()
	s.mutex.runlock()

	now := time.now().unix_milli()

	for conn_id, state in connections {
		// Check if keepalive is needed
		if now - state.last_ping >= s.config.keepalive_time_ms {
			pong := domain.GrpcStreamResponse{
				response_type: .pong
				pong:          domain.GrpcPongResponse{
					timestamp: now
				}
			}

			// Send keepalive
			select {
				state.send_chan <- pong {}
				else {}
			}

			// Update last ping
			s.mutex.@lock()
			if mut conn_state := s.connections[conn_id] {
				conn_state.last_ping = now
				conn_state.connection.last_activity = now
			}
			s.mutex.unlock()
		}
	}
}

// cleanup_stale_connections removes connections that have timed out
pub fn (mut s GrpcService) cleanup_stale_connections() []string {
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

		// Check keepalive timeout
		if state.last_ping > 0 && now - state.last_ping > s.config.keepalive_timeout_ms {
			// Could add pong tracking here
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

// get_stats returns gRPC service statistics
pub fn (mut s GrpcService) get_stats() GrpcStats {
	s.mutex.rlock()
	defer { s.mutex.runlock() }
	return s.stats
}

// Helper Functions

fn (mut s GrpcService) add_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		if conn_id !in s.topic_subs[topic] {
			s.topic_subs[topic] << conn_id
		}
	} else {
		s.topic_subs[topic] = [conn_id]
	}
}

fn (mut s GrpcService) remove_topic_subscription(topic string, conn_id string) {
	if topic in s.topic_subs {
		s.topic_subs[topic] = s.topic_subs[topic].filter(it != conn_id)
		if s.topic_subs[topic].len == 0 {
			s.topic_subs.delete(topic)
		}
	}
}

fn (s &GrpcService) count_subscriptions() int {
	mut count := 0
	for _, state in s.connections {
		count += state.subscriptions.len
	}
	return count
}

fn (mut s GrpcService) resolve_offset_unlocked(sub domain.Subscription) !i64 {
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
