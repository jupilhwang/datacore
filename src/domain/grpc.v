// Domain Layer - gRPC Models
// gRPC streaming protocol domain models
module domain

import time

// ============================================================================
// gRPC Request/Response Types
// Represents Protocol Buffer message equivalents in V
// ============================================================================

// ProduceRequest represents a gRPC produce request
pub struct GrpcProduceRequest {
pub:
	topic     string       // Target topic
	partition ?i32         // Target partition (optional, -1 for auto)
	records   []GrpcRecord // Records to produce
}

// GrpcRecord represents a single record in gRPC format
pub struct GrpcRecord {
pub:
	key       []u8            // Record key (optional)
	value     []u8            // Record value
	headers   map[string][]u8 // Record headers
	timestamp i64             // Timestamp (0 for server time)
}

// ProduceResponse represents a gRPC produce response
pub struct GrpcProduceResponse {
pub:
	topic        string // Topic name
	partition    i32    // Partition number
	base_offset  i64    // Base offset of produced records
	record_count int    // Number of records produced
	timestamp    i64    // Log append time
	error_code   i32    // Error code (0 = success)
	error_msg    string // Error message
}

// new_grpc_produce_response creates a successful produce response
pub fn new_grpc_produce_response(topic string, partition i32, base_offset i64, count int) GrpcProduceResponse {
	return GrpcProduceResponse{
		topic:        topic
		partition:    partition
		base_offset:  base_offset
		record_count: count
		timestamp:    time.now().unix_milli()
		error_code:   0
		error_msg:    ''
	}
}

// new_grpc_produce_error creates an error produce response
pub fn new_grpc_produce_error(topic string, partition i32, code i32, msg string) GrpcProduceResponse {
	return GrpcProduceResponse{
		topic:       topic
		partition:   partition
		base_offset: -1
		error_code:  code
		error_msg:   msg
		timestamp:   time.now().unix_milli()
	}
}

// ============================================================================
// Consume Streaming
// ============================================================================

// ConsumeRequest represents a gRPC consume request
pub struct GrpcConsumeRequest {
pub:
	topic       string  // Topic to consume
	partition   i32     // Partition number
	offset      i64     // Starting offset
	max_records int     // Max records per batch
	max_bytes   int     // Max bytes per batch
	group_id    ?string // Consumer group ID (optional)
}

// ConsumeResponse represents a gRPC consume response (stream element)
pub struct GrpcConsumeResponse {
pub:
	topic          string       // Topic name
	partition      i32          // Partition number
	records        []GrpcRecord // Fetched records
	high_watermark i64          // High watermark offset
	next_offset    i64          // Next offset to fetch
	error_code     i32          // Error code (0 = success)
	error_msg      string       // Error message
}

// new_grpc_consume_response creates a successful consume response
pub fn new_grpc_consume_response(topic string, partition i32, records []GrpcRecord, hwm i64, next i64) GrpcConsumeResponse {
	return GrpcConsumeResponse{
		topic:          topic
		partition:      partition
		records:        records
		high_watermark: hwm
		next_offset:    next
		error_code:     0
		error_msg:      ''
	}
}

// new_grpc_consume_error creates an error consume response
pub fn new_grpc_consume_error(topic string, partition i32, code i32, msg string) GrpcConsumeResponse {
	return GrpcConsumeResponse{
		topic:      topic
		partition:  partition
		records:    []GrpcRecord{}
		error_code: code
		error_msg:  msg
	}
}

// ============================================================================
// Bidirectional Streaming
// ============================================================================

// StreamRequest represents a bidirectional stream request
pub struct GrpcStreamRequest {
pub:
	request_type GrpcStreamRequestType // Request type
	produce      ?GrpcProduceRequest   // Produce request (if type is produce)
	consume      ?GrpcConsumeRequest   // Consume request (if type is subscribe)
	commit       ?GrpcCommitRequest    // Commit request (if type is commit)
	ack          ?GrpcAckRequest       // Ack request (if type is ack)
}

// StreamRequestType represents the type of stream request
pub enum GrpcStreamRequestType {
	produce   // Send records
	subscribe // Subscribe to topic/partition
	commit    // Commit offsets
	ack       // Acknowledge message receipt
	ping      // Keep-alive ping
}

// grpc_stream_request_type_from_int converts int to GrpcStreamRequestType
pub fn grpc_stream_request_type_from_int(i int) GrpcStreamRequestType {
	return match i {
		0 { .produce }
		1 { .subscribe }
		2 { .commit }
		3 { .ack }
		4 { .ping }
		else { .ping }
	}
}

// GrpcCommitRequest represents an offset commit request
pub struct GrpcCommitRequest {
pub:
	group_id string                // Consumer group ID
	offsets  []GrpcPartitionOffset // Offsets to commit
}

// GrpcPartitionOffset represents a partition offset
pub struct GrpcPartitionOffset {
pub:
	topic     string // Topic name
	partition i32    // Partition number
	offset    i64    // Offset to commit
	metadata  string // Commit metadata
}

// GrpcAckRequest represents a message acknowledgment
pub struct GrpcAckRequest {
pub:
	topic     string // Topic name
	partition i32    // Partition number
	offset    i64    // Acknowledged offset
}

// StreamResponse represents a bidirectional stream response
pub struct GrpcStreamResponse {
pub:
	response_type GrpcStreamResponseType // Response type
	produce       ?GrpcProduceResponse   // Produce result
	message       ?GrpcMessageResponse   // Consumed message
	commit        ?GrpcCommitResponse    // Commit result
	error         ?GrpcErrorResponse     // Error response
	pong          ?GrpcPongResponse      // Pong response
}

// StreamResponseType represents the type of stream response
pub enum GrpcStreamResponseType {
	produce_ack // Produce acknowledgment
	message     // Consumed message
	commit_ack  // Commit acknowledgment
	error       // Error response
	pong        // Pong response
}

// GrpcMessageResponse represents a consumed message
pub struct GrpcMessageResponse {
pub:
	topic     string          // Topic name
	partition i32             // Partition number
	offset    i64             // Message offset
	timestamp i64             // Message timestamp
	key       []u8            // Message key
	value     []u8            // Message value
	headers   map[string][]u8 // Message headers
}

// GrpcCommitResponse represents a commit result
pub struct GrpcCommitResponse {
pub:
	success bool   // Whether commit was successful
	message string // Result message
}

// GrpcErrorResponse represents an error
pub struct GrpcErrorResponse {
pub:
	code    i32    // Error code
	message string // Error message
}

// GrpcPongResponse represents a pong
pub struct GrpcPongResponse {
pub:
	timestamp i64 // Server timestamp
}

// ============================================================================
// gRPC Connection State
// ============================================================================

// GrpcConnectionState represents the state of a gRPC connection
pub enum GrpcConnectionState {
	connecting // Initial connection
	ready      // Ready to process requests
	streaming  // Active streaming
	closing    // Graceful shutdown
	closed     // Connection closed
}

// GrpcConnection represents an active gRPC connection
pub struct GrpcConnection {
pub:
	id         string // Connection ID
	client_ip  string // Client IP address
	created_at i64    // Connection creation time
pub mut:
	state          GrpcConnectionState // Current state
	subscriptions  []Subscription      // Active subscriptions (for consume streams)
	requests_recv  i64                 // Total requests received
	responses_sent i64                 // Total responses sent
	bytes_recv     i64                 // Total bytes received
	bytes_sent     i64                 // Total bytes sent
	last_activity  i64                 // Last activity timestamp
	stream_type    GrpcStreamType      // Type of stream
}

// GrpcStreamType represents the type of gRPC stream
pub enum GrpcStreamType {
	unary            // Unary request/response
	server_streaming // Server streaming (e.g., consume)
	client_streaming // Client streaming (e.g., batch produce)
	bidirectional    // Bidirectional streaming
}

// new_grpc_connection creates a new gRPC connection
pub fn new_grpc_connection(client_ip string, stream_type GrpcStreamType) GrpcConnection {
	now := time.now().unix_milli()
	return GrpcConnection{
		id:             'grpc-${time.now().unix_nano()}'
		client_ip:      client_ip
		created_at:     now
		state:          .connecting
		subscriptions:  []Subscription{}
		requests_recv:  0
		responses_sent: 0
		bytes_recv:     0
		bytes_sent:     0
		last_activity:  now
		stream_type:    stream_type
	}
}

// ============================================================================
// gRPC Configuration
// ============================================================================

// GrpcConfig holds gRPC server configuration
pub struct GrpcConfig {
pub:
	port                   int  = 9093    // gRPC server port
	max_connections        int  = 10000   // Maximum concurrent connections
	max_message_size       int  = 4194304 // Max message size (4MB)
	max_concurrent_streams int  = 100     // Max concurrent streams per connection
	keepalive_time_ms      int  = 30000   // Keepalive time (30s)
	keepalive_timeout_ms   int  = 10000   // Keepalive timeout (10s)
	connection_timeout_ms  int  = 300000  // Connection timeout (5min)
	max_batch_size         int  = 1000    // Max records per batch
	enable_reflection      bool = true    // Enable gRPC reflection for debugging
}

// default_grpc_config returns default gRPC configuration
pub fn default_grpc_config() GrpcConfig {
	return GrpcConfig{}
}

// ============================================================================
// gRPC Error Codes (Kafka-compatible)
// ============================================================================

pub const grpc_error_none = 0
pub const grpc_error_unknown = -1
pub const grpc_error_offset_out_of_range = 1
pub const grpc_error_invalid_message = 2
pub const grpc_error_unknown_topic = 3
pub const grpc_error_invalid_partition = 4
pub const grpc_error_leader_not_available = 5
pub const grpc_error_not_leader_for_partition = 6
pub const grpc_error_request_timed_out = 7
pub const grpc_error_message_too_large = 10
pub const grpc_error_group_coordinator_not_available = 15
pub const grpc_error_not_coordinator = 16
pub const grpc_error_invalid_topic = 17
pub const grpc_error_record_list_too_large = 18
pub const grpc_error_group_auth_failed = 30
pub const grpc_error_invalid_session_timeout = 26

// grpc_error_message returns a human-readable error message
pub fn grpc_error_message(code i32) string {
	return match code {
		grpc_error_none { 'No error' }
		grpc_error_offset_out_of_range { 'Offset out of range' }
		grpc_error_invalid_message { 'Invalid message' }
		grpc_error_unknown_topic { 'Unknown topic' }
		grpc_error_invalid_partition { 'Invalid partition' }
		grpc_error_leader_not_available { 'Leader not available' }
		grpc_error_not_leader_for_partition { 'Not leader for partition' }
		grpc_error_request_timed_out { 'Request timed out' }
		grpc_error_message_too_large { 'Message too large' }
		grpc_error_group_coordinator_not_available { 'Group coordinator not available' }
		grpc_error_not_coordinator { 'Not coordinator' }
		grpc_error_invalid_topic { 'Invalid topic' }
		grpc_error_record_list_too_large { 'Record list too large' }
		grpc_error_group_auth_failed { 'Group authorization failed' }
		grpc_error_invalid_session_timeout { 'Invalid session timeout' }
		else { 'Unknown error' }
	}
}

// ============================================================================
// Binary Encoding Helpers (for gRPC wire format)
// ============================================================================

// encode_grpc_record encodes a GrpcRecord to bytes
pub fn (r &GrpcRecord) encode() []u8 {
	mut buf := []u8{cap: 64 + r.key.len + r.value.len}

	// Key length (4 bytes) + key
	key_len := r.key.len
	buf << u8(key_len >> 24)
	buf << u8(key_len >> 16)
	buf << u8(key_len >> 8)
	buf << u8(key_len)
	buf << r.key

	// Value length (4 bytes) + value
	val_len := r.value.len
	buf << u8(val_len >> 24)
	buf << u8(val_len >> 16)
	buf << u8(val_len >> 8)
	buf << u8(val_len)
	buf << r.value

	// Timestamp (8 bytes)
	ts := r.timestamp
	buf << u8(ts >> 56)
	buf << u8(ts >> 48)
	buf << u8(ts >> 40)
	buf << u8(ts >> 32)
	buf << u8(ts >> 24)
	buf << u8(ts >> 16)
	buf << u8(ts >> 8)
	buf << u8(ts)

	// Headers count (4 bytes)
	header_count := r.headers.len
	buf << u8(header_count >> 24)
	buf << u8(header_count >> 16)
	buf << u8(header_count >> 8)
	buf << u8(header_count)

	// Headers
	for k, v in r.headers {
		// Key length (2 bytes) + key
		k_len := k.len
		buf << u8(k_len >> 8)
		buf << u8(k_len)
		buf << k.bytes()

		// Value length (2 bytes) + value
		v_len := v.len
		buf << u8(v_len >> 8)
		buf << u8(v_len)
		buf << v
	}

	return buf
}

// decode_grpc_record decodes bytes to GrpcRecord
pub fn decode_grpc_record(data []u8) !GrpcRecord {
	if data.len < 20 {
		return error('Data too short for GrpcRecord')
	}

	mut pos := 0

	// Key length + key
	key_len := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4
	if pos + key_len > data.len {
		return error('Invalid key length')
	}
	key := data[pos..pos + key_len].clone()
	pos += key_len

	// Value length + value
	if pos + 4 > data.len {
		return error('Data too short for value length')
	}
	val_len := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4
	if pos + val_len > data.len {
		return error('Invalid value length')
	}
	value := data[pos..pos + val_len].clone()
	pos += val_len

	// Timestamp
	if pos + 8 > data.len {
		return error('Data too short for timestamp')
	}
	timestamp := i64(data[pos]) << 56 | i64(data[pos + 1]) << 48 | i64(data[pos + 2]) << 40 | i64(data[
		pos + 3]) << 32 | i64(data[pos + 4]) << 24 | i64(data[pos + 5]) << 16 | i64(data[pos + 6]) << 8 | i64(data[
		pos + 7])
	pos += 8

	// Headers count
	if pos + 4 > data.len {
		return error('Data too short for header count')
	}
	header_count := int(data[pos]) << 24 | int(data[pos + 1]) << 16 | int(data[pos + 2]) << 8 | int(data[
		pos + 3])
	pos += 4

	// Headers
	mut headers := map[string][]u8{}
	for _ in 0 .. header_count {
		// Header key
		if pos + 2 > data.len {
			return error('Data too short for header key length')
		}
		hk_len := int(data[pos]) << 8 | int(data[pos + 1])
		pos += 2
		if pos + hk_len > data.len {
			return error('Invalid header key length')
		}
		hk := data[pos..pos + hk_len].bytestr()
		pos += hk_len

		// Header value
		if pos + 2 > data.len {
			return error('Data too short for header value length')
		}
		hv_len := int(data[pos]) << 8 | int(data[pos + 1])
		pos += 2
		if pos + hv_len > data.len {
			return error('Invalid header value length')
		}
		hv := data[pos..pos + hv_len].clone()
		pos += hv_len

		headers[hk] = hv
	}

	return GrpcRecord{
		key:       key
		value:     value
		timestamp: timestamp
		headers:   headers
	}
}

// convert_to_domain_record converts GrpcRecord to domain.Record
pub fn (r &GrpcRecord) to_domain_record() Record {
	return Record{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: if r.timestamp > 0 { time.unix(r.timestamp / 1000) } else { time.now() }
	}
}

// convert_from_domain_record creates GrpcRecord from domain.Record
pub fn grpc_record_from_domain(r &Record) GrpcRecord {
	return GrpcRecord{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: r.timestamp.unix_milli()
	}
}
