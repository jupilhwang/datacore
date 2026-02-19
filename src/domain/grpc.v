module domain

import time

/// GrpcProduceRequest represents a gRPC produce request.
pub struct GrpcProduceRequest {
pub:
	topic     string
	partition ?i32
	records   []GrpcRecord
}

/// GrpcRecord represents a single record in gRPC format.
pub struct GrpcRecord {
pub:
	key       []u8
	value     []u8
	headers   map[string][]u8
	timestamp i64
}

/// GrpcProduceResponse represents a gRPC produce response.
pub struct GrpcProduceResponse {
pub:
	topic        string
	partition    i32
	base_offset  i64
	record_count int
	timestamp    i64
	error_code   i32
	error_msg    string
}

/// new_grpc_produce_response creates a successful produce response.
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

/// new_grpc_produce_error creates an error produce response.
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

/// GrpcConsumeRequest represents a gRPC consume request.
pub struct GrpcConsumeRequest {
pub:
	topic       string
	partition   i32
	offset      i64
	max_records int
	max_bytes   int
	group_id    ?string
}

/// GrpcConsumeResponse represents a gRPC consume response (stream element).
pub struct GrpcConsumeResponse {
pub:
	topic          string
	partition      i32
	records        []GrpcRecord
	high_watermark i64
	next_offset    i64
	error_code     i32
	error_msg      string
}

/// new_grpc_consume_response creates a successful consume response.
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

/// new_grpc_consume_error creates an error consume response.
pub fn new_grpc_consume_error(topic string, partition i32, code i32, msg string) GrpcConsumeResponse {
	return GrpcConsumeResponse{
		topic:      topic
		partition:  partition
		records:    []GrpcRecord{}
		error_code: code
		error_msg:  msg
	}
}

/// GrpcStreamRequest represents a bidirectional stream request.
pub struct GrpcStreamRequest {
pub:
	request_type GrpcStreamRequestType
	produce      ?GrpcProduceRequest
	consume      ?GrpcConsumeRequest
	commit       ?GrpcCommitRequest
	ack          ?GrpcAckRequest
}

/// GrpcStreamRequestType represents the type of a stream request.
pub enum GrpcStreamRequestType {
	produce
	subscribe
	commit
	ack
	ping
}

/// grpc_stream_request_type_from_int converts an integer to a GrpcStreamRequestType.
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

/// GrpcCommitRequest represents an offset commit request.
pub struct GrpcCommitRequest {
pub:
	group_id string
	offsets  []GrpcPartitionOffset
}

/// GrpcPartitionOffset represents a partition offset.
pub struct GrpcPartitionOffset {
pub:
	topic     string
	partition i32
	offset    i64
	metadata  string
}

/// GrpcAckRequest represents a message acknowledgement.
pub struct GrpcAckRequest {
pub:
	topic     string
	partition i32
	offset    i64
}

/// GrpcStreamResponse represents a bidirectional stream response.
pub struct GrpcStreamResponse {
pub:
	response_type GrpcStreamResponseType
	produce       ?GrpcProduceResponse
	message       ?GrpcMessageResponse
	commit        ?GrpcCommitResponse
	error         ?GrpcErrorResponse
	pong          ?GrpcPongResponse
}

/// GrpcStreamResponseType represents the type of a stream response.
pub enum GrpcStreamResponseType {
	produce_ack
	message
	commit_ack
	error
	pong
}

/// GrpcMessageResponse represents a consumed message.
pub struct GrpcMessageResponse {
pub:
	topic     string
	partition i32
	offset    i64
	timestamp i64
	key       []u8
	value     []u8
	headers   map[string][]u8
}

/// GrpcCommitResponse represents a commit result.
pub struct GrpcCommitResponse {
pub:
	success bool
	message string
}

/// GrpcErrorResponse represents an error.
pub struct GrpcErrorResponse {
pub:
	code    i32
	message string
}

/// GrpcPongResponse represents a pong.
pub struct GrpcPongResponse {
pub:
	timestamp i64
}

/// GrpcConnectionState represents the state of a gRPC connection.
pub enum GrpcConnectionState {
	connecting
	ready
	streaming
	closing
	closed
}

/// GrpcConnection represents an active gRPC connection.
pub struct GrpcConnection {
pub:
	id         string
	client_ip  string
	created_at i64
pub mut:
	state          GrpcConnectionState
	subscriptions  []Subscription
	requests_recv  i64
	responses_sent i64
	bytes_recv     i64
	bytes_sent     i64
	last_activity  i64
	stream_type    GrpcStreamType
}

/// GrpcStreamType represents the type of a gRPC stream.
pub enum GrpcStreamType {
	unary
	server_streaming
	client_streaming
	bidirectional
}

/// new_grpc_connection creates a new gRPC connection.
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

/// GrpcConfig holds gRPC server configuration.
pub struct GrpcConfig {
pub:
	port                   int  = 9093
	max_connections        int  = 10000
	max_message_size       int  = 4194304
	max_concurrent_streams int  = 100
	keepalive_time_ms      int  = 30000
	keepalive_timeout_ms   int  = 10000
	connection_timeout_ms  int  = 300000
	max_batch_size         int  = 1000
	enable_reflection      bool = true
}

/// default_grpc_config returns the default gRPC configuration.
pub fn default_grpc_config() GrpcConfig {
	return GrpcConfig{}
}

/// grpc_error_none constant.
pub const grpc_error_none = 0
/// grpc_error_unknown constant.
pub const grpc_error_unknown = -1
/// grpc_error_offset_out_of_range constant.
pub const grpc_error_offset_out_of_range = 1
/// grpc_error_invalid_message constant.
pub const grpc_error_invalid_message = 2
/// grpc_error_unknown_topic constant.
pub const grpc_error_unknown_topic = 3
/// grpc_error_invalid_partition constant.
pub const grpc_error_invalid_partition = 4
/// grpc_error_leader_not_available constant.
pub const grpc_error_leader_not_available = 5
/// grpc_error_not_leader_for_partition constant.
pub const grpc_error_not_leader_for_partition = 6
/// grpc_error_request_timed_out constant.
pub const grpc_error_request_timed_out = 7
/// grpc_error_message_too_large constant.
pub const grpc_error_message_too_large = 10
/// grpc_error_group_coordinator_not_available constant.
pub const grpc_error_group_coordinator_not_available = 15
/// grpc_error_not_coordinator constant.
pub const grpc_error_not_coordinator = 16
/// grpc_error_invalid_topic constant.
pub const grpc_error_invalid_topic = 17
/// grpc_error_record_list_too_large constant.
pub const grpc_error_record_list_too_large = 18
/// grpc_error_group_auth_failed constant.
pub const grpc_error_group_auth_failed = 30
/// grpc_error_invalid_session_timeout constant.
pub const grpc_error_invalid_session_timeout = 26

/// grpc_error_message returns a human-readable error message.
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

/// encode encodes a GrpcRecord to bytes.
pub fn (r &GrpcRecord) encode() []u8 {
	mut buf := []u8{cap: 64 + r.key.len + r.value.len}

	key_len := r.key.len
	buf << u8(key_len >> 24)
	buf << u8(key_len >> 16)
	buf << u8(key_len >> 8)
	buf << u8(key_len)
	buf << r.key

	val_len := r.value.len
	buf << u8(val_len >> 24)
	buf << u8(val_len >> 16)
	buf << u8(val_len >> 8)
	buf << u8(val_len)
	buf << r.value

	ts := r.timestamp
	buf << u8(ts >> 56)
	buf << u8(ts >> 48)
	buf << u8(ts >> 40)
	buf << u8(ts >> 32)
	buf << u8(ts >> 24)
	buf << u8(ts >> 16)
	buf << u8(ts >> 8)
	buf << u8(ts)

	header_count := r.headers.len
	buf << u8(header_count >> 24)
	buf << u8(header_count >> 16)
	buf << u8(header_count >> 8)
	buf << u8(header_count)

	for k, v in r.headers {
		k_len := k.len
		buf << u8(k_len >> 8)
		buf << u8(k_len)
		buf << k.bytes()

		v_len := v.len
		buf << u8(v_len >> 8)
		buf << u8(v_len)
		buf << v
	}

	return buf
}

/// decode_grpc_record decodes bytes into a GrpcRecord.
pub fn decode_grpc_record(data []u8) !GrpcRecord {
	if data.len < 20 {
		return error('Data too short for GrpcRecord')
	}

	mut pos := 0

	key_len := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4
	if pos + key_len > data.len {
		return error('Invalid key length')
	}
	key := data[pos..pos + key_len].clone()
	pos += key_len

	if pos + 4 > data.len {
		return error('Data too short for value length')
	}
	val_len := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4
	if pos + val_len > data.len {
		return error('Invalid value length')
	}
	value := data[pos..pos + val_len].clone()
	pos += val_len

	if pos + 8 > data.len {
		return error('Data too short for timestamp')
	}
	timestamp := i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))
	pos += 8

	if pos + 4 > data.len {
		return error('Data too short for header count')
	}
	header_count := int(u32(data[pos]) << 24 | u32(data[pos + 1]) << 16 | u32(data[pos + 2]) << 8 | u32(data[
		pos + 3]))
	pos += 4

	mut headers := map[string][]u8{}
	for _ in 0 .. header_count {
		if pos + 2 > data.len {
			return error('Data too short for header key length')
		}
		hk_len := int(u32(data[pos]) << 8 | u32(data[pos + 1]))
		pos += 2
		if pos + hk_len > data.len {
			return error('Invalid header key length')
		}
		hk := data[pos..pos + hk_len].bytestr()
		pos += hk_len

		if pos + 2 > data.len {
			return error('Data too short for header value length')
		}
		hv_len := int(u32(data[pos]) << 8 | u32(data[pos + 1]))
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

/// to_domain_record converts a GrpcRecord to a domain.Record.
pub fn (r &GrpcRecord) to_domain_record() Record {
	return Record{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: if r.timestamp > 0 { time.unix(r.timestamp / 1000) } else { time.now() }
	}
}

/// grpc_record_from_domain creates a GrpcRecord from a domain.Record.
pub fn grpc_record_from_domain(r &Record) GrpcRecord {
	return GrpcRecord{
		key:       r.key
		value:     r.value
		headers:   r.headers
		timestamp: r.timestamp.unix_milli()
	}
}
