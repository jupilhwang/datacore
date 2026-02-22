// Infra Layer - Gateway Protocol Adapter
//
// Common adapter that normalizes requests from gRPC, SSE, and WebSocket
// protocols into a uniform format for the core engine, and converts
// core engine responses back into each protocol's wire format.
//
// This adapter acts as a bridge between the interface layer handlers and
// the storage/service layer. It is designed to be stateless; each call
// produces a deterministic result from its inputs.
module gateway

import domain
import infra.observability
import service.port
import time

// ProtocolType identifies which protocol a request originated from.
pub enum ProtocolType {
	grpc
	sse
	websocket
}

// GatewayRequest is a protocol-agnostic request destined for the core engine.
pub struct GatewayRequest {
pub:
	protocol    ProtocolType
	action      GatewayAction
	topic       string
	partition   ?i32
	offset      ?i64
	group_id    ?string
	client_id   string
	records     []GatewayRecord
	max_records int = 100
	max_bytes   int = 1048576
}

// GatewayAction is the operation requested by the client.
pub enum GatewayAction {
	produce
	consume
	commit
}

// GatewayRecord is a protocol-agnostic message record.
pub struct GatewayRecord {
pub:
	key       []u8
	value     []u8
	headers   map[string][]u8
	timestamp i64
}

// GatewayResponse is the protocol-agnostic response from the core engine.
pub struct GatewayResponse {
pub:
	action      GatewayAction
	topic       string
	partition   i32
	base_offset i64
	records     []GatewayRecord
	hwm         i64
	next_offset i64
	error_code  i32
	error_msg   string
}

// GatewayAdapter converts between protocol-specific types and core engine types.
pub struct GatewayAdapter {
mut:
	storage port.StoragePort
}

// new_gateway_adapter creates a new gateway adapter.
pub fn new_gateway_adapter(storage port.StoragePort) GatewayAdapter {
	return GatewayAdapter{
		storage: storage
	}
}

// handle_request executes a gateway request against the core engine.
pub fn (mut a GatewayAdapter) handle_request(req GatewayRequest) GatewayResponse {
	start_time := time.now()

	result := match req.action {
		.produce { a.handle_produce(req) }
		.consume { a.handle_consume(req) }
		.commit { a.handle_commit(req) }
	}

	elapsed_ms := time.since(start_time).milliseconds()

	observability.log_with_context('gateway', .debug, 'Request', 'Gateway request completed',
		{
		'protocol':   req.protocol.str()
		'action':     req.action.str()
		'topic':      req.topic
		'elapsed_ms': elapsed_ms.str()
		'error':      result.error_msg
	})

	return result
}

// handle_produce appends records to storage.
fn (mut a GatewayAdapter) handle_produce(req GatewayRequest) GatewayResponse {
	// Verify topic exists
	topic_meta := a.storage.get_topic(req.topic) or {
		return GatewayResponse{
			action:     .produce
			topic:      req.topic
			error_code: 3
			error_msg:  'Topic not found: ${req.topic}'
		}
	}

	partition := req.partition or { i32(0) }

	if partition < 0 || partition >= topic_meta.partition_count {
		return GatewayResponse{
			action:     .produce
			topic:      req.topic
			partition:  partition
			error_code: 4
			error_msg:  'Invalid partition: ${partition}'
		}
	}

	// Convert to domain records
	mut domain_records := []domain.Record{cap: req.records.len}
	for rec in req.records {
		domain_records << domain.Record{
			key:       rec.key
			value:     rec.value
			headers:   rec.headers
			timestamp: if rec.timestamp > 0 {
				time.unix(rec.timestamp / 1000)
			} else {
				time.now()
			}
		}
	}

	// Append to storage
	result := a.storage.append(req.topic, partition, domain_records, i16(1)) or {
		return GatewayResponse{
			action:     .produce
			topic:      req.topic
			partition:  partition
			error_code: -1
			error_msg:  'Failed to produce: ${err}'
		}
	}

	return GatewayResponse{
		action:      .produce
		topic:       req.topic
		partition:   partition
		base_offset: result.base_offset
		records:     []GatewayRecord{}
		error_code:  0
		error_msg:   ''
	}
}

// handle_consume fetches records from storage.
fn (mut a GatewayAdapter) handle_consume(req GatewayRequest) GatewayResponse {
	partition := req.partition or { i32(0) }
	offset := req.offset or { i64(0) }

	result := a.storage.fetch(req.topic, partition, offset, req.max_bytes) or {
		return GatewayResponse{
			action:     .consume
			topic:      req.topic
			partition:  partition
			error_code: -1
			error_msg:  'Failed to consume: ${err}'
		}
	}

	// Convert domain records to gateway records
	mut gateway_records := []GatewayRecord{cap: result.records.len}
	for rec in result.records {
		gateway_records << GatewayRecord{
			key:       rec.key
			value:     rec.value
			headers:   rec.headers
			timestamp: rec.timestamp.unix_milli()
		}
	}

	next_off := offset + i64(result.records.len)

	return GatewayResponse{
		action:      .consume
		topic:       req.topic
		partition:   partition
		base_offset: offset
		records:     gateway_records
		hwm:         result.high_watermark
		next_offset: next_off
		error_code:  0
		error_msg:   ''
	}
}

// handle_commit persists consumer group offsets.
fn (mut a GatewayAdapter) handle_commit(req GatewayRequest) GatewayResponse {
	group_id := req.group_id or {
		return GatewayResponse{
			action:     .commit
			topic:      req.topic
			error_code: 2
			error_msg:  'group_id is required for commit'
		}
	}

	partition := req.partition or { i32(0) }
	offset := req.offset or { i64(0) }

	a.storage.commit_offsets(group_id, [
		domain.PartitionOffset{
			topic:     req.topic
			partition: partition
			offset:    offset
		},
	]) or {
		return GatewayResponse{
			action:     .commit
			topic:      req.topic
			partition:  partition
			error_code: -1
			error_msg:  'Failed to commit: ${err}'
		}
	}

	return GatewayResponse{
		action:      .commit
		topic:       req.topic
		partition:   partition
		base_offset: offset
		error_code:  0
		error_msg:   ''
	}
}

// str returns the string representation of a ProtocolType.
pub fn (p ProtocolType) str() string {
	return match p {
		.grpc { 'grpc' }
		.sse { 'sse' }
		.websocket { 'websocket' }
	}
}

// str returns the string representation of a GatewayAction.
pub fn (a GatewayAction) str() string {
	return match a {
		.produce { 'produce' }
		.consume { 'consume' }
		.commit { 'commit' }
	}
}
