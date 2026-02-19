// Kafka protocol - FindCoordinator (API Key 10)
// Request/response types, parsing, encoding, and handlers
//
// Version history:
// - v0: Basic FindCoordinator
// - v1: Added KeyType (GROUP=0, TRANSACTION=1)
// - v2: Same as v1
// - v3: Flexible version
// - v4: Batch lookup support (CoordinatorKeys, KIP-699)
// - v5: Added TRANSACTION_ABORTABLE error code (KIP-890)
// - v6: Share Groups support (KeyType=2, KIP-932)
module kafka

import domain
import infra.observability
import time

// FindCoordinator (API Key 10)

/// CoordinatorKeyType defines coordinator key types.
/// Supported since v1; SHARE type was added in v6.
pub enum CoordinatorKeyType as i8 {
	group       = 0
	transaction = 1
	share       = 2
}

/// FindCoordinatorRequest holds the request data for FindCoordinator.
pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

/// FindCoordinatorResponse holds the response data for FindCoordinator.
pub struct FindCoordinatorResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    ?string
	node_id          i32
	host             string
	port             i32
	coordinators     []FindCoordinatorResponseNode
}

/// FindCoordinatorResponseNode holds coordinator information for a single key in a batch response.
pub struct FindCoordinatorResponseNode {
pub:
	key           string
	node_id       i32
	host          string
	port          i32
	error_code    i16
	error_message ?string
}

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
	mut key := ''
	mut coordinator_keys := []string{}
	mut key_type := i8(0)

	if version <= 3 {
		key = reader.read_flex_string(is_flexible)!
	}

	if version >= 1 {
		key_type = reader.read_i8()!
	}

	if version >= 4 {
		keys_len := reader.read_flex_array_len(is_flexible)!
		if keys_len > 0 {
			for _ in 0 .. keys_len {
				k := reader.read_flex_string(is_flexible)!
				coordinator_keys << k
			}
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return FindCoordinatorRequest{
		key:              key
		key_type:         key_type
		coordinator_keys: coordinator_keys
	}
}

/// encode serializes the FindCoordinatorResponse into bytes.
pub fn (r FindCoordinatorResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version <= 3 {
		writer.write_i16(r.error_code)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(r.error_message)
			} else {
				writer.write_nullable_string(r.error_message)
			}
		}

		writer.write_i32(r.node_id)
		if is_flexible {
			writer.write_compact_string(r.host)
		} else {
			writer.write_string(r.host)
		}
		writer.write_i32(r.port)
	} else {
		if is_flexible {
			writer.write_compact_array_len(r.coordinators.len)
		} else {
			writer.write_array_len(r.coordinators.len)
		}
		for node in r.coordinators {
			if is_flexible {
				writer.write_compact_string(node.key)
			} else {
				writer.write_string(node.key)
			}
			writer.write_i32(node.node_id)
			if is_flexible {
				writer.write_compact_string(node.host)
			} else {
				writer.write_string(node.host)
			}
			writer.write_i32(node.port)
			writer.write_i16(node.error_code)
			if is_flexible {
				writer.write_compact_nullable_string(node.error_message)
			} else {
				writer.write_nullable_string(node.error_message)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// coordinator_key_type_str converts a coordinator key type to its string representation.
fn coordinator_key_type_str(key_type i8) string {
	return match key_type {
		0 { 'GROUP' }
		1 { 'TRANSACTION' }
		2 { 'SHARE' }
		else { 'UNKNOWN' }
	}
}

fn (mut h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator,
		version))!

	key_type_str := coordinator_key_type_str(req.key_type)
	h.logger.debug('Processing find coordinator', observability.field_string('key', req.key),
		observability.field_string('key_type', key_type_str), observability.field_int('coordinator_keys',
		req.coordinator_keys.len), observability.field_int('version', version))

	resp := h.process_find_coordinator(req, version)!

	elapsed := time.since(start_time)
	h.logger.debug('Find coordinator completed', observability.field_int('node_id', h.broker_id),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

// compute_coordinator_broker determines the coordinator broker based on the group_id.
// Uses the partition assignment service if available; otherwise falls back to hash-based selection.
fn (mut h Handler) compute_coordinator_broker(key string, key_type i8) (i32, string, i32) {
	// Get active broker list in multi-broker mode
	mut active_brokers := []domain.BrokerInfo{}
	if mut registry := h.broker_registry {
		active_brokers = registry.list_active_brokers() or { []domain.BrokerInfo{} }
	}

	// Return self if no brokers or in single-broker mode
	if active_brokers.len == 0 {
		return h.broker_id, h.host, h.broker_port
	}

	// Use assignment-based coordinator selection for GROUP type if partition assigner is available
	if key_type == i8(CoordinatorKeyType.group) {
		if mut assigner := h.partition_assigner {
			// Hash group_id to determine partition number (range 0-999)
			mut hash := i32(0)
			for c in key.bytes() {
				hash = (hash * 31 + i32(c)) & 0x7FFFFFFF
			}
			partition := hash % 1000

			// Look up the leader broker for this partition from the assignment service
			coordinator_id := assigner.get_partition_leader('__consumer_offsets', partition) or {
				// Fall back to hash-based broker selection if no assignment exists
				active_brokers[hash % active_brokers.len].broker_id
			}

			// Find the selected broker's information
			for broker in active_brokers {
				if broker.broker_id == coordinator_id {
					return broker.broker_id, broker.host, broker.port
				}
			}
		}
	}

	// Default: hash-based broker selection (round-robin)
	mut hash := i32(0)
	for c in key.bytes() {
		hash = (hash * 31 + i32(c)) & 0x7FFFFFFF
	}
	selected_idx := hash % active_brokers.len
	selected := active_brokers[selected_idx]
	return selected.broker_id, selected.host, selected.port
}

fn (mut h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
	// v4+: batch lookup response (including v5 and v6)
	// v5 adds TRANSACTION_ABORTABLE error code support (KIP-890)
	// v6 adds Share Groups support (KIP-932)
	if version >= 4 {
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			// Validate key format for Share Group (v6): "groupId:topicId:partition"
			if version >= 6 && req.key_type == i8(CoordinatorKeyType.share) {
				// Validate Share Group key format
				parts := key.split(':')
				if parts.len != 3 {
					coordinators << FindCoordinatorResponseNode{
						key:           key
						node_id:       -1
						host:          ''
						port:          0
						error_code:    i16(ErrorCode.invalid_request)
						error_message: 'Invalid share group key format. Expected: groupId:topicId:partition'
					}
					continue
				}
			}

			// Determine coordinator broker (assignment-based or hash-based)
			node_id, host, port := h.compute_coordinator_broker(key, req.key_type)

			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       node_id
				host:          host
				port:          port
				error_code:    0
				error_message: none
			}
		}

		return FindCoordinatorResponse{
			throttle_time_ms: default_throttle_time_ms
			coordinators:     coordinators
		}
	}

	// v0-v3: single response
	// Determine coordinator broker (assignment-based or hash-based)
	node_id, host, port := h.compute_coordinator_broker(req.key, req.key_type)

	return FindCoordinatorResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		error_message:    none
		node_id:          node_id
		host:             host
		port:             port
		coordinators:     []FindCoordinatorResponseNode{}
	}
}
