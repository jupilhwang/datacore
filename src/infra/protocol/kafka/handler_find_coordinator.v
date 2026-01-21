// Kafka Protocol - FindCoordinator (API Key 10)
// Request/Response types, parsing, encoding, and handlers
module kafka

// ============================================================================
// FindCoordinator (API Key 10)
// ============================================================================

pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

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

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

fn (h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator,
		version))!
	resp := h.process_find_coordinator(req, version)!
	return resp.encode(version)
}

fn (h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
	if version >= 4 {
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       h.broker_id
				host:          h.host
				port:          h.broker_port
				error_code:    0
				error_message: none
			}
		}

		return FindCoordinatorResponse{
			throttle_time_ms: 0
			coordinators:     coordinators
		}
	}

	return FindCoordinatorResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    none
		node_id:          h.broker_id
		host:             h.host
		port:             h.broker_port
		coordinators:     []FindCoordinatorResponseNode{}
	}
}
