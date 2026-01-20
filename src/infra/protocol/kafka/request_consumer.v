// Infra Layer - Kafka Request Parsing - Consumer Group Operations
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat request parsing
module kafka

// JoinGroup Request
pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    ?string
	protocol_type        string
	protocols            []JoinGroupRequestProtocol
}

pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

fn parse_join_group_request(mut reader BinaryReader, version i16, is_flexible bool) !JoinGroupRequest {
	group_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	session_timeout_ms := reader.read_i32()!

	mut rebalance_timeout_ms := session_timeout_ms
	if version >= 1 {
		rebalance_timeout_ms = reader.read_i32()!
	}

	member_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	mut group_instance_id := ?string(none)
	if version >= 5 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	protocol_type := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	protocol_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut protocols := []JoinGroupRequestProtocol{}
	for _ in 0 .. protocol_count {
		name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		metadata := if is_flexible {
			reader.read_compact_bytes()!
		} else {
			reader.read_bytes()!
		}

		protocols << JoinGroupRequestProtocol{
			name:     name
			metadata: metadata
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return JoinGroupRequest{
		group_id:             group_id
		session_timeout_ms:   session_timeout_ms
		rebalance_timeout_ms: rebalance_timeout_ms
		member_id:            member_id
		group_instance_id:    group_instance_id
		protocol_type:        protocol_type
		protocols:            protocols
	}
}

// SyncGroup Request
pub struct SyncGroupRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
	protocol_type     ?string
	protocol_name     ?string
	assignments       []SyncGroupRequestAssignment
}

pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

fn parse_sync_group_request(mut reader BinaryReader, version i16, is_flexible bool) !SyncGroupRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	generation_id := reader.read_i32()!
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }

	mut group_instance_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	mut protocol_type := ?string(none)
	mut protocol_name := ?string(none)
	if version >= 5 {
		if is_flexible {
			pt := reader.read_compact_string()!
			pn := reader.read_compact_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		} else {
			pt := reader.read_nullable_string()!
			pn := reader.read_nullable_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		}
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut assignments := []SyncGroupRequestAssignment{}
	for _ in 0 .. count {
		mid := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		assign := if is_flexible { reader.read_compact_bytes()! } else { reader.read_bytes()! }
		assignments << SyncGroupRequestAssignment{
			member_id:  mid
			assignment: assign
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return SyncGroupRequest{
		group_id:          group_id
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
		protocol_type:     protocol_type
		protocol_name:     protocol_name
		assignments:       assignments
	}
}

// Heartbeat Request
pub struct HeartbeatRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
}

fn parse_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !HeartbeatRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	generation_id := reader.read_i32()!
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	mut group_instance_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}
	return HeartbeatRequest{
		group_id:          group_id
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
	}
}

// LeaveGroup Request
pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string // v0-v2: single member_id
}

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	return LeaveGroupRequest{
		group_id:  group_id
		member_id: member_id
	}
}

// ConsumerGroupHeartbeat Request (API Key 68) - KIP-848
// Used for the new Consumer Rebalance Protocol
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string // Static membership (group.instance.id)
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition // Current assignment
}

pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

fn parse_consumer_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ConsumerGroupHeartbeatRequest {
	// ConsumerGroupHeartbeat is always flexible (v0+)

	// group_id: COMPACT_STRING
	group_id := reader.read_compact_string()!

	// member_id: COMPACT_STRING
	member_id := reader.read_compact_string()!

	// member_epoch: INT32
	member_epoch := reader.read_i32()!

	// instance_id: COMPACT_NULLABLE_STRING
	raw_instance_id := reader.read_compact_nullable_string()!
	instance_id := if raw_instance_id.len > 0 { ?string(raw_instance_id) } else { ?string(none) }

	// rack_id: COMPACT_NULLABLE_STRING
	raw_rack_id := reader.read_compact_nullable_string()!
	rack_id := if raw_rack_id.len > 0 { ?string(raw_rack_id) } else { ?string(none) }

	// rebalance_timeout_ms: INT32
	rebalance_timeout_ms := reader.read_i32()!

	// subscribed_topic_names: COMPACT_ARRAY[COMPACT_STRING]
	topic_count := reader.read_compact_array_len()!
	mut subscribed_topic_names := []string{}
	for _ in 0 .. topic_count {
		subscribed_topic_names << reader.read_compact_string()!
	}

	// server_assignor: COMPACT_NULLABLE_STRING
	raw_server_assignor := reader.read_compact_nullable_string()!
	server_assignor := if raw_server_assignor.len > 0 {
		?string(raw_server_assignor)
	} else {
		?string(none)
	}

	// topic_partitions: COMPACT_ARRAY[TopicPartition]
	tp_count := reader.read_compact_array_len()!
	mut topic_partitions := []ConsumerGroupHeartbeatTopicPartition{}
	for _ in 0 .. tp_count {
		// topic_id: UUID (16 bytes)
		topic_id := reader.read_uuid()!

		// partitions: COMPACT_ARRAY[INT32]
		part_count := reader.read_compact_array_len()!
		mut partitions := []i32{}
		for _ in 0 .. part_count {
			partitions << reader.read_i32()!
		}

		topic_partitions << ConsumerGroupHeartbeatTopicPartition{
			topic_id:   topic_id
			partitions: partitions
		}

		// Skip tagged fields for each topic partition
		reader.skip_tagged_fields()!
	}

	return ConsumerGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		instance_id:            instance_id
		rack_id:                rack_id
		rebalance_timeout_ms:   rebalance_timeout_ms
		subscribed_topic_names: subscribed_topic_names
		server_assignor:        server_assignor
		topic_partitions:       topic_partitions
	}
}
