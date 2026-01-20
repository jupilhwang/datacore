// Adapter Layer - Kafka Consumer Response Building
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat responses
module kafka

// ============================================================================
// JoinGroup Response (API Key 11)
// ============================================================================

pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	generation_id    i32
	protocol_type    ?string
	protocol_name    ?string
	leader           string
	skip_assignment  bool
	member_id        string
	members          []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string // v5+
	metadata          []u8
}

pub fn (r JoinGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	writer.write_i32(r.generation_id)

	if version >= 7 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	} else {
		if is_flexible {
			writer.write_compact_string(r.protocol_name or { '' })
		} else {
			writer.write_string(r.protocol_name or { '' })
		}
	}

	if is_flexible {
		writer.write_compact_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_compact_string(r.member_id)
		writer.write_compact_array_len(r.members.len)
	} else {
		writer.write_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_string(r.member_id)
		writer.write_array_len(r.members.len)
	}

	for m in r.members {
		if is_flexible {
			writer.write_compact_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_compact_nullable_string(m.group_instance_id)
			}
			writer.write_compact_bytes(m.metadata)
			writer.write_tagged_fields()
		} else {
			writer.write_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_bytes(m.metadata)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// SyncGroup Response (API Key 14)
// ============================================================================

pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

pub fn (r SyncGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 5 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	}

	if is_flexible {
		writer.write_compact_bytes(r.assignment)
		writer.write_tagged_fields()
	} else {
		writer.write_bytes(r.assignment)
	}

	return writer.bytes()
}

// ============================================================================
// Heartbeat Response (API Key 12)
// ============================================================================

pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

pub fn (r HeartbeatResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// LeaveGroup Response (API Key 13)
// ============================================================================

pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

pub fn (r LeaveGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 3 {
		if is_flexible {
			writer.write_compact_array_len(r.members.len)
		} else {
			writer.write_array_len(r.members.len)
		}
		for m in r.members {
			if is_flexible {
				writer.write_compact_string(m.member_id)
				writer.write_compact_nullable_string(m.group_instance_id)
			} else {
				writer.write_string(m.member_id)
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_i16(m.error_code)
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

// ============================================================================
// ConsumerGroupHeartbeat Response (API Key 68) - KIP-848
// ============================================================================
// Response for the new Consumer Rebalance Protocol

pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         ?string
	member_id             ?string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ConsumerGroupHeartbeatAssignment
}

pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
	// ConsumerGroupHeartbeat is always flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.error_message)

	// member_id: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.member_id)

	// member_epoch: INT32
	writer.write_i32(r.member_epoch)

	// heartbeat_interval_ms: INT32
	writer.write_i32(r.heartbeat_interval_ms)

	// assignment: Assignment (nullable)
	if assignment := r.assignment {
		// Write topic_partitions array
		writer.write_compact_array_len(assignment.topic_partitions.len)

		for tp in assignment.topic_partitions {
			// topic_id: UUID (16 bytes)
			writer.write_uuid(tp.topic_id)

			// partitions: COMPACT_ARRAY[INT32]
			writer.write_compact_array_len(tp.partitions.len)
			for p in tp.partitions {
				writer.write_i32(p)
			}

			// Tagged fields for each topic partition
			writer.write_tagged_fields()
		}

		// Tagged fields for assignment
		writer.write_tagged_fields()
	} else {
		// Write -1 to indicate null assignment
		// For compact nullable structs, we use 0 to indicate null (length = 0 - 1 = -1)
		writer.write_uvarint(0)
	}

	// Tagged fields at the end
	writer.write_tagged_fields()

	return writer.bytes()
}
