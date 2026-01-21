// Kafka Protocol - Consumer Group Response Encoders
// Encoding methods for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

// ============================================================================
// JoinGroup Response Encoder (API Key 11)
// ============================================================================

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
// SyncGroup Response Encoder (API Key 14)
// ============================================================================

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
// Heartbeat Response Encoder (API Key 12)
// ============================================================================

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
// LeaveGroup Response Encoder (API Key 13)
// ============================================================================

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
// ConsumerGroupHeartbeat Response Encoder (API Key 68) - KIP-848
// ============================================================================

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
