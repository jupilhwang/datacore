// Kafka protocol - Consumer group response encoder
// Encoding methods for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

// JoinGroup response encoder (API Key 11)

/// encode encodes the JoinGroup response (API Key 11).
fn (r JoinGroupResponse) encode(version i16) []u8 {
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
			// v5+: static membership instance ID
			if version >= 5 {
				writer.write_compact_nullable_string(m.group_instance_id)
			}
			writer.write_compact_bytes(m.metadata)
			writer.write_tagged_fields()
		} else {
			writer.write_string(m.member_id)
			// v5+: static membership instance ID
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

// SyncGroup response encoder (API Key 14)

/// encode encodes the SyncGroup response (API Key 14).
fn (r SyncGroupResponse) encode(version i16) []u8 {
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

// Heartbeat response encoder (API Key 12)

/// encode encodes the Heartbeat response (API Key 12).
fn (r HeartbeatResponse) encode(version i16) []u8 {
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

// LeaveGroup response encoder (API Key 13)

/// encode encodes the LeaveGroup response (API Key 13).
fn (r LeaveGroupResponse) encode(version i16) []u8 {
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

// ConsumerGroupHeartbeat response encoder (API Key 68) - KIP-848

/// encode encodes the ConsumerGroupHeartbeat response (API Key 68) - KIP-848.
pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
	// ConsumerGroupHeartbeat is always flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32 - throttle time
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 - error code
	writer.write_i16(r.error_code)

	// error_message: COMPACT_NULLABLE_STRING - error message
	writer.write_compact_nullable_string(r.error_message)

	// member_id: COMPACT_NULLABLE_STRING - member ID
	writer.write_compact_nullable_string(r.member_id)

	// member_epoch: INT32 - member epoch
	writer.write_i32(r.member_epoch)

	// heartbeat_interval_ms: INT32 - heartbeat interval
	writer.write_i32(r.heartbeat_interval_ms)

	// assignment: Assignment (nullable) - partition assignment
	if assignment := r.assignment {
		// Write topic_partitions array
		writer.write_compact_array_len(assignment.topic_partitions.len)

		for tp in assignment.topic_partitions {
			// topic_id: UUID (16 bytes) - topic UUID
			writer.write_uuid(tp.topic_id)

			// partitions: COMPACT_ARRAY[INT32] - partition list
			writer.write_compact_array_len(tp.partitions.len)
			for p in tp.partitions {
				writer.write_i32(p)
			}

			// Tagged fields for each topic partition
			writer.write_tagged_fields()
		}

		// Tagged fields for the assignment
		writer.write_tagged_fields()
	} else {
		// Write -1 to indicate null assignment
		// For compact nullable struct, use 0 to indicate null (length = 0 - 1 = -1)
		writer.write_uvarint(0)
	}

	// Final tagged fields
	writer.write_tagged_fields()

	return writer.bytes()
}

// ConsumerGroupDescribe response encoder (API Key 69) - KIP-848

/// encode encodes the ConsumerGroupDescribe response (API Key 69) - KIP-848.
fn (r ConsumerGroupDescribeResponse) encode(version i16) []u8 {
	// ConsumerGroupDescribe is always flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32 - throttle time
	writer.write_i32(r.throttle_time_ms)

	// groups: COMPACT_ARRAY[Group] - group list
	writer.write_compact_array_len(r.groups.len)

	for g in r.groups {
		// error_code: INT16 - error code
		writer.write_i16(g.error_code)

		// error_message: COMPACT_NULLABLE_STRING - error message
		writer.write_compact_nullable_string(g.error_message)

		// group_id: COMPACT_STRING - group ID
		writer.write_compact_string(g.group_id)

		// group_state: COMPACT_STRING - group state
		writer.write_compact_string(g.group_state)

		// group_epoch: INT32 - group epoch
		writer.write_i32(g.group_epoch)

		// assignment_epoch: INT32 - assignment epoch
		writer.write_i32(g.assignment_epoch)

		// assignor_name: COMPACT_STRING - assignor name
		writer.write_compact_string(g.assignor_name)

		// members: COMPACT_ARRAY[Member] - member list
		writer.write_compact_array_len(g.members.len)

		for m in g.members {
			// member_id: COMPACT_STRING - member ID
			writer.write_compact_string(m.member_id)

			// instance_id: COMPACT_NULLABLE_STRING - instance ID
			writer.write_compact_nullable_string(m.instance_id)

			// rack_id: COMPACT_NULLABLE_STRING - rack ID
			writer.write_compact_nullable_string(m.rack_id)

			// member_epoch: INT32 - member epoch
			writer.write_i32(m.member_epoch)

			// client_id: COMPACT_STRING - client ID
			writer.write_compact_string(m.client_id)

			// client_host: COMPACT_STRING - client host
			writer.write_compact_string(m.client_host)

			// subscribed_topic_ids: COMPACT_ARRAY[UUID] - subscribed topic ID list
			writer.write_compact_array_len(m.subscribed_topic_ids.len)
			for topic_id in m.subscribed_topic_ids {
				writer.write_uuid(topic_id)
			}

			// assignment: Assignment (nullable) - current assignment
			if assignment := m.assignment {
				// Write topic_partitions array
				writer.write_compact_array_len(assignment.topic_partitions.len)

				for tp in assignment.topic_partitions {
					// topic_id: UUID (16 bytes) - topic UUID
					writer.write_uuid(tp.topic_id)

					// partitions: COMPACT_ARRAY[INT32] - partition list
					writer.write_compact_array_len(tp.partitions.len)
					for p in tp.partitions {
						writer.write_i32(p)
					}

					// Tagged fields for each topic partition
					writer.write_tagged_fields()
				}

				// Tagged fields for the assignment
				writer.write_tagged_fields()
			} else {
				// null assignment
				writer.write_uvarint(0)
			}

			// Tagged fields for the member
			writer.write_tagged_fields()
		}

		// authorized_operations: INT32 - authorized operations
		writer.write_i32(g.authorized_operations)

		// Tagged fields for the group
		writer.write_tagged_fields()
	}

	// Final tagged fields
	writer.write_tagged_fields()

	return writer.bytes()
}
