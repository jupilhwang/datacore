// Kafka Protocol - Consumer Group Request Parsers
// Parsing functions for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

// ============================================================================
// JoinGroup Request Parser (API Key 11)
// ============================================================================

fn parse_join_group_request(mut reader BinaryReader, version i16, is_flexible bool) !JoinGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!

	session_timeout_ms := reader.read_i32()!

	mut rebalance_timeout_ms := session_timeout_ms
	if version >= 1 {
		rebalance_timeout_ms = reader.read_i32()!
	}

	member_id := reader.read_flex_string(is_flexible)!

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

	protocol_type := reader.read_flex_string(is_flexible)!

	protocol_count := reader.read_flex_array_len(is_flexible)!

	mut protocols := []JoinGroupRequestProtocol{}
	for _ in 0 .. protocol_count {
		name := reader.read_flex_string(is_flexible)!
		metadata := if is_flexible {
			reader.read_compact_bytes()!
		} else {
			reader.read_bytes()!
		}

		protocols << JoinGroupRequestProtocol{
			name:     name
			metadata: metadata
		}

		reader.skip_flex_tagged_fields(is_flexible)!
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

// ============================================================================
// SyncGroup Request Parser (API Key 14)
// ============================================================================

fn parse_sync_group_request(mut reader BinaryReader, version i16, is_flexible bool) !SyncGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!
	generation_id := reader.read_i32()!
	member_id := reader.read_flex_string(is_flexible)!

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

	count := reader.read_flex_array_len(is_flexible)!
	mut assignments := []SyncGroupRequestAssignment{}
	for _ in 0 .. count {
		mid := reader.read_flex_string(is_flexible)!
		assign := if is_flexible { reader.read_compact_bytes()! } else { reader.read_bytes()! }
		assignments << SyncGroupRequestAssignment{
			member_id:  mid
			assignment: assign
		}
		reader.skip_flex_tagged_fields(is_flexible)!
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

// ============================================================================
// Heartbeat Request Parser (API Key 12)
// ============================================================================

fn parse_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !HeartbeatRequest {
	group_id := reader.read_flex_string(is_flexible)!
	generation_id := reader.read_i32()!
	member_id := reader.read_flex_string(is_flexible)!
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

// ============================================================================
// LeaveGroup Request Parser (API Key 13)
// ============================================================================

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!

	// v0-v2: single member_id
	mut member_id := ''
	mut members := []LeaveGroupMember{}

	if version <= 2 {
		member_id = reader.read_flex_string(is_flexible)!
	} else {
		// v3+: members array
		members_len := if is_flexible {
			int(reader.read_uvarint()! - 1)
		} else {
			int(reader.read_i32()!)
		}

		for _ in 0 .. members_len {
			m_member_id := reader.read_flex_string(is_flexible)!
			raw_group_instance_id := reader.read_flex_nullable_string(is_flexible)!
			m_group_instance_id := if raw_group_instance_id.len > 0 {
				?string(raw_group_instance_id)
			} else {
				?string(none)
			}

			// v5+: reason field
			mut m_reason := ?string(none)
			if version >= 5 {
				raw_reason := reader.read_flex_nullable_string(is_flexible)!
				if raw_reason.len > 0 {
					m_reason = raw_reason
				}
			}

			// Skip tagged fields for each member in flexible version
			reader.skip_flex_tagged_fields(is_flexible)!

			members << LeaveGroupMember{
				member_id:         m_member_id
				group_instance_id: m_group_instance_id
				reason:            m_reason
			}
		}
	}

	// Skip tagged fields for flexible version
	reader.skip_flex_tagged_fields(is_flexible)!

	return LeaveGroupRequest{
		group_id:  group_id
		member_id: member_id
		members:   members
	}
}

// ============================================================================
// ConsumerGroupHeartbeat Request Parser (API Key 68) - KIP-848
// ============================================================================

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
