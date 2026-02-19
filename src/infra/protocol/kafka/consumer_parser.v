// Kafka protocol - Consumer group request parser
// Parsing functions for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

// JoinGroup 요청 파서 (API Key 11)

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

// SyncGroup 요청 파서 (API Key 14)

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

// Heartbeat 요청 파서 (API Key 12)

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

// LeaveGroup 요청 파서 (API Key 13)

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!

	// v0-v2: 단일 member_id
	mut member_id := ''
	mut members := []LeaveGroupMember{}

	if version <= 2 {
		member_id = reader.read_flex_string(is_flexible)!
	} else {
		// v3+: members 배열
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

			// v5+: 탈퇴 사유 필드
			mut m_reason := ?string(none)
			if version >= 5 {
				raw_reason := reader.read_flex_nullable_string(is_flexible)!
				if raw_reason.len > 0 {
					m_reason = raw_reason
				}
			}

			// flexible 버전에서 각 멤버의 태그된 필드 건너뛰기
			reader.skip_flex_tagged_fields(is_flexible)!

			members << LeaveGroupMember{
				member_id:         m_member_id
				group_instance_id: m_group_instance_id
				reason:            m_reason
			}
		}
	}

	// flexible 버전의 태그된 필드 건너뛰기
	reader.skip_flex_tagged_fields(is_flexible)!

	return LeaveGroupRequest{
		group_id:  group_id
		member_id: member_id
		members:   members
	}
}

// ConsumerGroupHeartbeat 요청 파서 (API Key 68) - KIP-848

fn parse_consumer_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ConsumerGroupHeartbeatRequest {
	// ConsumerGroupHeartbeat는 항상 flexible (v0+)

	// group_id: COMPACT_STRING - 그룹 ID
	group_id := reader.read_compact_string()!

	// member_id: COMPACT_STRING - 멤버 ID
	member_id := reader.read_compact_string()!

	// member_epoch: INT32 - 멤버 에포크
	member_epoch := reader.read_i32()!

	// instance_id: COMPACT_NULLABLE_STRING - 정적 멤버십 인스턴스 ID
	raw_instance_id := reader.read_compact_nullable_string()!
	instance_id := if raw_instance_id.len > 0 { ?string(raw_instance_id) } else { ?string(none) }

	// rack_id: COMPACT_NULLABLE_STRING - 랙 ID
	raw_rack_id := reader.read_compact_nullable_string()!
	rack_id := if raw_rack_id.len > 0 { ?string(raw_rack_id) } else { ?string(none) }

	// rebalance_timeout_ms: INT32 - 리밸런싱 타임아웃
	rebalance_timeout_ms := reader.read_i32()!

	// subscribed_topic_names: COMPACT_ARRAY[COMPACT_STRING] - 구독 토픽 목록
	topic_count := reader.read_compact_array_len()!
	mut subscribed_topic_names := []string{}
	for _ in 0 .. topic_count {
		subscribed_topic_names << reader.read_compact_string()!
	}

	// server_assignor: COMPACT_NULLABLE_STRING - 서버 할당자
	raw_server_assignor := reader.read_compact_nullable_string()!
	server_assignor := if raw_server_assignor.len > 0 {
		?string(raw_server_assignor)
	} else {
		?string(none)
	}

	// topic_partitions: COMPACT_ARRAY[TopicPartition] - 현재 할당된 파티션
	tp_count := reader.read_compact_array_len()!
	mut topic_partitions := []ConsumerGroupHeartbeatTopicPartition{}
	for _ in 0 .. tp_count {
		// topic_id: UUID (16바이트) - 토픽 UUID
		topic_id := reader.read_uuid()!

		// partitions: COMPACT_ARRAY[INT32] - 파티션 목록
		part_count := reader.read_compact_array_len()!
		mut partitions := []i32{}
		for _ in 0 .. part_count {
			partitions << reader.read_i32()!
		}

		topic_partitions << ConsumerGroupHeartbeatTopicPartition{
			topic_id:   topic_id
			partitions: partitions
		}

		// 각 토픽 파티션의 태그된 필드 건너뛰기
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

// ConsumerGroupDescribe 요청 파서 (API Key 69) - KIP-848

fn parse_consumer_group_describe_request(mut reader BinaryReader, version i16, is_flexible bool) !ConsumerGroupDescribeRequest {
	// ConsumerGroupDescribe는 항상 flexible (v0+)

	// group_ids: COMPACT_ARRAY[COMPACT_STRING] - 그룹 ID 목록
	group_count := reader.read_compact_array_len()!
	mut group_ids := []string{}
	for _ in 0 .. group_count {
		group_ids << reader.read_compact_string()!
	}

	// include_authorized_operations: BOOLEAN - 권한 있는 작업 포함 여부
	include_authorized_operations := reader.read_i8()! != 0

	// 태그된 필드 건너뛰기
	reader.skip_tagged_fields()!

	return ConsumerGroupDescribeRequest{
		group_ids:                     group_ids
		include_authorized_operations: include_authorized_operations
	}
}
