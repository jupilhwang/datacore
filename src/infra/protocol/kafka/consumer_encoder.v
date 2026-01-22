// Kafka 프로토콜 - 컨슈머 그룹 응답 인코더
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat 인코딩 메서드
module kafka

// ============================================================================
// JoinGroup 응답 인코더 (API Key 11)
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
			// v5+: 정적 멤버십 인스턴스 ID
			if version >= 5 {
				writer.write_compact_nullable_string(m.group_instance_id)
			}
			writer.write_compact_bytes(m.metadata)
			writer.write_tagged_fields()
		} else {
			writer.write_string(m.member_id)
			// v5+: 정적 멤버십 인스턴스 ID
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
// SyncGroup 응답 인코더 (API Key 14)
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
// Heartbeat 응답 인코더 (API Key 12)
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
// LeaveGroup 응답 인코더 (API Key 13)
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
// ConsumerGroupHeartbeat 응답 인코더 (API Key 68) - KIP-848
// ============================================================================

pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
	// ConsumerGroupHeartbeat는 항상 flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32 - 스로틀링 시간
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 - 에러 코드
	writer.write_i16(r.error_code)

	// error_message: COMPACT_NULLABLE_STRING - 에러 메시지
	writer.write_compact_nullable_string(r.error_message)

	// member_id: COMPACT_NULLABLE_STRING - 멤버 ID
	writer.write_compact_nullable_string(r.member_id)

	// member_epoch: INT32 - 멤버 에포크
	writer.write_i32(r.member_epoch)

	// heartbeat_interval_ms: INT32 - 하트비트 간격
	writer.write_i32(r.heartbeat_interval_ms)

	// assignment: Assignment (nullable) - 파티션 할당
	if assignment := r.assignment {
		// topic_partitions 배열 쓰기
		writer.write_compact_array_len(assignment.topic_partitions.len)

		for tp in assignment.topic_partitions {
			// topic_id: UUID (16바이트) - 토픽 UUID
			writer.write_uuid(tp.topic_id)

			// partitions: COMPACT_ARRAY[INT32] - 파티션 목록
			writer.write_compact_array_len(tp.partitions.len)
			for p in tp.partitions {
				writer.write_i32(p)
			}

			// 각 토픽 파티션의 태그된 필드
			writer.write_tagged_fields()
		}

		// 할당의 태그된 필드
		writer.write_tagged_fields()
	} else {
		// null 할당을 나타내기 위해 -1 쓰기
		// compact nullable 구조체의 경우 0을 사용하여 null을 나타냄 (length = 0 - 1 = -1)
		writer.write_uvarint(0)
	}

	// 마지막 태그된 필드
	writer.write_tagged_fields()

	return writer.bytes()
}
