// Kafka 프로토콜 - Share Group 인코더 (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// 응답 인코딩 메서드
module kafka

/// encode는 ShareGroupHeartbeatResponse를 바이트로 인코딩합니다.
pub fn (r ShareGroupHeartbeatResponse) encode(version i16) []u8 {
	is_flexible := true // Share Group API는 항상 flexible
	mut writer := new_writer()

	// ThrottleTimeMs - 스로틀링 시간
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode - 에러 코드
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable) - 에러 메시지
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// MemberId (nullable) - 멤버 ID
	if is_flexible {
		if r.member_id.len > 0 {
			writer.write_compact_string(r.member_id)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.member_id)
	}

	// MemberEpoch - 멤버 에포크
	writer.write_i32(r.member_epoch)

	// HeartbeatIntervalMs - 하트비트 간격
	writer.write_i32(r.heartbeat_interval_ms)

	// Assignment (nullable) - 파티션 할당
	if assignment := r.assignment {
		// 할당 쓰기
		if is_flexible {
			writer.write_compact_array_len(assignment.topic_partitions.len)
		} else {
			writer.write_array_len(assignment.topic_partitions.len)
		}
		for tp in assignment.topic_partitions {
			// TopicId (UUID) - 토픽 UUID
			writer.write_uuid(tp.topic_id)
			// Partitions - 파티션 목록
			if is_flexible {
				writer.write_compact_array_len(tp.partitions.len)
			} else {
				writer.write_array_len(tp.partitions.len)
			}
			for p in tp.partitions {
				writer.write_i32(p)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	} else {
		// Null 할당
		if is_flexible {
			writer.write_compact_array_len(-1)
		} else {
			writer.write_array_len(-1)
		}
	}

	// 태그된 필드
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 ShareFetchResponse를 바이트로 인코딩합니다.
pub fn (r ShareFetchResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs - 스로틀링 시간
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode - 에러 코드
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable) - 에러 메시지
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// AcquisitionLockTimeoutMs (v1+) - 획득 잠금 타임아웃
	if version >= 1 {
		writer.write_i32(r.acquisition_lock_timeout_ms)
	}

	// Responses 배열 - 토픽별 응답
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId (UUID) - 토픽 UUID
		writer.write_uuid(topic_resp.topic_id)

		// Partitions 배열 - 파티션별 응답
		if is_flexible {
			writer.write_compact_array_len(topic_resp.partitions.len)
		} else {
			writer.write_array_len(topic_resp.partitions.len)
		}
		for part_resp in topic_resp.partitions {
			// PartitionIndex - 파티션 인덱스
			writer.write_i32(part_resp.partition_index)
			// ErrorCode - 에러 코드
			writer.write_i16(part_resp.error_code)
			// ErrorMessage (nullable) - 에러 메시지
			if is_flexible {
				if part_resp.error_message.len > 0 {
					writer.write_compact_string(part_resp.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// AcknowledgeErrorCode - 확인 에러 코드
			writer.write_i16(part_resp.acknowledge_error_code)
			// AcknowledgeErrorMessage (nullable) - 확인 에러 메시지
			if is_flexible {
				if part_resp.acknowledge_error_message.len > 0 {
					writer.write_compact_string(part_resp.acknowledge_error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// CurrentLeader - 현재 리더 정보
			writer.write_i32(part_resp.current_leader.leader_id)
			writer.write_i32(part_resp.current_leader.leader_epoch)
			if is_flexible {
				writer.write_tagged_fields()
			}
			// Records (nullable bytes) - 레코드 배치
			if is_flexible {
				if part_resp.records.len > 0 {
					writer.write_compact_bytes(part_resp.records)
				} else {
					// null을 빈 compact bytes로 쓰기 (unsigned varint에서 length + 1 = 1은 0바이트를 의미)
					writer.write_uvarint(1)
				}
			} else {
				if part_resp.records.len > 0 {
					writer.write_bytes(part_resp.records)
				} else {
					writer.write_i32(-1) // null bytes
				}
			}
			// AcquiredRecords 배열 - 획득된 레코드 범위
			if is_flexible {
				writer.write_compact_array_len(part_resp.acquired_records.len)
			} else {
				writer.write_array_len(part_resp.acquired_records.len)
			}
			for ar in part_resp.acquired_records {
				writer.write_i64(ar.first_offset)
				writer.write_i64(ar.last_offset)
				writer.write_i16(ar.delivery_count)
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// NodeEndpoints 배열 - 노드 엔드포인트 목록
	if is_flexible {
		writer.write_compact_array_len(r.node_endpoints.len)
	} else {
		writer.write_array_len(r.node_endpoints.len)
	}
	for ep in r.node_endpoints {
		writer.write_i32(ep.node_id)
		if is_flexible {
			writer.write_compact_string(ep.host)
		} else {
			writer.write_string(ep.host)
		}
		writer.write_i32(ep.port)
		if is_flexible {
			if ep.rack.len > 0 {
				writer.write_compact_string(ep.rack)
			} else {
				writer.write_compact_nullable_string(none)
			}
		} else {
			writer.write_nullable_string(ep.rack)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode는 ShareAcknowledgeResponse를 바이트로 인코딩합니다.
pub fn (r ShareAcknowledgeResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs - 스로틀링 시간
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode - 에러 코드
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable) - 에러 메시지
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// Responses 배열 - 토픽별 응답
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId - 토픽 UUID
		writer.write_uuid(topic_resp.topic_id)

		// Partitions 배열 - 파티션별 응답
		if is_flexible {
			writer.write_compact_array_len(topic_resp.partitions.len)
		} else {
			writer.write_array_len(topic_resp.partitions.len)
		}
		for part_resp in topic_resp.partitions {
			writer.write_i32(part_resp.partition_index)
			writer.write_i16(part_resp.error_code)
			if is_flexible {
				if part_resp.error_message.len > 0 {
					writer.write_compact_string(part_resp.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// CurrentLeader - 현재 리더 정보
			writer.write_i32(part_resp.current_leader.leader_id)
			writer.write_i32(part_resp.current_leader.leader_epoch)
			if is_flexible {
				writer.write_tagged_fields()
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// NodeEndpoints 배열 - 노드 엔드포인트 목록
	if is_flexible {
		writer.write_compact_array_len(r.node_endpoints.len)
	} else {
		writer.write_array_len(r.node_endpoints.len)
	}
	for ep in r.node_endpoints {
		writer.write_i32(ep.node_id)
		if is_flexible {
			writer.write_compact_string(ep.host)
		} else {
			writer.write_string(ep.host)
		}
		writer.write_i32(ep.port)
		if is_flexible {
			if ep.rack.len > 0 {
				writer.write_compact_string(ep.rack)
			} else {
				writer.write_compact_nullable_string(none)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
