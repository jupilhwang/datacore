// Kafka 프로토콜 - Share Group 파서 (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// 요청 파싱 함수
module kafka

/// parse_share_group_heartbeat_request는 ShareGroupHeartbeat 요청을 파싱합니다.
fn parse_share_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareGroupHeartbeatRequest {
	// GroupId - 그룹 ID
	group_id := reader.read_flex_string(is_flexible)!

	// MemberId - 멤버 ID
	member_id := reader.read_flex_string(is_flexible)!

	// MemberEpoch - 멤버 에포크
	member_epoch := reader.read_i32()!

	// RackId (nullable) - 랙 ID
	rack_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// SubscribedTopicNames (nullable array) - 구독 토픽 목록
	mut subscribed_topic_names := []string{}
	topic_count := reader.read_flex_array_len(is_flexible)!
	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			topic := reader.read_flex_string(is_flexible)!
			subscribed_topic_names << topic
		}
	}

	// flexible인 경우 태그된 필드 건너뛰기
	reader.skip_flex_tagged_fields(is_flexible)!

	return ShareGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		rack_id:                rack_id
		subscribed_topic_names: subscribed_topic_names
	}
}

/// parse_share_fetch_request는 ShareFetch 요청을 파싱합니다.
fn parse_share_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareFetchRequest {
	// GroupId (nullable) - 그룹 ID
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable) - 멤버 ID
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch - Share 세션 에포크
	share_session_epoch := reader.read_i32()!

	// MaxWaitMs - 최대 대기 시간
	max_wait_ms := reader.read_i32()!

	// MinBytes - 최소 바이트 수
	min_bytes := reader.read_i32()!

	// MaxBytes - 최대 바이트 수
	max_bytes := reader.read_i32()!

	// MaxRecords (v1+) - 최대 레코드 수
	mut max_records := i32(0)
	if version >= 1 {
		max_records = reader.read_i32()!
	}

	// BatchSize (v1+) - 배치 크기
	mut batch_size := i32(0)
	if version >= 1 {
		batch_size = reader.read_i32()!
	}

	// Topics 배열 - 조회할 토픽 목록
	mut topics := []ShareFetchTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID) - 토픽 UUID
		topic_id := reader.read_uuid()!

		// Partitions 배열 - 파티션 목록
		mut partitions := []ShareFetchPartition{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches 배열 - 확인 배치
			mut ack_batches := []ShareAcknowledgementBatch{}
			ack_count := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. ack_count {
				first_offset := reader.read_i64()!
				last_offset := reader.read_i64()!

				// AcknowledgeTypes 배열 - 확인 유형
				mut ack_types := []u8{}
				ack_types_count := reader.read_flex_array_len(is_flexible)!
				for _ in 0 .. ack_types_count {
					ack_types << u8(reader.read_i8()!)
				}
				reader.skip_flex_tagged_fields(is_flexible)!
				ack_batches << ShareAcknowledgementBatch{
					first_offset:      first_offset
					last_offset:       last_offset
					acknowledge_types: ack_types
				}
			}
			reader.skip_flex_tagged_fields(is_flexible)!
			partitions << ShareFetchPartition{
				partition_index:         partition_index
				acknowledgement_batches: ack_batches
			}
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << ShareFetchTopic{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	// ForgottenTopicsData 배열 - 세션에서 제거할 토픽
	mut forgotten_topics := []ShareForgottenTopic{}
	forgotten_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. forgotten_count {
		topic_id := reader.read_uuid()!
		mut parts := []i32{}
		parts_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. parts_count {
			parts << reader.read_i32()!
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		forgotten_topics << ShareForgottenTopic{
			topic_id:   topic_id
			partitions: parts
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return ShareFetchRequest{
		group_id:            group_id
		member_id:           member_id
		share_session_epoch: share_session_epoch
		max_wait_ms:         max_wait_ms
		min_bytes:           min_bytes
		max_bytes:           max_bytes
		max_records:         max_records
		batch_size:          batch_size
		topics:              topics
		forgotten_topics:    forgotten_topics
	}
}

/// parse_share_acknowledge_request는 ShareAcknowledge 요청을 파싱합니다.
fn parse_share_acknowledge_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareAcknowledgeRequest {
	// GroupId (nullable) - 그룹 ID
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable) - 멤버 ID
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch - Share 세션 에포크
	share_session_epoch := reader.read_i32()!

	// Topics 배열 - 확인할 토픽 목록
	mut topics := []ShareAcknowledgeTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		topic_id := reader.read_uuid()!

		// Partitions 배열 - 파티션 목록
		mut partitions := []ShareAcknowledgePartition{}
		parts_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. parts_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches - 확인 배치
			mut ack_batches := []ShareAcknowledgementBatch{}
			ack_count := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. ack_count {
				first_offset := reader.read_i64()!
				last_offset := reader.read_i64()!
				mut ack_types := []u8{}
				ack_types_count := reader.read_flex_array_len(is_flexible)!
				for _ in 0 .. ack_types_count {
					ack_types << u8(reader.read_i8()!)
				}
				reader.skip_flex_tagged_fields(is_flexible)!
				ack_batches << ShareAcknowledgementBatch{
					first_offset:      first_offset
					last_offset:       last_offset
					acknowledge_types: ack_types
				}
			}
			reader.skip_flex_tagged_fields(is_flexible)!
			partitions << ShareAcknowledgePartition{
				partition_index:         partition_index
				acknowledgement_batches: ack_batches
			}
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << ShareAcknowledgeTopic{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return ShareAcknowledgeRequest{
		group_id:            group_id
		member_id:           member_id
		share_session_epoch: share_session_epoch
		topics:              topics
	}
}
