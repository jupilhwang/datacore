// Kafka protocol - Share Group parser (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// Request parsing functions
module kafka

/// parse_share_group_heartbeat_request parses a ShareGroupHeartbeat request.
fn parse_share_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareGroupHeartbeatRequest {
	// GroupId - group ID
	group_id := reader.read_flex_string(is_flexible)!

	// MemberId - member ID
	member_id := reader.read_flex_string(is_flexible)!

	// MemberEpoch - member epoch
	member_epoch := reader.read_i32()!

	// RackId (nullable) - rack ID
	rack_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// SubscribedTopicNames (nullable array) - subscribed topic list
	mut subscribed_topic_names := []string{}
	topic_count := reader.read_flex_array_len(is_flexible)!
	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			topic := reader.read_flex_string(is_flexible)!
			subscribed_topic_names << topic
		}
	}

	// skip tagged fields if flexible
	reader.skip_flex_tagged_fields(is_flexible)!

	return ShareGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		rack_id:                rack_id
		subscribed_topic_names: subscribed_topic_names
	}
}

/// parse_share_fetch_request parses a ShareFetch request.
fn parse_share_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareFetchRequest {
	// GroupId (nullable) - group ID
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable) - member ID
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch - share session epoch
	share_session_epoch := reader.read_i32()!

	// MaxWaitMs - maximum wait time in milliseconds
	max_wait_ms := reader.read_i32()!

	// MinBytes - minimum byte count
	min_bytes := reader.read_i32()!

	// MaxBytes - maximum byte count
	max_bytes := reader.read_i32()!

	// MaxRecords (v1+) - maximum record count
	mut max_records := i32(0)
	if version >= 1 {
		max_records = reader.read_i32()!
	}

	// BatchSize (v1+) - batch size
	mut batch_size := i32(0)
	if version >= 1 {
		batch_size = reader.read_i32()!
	}

	// Topics array - list of topics to fetch
	mut topics := []ShareFetchTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID) - topic UUID
		topic_id := reader.read_uuid()!

		// Partitions array - partition list
		mut partitions := []ShareFetchPartition{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches array - acknowledgement batches
			mut ack_batches := []ShareAcknowledgementBatch{}
			ack_count := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. ack_count {
				first_offset := reader.read_i64()!
				last_offset := reader.read_i64()!

				// AcknowledgeTypes array - acknowledgement types
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

	// ForgottenTopicsData array - topics to remove from the session
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

/// parse_share_acknowledge_request parses a ShareAcknowledge request.
fn parse_share_acknowledge_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareAcknowledgeRequest {
	// GroupId (nullable) - group ID
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable) - member ID
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch - share session epoch
	share_session_epoch := reader.read_i32()!

	// Topics array - list of topics to acknowledge
	mut topics := []ShareAcknowledgeTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		topic_id := reader.read_uuid()!

		// Partitions array - partition list
		mut partitions := []ShareAcknowledgePartition{}
		parts_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. parts_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches - acknowledgement batches
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
