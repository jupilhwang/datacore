// Kafka protocol - Share Group State parser (KIP-932)
// InitializeShareGroupState (API Key 83), ReadShareGroupState (API Key 84),
// WriteShareGroupState (API Key 85), DeleteShareGroupState (API Key 86)
// Request parsing functions.
module kafka

/// parse_initialize_share_group_state_request parses an InitializeShareGroupState request (API Key 83).
fn parse_initialize_share_group_state_request(mut reader BinaryReader, version i16, is_flexible bool) !InitializeShareGroupStateRequest {
	// GroupId
	group_id := reader.read_flex_string(is_flexible)!

	// Topics array
	mut topics := []InitializeShareGroupStateTopicData{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID)
		topic_id := reader.read_uuid()!

		// Partitions array
		mut partitions := []InitializeShareGroupStatePartitionData{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition := reader.read_i32()!
			state_epoch := reader.read_i32()!
			start_offset := reader.read_i64()!
			reader.skip_flex_tagged_fields(is_flexible)!
			partitions << InitializeShareGroupStatePartitionData{
				partition:    partition
				state_epoch:  state_epoch
				start_offset: start_offset
			}
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << InitializeShareGroupStateTopicData{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return InitializeShareGroupStateRequest{
		group_id: group_id
		topics:   topics
	}
}

/// parse_read_share_group_state_request parses a ReadShareGroupState request (API Key 84).
fn parse_read_share_group_state_request(mut reader BinaryReader, version i16, is_flexible bool) !ReadShareGroupStateRequest {
	// GroupId
	group_id := reader.read_flex_string(is_flexible)!

	// Topics array
	mut topics := []ReadShareGroupStateTopicData{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID)
		topic_id := reader.read_uuid()!

		// Partitions array
		mut partitions := []ReadShareGroupStatePartitionData{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition := reader.read_i32()!
			leader_epoch := reader.read_i32()!
			reader.skip_flex_tagged_fields(is_flexible)!
			partitions << ReadShareGroupStatePartitionData{
				partition:    partition
				leader_epoch: leader_epoch
			}
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << ReadShareGroupStateTopicData{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return ReadShareGroupStateRequest{
		group_id: group_id
		topics:   topics
	}
}

/// parse_write_share_group_state_request parses a WriteShareGroupState request (API Key 85).
fn parse_write_share_group_state_request(mut reader BinaryReader, version i16, is_flexible bool) !WriteShareGroupStateRequest {
	// GroupId
	group_id := reader.read_flex_string(is_flexible)!

	// Topics array
	mut topics := []WriteShareGroupStateTopicData{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID)
		topic_id := reader.read_uuid()!

		// Partitions array
		mut partitions := []WriteShareGroupStatePartitionData{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition := reader.read_i32()!
			state_epoch := reader.read_i32()!
			leader_epoch := reader.read_i32()!
			start_offset := reader.read_i64()!
			delivery_complete_count := reader.read_i32()!

			// StateBatches array
			mut state_batches := []StateBatch{}
			batches_count := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. batches_count {
				first_offset := reader.read_i64()!
				last_offset := reader.read_i64()!
				delivery_state := reader.read_i8()!
				delivery_count := reader.read_i16()!
				reader.skip_flex_tagged_fields(is_flexible)!
				state_batches << StateBatch{
					first_offset:   first_offset
					last_offset:    last_offset
					delivery_state: delivery_state
					delivery_count: delivery_count
				}
			}

			reader.skip_flex_tagged_fields(is_flexible)!
			partitions << WriteShareGroupStatePartitionData{
				partition:               partition
				state_epoch:             state_epoch
				leader_epoch:            leader_epoch
				start_offset:            start_offset
				delivery_complete_count: delivery_complete_count
				state_batches:           state_batches
			}
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << WriteShareGroupStateTopicData{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return WriteShareGroupStateRequest{
		group_id: group_id
		topics:   topics
	}
}

/// parse_delete_share_group_state_request parses a DeleteShareGroupState request (API Key 86).
fn parse_delete_share_group_state_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteShareGroupStateRequest {
	// GroupId
	group_id := reader.read_flex_string(is_flexible)!

	// Topics array
	mut topics := []DeleteShareGroupStateTopicData{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID)
		topic_id := reader.read_uuid()!

		// Partitions array (simple INT32 array)
		mut partitions := []i32{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partitions << reader.read_i32()!
		}
		reader.skip_flex_tagged_fields(is_flexible)!
		topics << DeleteShareGroupStateTopicData{
			topic_id:   topic_id
			partitions: partitions
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return DeleteShareGroupStateRequest{
		group_id: group_id
		topics:   topics
	}
}
