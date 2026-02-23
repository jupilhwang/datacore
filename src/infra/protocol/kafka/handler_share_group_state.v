// Kafka protocol - Share Group State handlers (KIP-932)
// InitializeShareGroupState (API Key 83), ReadShareGroupState (API Key 84),
// WriteShareGroupState (API Key 85), DeleteShareGroupState (API Key 86)
module kafka

import domain

// handle_initialize_share_group_state handles the InitializeShareGroupState request (API Key 83).
// Initializes share partition state when a share group first accesses a partition.
fn (mut h Handler) handle_initialize_share_group_state(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_initialize_share_group_state_request(mut reader, version, is_flexible)!

	if req.group_id.len == 0 {
		resp := InitializeShareGroupStateResponse{
			throttle_time_ms: default_throttle_time_ms
			results:          [
				InitializeShareGroupStateResult{
					partition:     -1
					error_code:    i16(ErrorCode.invalid_group_id)
					error_message: 'group_id must not be empty'
				},
			]
		}
		return resp.encode(version)
	}

	mut coordinator := h.get_share_group_coordinator() or {
		resp := InitializeShareGroupStateResponse{
			throttle_time_ms: default_throttle_time_ms
			results:          [
				InitializeShareGroupStateResult{
					partition:     -1
					error_code:    i16(ErrorCode.coordinator_not_available)
					error_message: 'share group coordinator not available'
				},
			]
		}
		return resp.encode(version)
	}

	mut results := []InitializeShareGroupStateResult{}

	for topic in req.topics {
		// Resolve topic name from topic_id
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or {
			// Return error for all partitions in this topic
			for part in topic.partitions {
				results << InitializeShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.unknown_topic_id)
					error_message: 'unknown topic id'
				}
			}
			continue
		}

		for part in topic.partitions {
			// Validate partition index
			if part.partition < 0 || part.partition >= i32(topic_meta.partition_count) {
				results << InitializeShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.unknown_topic_or_partition)
					error_message: 'invalid partition index'
				}
				continue
			}

			// Initialize the share partition via the coordinator
			_ = coordinator.get_or_create_partition(req.group_id, topic_meta.name, part.partition)

			// Persist the initial state
			state := domain.SharePartitionState{
				group_id:     req.group_id
				topic_name:   topic_meta.name
				partition:    part.partition
				start_offset: part.start_offset
				end_offset:   part.start_offset
			}
			h.storage.save_share_partition_state(state) or {
				results << InitializeShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.kafka_storage_error)
					error_message: 'failed to persist state: ${err}'
				}
				continue
			}

			results << InitializeShareGroupStateResult{
				partition:     part.partition
				error_code:    i16(ErrorCode.none)
				error_message: ''
			}
		}
	}

	resp := InitializeShareGroupStateResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}
	return resp.encode(version)
}

// handle_read_share_group_state handles the ReadShareGroupState request (API Key 84).
// Reads the current state of share partitions.
fn (mut h Handler) handle_read_share_group_state(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_read_share_group_state_request(mut reader, version, is_flexible)!

	if req.group_id.len == 0 {
		resp := ReadShareGroupStateResponse{
			throttle_time_ms: default_throttle_time_ms
			results:          []
		}
		return resp.encode(version)
	}

	mut topic_results := []ReadShareGroupStateTopicResult{}

	for topic in req.topics {
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or {
			// Return error results for all partitions
			mut part_results := []ReadShareGroupStatePartitionResult{}
			for part in topic.partitions {
				part_results << ReadShareGroupStatePartitionResult{
					partition:     part.partition
					state_epoch:   0
					start_offset:  -1
					state_batches: []
					error_code:    i16(ErrorCode.unknown_topic_id)
					error_message: 'unknown topic id'
				}
			}
			topic_results << ReadShareGroupStateTopicResult{
				topic_id:   topic.topic_id
				partitions: part_results
			}
			continue
		}

		mut part_results := []ReadShareGroupStatePartitionResult{}

		for part in topic.partitions {
			// Load persisted state from storage
			state := h.storage.load_share_partition_state(req.group_id, topic_meta.name,
				part.partition) or {
				// No state found - return empty
				part_results << ReadShareGroupStatePartitionResult{
					partition:     part.partition
					state_epoch:   0
					start_offset:  -1
					state_batches: []
					error_code:    i16(ErrorCode.group_id_not_found)
					error_message: 'share partition state not found'
				}
				continue
			}

			// Convert record states to StateBatch entries
			state_batches := convert_state_to_batches(state)

			part_results << ReadShareGroupStatePartitionResult{
				partition:     part.partition
				state_epoch:   0
				start_offset:  state.start_offset
				state_batches: state_batches
				error_code:    i16(ErrorCode.none)
				error_message: ''
			}
		}

		topic_results << ReadShareGroupStateTopicResult{
			topic_id:   topic.topic_id
			partitions: part_results
		}
	}

	resp := ReadShareGroupStateResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          topic_results
	}
	return resp.encode(version)
}

// handle_write_share_group_state handles the WriteShareGroupState request (API Key 85).
// Writes updated share partition state.
fn (mut h Handler) handle_write_share_group_state(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_write_share_group_state_request(mut reader, version, is_flexible)!

	if req.group_id.len == 0 {
		resp := WriteShareGroupStateResponse{
			throttle_time_ms: default_throttle_time_ms
			results:          [
				WriteShareGroupStateResult{
					partition:     -1
					error_code:    i16(ErrorCode.invalid_group_id)
					error_message: 'group_id must not be empty'
				},
			]
		}
		return resp.encode(version)
	}

	mut results := []WriteShareGroupStateResult{}

	for topic in req.topics {
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or {
			for part in topic.partitions {
				results << WriteShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.unknown_topic_id)
					error_message: 'unknown topic id'
				}
			}
			continue
		}

		for part in topic.partitions {
			// Validate partition index
			if part.partition < 0 || part.partition >= i32(topic_meta.partition_count) {
				results << WriteShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.unknown_topic_or_partition)
					error_message: 'invalid partition index'
				}
				continue
			}

			// Convert StateBatch entries to domain record states
			mut record_states := map[i64]u8{}
			for batch in part.state_batches {
				for offset := batch.first_offset; offset <= batch.last_offset; offset++ {
					record_states[offset] = u8(batch.delivery_state)
				}
			}

			// Build and persist the share partition state
			state := domain.SharePartitionState{
				group_id:      req.group_id
				topic_name:    topic_meta.name
				partition:     part.partition
				start_offset:  part.start_offset
				end_offset:    compute_end_offset(part.state_batches, part.start_offset)
				record_states: record_states
			}
			h.storage.save_share_partition_state(state) or {
				results << WriteShareGroupStateResult{
					partition:     part.partition
					error_code:    i16(ErrorCode.kafka_storage_error)
					error_message: 'failed to persist state: ${err}'
				}
				continue
			}

			results << WriteShareGroupStateResult{
				partition:     part.partition
				error_code:    i16(ErrorCode.none)
				error_message: ''
			}
		}
	}

	resp := WriteShareGroupStateResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}
	return resp.encode(version)
}

// handle_delete_share_group_state handles the DeleteShareGroupState request (API Key 86).
// Deletes share partition state when cleaning up a share group.
fn (mut h Handler) handle_delete_share_group_state(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_delete_share_group_state_request(mut reader, version, is_flexible)!

	if req.group_id.len == 0 {
		resp := DeleteShareGroupStateResponse{
			throttle_time_ms: default_throttle_time_ms
			results:          [
				DeleteShareGroupStateResult{
					partition:     -1
					error_code:    i16(ErrorCode.invalid_group_id)
					error_message: 'group_id must not be empty'
				},
			]
		}
		return resp.encode(version)
	}

	mut results := []DeleteShareGroupStateResult{}

	for topic in req.topics {
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or {
			for partition in topic.partitions {
				results << DeleteShareGroupStateResult{
					partition:     partition
					error_code:    i16(ErrorCode.unknown_topic_id)
					error_message: 'unknown topic id'
				}
			}
			continue
		}

		for partition in topic.partitions {
			h.storage.delete_share_partition_state(req.group_id, topic_meta.name, partition) or {
				results << DeleteShareGroupStateResult{
					partition:     partition
					error_code:    i16(ErrorCode.kafka_storage_error)
					error_message: 'failed to delete state: ${err}'
				}
				continue
			}

			results << DeleteShareGroupStateResult{
				partition:     partition
				error_code:    i16(ErrorCode.none)
				error_message: ''
			}
		}
	}

	resp := DeleteShareGroupStateResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}
	return resp.encode(version)
}

// --- Helper functions ---

// convert_state_to_batches converts a SharePartitionState's record states into
// a compact list of StateBatch entries by grouping consecutive offsets with the
// same delivery state.
fn convert_state_to_batches(state domain.SharePartitionState) []StateBatch {
	if state.record_states.len == 0 {
		return []
	}

	// Collect offsets and sort them
	mut offsets := []i64{}
	for offset, _ in state.record_states {
		offsets << offset
	}
	offsets.sort(a < b)

	mut batches := []StateBatch{}
	mut first_offset := offsets[0]
	mut last_offset := offsets[0]
	mut current_state := i8(state.record_states[offsets[0]])
	mut current_delivery := delivery_count_for_offset(state, offsets[0])

	for i := 1; i < offsets.len; i++ {
		offset := offsets[i]
		offset_state := i8(state.record_states[offset])
		offset_delivery := delivery_count_for_offset(state, offset)

		if offset == last_offset + 1 && offset_state == current_state
			&& offset_delivery == current_delivery {
			// Extend current batch
			last_offset = offset
		} else {
			// Flush current batch and start a new one
			batches << StateBatch{
				first_offset:   first_offset
				last_offset:    last_offset
				delivery_state: current_state
				delivery_count: current_delivery
			}
			first_offset = offset
			last_offset = offset
			current_state = offset_state
			current_delivery = offset_delivery
		}
	}

	// Flush last batch
	batches << StateBatch{
		first_offset:   first_offset
		last_offset:    last_offset
		delivery_state: current_state
		delivery_count: current_delivery
	}

	return batches
}

// delivery_count_for_offset returns the delivery count for a specific offset
// from the acquired records in a SharePartitionState.
fn delivery_count_for_offset(state domain.SharePartitionState, offset i64) i16 {
	if acquired := state.acquired_records[offset] {
		return i16(acquired.delivery_count)
	}
	return 0
}

// compute_end_offset calculates the end offset from state batches.
fn compute_end_offset(batches []StateBatch, start_offset i64) i64 {
	mut end := start_offset
	for batch in batches {
		if batch.last_offset >= end {
			end = batch.last_offset + 1
		}
	}
	return end
}
