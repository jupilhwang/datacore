// Kafka protocol - Share Group handlers (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
module kafka

import domain
import service.group

// Handler

// handle_share_group_heartbeat handles the ShareGroupHeartbeat request (API Key 76)
fn (mut h Handler) handle_share_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_share_group_heartbeat_request(mut reader, version, is_flexible)!

	// Get Share Group coordinator
	mut coordinator := h.get_share_group_coordinator() or {
		resp := ShareGroupHeartbeatResponse{
			throttle_time_ms:      0
			error_code:            i16(ErrorCode.coordinator_not_available)
			error_message:         'share group coordinator not available'
			member_id:             req.member_id
			member_epoch:          0
			heartbeat_interval_ms: 5000
			assignment:            none
		}
		return resp.encode(version)
	}

	// Process heartbeat
	hb_req := group.ShareGroupHeartbeatRequest{
		group_id:               req.group_id
		member_id:              req.member_id
		member_epoch:           req.member_epoch
		rack_id:                req.rack_id
		subscribed_topic_names: req.subscribed_topic_names
	}
	hb_resp := coordinator.heartbeat(hb_req)

	// Convert assignment to response format
	mut assignment := ?ShareGroupAssignment(none)
	if hb_resp.assignment.len > 0 {
		mut topic_parts := []ShareGroupTopicPartitions{}
		for a in hb_resp.assignment {
			topic_parts << ShareGroupTopicPartitions{
				topic_id:   a.topic_id
				partitions: a.partitions
			}
		}
		assignment = ShareGroupAssignment{
			topic_partitions: topic_parts
		}
	}

	resp := ShareGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            hb_resp.error_code
		error_message:         hb_resp.error_message
		member_id:             hb_resp.member_id
		member_epoch:          hb_resp.member_epoch
		heartbeat_interval_ms: hb_resp.heartbeat_interval
		assignment:            assignment
	}

	return resp.encode(version)
}

// handle_share_fetch handles ShareFetch requests (API Key 78)
fn (mut h Handler) handle_share_fetch(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_share_fetch_request(mut reader, version, is_flexible)!

	// Get share group coordinator
	mut coordinator := h.get_share_group_coordinator() or {
		resp := ShareFetchResponse{
			throttle_time_ms:            0
			error_code:                  i16(ErrorCode.coordinator_not_available)
			error_message:               'share group coordinator not available'
			acquisition_lock_timeout_ms: 30000
			responses:                   []
			node_endpoints:              []
		}
		return resp.encode(version)
	}

	mut responses := []ShareFetchTopicResponse{}

	// Process each topic
	for topic in req.topics {
		// Get topic name from topic_id
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or { continue }
		topic_name := topic_meta.name

		mut partition_responses := []ShareFetchPartitionResponse{}

		for part in topic.partitions {
			// First process any acknowledgements
			mut ack_error_code := i16(0)
			mut ack_error_msg := ''
			for ack_batch in part.acknowledgement_batches {
				// Convert ack types to domain types
				ack_type := if ack_batch.acknowledge_types.len > 0 {
					match ack_batch.acknowledge_types[0] {
						1 { domain.AcknowledgeType.accept }
						2 { domain.AcknowledgeType.release }
						3 { domain.AcknowledgeType.reject }
						else { domain.AcknowledgeType.accept }
					}
				} else {
					domain.AcknowledgeType.accept
				}

				batch := domain.AcknowledgementBatch{
					topic_name:       topic_name
					partition:        part.partition_index
					first_offset:     ack_batch.first_offset
					last_offset:      ack_batch.last_offset
					acknowledge_type: ack_type
					gap_offsets:      []i64{}
				}
				result := coordinator.acknowledge_records(req.group_id, req.member_id,
					batch)
				if result.error_code != 0 {
					ack_error_code = result.error_code
					ack_error_msg = result.error_message
				}
			}

			// Then acquire new records
			max_records := if req.max_records > 0 { int(req.max_records) } else { 100 }
			acquired := coordinator.acquire_records(req.group_id, req.member_id, topic_name,
				part.partition_index, max_records)

			// Convert to response format
			mut acquired_records := []ShareAcquiredRecords{}
			if acquired.len > 0 {
				// Group consecutive offsets into ranges
				mut first_offset := acquired[0].offset
				mut last_offset := acquired[0].offset
				mut delivery_count := i16(acquired[0].delivery_count)

				for i, ar in acquired {
					if i == 0 {
						continue
					}
					if ar.offset == last_offset + 1 && ar.delivery_count == i32(delivery_count) {
						last_offset = ar.offset
					} else {
						acquired_records << ShareAcquiredRecords{
							first_offset:   first_offset
							last_offset:    last_offset
							delivery_count: delivery_count
						}
						first_offset = ar.offset
						last_offset = ar.offset
						delivery_count = i16(ar.delivery_count)
					}
				}
				// Add last range
				acquired_records << ShareAcquiredRecords{
					first_offset:   first_offset
					last_offset:    last_offset
					delivery_count: delivery_count
				}
			}

			// Fetch actual records
			mut records := []u8{}
			if acquired.len > 0 {
				// Fetch records from storage
				first := acquired[0].offset
				if fetched := h.storage.fetch(topic_name, part.partition_index, first,
					req.max_bytes)
				{
					// Convert records to bytes (would need record serialization)
					// For now, we just return empty records - actual implementation would serialize
					_ = fetched
					records = []u8{}
				}
			}

			partition_responses << ShareFetchPartitionResponse{
				partition_index:           part.partition_index
				error_code:                0
				error_message:             ''
				acknowledge_error_code:    ack_error_code
				acknowledge_error_message: ack_error_msg
				current_leader:            ShareLeaderIdAndEpoch{
					leader_id:    h.broker_id
					leader_epoch: 0
				}
				records:                   records
				acquired_records:          acquired_records
			}
		}

		responses << ShareFetchTopicResponse{
			topic_id:   topic.topic_id
			partitions: partition_responses
		}
	}

	// Handle forgotten topics (remove from session)
	for forgotten in req.forgotten_topics {
		topic_meta := h.storage.get_topic_by_id(forgotten.topic_id) or { continue }
		topic_name := topic_meta.name
		// Remove partitions from session
		mut parts_to_remove := []domain.ShareSessionPartition{}
		for p in forgotten.partitions {
			parts_to_remove << domain.ShareSessionPartition{
				topic_id:   forgotten.topic_id
				topic_name: topic_name
				partition:  p
			}
		}
		coordinator.update_session(req.group_id, req.member_id, req.share_session_epoch,
			[], parts_to_remove) or {
			h.logger.warn('Failed to update share session for forgotten topic group_id=${req.group_id} member_id=${req.member_id} error=${err.str()}')
		}
	}

	resp := ShareFetchResponse{
		throttle_time_ms:            0
		error_code:                  0
		error_message:               ''
		acquisition_lock_timeout_ms: 30000
		responses:                   responses
		node_endpoints:              []
	}

	return resp.encode(version)
}

// handle_share_acknowledge handles ShareAcknowledge requests (API Key 79)
fn (mut h Handler) handle_share_acknowledge(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true
	req := parse_share_acknowledge_request(mut reader, version, is_flexible)!

	// Get share group coordinator
	mut coordinator := h.get_share_group_coordinator() or {
		resp := ShareAcknowledgeResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.coordinator_not_available)
			error_message:    'share group coordinator not available'
			responses:        []
			node_endpoints:   []
		}
		return resp.encode(version)
	}

	mut responses := []ShareAcknowledgeTopicResponse{}

	// Process each topic
	for topic in req.topics {
		topic_meta := h.storage.get_topic_by_id(topic.topic_id) or { continue }
		topic_name := topic_meta.name

		mut partition_responses := []ShareAcknowledgePartitionResponse{}

		for part in topic.partitions {
			mut error_code := i16(0)
			mut error_msg := ''

			for ack_batch in part.acknowledgement_batches {
				// Determine acknowledge type from the array
				// AcknowledgeTypes: 0:Gap, 1:Accept, 2:Release, 3:Reject
				for offset := ack_batch.first_offset; offset <= ack_batch.last_offset; offset++ {
					idx := int(offset - ack_batch.first_offset)
					ack_type_val := if idx < ack_batch.acknowledge_types.len {
						ack_batch.acknowledge_types[idx]
					} else if ack_batch.acknowledge_types.len > 0 {
						ack_batch.acknowledge_types[0]
					} else {
						u8(1) // Default to Accept
					}

					// Skip gaps (type 0)
					if ack_type_val == 0 {
						continue
					}

					ack_type := match ack_type_val {
						1 { domain.AcknowledgeType.accept }
						2 { domain.AcknowledgeType.release }
						3 { domain.AcknowledgeType.reject }
						else { domain.AcknowledgeType.accept }
					}

					batch := domain.AcknowledgementBatch{
						topic_name:       topic_name
						partition:        part.partition_index
						first_offset:     offset
						last_offset:      offset
						acknowledge_type: ack_type
						gap_offsets:      []i64{}
					}
					result := coordinator.acknowledge_records(req.group_id, req.member_id,
						batch)
					if result.error_code != 0 {
						error_code = result.error_code
						error_msg = result.error_message
					}
				}
			}

			partition_responses << ShareAcknowledgePartitionResponse{
				partition_index: part.partition_index
				error_code:      error_code
				error_message:   error_msg
				current_leader:  ShareLeaderIdAndEpoch{
					leader_id:    h.broker_id
					leader_epoch: 0
				}
			}
		}

		responses << ShareAcknowledgeTopicResponse{
			topic_id:   topic.topic_id
			partitions: partition_responses
		}
	}

	resp := ShareAcknowledgeResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		error_message:    ''
		responses:        responses
		node_endpoints:   []
	}

	return resp.encode(version)
}

// get_share_group_coordinator returns the share group coordinator if available
fn (mut h Handler) get_share_group_coordinator() ?&group.ShareGroupCoordinator {
	return h.share_group_coordinator
}
