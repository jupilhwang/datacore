// Kafka Protocol - Share Group Operations (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain
import service.group

// ============================================================================
// ShareGroupHeartbeat (API Key 76)
// ============================================================================

// ShareGroupHeartbeatRequest represents a share group heartbeat request
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

// ShareGroupHeartbeatResponse represents a share group heartbeat response
pub struct ShareGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         string
	member_id             string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ShareGroupAssignment
}

// ShareGroupAssignment represents the partition assignment in heartbeat response
pub struct ShareGroupAssignment {
pub:
	topic_partitions []ShareGroupTopicPartitions
}

// ShareGroupTopicPartitions represents topic partition assignments
pub struct ShareGroupTopicPartitions {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

fn parse_share_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareGroupHeartbeatRequest {
	// GroupId
	group_id := reader.read_flex_string(is_flexible)!

	// MemberId
	member_id := reader.read_flex_string(is_flexible)!

	// MemberEpoch
	member_epoch := reader.read_i32()!

	// RackId (nullable)
	rack_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// SubscribedTopicNames (nullable array)
	mut subscribed_topic_names := []string{}
	topic_count := reader.read_flex_array_len(is_flexible)!
	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			topic := reader.read_flex_string(is_flexible)!
			subscribed_topic_names << topic
		}
	}

	// Skip tagged fields if flexible
	reader.skip_flex_tagged_fields(is_flexible)!

	return ShareGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		rack_id:                rack_id
		subscribed_topic_names: subscribed_topic_names
	}
}

pub fn (r ShareGroupHeartbeatResponse) encode(version i16) []u8 {
	is_flexible := true // Share Group APIs are always flexible
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// MemberId (nullable)
	if is_flexible {
		if r.member_id.len > 0 {
			writer.write_compact_string(r.member_id)
		} else {
			writer.write_compact_nullable_string(none)
		}
	} else {
		writer.write_nullable_string(r.member_id)
	}

	// MemberEpoch
	writer.write_i32(r.member_epoch)

	// HeartbeatIntervalMs
	writer.write_i32(r.heartbeat_interval_ms)

	// Assignment (nullable)
	if assignment := r.assignment {
		// Write assignment
		if is_flexible {
			writer.write_compact_array_len(assignment.topic_partitions.len)
		} else {
			writer.write_array_len(assignment.topic_partitions.len)
		}
		for tp in assignment.topic_partitions {
			// TopicId (UUID)
			writer.write_uuid(tp.topic_id)
			// Partitions
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
		// Null assignment
		if is_flexible {
			writer.write_compact_array_len(-1)
		} else {
			writer.write_array_len(-1)
		}
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// ShareFetch (API Key 78)
// ============================================================================

// ShareFetchRequest represents a share fetch request
pub struct ShareFetchRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	max_wait_ms         i32
	min_bytes           i32
	max_bytes           i32
	max_records         i32
	batch_size          i32
	topics              []ShareFetchTopic
	forgotten_topics    []ShareForgottenTopic
}

// ShareFetchTopic represents topics to fetch in a share fetch request
pub struct ShareFetchTopic {
pub:
	topic_id   []u8 // UUID
	partitions []ShareFetchPartition
}

// ShareFetchPartition represents partitions to fetch
pub struct ShareFetchPartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

// ShareAcknowledgementBatch represents a batch of acknowledgements
pub struct ShareAcknowledgementBatch {
pub:
	first_offset      i64
	last_offset       i64
	acknowledge_types []u8 // 0:Gap, 1:Accept, 2:Release, 3:Reject
}

// ShareForgottenTopic represents topics to remove from session
pub struct ShareForgottenTopic {
pub:
	topic_id   []u8
	partitions []i32
}

// ShareFetchResponse represents a share fetch response
pub struct ShareFetchResponse {
pub:
	throttle_time_ms            i32
	error_code                  i16
	error_message               string
	acquisition_lock_timeout_ms i32
	responses                   []ShareFetchTopicResponse
	node_endpoints              []ShareNodeEndpoint
}

// ShareFetchTopicResponse represents topic responses
pub struct ShareFetchTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareFetchPartitionResponse
}

// ShareFetchPartitionResponse represents partition responses
pub struct ShareFetchPartitionResponse {
pub:
	partition_index           i32
	error_code                i16
	error_message             string
	acknowledge_error_code    i16
	acknowledge_error_message string
	current_leader            ShareLeaderIdAndEpoch
	records                   []u8 // Record batch bytes
	acquired_records          []ShareAcquiredRecords
}

// ShareLeaderIdAndEpoch represents leader info
pub struct ShareLeaderIdAndEpoch {
pub:
	leader_id    i32
	leader_epoch i32
}

// ShareAcquiredRecords represents acquired record ranges
pub struct ShareAcquiredRecords {
pub:
	first_offset   i64
	last_offset    i64
	delivery_count i16
}

// ShareNodeEndpoint represents broker endpoint
pub struct ShareNodeEndpoint {
pub:
	node_id i32
	host    string
	port    i32
	rack    string
}

fn parse_share_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareFetchRequest {
	// GroupId (nullable)
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable)
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch
	share_session_epoch := reader.read_i32()!

	// MaxWaitMs
	max_wait_ms := reader.read_i32()!

	// MinBytes
	min_bytes := reader.read_i32()!

	// MaxBytes
	max_bytes := reader.read_i32()!

	// MaxRecords (v1+)
	mut max_records := i32(0)
	if version >= 1 {
		max_records = reader.read_i32()!
	}

	// BatchSize (v1+)
	mut batch_size := i32(0)
	if version >= 1 {
		batch_size = reader.read_i32()!
	}

	// Topics array
	mut topics := []ShareFetchTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		// TopicId (UUID)
		topic_id := reader.read_uuid()!

		// Partitions array
		mut partitions := []ShareFetchPartition{}
		partitions_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. partitions_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches array
			mut ack_batches := []ShareAcknowledgementBatch{}
			ack_count := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. ack_count {
				first_offset := reader.read_i64()!
				last_offset := reader.read_i64()!

				// AcknowledgeTypes array
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

	// ForgottenTopicsData array
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

pub fn (r ShareFetchResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// AcquisitionLockTimeoutMs (v1+)
	if version >= 1 {
		writer.write_i32(r.acquisition_lock_timeout_ms)
	}

	// Responses array
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId (UUID)
		writer.write_uuid(topic_resp.topic_id)

		// Partitions array
		if is_flexible {
			writer.write_compact_array_len(topic_resp.partitions.len)
		} else {
			writer.write_array_len(topic_resp.partitions.len)
		}
		for part_resp in topic_resp.partitions {
			// PartitionIndex
			writer.write_i32(part_resp.partition_index)
			// ErrorCode
			writer.write_i16(part_resp.error_code)
			// ErrorMessage (nullable)
			if is_flexible {
				if part_resp.error_message.len > 0 {
					writer.write_compact_string(part_resp.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// AcknowledgeErrorCode
			writer.write_i16(part_resp.acknowledge_error_code)
			// AcknowledgeErrorMessage (nullable)
			if is_flexible {
				if part_resp.acknowledge_error_message.len > 0 {
					writer.write_compact_string(part_resp.acknowledge_error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			}
			// CurrentLeader
			writer.write_i32(part_resp.current_leader.leader_id)
			writer.write_i32(part_resp.current_leader.leader_epoch)
			if is_flexible {
				writer.write_tagged_fields()
			}
			// Records (nullable bytes)
			if is_flexible {
				if part_resp.records.len > 0 {
					writer.write_compact_bytes(part_resp.records)
				} else {
					// Write null as empty compact bytes (length 0+1 = 1 in unsigned varint)
					writer.write_uvarint(1) // length + 1 = 1 means 0 bytes
				}
			} else {
				if part_resp.records.len > 0 {
					writer.write_bytes(part_resp.records)
				} else {
					writer.write_i32(-1) // null bytes
				}
			}
			// AcquiredRecords array
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

	// NodeEndpoints array
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

// ============================================================================
// ShareAcknowledge (API Key 79)
// ============================================================================

// ShareAcknowledgeRequest represents a share acknowledge request
pub struct ShareAcknowledgeRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	topics              []ShareAcknowledgeTopic
}

// ShareAcknowledgeTopic represents topics with acknowledgements
pub struct ShareAcknowledgeTopic {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartition
}

// ShareAcknowledgePartition represents partitions with acknowledgements
pub struct ShareAcknowledgePartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

// ShareAcknowledgeResponse represents a share acknowledge response
pub struct ShareAcknowledgeResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    string
	responses        []ShareAcknowledgeTopicResponse
	node_endpoints   []ShareNodeEndpoint
}

// ShareAcknowledgeTopicResponse represents topic responses
pub struct ShareAcknowledgeTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartitionResponse
}

// ShareAcknowledgePartitionResponse represents partition responses
pub struct ShareAcknowledgePartitionResponse {
pub:
	partition_index i32
	error_code      i16
	error_message   string
	current_leader  ShareLeaderIdAndEpoch
}

fn parse_share_acknowledge_request(mut reader BinaryReader, version i16, is_flexible bool) !ShareAcknowledgeRequest {
	// GroupId (nullable)
	group_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// MemberId (nullable)
	member_id := reader.read_flex_nullable_string(is_flexible) or { '' }

	// ShareSessionEpoch
	share_session_epoch := reader.read_i32()!

	// Topics array
	mut topics := []ShareAcknowledgeTopic{}
	topics_count := reader.read_flex_array_len(is_flexible)!
	for _ in 0 .. topics_count {
		topic_id := reader.read_uuid()!

		// Partitions array
		mut partitions := []ShareAcknowledgePartition{}
		parts_count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. parts_count {
			partition_index := reader.read_i32()!

			// AcknowledgementBatches
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

pub fn (r ShareAcknowledgeResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// ErrorCode
	writer.write_i16(r.error_code)

	// ErrorMessage (nullable)
	if is_flexible {
		if r.error_message.len > 0 {
			writer.write_compact_string(r.error_message)
		} else {
			writer.write_compact_nullable_string(none)
		}
	}

	// Responses array
	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}
	for topic_resp in r.responses {
		// TopicId
		writer.write_uuid(topic_resp.topic_id)

		// Partitions array
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
			// CurrentLeader
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

	// NodeEndpoints array
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

// ============================================================================
// Handlers
// ============================================================================

// handle_share_group_heartbeat handles ShareGroupHeartbeat requests (API Key 76)
fn (mut h Handler) handle_share_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := true // Share Group APIs are always flexible
	req := parse_share_group_heartbeat_request(mut reader, version, is_flexible)!

	// Get share group coordinator
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
			[], parts_to_remove) or {}
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
			throttle_time_ms: 0
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
		throttle_time_ms: 0
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
