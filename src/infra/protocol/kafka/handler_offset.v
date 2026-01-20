// Kafka Protocol - Offset Operations
// OffsetCommit, OffsetFetch
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain

// OffsetCommit Request
pub struct OffsetCommitRequest {
pub:
	group_id string
	topics   []OffsetCommitRequestTopic
}

pub struct OffsetCommitRequestTopic {
pub:
	name       string
	partitions []OffsetCommitRequestPartition
}

pub struct OffsetCommitRequestPartition {
pub:
	partition_index    i32
	committed_offset   i64
	committed_metadata string
}

fn parse_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetCommitRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	// v1+: generation_id
	if version >= 1 {
		_ = reader.read_i32()!
	}
	// v1+: member_id
	if version >= 1 {
		_ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}
	// v7+: group_instance_id
	if version >= 7 {
		_ = if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
	}
	// v2-v4: retention_time_ms (deprecated, removed in v5)
	if version >= 2 && version <= 4 {
		_ = reader.read_i64()!
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []OffsetCommitRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		pcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut partitions := []OffsetCommitRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			co := reader.read_i64()!
			// v6+: committed_leader_epoch
			if version >= 6 {
				_ = reader.read_i32()!
			}
			// v1-v4: commit_timestamp (deprecated, removed in v5)
			if version >= 1 && version <= 4 {
				_ = reader.read_i64()!
			}
			cm := if is_flexible { reader.read_compact_nullable_string() or { '' } } else { reader.read_nullable_string() or {
					''} }
			partitions << OffsetCommitRequestPartition{
				partition_index:    pi
				committed_offset:   co
				committed_metadata: cm
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
		topics << OffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	return OffsetCommitRequest{
		group_id: group_id
		topics:   topics
	}
}

pub struct OffsetFetchRequest {
pub:
	group_id       string
	topics         []OffsetFetchRequestTopic
	groups         []OffsetFetchRequestGroup
	require_stable bool
}

pub struct OffsetFetchRequestTopic {
pub:
	name       string
	partitions []i32
}

pub struct OffsetFetchRequestGroup {
pub:
	group_id     string
	member_id    ?string
	member_epoch i32
	topics       []OffsetFetchRequestGroupTopic
}

pub struct OffsetFetchRequestGroupTopic {
pub:
	name       string
	partitions []i32
}

fn parse_offset_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetFetchRequest {
	mut group_id := ''
	mut topics := []OffsetFetchRequestTopic{}
	mut groups := []OffsetFetchRequestGroup{}
	mut require_stable := false

	if version <= 7 {
		group_id = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if count >= 0 {
			for _ in 0 .. count {
				name := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				pcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut partitions := []i32{}
				for _ in 0 .. pcount {
					partitions << reader.read_i32()!
				}
				topics << OffsetFetchRequestTopic{
					name:       name
					partitions: partitions
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	} else {
		gcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if gcount >= 0 {
			for _ in 0 .. gcount {
				gid := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				mut member_id := ?string(none)
				mut member_epoch := i32(-1)
				if version >= 9 {
					if is_flexible {
						mid := reader.read_compact_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					} else {
						mid := reader.read_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					}
					member_epoch = reader.read_i32()!
				}
				tcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut gtopics := []OffsetFetchRequestGroupTopic{}
				if tcount >= 0 {
					for _ in 0 .. tcount {
						name := if is_flexible {
							reader.read_compact_string()!
						} else {
							reader.read_string()!
						}
						pcount := if is_flexible {
							reader.read_compact_array_len()!
						} else {
							reader.read_array_len()!
						}
						mut partitions := []i32{}
						for _ in 0 .. pcount {
							partitions << reader.read_i32()!
						}
						gtopics << OffsetFetchRequestGroupTopic{
							name:       name
							partitions: partitions
						}
						if is_flexible {
							reader.skip_tagged_fields()!
						}
					}
				}
				groups << OffsetFetchRequestGroup{
					group_id:     gid
					member_id:    member_id
					member_epoch: member_epoch
					topics:       gtopics
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	}

	// v7+: require_stable
	if version >= 7 {
		require_stable = reader.read_i8()! != 0
	}

	return OffsetFetchRequest{
		group_id:       group_id
		topics:         topics
		groups:         groups
		require_stable: require_stable
	}
}

// ============================================================================
// OffsetCommit Response (API Key 8)
// ============================================================================

pub struct OffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetCommitResponseTopic
}

pub struct OffsetCommitResponseTopic {
pub:
	name       string
	partitions []OffsetCommitResponsePartition
}

pub struct OffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

pub fn (r OffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 8
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}
		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
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

	return writer.bytes()
}

// ============================================================================
// OffsetFetch Response (API Key 9)
// ============================================================================

pub struct OffsetFetchResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetFetchResponseTopic
	error_code       i16
	groups           []OffsetFetchResponseGroup
}

pub struct OffsetFetchResponseTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub struct OffsetFetchResponsePartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     ?string
	error_code             i16
}

pub struct OffsetFetchResponseGroup {
pub:
	group_id   string
	topics     []OffsetFetchResponseGroupTopic
	error_code i16
}

pub struct OffsetFetchResponseGroupTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub fn (r OffsetFetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version >= 8 {
		if is_flexible {
			writer.write_compact_array_len(r.groups.len)
		} else {
			writer.write_array_len(r.groups.len)
		}
		for g in r.groups {
			if is_flexible {
				writer.write_compact_string(g.group_id)
				writer.write_compact_array_len(g.topics.len)
			} else {
				writer.write_string(g.group_id)
				writer.write_array_len(g.topics.len)
			}
			for t in g.topics {
				if is_flexible {
					writer.write_compact_string(t.name)
					writer.write_compact_array_len(t.partitions.len)
				} else {
					writer.write_string(t.name)
					writer.write_array_len(t.partitions.len)
				}
				for p in t.partitions {
					writer.write_i32(p.partition_index)
					writer.write_i64(p.committed_offset)
					writer.write_i32(p.committed_leader_epoch)
					if is_flexible {
						writer.write_compact_nullable_string(p.committed_metadata)
					} else {
						writer.write_nullable_string(p.committed_metadata)
					}
					writer.write_i16(p.error_code)
					if is_flexible {
						writer.write_tagged_fields()
					}
				}
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			writer.write_i16(g.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	} else {
		if is_flexible {
			writer.write_compact_array_len(r.topics.len)
		} else {
			writer.write_array_len(r.topics.len)
		}
		for t in r.topics {
			if is_flexible {
				writer.write_compact_string(t.name)
			} else {
				writer.write_string(t.name)
			}

			if is_flexible {
				writer.write_compact_array_len(t.partitions.len)
			} else {
				writer.write_array_len(t.partitions.len)
			}
			for p in t.partitions {
				writer.write_i32(p.partition_index)
				writer.write_i64(p.committed_offset)
				if version >= 5 {
					writer.write_i32(p.committed_leader_epoch)
				}
				if is_flexible {
					writer.write_compact_nullable_string(p.committed_metadata)
				} else {
					writer.write_nullable_string(p.committed_metadata)
				}
				writer.write_i16(p.error_code)
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if version >= 2 {
			writer.write_i16(r.error_code)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// OffsetCommit handler - persists consumer group offsets
fn (mut h Handler) handle_offset_commit(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_offset_commit_request(mut reader, version, is_flexible_version(.offset_commit,
		version))!

	mut all_offsets := []domain.PartitionOffset{}
	for t in req.topics {
		for p in t.partitions {
			all_offsets << domain.PartitionOffset{
				topic:        t.name
				partition:    int(p.partition_index)
				offset:       p.committed_offset
				leader_epoch: -1
				metadata:     p.committed_metadata
			}
		}
	}

	h.storage.commit_offsets(req.group_id, all_offsets) or {
		mut topics := []OffsetCommitResponseTopic{}
		for t in req.topics {
			mut partitions := []OffsetCommitResponsePartition{}
			for p in t.partitions {
				partitions << OffsetCommitResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_server_error)
				}
			}
			topics << OffsetCommitResponseTopic{
				name:       t.name
				partitions: partitions
			}
		}
		return OffsetCommitResponse{
			throttle_time_ms: 0
			topics:           topics
		}.encode(version)
	}

	mut topics := []OffsetCommitResponseTopic{}
	for t in req.topics {
		mut partitions := []OffsetCommitResponsePartition{}
		for p in t.partitions {
			partitions << OffsetCommitResponsePartition{
				partition_index: p.partition_index
				error_code:      0
			}
		}
		topics << OffsetCommitResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}

	resp := OffsetCommitResponse{
		throttle_time_ms: 0
		topics:           topics
	}

	return resp.encode(version)
}

// OffsetFetch handler - retrieves consumer group offsets
fn (mut h Handler) handle_offset_fetch(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_offset_fetch_request(mut reader, version, is_flexible_version(.offset_fetch,
		version))!

	if version >= 8 {
		mut groups := []OffsetFetchResponseGroup{}

		mut req_groups := req.groups.clone()
		if req_groups.len == 0 && req.group_id.len > 0 {
			mut gtopics := []OffsetFetchRequestGroupTopic{}
			for t in req.topics {
				gtopics << OffsetFetchRequestGroupTopic{
					name:       t.name
					partitions: t.partitions
				}
			}
			req_groups << OffsetFetchRequestGroup{
				group_id:     req.group_id
				member_id:    none
				member_epoch: -1
				topics:       gtopics
			}
		}

		for g in req_groups {
			mut partitions_to_fetch := []domain.TopicPartition{}
			for t in g.topics {
				for p in t.partitions {
					partitions_to_fetch << domain.TopicPartition{
						topic:     t.name
						partition: int(p)
					}
				}
			}

			fetched_offsets := h.storage.fetch_offsets(g.group_id, partitions_to_fetch) or {
				groups << OffsetFetchResponseGroup{
					group_id:   g.group_id
					topics:     []
					error_code: i16(ErrorCode.unknown_server_error)
				}
				continue
			}

			mut topics_map := map[string][]OffsetFetchResponsePartition{}
			for result in fetched_offsets {
				if result.topic !in topics_map {
					topics_map[result.topic] = []
				}
				topics_map[result.topic] << OffsetFetchResponsePartition{
					partition_index:        i32(result.partition)
					committed_offset:       result.offset
					committed_leader_epoch: -1
					committed_metadata:     if result.metadata.len > 0 {
						result.metadata
					} else {
						none
					}
					error_code:             result.error_code
				}
			}

			mut topics := []OffsetFetchResponseGroupTopic{}
			for name, partitions in topics_map {
				topics << OffsetFetchResponseGroupTopic{
					name:       name
					partitions: partitions
				}
			}

			groups << OffsetFetchResponseGroup{
				group_id:   g.group_id
				topics:     topics
				error_code: 0
			}
		}

		resp := OffsetFetchResponse{
			throttle_time_ms: 0
			topics:           []
			error_code:       0
			groups:           groups
		}
		return resp.encode(version)
	}

	// v0-7 behavior
	mut partitions_to_fetch := []domain.TopicPartition{}
	for t in req.topics {
		for p in t.partitions {
			partitions_to_fetch << domain.TopicPartition{
				topic:     t.name
				partition: int(p)
			}
		}
	}

	fetched_offsets := h.storage.fetch_offsets(req.group_id, partitions_to_fetch) or {
		return OffsetFetchResponse{
			throttle_time_ms: 0
			topics:           []
			error_code:       i16(ErrorCode.unknown_server_error)
			groups:           []
		}.encode(version)
	}

	mut topics_map := map[string][]OffsetFetchResponsePartition{}
	for result in fetched_offsets {
		if result.topic !in topics_map {
			topics_map[result.topic] = []
		}
		topics_map[result.topic] << OffsetFetchResponsePartition{
			partition_index:        i32(result.partition)
			committed_offset:       result.offset
			committed_leader_epoch: -1
			committed_metadata:     if result.metadata.len > 0 { result.metadata } else { none }
			error_code:             result.error_code
		}
	}

	mut topics := []OffsetFetchResponseTopic{}
	for name, partitions in topics_map {
		topics << OffsetFetchResponseTopic{
			name:       name
			partitions: partitions
		}
	}

	resp := OffsetFetchResponse{
		throttle_time_ms: 0
		topics:           topics
		error_code:       0
		groups:           []
	}

	return resp.encode(version)
}

// Process functions (Frame-based)
fn (mut h Handler) process_offset_commit(req OffsetCommitRequest, version i16) !OffsetCommitResponse {
	mut all_offsets := []domain.PartitionOffset{}
	for t in req.topics {
		for p in t.partitions {
			all_offsets << domain.PartitionOffset{
				topic:        t.name
				partition:    int(p.partition_index)
				offset:       p.committed_offset
				leader_epoch: -1
				metadata:     p.committed_metadata
			}
		}
	}

	h.storage.commit_offsets(req.group_id, all_offsets) or {
		mut topics := []OffsetCommitResponseTopic{}
		for t in req.topics {
			mut partitions := []OffsetCommitResponsePartition{}
			for p in t.partitions {
				partitions << OffsetCommitResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_server_error)
				}
			}
			topics << OffsetCommitResponseTopic{
				name:       t.name
				partitions: partitions
			}
		}
		return OffsetCommitResponse{
			throttle_time_ms: 0
			topics:           topics
		}
	}

	mut topics := []OffsetCommitResponseTopic{}
	for t in req.topics {
		mut partitions := []OffsetCommitResponsePartition{}
		for p in t.partitions {
			partitions << OffsetCommitResponsePartition{
				partition_index: p.partition_index
				error_code:      0
			}
		}
		topics << OffsetCommitResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}

	return OffsetCommitResponse{
		throttle_time_ms: 0
		topics:           topics
	}
}

fn (mut h Handler) process_offset_fetch(req OffsetFetchRequest, version i16) !OffsetFetchResponse {
	mut partitions_to_fetch := []domain.TopicPartition{}
	for t in req.topics {
		for p in t.partitions {
			partitions_to_fetch << domain.TopicPartition{
				topic:     t.name
				partition: int(p)
			}
		}
	}

	fetched_offsets := h.storage.fetch_offsets(req.group_id, partitions_to_fetch) or {
		return OffsetFetchResponse{
			throttle_time_ms: 0
			topics:           []
			error_code:       i16(ErrorCode.unknown_server_error)
			groups:           []
		}
	}

	mut topics_map := map[string][]OffsetFetchResponsePartition{}
	for result in fetched_offsets {
		if result.topic !in topics_map {
			topics_map[result.topic] = []
		}
		topics_map[result.topic] << OffsetFetchResponsePartition{
			partition_index:        i32(result.partition)
			committed_offset:       result.offset
			committed_leader_epoch: -1
			committed_metadata:     if result.metadata.len > 0 {
				?string(result.metadata)
			} else {
				none
			}
			error_code:             0
		}
	}

	mut topics := []OffsetFetchResponseTopic{}
	for topic_name, parts in topics_map {
		topics << OffsetFetchResponseTopic{
			name:       topic_name
			partitions: parts
		}
	}

	return OffsetFetchResponse{
		throttle_time_ms: 0
		topics:           topics
		error_code:       0
		groups:           []
	}
}
