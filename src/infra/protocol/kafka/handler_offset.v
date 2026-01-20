// Offset handlers - OffsetCommit, OffsetFetch
module kafka

import domain

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

// Process functions (Frame-based stubs)
fn (mut h Handler) process_offset_commit(req OffsetCommitRequest, version i16) !OffsetCommitResponse {
	return OffsetCommitResponse{
		throttle_time_ms: 0
		topics:           []
	}
}

fn (mut h Handler) process_offset_fetch(req OffsetFetchRequest, version i16) !OffsetFetchResponse {
	if version >= 8 {
		return OffsetFetchResponse{
			throttle_time_ms: 0
			topics:           []
			error_code:       0
			groups:           []
		}
	}
	return OffsetFetchResponse{
		throttle_time_ms: 0
		topics:           []
		error_code:       0
		groups:           []
	}
}
