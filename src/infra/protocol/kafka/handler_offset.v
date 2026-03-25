// Kafka protocol - Offset operations
// OffsetCommit, OffsetFetch
// Request/response types, parsing, encoding, and handlers
module kafka

import domain
import infra.observability
import service.port
import time

// OffsetCommit request
/// OffsetCommitRequest holds the request data for OffsetCommit.
pub struct OffsetCommitRequest {
pub:
	group_id string
	topics   []OffsetCommitRequestTopic
}

/// OffsetCommitRequestTopic holds the topic and partitions for an OffsetCommit request.
pub struct OffsetCommitRequestTopic {
pub:
	name       string
	partitions []OffsetCommitRequestPartition
}

/// OffsetCommitRequestPartition holds the partition offset data for an OffsetCommit request.
pub struct OffsetCommitRequestPartition {
pub:
	partition_index    i32
	committed_offset   i64
	committed_metadata string
}

fn parse_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetCommitRequest {
	group_id := reader.read_flex_string(is_flexible)!
	// v1+: generation_id
	if version >= 1 {
		_ = reader.read_i32()!
	}
	// v1+: member_id
	if version >= 1 {
		_ = reader.read_flex_string(is_flexible)!
	}
	// v7+: group_instance_id
	if version >= 7 {
		_ = reader.read_flex_nullable_string(is_flexible)!
	}
	// v2-v4: retention_time_ms (deprecated, removed in v5)
	if version >= 2 && version <= 4 {
		_ = reader.read_i64()!
	}

	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []OffsetCommitRequestTopic{}
	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!
		pcount := reader.read_flex_array_len(is_flexible)!
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
			cm := reader.read_flex_nullable_string(is_flexible) or { '' }
			partitions << OffsetCommitRequestPartition{
				partition_index:    pi
				committed_offset:   co
				committed_metadata: cm
			}
			reader.skip_flex_tagged_fields(is_flexible)!
		}
		topics << OffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}
		reader.skip_flex_tagged_fields(is_flexible)!
	}
	return OffsetCommitRequest{
		group_id: group_id
		topics:   topics
	}
}

/// OffsetFetchRequest holds the request data for OffsetFetch.
pub struct OffsetFetchRequest {
pub:
	group_id       string
	topics         []OffsetFetchRequestTopic
	groups         []OffsetFetchRequestGroup
	require_stable bool
}

/// OffsetFetchRequestTopic holds the topic and partition indices to fetch offsets for.
pub struct OffsetFetchRequestTopic {
pub:
	name       string
	partitions []i32
}

/// OffsetFetchRequestGroup holds the group and topic data for a v8+ OffsetFetch request.
pub struct OffsetFetchRequestGroup {
pub:
	group_id     string
	member_id    ?string
	member_epoch i32
	topics       []OffsetFetchRequestGroupTopic
}

/// OffsetFetchRequestGroupTopic holds the topic and partitions within a group-level OffsetFetch request.
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
		group_id = reader.read_flex_string(is_flexible)!
		count := reader.read_flex_array_len(is_flexible)!
		if count >= 0 {
			for _ in 0 .. count {
				name := reader.read_flex_string(is_flexible)!
				pcount := reader.read_flex_array_len(is_flexible)!
				mut partitions := []i32{}
				for _ in 0 .. pcount {
					partitions << reader.read_i32()!
				}
				topics << OffsetFetchRequestTopic{
					name:       name
					partitions: partitions
				}
				reader.skip_flex_tagged_fields(is_flexible)!
			}
		}
	} else {
		gcount := reader.read_flex_array_len(is_flexible)!
		if gcount >= 0 {
			for _ in 0 .. gcount {
				gid := reader.read_flex_string(is_flexible)!
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
				tcount := reader.read_flex_array_len(is_flexible)!
				mut gtopics := []OffsetFetchRequestGroupTopic{}
				if tcount >= 0 {
					for _ in 0 .. tcount {
						name := reader.read_flex_string(is_flexible)!
						pcount := reader.read_flex_array_len(is_flexible)!
						mut partitions := []i32{}
						for _ in 0 .. pcount {
							partitions << reader.read_i32()!
						}
						gtopics << OffsetFetchRequestGroupTopic{
							name:       name
							partitions: partitions
						}
						reader.skip_flex_tagged_fields(is_flexible)!
					}
				}
				groups << OffsetFetchRequestGroup{
					group_id:     gid
					member_id:    member_id
					member_epoch: member_epoch
					topics:       gtopics
				}
				reader.skip_flex_tagged_fields(is_flexible)!
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

// OffsetCommit Response (API Key 8)

/// OffsetCommitResponse holds the response data for OffsetCommit (API Key 8).
pub struct OffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetCommitResponseTopic
}

/// OffsetCommitResponseTopic holds the per-topic results for an OffsetCommit response.
pub struct OffsetCommitResponseTopic {
pub:
	name       string
	partitions []OffsetCommitResponsePartition
}

/// OffsetCommitResponsePartition holds the per-partition result for an OffsetCommit response.
pub struct OffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

/// encode serializes the OffsetCommitResponse into bytes.
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

// OffsetFetch Response (API Key 9)

/// OffsetFetchResponse holds the response data for OffsetFetch (API Key 9).
pub struct OffsetFetchResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetFetchResponseTopic
	error_code       i16
	groups           []OffsetFetchResponseGroup
}

/// OffsetFetchResponseTopic holds the per-topic results for an OffsetFetch response.
pub struct OffsetFetchResponseTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

/// OffsetFetchResponsePartition holds the per-partition committed offset for an OffsetFetch response.
pub struct OffsetFetchResponsePartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     ?string
	error_code             i16
}

/// OffsetFetchResponseGroup holds the per-group results for a v8+ OffsetFetch response.
pub struct OffsetFetchResponseGroup {
pub:
	group_id   string
	topics     []OffsetFetchResponseGroupTopic
	error_code i16
}

/// OffsetFetchResponseGroupTopic holds the per-topic partition offsets within a group OffsetFetch response.
pub struct OffsetFetchResponseGroupTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

/// encode serializes the OffsetFetchResponse into bytes.
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

// handle_offset_commit - commits consumer group offsets
fn (mut h Handler) handle_offset_commit(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_offset_commit_request(mut reader, version, is_flexible_version(.offset_commit,
		version))!

	h.logger.debug('Processing offset commit', observability.field_string('group_id',
		req.group_id), observability.field_int('topics', req.topics.len))

	// Convert protocol request to service request
	mut all_offsets := []domain.PartitionOffset{cap: req.topics.len * 4}
	mut total_partitions := 0
	for t in req.topics {
		for p in t.partitions {
			total_partitions += 1
			all_offsets << domain.PartitionOffset{
				topic:        t.name
				partition:    int(p.partition_index)
				offset:       p.committed_offset
				leader_epoch: -1
				metadata:     p.committed_metadata
			}
		}
	}

	// Commit offsets via OffsetManager
	service_resp := h.offset_manager.commit_offsets(port.OffsetCommitRequest{
		group_id: req.group_id
		offsets:  all_offsets
	}) or {
		h.logger.error('Offset commit failed', observability.field_string('group_id',
			req.group_id), observability.field_string('error', err.str()))

		// Build error response
		mut topics := []OffsetCommitResponseTopic{cap: req.topics.len}
		for t in req.topics {
			mut partitions := []OffsetCommitResponsePartition{cap: t.partitions.len}
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
			throttle_time_ms: default_throttle_time_ms
			topics:           topics
		}.encode(version)
	}

	// Convert service response to protocol response
	topics := build_commit_response_from_results(service_resp.results)

	resp := OffsetCommitResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}

	elapsed := time.since(start_time)
	h.logger.debug('Offset commit completed', observability.field_string('group_id', req.group_id),
		observability.field_int('partitions', total_partitions), observability.field_duration('latency',
		elapsed))

	return resp.encode(version)
}

// handle_offset_fetch - fetches committed consumer group offsets
fn (mut h Handler) handle_offset_fetch(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_offset_fetch_request(mut reader, version, is_flexible_version(.offset_fetch,
		version))!

	h.logger.debug('Processing offset fetch', observability.field_string('group_id', req.group_id),
		observability.field_int('topics', req.topics.len), observability.field_int('groups',
		req.groups.len))

	if version >= 8 {
		return h.handle_offset_fetch_v8plus(req, version)
	}
	return h.handle_offset_fetch_legacy(req, start_time, version)
}

// handle_offset_fetch_v8plus handles OffsetFetch for v8+ (multi-group path).
// fetch_offsets_for_group fetches committed offsets for a single consumer group.
fn (mut h Handler) fetch_offsets_for_group(g OffsetFetchRequestGroup, require_stable bool) OffsetFetchResponseGroup {
	mut partitions_to_fetch := []domain.TopicPartition{cap: g.topics.len * 4}
	for t in g.topics {
		for p in t.partitions {
			partitions_to_fetch << domain.TopicPartition{
				topic:     t.name
				partition: int(p)
			}
		}
	}

	service_resp := h.offset_manager.fetch_offsets(port.OffsetFetchRequest{
		group_id:       g.group_id
		partitions:     partitions_to_fetch
		require_stable: require_stable
	}) or {
		return OffsetFetchResponseGroup{
			group_id:   g.group_id
			topics:     []
			error_code: i16(ErrorCode.unknown_server_error)
		}
	}

	topics_map := group_fetch_partitions_by_topic(service_resp.results)

	mut topics := []OffsetFetchResponseGroupTopic{cap: topics_map.len}
	for name, partitions in topics_map {
		topics << OffsetFetchResponseGroupTopic{
			name:       name
			partitions: partitions
		}
	}

	return OffsetFetchResponseGroup{
		group_id:   g.group_id
		topics:     topics
		error_code: service_resp.error_code
	}
}

fn (mut h Handler) handle_offset_fetch_v8plus(req OffsetFetchRequest, version i16) ![]u8 {
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
		groups << h.fetch_offsets_for_group(g, req.require_stable)
	}

	resp := OffsetFetchResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           []
		error_code:       0
		groups:           groups
	}
	return resp.encode(version)
}

// handle_offset_fetch_legacy handles OffsetFetch for v0-7 (single group path).
fn (mut h Handler) handle_offset_fetch_legacy(req OffsetFetchRequest, start_time time.Time, version i16) ![]u8 {
	mut partitions_to_fetch := []domain.TopicPartition{cap: req.topics.len * 4}
	for t in req.topics {
		for p in t.partitions {
			partitions_to_fetch << domain.TopicPartition{
				topic:     t.name
				partition: int(p)
			}
		}
	}

	service_resp := h.offset_manager.fetch_offsets(port.OffsetFetchRequest{
		group_id:       req.group_id
		partitions:     partitions_to_fetch
		require_stable: req.require_stable
	}) or {
		return OffsetFetchResponse{
			throttle_time_ms: default_throttle_time_ms
			topics:           []
			error_code:       i16(ErrorCode.unknown_server_error)
			groups:           []
		}.encode(version)
	}

	topics_map := group_fetch_partitions_by_topic(service_resp.results)

	mut topics := []OffsetFetchResponseTopic{cap: topics_map.len}
	for name, partitions in topics_map {
		topics << OffsetFetchResponseTopic{
			name:       name
			partitions: partitions
		}
	}

	resp := OffsetFetchResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
		error_code:       service_resp.error_code
		groups:           []
	}

	elapsed := time.since(start_time)
	h.logger.debug('Offset fetch completed', observability.field_string('group_id', req.group_id),
		observability.field_int('topics', topics.len), observability.field_duration('latency',
		elapsed))

	return resp.encode(version)
}

// Processing functions (frame-based)
fn (mut h Handler) process_offset_commit(req OffsetCommitRequest, version i16) !OffsetCommitResponse {
	_ = version
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
			throttle_time_ms: default_throttle_time_ms
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
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}
}

fn (mut h Handler) process_offset_fetch(req OffsetFetchRequest, version i16) !OffsetFetchResponse {
	_ = version
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
			throttle_time_ms: default_throttle_time_ms
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
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
		error_code:       0
		groups:           []
	}
}

// Helper Functions

/// build_commit_response_from_results converts a service response into OffsetCommit protocol response topics.
fn build_commit_response_from_results(results []port.OffsetCommitResult) []OffsetCommitResponseTopic {
	mut topics_map := map[string][]OffsetCommitResponsePartition{}
	for result in results {
		if result.topic !in topics_map {
			topics_map[result.topic] = []OffsetCommitResponsePartition{}
		}
		topics_map[result.topic] << OffsetCommitResponsePartition{
			partition_index: i32(result.partition)
			error_code:      result.error_code
		}
	}

	mut topics := []OffsetCommitResponseTopic{cap: topics_map.len}
	for topic_name, partitions in topics_map {
		topics << OffsetCommitResponseTopic{
			name:       topic_name
			partitions: partitions
		}
	}
	return topics
}

/// build_fetch_response_from_results converts a service response into OffsetFetch protocol response partitions.
fn build_fetch_response_from_results(results []port.OffsetFetchResult) []OffsetFetchResponsePartition {
	mut partitions := []OffsetFetchResponsePartition{cap: results.len}
	for result in results {
		partitions << OffsetFetchResponsePartition{
			partition_index:        i32(result.partition)
			committed_offset:       result.committed_offset
			committed_leader_epoch: result.committed_leader_epoch
			committed_metadata:     if result.metadata.len > 0 {
				result.metadata
			} else {
				none
			}
			error_code:             result.error_code
		}
	}
	return partitions
}

/// group_fetch_partitions_by_topic groups OffsetFetch results by topic name.
fn group_fetch_partitions_by_topic(results []port.OffsetFetchResult) map[string][]OffsetFetchResponsePartition {
	mut topics_map := map[string][]OffsetFetchResponsePartition{}
	for result in results {
		if result.topic !in topics_map {
			topics_map[result.topic] = []OffsetFetchResponsePartition{}
		}
		topics_map[result.topic] << OffsetFetchResponsePartition{
			partition_index:        i32(result.partition)
			committed_offset:       result.committed_offset
			committed_leader_epoch: result.committed_leader_epoch
			committed_metadata:     if result.metadata.len > 0 {
				result.metadata
			} else {
				none
			}
			error_code:             result.error_code
		}
	}
	return topics_map
}
