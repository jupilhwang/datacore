// Kafka protocol - DescribeLogDirs (API Key 35) and AlterReplicaLogDirs (API Key 34)
// Request/response types, parsing, encoding, and handlers
//
// DataCore is a stateless broker; log directories are virtual.
// DescribeLogDirs returns virtual log dir info based on partition metadata.
// AlterReplicaLogDirs is a no-op in stateless architecture (shared storage).
module kafka

import domain
import service.port
import time

// DescribeLogDirs (API Key 35)
// Valid versions: 1-4; flexible: 2+

/// DescribeLogDirsRequest holds the request data for DescribeLogDirs (API Key 35).
pub struct DescribeLogDirsRequest {
pub:
	topics ?[]DescribeLogDirsTopic
}

/// DescribeLogDirsTopic holds a topic and partition list for DescribeLogDirs.
pub struct DescribeLogDirsTopic {
pub:
	topic      string
	partitions []i32
}

/// DescribeLogDirsResponse holds the response data for DescribeLogDirs.
pub struct DescribeLogDirsResponse {
pub:
	throttle_time_ms i32
	error_code       i16 // v3+
	results          []DescribeLogDirsResult
}

/// DescribeLogDirsResult holds a single log directory result.
pub struct DescribeLogDirsResult {
pub:
	error_code   i16
	log_dir      string
	topics       []DescribeLogDirsTopicResult
	total_bytes  i64 = -1 // v4+
	usable_bytes i64 = -1 // v4+
}

/// DescribeLogDirsTopicResult holds per-topic data in a DescribeLogDirs result.
pub struct DescribeLogDirsTopicResult {
pub:
	name       string
	partitions []DescribeLogDirsPartitionResult
}

/// DescribeLogDirsPartitionResult holds per-partition data in a DescribeLogDirs result.
pub struct DescribeLogDirsPartitionResult {
pub:
	partition_index i32
	partition_size  i64
	offset_lag      i64
	is_future_key   bool
}

fn parse_describe_log_dirs_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeLogDirsRequest {
	// topics (nullable array)
	count := reader.read_flex_array_len(is_flexible)!

	mut topics := ?[]DescribeLogDirsTopic(none)
	if count >= 0 {
		mut topic_list := []DescribeLogDirsTopic{}
		for _ in 0 .. count {
			topic_name := reader.read_flex_string(is_flexible)!
			p_count := reader.read_flex_array_len(is_flexible)!
			mut partitions := []i32{}
			for _ in 0 .. p_count {
				partitions << reader.read_i32()!
			}
			reader.skip_flex_tagged_fields(is_flexible)!
			topic_list << DescribeLogDirsTopic{
				topic:      topic_name
				partitions: partitions
			}
		}
		topics = topic_list.clone()
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return DescribeLogDirsRequest{
		topics: topics
	}
}

/// encode serializes the DescribeLogDirsResponse into bytes.
pub fn (r DescribeLogDirsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	// v3+ top-level error_code
	if version >= 3 {
		writer.write_i16(r.error_code)
	}

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)
		if is_flexible {
			writer.write_compact_string(res.log_dir)
			writer.write_compact_array_len(res.topics.len)
		} else {
			writer.write_string(res.log_dir)
			writer.write_array_len(res.topics.len)
		}

		for t in res.topics {
			if is_flexible {
				writer.write_compact_string(t.name)
				writer.write_compact_array_len(t.partitions.len)
			} else {
				writer.write_string(t.name)
				writer.write_array_len(t.partitions.len)
			}

			for p in t.partitions {
				writer.write_i32(p.partition_index)
				writer.write_i64(p.partition_size)
				writer.write_i64(p.offset_lag)
				writer.write_i8(if p.is_future_key { i8(1) } else { i8(0) })
				if is_flexible {
					writer.write_tagged_fields()
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		// v4+: total_bytes and usable_bytes
		if version >= 4 {
			writer.write_i64(res.total_bytes)
			writer.write_i64(res.usable_bytes)
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

/// handle_describe_log_dirs handles the DescribeLogDirs API (Key 35).
pub fn (mut h Handler) handle_describe_log_dirs(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 2
	mut reader := new_reader(body)
	req := parse_describe_log_dirs_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing describe log dirs', port.field_bool('all_topics', req.topics == none))

	resp := h.process_describe_log_dirs(req, version)!

	elapsed := time.since(start_time)
	h.logger.debug('Describe log dirs completed', port.field_int('results', resp.results.len),
		port.field_duration('latency', elapsed))

	return resp.encode(version)
}

// virtual_log_dir is the virtual log directory path for stateless brokers.
const virtual_log_dir = '/var/kafka-logs'

fn (mut h Handler) process_describe_log_dirs(req DescribeLogDirsRequest, version i16) !DescribeLogDirsResponse {
	_ = version
	mut topic_results := []DescribeLogDirsTopicResult{}

	if requested_topics := req.topics {
		// Specific topics requested
		for t in requested_topics {
			topic_meta := h.storage.get_topic(t.topic) or {
				h.logger.debug('Topic not found in describe log dirs', port.field_string('topic',
					t.topic))
				continue
			}

			mut partition_results := []DescribeLogDirsPartitionResult{}
			for p_idx in t.partitions {
				if p_idx < 0 || p_idx >= i32(topic_meta.partition_count) {
					continue
				}
				info := h.storage.get_partition_info(t.topic, int(p_idx)) or {
					partition_results << DescribeLogDirsPartitionResult{
						partition_index: p_idx
						partition_size:  0
						offset_lag:      0
						is_future_key:   false
					}
					continue
				}
				// Estimate partition size from offset range (each record ~1KB avg)
				estimated_size := (info.latest_offset - info.earliest_offset) * 1024
				partition_results << DescribeLogDirsPartitionResult{
					partition_index: p_idx
					partition_size:  estimated_size
					offset_lag:      0
					is_future_key:   false
				}
			}

			topic_results << DescribeLogDirsTopicResult{
				name:       t.topic
				partitions: partition_results
			}
		}
	} else {
		// All topics requested
		all_topics := h.storage.list_topics() or { []domain.TopicMetadata{} }
		for topic_meta in all_topics {
			mut partition_results := []DescribeLogDirsPartitionResult{}
			for i in 0 .. topic_meta.partition_count {
				info := h.storage.get_partition_info(topic_meta.name, i) or {
					partition_results << DescribeLogDirsPartitionResult{
						partition_index: i32(i)
						partition_size:  0
						offset_lag:      0
						is_future_key:   false
					}
					continue
				}
				estimated_size := (info.latest_offset - info.earliest_offset) * 1024
				partition_results << DescribeLogDirsPartitionResult{
					partition_index: i32(i)
					partition_size:  estimated_size
					offset_lag:      0
					is_future_key:   false
				}
			}
			topic_results << DescribeLogDirsTopicResult{
				name:       topic_meta.name
				partitions: partition_results
			}
		}
	}

	result := DescribeLogDirsResult{
		error_code:   0
		log_dir:      virtual_log_dir
		topics:       topic_results
		total_bytes:  -1
		usable_bytes: -1
	}

	return DescribeLogDirsResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		results:          [result]
	}
}

// AlterReplicaLogDirs (API Key 34)
// Valid versions: 1-2; flexible: 2+

/// AlterReplicaLogDirsRequest holds the request data for AlterReplicaLogDirs (API Key 34).
pub struct AlterReplicaLogDirsRequest {
pub:
	dirs []AlterReplicaLogDir
}

/// AlterReplicaLogDir holds a target directory and its topics.
pub struct AlterReplicaLogDir {
pub:
	path   string
	topics []AlterReplicaLogDirTopic
}

/// AlterReplicaLogDirTopic holds topic/partition data for AlterReplicaLogDirs.
pub struct AlterReplicaLogDirTopic {
pub:
	name       string
	partitions []i32
}

/// AlterReplicaLogDirsResponse holds the response data for AlterReplicaLogDirs.
pub struct AlterReplicaLogDirsResponse {
pub:
	throttle_time_ms i32
	results          []AlterReplicaLogDirTopicResult
}

/// AlterReplicaLogDirTopicResult holds per-topic results.
pub struct AlterReplicaLogDirTopicResult {
pub:
	topic_name string
	partitions []AlterReplicaLogDirPartitionResult
}

/// AlterReplicaLogDirPartitionResult holds per-partition result.
pub struct AlterReplicaLogDirPartitionResult {
pub:
	partition_index i32
	error_code      i16
}

fn parse_alter_replica_log_dirs_request(mut reader BinaryReader, version i16, is_flexible bool) !AlterReplicaLogDirsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut dirs := []AlterReplicaLogDir{}

	for _ in 0 .. count {
		path := reader.read_flex_string(is_flexible)!
		t_count := reader.read_flex_array_len(is_flexible)!
		mut topics := []AlterReplicaLogDirTopic{}

		for _ in 0 .. t_count {
			topic_name := reader.read_flex_string(is_flexible)!
			p_count := reader.read_flex_array_len(is_flexible)!
			mut partitions := []i32{}
			for _ in 0 .. p_count {
				partitions << reader.read_i32()!
			}
			reader.skip_flex_tagged_fields(is_flexible)!
			topics << AlterReplicaLogDirTopic{
				name:       topic_name
				partitions: partitions
			}
		}

		reader.skip_flex_tagged_fields(is_flexible)!
		dirs << AlterReplicaLogDir{
			path:   path
			topics: topics
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return AlterReplicaLogDirsRequest{
		dirs: dirs
	}
}

/// encode serializes the AlterReplicaLogDirsResponse into bytes.
pub fn (r AlterReplicaLogDirsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		if is_flexible {
			writer.write_compact_string(res.topic_name)
			writer.write_compact_array_len(res.partitions.len)
		} else {
			writer.write_string(res.topic_name)
			writer.write_array_len(res.partitions.len)
		}

		for p in res.partitions {
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

/// handle_alter_replica_log_dirs handles the AlterReplicaLogDirs API (Key 34).
/// DataCore is stateless — replicas live on shared storage (S3/PostgreSQL),
/// so log directory changes are no-ops. We verify topics/partitions exist
/// and return success.
pub fn (mut h Handler) handle_alter_replica_log_dirs(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 2
	mut reader := new_reader(body)
	req := parse_alter_replica_log_dirs_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing alter replica log dirs (stateless no-op)', port.field_int('dirs',
		req.dirs.len))

	// Collect unique topics across all dirs
	mut topic_partition_map := map[string][]i32{}
	for dir in req.dirs {
		for t in dir.topics {
			for p in t.partitions {
				topic_partition_map[t.name] << p
			}
		}
	}

	mut results := []AlterReplicaLogDirTopicResult{}

	for topic_name, partitions in topic_partition_map {
		topic_meta := h.storage.get_topic(topic_name) or {
			// Topic does not exist — return error for all partitions
			mut part_results := []AlterReplicaLogDirPartitionResult{}
			for p in partitions {
				part_results << AlterReplicaLogDirPartitionResult{
					partition_index: p
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
				}
			}
			results << AlterReplicaLogDirTopicResult{
				topic_name: topic_name
				partitions: part_results
			}
			continue
		}

		mut part_results := []AlterReplicaLogDirPartitionResult{}
		for p in partitions {
			if p < 0 || p >= i32(topic_meta.partition_count) {
				part_results << AlterReplicaLogDirPartitionResult{
					partition_index: p
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
				}
			} else {
				// In stateless mode, log dir changes are no-ops (shared storage)
				part_results << AlterReplicaLogDirPartitionResult{
					partition_index: p
					error_code:      0
				}
			}
		}

		results << AlterReplicaLogDirTopicResult{
			topic_name: topic_name
			partitions: part_results
		}
	}

	resp := AlterReplicaLogDirsResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}

	elapsed := time.since(start_time)
	h.logger.debug('Alter replica log dirs completed', port.field_int('results', results.len),
		port.field_duration('latency', elapsed))

	return resp.encode(version)
}
