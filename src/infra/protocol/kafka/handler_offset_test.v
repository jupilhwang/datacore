// OffsetCommit/OffsetFetch 핸들러 단위 테스트
// process_offset_commit, process_offset_fetch, 헬퍼 함수,
// handle_offset_commit, handle_offset_fetch, 응답 인코딩 등
module kafka

import domain
import infra.compression
import service.port

// -- 오프셋 추적 가능한 테스트용 스토리지 --
// MockStorage는 commit_offsets가 no-op이므로, 실제 오프셋 추적이 필요한 테스트를 위해 별도 정의

struct OffsetTrackingStorage {
mut:
	topics  map[string]domain.TopicMetadata
	groups  map[string]domain.ConsumerGroup
	offsets map[string]map[string]i64
}

fn new_offset_tracking_storage() &OffsetTrackingStorage {
	return &OffsetTrackingStorage{
		topics:  map[string]domain.TopicMetadata{}
		groups:  map[string]domain.ConsumerGroup{}
		offsets: map[string]map[string]i64{}
	}
}

fn (mut s OffsetTrackingStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        []u8{len: 16}
		partition_count: partitions
	}
	s.topics[name] = meta
	return meta
}

fn (mut s OffsetTrackingStorage) delete_topic(name string) ! {
	s.topics.delete(name)
}

fn (mut s OffsetTrackingStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (mut s OffsetTrackingStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found') }
}

fn (mut s OffsetTrackingStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('topic not found')
}

fn (mut s OffsetTrackingStorage) add_partitions(name string, new_count int) ! {}

fn (mut s OffsetTrackingStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	return domain.AppendResult{}
}

fn (mut s OffsetTrackingStorage) fetch(topic string, partition int, offset_val i64, max_bytes int) !domain.FetchResult {
	return domain.FetchResult{}
}

fn (mut s OffsetTrackingStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (mut s OffsetTrackingStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	if topic !in s.topics {
		return error('topic not found')
	}
	return domain.PartitionInfo{
		earliest_offset: 0
		latest_offset:   100
	}
}

fn (mut s OffsetTrackingStorage) save_group(group domain.ConsumerGroup) ! {
	s.groups[group.group_id] = group
}

fn (mut s OffsetTrackingStorage) load_group(group_id string) !domain.ConsumerGroup {
	return s.groups[group_id] or { return error('group not found') }
}

fn (mut s OffsetTrackingStorage) delete_group(group_id string) ! {
	s.groups.delete(group_id)
}

fn (mut s OffsetTrackingStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (mut s OffsetTrackingStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	if group_id !in s.offsets {
		s.offsets[group_id] = map[string]i64{}
	}
	for o in offsets {
		key := '${o.topic}:${o.partition}'
		s.offsets[group_id][key] = o.offset
	}
}

fn (mut s OffsetTrackingStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut results := []domain.OffsetFetchResult{}
	for p in partitions {
		key := '${p.topic}:${p.partition}'
		offset_val := if group_id in s.offsets {
			s.offsets[group_id][key] or { i64(-1) }
		} else {
			i64(-1)
		}
		results << domain.OffsetFetchResult{
			topic:     p.topic
			partition: p.partition
			offset:    offset_val
			metadata:  ''
		}
	}
	return results
}

fn (mut s OffsetTrackingStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &OffsetTrackingStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (s &OffsetTrackingStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

fn (s OffsetTrackingStorage) save_share_partition_state(state domain.SharePartitionState) ! {}

fn (s OffsetTrackingStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	return none
}

fn (s OffsetTrackingStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {}

fn (s OffsetTrackingStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	return []
}

// -- 헬퍼 함수 테스트 --

fn test_build_commit_response_from_results_success() {
	results := [
		port.OffsetCommitResult{
			topic:         'topic-a'
			partition:     0
			error_code:    0
			error_message: ''
		},
		port.OffsetCommitResult{
			topic:         'topic-a'
			partition:     1
			error_code:    0
			error_message: ''
		},
	]

	topics := build_commit_response_from_results(results)

	assert topics.len == 1
	assert topics[0].name == 'topic-a'
	assert topics[0].partitions.len == 2
	assert topics[0].partitions[0].partition_index == 0
	assert topics[0].partitions[0].error_code == 0
	assert topics[0].partitions[1].partition_index == 1
	assert topics[0].partitions[1].error_code == 0
}

fn test_build_commit_response_from_results_multiple_topics() {
	results := [
		port.OffsetCommitResult{
			topic:         'topic-a'
			partition:     0
			error_code:    0
			error_message: ''
		},
		port.OffsetCommitResult{
			topic:         'topic-b'
			partition:     0
			error_code:    0
			error_message: ''
		},
	]

	topics := build_commit_response_from_results(results)

	assert topics.len == 2
}

fn test_build_commit_response_from_results_with_errors() {
	results := [
		port.OffsetCommitResult{
			topic:         'topic-a'
			partition:     0
			error_code:    i16(ErrorCode.unknown_server_error)
			error_message: 'storage error'
		},
	]

	topics := build_commit_response_from_results(results)

	assert topics.len == 1
	assert topics[0].partitions[0].error_code == i16(ErrorCode.unknown_server_error)
}

fn test_build_commit_response_from_results_empty() {
	results := []port.OffsetCommitResult{}
	topics := build_commit_response_from_results(results)
	assert topics.len == 0
}

fn test_build_fetch_response_from_results_success() {
	results := [
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              0
			committed_offset:       100
			committed_leader_epoch: 5
			metadata:               'test-metadata'
			error_code:             0
		},
	]

	partitions := build_fetch_response_from_results(results)

	assert partitions.len == 1
	assert partitions[0].partition_index == 0
	assert partitions[0].committed_offset == 100
	assert partitions[0].committed_leader_epoch == 5
	assert partitions[0].committed_metadata or { '' } == 'test-metadata'
	assert partitions[0].error_code == 0
}

fn test_build_fetch_response_from_results_empty_metadata() {
	results := [
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              0
			committed_offset:       50
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
	]

	partitions := build_fetch_response_from_results(results)

	assert partitions.len == 1
	// 빈 metadata는 none으로 변환
	assert partitions[0].committed_metadata == none
}

fn test_group_fetch_partitions_by_topic_single() {
	results := [
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              0
			committed_offset:       10
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              1
			committed_offset:       20
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
	]

	topics_map := group_fetch_partitions_by_topic(results)

	assert topics_map.len == 1
	assert 'topic-a' in topics_map
	assert topics_map['topic-a'].len == 2
	assert topics_map['topic-a'][0].committed_offset == 10
	assert topics_map['topic-a'][1].committed_offset == 20
}

fn test_group_fetch_partitions_by_topic_multiple() {
	results := [
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              0
			committed_offset:       10
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
		port.OffsetFetchResult{
			topic:                  'topic-b'
			partition:              0
			committed_offset:       30
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
		port.OffsetFetchResult{
			topic:                  'topic-a'
			partition:              1
			committed_offset:       20
			committed_leader_epoch: -1
			metadata:               ''
			error_code:             0
		},
	]

	topics_map := group_fetch_partitions_by_topic(results)

	assert topics_map.len == 2
	assert topics_map['topic-a'].len == 2
	assert topics_map['topic-b'].len == 1
}

fn test_group_fetch_partitions_by_topic_empty() {
	results := []port.OffsetFetchResult{}
	topics_map := group_fetch_partitions_by_topic(results)
	assert topics_map.len == 0
}

// -- process_offset_commit 테스트 --

fn create_offset_test_compression() &compression.CompressionService {
	return compression.new_default_compression_service() or {
		panic('compression service init failed: ${err}')
	}
}

fn test_process_offset_commit_success() {
	mut storage := new_offset_tracking_storage()
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
		topic_id:        []u8{len: 16}
	}
	cs := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)

	req := OffsetCommitRequest{
		group_id: 'test-group'
		topics:   [
			OffsetCommitRequestTopic{
				name:       'test-topic'
				partitions: [
					OffsetCommitRequestPartition{
						partition_index:    0
						committed_offset:   100
						committed_metadata: 'meta-1'
					},
					OffsetCommitRequestPartition{
						partition_index:    1
						committed_offset:   200
						committed_metadata: ''
					},
				]
			},
		]
	}

	resp := handler.process_offset_commit(req, 0)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.topics.len == 1
	assert resp.topics[0].name == 'test-topic'
	assert resp.topics[0].partitions.len == 2
	assert resp.topics[0].partitions[0].partition_index == 0
	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[0].partitions[1].partition_index == 1
	assert resp.topics[0].partitions[1].error_code == 0
}

fn test_process_offset_commit_multiple_topics() {
	mut storage := new_offset_tracking_storage()
	storage.topics['topic-a'] = domain.TopicMetadata{
		name:            'topic-a'
		partition_count: 1
	}
	storage.topics['topic-b'] = domain.TopicMetadata{
		name:            'topic-b'
		partition_count: 1
	}
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	req := OffsetCommitRequest{
		group_id: 'multi-group'
		topics:   [
			OffsetCommitRequestTopic{
				name:       'topic-a'
				partitions: [
					OffsetCommitRequestPartition{
						partition_index:  0
						committed_offset: 50
					},
				]
			},
			OffsetCommitRequestTopic{
				name:       'topic-b'
				partitions: [
					OffsetCommitRequestPartition{
						partition_index:  0
						committed_offset: 75
					},
				]
			},
		]
	}

	resp := handler.process_offset_commit(req, 0)!

	assert resp.topics.len == 2
	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[1].partitions[0].error_code == 0
}

fn test_process_offset_commit_empty_topics() {
	storage := new_offset_tracking_storage()
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	req := OffsetCommitRequest{
		group_id: 'test-group'
		topics:   []
	}

	resp := handler.process_offset_commit(req, 0)!

	assert resp.topics.len == 0
}

// -- process_offset_fetch 테스트 --

fn test_process_offset_fetch_no_committed_offsets() {
	storage := new_offset_tracking_storage()
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	req := OffsetFetchRequest{
		group_id: 'test-group'
		topics:   [
			OffsetFetchRequestTopic{
				name:       'test-topic'
				partitions: [i32(0), i32(1)]
			},
		]
	}

	resp := handler.process_offset_fetch(req, 0)!

	assert resp.throttle_time_ms == default_throttle_time_ms
	assert resp.error_code == 0
	// 커밋된 오프셋이 없으므로 -1 반환
	for t in resp.topics {
		for p in t.partitions {
			assert p.committed_offset == -1
		}
	}
}

fn test_process_offset_fetch_with_committed_offsets() {
	mut storage := new_offset_tracking_storage()
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 2
		topic_id:        []u8{len: 16}
	}
	// 오프셋 사전 커밋
	storage.commit_offsets('test-group', [
		domain.PartitionOffset{
			topic:     'test-topic'
			partition: 0
			offset:    42
		},
		domain.PartitionOffset{
			topic:     'test-topic'
			partition: 1
			offset:    84
		},
	])!

	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	req := OffsetFetchRequest{
		group_id: 'test-group'
		topics:   [
			OffsetFetchRequestTopic{
				name:       'test-topic'
				partitions: [i32(0), i32(1)]
			},
		]
	}

	resp := handler.process_offset_fetch(req, 0)!

	assert resp.error_code == 0
	assert resp.topics.len == 1
	assert resp.topics[0].name == 'test-topic'
	assert resp.topics[0].partitions.len == 2
	// 커밋된 오프셋 확인
	mut offsets := map[int]i64{}
	for p in resp.topics[0].partitions {
		offsets[int(p.partition_index)] = p.committed_offset
	}
	assert offsets[0] == 42
	assert offsets[1] == 84
}

fn test_process_offset_fetch_empty_topics() {
	storage := new_offset_tracking_storage()
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	req := OffsetFetchRequest{
		group_id: 'test-group'
		topics:   []
	}

	resp := handler.process_offset_fetch(req, 0)!

	assert resp.topics.len == 0
	assert resp.error_code == 0
}

// -- OffsetCommitResponse 인코딩 테스트 --

fn test_offset_commit_response_encode_v0() {
	resp := OffsetCommitResponse{
		throttle_time_ms: 0
		topics:           [
			OffsetCommitResponseTopic{
				name:       'test-topic'
				partitions: [
					OffsetCommitResponsePartition{
						partition_index: 0
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	// v0: topics array (throttle_time_ms 없음)
	topics_len := reader.read_i32()!
	assert topics_len == 1
	name := reader.read_string()!
	assert name == 'test-topic'
	parts_len := reader.read_i32()!
	assert parts_len == 1
	pi := reader.read_i32()!
	assert pi == 0
	ec := reader.read_i16()!
	assert ec == 0
}

fn test_offset_commit_response_encode_v3_with_throttle() {
	resp := OffsetCommitResponse{
		throttle_time_ms: 100
		topics:           [
			OffsetCommitResponseTopic{
				name:       'topic-1'
				partitions: [
					OffsetCommitResponsePartition{
						partition_index: 0
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(3)
	mut reader := new_reader(encoded)

	// v3+: throttle_time_ms 포함
	throttle := reader.read_i32()!
	assert throttle == 100
}

// -- OffsetFetchResponse 인코딩 테스트 --

fn test_offset_fetch_response_encode_v0() {
	resp := OffsetFetchResponse{
		throttle_time_ms: 0
		topics:           [
			OffsetFetchResponseTopic{
				name:       'test-topic'
				partitions: [
					OffsetFetchResponsePartition{
						partition_index:        0
						committed_offset:       100
						committed_leader_epoch: -1
						committed_metadata:     'meta'
						error_code:             0
					},
				]
			},
		]
		error_code:       0
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	// v0: topics array
	topics_len := reader.read_i32()!
	assert topics_len == 1
	name := reader.read_string()!
	assert name == 'test-topic'
	parts_len := reader.read_i32()!
	assert parts_len == 1
	pi := reader.read_i32()!
	assert pi == 0
	co := reader.read_i64()!
	assert co == 100
}

fn test_offset_fetch_response_encode_v3_with_error() {
	resp := OffsetFetchResponse{
		throttle_time_ms: 50
		topics:           []
		error_code:       i16(ErrorCode.unknown_server_error)
	}

	encoded := resp.encode(3)
	mut reader := new_reader(encoded)

	// v3+: throttle_time_ms
	throttle := reader.read_i32()!
	assert throttle == 50
	// topics array
	topics_len := reader.read_i32()!
	assert topics_len == 0
	// v2+: error_code
	ec := reader.read_i16()!
	assert ec == i16(ErrorCode.unknown_server_error)
}

// -- handle_offset_commit 바이트 수준 테스트 --

fn test_handle_offset_commit_v0_bytes() {
	mut storage := new_offset_tracking_storage()
	storage.topics['test-topic'] = domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 1
		topic_id:        []u8{len: 16}
	}
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// v0 요청 바이너리 생성
	mut writer := new_writer()
	writer.write_string('my-group') // group_id
	// topics array (1개)
	writer.write_i32(1)
	writer.write_string('test-topic') // topic name
	// partitions array (1개)
	writer.write_i32(1)
	writer.write_i32(0) // partition_index
	writer.write_i64(50) // committed_offset
	writer.write_nullable_string('meta') // committed_metadata

	result := handler.handle_offset_commit(writer.bytes(), 0)!
	assert result.len > 0

	// 응답 파싱
	mut reader := new_reader(result)
	topics_len := reader.read_i32()!
	assert topics_len == 1
	topic_name := reader.read_string()!
	assert topic_name == 'test-topic'
	parts_len := reader.read_i32()!
	assert parts_len == 1
	pi := reader.read_i32()!
	assert pi == 0
	ec := reader.read_i16()!
	assert ec == 0
}

// -- handle_offset_fetch 바이트 수준 테스트 --

fn test_handle_offset_fetch_v0_bytes() {
	storage := new_offset_tracking_storage()
	compression_service := create_offset_test_compression()
	mut handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, compression_service)

	// v0 요청 바이너리 생성
	mut writer := new_writer()
	writer.write_string('my-group') // group_id
	// topics array (1개)
	writer.write_i32(1)
	writer.write_string('test-topic') // topic name
	// partitions array (1개)
	writer.write_i32(1)
	writer.write_i32(0) // partition_index

	result := handler.handle_offset_fetch(writer.bytes(), 0)!
	assert result.len > 0

	// 응답 파싱
	mut reader := new_reader(result)
	topics_len := reader.read_i32()!
	assert topics_len >= 0
}

// -- 파싱 테스트 --

fn test_parse_offset_commit_request_v0() {
	mut writer := new_writer()
	writer.write_string('group-1') // group_id
	writer.write_i32(1) // topics array len
	writer.write_string('topic-1') // topic name
	writer.write_i32(1) // partitions array len
	writer.write_i32(0) // partition_index
	writer.write_i64(100) // committed_offset
	writer.write_nullable_string('test-meta') // committed_metadata

	mut reader := new_reader(writer.bytes())
	req := parse_offset_commit_request(mut reader, 0, false)!

	assert req.group_id == 'group-1'
	assert req.topics.len == 1
	assert req.topics[0].name == 'topic-1'
	assert req.topics[0].partitions.len == 1
	assert req.topics[0].partitions[0].partition_index == 0
	assert req.topics[0].partitions[0].committed_offset == 100
}

fn test_parse_offset_fetch_request_v0() {
	mut writer := new_writer()
	writer.write_string('group-1') // group_id
	writer.write_i32(1) // topics array len
	writer.write_string('topic-1') // topic name
	writer.write_i32(2) // partitions array len
	writer.write_i32(0) // partition 0
	writer.write_i32(1) // partition 1

	mut reader := new_reader(writer.bytes())
	req := parse_offset_fetch_request(mut reader, 0, false)!

	assert req.group_id == 'group-1'
	assert req.topics.len == 1
	assert req.topics[0].name == 'topic-1'
	assert req.topics[0].partitions.len == 2
	assert req.topics[0].partitions[0] == 0
	assert req.topics[0].partitions[1] == 1
}
