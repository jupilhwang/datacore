// 서비스 레이어 - Offset 유스케이스
// OffsetCommit, OffsetFetch 비즈니스 로직을 처리합니다.
// 컨슈머 그룹의 오프셋 관리를 담당합니다.
module offset

import domain
import service.port
import infra.observability

/// OffsetManager는 오프셋 관리 비즈니스 로직을 처리합니다.
/// 오프셋 커밋, 조회, 유효성 검증을 담당합니다.
pub struct OffsetManager {
mut:
	storage port.StoragePort
	logger  &observability.Logger
}

/// new_offset_manager는 새로운 OffsetManager를 생성합니다.
pub fn new_offset_manager(storage port.StoragePort, logger &observability.Logger) &OffsetManager {
	return &OffsetManager{
		storage: storage
		logger:  logger
	}
}

/// commit_offsets는 오프셋 커밋 요청을 처리합니다.
/// 유효성 검증 후 스토리지에 오프셋을 저장합니다.
pub fn (mut m OffsetManager) commit_offsets(req OffsetCommitRequest) !OffsetCommitResponse {
	// 그룹 ID 유효성 검사
	if req.group_id.len == 0 {
		m.logger.warn('Invalid group_id: empty string')
		return error('invalid group_id')
	}

	// 오프셋 목록 유효성 검사
	if req.offsets.len == 0 {
		m.logger.warn('No offsets to commit', observability.field_string('group_id', req.group_id))
		return OffsetCommitResponse{
			results: []
		}
	}

	m.logger.debug('Committing offsets', observability.field_string('group_id', req.group_id),
		observability.field_int('count', req.offsets.len))

	// 스토리지에 오프셋 커밋
	m.storage.commit_offsets(req.group_id, req.offsets) or {
		m.logger.error('Failed to commit offsets', observability.field_string('group_id',
			req.group_id), observability.field_string('error', err.str()))

		// 모든 파티션에 대해 에러 결과 생성
		mut results := []OffsetCommitResult{cap: req.offsets.len}
		for offset in req.offsets {
			results << OffsetCommitResult{
				topic:         offset.topic
				partition:     offset.partition
				error_code:    i16(domain.ErrorCode.unknown_server_error)
				error_message: err.str()
			}
		}
		return OffsetCommitResponse{
			results: results
		}
	}

	// 성공 결과 생성
	mut results := []OffsetCommitResult{cap: req.offsets.len}
	for offset in req.offsets {
		results << OffsetCommitResult{
			topic:         offset.topic
			partition:     offset.partition
			error_code:    0
			error_message: ''
		}
	}

	m.logger.debug('Offsets committed successfully', observability.field_string('group_id',
		req.group_id), observability.field_int('count', req.offsets.len))

	return OffsetCommitResponse{
		results: results
	}
}

/// fetch_offsets는 오프셋 조회 요청을 처리합니다.
/// 스토리지에서 커밋된 오프셋을 조회하고 TopicId 변환을 처리합니다.
pub fn (mut m OffsetManager) fetch_offsets(req OffsetFetchRequest) !OffsetFetchResponse {
	// 그룹 ID 유효성 검사
	if req.group_id.len == 0 {
		m.logger.warn('Invalid group_id: empty string')
		return OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.invalid_group_id)
		}
	}

	// 파티션 목록이 비어있으면 빈 결과 반환
	if req.partitions.len == 0 {
		m.logger.debug('No partitions to fetch', observability.field_string('group_id',
			req.group_id))
		return OffsetFetchResponse{
			results:    []
			error_code: 0
		}
	}

	m.logger.debug('Fetching offsets', observability.field_string('group_id', req.group_id),
		observability.field_int('partitions', req.partitions.len))

	// 스토리지에서 오프셋 조회
	fetched := m.storage.fetch_offsets(req.group_id, req.partitions) or {
		m.logger.error('Failed to fetch offsets', observability.field_string('group_id',
			req.group_id), observability.field_string('error', err.str()))
		return OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	// 결과 변환 (domain.OffsetFetchResult → offset.OffsetFetchResult)
	mut results := []OffsetFetchResult{cap: fetched.len}
	for f in fetched {
		// TopicId 조회 시도 (v10 지원)
		mut topic_id := ?[]u8(none)
		topic_meta := m.storage.get_topic(f.topic) or {
			// 토픽을 찾을 수 없는 경우에도 결과 반환 (에러 코드 포함)
			results << OffsetFetchResult{
				topic:                  f.topic
				topic_id:               none
				partition:              f.partition
				committed_offset:       f.offset
				committed_leader_epoch: -1
				metadata:               f.metadata
				error_code:             i16(domain.ErrorCode.unknown_topic_or_partition)
			}
			continue
		}

		// TopicId가 있으면 설정
		if topic_meta.topic_id.len > 0 {
			topic_id = topic_meta.topic_id.clone()
		}

		results << OffsetFetchResult{
			topic:                  f.topic
			topic_id:               topic_id
			partition:              f.partition
			committed_offset:       f.offset
			committed_leader_epoch: -1
			metadata:               f.metadata
			error_code:             f.error_code
		}
	}

	m.logger.debug('Offsets fetched successfully', observability.field_string('group_id',
		req.group_id), observability.field_int('count', results.len))

	return OffsetFetchResponse{
		results:    results
		error_code: 0
	}
}

/// fetch_offsets_by_topic_id는 TopicId 기반으로 오프셋을 조회합니다 (v10+).
/// TopicId를 TopicName으로 변환한 후 오프셋을 조회합니다.
pub fn (mut m OffsetManager) fetch_offsets_by_topic_id(group_id string, topic_id []u8, partitions []int) !OffsetFetchResponse {
	// TopicId로 토픽 조회
	topic_meta := m.storage.get_topic_by_id(topic_id) or {
		m.logger.warn('Topic not found by ID', observability.field_string('group_id',
			group_id))
		return OffsetFetchResponse{
			results:    []
			error_code: i16(domain.ErrorCode.unknown_topic_id)
		}
	}

	// TopicPartition 목록 생성
	mut topic_partitions := []domain.TopicPartition{cap: partitions.len}
	for p in partitions {
		topic_partitions << domain.TopicPartition{
			topic:     topic_meta.name
			partition: p
		}
	}

	// 일반 fetch_offsets 호출
	return m.fetch_offsets(OffsetFetchRequest{
		group_id:       group_id
		partitions:     topic_partitions
		require_stable: false
	})
}
