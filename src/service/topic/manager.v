// 토픽 관리 비즈니스 로직을 처리합니다.
// 토픽 생성, 삭제, 조회, 파티션 추가 등의 기능을 제공합니다.
module topic

import domain
import service.port

/// TopicManager는 토픽 관리 비즈니스 로직을 처리합니다.
/// 토픽 생명주기 관리 및 설정 검증을 담당합니다.
pub struct TopicManager {
	storage port.StoragePort
}

/// new_topic_manager는 새로운 TopicManager를 생성합니다.
pub fn new_topic_manager(storage port.StoragePort) &TopicManager {
	return &TopicManager{
		storage: storage
	}
}

/// CreateTopicRequest는 토픽 생성 요청을 나타냅니다.
pub struct CreateTopicRequest {
pub:
	name               string
	num_partitions     int
	replication_factor i16
	configs            map[string]string
}

/// CreateTopicResponse는 토픽 생성 응답을 나타냅니다.
pub struct CreateTopicResponse {
pub:
	name               string
	error_code         i16
	error_message      string
	num_partitions     int
	replication_factor i16
}

/// create_topic은 새로운 토픽을 생성합니다.
/// 토픽 이름 및 파티션 수 유효성 검사를 수행합니다.
pub fn (m &TopicManager) create_topic(req CreateTopicRequest) CreateTopicResponse {
	// 토픽 이름 유효성 검사
	// 내부 토픽(__로 시작)은 __schemas만 허용
	if req.name.len == 0 || req.name.starts_with('__') && req.name != '__schemas' {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.invalid_topic_exception)
			error_message: 'Invalid topic name'
		}
	}

	// 파티션 수 유효성 검사
	if req.num_partitions <= 0 {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.invalid_partitions)
			error_message: 'Number of partitions must be positive'
		}
	}

	// 토픽 중복 확인
	if _ := m.storage.get_topic(req.name) {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.topic_already_exists)
			error_message: 'Topic already exists'
		}
	}

	// 토픽 설정 구성
	config := domain.TopicConfig{
		retention_ms:    parse_config_i64(req.configs, 'retention.ms', 604800000)
		retention_bytes: parse_config_i64(req.configs, 'retention.bytes', -1)
		segment_bytes:   parse_config_i64(req.configs, 'segment.bytes', 1073741824)
		cleanup_policy:  req.configs['cleanup.policy'] or { 'delete' }
	}

	// 토픽 생성
	m.storage.create_topic(req.name, req.num_partitions, config) or {
		return CreateTopicResponse{
			name:          req.name
			error_code:    i16(domain.ErrorCode.unknown_server_error)
			error_message: err.str()
		}
	}

	return CreateTopicResponse{
		name:               req.name
		error_code:         0
		num_partitions:     req.num_partitions
		replication_factor: req.replication_factor
	}
}

/// delete_topic은 토픽을 삭제합니다.
/// 내부 토픽(__로 시작)은 __schemas를 제외하고 삭제할 수 없습니다.
pub fn (m &TopicManager) delete_topic(name string) ! {
	// 내부 토픽 삭제 방지
	if name.starts_with('__') && name != '__schemas' {
		return error('Cannot delete internal topic')
	}

	m.storage.delete_topic(name)!
}

/// list_topics는 모든 토픽 목록을 반환합니다.
pub fn (m &TopicManager) list_topics() ![]domain.TopicMetadata {
	return m.storage.list_topics()
}

/// get_topic은 토픽 메타데이터를 조회합니다.
pub fn (m &TopicManager) get_topic(name string) !domain.TopicMetadata {
	return m.storage.get_topic(name)
}

/// add_partitions는 토픽에 파티션을 추가합니다.
/// 새 파티션 수는 현재 파티션 수보다 커야 합니다.
pub fn (m &TopicManager) add_partitions(name string, new_count int) ! {
	// 현재 토픽 정보 조회
	topic := m.storage.get_topic(name)!

	// 새 파티션 수 유효성 검사
	if new_count <= topic.partition_count {
		return error('New partition count must be greater than current')
	}

	m.storage.add_partitions(name, new_count)!
}

/// parse_config_i64는 설정 맵에서 i64 값을 파싱합니다.
/// 키가 없거나 파싱 실패 시 기본값을 반환합니다.
fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
	if val := configs[key] {
		return val.i64()
	}
	return default_val
}
