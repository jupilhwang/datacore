// TopicManager 단위 테스트.
// 토픽 CRUD 관리, 중복 생성 방지, 파티션 추가, 설정 유효성 검증 로직을 검증한다.
module topic

import domain
import infra.storage.plugins.memory

// 헬퍼: 테스트용 스토리지 생성
fn create_topic_test_storage() &memory.MemoryStorageAdapter {
	return memory.new_memory_adapter()
}

// 헬퍼: 테스트용 TopicManager 생성
fn create_topic_test_manager() (&TopicManager, &memory.MemoryStorageAdapter) {
	storage := create_topic_test_storage()
	manager := new_topic_manager(storage)
	return manager, storage
}

// ============================================================
// create_topic 테스트
// ============================================================

fn test_create_topic_success() {
	// 유효한 요청으로 토픽을 생성하면 성공해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:               'test-topic'
		num_partitions:     3
		replication_factor: 1
		configs:            map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == 0, '오류 코드가 0이어야 한다, 실제: ${resp.error_code}'
	assert resp.name == 'test-topic'
	assert resp.num_partitions == 3
	assert resp.replication_factor == 1
}

fn test_create_topic_with_configs() {
	// 사용자 지정 설정으로 토픽을 생성할 수 있어야 한다.
	mut manager, _ := create_topic_test_manager()

	configs := {
		'retention.ms':   '86400000'
		'segment.bytes':  '536870912'
		'cleanup.policy': 'compact'
	}

	req := CreateTopicRequest{
		name:               'configured-topic'
		num_partitions:     1
		replication_factor: 1
		configs:            configs
	}

	resp := manager.create_topic(req)

	assert resp.error_code == 0
	assert resp.name == 'configured-topic'
}

fn test_create_topic_schemas_internal() {
	// __schemas 내부 토픽은 생성이 허용되어야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           '__schemas'
		num_partitions: 1
		configs:        map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == 0, '__schemas 토픽 생성이 허용되어야 한다, 실제 오류: ${resp.error_code}'
}

// ============================================================
// create_topic 실패 케이스 테스트
// ============================================================

fn test_create_topic_empty_name() {
	// 빈 토픽 이름은 invalid_topic_exception을 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           ''
		num_partitions: 1
		configs:        map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == i16(domain.ErrorCode.invalid_topic_exception), '빈 이름에 대한 오류 코드가 invalid_topic_exception이어야 한다'
	assert resp.error_message.len > 0
}

fn test_create_topic_internal_prefix_rejected() {
	// __ 접두사를 가진 토픽(__schemas 제외)은 생성이 거부되어야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           '__consumer_offsets'
		num_partitions: 50
		configs:        map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == i16(domain.ErrorCode.invalid_topic_exception), '내부 토픽 생성이 거부되어야 한다'
}

fn test_create_topic_invalid_partition_count() {
	// 파티션 수가 0 이하이면 invalid_partitions를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'bad-partitions'
		num_partitions: 0
		configs:        map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == i16(domain.ErrorCode.invalid_partitions), '파티션 수 0에 대한 오류 코드가 invalid_partitions여야 한다'
}

fn test_create_topic_negative_partition_count() {
	// 음수 파티션 수도 invalid_partitions를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'negative-partitions'
		num_partitions: -1
		configs:        map[string]string{}
	}

	resp := manager.create_topic(req)

	assert resp.error_code == i16(domain.ErrorCode.invalid_partitions)
}

// ============================================================
// 중복 토픽 생성 테스트
// ============================================================

fn test_create_topic_duplicate() {
	// 이미 존재하는 토픽을 다시 생성하면 topic_already_exists를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'dup-topic'
		num_partitions: 1
		configs:        map[string]string{}
	}

	// 첫 번째 생성은 성공
	resp1 := manager.create_topic(req)
	assert resp1.error_code == 0

	// 두 번째 생성은 중복 오류
	resp2 := manager.create_topic(req)
	assert resp2.error_code == i16(domain.ErrorCode.topic_already_exists), '중복 토픽 생성의 오류 코드가 topic_already_exists여야 한다'
	assert resp2.error_message.len > 0
}

// ============================================================
// delete_topic 테스트
// ============================================================

fn test_delete_topic_success() {
	// 존재하는 토픽을 삭제하면 성공해야 한다.
	mut manager, _ := create_topic_test_manager()

	create_req := CreateTopicRequest{
		name:           'to-delete'
		num_partitions: 1
		configs:        map[string]string{}
	}
	manager.create_topic(create_req)

	manager.delete_topic('to-delete') or {
		assert false, '토픽 삭제가 성공해야 한다: ${err}'
		return
	}

	// 삭제 후 get_topic이 실패해야 한다
	manager.get_topic('to-delete') or { return }
	assert false, '삭제된 토픽 조회가 실패해야 한다'
}

fn test_delete_topic_nonexistent() {
	// 존재하지 않는 토픽 삭제는 오류를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	manager.delete_topic('ghost-topic') or {
		assert err.msg().len > 0
		return
	}
	assert false, '존재하지 않는 토픽 삭제는 오류를 반환해야 한다'
}

fn test_delete_internal_topic_rejected() {
	// __ 접두사를 가진 내부 토픽 삭제는 거부되어야 한다.
	mut manager, _ := create_topic_test_manager()

	manager.delete_topic('__consumer_offsets') or {
		assert err.msg().contains('internal')
		return
	}
	assert false, '내부 토픽 삭제는 거부되어야 한다'
}

fn test_delete_schemas_topic_allowed() {
	// __schemas 토픽 삭제는 허용되어야 한다.
	mut manager, _ := create_topic_test_manager()

	// 먼저 __schemas 토픽 생성
	create_req := CreateTopicRequest{
		name:           '__schemas'
		num_partitions: 1
		configs:        map[string]string{}
	}
	manager.create_topic(create_req)

	// __schemas 삭제 시도
	manager.delete_topic('__schemas') or {
		assert false, '__schemas 토픽 삭제가 허용되어야 한다: ${err}'
		return
	}
}

// ============================================================
// list_topics 테스트
// ============================================================

fn test_list_topics_empty() {
	// 토픽이 없으면 빈 목록을 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	topics := manager.list_topics() or {
		assert false, 'list_topics 실패: ${err}'
		return
	}

	assert topics.len == 0
}

fn test_list_topics_multiple() {
	// 여러 토픽을 생성한 후 모두 목록에 나타나야 한다.
	mut manager, _ := create_topic_test_manager()

	for name in ['topic-a', 'topic-b', 'topic-c'] {
		req := CreateTopicRequest{
			name:           name
			num_partitions: 1
			configs:        map[string]string{}
		}
		manager.create_topic(req)
	}

	topics := manager.list_topics() or {
		assert false, 'list_topics 실패: ${err}'
		return
	}

	assert topics.len == 3, '토픽 수가 3이어야 한다, 실제: ${topics.len}'
}

// ============================================================
// get_topic 테스트
// ============================================================

fn test_get_topic_success() {
	// 생성된 토픽의 메타데이터를 조회할 수 있어야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'get-me'
		num_partitions: 5
		configs:        map[string]string{}
	}
	manager.create_topic(req)

	meta := manager.get_topic('get-me') or {
		assert false, '토픽 조회 실패: ${err}'
		return
	}

	assert meta.name == 'get-me'
	assert meta.partition_count == 5
}

fn test_get_topic_not_found() {
	// 존재하지 않는 토픽 조회는 오류를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	manager.get_topic('no-such-topic') or {
		assert err.msg().len > 0
		return
	}
	assert false, '존재하지 않는 토픽 조회는 오류를 반환해야 한다'
}

// ============================================================
// add_partitions 테스트
// ============================================================

fn test_add_partitions_success() {
	// 파티션 수를 증가시키면 성공해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'expandable'
		num_partitions: 2
		configs:        map[string]string{}
	}
	manager.create_topic(req)

	manager.add_partitions('expandable', 5) or {
		assert false, '파티션 추가 실패: ${err}'
		return
	}

	meta := manager.get_topic('expandable') or {
		assert false, '토픽 조회 실패: ${err}'
		return
	}
	assert meta.partition_count == 5, '파티션 수가 5여야 한다, 실제: ${meta.partition_count}'
}

fn test_add_partitions_same_count_rejected() {
	// 현재와 동일한 파티션 수로 추가하면 오류를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'no-change'
		num_partitions: 3
		configs:        map[string]string{}
	}
	manager.create_topic(req)

	manager.add_partitions('no-change', 3) or {
		assert err.msg().contains('greater')
		return
	}
	assert false, '동일 파티션 수 추가는 오류를 반환해야 한다'
}

fn test_add_partitions_decrease_rejected() {
	// 파티션 수를 줄이면 오류를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	req := CreateTopicRequest{
		name:           'no-shrink'
		num_partitions: 5
		configs:        map[string]string{}
	}
	manager.create_topic(req)

	manager.add_partitions('no-shrink', 2) or {
		assert err.msg().contains('greater')
		return
	}
	assert false, '파티션 수 감소는 오류를 반환해야 한다'
}

fn test_add_partitions_nonexistent_topic() {
	// 존재하지 않는 토픽에 파티션 추가는 오류를 반환해야 한다.
	mut manager, _ := create_topic_test_manager()

	manager.add_partitions('no-topic', 5) or {
		assert err.msg().len > 0
		return
	}
	assert false, '존재하지 않는 토픽에 파티션 추가는 오류를 반환해야 한다'
}
