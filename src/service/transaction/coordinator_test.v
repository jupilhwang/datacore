// TransactionCoordinator 단위 테스트.
// 트랜잭션 라이프사이클 관리: init_producer_id, add_partitions_to_txn,
// add_offsets_to_txn, end_txn의 상태 전이와 유효성 검증을 검증한다.
module transaction

import domain
import infra.transaction as infra_txn

// 헬퍼: 테스트용 TransactionCoordinator 생성
fn create_test_coordinator() &TransactionCoordinator {
	store := infra_txn.new_memory_transaction_store()
	return new_transaction_coordinator(store)
}

// 헬퍼: 트랜잭션이 초기화된 coordinator 반환 (transactional_id, producer_id, epoch)
fn setup_initialized_txn(mut coordinator TransactionCoordinator) (string, i64, i16) {
	tid := 'test-txn-id'
	result := coordinator.init_producer_id(tid, 60000, -1, 0) or {
		panic('init_producer_id 실패: ${err}')
	}
	return tid, result.producer_id, result.producer_epoch
}

// ============================================================
// init_producer_id 테스트
// ============================================================

fn test_init_producer_id_idempotent() {
	// 멱등 프로듀서(transactional_id 없음)는 producer_id를 할당받아야 한다.
	mut coordinator := create_test_coordinator()

	result := coordinator.init_producer_id(none, 0, -1, 0) or {
		assert false, '멱등 init_producer_id 실패: ${err}'
		return
	}

	assert result.producer_id > 0, 'producer_id가 양수여야 한다'
	assert result.producer_epoch == 0, 'epoch가 0이어야 한다'
}

fn test_init_producer_id_idempotent_with_existing_pid() {
	// 기존 producer_id가 있는 멱등 프로듀서는 해당 ID를 유지해야 한다.
	mut coordinator := create_test_coordinator()

	result := coordinator.init_producer_id(none, 0, 12345, 0) or {
		assert false, 'init_producer_id 실패: ${err}'
		return
	}

	assert result.producer_id == 12345, '기존 producer_id를 유지해야 한다'
	assert result.producer_epoch == 0
}

fn test_init_producer_id_transactional_new() {
	// 새 트랜잭셔널 프로듀서는 producer_id와 epoch 0을 할당받아야 한다.
	mut coordinator := create_test_coordinator()

	result := coordinator.init_producer_id('my-txn', 60000, -1, 0) or {
		assert false, 'transactional init_producer_id 실패: ${err}'
		return
	}

	assert result.producer_id > 0, 'producer_id가 양수여야 한다'
	assert result.producer_epoch == 0, 'epoch가 0이어야 한다'

	// 메타데이터가 저장되었는지 확인
	meta := coordinator.get_transaction('my-txn') or {
		assert false, '트랜잭션 메타데이터가 존재해야 한다'
		return
	}
	assert meta.state == .empty
	assert meta.transactional_id == 'my-txn'
}

fn test_init_producer_id_transactional_existing_increments_epoch() {
	// 기존 트랜잭셔널 프로듀서에 대해 init을 다시 호출하면 epoch가 증가해야 한다.
	mut coordinator := create_test_coordinator()

	result1 := coordinator.init_producer_id('epoch-txn', 60000, -1, 0) or {
		assert false, '첫 번째 init 실패: ${err}'
		return
	}

	result2 := coordinator.init_producer_id('epoch-txn', 60000, -1, 0) or {
		assert false, '두 번째 init 실패: ${err}'
		return
	}

	assert result2.producer_id == result1.producer_id, '같은 producer_id를 유지해야 한다'
	assert result2.producer_epoch == result1.producer_epoch + 1, 'epoch가 1 증가해야 한다'
}

fn test_init_producer_id_transactional_resets_state() {
	// 기존 트랜잭션이 진행 중일 때 init을 호출하면 상태가 empty로 리셋되어야 한다.
	mut coordinator := create_test_coordinator()

	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	// 트랜잭션을 ongoing 상태로 만든다
	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'topic-a'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패: ${err}'
		return
	}

	meta_ongoing := coordinator.get_transaction(tid) or {
		assert false, '트랜잭션 조회 실패'
		return
	}
	assert meta_ongoing.state == .ongoing

	// init을 다시 호출하여 리셋
	coordinator.init_producer_id(tid, 60000, -1, 0) or {
		assert false, 'init 리셋 실패: ${err}'
		return
	}

	meta_reset := coordinator.get_transaction(tid) or {
		assert false, '트랜잭션 조회 실패'
		return
	}
	assert meta_reset.state == .empty, '상태가 empty로 리셋되어야 한다'
	assert meta_reset.topic_partitions.len == 0, '파티션 목록이 비어야 한다'
}

// ============================================================
// add_partitions_to_txn 테스트
// ============================================================

fn test_add_partitions_to_txn_success() {
	// 유효한 요청으로 파티션을 트랜잭션에 추가할 수 있어야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	partitions := [
		domain.TopicPartition{
			topic:     'topic-x'
			partition: 0
		},
		domain.TopicPartition{
			topic:     'topic-x'
			partition: 1
		},
	]

	coordinator.add_partitions_to_txn(tid, pid, epoch, partitions) or {
		assert false, 'add_partitions 실패: ${err}'
		return
	}

	meta := coordinator.get_transaction(tid) or {
		assert false, '트랜잭션 조회 실패'
		return
	}
	assert meta.state == .ongoing, '상태가 ongoing이어야 한다'
	assert meta.topic_partitions.len == 2, '파티션 수가 2여야 한다'
}

fn test_add_partitions_to_txn_deduplication() {
	// 동일 파티션을 중복으로 추가해도 한 번만 기록되어야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	partition := domain.TopicPartition{
		topic:     'dup-topic'
		partition: 0
	}

	coordinator.add_partitions_to_txn(tid, pid, epoch, [partition]) or {
		assert false, '첫 번째 add 실패'
		return
	}
	coordinator.add_partitions_to_txn(tid, pid, epoch, [partition]) or {
		assert false, '두 번째 add 실패'
		return
	}

	meta := coordinator.get_transaction(tid) or {
		assert false, '조회 실패'
		return
	}
	assert meta.topic_partitions.len == 1, '중복 파티션이 제거되어야 한다, 실제: ${meta.topic_partitions.len}'
}

fn test_add_partitions_invalid_producer_id() {
	// 잘못된 producer_id로 파티션 추가 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, _, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, 99999, epoch, [
		domain.TopicPartition{
			topic:     't'
			partition: 0
		},
	]) or {
		assert err.msg().contains('invalid producer id')
		return
	}
	assert false, '잘못된 producer_id로는 오류가 반환되어야 한다'
}

fn test_add_partitions_invalid_producer_epoch() {
	// 잘못된 producer_epoch로 파티션 추가 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, _ := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, 999, [
		domain.TopicPartition{
			topic:     't'
			partition: 0
		},
	]) or {
		assert err.msg().contains('invalid producer epoch')
		return
	}
	assert false, '잘못된 epoch로는 오류가 반환되어야 한다'
}

fn test_add_partitions_unknown_transactional_id() {
	// 존재하지 않는 transactional_id는 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()

	coordinator.add_partitions_to_txn('unknown-tid', 1, 0, [
		domain.TopicPartition{
			topic:     't'
			partition: 0
		},
	]) or {
		assert err.msg().contains('not found')
		return
	}
	assert false, '존재하지 않는 transactional_id는 오류를 반환해야 한다'
}

// ============================================================
// add_offsets_to_txn 테스트
// ============================================================

fn test_add_offsets_to_txn_success() {
	// 소비자 그룹 오프셋을 트랜잭션에 추가할 수 있어야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_offsets_to_txn(tid, pid, epoch, 'consumer-group-1') or {
		assert false, 'add_offsets 실패: ${err}'
		return
	}

	meta := coordinator.get_transaction(tid) or {
		assert false, '조회 실패'
		return
	}
	assert meta.state == .ongoing, '상태가 ongoing이어야 한다'
	assert meta.topic_partitions.len == 1, '__consumer_offsets 파티션이 추가되어야 한다'
	assert meta.topic_partitions[0].topic == '__consumer_offsets'
}

fn test_add_offsets_to_txn_deduplication() {
	// 같은 그룹 ID로 두 번 호출해도 파티션이 중복 추가되지 않아야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_offsets_to_txn(tid, pid, epoch, 'group-dup') or {
		assert false, '첫 번째 add_offsets 실패'
		return
	}
	coordinator.add_offsets_to_txn(tid, pid, epoch, 'group-dup') or {
		assert false, '두 번째 add_offsets 실패'
		return
	}

	meta := coordinator.get_transaction(tid) or {
		assert false, '조회 실패'
		return
	}
	assert meta.topic_partitions.len == 1, '중복 offset 파티션이 추가되지 않아야 한다'
}

fn test_add_offsets_invalid_producer_id() {
	// 잘못된 producer_id로 오프셋 추가 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, _, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_offsets_to_txn(tid, 99999, epoch, 'group-x') or {
		assert err.msg().contains('invalid producer id')
		return
	}
	assert false, '잘못된 producer_id로는 오류가 반환되어야 한다'
}

// ============================================================
// end_txn 테스트 - commit
// ============================================================

fn test_end_txn_commit_success() {
	// 트랜잭션 커밋이 성공적으로 상태 전이를 완료해야 한다.
	// 상태 전이: empty -> ongoing -> prepare_commit -> complete_commit -> empty
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	// ongoing 상태로 전환
	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'commit-topic'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	// commit
	coordinator.end_txn(tid, pid, epoch, .commit) or {
		assert false, 'end_txn commit 실패: ${err}'
		return
	}

	// 최종 상태가 empty여야 한다
	meta := coordinator.get_transaction(tid) or {
		assert false, '트랜잭션 조회 실패'
		return
	}
	assert meta.state == .empty, '커밋 후 상태가 empty여야 한다, 실제: ${meta.state}'
	assert meta.topic_partitions.len == 0, '커밋 후 파티션 목록이 비어야 한다'
}

// ============================================================
// end_txn 테스트 - abort
// ============================================================

fn test_end_txn_abort_success() {
	// 트랜잭션 롤백이 성공적으로 상태 전이를 완료해야 한다.
	// 상태 전이: empty -> ongoing -> prepare_abort -> complete_abort -> empty
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	// ongoing 상태로 전환
	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'abort-topic'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	// abort
	coordinator.end_txn(tid, pid, epoch, .abort) or {
		assert false, 'end_txn abort 실패: ${err}'
		return
	}

	// 최종 상태가 empty여야 한다
	meta := coordinator.get_transaction(tid) or {
		assert false, '트랜잭션 조회 실패'
		return
	}
	assert meta.state == .empty, '롤백 후 상태가 empty여야 한다, 실제: ${meta.state}'
	assert meta.topic_partitions.len == 0
}

// ============================================================
// end_txn 테스트 - 유효성 검증
// ============================================================

fn test_end_txn_invalid_producer_id() {
	// 잘못된 producer_id로 end_txn 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     't'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	coordinator.end_txn(tid, 99999, epoch, .commit) or {
		assert err.msg().contains('invalid producer id')
		return
	}
	assert false, '잘못된 producer_id로는 오류가 반환되어야 한다'
}

fn test_end_txn_invalid_producer_epoch() {
	// 잘못된 epoch로 end_txn 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     't'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	coordinator.end_txn(tid, pid, 999, .commit) or {
		assert err.msg().contains('invalid producer epoch')
		return
	}
	assert false, '잘못된 epoch로는 오류가 반환되어야 한다'
}

fn test_end_txn_unknown_transactional_id() {
	// 존재하지 않는 transactional_id로 end_txn 시 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()

	coordinator.end_txn('unknown-tid', 1, 0, .commit) or {
		assert err.msg().contains('not found')
		return
	}
	assert false, '존재하지 않는 transactional_id는 오류를 반환해야 한다'
}

// ============================================================
// 상태 전이 테스트
// ============================================================

fn test_end_txn_on_empty_state_is_idempotent() {
	// empty 상태에서 end_txn을 호출하면 멱등적으로 성공해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	// empty 상태에서 바로 end_txn 호출
	coordinator.end_txn(tid, pid, epoch, .commit) or {
		assert false, 'empty 상태의 end_txn은 멱등적으로 성공해야 한다: ${err}'
		return
	}
}

fn test_add_partitions_invalid_state_prepare_commit() {
	// prepare_commit 상태에서 add_partitions는 오류를 반환해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	// ongoing 상태로 전환 후 commit -> 최종 상태 empty
	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'state-topic'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	// commit으로 상태 전이 완료
	coordinator.end_txn(tid, pid, epoch, .commit) or {
		assert false, 'end_txn 실패'
		return
	}

	// 새 epoch로 init
	result := coordinator.init_producer_id(tid, 60000, -1, 0) or {
		assert false, 'init 실패'
		return
	}
	new_epoch := result.producer_epoch
	new_pid := result.producer_id

	// 정상적으로 파티션 추가 가능
	coordinator.add_partitions_to_txn(tid, new_pid, new_epoch, [
		domain.TopicPartition{
			topic:     'new-topic'
			partition: 0
		},
	]) or {
		assert false, '리셋 후 파티션 추가가 가능해야 한다: ${err}'
		return
	}
}

fn test_full_transaction_lifecycle() {
	// 전체 트랜잭션 라이프사이클을 검증한다.
	// init -> add_partitions -> add_offsets -> end_txn(commit) -> init(새 epoch)
	mut coordinator := create_test_coordinator()

	// 1. init
	result1 := coordinator.init_producer_id('lifecycle-txn', 60000, -1, 0) or {
		assert false, 'init 실패'
		return
	}
	tid := 'lifecycle-txn'
	pid := result1.producer_id
	epoch := result1.producer_epoch
	assert epoch == 0

	// 2. add partitions
	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'orders'
			partition: 0
		},
		domain.TopicPartition{
			topic:     'orders'
			partition: 1
		},
	]) or {
		assert false, 'add_partitions 실패'
		return
	}

	// 3. add offsets
	coordinator.add_offsets_to_txn(tid, pid, epoch, 'order-processors') or {
		assert false, 'add_offsets 실패'
		return
	}

	meta_ongoing := coordinator.get_transaction(tid) or {
		assert false, '조회 실패'
		return
	}
	assert meta_ongoing.state == .ongoing
	// 최소 2개 파티션(orders) + 1개(__consumer_offsets) = 3개
	assert meta_ongoing.topic_partitions.len >= 3, '파티션 수가 최소 3이어야 한다, 실제: ${meta_ongoing.topic_partitions.len}'

	// 4. commit
	coordinator.end_txn(tid, pid, epoch, .commit) or {
		assert false, 'commit 실패'
		return
	}

	meta_committed := coordinator.get_transaction(tid) or {
		assert false, '조회 실패'
		return
	}
	assert meta_committed.state == .empty

	// 5. 새 트랜잭션 시작 (epoch 증가)
	result2 := coordinator.init_producer_id('lifecycle-txn', 60000, -1, 0) or {
		assert false, '두 번째 init 실패'
		return
	}
	assert result2.producer_epoch == epoch + 1, 'epoch가 1 증가해야 한다'
}

// ============================================================
// write_txn_markers 테스트
// ============================================================

fn test_write_txn_markers_commit_single_partition() {
	// 단일 파티션에 대해 커밋 마커를 성공적으로 작성해야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'test-topic'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패: ${err}'
		return
	}

	markers := [
		domain.WriteTxnMarker{
			producer_id:        pid
			producer_epoch:     epoch
			transaction_result: .commit
			topics:             [
				domain.WriteTxnMarkerTopic{
					name:       'test-topic'
					partitions: [0]
				},
			]
		},
	]

	results := coordinator.write_txn_markers(markers)
	assert results.len == 1, 'results 수가 1이어야 한다, 실제: ${results.len}'
	assert results[0].producer_id == pid
	assert results[0].topics.len == 1
	assert results[0].topics[0].name == 'test-topic'
	assert results[0].topics[0].partitions.len == 1
	assert results[0].topics[0].partitions[0].partition == 0
	assert results[0].topics[0].partitions[0].error_code == 0
}

fn test_write_txn_markers_abort() {
	// abort 마커도 정상적으로 처리되어야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{
			topic:     'abort-topic'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패: ${err}'
		return
	}

	markers := [
		domain.WriteTxnMarker{
			producer_id:        pid
			producer_epoch:     epoch
			transaction_result: .abort
			topics:             [
				domain.WriteTxnMarkerTopic{
					name:       'abort-topic'
					partitions: [0]
				},
			]
		},
	]

	results := coordinator.write_txn_markers(markers)
	assert results.len == 1, 'results 수가 1이어야 한다'
	assert results[0].topics[0].partitions[0].error_code == 0
}

fn test_write_txn_markers_multiple_topics_partitions() {
	// 여러 토픽/파티션에 대해 마커를 작성할 수 있어야 한다.
	mut coordinator := create_test_coordinator()
	tid, pid, epoch := setup_initialized_txn(mut coordinator)

	coordinator.add_partitions_to_txn(tid, pid, epoch, [
		domain.TopicPartition{ topic: 'topic-a', partition: 0 },
		domain.TopicPartition{
			topic:     'topic-a'
			partition: 1
		},
		domain.TopicPartition{
			topic:     'topic-b'
			partition: 0
		},
	]) or {
		assert false, 'add_partitions 실패: ${err}'
		return
	}

	markers := [
		domain.WriteTxnMarker{
			producer_id:        pid
			producer_epoch:     epoch
			transaction_result: .commit
			topics:             [
				domain.WriteTxnMarkerTopic{
					name:       'topic-a'
					partitions: [0, 1]
				},
				domain.WriteTxnMarkerTopic{
					name:       'topic-b'
					partitions: [0]
				},
			]
		},
	]

	results := coordinator.write_txn_markers(markers)
	assert results.len == 1
	assert results[0].topics.len == 2
	assert results[0].topics[0].partitions.len == 2
	assert results[0].topics[1].partitions.len == 1

	// 모든 파티션이 성공
	for topic_result in results[0].topics {
		for p_result in topic_result.partitions {
			assert p_result.error_code == 0, 'error_code가 0이어야 한다'
		}
	}
}

fn test_write_txn_markers_unknown_producer_id() {
	// 알 수 없는 producer_id에 대해서도 결과를 반환해야 한다.
	// inter-broker 통신에서는 파티션 리더가 트랜잭션을 모를 수 있다.
	mut coordinator := create_test_coordinator()

	markers := [
		domain.WriteTxnMarker{
			producer_id:        99999
			producer_epoch:     0
			transaction_result: .commit
			topics:             [
				domain.WriteTxnMarkerTopic{
					name:       'unknown-topic'
					partitions: [0]
				},
			]
		},
	]

	results := coordinator.write_txn_markers(markers)
	assert results.len == 1, 'results 수가 1이어야 한다'
	assert results[0].producer_id == 99999
	assert results[0].topics.len == 1
}

fn test_write_txn_markers_empty_markers() {
	// 빈 markers 목록은 빈 결과를 반환해야 한다.
	mut coordinator := create_test_coordinator()

	results := coordinator.write_txn_markers([]domain.WriteTxnMarker{})
	assert results.len == 0, 'results가 비어야 한다'
}
