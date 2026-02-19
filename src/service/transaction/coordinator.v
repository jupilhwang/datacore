// Kafka 트랜잭션의 생명주기를 관리합니다.
// Exactly-once 시맨틱을 보장하기 위한 트랜잭션 상태 전이를 처리합니다.
module transaction

import domain
import service.port
import time
import rand

/// TransactionCoordinator는 트랜잭션 프로듀서와 트랜잭션을 관리합니다.
/// 트랜잭션 시작, 파티션 추가, 커밋/롤백을 담당합니다.
pub struct TransactionCoordinator {
mut:
	store port.TransactionStore
}

/// new_transaction_coordinator는 새로운 트랜잭션 코디네이터를 생성합니다.
pub fn new_transaction_coordinator(store port.TransactionStore) &TransactionCoordinator {
	return &TransactionCoordinator{
		store: store
	}
}

/// get_transaction은 주어진 transactional_id에 대한 트랜잭션 메타데이터를 반환합니다.
pub fn (mut c TransactionCoordinator) get_transaction(transactional_id string) !domain.TransactionMetadata {
	return c.store.get_transaction(transactional_id)
}

/// init_producer_id는 트랜잭션 또는 멱등성 프로듀서를 위한 프로듀서 ID를 초기화합니다.
/// transactional_id: 트랜잭션 ID (멱등성만 사용 시 none)
/// transaction_timeout_ms: 트랜잭션 타임아웃 (ms)
/// producer_id: 기존 프로듀서 ID (-1이면 새로 생성)
/// producer_epoch: 기존 프로듀서 에포크
pub fn (mut c TransactionCoordinator) init_producer_id(transactional_id ?string, transaction_timeout_ms i32, producer_id i64, producer_epoch i16) !domain.InitProducerIdResult {
	// 1. 멱등성 프로듀서 (transactional_id 없음)
	if transactional_id == none {
		// 새 프로듀서 ID 생성
		new_pid := if producer_id == -1 {
			rand.i64()
		} else {
			producer_id
		}
		// 양수로 보장
		final_pid := if new_pid < 0 { -new_pid } else { new_pid }

		return domain.InitProducerIdResult{
			producer_id:    final_pid
			producer_epoch: 0
		}
	}

	// 2. 트랜잭션 프로듀서
	tid := transactional_id or { return error('transactional_id is required') }

	// 트랜잭션 메타데이터 존재 확인
	mut metadata := c.store.get_transaction(tid) or {
		// 새 메타데이터 생성
		new_pid := rand.i64()
		final_pid := if new_pid < 0 { -new_pid } else { new_pid }

		meta := domain.TransactionMetadata{
			transactional_id:          tid
			producer_id:               final_pid
			producer_epoch:            0
			txn_timeout_ms:            transaction_timeout_ms
			state:                     .empty
			topic_partitions:          []
			txn_start_timestamp:       time.now().unix_milli()
			txn_last_update_timestamp: time.now().unix_milli()
		}
		c.store.save_transaction(meta)!
		return domain.InitProducerIdResult{
			producer_id:    meta.producer_id
			producer_epoch: meta.producer_epoch
		}
	}

	// 기존 트랜잭션 - 에포크 증가
	// 트랜잭션이 진행 중이면 롤백해야 함 (암묵적 롤백)
	// 현재는 에포크만 증가하고 상태를 리셋
	new_epoch := metadata.producer_epoch + 1

	updated_meta := domain.TransactionMetadata{
		...metadata
		producer_epoch:            new_epoch
		state:                     .empty
		topic_partitions:          []
		txn_last_update_timestamp: time.now().unix_milli()
	}

	c.store.save_transaction(updated_meta)!

	return domain.InitProducerIdResult{
		producer_id:    updated_meta.producer_id
		producer_epoch: updated_meta.producer_epoch
	}
}

/// add_partitions_to_txn은 트랜잭션에 파티션을 추가합니다.
/// 트랜잭션에 포함될 토픽/파티션 목록을 등록합니다.
pub fn (mut c TransactionCoordinator) add_partitions_to_txn(transactional_id string, producer_id i64, producer_epoch i16, partitions []domain.TopicPartition) ! {
	// 1. 트랜잭션 메타데이터 조회
	mut meta := c.store.get_transaction(transactional_id) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	// 2. 프로듀서 ID 및 에포크 검증
	if meta.producer_id != producer_id {
		return error('invalid producer id')
	}
	if meta.producer_epoch != producer_epoch {
		return error('invalid producer epoch')
	}

	// 3. 상태 검증
	if meta.state != .empty && meta.state != .ongoing {
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. 파티션 추가
	mut new_partitions := meta.topic_partitions.clone()
	for p in partitions {
		// 중복 확인
		mut exists := false
		for existing in new_partitions {
			if existing.topic == p.topic && existing.partition == p.partition {
				exists = true
				break
			}
		}
		if !exists {
			new_partitions << p
		}
	}

	// 5. 상태 업데이트
	updated_meta := domain.TransactionMetadata{
		...meta
		state:                     .ongoing
		topic_partitions:          new_partitions
		txn_last_update_timestamp: time.now().unix_milli()
	}

	c.store.save_transaction(updated_meta)!
}

/// add_offsets_to_txn은 트랜잭션에 컨슈머 그룹 오프셋을 추가합니다.
/// 트랜잭션 커밋 시 오프셋도 함께 커밋됩니다.
pub fn (mut c TransactionCoordinator) add_offsets_to_txn(transactional_id string, producer_id i64, producer_epoch i16, group_id string) ! {
	// 1. 트랜잭션 메타데이터 조회
	mut meta := c.store.get_transaction(transactional_id) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	// 2. 프로듀서 ID 및 에포크 검증
	if meta.producer_id != producer_id {
		return error('invalid producer id')
	}
	if meta.producer_epoch != producer_epoch {
		return error('invalid producer epoch')
	}

	// 3. 상태 검증
	if meta.state != .empty && meta.state != .ongoing {
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. 이 그룹의 __consumer_offsets 파티션을 트랜잭션에 추가
	// 파티션은 group_id 해시 % 50 (기본 __consumer_offsets 파티션 수)로 결정
	group_partition := hash_group_id(group_id) % 50

	// __consumer_offsets 파티션을 트랜잭션에 추가
	mut new_partitions := meta.topic_partitions.clone()
	consumer_offsets_partition := domain.TopicPartition{
		topic:     '__consumer_offsets'
		partition: group_partition
	}

	// 이미 추가되었는지 확인
	mut already_added := false
	for tp in new_partitions {
		if tp.topic == '__consumer_offsets' && tp.partition == group_partition {
			already_added = true
			break
		}
	}

	if !already_added {
		new_partitions << consumer_offsets_partition
	}

	// 상태를 ongoing으로 업데이트하고 파티션 추가
	updated_meta := domain.TransactionMetadata{
		...meta
		state:                     .ongoing
		topic_partitions:          new_partitions
		txn_last_update_timestamp: time.now().unix_milli()
	}

	c.store.save_transaction(updated_meta)!
}

/// hash_group_id는 group_id의 해시를 계산하여 __consumer_offsets 파티션을 결정합니다.
/// Java의 String.hashCode()와 동등한 해시 함수를 사용합니다.
fn hash_group_id(group_id string) int {
	mut hash := u32(0)
	for c in group_id {
		hash = hash * 31 + u32(c)
	}
	// 양수로 변환
	return int(hash & 0x7fffffff)
}

/// end_txn은 트랜잭션을 종료합니다 (커밋 또는 롤백).
/// 트랜잭션 상태를 Prepare → Complete → Empty로 전이합니다.
pub fn (mut c TransactionCoordinator) end_txn(transactional_id string, producer_id i64, producer_epoch i16, result domain.TransactionResult) ! {
	// 1. 트랜잭션 메타데이터 조회
	mut meta := c.store.get_transaction(transactional_id) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	// 2. 프로듀서 ID 및 에포크 검증
	if meta.producer_id != producer_id {
		return error('invalid producer id')
	}
	if meta.producer_epoch != producer_epoch {
		return error('invalid producer epoch')
	}

	// 3. 상태 검증
	if meta.state != .ongoing {
		// 이미 empty 상태에서 커밋/롤백 시도 시 OK (멱등성)
		if meta.state == .empty {
			return
		}
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. 상태 전이
	// 실제 구현에서는 로그에 마커를 기록해야 함
	// 현재는 상태만 CompleteCommit/CompleteAbort → Empty로 업데이트

	// Prepare 상태로 전이
	prepare_state := if result == .commit {
		domain.TransactionState.prepare_commit
	} else {
		domain.TransactionState.prepare_abort
	}
	meta_prepare := domain.TransactionMetadata{
		...meta
		state:                     prepare_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_prepare)!

	// 마커 기록 (TODO: WriteTxnMarkers 구현)
	// 현재는 마커가 성공적으로 기록되었다고 가정

	// Complete 상태로 전이
	complete_state := if result == .commit {
		domain.TransactionState.complete_commit
	} else {
		domain.TransactionState.complete_abort
	}
	meta_complete := domain.TransactionMetadata{
		...meta_prepare
		state:                     complete_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_complete)!

	// Empty 상태로 전이 (트랜잭션 완료)
	meta_empty := domain.TransactionMetadata{
		...meta_complete
		state:                     .empty
		topic_partitions:          []
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_empty)!
}
