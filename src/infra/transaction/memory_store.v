/// 인프라 레이어 - 메모리 기반 트랜잭션 저장소
module transaction

import domain
import sync

/// MemoryTransactionStore는 인메모리 저장소로 TransactionStore 인터페이스를 구현합니다
pub struct MemoryTransactionStore {
mut:
	transactions map[string]domain.TransactionMetadata
	lock         sync.RwMutex
}

/// 새로운 인메모리 트랜잭션 저장소를 생성합니다
pub fn new_memory_transaction_store() &MemoryTransactionStore {
	return &MemoryTransactionStore{
		transactions: map[string]domain.TransactionMetadata{}
	}
}

/// transactional_id로 트랜잭션 메타데이터를 조회합니다
pub fn (mut s MemoryTransactionStore) get_transaction(transactional_id string) !domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if meta := s.transactions[transactional_id] {
		return meta
	}
	return error('transactional_id not found: ${transactional_id}')
}

/// 트랜잭션 메타데이터를 저장합니다
pub fn (mut s MemoryTransactionStore) save_transaction(metadata domain.TransactionMetadata) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	s.transactions[metadata.transactional_id] = metadata
}

/// 트랜잭션 메타데이터를 삭제합니다
pub fn (mut s MemoryTransactionStore) delete_transaction(transactional_id string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if transactional_id !in s.transactions {
		return error('transactional_id not found: ${transactional_id}')
	}
	s.transactions.delete(transactional_id)
}

/// 모든 트랜잭션 목록을 반환합니다
pub fn (mut s MemoryTransactionStore) list_transactions() ![]domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	mut result := []domain.TransactionMetadata{}
	for _, meta in s.transactions {
		result << meta
	}
	return result
}
