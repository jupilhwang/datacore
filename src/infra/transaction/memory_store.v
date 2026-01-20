// Infra Layer - Memory-based Transaction Store
module transaction

import domain
import sync

// MemoryTransactionStore implements TransactionStore interface with in-memory storage
pub struct MemoryTransactionStore {
mut:
	transactions map[string]domain.TransactionMetadata
	lock         sync.RwMutex
}

// new_memory_transaction_store creates a new in-memory transaction store
pub fn new_memory_transaction_store() &MemoryTransactionStore {
	return &MemoryTransactionStore{
		transactions: map[string]domain.TransactionMetadata{}
	}
}

// get_transaction retrieves transaction metadata by transactional ID
pub fn (mut s MemoryTransactionStore) get_transaction(transactional_id string) !domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if meta := s.transactions[transactional_id] {
		return meta
	}
	return error('transactional_id not found: ${transactional_id}')
}

// save_transaction saves transaction metadata
pub fn (mut s MemoryTransactionStore) save_transaction(metadata domain.TransactionMetadata) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	s.transactions[metadata.transactional_id] = metadata
}

// delete_transaction deletes transaction metadata
pub fn (mut s MemoryTransactionStore) delete_transaction(transactional_id string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if transactional_id !in s.transactions {
		return error('transactional_id not found: ${transactional_id}')
	}
	s.transactions.delete(transactional_id)
}

// list_transactions lists all transactions
pub fn (mut s MemoryTransactionStore) list_transactions() ![]domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	mut result := []domain.TransactionMetadata{}
	for _, meta in s.transactions {
		result << meta
	}
	return result
}
