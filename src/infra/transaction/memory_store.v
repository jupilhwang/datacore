/// Infrastructure layer - in-memory transaction store
module transaction

import domain
import sync

/// MemoryTransactionStore implements the TransactionStore interface using an in-memory store
pub struct MemoryTransactionStore {
mut:
	transactions map[string]domain.TransactionMetadata
	lock         sync.RwMutex
}

/// Creates a new in-memory transaction store
pub fn new_memory_transaction_store() &MemoryTransactionStore {
	return &MemoryTransactionStore{
		transactions: map[string]domain.TransactionMetadata{}
	}
}

/// Retrieves transaction metadata by transactional_id
pub fn (mut s MemoryTransactionStore) get_transaction(transactional_id string) !domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if meta := s.transactions[transactional_id] {
		return meta
	}
	return error('transactional_id not found: ${transactional_id}')
}

/// Saves transaction metadata
pub fn (mut s MemoryTransactionStore) save_transaction(metadata domain.TransactionMetadata) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	s.transactions[metadata.transactional_id] = metadata
}

/// Deletes transaction metadata
pub fn (mut s MemoryTransactionStore) delete_transaction(transactional_id string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if transactional_id !in s.transactions {
		return error('transactional_id not found: ${transactional_id}')
	}
	s.transactions.delete(transactional_id)
}

/// Returns a list of all transactions
pub fn (mut s MemoryTransactionStore) list_transactions() ![]domain.TransactionMetadata {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	mut result := []domain.TransactionMetadata{}
	for _, meta in s.transactions {
		result << meta
	}
	return result
}
