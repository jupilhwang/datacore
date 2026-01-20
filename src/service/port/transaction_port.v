module port

import domain

// TransactionStore defines the interface for storing transaction metadata
pub interface TransactionStore {
mut:
	// Get transaction metadata by transactional ID
	get_transaction(transactional_id string) !domain.TransactionMetadata

	// Save transaction metadata (create or update)
	save_transaction(metadata domain.TransactionMetadata) !

	// Delete transaction metadata
	delete_transaction(transactional_id string) !

	// List all transactions (for debugging/monitoring)
	list_transactions() ![]domain.TransactionMetadata
}
