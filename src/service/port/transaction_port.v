// Used to support Kafka's transaction features.
module port

import domain

/// TransactionStore is an interface for storing transaction metadata.
/// Used by the transaction coordinator to persist transaction state.
pub interface TransactionStore {
mut:
	/// Retrieves transaction metadata by transactional_id.
	get_transaction(transactional_id string) !domain.TransactionMetadata

	/// Saves transaction metadata (create or update).
	save_transaction(metadata domain.TransactionMetadata) !

	/// Deletes transaction metadata.
	delete_transaction(transactional_id string) !

	/// Returns a list of all transactions (for debugging/monitoring).
	list_transactions() ![]domain.TransactionMetadata
}
