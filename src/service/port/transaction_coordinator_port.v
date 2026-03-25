// Transaction coordinator interface following DIP.
// Abstracts transaction lifecycle management from the concrete TransactionCoordinator.
module port

import domain

/// TransactionCoordinatorPort abstracts Kafka transaction coordination.
/// Implemented by service/transaction TransactionCoordinator.
pub interface TransactionCoordinatorPort {
mut:
	/// Retrieves transaction metadata by transactional_id.
	get_transaction(transactional_id string) !domain.TransactionMetadata

	/// Initializes a producer ID for idempotent/transactional producers.
	init_producer_id(transactional_id ?string, transaction_timeout_ms i32, producer_id i64, producer_epoch i16) !domain.InitProducerIdResult

	/// Adds partitions to an ongoing transaction.
	add_partitions_to_txn(transactional_id string, producer_id i64, producer_epoch i16, partitions []domain.TopicPartition) !

	/// Associates a consumer group with a transaction for offset commits.
	add_offsets_to_txn(transactional_id string, producer_id i64, producer_epoch i16, group_id string) !

	/// Writes transaction markers (commit/abort) to partition leaders.
	write_txn_markers(markers []domain.WriteTxnMarker) []domain.WriteTxnMarkerResult

	/// Ends (commits or aborts) a transaction.
	end_txn(transactional_id string, producer_id i64, producer_epoch i16, result domain.TransactionResult) !
}
