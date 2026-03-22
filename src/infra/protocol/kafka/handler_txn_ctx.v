// TransactionSubHandler owns the dependencies specific to transaction operations.
//
// Holds a reference to HandlerContext for shared state, plus the
// transaction coordinator needed by InitProducerId, AddPartitionsToTxn,
// AddOffsetsToTxn, EndTxn, WriteTxnMarkers, and TxnOffsetCommit.
module kafka

import service.transaction

/// TransactionSubHandler groups the dependencies needed for transaction request handling.
/// The main Handler delegates transaction-related work through this sub-handler.
pub struct TransactionSubHandler {
pub mut:
	ctx             &HandlerContext
	txn_coordinator ?transaction.TransactionCoordinator
}
