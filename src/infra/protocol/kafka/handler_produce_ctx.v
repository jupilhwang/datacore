// ProduceSubHandler owns the dependencies specific to produce operations.
//
// Holds a reference to HandlerContext for shared state, plus the
// transaction coordinator and schema registry needed by the produce path.
module kafka

import service.schema
import service.transaction

/// ProduceSubHandler groups the dependencies needed for produce request handling.
/// The main Handler delegates produce-related work through this sub-handler.
pub struct ProduceSubHandler {
pub mut:
	ctx             &HandlerContext
	txn_coordinator ?transaction.TransactionCoordinator
	schema_registry ?&schema.SchemaRegistry
}
