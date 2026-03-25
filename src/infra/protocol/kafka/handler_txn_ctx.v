// TransactionSubHandler is the sub-handler type for transaction operations.
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// TransactionSubHandler is the sub-handler type for transaction request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct TransactionSubHandler {
}
