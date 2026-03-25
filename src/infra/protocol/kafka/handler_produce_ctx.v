// ProduceSubHandler is the sub-handler type for produce operations.
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// ProduceSubHandler is the sub-handler type for produce request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct ProduceSubHandler {
}
