// GroupSubHandler is the sub-handler type for consumer group operations.
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// GroupSubHandler is the sub-handler type for consumer group request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct GroupSubHandler {
}
