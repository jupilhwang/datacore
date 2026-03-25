// ShareGroupSubHandler is the sub-handler type for share group operations (KIP-932).
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// ShareGroupSubHandler is the sub-handler type for share group request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct ShareGroupSubHandler {
}
