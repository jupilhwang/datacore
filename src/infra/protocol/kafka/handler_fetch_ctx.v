// FetchSubHandler is the sub-handler type for fetch operations.
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// FetchSubHandler is the sub-handler type for fetch request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct FetchSubHandler {
}
