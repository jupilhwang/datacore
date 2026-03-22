// GroupSubHandler owns the dependencies specific to consumer group operations.
//
// Holds a reference to HandlerContext for shared state, plus the
// offset manager needed by group coordination, offset commit/fetch,
// and consumer group management.
module kafka

import service.offset

/// GroupSubHandler groups the dependencies needed for consumer group request handling.
/// The main Handler delegates group-related work through this sub-handler.
pub struct GroupSubHandler {
pub mut:
	ctx            &HandlerContext
	offset_manager &offset.OffsetManager
}
