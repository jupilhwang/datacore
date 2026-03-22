// FetchSubHandler owns the dependencies specific to fetch operations.
//
// Currently only needs the shared HandlerContext, since fetch logic
// relies on storage and compression from the common context.
module kafka

/// FetchSubHandler groups the dependencies needed for fetch request handling.
/// The main Handler delegates fetch-related work through this sub-handler.
pub struct FetchSubHandler {
pub mut:
	ctx &HandlerContext
}
