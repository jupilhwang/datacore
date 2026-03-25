// ShareGroupSubHandler owns the dependencies specific to share group operations (KIP-932).
//
// Holds a reference to HandlerContext for shared state, plus the
// share group coordinator needed by ShareGroupHeartbeat, ShareFetch,
// ShareAcknowledge, and share group state management.
module kafka

import service.port

/// ShareGroupSubHandler groups the dependencies needed for share group request handling.
/// The main Handler delegates share-group-related work through this sub-handler.
pub struct ShareGroupSubHandler {
pub mut:
	ctx                     &HandlerContext
	share_group_coordinator ?port.ShareGroupCoordinatorPort
}
