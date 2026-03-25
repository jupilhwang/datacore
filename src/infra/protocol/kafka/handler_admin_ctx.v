// AdminSubHandler owns the dependencies specific to admin/cluster operations.
//
// Holds a reference to HandlerContext for shared state, plus the
// broker registry and partition assigner needed by cluster management,
// topic administration, and config operations.
module kafka

import service.port

/// AdminSubHandler groups the dependencies needed for admin request handling.
/// The main Handler delegates admin-related work through this sub-handler.
pub struct AdminSubHandler {
pub mut:
	ctx                &HandlerContext
	broker_registry    ?port.BrokerRegistryPort
	partition_assigner ?port.PartitionAssignerPort
}
