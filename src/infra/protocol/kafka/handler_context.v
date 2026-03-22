// HandlerContext holds shared dependencies used by all sub-handlers.
//
// This struct centralizes the common state that every Kafka API handler
// group needs, avoiding redundant fields across sub-handler structs.
// Each sub-handler embeds a reference to HandlerContext plus any
// domain-specific dependencies it requires.
module kafka

import infra.compression
import infra.observability
import service.port

/// HandlerContext holds the shared dependencies that most or all sub-handlers need.
/// Sub-handler structs embed a reference to this context rather than duplicating fields.
pub struct HandlerContext {
pub mut:
	broker_id           i32
	host                string
	port                i32
	cluster_id          string
	storage             port.StoragePort
	logger              &observability.Logger
	metrics             &observability.ProtocolMetrics
	compression_service &compression.CompressionService
}
