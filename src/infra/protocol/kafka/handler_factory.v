// Factory functions for creating Handler instances.
//
// Separated from handler.v to isolate concrete module imports.
// All construction helpers delegate to new_handler_from_config.
module kafka

import domain
import infra.compression
import infra.observability
import service.offset
import service.port

// StorageSubPorts wraps a StoragePort in a concrete struct, enabling V to satisfy
// narrower sub-interfaces (TopicStoragePort, OffsetStoragePort, etc.).
// V does not support narrowing one interface type to another directly; this struct
// delegates all sub-interface methods to the underlying StoragePort.
struct StorageSubPorts {
mut:
	s port.StoragePort
}

fn (mut w StorageSubPorts) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return w.s.create_topic(name, partitions, config)
}

fn (mut w StorageSubPorts) delete_topic(name string) ! {
	return w.s.delete_topic(name)
}

fn (mut w StorageSubPorts) list_topics() ![]domain.TopicMetadata {
	return w.s.list_topics()
}

fn (mut w StorageSubPorts) get_topic(name string) !domain.TopicMetadata {
	return w.s.get_topic(name)
}

fn (mut w StorageSubPorts) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return w.s.get_topic_by_id(topic_id)
}

fn (mut w StorageSubPorts) add_partitions(name string, new_count int) ! {
	return w.s.add_partitions(name, new_count)
}

fn (mut w StorageSubPorts) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	return w.s.commit_offsets(group_id, offsets)
}

fn (mut w StorageSubPorts) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return w.s.fetch_offsets(group_id, partitions)
}

// CompressionPortAdapter bridges the concrete CompressionService (which uses
// CompressionType enum) to the port.CompressionPort interface (which uses i16).
struct CompressionPortAdapter {
mut:
	inner &compression.CompressionService
}

fn (mut a CompressionPortAdapter) compress(data []u8, compression_type i16) ![]u8 {
	ct := compression.compression_type_from_i16(compression_type)!
	return a.inner.compress(data, ct)
}

fn (mut a CompressionPortAdapter) decompress(data []u8, compression_type i16) ![]u8 {
	ct := compression.compression_type_from_i16(compression_type)!
	return a.inner.decompress(data, ct)
}

/// new_handler_from_config creates a Handler from a HandlerConfig.
/// This is the single initialization path that all constructor helpers delegate to.
/// It builds the shared HandlerContext and wires each sub-handler with its
/// domain-specific dependencies.
pub fn new_handler_from_config(cfg HandlerConfig) Handler {
	ctx := &HandlerContext{
		broker_id:           cfg.broker_id
		host:                cfg.host
		port:                cfg.broker_port
		cluster_id:          cfg.cluster_id
		storage:             cfg.storage
		logger:              cfg.logger
		metrics:             cfg.metrics
		compression_service: cfg.compression_service
	}

	return Handler{
		broker_id:               cfg.broker_id
		host:                    cfg.host
		broker_port:             cfg.broker_port
		cluster_id:              cfg.cluster_id
		storage:                 cfg.storage
		offset_manager:          cfg.offset_manager
		broker_registry:         none
		partition_assigner:      none
		auth_manager:            cfg.auth_manager
		acl_manager:             cfg.acl_manager
		txn_coordinator:         cfg.txn_coordinator
		share_group_coordinator: cfg.share_coordinator
		logger:                  cfg.logger
		metrics:                 cfg.metrics
		compression_service:     cfg.compression_service
		audit_logger:            cfg.audit_logger
		handler_ctx:             ctx
		produce:                 &ProduceSubHandler{
			ctx:             ctx
			txn_coordinator: cfg.txn_coordinator
			schema_registry: none
		}
		fetch_handler:           &FetchSubHandler{
			ctx: ctx
		}
		auth:                    &AuthSubHandler{
			ctx:          ctx
			auth_manager: cfg.auth_manager
			acl_manager:  cfg.acl_manager
			audit_logger: cfg.audit_logger
		}
		txn:                     &TransactionSubHandler{
			ctx:             ctx
			txn_coordinator: cfg.txn_coordinator
		}
		group_handler:           &GroupSubHandler{
			ctx:            ctx
			offset_manager: cfg.offset_manager
		}
		admin:                   &AdminSubHandler{
			ctx: ctx
		}
		share:                   &ShareGroupSubHandler{
			ctx:                     ctx
			share_group_coordinator: cfg.share_coordinator
		}
	}
}

// build_default_handler_config creates a HandlerConfig with default logger, metrics,
// and offset manager using the concrete implementations.
fn build_default_handler_config(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, compression_svc &compression.CompressionService) HandlerConfig {
	logger := observability.get_named_logger('kafka.handler')
	mut sub := StorageSubPorts{
		s: storage
	}
	offset_mgr := offset.new_offset_manager(sub, sub, observability.new_logger_adapter(logger))
	metrics := observability.new_protocol_metrics()
	comp_adapter := &CompressionPortAdapter{
		inner: compression_svc
	}

	return HandlerConfig{
		broker_id:           broker_id
		host:                host
		broker_port:         broker_port
		cluster_id:          cluster_id
		storage:             storage
		compression_service: comp_adapter
		logger:              logger
		metrics:             metrics
		offset_manager:      offset_mgr
	}
}

/// new_handler creates a new Kafka protocol handler with storage only.
///
/// This is the basic handler with no authentication or ACL.
/// Suitable for development and testing environments.
pub fn new_handler(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, compression_service &compression.CompressionService) Handler {
	cfg := build_default_handler_config(broker_id, host, broker_port, cluster_id, storage,
		compression_service)
	return new_handler_from_config(cfg)
}

/// new_handler_with_auth creates a new Kafka protocol handler with storage and an auth manager.
///
/// Use this in environments that require SASL authentication.
pub fn new_handler_with_auth(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager port.AuthManager, compression_service &compression.CompressionService) Handler {
	mut cfg := build_default_handler_config(broker_id, host, broker_port, cluster_id,
		storage, compression_service)
	cfg = HandlerConfig{
		...cfg
		auth_manager: auth_manager
	}
	return new_handler_from_config(cfg)
}

/// new_handler_full creates a fully configured Kafka protocol handler with all components.
///
/// Use this in production when authentication, ACL, and transactions are all required.
pub fn new_handler_full(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?port.TransactionCoordinatorPort, compression_service &compression.CompressionService) Handler {
	mut cfg := build_default_handler_config(broker_id, host, broker_port, cluster_id,
		storage, compression_service)
	cfg = HandlerConfig{
		...cfg
		auth_manager:    auth_manager
		acl_manager:     acl_manager
		txn_coordinator: txn_coordinator
	}
	return new_handler_from_config(cfg)
}

/// new_handler_with_share_groups creates a Kafka protocol handler with Share Group support (KIP-932).
///
/// Supports queue-based message consumption patterns.
pub fn new_handler_with_share_groups(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?port.TransactionCoordinatorPort, share_coordinator port.ShareGroupCoordinatorPort, compression_service &compression.CompressionService) Handler {
	mut cfg := build_default_handler_config(broker_id, host, broker_port, cluster_id,
		storage, compression_service)
	cfg = HandlerConfig{
		...cfg
		auth_manager:      auth_manager
		acl_manager:       acl_manager
		txn_coordinator:   txn_coordinator
		share_coordinator: share_coordinator
	}
	return new_handler_from_config(cfg)
}
