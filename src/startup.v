// DataCore - Broker startup helpers
// Contains helper functions extracted from start_broker to improve readability
// and testability of the startup sequence.
module startup

import os
import time
import config as cfg
import domain
import infra.compression
import infra.observability
import infra.performance.core as perf_core
import interface.grpc as iface_grpc
import infra.protocol.kafka
import infra.storage.plugins.memory
import infra.storage.plugins.s3
import service.cluster
import service.port

// StorageResult holds the initialized storage port and optional S3 adapter reference.
pub struct StorageResult {
pub mut:
	storage    port.StoragePort
	s3_adapter ?&s3.S3StorageAdapter
}

// init_storage initializes the storage engine based on configuration.
// Returns a StorageResult containing the port and an optional S3 adapter reference.
pub fn init_storage(conf cfg.Config, mut logger observability.Logger) !StorageResult {
	engine := conf.storage.engine.to_lower()

	if engine == 'memory' {
		logger.info('Initializing Memory storage', observability.field_int('max_memory_mb',
			conf.storage.memory.max_memory_mb), observability.field_int('segment_size',
			conf.storage.memory.segment_size_bytes))
		return StorageResult{
			storage: port.StoragePort(memory.new_memory_adapter())
		}
	}

	if engine == 's3' {
		return init_s3_storage(conf, mut logger)!
	}

	logger.warn('Unknown storage engine, falling back to memory', observability.field_string('engine',
		engine))
	return StorageResult{
		storage: port.StoragePort(memory.new_memory_adapter())
	}
}

// init_s3_storage initializes S3 storage and returns the result.
fn init_s3_storage(conf cfg.Config, mut logger observability.Logger) !StorageResult {
	os.setenv('TZ', conf.storage.s3.timezone, true)

	masked_key := if conf.storage.s3.access_key.len > 4 {
		conf.storage.s3.access_key[0..4] + '****'
	} else {
		'****'
	}
	logger.info('Initializing S3 storage', observability.field_string('bucket', conf.storage.s3.bucket),
		observability.field_string('region', conf.storage.s3.region), observability.field_string('endpoint',
		conf.storage.s3.endpoint), observability.field_string('access_key', masked_key))

	if mut s3_adapter := s3.new_s3_adapter_from_storage_config(conf.storage.s3, i32(conf.broker.broker_id),
		conf.broker.cluster_id)
	{
		// config 패키지의 iceberg_ 필드들을 s3 어댑터의 IcebergConfig에 주입 (런타임 연결)
		s3_adapter.set_iceberg_config(s3.IcebergConfig{
			enabled:           conf.storage.s3.iceberg_enabled
			format:            conf.storage.s3.iceberg_format
			compression:       conf.storage.s3.iceberg_compression
			write_mode:        conf.storage.s3.iceberg_write_mode
			partition_by:      conf.storage.s3.iceberg_partition_by
			max_rows_per_file: conf.storage.s3.iceberg_max_rows_per_file
			max_file_size_mb:  conf.storage.s3.iceberg_max_file_size_mb
			schema_evolution:  conf.storage.s3.iceberg_schema_evolution
			format_version:    conf.storage.s3.iceberg_format_version
		})
		s3_adapter.start_workers()
		return StorageResult{
			storage:    port.StoragePort(s3_adapter)
			s3_adapter: s3_adapter
		}
	} else {
		logger.error('Failed to init S3 storage, falling back to memory storage', observability.field_string('error',
			err.msg()))
		return StorageResult{
			storage: port.StoragePort(memory.new_memory_adapter())
		}
	}
}

// init_protocol_handler creates and returns the Kafka protocol handler.
pub fn init_protocol_handler(conf cfg.Config, storage port.StoragePort, compression_service &compression.CompressionService) kafka.Handler {
	return kafka.new_handler(conf.broker.broker_id, conf.broker.advertised_host, conf.broker.port,
		conf.broker.cluster_id, storage, compression_service)
}

// ClusterRegistryResult holds the broker registry and cluster metadata port.
pub struct ClusterRegistryResult {
pub mut:
	registry     ?&cluster.BrokerRegistry
	cluster_port ?&port.ClusterMetadataPort
}

// init_cluster_registry initializes the broker registry for multi-broker mode.
// Returns none if the storage does not support multi-broker mode.
pub fn init_cluster_registry(conf cfg.Config, mut storage port.StoragePort, s3_adapter_ref ?&s3.S3StorageAdapter, mut protocol_handler kafka.Handler, mut logger observability.Logger) ClusterRegistryResult {
	capability := storage.get_storage_capability()
	if !capability.supports_multi_broker {
		return ClusterRegistryResult{}
	}

	logger.info('Initializing multi-broker cluster registry')

	engine := conf.storage.engine.to_lower()
	mut cluster_port_opt := ?&port.ClusterMetadataPort(none)

	if engine == 's3' {
		if mut s3_ref := s3_adapter_ref {
			cluster_port_opt = s3.new_s3_cluster_metadata_adapter(s3_ref)
		}
	} else {
		cluster_port_opt = storage.get_cluster_metadata_port()
	}

	mut cluster_port := cluster_port_opt or {
		logger.error('Storage does not provide cluster metadata port')
		return ClusterRegistryResult{}
	}

	registry_config := cluster.BrokerRegistryConfig{
		broker_id:             conf.broker.broker_id
		host:                  conf.broker.advertised_host
		port:                  conf.broker.port
		rack:                  ''
		cluster_id:            conf.broker.cluster_id
		version:               '0.20.0'
		heartbeat_interval_ms: 3000
		session_timeout_ms:    10000
	}

	mut broker_registry := cluster.new_broker_registry(registry_config, capability, cluster_port)
	registry_logger := observability.new_logger_adapter(observability.get_named_logger('broker_registry'))
	broker_registry.set_logger(registry_logger)

	mut broker_info := domain.BrokerInfo{}
	mut register_success := false
	for reg_attempt in 0 .. 3 {
		broker_info = broker_registry.register() or {
			logger.warn('Broker registration attempt ${reg_attempt + 1}/3 failed', observability.field_int('broker_id',
				conf.broker.broker_id), observability.field_string('cluster_id', conf.broker.cluster_id),
				observability.field_string('error', err.msg()))
			if reg_attempt < 2 {
				wait_secs := 2 * (1 << reg_attempt)
				logger.info('Waiting ${wait_secs}s before next registration attempt')
				time.sleep(wait_secs * time.second)
			}
			// Continue to next attempt; register_success remains false
			continue
		}
		// Registration succeeded — no sentinel check needed
		register_success = true
		break
	}

	if !register_success {
		logger.error('Failed to register broker with cluster after 3 attempts', observability.field_int('broker_id',
			conf.broker.broker_id), observability.field_string('cluster_id', conf.broker.cluster_id))
		return ClusterRegistryResult{}
	}

	logger.info('Broker registered with cluster', observability.field_int('broker_id',
		broker_info.broker_id), observability.field_string('host', broker_info.host),
		observability.field_int('port', broker_info.port))

	protocol_handler.set_broker_registry(broker_registry)

	assigner_config := cluster.PartitionAssignerServiceConfig{
		broker_id:     conf.broker.broker_id
		strategy:      .round_robin
		rack_aware:    false
		sticky_assign: true
		cluster_id:    conf.broker.cluster_id
	}
	mut partition_assigner := cluster.new_partition_assigner(assigner_config, cluster_port)
	assigner_logger := observability.new_logger_adapter(observability.get_named_logger('partition_assigner'))
	partition_assigner.set_logger(assigner_logger)

	// Inject metrics into partition assigner
	mut reg := observability.get_registry()
	partition_assigner.set_metrics(reg.register('partition_assigner_assignments_total',
		'Total partition assignments', .counter), reg.register('partition_assigner_rebalance_total',
		'Total rebalances performed', .counter), reg.register('partition_assigner_rebalance_duration_ms',
		'Rebalance duration in milliseconds', .histogram), reg.register('partition_assigner_assignment_changes_total',
		'Total assignment changes', .counter))

	broker_registry.set_partition_assigner(partition_assigner)
	protocol_handler.set_partition_assigner(partition_assigner)

	logger.info('Cluster registry initialized', observability.field_int('broker_id', conf.broker.broker_id),
		observability.field_string('cluster_id', conf.broker.cluster_id))

	return ClusterRegistryResult{
		registry:     broker_registry
		cluster_port: cluster_port
	}
}

// init_grpc_server creates and starts the gRPC gateway server if enabled.
// Returns the server instance or none if gRPC is disabled.
pub fn init_grpc_server(conf cfg.Config, storage port.StoragePort, mut logger observability.Logger) ?&iface_grpc.GrpcServer {
	if !conf.grpc.enabled {
		return none
	}

	server_config := iface_grpc.GrpcServerConfig{
		host:       conf.grpc.host
		port:       conf.grpc.port
		broker_id:  conf.broker.broker_id
		cluster_id: conf.broker.cluster_id
	}

	grpc_config := domain.GrpcConfig{
		port:             conf.grpc.port
		max_connections:  conf.grpc.max_connections
		max_message_size: conf.grpc.max_message_size
	}

	mut srv := iface_grpc.new_grpc_server(server_config, storage, grpc_config)
	srv.start_background()

	logger.info('gRPC gateway started', observability.field_string('host', conf.grpc.host),
		observability.field_int('port', conf.grpc.port))

	return srv
}

// init_writer_pool initializes the global WriterPool at application startup.
// Must be called before any pooled_writer() or release_writer() calls.
pub fn init_writer_pool(config perf_core.PoolConfig) {
	perf_core.init_global_writer_pool(config)
}

// default_writer_pool_config returns the default PoolConfig for the global WriterPool.
pub fn default_writer_pool_config() perf_core.PoolConfig {
	return perf_core.PoolConfig{
		max_tiny:   500
		max_small:  200
		max_medium: 50
		max_large:  10
		max_huge:   2
	}
}
