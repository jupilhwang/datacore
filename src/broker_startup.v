// DataCore - Broker startup helpers
// Contains helper functions extracted from start_broker to improve readability
// and testability of the startup sequence.
module main

import os
import time
import config as cfg
import domain
import infra.compression
import infra.observability
import infra.protocol.kafka
import infra.storage.plugins.memory
import infra.storage.plugins.s3
import interface.cli
import service.cluster
import service.port

// StorageResult holds the initialized storage port and optional S3 adapter reference.
struct StorageResult {
mut:
	storage    port.StoragePort
	s3_adapter ?&s3.S3StorageAdapter
}

// init_storage initializes the storage engine based on configuration.
// Returns a StorageResult containing the port and an optional S3 adapter reference.
fn init_storage(conf cfg.Config, mut logger observability.Logger) !StorageResult {
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

	s_config := s3.S3Config{
		bucket_name:            conf.storage.s3.bucket
		region:                 conf.storage.s3.region
		endpoint:               conf.storage.s3.endpoint
		access_key:             conf.storage.s3.access_key
		secret_key:             conf.storage.s3.secret_key
		prefix:                 conf.storage.s3.prefix
		use_path_style:         true
		timezone:               conf.storage.s3.timezone
		broker_id:              i32(conf.broker.broker_id)
		batch_timeout_ms:       conf.storage.s3.batch_timeout_ms
		batch_max_bytes:        conf.storage.s3.batch_max_bytes
		compaction_interval_ms: conf.storage.s3.compaction_interval_ms
		target_segment_bytes:   conf.storage.s3.target_segment_bytes
		index_cache_ttl_ms:     conf.storage.s3.index_cache_ttl_ms
	}

	masked_key := if s_config.access_key.len > 4 {
		s_config.access_key[0..4] + '****'
	} else {
		'****'
	}
	logger.info('Initializing S3 storage', observability.field_string('bucket', s_config.bucket_name),
		observability.field_string('region', s_config.region), observability.field_string('endpoint',
		s_config.endpoint), observability.field_string('access_key', masked_key))

	if mut s3_adapter := s3.new_s3_adapter(s_config) {
		s3_adapter.start_workers()
		return StorageResult{
			storage:    port.StoragePort(s3_adapter)
			s3_adapter: s3_adapter
		}
	} else {
		cli.print_failed('Failed to init S3 storage: ${err}')
		cli.print_failed('Falling back to memory storage')
		return StorageResult{
			storage: port.StoragePort(memory.new_memory_adapter())
		}
	}
}

// init_protocol_handler creates and returns the Kafka protocol handler.
fn init_protocol_handler(conf cfg.Config, storage port.StoragePort, compression_service &compression.CompressionService) kafka.Handler {
	return kafka.new_handler(conf.broker.broker_id, conf.broker.advertised_host, conf.broker.port,
		conf.broker.cluster_id, storage, compression_service)
}

// ClusterRegistryResult holds the broker registry and cluster metadata port.
struct ClusterRegistryResult {
mut:
	registry     ?&cluster.BrokerRegistry
	cluster_port ?&port.ClusterMetadataPort
}

// init_cluster_registry initializes the broker registry for multi-broker mode.
// Returns none if the storage does not support multi-broker mode.
fn init_cluster_registry(conf cfg.Config, mut storage port.StoragePort, s3_adapter_ref ?&s3.S3StorageAdapter, mut protocol_handler kafka.Handler, mut logger observability.Logger) ClusterRegistryResult {
	capability := storage.get_storage_capability()
	if !capability.supports_multi_broker {
		return ClusterRegistryResult{}
	}

	cli.print_progress('Initializing multi-broker cluster registry')

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
		cli.print_failed('Storage does not provide cluster metadata port')
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
			domain.BrokerInfo{}
		}
		if broker_info.broker_id > 0 {
			register_success = true
			break
		}
	}

	if !register_success {
		cli.print_failed('Failed to register broker after 3 attempts - continuing without cluster registration')
		logger.error('Failed to register broker with cluster after 3 attempts', observability.field_int('broker_id',
			conf.broker.broker_id), observability.field_string('cluster_id', conf.broker.cluster_id))
		cli.print_done()
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

	broker_registry.set_partition_assigner(partition_assigner)
	protocol_handler.set_partition_assigner(partition_assigner)

	cli.print_done()

	return ClusterRegistryResult{
		registry:     broker_registry
		cluster_port: cluster_port
	}
}
