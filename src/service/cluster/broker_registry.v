// Manages broker registration and status within the cluster.
module cluster

import domain
import service.port
import sync
import time

// Broker Registry

/// MetricsProvider is a callback function type that provides broker load metrics from an external source.
/// Returns metrics such as connection count, bytes in/out per second.
pub type MetricsProvider = fn () domain.BrokerLoad

/// BrokerRegistry manages broker registration and status within the cluster.
/// Supports both single-broker mode and multi-broker mode.
pub struct BrokerRegistry {
	config domain.ClusterConfig
mut:
	local_broker     domain.BrokerInfo
	registry_port    ?port.BrokerRegistryPort
	state_port       ?port.ClusterStatePort
	health_port      ?port.BrokerHealthPort
	brokers          map[i32]domain.BrokerInfo
	lock             sync.RwMutex
	running          bool
	capability       domain.StorageCapability
	metrics_provider ?MetricsProvider
	prev_bytes_in    u64
	prev_bytes_out   u64
	prev_time        i64
	// Partition assignment service
	partition_assigner ?&PartitionAssigner
	logger             port.LoggerPort
	// Broker change callback
	on_broker_change_cb ?fn (changes BrokerChanges)
}

/// BrokerRegistryConfig holds registry configuration.
pub struct BrokerRegistryConfig {
pub:
	broker_id  i32
	host       string
	port       i32
	rack       string
	cluster_id string
	version    string
	// Timing configuration
	heartbeat_interval_ms i32 = 3000
	session_timeout_ms    i32 = 10000
}

/// new_broker_registry creates a new broker registry.
/// config: broker configuration
/// capability: storage capability information
/// registry_port: broker registration port (multi-broker mode)
/// state_port: cluster state port (multi-broker mode)
/// health_port: broker health port (multi-broker mode)
pub fn new_broker_registry(config BrokerRegistryConfig, capability domain.StorageCapability, registry_port ?port.BrokerRegistryPort, state_port ?port.ClusterStatePort, health_port ?port.BrokerHealthPort) &BrokerRegistry {
	local_broker := domain.BrokerInfo{
		broker_id:         config.broker_id
		host:              config.host
		port:              config.port
		rack:              config.rack
		security_protocol: 'PLAINTEXT'
		endpoints:         [
			domain.BrokerEndpoint{
				name:              'PLAINTEXT'
				host:              config.host
				port:              config.port
				security_protocol: 'PLAINTEXT'
			},
		]
		status:            .starting
		registered_at:     time.now().unix_milli()
		last_heartbeat:    time.now().unix_milli()
		version:           config.version
		features:          []string{}
	}

	cluster_config := domain.ClusterConfig{
		cluster_id:                   config.cluster_id
		broker_heartbeat_interval_ms: config.heartbeat_interval_ms
		broker_session_timeout_ms:    config.session_timeout_ms
		multi_broker_enabled:         capability.supports_multi_broker
	}

	return &BrokerRegistry{
		config:              cluster_config
		local_broker:        local_broker
		registry_port:       registry_port
		state_port:          state_port
		health_port:         health_port
		brokers:             map[i32]domain.BrokerInfo{}
		capability:          capability
		running:             false
		prev_bytes_in:       0
		prev_bytes_out:      0
		prev_time:           time.now().unix_milli()
		partition_assigner:  none
		logger:              port.new_noop_logger()
		on_broker_change_cb: none
	}
}

/// set_metrics_provider sets the callback function that provides broker load metrics.
/// This callback is periodically called in heartbeat_loop to collect actual server metrics.
pub fn (mut r BrokerRegistry) set_metrics_provider(provider MetricsProvider) {
	r.metrics_provider = provider
}

/// set_logger sets the logger for the broker registry.
pub fn (mut r BrokerRegistry) set_logger(logger port.LoggerPort) {
	r.logger = logger
}

/// set_partition_assigner sets the partition assignment service.
pub fn (mut r BrokerRegistry) set_partition_assigner(assigner &PartitionAssigner) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.partition_assigner = assigner
	r.logger.info('Partition assigner registered')
}

/// set_on_broker_change sets the callback to be called when a broker changes.
pub fn (mut r BrokerRegistry) set_on_broker_change(callback fn (changes BrokerChanges)) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.on_broker_change_cb = callback
}

/// register registers the local broker with the cluster.
pub fn (mut r BrokerRegistry) register() !domain.BrokerInfo {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	r.local_broker.registered_at = now
	r.local_broker.last_heartbeat = now
	r.local_broker.status = .active

	// In multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.registry_port {
			// Register with distributed storage
			registered := mp.register_broker(r.local_broker)!
			r.local_broker = registered
			r.brokers[registered.broker_id] = registered

			// Trigger new broker join event
			changes := BrokerChanges{
				reason:  'broker_joined'
				added:   [registered]
				removed: []domain.BrokerInfo{}
			}
			r.on_broker_change(changes) or {
				r.logger.warn('Failed to handle broker change error=${err.str()}')
			}

			return registered
		}
	}

	// Single-broker mode - store locally only
	r.brokers[r.local_broker.broker_id] = r.local_broker
	return r.local_broker
}

/// deregister removes the local broker from the cluster.
pub fn (mut r BrokerRegistry) deregister() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.local_broker.status = .shutdown

	// Trigger broker leave event (before deletion)
	changes := BrokerChanges{
		reason:  'broker_left'
		added:   []domain.BrokerInfo{}
		removed: [r.local_broker]
	}
	r.on_broker_change(changes) or {
		r.logger.warn('Failed to handle broker change error=${err.str()}')
	}

	// In multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.registry_port {
			mp.deregister_broker(r.local_broker.broker_id)!
		}
	}

	r.brokers.delete(r.local_broker.broker_id)
}

/// on_broker_change is called when the broker list changes.
/// Note: Caller must already hold r.lock (internal use only).
pub fn (mut r BrokerRegistry) on_broker_change(changes BrokerChanges) ! {
	r.logger.info('Broker change detected reason=${changes.reason} added=${changes.added.len} removed=${changes.removed.len}')

	// Call callback
	if cb := r.on_broker_change_cb {
		spawn cb(changes)
	}

	// TODO(jira#XXX): Perform rebalancing for all topics

	return
}

/// trigger_rebalance_for_topic performs rebalancing for a specific topic.
pub fn (mut r BrokerRegistry) trigger_rebalance_for_topic(topic_name string) ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if mut assigner := r.partition_assigner {
		active_brokers := r.list_active_brokers_internal() or { []domain.BrokerInfo{} }

		if active_brokers.len == 0 {
			return error('no active brokers available for rebalance')
		}

		assigner.rebalance_partitions(topic_name, active_brokers) or {
			return error('rebalance failed: ${err}')
		}

		r.logger.info('Rebalance completed for topic topic=${topic_name}')
	} else {
		return error('partition assigner not configured')
	}
}
