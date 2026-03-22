// Manages broker registration, heartbeats, and status monitoring within the cluster.
module cluster

import domain
import service.port
import infra.observability
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
	metadata_port    ?port.ClusterMetadataPort
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
	// Rebalance trigger for automated partition redistribution
	rebalance_trigger ?&RebalanceTrigger
	logger            &observability.Logger
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
/// metadata_port: cluster metadata port for distributed storage (multi-broker mode)
pub fn new_broker_registry(config BrokerRegistryConfig, capability domain.StorageCapability, metadata_port ?port.ClusterMetadataPort) &BrokerRegistry {
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
		metadata_port:       metadata_port
		brokers:             map[i32]domain.BrokerInfo{}
		capability:          capability
		running:             false
		prev_bytes_in:       0
		prev_bytes_out:      0
		prev_time:           time.now().unix_milli()
		partition_assigner:  none
		rebalance_trigger:   none
		logger:              observability.get_named_logger('broker_registry')
		on_broker_change_cb: none
	}
}

/// set_metrics_provider sets the callback function that provides broker load metrics.
/// This callback is periodically called in heartbeat_loop to collect actual server metrics.
pub fn (mut r BrokerRegistry) set_metrics_provider(provider MetricsProvider) {
	r.metrics_provider = provider
}

/// set_partition_assigner sets the partition assignment service.
pub fn (mut r BrokerRegistry) set_partition_assigner(assigner &PartitionAssigner) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.partition_assigner = assigner
	r.logger.info('Partition assigner registered')
}

/// set_rebalance_trigger sets the rebalance trigger for automated partition redistribution.
pub fn (mut r BrokerRegistry) set_rebalance_trigger(trigger &RebalanceTrigger) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.rebalance_trigger = trigger
	r.logger.info('Rebalance trigger registered')
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
		if mut mp := r.metadata_port {
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
				r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
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
		r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
	}

	// In multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.deregister_broker(r.local_broker.broker_id)!
		}
	}

	r.brokers.delete(r.local_broker.broker_id)
}

/// send_heartbeat sends a heartbeat for the local broker.
pub fn (mut r BrokerRegistry) send_heartbeat(load domain.BrokerLoad) ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	r.local_broker.last_heartbeat = now

	heartbeat := domain.BrokerHeartbeat{
		broker_id:      r.local_broker.broker_id
		timestamp:      now
		current_load:   load
		wants_shutdown: r.local_broker.status == .draining
	}

	// In multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.update_broker_heartbeat(heartbeat)!
		}
	}

	// Update local cache
	r.brokers[r.local_broker.broker_id] = r.local_broker
}

/// get_local_broker returns local broker information.
pub fn (r &BrokerRegistry) get_local_broker() domain.BrokerInfo {
	return r.local_broker
}

/// get_broker returns information for a specific broker.
pub fn (mut r BrokerRegistry) get_broker(broker_id i32) !domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.get_broker(broker_id)
		}
	}

	// Fall back to local cache
	return r.brokers[broker_id] or { return error('broker not found: ${broker_id}') }
}

/// list_brokers returns all registered brokers.
pub fn (mut r BrokerRegistry) list_brokers() ![]domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_brokers()
		}
	}

	// Fall back to local cache
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		result << broker
	}
	return result
}

/// list_active_brokers returns only active brokers.
pub fn (mut r BrokerRegistry) list_active_brokers() ![]domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_active_brokers()
		}
	}

	// Fall back to local cache - filter active brokers
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		if broker.status == .active {
			result << broker
		}
	}
	return result
}

/// check_expired_brokers checks for brokers that have missed heartbeats.
pub fn (mut r BrokerRegistry) check_expired_brokers() ![]i32 {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	timeout := i64(r.config.broker_session_timeout_ms)
	mut expired := []i32{}
	mut expired_brokers := []domain.BrokerInfo{}

	// Check distributed storage in multi-broker mode
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			brokers := mp.list_brokers()!
			for broker in brokers {
				if broker.status == .active && now - broker.last_heartbeat > timeout {
					expired << broker.broker_id
					expired_brokers << broker
					mp.mark_broker_dead(broker.broker_id) or {}
				}
			}

			// Trigger change event if expired brokers found
			if expired.len > 0 {
				changes := BrokerChanges{
					reason:  'broker_failed'
					added:   []domain.BrokerInfo{}
					removed: expired_brokers
				}
				r.on_broker_change(changes) or {
					r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
				}
			}

			return expired
		}
	}

	// Single-broker mode - check local cache
	for broker_id, broker in r.brokers {
		if broker.status == .active && now - broker.last_heartbeat > timeout {
			expired << broker_id
			expired_brokers << broker
			r.brokers[broker_id] = domain.BrokerInfo{
				...broker
				status: .dead
			}
		}
	}

	// Trigger change event if expired brokers found
	if expired.len > 0 {
		changes := BrokerChanges{
			reason:  'broker_failed'
			added:   []domain.BrokerInfo{}
			removed: expired_brokers
		}
		r.on_broker_change(changes) or {
			r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
		}
	}

	return expired
}

/// start_heartbeat_worker starts the background heartbeat worker.
pub fn (mut r BrokerRegistry) start_heartbeat_worker() {
	r.running = true
	spawn r.heartbeat_loop()
}

/// stop_heartbeat_worker stops the background heartbeat worker.
pub fn (mut r BrokerRegistry) stop_heartbeat_worker() {
	r.running = false
}

fn (mut r BrokerRegistry) heartbeat_loop() {
	interval := time.Duration(r.config.broker_heartbeat_interval_ms * time.millisecond)

	for r.running {
		// Collect metrics: use actual metrics if external provider is set
		load := if provider := r.metrics_provider {
			provider()
		} else {
			// Return default empty metrics if no provider set
			domain.BrokerLoad{}
		}

		r.send_heartbeat(load) or {
			r.logger.warn('Failed to send heartbeat', observability.field_err_str(err.str()))
		}

		// Check for expired brokers
		expired := r.check_expired_brokers() or { []i32{} }
		if expired.len > 0 {
			r.logger.info('Detected expired brokers', observability.field_int('count',
				i64(expired.len)))
		}

		time.sleep(interval)
	}
}

/// get_cluster_metadata returns the current cluster metadata.
pub fn (mut r BrokerRegistry) get_cluster_metadata() !domain.ClusterMetadata {
	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.get_cluster_metadata()
		}
	}

	// Construct from local state
	brokers := r.list_brokers()!

	return domain.ClusterMetadata{
		cluster_id:       r.config.cluster_id
		controller_id:    r.local_broker.broker_id
		brokers:          brokers
		metadata_version: 1
		updated_at:       time.now().unix_milli()
	}
}

/// is_multi_broker_enabled returns whether multi-broker mode is enabled.
pub fn (r &BrokerRegistry) is_multi_broker_enabled() bool {
	return r.capability.supports_multi_broker
}

/// get_capability returns storage capability information.
pub fn (r &BrokerRegistry) get_capability() domain.StorageCapability {
	return r.capability
}

/// on_broker_change is called when the broker list changes.
/// Triggers partition rebalancing and calls the callback.
/// Note: Caller must already hold r.lock (internal use only).
pub fn (mut r BrokerRegistry) on_broker_change(changes BrokerChanges) ! {
	r.logger.info('Broker change detected', observability.field_string('reason', changes.reason),
		observability.field_int('added', i64(changes.added.len)), observability.field_int('removed',
		i64(changes.removed.len)))

	// Call callback
	if cb := r.on_broker_change_cb {
		spawn cb(changes)
	}

	// Trigger rebalancing if partition assigner is configured
	if r.partition_assigner != none {
		// Get list of active brokers
		active_brokers := r.list_active_brokers_internal() or { []domain.BrokerInfo{} }

		if active_brokers.len == 0 {
			r.logger.warn('No active brokers available for rebalance')
			return
		}

		// Delegate to rebalance trigger if available
		if mut trigger := r.rebalance_trigger {
			for broker in changes.added {
				trigger.on_broker_added(broker.broker_id)
			}
			for broker in changes.removed {
				trigger.on_broker_removed(broker.broker_id)
			}
		} else {
			r.logger.info('No rebalance trigger configured, skipping partition rebalance',
				observability.field_int('active_brokers', i64(active_brokers.len)))
		}
	}

	return
}

/// list_active_brokers_internal returns active broker list without acquiring a lock.
/// For internal use (called while already holding lock)
fn (mut r BrokerRegistry) list_active_brokers_internal() ![]domain.BrokerInfo {
	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_active_brokers()
		}
	}

	// Fall back to local cache - filter active brokers
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		if broker.status == .active {
			result << broker
		}
	}
	return result
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

		r.logger.info('Rebalance completed for topic', observability.field_string('topic',
			topic_name))
	} else {
		return error('partition assigner not configured')
	}
}
