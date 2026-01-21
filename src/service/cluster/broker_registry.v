// Service Layer - Broker Registry
// Manages broker registration, heartbeats, and health monitoring
module cluster

import domain
import service.port
import sync
import time

// ============================================================================
// Broker Registry
// ============================================================================

// BrokerRegistry manages broker registration and health in the cluster
pub struct BrokerRegistry {
	config domain.ClusterConfig
mut:
	// Local broker info
	local_broker domain.BrokerInfo
	// Cluster metadata port (for distributed storage)
	metadata_port ?port.ClusterMetadataPort
	// In-memory cache of brokers (for single-broker mode or caching)
	brokers map[i32]domain.BrokerInfo
	// Thread safety
	lock sync.RwMutex
	// Background worker control
	running bool
	// Storage capability
	capability domain.StorageCapability
}

// BrokerRegistryConfig holds configuration for the registry
pub struct BrokerRegistryConfig {
pub:
	broker_id  i32
	host       string
	port       i32
	rack       string
	cluster_id string
	version    string
	// Timing
	heartbeat_interval_ms i32 = 3000
	session_timeout_ms    i32 = 10000
}

// new_broker_registry creates a new broker registry
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
		config:        cluster_config
		local_broker:  local_broker
		metadata_port: metadata_port
		brokers:       map[i32]domain.BrokerInfo{}
		capability:    capability
		running:       false
	}
}

// ============================================================================
// Registration
// ============================================================================

// register registers the local broker with the cluster
pub fn (mut r BrokerRegistry) register() !domain.BrokerInfo {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	r.local_broker.registered_at = now
	r.local_broker.last_heartbeat = now
	r.local_broker.status = .active

	// If multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			// Register with distributed storage
			registered := mp.register_broker(r.local_broker)!
			r.local_broker = registered
			r.brokers[registered.broker_id] = registered
			return registered
		}
	}

	// Single-broker mode - just store locally
	r.brokers[r.local_broker.broker_id] = r.local_broker
	return r.local_broker
}

// deregister removes the local broker from the cluster
pub fn (mut r BrokerRegistry) deregister() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.local_broker.status = .shutdown

	// If multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.deregister_broker(r.local_broker.broker_id)!
		}
	}

	r.brokers.delete(r.local_broker.broker_id)
}

// ============================================================================
// Heartbeat
// ============================================================================

// send_heartbeat sends a heartbeat for the local broker
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

	// If multi-broker mode with distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.update_broker_heartbeat(heartbeat)!
		}
	}

	// Update local cache
	r.brokers[r.local_broker.broker_id] = r.local_broker
}

// ============================================================================
// Query
// ============================================================================

// get_local_broker returns the local broker info
pub fn (r &BrokerRegistry) get_local_broker() domain.BrokerInfo {
	return r.local_broker
}

// get_broker returns information about a specific broker
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

// list_brokers returns all registered brokers
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

// list_active_brokers returns only active brokers
pub fn (mut r BrokerRegistry) list_active_brokers() ![]domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_active_brokers()
		}
	}

	// Fall back to local cache - filter active
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		if broker.status == .active {
			result << broker
		}
	}
	return result
}

// ============================================================================
// Health Monitoring
// ============================================================================

// check_expired_brokers checks for brokers that have missed heartbeats
pub fn (mut r BrokerRegistry) check_expired_brokers() ![]i32 {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	timeout := i64(r.config.broker_session_timeout_ms)
	mut expired := []i32{}

	// In multi-broker mode, check distributed storage
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			brokers := mp.list_brokers()!
			for broker in brokers {
				if broker.status == .active && now - broker.last_heartbeat > timeout {
					expired << broker.broker_id
					mp.mark_broker_dead(broker.broker_id) or {}
				}
			}
			return expired
		}
	}

	// Single-broker mode - check local cache
	for broker_id, broker in r.brokers {
		if broker.status == .active && now - broker.last_heartbeat > timeout {
			expired << broker_id
			r.brokers[broker_id] = domain.BrokerInfo{
				...broker
				status: .dead
			}
		}
	}

	return expired
}

// ============================================================================
// Background Worker
// ============================================================================

// start_heartbeat_worker starts the background heartbeat worker
pub fn (mut r BrokerRegistry) start_heartbeat_worker() {
	r.running = true
	spawn r.heartbeat_loop()
}

// stop_heartbeat_worker stops the background heartbeat worker
pub fn (mut r BrokerRegistry) stop_heartbeat_worker() {
	r.running = false
}

fn (mut r BrokerRegistry) heartbeat_loop() {
	interval := time.Duration(r.config.broker_heartbeat_interval_ms * time.millisecond)

	for r.running {
		// Send heartbeat
		load := domain.BrokerLoad{} // TODO: Collect actual metrics
		r.send_heartbeat(load) or { eprintln('[WARN] Failed to send heartbeat: ${err}') }

		// Check for expired brokers
		expired := r.check_expired_brokers() or { []i32{} }
		if expired.len > 0 {
			eprintln('[INFO] Detected ${expired.len} expired brokers: ${expired}')
		}

		time.sleep(interval)
	}
}

// ============================================================================
// Cluster Metadata
// ============================================================================

// get_cluster_metadata returns the current cluster metadata
pub fn (mut r BrokerRegistry) get_cluster_metadata() !domain.ClusterMetadata {
	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.get_cluster_metadata()
		}
	}

	// Build from local state
	brokers := r.list_brokers()!

	return domain.ClusterMetadata{
		cluster_id:       r.config.cluster_id
		controller_id:    r.local_broker.broker_id // In single-broker, we are the controller
		brokers:          brokers
		metadata_version: 1
		updated_at:       time.now().unix_milli()
	}
}

// is_multi_broker_enabled returns whether multi-broker mode is enabled
pub fn (r &BrokerRegistry) is_multi_broker_enabled() bool {
	return r.capability.supports_multi_broker
}

// get_capability returns the storage capability
pub fn (r &BrokerRegistry) get_capability() domain.StorageCapability {
	return r.capability
}
