// Query and lookup methods for BrokerRegistry.
module cluster

import domain
import time

/// get_local_broker returns local broker information.
fn (r &BrokerRegistry) get_local_broker() domain.BrokerInfo {
	return r.local_broker
}

/// get_broker returns information for a specific broker.
pub fn (mut r BrokerRegistry) get_broker(broker_id i32) !domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.registry_port {
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
		if mut mp := r.registry_port {
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
		if mut mp := r.registry_port {
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

/// get_cluster_metadata returns the current cluster metadata.
fn (mut r BrokerRegistry) get_cluster_metadata() !domain.ClusterMetadata {
	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.state_port {
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
fn (r &BrokerRegistry) is_multi_broker_enabled() bool {
	return r.capability.supports_multi_broker
}

/// get_capability returns storage capability information.
fn (r &BrokerRegistry) get_capability() domain.StorageCapability {
	return r.capability
}

/// list_active_brokers_internal returns active broker list without acquiring a lock.
/// For internal use (called while already holding lock)
fn (mut r BrokerRegistry) list_active_brokers_internal() ![]domain.BrokerInfo {
	// Try distributed storage first
	if r.capability.supports_multi_broker {
		if mut mp := r.registry_port {
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
