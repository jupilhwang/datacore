// Heartbeat and health monitoring methods for BrokerRegistry.
module cluster

import domain
import time

/// send_heartbeat sends a heartbeat for the local broker.
fn (mut r BrokerRegistry) send_heartbeat(load domain.BrokerLoad) ! {
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
		if mut mp := r.lifecycle_port {
			mp.update_broker_heartbeat(heartbeat)!
		}
	}

	// Update local cache
	r.brokers[r.local_broker.broker_id] = r.local_broker
}

/// check_expired_brokers checks for brokers that have missed heartbeats.
fn (mut r BrokerRegistry) check_expired_brokers() ![]i32 {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	timeout := i64(r.config.broker_session_timeout_ms)
	mut expired := []i32{}
	mut expired_brokers := []domain.BrokerInfo{}

	// Check distributed storage in multi-broker mode
	if r.capability.supports_multi_broker {
		if mut rp := r.registry_port {
			brokers := rp.list_brokers()!
			for broker in brokers {
				if broker.status == .active && now - broker.last_heartbeat > timeout {
					expired << broker.broker_id
					expired_brokers << broker
					if mut hp := r.health_port {
						hp.mark_broker_dead(broker.broker_id) or {
							r.logger.warn('Failed to mark broker dead broker_id=${broker.broker_id} error=${err.str()}')
						}
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
					r.logger.warn('Failed to handle broker change error=${err.str()}')
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
			r.logger.warn('Failed to handle broker change error=${err.str()}')
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

		r.send_heartbeat(load) or { r.logger.warn('Failed to send heartbeat error=${err.str()}') }

		// Check for expired brokers
		expired := r.check_expired_brokers() or { []i32{} }
		if expired.len > 0 {
			r.logger.info('Detected expired brokers count=${expired.len}')
		}

		time.sleep(interval)
	}
}
