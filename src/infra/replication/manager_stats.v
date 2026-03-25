module replication

import domain

// Replication Statistics and Metrics
//
// This file encapsulates all stats recording and metrics query operations.
// Protected data groups:
//   - stats + stats_lock (replication counters)
//   - partition_metrics + partition_metrics_lock (per-partition metrics)
//   - metrics + metrics_lock (ReplicationMetrics -- caller must hold metrics_lock)
//
// Lock ordering position: stats_lock is last (assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock)
// partition_metrics_lock follows the same ordering as stats_lock (both are leaf locks, never held together).

// get_stats returns a snapshot of the current replication statistics.
// Thread-safe; acquires stats_lock internally.
/// get_stats returns a snapshot of the current replication statistics.
pub fn (mut m Manager) get_stats() domain.ReplicationStats {
	m.stats_lock.@lock()
	stats := m.stats
	m.stats_lock.unlock()
	return stats
}

// get_metrics returns a snapshot of the comprehensive replication metrics.
// Thread-safe; acquires metrics_lock internally.
/// get_metrics returns a snapshot of the comprehensive replication metrics.
fn (mut m Manager) get_metrics() domain.ReplicationMetricsSnapshot {
	m.metrics_lock.@lock()
	result := m.metrics.snapshot()
	m.metrics_lock.unlock()
	return result
}

// get_partition_metrics returns a copy of the per-partition metrics map.
// Thread-safe; acquires partition_metrics_lock internally.
/// get_partition_metrics returns per-partition replication metrics.
fn (mut m Manager) get_partition_metrics() map[string]domain.PartitionMetrics {
	m.partition_metrics_lock.@lock()
	mut result := map[string]domain.PartitionMetrics{}
	for k, v in m.partition_metrics {
		result[k] = v
	}
	m.partition_metrics_lock.unlock()
	return result
}

// flush_metrics_rates recomputes per-second throughput rates.
// Should be called periodically (e.g., every second) from a background task.
// Thread-safe; acquires metrics_lock internally.
/// flush_metrics_rates recomputes per-second throughput rates.
fn (mut m Manager) flush_metrics_rates() {
	m.metrics_lock.@lock()
	m.metrics.flush_rates()
	m.metrics_lock.unlock()
}

// record_stats_replicate increments the replicated counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_replicate() {
	m.stats_lock.@lock()
	m.stats.record_replicate()
	m.stats_lock.unlock()
}

// record_stats_ack increments the ack counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_ack() {
	m.stats_lock.@lock()
	m.stats.record_ack()
	m.stats_lock.unlock()
}

// record_stats_flush_ack increments the flush ack counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_flush_ack() {
	m.stats_lock.@lock()
	m.stats.record_flush_ack()
	m.stats_lock.unlock()
}

// record_stats_retry_exhausted increments the retry exhausted counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_retry_exhausted() {
	m.stats_lock.@lock()
	m.stats.record_retry_exhausted()
	m.stats_lock.unlock()
}

// record_stats_heartbeat updates the heartbeat timestamp.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_heartbeat() {
	m.stats_lock.@lock()
	m.stats.update_heartbeat()
	m.stats_lock.unlock()
}

// record_stats_orphans_cleaned increments orphan cleanup counters.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_orphans_cleaned(count int) {
	m.stats_lock.@lock()
	m.stats.total_orphans_cleaned += count
	m.stats.record_ttl_dropped(i64(count))
	m.stats_lock.unlock()
}

// record_stats_ttl_dropped increments the TTL dropped counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_ttl_dropped(count i64) {
	m.stats_lock.@lock()
	m.stats.record_ttl_dropped(count)
	m.stats_lock.unlock()
}

// record_stats_buffer_overflow increments the buffer overflow counter.
// Thread-safe; acquires stats_lock internally.
fn (mut m Manager) record_stats_buffer_overflow() {
	m.stats_lock.@lock()
	m.stats.record_buffer_overflow()
	m.stats_lock.unlock()
}

// record_partition_sent records a sent message for the given partition.
// Creates the partition entry if it does not exist.
// Thread-safe; acquires partition_metrics_lock internally.
fn (mut m Manager) record_partition_sent(key string, topic string, partition i32, bytes i64) {
	m.partition_metrics_lock.@lock()
	if key !in m.partition_metrics {
		m.partition_metrics[key] = domain.new_partition_metrics(topic, partition)
	}
	m.partition_metrics[key].record_sent(bytes)
	m.partition_metrics_lock.unlock()
}

// record_partition_acked records an ack and lag sample for the given partition.
// Creates the partition entry if it does not exist.
// Thread-safe; acquires partition_metrics_lock internally.
fn (mut m Manager) record_partition_acked(key string, topic string, partition i32, lag_ms f64) {
	m.partition_metrics_lock.@lock()
	if key !in m.partition_metrics {
		m.partition_metrics[key] = domain.new_partition_metrics(topic, partition)
	}
	m.partition_metrics[key].record_acked()
	m.partition_metrics[key].record_lag(lag_ms)
	m.partition_metrics_lock.unlock()
}
