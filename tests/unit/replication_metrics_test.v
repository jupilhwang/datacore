// Unit tests for comprehensive ReplicationMetrics.
// Covers: initialization, thread-safe updates, percentile calculation, and rate calculation.
module main

import domain
import sync
import time

// --- helpers ---

fn make_metrics() ReplicationMetrics {
	return domain.new_replication_metrics()
}

// --- initialization ---

fn test_metrics_initial_state() {
	m := make_metrics()
	s := m.snapshot()
	assert s.bytes_in_per_sec == 0.0, 'bytes_in_per_sec should start at 0'
	assert s.bytes_out_per_sec == 0.0, 'bytes_out_per_sec should start at 0'
	assert s.messages_in_per_sec == 0.0, 'messages_in_per_sec should start at 0'
	assert s.messages_out_per_sec == 0.0, 'messages_out_per_sec should start at 0'
	assert s.avg_replication_lag_ms == 0.0, 'avg_replication_lag_ms should start at 0'
	assert s.max_replication_lag_ms == 0.0, 'max_replication_lag_ms should start at 0'
	assert s.p95_replication_lag_ms == 0.0, 'p95_replication_lag_ms should start at 0'
	assert s.p99_replication_lag_ms == 0.0, 'p99_replication_lag_ms should start at 0'
	assert s.pending_requests == 0, 'pending_requests should start at 0'
	assert s.failed_requests == 0, 'failed_requests should start at 0'
	assert s.retried_requests == 0, 'retried_requests should start at 0'
	assert s.active_connections == 0, 'active_connections should start at 0'
	assert s.connection_errors == 0, 'connection_errors should start at 0'
	assert s.reconnection_count == 0, 'reconnection_count should start at 0'
}

// --- throughput counters ---

fn test_bytes_in_accumulates() {
	mut m := make_metrics()
	m.record_bytes_in(100)
	m.record_bytes_in(200)
	m.record_bytes_in(50)
	// Flush with a small artificial window (>0 ms)
	time.sleep(2 * time.millisecond)
	m.flush_rates()
	s := m.snapshot()
	assert s.bytes_in_per_sec > 0.0, 'bytes_in_per_sec should be > 0 after flush'
	assert s.messages_in_per_sec > 0.0, 'messages_in_per_sec should be > 0 (3 calls)'
}

fn test_bytes_out_accumulates() {
	mut m := make_metrics()
	m.record_bytes_out(512)
	m.record_bytes_out(1024)
	time.sleep(2 * time.millisecond)
	m.flush_rates()
	s := m.snapshot()
	assert s.bytes_out_per_sec > 0.0, 'bytes_out_per_sec should be > 0 after flush'
	assert s.messages_out_per_sec > 0.0, 'messages_out_per_sec should be > 0 (2 calls)'
}

fn test_rate_resets_after_flush() {
	mut m := make_metrics()
	m.record_bytes_in(999)
	time.sleep(2 * time.millisecond)
	m.flush_rates()
	// Second flush with no new data
	time.sleep(2 * time.millisecond)
	m.flush_rates()
	s := m.snapshot()
	assert s.bytes_in_per_sec == 0.0, 'bytes_in_per_sec should reset after empty window'
	assert s.messages_in_per_sec == 0.0, 'messages_in_per_sec should reset after empty window'
}

// --- lag / percentile calculations ---

fn test_single_lag_sample() {
	mut m := make_metrics()
	m.record_lag_sample(42.0)
	s := m.snapshot()
	assert s.avg_replication_lag_ms == 42.0, 'avg should equal the single sample'
	assert s.max_replication_lag_ms == 42.0, 'max should equal the single sample'
}

fn test_avg_lag_multiple_samples() {
	mut m := make_metrics()
	m.record_lag_sample(10.0)
	m.record_lag_sample(20.0)
	m.record_lag_sample(30.0)
	s := m.snapshot()
	assert s.avg_replication_lag_ms == 20.0, 'avg of 10/20/30 should be 20'
	assert s.max_replication_lag_ms == 30.0, 'max should be 30'
}

fn test_p95_p99_with_100_samples() {
	mut m := make_metrics()
	// Insert 100 ascending samples: 1.0, 2.0, ..., 100.0
	for i := 1; i <= 100; i++ {
		m.record_lag_sample(f64(i))
	}
	s := m.snapshot()
	// p95 index = int(99 * 0.95) = 94 -> value at sorted[94] = 95.0
	assert s.p95_replication_lag_ms == 95.0, 'p95 should be 95.0 for 1-100 ascending, got ${s.p95_replication_lag_ms}'
	// p99 index = int(99 * 0.99) = 98 -> value at sorted[98] = 99.0
	assert s.p99_replication_lag_ms == 99.0, 'p99 should be 99.0 for 1-100 ascending, got ${s.p99_replication_lag_ms}'
}

fn test_rolling_window_wraps_oldest_samples() {
	mut m := make_metrics()
	// Fill the 1024-sample window with value 1.0
	for _ in 0 .. 1024 {
		m.record_lag_sample(1.0)
	}
	s1 := m.snapshot()
	assert s1.avg_replication_lag_ms == 1.0, 'avg should be 1.0 before overwrite'

	// Overwrite all 1024 slots with 5.0
	for _ in 0 .. 1024 {
		m.record_lag_sample(5.0)
	}
	s2 := m.snapshot()
	assert s2.avg_replication_lag_ms == 5.0, 'avg should be 5.0 after full overwrite'
}

// --- queue / request counters ---

fn test_pending_increment_decrement() {
	mut m := make_metrics()
	m.increment_pending()
	m.increment_pending()
	s1 := m.snapshot()
	assert s1.pending_requests == 2, 'should have 2 pending after 2 increments'
	m.decrement_pending()
	s2 := m.snapshot()
	assert s2.pending_requests == 1, 'should have 1 pending after 1 decrement'
}

fn test_pending_no_underflow() {
	mut m := make_metrics()
	m.decrement_pending() // decrement from 0 - should not go negative
	s := m.snapshot()
	assert s.pending_requests == 0, 'pending should not go below 0'
}

fn test_failed_request_counter() {
	mut m := make_metrics()
	m.record_failed_request()
	m.record_failed_request()
	m.record_failed_request()
	s := m.snapshot()
	assert s.failed_requests == 3, 'failed_requests should be 3'
}

fn test_retried_request_counter() {
	mut m := make_metrics()
	m.record_retried_request()
	m.record_retried_request()
	s := m.snapshot()
	assert s.retried_requests == 2, 'retried_requests should be 2'
}

// --- connection counters ---

fn test_active_connections_gauge() {
	mut m := make_metrics()
	m.set_active_connections(5)
	s1 := m.snapshot()
	assert s1.active_connections == 5, 'active_connections should be 5'
	m.set_active_connections(3)
	s2 := m.snapshot()
	assert s2.active_connections == 3, 'active_connections should update to 3'
}

fn test_connection_error_counter() {
	mut m := make_metrics()
	m.record_connection_error()
	m.record_connection_error()
	s := m.snapshot()
	assert s.connection_errors == 2, 'connection_errors should be 2'
}

fn test_reconnection_counter() {
	mut m := make_metrics()
	m.record_reconnection()
	m.record_reconnection()
	m.record_reconnection()
	s := m.snapshot()
	assert s.reconnection_count == 3, 'reconnection_count should be 3'
}

// --- reset ---

fn test_reset_counters_preserves_lag() {
	mut m := make_metrics()
	m.record_lag_sample(50.0)
	m.record_failed_request()
	m.record_retried_request()
	m.record_bytes_in(1000)
	m.reset_counters()
	s := m.snapshot()
	// Cumulative counters cleared
	assert s.failed_requests == 0, 'failed_requests should reset to 0'
	assert s.retried_requests == 0, 'retried_requests should reset to 0'
	// Lag percentile state preserved
	assert s.avg_replication_lag_ms == 50.0, 'avg_lag should survive reset_counters'
}

// --- thread-safe concurrent updates ---

fn test_thread_safe_concurrent_updates() {
	mut m := make_metrics()
	mut wg := sync.new_waitgroup()
	goroutines := 20
	ops_per_goroutine := 50

	wg.add(goroutines)
	for _ in 0 .. goroutines {
		spawn fn [mut m, mut wg, ops_per_goroutine] () {
			for _ in 0 .. ops_per_goroutine {
				m.record_bytes_in(100)
				m.record_bytes_out(100)
				m.record_lag_sample(5.0)
				m.increment_pending()
				m.decrement_pending()
				m.record_failed_request()
			}
			wg.done()
		}()
	}
	wg.wait()

	time.sleep(2 * time.millisecond)
	m.flush_rates()
	s := m.snapshot()
	total_expected := i64(goroutines * ops_per_goroutine)
	assert s.failed_requests == total_expected, 'failed_requests should be ${total_expected}, got ${s.failed_requests}'
	assert s.avg_replication_lag_ms > 0.0, 'avg_lag should be > 0 after concurrent samples'
}

// --- PartitionMetrics ---

fn test_partition_metrics_initialization() {
	pm := domain.new_partition_metrics('test-topic', 0)
	assert pm.topic == 'test-topic', 'topic should match'
	assert pm.partition == 0, 'partition should match'
	assert pm.bytes_replicated == 0, 'bytes_replicated should start at 0'
	assert pm.messages_sent == 0, 'messages_sent should start at 0'
	assert pm.messages_acked == 0, 'messages_acked should start at 0'
}

fn test_partition_metrics_sent_and_acked() {
	mut pm := domain.new_partition_metrics('orders', 3)
	pm.record_sent(256)
	pm.record_sent(512)
	pm.record_acked()
	assert pm.bytes_replicated == 768, 'bytes_replicated should be 768'
	assert pm.messages_sent == 2, 'messages_sent should be 2'
	assert pm.messages_acked == 1, 'messages_acked should be 1'
}

fn test_partition_metrics_avg_lag() {
	mut pm := domain.new_partition_metrics('events', 1)
	pm.record_lag(10.0)
	pm.record_lag(20.0)
	pm.record_lag(30.0)
	avg := pm.avg_lag_ms()
	assert avg == 20.0, 'avg lag should be 20.0, got ${avg}'
}

fn test_partition_metrics_avg_lag_no_samples() {
	mut pm := domain.new_partition_metrics('empty-topic', 0)
	avg := pm.avg_lag_ms()
	assert avg == 0.0, 'avg lag with no samples should be 0.0'
}

fn test_partition_metrics_thread_safe() {
	mut pm := domain.new_partition_metrics('concurrent-topic', 0)
	mut wg := sync.new_waitgroup()
	goroutines := 10
	wg.add(goroutines)
	for _ in 0 .. goroutines {
		spawn fn [mut pm, mut wg] () {
			for _ in 0 .. 100 {
				pm.record_sent(64)
				pm.record_acked()
				pm.record_lag(2.0)
			}
			wg.done()
		}()
	}
	wg.wait()
	expected := i64(goroutines * 100)
	assert pm.messages_sent == expected, 'messages_sent should be ${expected}, got ${pm.messages_sent}'
	assert pm.messages_acked == expected, 'messages_acked should be ${expected}, got ${pm.messages_acked}'
}
