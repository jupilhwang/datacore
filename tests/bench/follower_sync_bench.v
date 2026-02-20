// Follower sync benchmark.
// Measures the performance of the follower-side sync operations:
//   - store_replica_buffer (ingestion of replicated data)
//   - delete_replica_buffer (processing of FLUSH_ACK from leader)
//   - get_all_replica_buffers (crash-recovery path)
//
// Also measures the orphan cleanup cycle which runs periodically on followers
// to purge stale buffers that never received a FLUSH_ACK.
//
// This benchmark simulates a real follower workload:
//   - Continuous incoming replication stream from leader
//   - Periodic FLUSH_ACK deletions freeing buffer space
//   - Occasional full-buffer scans for recovery
module main

import domain
import infra.replication
import time
import sync

// ---- top-level constants ----------------------------------------------------

const fs_ingest_record_size = 1024
const fs_ingest_batch = 100
const fs_ingest_warmup = 2
const fs_ingest_iterations = 5

const fs_flush_partitions = 2
const fs_flush_msgs_per_part = 50
const fs_flush_warmup = 2
const fs_flush_iterations = 5

const fs_recovery_partitions = 4
const fs_recovery_msgs_per_part = 50
const fs_recovery_warmup = 3
const fs_recovery_iterations = 20

const fs_cleanup_stale = 50
const fs_cleanup_recent = 25
const fs_cleanup_warmup = 2
const fs_cleanup_iterations = 10

const fs_mixed_total = 100
const fs_mixed_warmup = 2
const fs_mixed_iterations = 5

const fs_concurrent_streams = 2
const fs_concurrent_msgs_each = 10
const fs_concurrent_warmup = 1
const fs_concurrent_iterations = 3

// Follower performance targets
const fs_min_ingest_ops = 10_000.0 // 10K msgs/sec
const fs_max_cleanup_ns = i64(50_000_000) // 50ms

// ---- helpers ----------------------------------------------------------------

// vfmt off
fn make_replication_config_follower() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                       true
		replication_port:              29793
		replica_count:                 2
		replica_timeout_ms:            1000
		heartbeat_interval_ms:         60000
		reassignment_interval_ms:      60000
		orphan_cleanup_interval_ms:    60000
		retry_count:                   1
		replica_buffer_ttl_ms:         0
		max_replica_buffer_size_bytes: 0
	}
}

fn make_manager_follower(port int) &replication.Manager {
	mut cfg := make_replication_config_follower()
	cfg.replication_port = port
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := replication.Server.new(cfg.replication_port, handler)
	mut cli := replication.Client.new(cfg.replica_timeout_ms)
	return &replication.Manager{
		broker_id:           'bench-follower'
		config:              cfg
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: []domain.BrokerRef{}
		logger:              make_bench_logger()
		running:             false
	}
}

// vfmt on

fn preload_buffers(mut m replication.Manager, num_buffers int, num_partitions int, record_size int) {
	for i in 0 .. num_buffers {
		buf := domain.ReplicaBuffer{
			topic:        'follower-topic'
			partition:    i32(i % num_partitions)
			offset:       i64(i)
			records_data: make_record_data(record_size, i)
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}
}

// ---- benchmarks -------------------------------------------------------------

fn bench_follower_ingest_rate() BenchmarkResult {
	mut m := make_manager_follower(29793)

	result := run_benchmark('follower ingest rate (1KB x 500)', fs_ingest_iterations,
		fs_ingest_warmup, fn [mut m] () {
		for i in 0 .. fs_ingest_batch {
			buf := domain.ReplicaBuffer{
				topic:        'ingest-topic'
				partition:    i32(i % 4)
				offset:       i64(i)
				records_data: make_record_data(fs_ingest_record_size, i)
				timestamp:    time.now().unix_milli()
			}
			m.store_replica_buffer(buf) or {}
		}
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * fs_ingest_batch
	}
}

fn bench_follower_flush_ack_processing() BenchmarkResult {
	mut m := make_manager_follower(29893)

	result := run_benchmark('follower FLUSH_ACK processing (4 partitions x 500)', fs_flush_iterations,
		fs_flush_warmup, fn [mut m] () {
		for p in 0 .. fs_flush_partitions {
			for i in 0 .. fs_flush_msgs_per_part {
				buf := domain.ReplicaBuffer{
					topic:        'flush-topic'
					partition:    i32(p)
					offset:       i64(i * 10)
					records_data: make_record_data(512, i)
					timestamp:    time.now().unix_milli()
				}
				m.store_replica_buffer(buf) or {}
			}
		}
		for p in 0 .. fs_flush_partitions {
			m.delete_replica_buffer('flush-topic', i32(p), i64(fs_flush_msgs_per_part * 10)) or {}
		}
	})

	total_msgs := fs_flush_partitions * fs_flush_msgs_per_part
	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * total_msgs
	}
}

fn bench_follower_recovery_scan() BenchmarkResult {
	mut m := make_manager_follower(29993)
	preload_buffers(mut m, fs_recovery_partitions * fs_recovery_msgs_per_part, fs_recovery_partitions,
		512)

	return run_benchmark('follower recovery scan (1600 buffers, 8 partitions)', fs_recovery_iterations,
		fs_recovery_warmup, fn [mut m] () {
		_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
	})
}

fn bench_follower_orphan_cleanup() BenchmarkResult {
	mut m := make_manager_follower(30093)
	m.config.replica_buffer_ttl_ms = 500

	now_ms := time.now().unix_milli()
	old_ts := now_ms - 2000
	recent_ts := now_ms - 100

	result := run_benchmark('follower orphan cleanup (200 stale + 100 recent)', fs_cleanup_iterations,
		fs_cleanup_warmup, fn [mut m, old_ts, recent_ts] () {
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}

		for i in 0 .. fs_cleanup_stale {
			m.replica_buffers_lock.@lock()
			key := 'orphan-topic:${i % 4}'
			if key !in m.replica_buffers {
				m.replica_buffers[key] = []domain.ReplicaBuffer{}
			}
			m.replica_buffers[key] << domain.ReplicaBuffer{
				topic:        'orphan-topic'
				partition:    i32(i % 4)
				offset:       i64(i)
				records_data: make_record_data(128, i)
				timestamp:    old_ts
			}
			m.replica_buffers_lock.unlock()
		}

		for i in 0 .. fs_cleanup_recent {
			m.replica_buffers_lock.@lock()
			key := 'orphan-topic:${i % 4}'
			if key !in m.replica_buffers {
				m.replica_buffers[key] = []domain.ReplicaBuffer{}
			}
			m.replica_buffers[key] << domain.ReplicaBuffer{
				topic:        'orphan-topic'
				partition:    i32(i % 4)
				offset:       i64(fs_cleanup_stale + i)
				records_data: make_record_data(128, i)
				timestamp:    recent_ts
			}
			m.replica_buffers_lock.unlock()
		}

		ttl_ms := m.config.replica_buffer_ttl_ms
		cutoff := time.now().unix_milli() - ttl_ms
		keys := m.replica_buffers.keys()
		for key in keys {
			m.replica_buffers_lock.@lock()
			buffers := m.replica_buffers[key] or {
				m.replica_buffers_lock.unlock()
				continue
			}
			mut remaining := []domain.ReplicaBuffer{}
			for buf in buffers {
				if buf.timestamp > cutoff {
					remaining << buf
				}
			}
			m.replica_buffers[key] = remaining
			m.replica_buffers_lock.unlock()
		}
	})

	return result
}

fn bench_follower_mixed_workload() BenchmarkResult {
	mut m := make_manager_follower(30193)
	preload_buffers(mut m, 200, 4, 512)

	mut op_counter := i64(0)

	result := run_benchmark('follower mixed workload (80% store, 15% del, 5% scan)', fs_mixed_iterations,
		fs_mixed_warmup, fn [mut m, mut op_counter] () {
		for i in 0 .. fs_mixed_total {
			op := i % 100
			if op < 80 {
				buf := domain.ReplicaBuffer{
					topic:        'mixed-topic'
					partition:    i32(i % 4)
					offset:       op_counter
					records_data: make_record_data(512, i)
					timestamp:    time.now().unix_milli()
				}
				m.store_replica_buffer(buf) or {}
				op_counter++
			} else if op < 95 {
				m.delete_replica_buffer('mixed-topic', i32(i % 4), op_counter - 10) or {}
			} else {
				_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
			}
		}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * fs_mixed_total
	}
}

fn bench_follower_concurrent_ingest() BenchmarkResult {
	mut m := make_manager_follower(30293)

	result := run_benchmark('follower concurrent ingest (4 streams x 250)', fs_concurrent_iterations,
		fs_concurrent_warmup, fn [mut m] () {
		mut wg := sync.new_waitgroup()
		wg.add(fs_concurrent_streams)

		for s in 0 .. fs_concurrent_streams {
			spawn fn [mut m, mut wg, s] () {
				for i in 0 .. fs_concurrent_msgs_each {
					buf := domain.ReplicaBuffer{
						topic:        'stream-topic'
						partition:    i32(s)
						offset:       i64(i)
						records_data: make_record_data(512, i)
						timestamp:    time.now().unix_milli()
					}
					m.store_replica_buffer(buf) or {}
				}
				wg.done()
			}()
		}
		wg.wait()
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}
	})

	total := fs_concurrent_streams * fs_concurrent_msgs_each
	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * total
	}
}

// ---- entry point ------------------------------------------------------------

pub fn run_follower_bench() {
	println('')
	println('=== Follower Sync Benchmark ===')
	println('Measures follower-side sync operations: ingest, FLUSH_ACK, recovery, cleanup')
	println('')

	print_header()

	results := [
		bench_follower_ingest_rate(),
		bench_follower_flush_ack_processing(),
		bench_follower_recovery_scan(),
		bench_follower_orphan_cleanup(),
		bench_follower_mixed_workload(),
		bench_follower_concurrent_ingest(),
	]

	for r in results {
		print_result(r)
	}
	print_footer()

	println('')
	println('--- Target Verification ---')
	mut all_pass := true
	for r in results {
		if r.name.contains('ingest rate') || r.name.contains('concurrent ingest')
			|| r.name.contains('mixed workload') {
			if !check_throughput_target(r, fs_min_ingest_ops) {
				all_pass = false
			}
		} else if r.name.contains('orphan cleanup') {
			if !check_latency_target(r, fs_max_cleanup_ns) {
				all_pass = false
			}
		}
	}

	println('')
	if all_pass {
		println('All follower sync targets MET.')
	} else {
		println('Some follower sync targets MISSED - review results above.')
	}
	println('')
}
