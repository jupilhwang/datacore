// Replication throughput benchmark.
// Measures how many REPLICATE messages the ReplicationManager can process per second
// in the in-process path (store_replica_buffer), which is the hot path on every
// replica broker that receives data from the leader.
//
// Performance target: 10,000+ messages/second
module main

import domain
import infra.replication
import time
import sync

// ---- top-level constants ----------------------------------------------------

// Small record throughput test parameters
const tp_small_record_size = 256
const tp_small_batch_size = 500
const tp_small_warmup = 2
const tp_small_iterations = 10

// Medium record throughput test parameters
const tp_medium_record_size = 1024
const tp_medium_batch_size = 500
const tp_medium_warmup = 2
const tp_medium_iterations = 10

// Large record throughput test parameters
const tp_large_record_size = 65536
const tp_large_batch_size = 100
const tp_large_warmup = 2
const tp_large_iterations = 5

// Read throughput test parameters
const tp_preload_count = 1000
const tp_read_warmup = 2
const tp_read_iterations = 20

// Delete throughput test parameters
const tp_delete_batch = 200
const tp_delete_warmup = 2
const tp_delete_iterations = 10

// Concurrent throughput test parameters
const tp_concurrent_goroutines = 4
const tp_concurrent_msgs_each = 100
const tp_concurrent_warmup = 2
const tp_concurrent_iterations = 5

// Performance target
const tp_target_ops_per_sec = 10_000.0

// ---- helpers ----------------------------------------------------------------

// vfmt off
fn make_replication_config_throughput() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                       true
		replication_port:              29093
		replica_count:                 2
		replica_timeout_ms:            1000
		heartbeat_interval_ms:         10000
		reassignment_interval_ms:      60000
		orphan_cleanup_interval_ms:    60000
		retry_count:                   1
		replica_buffer_ttl_ms:         0
		max_replica_buffer_size_bytes: 0
	}
}

fn make_manager_throughput() &replication.Manager {
	config := make_replication_config_throughput()
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := replication.Server.new(config.replication_port, handler)
	mut cli := replication.Client.new(config.replica_timeout_ms)
	return &replication.Manager{
		broker_id:           'bench-leader'
		config:              config
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: [
			domain.BrokerRef{
				broker_id: 'bench-replica-1'
				addr:      'localhost:29094'
			},
			domain.BrokerRef{
				broker_id: 'bench-replica-2'
				addr:      'localhost:29095'
			},
		]
		logger:              make_bench_logger()
		running:             false
	}
}

// vfmt on

// store_n_buffers stores n buffers into the manager and returns elapsed nanoseconds.
fn store_n_buffers(mut m replication.Manager, n int, record_size int) {
	records_data := make_record_data(record_size, 42)
	for i in 0 .. n {
		buf := domain.ReplicaBuffer{
			topic:        'throughput-topic'
			partition:    i32(i % 4)
			offset:       i64(i)
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}
}

// ---- benchmarks -------------------------------------------------------------

fn bench_store_throughput_small() BenchmarkResult {
	mut m := make_manager_throughput()

	result := run_benchmark('store_replica_buffer (256B records, batch=1000)', tp_small_iterations,
		tp_small_warmup, fn [mut m] () {
		store_n_buffers(mut m, tp_small_batch_size, tp_small_record_size)
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * tp_small_batch_size
	}
}

fn bench_store_throughput_medium() BenchmarkResult {
	mut m := make_manager_throughput()

	result := run_benchmark('store_replica_buffer (1KB records, batch=1000)', tp_medium_iterations,
		tp_medium_warmup, fn [mut m] () {
		store_n_buffers(mut m, tp_medium_batch_size, tp_medium_record_size)
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * tp_medium_batch_size
	}
}

fn bench_store_throughput_large() BenchmarkResult {
	mut m := make_manager_throughput()

	result := run_benchmark('store_replica_buffer (64KB records, batch=200)', tp_large_iterations,
		tp_large_warmup, fn [mut m] () {
		store_n_buffers(mut m, tp_large_batch_size, tp_large_record_size)
		m.replica_buffers = map[string][]domain.ReplicaBuffer{}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * tp_large_batch_size
	}
}

fn bench_get_all_buffers_throughput() BenchmarkResult {
	mut m := make_manager_throughput()
	for i in 0 .. tp_preload_count {
		buf := domain.ReplicaBuffer{
			topic:        'read-topic'
			partition:    i32(i % 8)
			offset:       i64(i)
			records_data: make_record_data(512, i)
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}

	return run_benchmark('get_all_replica_buffers (5000 buffers pre-loaded)', tp_read_iterations,
		tp_read_warmup, fn [mut m] () {
		_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
	})
}

fn bench_delete_buffers_throughput() BenchmarkResult {
	mut m := make_manager_throughput()

	result := run_benchmark('delete_replica_buffer (batch=500)', tp_delete_iterations,
		tp_delete_warmup, fn [mut m] () {
		for i in 0 .. tp_delete_batch {
			buf := domain.ReplicaBuffer{
				topic:        'del-topic'
				partition:    0
				offset:       i64(i)
				records_data: make_record_data(256, i)
				timestamp:    time.now().unix_milli()
			}
			m.store_replica_buffer(buf) or {}
		}
		m.delete_replica_buffer('del-topic', 0, i64(tp_delete_batch - 1)) or {}
	})

	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * tp_delete_batch
	}
}

fn bench_concurrent_store_throughput() BenchmarkResult {
	mut m := make_manager_throughput()

	result := run_benchmark('concurrent store_replica_buffer (4 goroutines x 500)', tp_concurrent_iterations,
		tp_concurrent_warmup, fn [mut m] () {
		mut wg := sync.new_waitgroup()
		wg.add(tp_concurrent_goroutines)

		for g in 0 .. tp_concurrent_goroutines {
			spawn fn [mut m, mut wg, g] () {
				for i in 0 .. tp_concurrent_msgs_each {
					buf := domain.ReplicaBuffer{
						topic:        'concurrent-topic'
						partition:    i32(g)
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

	total_msgs := tp_concurrent_goroutines * tp_concurrent_msgs_each
	return BenchmarkResult{
		...result
		ops_per_second: result.ops_per_second * total_msgs
	}
}

// ---- entry point ------------------------------------------------------------

pub fn run_throughput_bench() {
	println('')
	println('=== Replication Throughput Benchmark ===')
	println('Performance target: 10,000+ messages/second (in-process store path)')
	println('')

	print_header()

	results := [
		bench_store_throughput_small(),
		bench_store_throughput_medium(),
		bench_store_throughput_large(),
		bench_get_all_buffers_throughput(),
		bench_delete_buffers_throughput(),
		bench_concurrent_store_throughput(),
	]

	for r in results {
		print_result(r)
	}
	print_footer()

	println('')
	println('--- Target Verification ---')
	mut all_pass := true
	for r in results {
		if r.name.contains('store_replica_buffer') && r.name.contains('256B') {
			if !check_throughput_target(r, tp_target_ops_per_sec) {
				all_pass = false
			}
		}
		if r.name.contains('store_replica_buffer') && r.name.contains('1KB') {
			if !check_throughput_target(r, tp_target_ops_per_sec) {
				all_pass = false
			}
		}
	}

	println('')
	if all_pass {
		println('All throughput targets MET.')
	} else {
		println('Some throughput targets MISSED - review results above.')
	}
	println('')
}
