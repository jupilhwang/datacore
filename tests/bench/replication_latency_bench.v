// Replication latency benchmark.
// Measures the end-to-end latency of the in-process replication path:
//   store_replica_buffer -> delete_replica_buffer (simulating REPLICATE + FLUSH_ACK)
//
// Performance target: < 5ms average latency per hop (store + retrieve)
module main

import domain
import infra.replication
import time

// ---- top-level constants ----------------------------------------------------

const lat_single_record_size = 256
const lat_single_warmup = 5
const lat_single_iterations = 200

const lat_rt_record_size = 512
const lat_rt_warmup = 5
const lat_rt_iterations = 200

const lat_1kb_record_size = 1024
const lat_1kb_warmup = 5
const lat_1kb_iterations = 200

const lat_10kb_record_size = 10240
const lat_10kb_warmup = 3
const lat_10kb_iterations = 100

const lat_preload_count = 50
const lat_preload_warmup = 5
const lat_preload_iterations = 100

const lat_stats_warmup = 10
const lat_stats_iterations = 500

const lat_percentile_size = 512
const lat_percentile_samples = 500
const lat_percentile_warmup = 20

// 5ms = 5,000,000 ns
const lat_target_ns = i64(5_000_000)

// ---- helpers ----------------------------------------------------------------

// vfmt off
fn make_replication_config_latency() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                       true
		replication_port:              29193
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

fn make_manager_latency() &replication.Manager {
	config := make_replication_config_latency()
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := replication.Server.new(config.replication_port, handler)
	mut cli := replication.Client.new(config.replica_timeout_ms)
	return &replication.Manager{
		broker_id:           'bench-latency-leader'
		config:              config
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: [
			domain.BrokerRef{
				broker_id: 'bench-latency-replica-1'
				addr:      'localhost:29194'
			},
		]
		logger:              make_bench_logger()
		running:             false
	}
}

// vfmt on

// ---- benchmarks -------------------------------------------------------------

fn bench_single_store_latency() BenchmarkResult {
	mut m := make_manager_latency()
	records_data := make_record_data(lat_single_record_size, 7)
	mut counter := i64(0)

	return run_benchmark('store_replica_buffer latency (single, 256B)', lat_single_iterations,
		lat_single_warmup, fn [mut m, records_data, mut counter] () {
		buf := domain.ReplicaBuffer{
			topic:        'latency-topic'
			partition:    0
			offset:       counter
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
		counter++
	})
}

fn bench_store_and_delete_roundtrip_latency() BenchmarkResult {
	mut m := make_manager_latency()
	records_data := make_record_data(lat_rt_record_size, 13)
	mut offset := i64(0)

	return run_benchmark('store + delete roundtrip latency (512B)', lat_rt_iterations,
		lat_rt_warmup, fn [mut m, records_data, mut offset] () {
		buf := domain.ReplicaBuffer{
			topic:        'rt-topic'
			partition:    0
			offset:       offset
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
		m.delete_replica_buffer('rt-topic', 0, offset) or {}
		offset++
	})
}

fn bench_store_latency_1kb() BenchmarkResult {
	mut m := make_manager_latency()
	records_data := make_record_data(lat_1kb_record_size, 99)
	mut counter := i64(0)

	return run_benchmark('store_replica_buffer latency (single, 1KB)', lat_1kb_iterations,
		lat_1kb_warmup, fn [mut m, records_data, mut counter] () {
		buf := domain.ReplicaBuffer{
			topic:        'latency-1kb'
			partition:    0
			offset:       counter
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
		counter++
	})
}

fn bench_store_latency_10kb() BenchmarkResult {
	mut m := make_manager_latency()
	records_data := make_record_data(lat_10kb_record_size, 55)
	mut counter := i64(0)

	return run_benchmark('store_replica_buffer latency (single, 10KB)', lat_10kb_iterations,
		lat_10kb_warmup, fn [mut m, records_data, mut counter] () {
		buf := domain.ReplicaBuffer{
			topic:        'latency-10kb'
			partition:    0
			offset:       counter
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
		counter++
	})
}

fn bench_get_all_latency() BenchmarkResult {
	mut m := make_manager_latency()
	for i in 0 .. lat_preload_count {
		buf := domain.ReplicaBuffer{
			topic:        'preloaded'
			partition:    i32(i % 4)
			offset:       i64(i)
			records_data: make_record_data(256, i)
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}

	return run_benchmark('get_all_replica_buffers latency (100 buffers)', lat_preload_iterations,
		lat_preload_warmup, fn [mut m] () {
		_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
	})
}

fn bench_stats_update_latency() BenchmarkResult {
	mut m := make_manager_latency()

	return run_benchmark('get_stats latency', lat_stats_iterations, lat_stats_warmup,
		fn [mut m] () {
		_ := m.get_stats()
	})
}

// percentile calculates the p-th percentile of sorted latency samples (in ns).
fn percentile(samples []i64, p f64) i64 {
	if samples.len == 0 {
		return 0
	}
	idx := int(f64(samples.len - 1) * p / 100.0)
	return samples[idx]
}

fn bench_store_latency_percentiles() {
	mut m := make_manager_latency()
	records_data := make_record_data(lat_percentile_size, 17)

	// Warmup
	for _ in 0 .. lat_percentile_warmup {
		buf := domain.ReplicaBuffer{
			topic:        'pct-topic'
			partition:    0
			offset:       i64(0)
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}
	m.replica_buffers = map[string][]domain.ReplicaBuffer{}

	// Measurement
	mut samples := []i64{cap: lat_percentile_samples}
	for i in 0 .. lat_percentile_samples {
		buf := domain.ReplicaBuffer{
			topic:        'pct-topic'
			partition:    0
			offset:       i64(i)
			records_data: records_data
			timestamp:    time.now().unix_milli()
		}
		start := time.now()
		m.store_replica_buffer(buf) or {}
		elapsed := time.since(start).nanoseconds()
		samples << elapsed
	}

	// Sort (insertion sort - acceptable for 2000 samples)
	for i := 1; i < samples.len; i++ {
		key := samples[i]
		mut j := i - 1
		for j >= 0 && samples[j] > key {
			samples[j + 1] = samples[j]
			j--
		}
		samples[j + 1] = key
	}

	p50 := f64(percentile(samples, 50.0)) / 1000.0
	p95 := f64(percentile(samples, 95.0)) / 1000.0
	p99 := f64(percentile(samples, 99.0)) / 1000.0
	p999 := f64(percentile(samples, 99.9)) / 1000.0

	println('  store_replica_buffer (512B) percentiles (n=${lat_percentile_samples}):')
	println('    P50   = ${p50:.2f} us')
	println('    P95   = ${p95:.2f} us')
	println('    P99   = ${p99:.2f} us')
	println('    P99.9 = ${p999:.2f} us')
}

// ---- entry point ------------------------------------------------------------

pub fn run_latency_bench() {
	println('')
	println('=== Replication Latency Benchmark ===')
	println('Performance target: < 5ms (5,000 us) average per-hop latency')
	println('')

	print_header()

	results := [
		bench_single_store_latency(),
		bench_store_latency_1kb(),
		bench_store_latency_10kb(),
		bench_store_and_delete_roundtrip_latency(),
		bench_get_all_latency(),
		bench_stats_update_latency(),
	]

	for r in results {
		print_result(r)
	}
	print_footer()

	println('')
	println('--- Percentile Distribution ---')
	bench_store_latency_percentiles()

	println('')
	println('--- Target Verification ---')
	mut all_pass := true
	for r in results {
		if r.name.contains('store_replica_buffer latency') || r.name.contains('roundtrip') {
			if !check_latency_target(r, lat_target_ns) {
				all_pass = false
			}
		}
	}

	println('')
	if all_pass {
		println('All latency targets MET.')
	} else {
		println('Some latency targets MISSED - review results above.')
	}
	println('')
}
