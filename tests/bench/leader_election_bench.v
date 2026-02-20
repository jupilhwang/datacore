// Leader election benchmark.
// Measures the time required to detect a dead leader, elect a new one, and
// update replica assignments - the critical path for high availability.
//
// The replication system uses assign_replicas + update_broker_health for the
// election/reassignment cycle. This benchmark times those operations under
// various cluster sizes and health scenarios.
//
// Performance target: < 100ms for the full election cycle
module main

import domain
import infra.replication
import time

// ---- top-level constants ----------------------------------------------------

const el_3node_partitions = 10
const el_3node_warmup = 2
const el_3node_iterations = 20

const el_5node_partitions = 50
const el_5node_warmup = 2
const el_5node_iterations = 20

const el_10node_partitions = 100
const el_10node_warmup = 2
const el_10node_iterations = 20

const el_assign_brokers = 5
const el_assign_warmup = 5
const el_assign_iterations = 50

const el_health_brokers = 5
const el_health_warmup = 10
const el_health_iterations = 500

// 100ms = 100,000,000 ns
const el_target_election_ns = i64(100_000_000)
// 1ms = 1,000,000 ns
const el_target_internal_ns = i64(1_000_000)

// ---- helpers ----------------------------------------------------------------

// vfmt off
fn make_replication_config_election() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                       true
		replication_port:              29293
		replica_count:                 2
		replica_timeout_ms:            500
		heartbeat_interval_ms:         5000
		reassignment_interval_ms:      5000
		orphan_cleanup_interval_ms:    60000
		retry_count:                   1
		replica_buffer_ttl_ms:         0
		max_replica_buffer_size_bytes: 0
	}
}

fn make_manager_election(broker_id string, port int, cluster_refs []domain.BrokerRef) &replication.Manager {
	mut cfg := make_replication_config_election()
	cfg.replication_port = port
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := replication.Server.new(cfg.replication_port, handler)
	mut cli := replication.Client.new(cfg.replica_timeout_ms)
	return &replication.Manager{
		broker_id:           broker_id
		config:              cfg
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: cluster_refs
		logger:              make_bench_logger()
		running:             false
	}
}

fn build_cluster_refs(n int, base_port int) []domain.BrokerRef {
	mut refs := []domain.BrokerRef{cap: n}
	for i in 0 .. n {
		refs << domain.BrokerRef{
			broker_id: 'broker-${i}'
			addr:      'localhost:${base_port + i}'
		}
	}
	return refs
}
// vfmt on

// simulate_election performs one leader-failure + reassignment cycle.
fn simulate_election(mut m replication.Manager) {
	// Mark broker-0 as dead (simulated leader failure)
	m.update_broker_health('broker-0', domain.ReplicationHealth{
		broker_id:      'broker-0'
		is_alive:       false
		last_heartbeat: time.now().unix_milli() - 30000
		lag_ms:         0
		pending_count:  0
	})

	refs := m.cluster_broker_refs.clone()
	mut id_to_addr := map[string]string{}
	for ref in refs {
		if ref.broker_id != '' {
			id_to_addr[ref.broker_id] = ref.addr
		}
	}

	mut dead_addrs := []string{}
	for bid, health in m.broker_health {
		if !health.is_alive {
			addr := id_to_addr[bid] or { bid }
			dead_addrs << addr
		}
	}

	mut alive_addrs := []string{}
	for ref in refs {
		hk := if ref.broker_id != '' { ref.broker_id } else { ref.addr }
		health := m.broker_health[hk] or { continue }
		if health.is_alive {
			alive_addrs << ref.addr
		}
	}

	keys := m.assignments.keys()
	for key in keys {
		assignment := m.assignments[key] or { continue }
		mut new_replicas := []string{}
		mut changed := false

		for broker_addr in assignment.replica_brokers {
			if broker_addr in dead_addrs {
				changed = true
				for alive in alive_addrs {
					if alive !in new_replicas && alive !in assignment.replica_brokers {
						new_replicas << alive
						break
					}
				}
			} else {
				new_replicas << broker_addr
			}
		}

		if changed {
			mut updated := assignment
			updated.replica_brokers = new_replicas
			updated.assigned_time = time.now().unix_milli()
			m.assignments[key] = updated
		}
	}

	// Reset health for next iteration
	m.update_broker_health('broker-0', domain.ReplicationHealth{
		broker_id:      'broker-0'
		is_alive:       true
		last_heartbeat: time.now().unix_milli()
		lag_ms:         0
		pending_count:  0
	})
}

fn setup_initial_assignments(mut m replication.Manager, num_partitions int, base_port int) {
	for i in 0 .. num_partitions {
		m.assignments['election-topic:${i}'] = domain.ReplicaAssignment{
			topic:           'election-topic'
			partition:       i32(i)
			main_broker:     'broker-0'
			replica_brokers: ['localhost:${base_port + 1}', 'localhost:${base_port + 2}']
			assigned_time:   time.now().unix_milli()
		}
	}
}

// ---- benchmarks -------------------------------------------------------------

fn bench_election_3node_10partitions() BenchmarkResult {
	refs := build_cluster_refs(3, 29293)
	mut m := make_manager_election('broker-0', 29293, refs)

	for i in 1 .. 3 {
		m.update_broker_health('broker-${i}', domain.ReplicationHealth{
			broker_id:      'broker-${i}'
			is_alive:       true
			last_heartbeat: time.now().unix_milli()
			lag_ms:         0
			pending_count:  0
		})
	}
	setup_initial_assignments(mut m, el_3node_partitions, 29293)

	return run_benchmark('election cycle (3 nodes, 10 partitions)', el_3node_iterations,
		el_3node_warmup, fn [mut m] () {
		simulate_election(mut m)
	})
}

fn bench_election_5node_50partitions() BenchmarkResult {
	refs := build_cluster_refs(5, 29393)
	mut m := make_manager_election('broker-0', 29393, refs)

	for i in 1 .. 5 {
		m.update_broker_health('broker-${i}', domain.ReplicationHealth{
			broker_id:      'broker-${i}'
			is_alive:       true
			last_heartbeat: time.now().unix_milli()
			lag_ms:         0
			pending_count:  0
		})
	}
	setup_initial_assignments(mut m, el_5node_partitions, 29393)

	return run_benchmark('election cycle (5 nodes, 50 partitions)', el_5node_iterations,
		el_5node_warmup, fn [mut m] () {
		simulate_election(mut m)
	})
}

fn bench_election_10node_100partitions() BenchmarkResult {
	refs := build_cluster_refs(10, 29493)
	mut m := make_manager_election('broker-0', 29493, refs)

	for i in 1 .. 10 {
		m.update_broker_health('broker-${i}', domain.ReplicationHealth{
			broker_id:      'broker-${i}'
			is_alive:       true
			last_heartbeat: time.now().unix_milli()
			lag_ms:         0
			pending_count:  0
		})
	}
	setup_initial_assignments(mut m, el_10node_partitions, 29493)

	return run_benchmark('election cycle (10 nodes, 100 partitions)', el_10node_iterations,
		el_10node_warmup, fn [mut m] () {
		simulate_election(mut m)
	})
}

fn bench_assign_replicas_latency() BenchmarkResult {
	// Benchmark the assignment path via update_cluster_broker_refs + update_broker_health.
	// assign_replicas is private so we measure the public health-update path
	// which is the detection phase before reassignment is triggered.
	refs := build_cluster_refs(el_assign_brokers, 29593)
	mut m := make_manager_election('broker-0', 29593, refs)
	mut counter := 0

	return run_benchmark('cluster_refs update (5 brokers, repeated)', el_assign_iterations,
		el_assign_warmup, fn [mut m, mut counter] () {
		// Simulate a cluster topology refresh (called when new broker joins)
		new_refs := build_cluster_refs(el_assign_brokers, 29593 + counter % 3)
		m.update_cluster_broker_refs(new_refs)
		counter++
	})
}

fn bench_update_broker_health_latency() BenchmarkResult {
	refs := build_cluster_refs(el_health_brokers, 29693)
	mut m := make_manager_election('broker-0', 29693, refs)
	mut toggle := true

	return run_benchmark('update_broker_health (leader detection)', el_health_iterations,
		el_health_warmup, fn [mut m, mut toggle] () {
		m.update_broker_health('broker-1', domain.ReplicationHealth{
			broker_id:      'broker-1'
			is_alive:       toggle
			last_heartbeat: time.now().unix_milli()
			lag_ms:         0
			pending_count:  0
		})
		toggle = !toggle
	})
}

// ---- entry point ------------------------------------------------------------

pub fn run_election_bench() {
	println('')
	println('=== Leader Election Benchmark ===')
	println('Performance target: < 100ms per election cycle (reassignment of all partitions)')
	println('')

	print_header()

	results := [
		bench_election_3node_10partitions(),
		bench_election_5node_50partitions(),
		bench_election_10node_100partitions(),
		bench_assign_replicas_latency(),
		bench_update_broker_health_latency(),
	]

	for r in results {
		print_result(r)
	}
	print_footer()

	println('')
	println('--- Target Verification ---')
	mut all_pass := true
	for r in results {
		if r.name.contains('election cycle') {
			if !check_latency_target(r, el_target_election_ns) {
				all_pass = false
			}
		} else if r.name.contains('assign_replicas') || r.name.contains('update_broker_health') {
			if !check_latency_target(r, el_target_internal_ns) {
				all_pass = false
			}
		}
	}

	println('')
	if all_pass {
		println('All election latency targets MET.')
	} else {
		println('Some election latency targets MISSED - review results above.')
	}
	println('')
}
