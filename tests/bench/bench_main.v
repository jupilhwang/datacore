// Replication benchmark runner.
// Runs all replication performance benchmarks in sequence and prints a summary.
//
// Usage (from project src/ directory):
//   v -enable-globals run ../tests/bench/
//
// Individual benchmark selection via RUN_BENCH environment variable:
//   RUN_BENCH=throughput   - run throughput benchmark only
//   RUN_BENCH=latency      - run latency benchmark only
//   RUN_BENCH=election     - run leader election benchmark only
//   RUN_BENCH=follower     - run follower sync benchmark only
//   (default: run all)
module main

import os

fn main() {
	filter := os.getenv('RUN_BENCH')

	println('')
	println('DataCore Replication Benchmark Suite')
	println('=====================================')
	if filter != '' {
		println('Filter: ${filter}')
	}
	println('')

	if filter == '' || filter == 'throughput' {
		run_throughput_bench()
	}

	if filter == '' || filter == 'latency' {
		run_latency_bench()
	}

	if filter == '' || filter == 'election' {
		run_election_bench()
	}

	if filter == '' || filter == 'follower' {
		run_follower_bench()
	}

	println('')
	println('Benchmark suite complete.')
	println('')
}
