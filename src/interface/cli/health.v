// Interface Layer - CLI Health Commands
//
// Provides health check commands for the DataCore broker.
// Supports general health, readiness, and liveness checks.
//
// Key features:
// - Health check (Kafka + REST API connectivity)
// - Readiness probe (broker is ready to serve traffic)
// - Liveness probe (broker process is alive)
module cli

import net
import net.http
import os
import time

/// HealthOptions holds health check command options.
pub struct HealthOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	rest_endpoint    string = 'http://localhost:8080'
	pid_path         string = '/tmp/datacore.pid'
	timeout_sec      int    = 5
}

/// parse_health_options parses health check command options.
pub fn parse_health_options(args []string) HealthOptions {
	mut opts := HealthOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = HealthOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--endpoint', '-e' {
				if i + 1 < args.len {
					opts = HealthOptions{
						...opts
						rest_endpoint: args[i + 1]
					}
					i += 1
				}
			}
			'--pid', '-p' {
				if i + 1 < args.len {
					opts = HealthOptions{
						...opts
						pid_path: args[i + 1]
					}
					i += 1
				}
			}
			'--timeout' {
				if i + 1 < args.len {
					opts = HealthOptions{
						...opts
						timeout_sec: args[i + 1].int()
					}
					i += 1
				}
			}
			else {}
		}
		i += 1
	}

	return opts
}

/// run_health checks the overall health of the DataCore broker.
pub fn run_health(opts HealthOptions) ! {
	println('\x1b[33mDataCore Health Check\x1b[0m')
	println('')

	mut all_ok := true

	// 1. Check Kafka TCP connectivity
	kafka_ok := check_kafka_connectivity(opts.bootstrap_server, opts.timeout_sec)
	print_health_item('Kafka TCP', opts.bootstrap_server, kafka_ok)
	if !kafka_ok {
		all_ok = false
	}

	// 2. Check REST API
	rest_ok := check_rest_api(opts.rest_endpoint, opts.timeout_sec)
	print_health_item('REST API', opts.rest_endpoint, rest_ok)
	if !rest_ok {
		all_ok = false
	}

	// 3. Check process (PID file)
	process_ok := check_process(opts.pid_path)
	print_health_item('Process', opts.pid_path, process_ok)
	if !process_ok {
		all_ok = false
	}

	println('')
	if all_ok {
		println('\x1b[32m\u2713 Healthy\x1b[0m')
	} else {
		println('\x1b[31m\u2717 Unhealthy\x1b[0m')
		exit(1)
	}
}

/// run_readiness checks if the broker is ready to serve traffic.
pub fn run_readiness(opts HealthOptions) ! {
	// Readiness = Kafka port is accepting connections
	kafka_ok := check_kafka_connectivity(opts.bootstrap_server, opts.timeout_sec)

	if kafka_ok {
		println('\x1b[32m\u2713 Ready\x1b[0m')
	} else {
		println('\x1b[31m\u2717 Not Ready\x1b[0m')
		exit(1)
	}
}

/// run_liveness checks if the broker process is alive.
pub fn run_liveness(opts HealthOptions) ! {
	// Liveness = PID file exists and process is running
	if !os.exists(opts.pid_path) {
		println('\x1b[31m\u2717 Not Alive (no PID file)\x1b[0m')
		exit(1)
	}

	pid_str := os.read_file(opts.pid_path) or {
		println('\x1b[31m\u2717 Not Alive (cannot read PID file)\x1b[0m')
		exit(1)
	}

	pid := pid_str.trim_space().int()
	if pid <= 0 {
		println('\x1b[31m\u2717 Not Alive (invalid PID)\x1b[0m')
		exit(1)
	}

	$if linux || macos {
		result := os.execute('kill -0 ${pid} 2>/dev/null')
		if result.exit_code != 0 {
			println('\x1b[31m\u2717 Not Alive (PID ${pid} not found)\x1b[0m')
			exit(1)
		}
	}

	println('\x1b[32m\u2713 Alive (PID: ${pid})\x1b[0m')
}

/// print_health_help prints health command help.
pub fn print_health_help() {
	println('\x1b[33mHealth Commands:\x1b[0m')
	println('')
	println('Usage: datacore health [options]')
	println('       datacore readiness [options]')
	println('       datacore liveness [options]')
	println('')
	println('Commands:')
	println('  health      Full health check (Kafka + REST API + process)')
	println('  readiness   Readiness probe (Kafka connectivity)')
	println('  liveness    Liveness probe (process alive check)')
	println('')
	println('Options:')
	println('  -b, --bootstrap-server  Kafka broker address (default: localhost:9092)')
	println('  -e, --endpoint          REST API endpoint (default: http://localhost:8080)')
	println('  -p, --pid               PID file path (default: /tmp/datacore.pid)')
	println('      --timeout           Connection timeout in seconds (default: 5)')
	println('')
	println('Exit Codes:')
	println('  0  Healthy / Ready / Alive')
	println('  1  Unhealthy / Not Ready / Not Alive')
	println('')
	println('Examples:')
	println('  datacore health')
	println('  datacore readiness --bootstrap-server localhost:9092')
	println('  datacore liveness --pid /var/run/datacore.pid')
}

fn check_kafka_connectivity(addr string, timeout_sec int) bool {
	parts := addr.split(':')
	host := if parts.len > 0 { parts[0] } else { 'localhost' }
	port := if parts.len > 1 { parts[1].int() } else { 9092 }

	mut conn := net.dial_tcp('${host}:${port}') or { return false }
	conn.set_read_timeout(timeout_sec * time.second)

	// Send a minimal ApiVersions request to verify protocol compatibility
	request := build_api_versions_request()
	conn.write(request) or {
		conn.close() or {}
		return false
	}

	mut size_buf := []u8{len: 4}
	conn.read(mut size_buf) or {
		conn.close() or {}
		return false
	}

	conn.close() or {}
	return true
}

fn check_rest_api(endpoint string, timeout_sec int) bool {
	health_url := '${endpoint}/health'
	resp := http.get(health_url) or {
		// Try /metrics as fallback
		metrics_resp := http.get('${endpoint}/metrics') or { return false }
		return metrics_resp.status_code == 200
	}
	return resp.status_code == 200
}

fn check_process(pid_path string) bool {
	if !os.exists(pid_path) {
		return false
	}
	pid_str := os.read_file(pid_path) or { return false }
	pid := pid_str.trim_space().int()
	if pid <= 0 {
		return false
	}
	$if linux || macos {
		result := os.execute('kill -0 ${pid} 2>/dev/null')
		return result.exit_code == 0
	}
	return true
}

fn print_health_item(name string, target string, ok bool) {
	status := if ok { '\x1b[32m\u2713 OK\x1b[0m  ' } else { '\x1b[31m\u2717 FAIL\x1b[0m' }
	println('  ${status}  ${name}  ${target}')
}

fn build_api_versions_request() []u8 {
	mut msg := []u8{}

	// API Key: 18 (ApiVersions)
	// API Version: 3
	// Correlation ID: 1
	// Client ID: "datacore-health"

	client_id := 'datacore-health'
	mut header := []u8{}

	header << u8(0)
	header << u8(18)
	header << u8(0)
	header << u8(3)
	header << u8(0)
	header << u8(0)
	header << u8(0)
	header << u8(1)
	// Non-flexible: client ID as nullable string (i16 length + bytes)
	header << u8(client_id.len >> 8)
	header << u8(client_id.len & 0xff)
	header << client_id.bytes()

	// Body: client software name + version (compact strings) + tagged fields
	header << u8(1) // compact string len+1=1 means empty
	header << u8(1)
	header << u8(0) // tagged fields

	size := i32(header.len)
	msg << u8(size >> 24)
	msg << u8((size >> 16) & 0xff)
	msg << u8((size >> 8) & 0xff)
	msg << u8(size & 0xff)
	msg << header

	return msg
}
