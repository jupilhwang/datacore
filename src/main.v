// DataCore - Kafka-compatible message broker
// Copyright 2026 DataCore Authors
// SPDX-License-Identifier: Apache-2.0
//
// Architecture: Clean Architecture
// Layers: Interface → Infra → Service → Domain

module main

import os
import time
import net.http
import config as cfg
import domain
import interface.server
import interface.cli
import interface.rest
import infra.protocol.kafka
import infra.storage.plugins.memory
import infra.storage.plugins.s3
import infra.observability
import service.cluster
import service.port

fn main() {
	args := os.args[1..]
	app := cli.new_app()

	if args.len == 0 {
		app.print_help()
		return
	}

	match args[0] {
		'help', '-h', '--help' {
			if args.len > 1 && args[1] == 'broker' {
				app.print_broker_help()
			} else {
				app.print_help()
			}
		}
		'version', '-v', '--version' {
			app.print_version()
		}
		'broker' {
			run_broker(app, args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'topic' {
			run_topic(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'produce' {
			run_produce(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'consume' {
			run_consume(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'group' {
			run_group(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		else {
			eprintln('\x1b[31mError:\x1b[0m Unknown command: ${args[0]}')
			println('')
			app.print_help()
			exit(1)
		}
	}
}

fn run_broker(app &cli.App, args []string) ! {
	if args.len == 0 {
		app.print_broker_help()
		return
	}

	opts := cli.parse_options(args)

	match args[0] {
		'start' {
			// Pass args[1..] to skip 'start' command
			start_broker(app, opts, args[1..])!
		}
		'stop' {
			stop_broker(opts)!
		}
		'status' {
			show_status(opts)!
		}
		'help', '-h', '--help' {
			app.print_broker_help()
		}
		else {
			return error('Unknown broker command: ${args[0]}. Use "datacore broker help" for available commands.')
		}
	}
}

fn start_broker(app &cli.App, opts cli.CliOptions, args []string) ! {
	// Parse CLI arguments for configuration override
	cli_args := cfg.parse_cli_args(args)

	// Print banner
	cli.print_banner(app)

	// Load configuration with CLI args priority
	cli.print_progress('Loading configuration')
	conf := cfg.load_config_with_args(opts.config_path, cli_args) or {
		cli.print_failed('${err}')
		return error('Failed to load config')
	}
	cli.print_done()

	// Print startup info
	cli.print_startup_info(conf.broker.host, conf.broker.port, conf.broker.broker_id,
		conf.broker.cluster_id)

	// Initialize logger with new configuration
	log_level := observability.log_level_from_string(conf.observability.logging.level)
	log_format := if conf.observability.logging.format == 'json' {
		observability.OutputFormat.json
	} else {
		observability.OutputFormat.text
	}
	log_output := observability.log_output_from_string(conf.observability.logging.output)

	// Initialize global logger
	observability.init_global_logger(observability.LoggerConfig{
		name:          'datacore'
		level:         log_level
		format:        log_format
		output:        log_output
		service:       'datacore-broker'
		otlp_endpoint: conf.observability.logging.otlp_endpoint
	})

	mut logger := observability.get_logger()

	// Log configuration summary
	logger.info('Broker configuration summary', observability.field_string('host', conf.broker.host),
		observability.field_int('port', conf.broker.port), observability.field_int('broker_id',
		conf.broker.broker_id), observability.field_string('cluster_id', conf.broker.cluster_id),
		observability.field_int('max_conn', conf.broker.max_connections), observability.field_int('max_req_size',
		conf.broker.max_request_size))

	logger.info('Observability summary', observability.field_bool('metrics_enabled', conf.observability.metrics.enabled),
		observability.field_int('metrics_port', conf.observability.metrics.prometheus_port),
		observability.field_bool('tracing_enabled', conf.observability.tracing.enabled),
		observability.field_string('log_level', conf.observability.logging.level))

	// === Clean Architecture: Dependency Injection ===

	// 1. Create Infra Layer components
	cli.print_progress('Initializing storage engine (${conf.storage.engine})')

	engine := conf.storage.engine.to_lower()
	mut storage_opt := ?port.StoragePort(none)

	if engine == 'memory' {
		logger.info('Initializing Memory storage', observability.field_int('max_memory_mb',
			conf.storage.memory.max_memory_mb), observability.field_int('segment_size',
			conf.storage.memory.segment_size_bytes))
		storage_opt = port.StoragePort(memory.new_memory_adapter())
	} else if engine == 's3' {
		// Set timezone for S3 operations
		os.setenv('TZ', conf.storage.s3.timezone, true)

		// Map config to S3Config
		mut s_config := s3.S3Config{
			bucket_name:            conf.storage.s3.bucket
			region:                 conf.storage.s3.region
			endpoint:               conf.storage.s3.endpoint
			access_key:             conf.storage.s3.access_key
			secret_key:             conf.storage.s3.secret_key
			prefix:                 conf.storage.s3.prefix
			use_path_style:         true // AWS S3 path-style URLs
			timezone:               conf.storage.s3.timezone
			batch_timeout_ms:       conf.storage.s3.batch_timeout_ms
			batch_max_bytes:        conf.storage.s3.batch_max_bytes
			compaction_interval_ms: conf.storage.s3.compaction_interval_ms
			target_segment_bytes:   conf.storage.s3.target_segment_bytes
			index_cache_ttl_ms:     conf.storage.s3.index_cache_ttl_ms
		}

		masked_key := if s_config.access_key.len > 4 {
			s_config.access_key[0..4] + '****'
		} else {
			'****'
		}
		logger.info('Initializing S3 storage', observability.field_string('bucket', s_config.bucket_name),
			observability.field_string('region', s_config.region), observability.field_string('endpoint',
			s_config.endpoint), observability.field_string('access_key', masked_key))

		if mut s3_adapter := s3.new_s3_adapter(s_config) {
			storage_opt = port.StoragePort(s3_adapter)
			s3_adapter.start_workers() // S3 Compaction Worker 시작
		} else {
			cli.print_failed('Failed to init S3 storage: ${err}')
			cli.print_failed('Falling back to memory storage')
			storage_opt = port.StoragePort(memory.new_memory_adapter())
		}
	} else {
		cli.print_failed('Unknown storage engine: ${engine}')
		cli.print_failed('Falling back to memory storage')
		storage_opt = port.StoragePort(memory.new_memory_adapter())
	}

	mut storage := storage_opt or {
		cli.print_failed('Failed to initialize any storage engine')
		exit(1)
	}

	cli.print_done()

	// 2. Create Protocol Handler with storage injection
	cli.print_progress('Initializing Kafka protocol handler')
	// Pass advertised host/port to handler so metadata responses contain the correct connect string
	mut protocol_handler := kafka.new_handler(conf.broker.broker_id, conf.broker.advertised_host,
		conf.broker.port, conf.broker.cluster_id, storage)
	cli.print_done()

	// 3. Initialize Broker Registry for multi-broker mode (S3 storage)
	mut broker_registry_opt := ?&cluster.BrokerRegistry(none)
	capability := storage.get_storage_capability()

	if capability.supports_multi_broker {
		cli.print_progress('Initializing multi-broker cluster registry')

		// Get cluster metadata port from storage
		if mut cluster_port := storage.get_cluster_metadata_port() {
			registry_config := cluster.BrokerRegistryConfig{
				broker_id:             conf.broker.broker_id
				host:                  conf.broker.advertised_host
				port:                  conf.broker.port
				rack:                  '' // TODO: Add rack config
				cluster_id:            conf.broker.cluster_id
				version:               '0.20.0'
				heartbeat_interval_ms: 3000
				session_timeout_ms:    10000
			}

			mut broker_registry := cluster.new_broker_registry(registry_config, capability,
				cluster_port)

			// Register this broker with the cluster
			broker_info := broker_registry.register() or {
				cli.print_failed('Failed to register broker: ${err}')
				domain.BrokerInfo{}
			}

			if broker_info.broker_id > 0 {
				logger.info('Broker registered with cluster', observability.field_int('broker_id',
					broker_info.broker_id), observability.field_string('host', broker_info.host),
					observability.field_int('port', broker_info.port))

				// Start heartbeat worker
				broker_registry.start_heartbeat_worker()

				// Set broker registry on handler for multi-broker metadata responses
				protocol_handler.set_broker_registry(broker_registry)
				broker_registry_opt = broker_registry

				cli.print_done()
			} else {
				cli.print_failed('Broker registration returned invalid info')
			}
		} else {
			cli.print_failed('Storage does not provide cluster metadata port')
		}
	}

	if conf.schema_registry.enabled {
		logger.info('Schema Registry initialized', observability.field_string('topic',
			conf.schema_registry.topic))
	}

	// 3. Metrics server is now served by REST API server on port 8080 (/metrics)
	// Separate metrics server on port 9093 is deprecated

	// 4. Write PID file
	cli.write_pid(opts.pid_path) or {
		logger.warn('Failed to write PID file', observability.field_string('path', opts.pid_path))
	}

	// 5. Start REST API Server (SSE/WebSocket) if enabled
	if conf.rest.enabled {
		cli.print_progress('Starting REST API server (SSE/WebSocket)')
		rest_config := rest.RestServerConfig{
			host:            conf.rest.host
			port:            conf.rest.port
			max_connections: conf.rest.max_connections
			static_dir:      conf.rest.static_dir
			sse_config:      domain.SSEConfig{
				heartbeat_interval_ms: conf.rest.sse_heartbeat_interval_ms
				connection_timeout_ms: conf.rest.sse_connection_timeout_ms
			}
			ws_config:       domain.WebSocketConfig{
				max_message_size: conf.rest.ws_max_message_size
				ping_interval_ms: conf.rest.ws_ping_interval_ms
			}
		}
		mut rest_server := rest.new_rest_server(rest_config, storage)
		rest_server.start_background()
		cli.print_done()
		logger.info('REST API server started', observability.field_string('host', conf.rest.host),
			observability.field_int('port', conf.rest.port))
	}

	// 6. Create and start TCP Server
	server_config := server.ServerConfig{
		host:       conf.broker.host
		port:       conf.broker.port
		broker_id:  conf.broker.broker_id
		cluster_id: conf.broker.cluster_id
	}

	mut tcp_server := server.new_server(server_config, protocol_handler)

	// Log startup
	logger.info('Broker started', observability.field_int('broker_id', conf.broker.broker_id),
		observability.field_string('host', conf.broker.host), observability.field_int('port',
		conf.broker.port))

	// Start server (blocking)
	defer {
		cli.print_shutdown()

		// Deregister broker from cluster if multi-broker mode
		if mut registry := broker_registry_opt {
			registry.stop_heartbeat_worker()
			registry.deregister() or { logger.warn('Failed to deregister broker from cluster') }
			logger.info('Broker deregistered from cluster')
		}

		cli.remove_pid(opts.pid_path)
		logger.info('Broker stopped')
		cli.print_shutdown_complete()
	}

	tcp_server.start()!
}

fn stop_broker(opts cli.CliOptions) ! {
	pid := cli.read_pid(opts.pid_path) or {
		println('\x1b[33m⚠\x1b[0m  Broker is not running (no PID file found)')
		return
	}

	if !cli.check_pid_running(pid) {
		println('\x1b[33m⚠\x1b[0m  Broker is not running (PID ${pid} not found)')
		cli.remove_pid(opts.pid_path)
		return
	}

	cli.print_progress('Stopping broker (PID: ${pid})')

	// Send SIGTERM (or SIGKILL if force)
	signal := if opts.force { 'KILL' } else { 'TERM' }
	result := os.execute('kill -${signal} ${pid}')

	if result.exit_code != 0 {
		cli.print_failed('Failed to stop broker')
		return error('Failed to send signal to process')
	}

	cli.print_done()
	cli.remove_pid(opts.pid_path)
	println('\x1b[32m✓\x1b[0m Broker stopped.')
}

// Topic commands
fn run_topic(args []string) ! {
	if args.len == 0 {
		return error('Usage: datacore topic <create|list|delete|describe> [options]')
	}

	opts := cli.parse_topic_options(args[1..])

	match args[0] {
		'create' {
			cli.run_topic_create(opts)!
		}
		'list' {
			cli.run_topic_list(opts)!
		}
		'delete' {
			cli.run_topic_delete(opts)!
		}
		'describe' {
			cli.run_topic_describe(opts)!
		}
		'help', '-h', '--help' {
			println('\x1b[33mTopic Commands:\x1b[0m')
			println('')
			println('Usage: datacore topic <command> [options]')
			println('')
			println('Commands:')
			println('  create    Create a new topic')
			println('  list      List all topics')
			println('  delete    Delete a topic')
			println('  describe  Describe a topic')
			println('')
			println('Options:')
			println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
			println('  -t, --topic             Topic name')
			println('  -p, --partitions        Number of partitions (default: 1)')
			println('  -r, --replication-factor Replication factor (default: 1)')
		}
		else {
			return error('Unknown topic command: ${args[0]}')
		}
	}
}

// Produce command
fn run_produce(args []string) ! {
	if args.len == 0 || args[0] == 'help' || args[0] == '-h' || args[0] == '--help' {
		cli.print_produce_help()
		return
	}

	opts := cli.parse_produce_options(args)
	cli.run_produce(opts)!
}

// Consume command
fn run_consume(args []string) ! {
	if args.len == 0 || args[0] == 'help' || args[0] == '-h' || args[0] == '--help' {
		cli.print_consume_help()
		return
	}

	opts := cli.parse_consume_options(args)
	cli.run_consume(opts)!
}

// Group commands
fn run_group(args []string) ! {
	if args.len == 0 {
		return error('Usage: datacore group <list|describe> [options]')
	}

	opts := cli.parse_group_options(args[1..])

	match args[0] {
		'list' {
			cli.run_group_list(opts)!
		}
		'describe' {
			cli.run_group_describe(opts)!
		}
		else {
			return error('Unknown group command: ${args[0]}')
		}
	}
}

// ============================================================
// Broker Status Implementation
// ============================================================

struct BrokerStats {
	topics      int
	partitions  int
	connections int
	error_msg   string
}

// get_metric_value extracts a gauge metric value from Prometheus text format
fn get_metric_value(body string, name string) int {
	line_start := body.index('${name}') or { return 0 }
	line_end := body.index_after('\n', line_start) or { return 0 }

	line := body[line_start..line_end]
	parts := line.split(' ')

	if parts.len >= 2 {
		return parts[1].trim_space().int()
	}

	return 0
}

// get_broker_stats queries the running broker's metrics endpoint for live status
fn get_broker_stats(metrics_port int) BrokerStats {
	url := 'http://localhost:${metrics_port}/metrics'

	resp := http.get(url) or {
		return BrokerStats{
			error_msg: 'Failed to connect to metrics server on port ${metrics_port}'
		}
	}

	if resp.status_code != 200 {
		return BrokerStats{
			error_msg: 'Metrics server returned status ${resp.status_code}'
		}
	}

	body := resp.body

	return BrokerStats{
		topics:      get_metric_value(body, 'datacore_topics_total')
		partitions:  get_metric_value(body, 'datacore_partitions_total')
		connections: get_metric_value(body, 'datacore_active_connections')
	}
}

fn show_status(opts cli.CliOptions) ! {
	conf := cfg.load_config(opts.config_path) or {
		return error('Failed to load config for status check: ${err}')
	}

	pid := cli.read_pid(opts.pid_path) or {
		cli.print_status(cli.BrokerStatus{
			running: false
		})
		return
	}

	running := cli.check_pid_running(pid)

	if running {
		stats := get_broker_stats(conf.observability.metrics.prometheus_port)

		mut topics := 0
		mut partitions := 0
		mut connections := 0

		if stats.error_msg.len > 0 {
			eprintln('\x1b[33m⚠\x1b[0m Failed to fetch live metrics: ${stats.error_msg}')
			topics = -1
			partitions = -1
			connections = -1
		} else {
			topics = stats.topics
			partitions = stats.partitions
			connections = stats.connections
		}

		cli.print_status(cli.BrokerStatus{
			running:     true
			pid:         pid
			uptime:      time.Duration(0)
			host:        conf.broker.host
			port:        conf.broker.port
			broker_id:   conf.broker.broker_id
			cluster_id:  conf.broker.cluster_id
			topics:      topics
			partitions:  partitions
			connections: connections
		})
	} else {
		cli.remove_pid(opts.pid_path)
		cli.print_status(cli.BrokerStatus{
			running: false
		})
	}
}
