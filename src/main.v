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
import startup
import interface.server
import interface.cli
import interface.rest
import infra.compression
import infra.observability
import service.schema

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
		'share-group' {
			run_share_group(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'acl' {
			run_acl(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'cluster' {
			run_cluster(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'offset' {
			run_offset(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'health' {
			run_health_check(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'readiness' {
			run_readiness_check(args[1..]) or {
				eprintln('\x1b[31mError:\x1b[0m ${err}')
				exit(1)
			}
		}
		'liveness' {
			run_liveness_check(args[1..]) or {
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

	// Initialize global writer pool for protocol encoding
	startup.init_writer_pool(startup.default_writer_pool_config())

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
	mut storage_result := startup.init_storage(conf, mut logger) or {
		cli.print_failed('Failed to initialize storage: ${err}')
		exit(1)
	}
	mut storage := storage_result.storage
	s3_adapter_ref := storage_result.s3_adapter
	cli.print_done()

	// 2. Create Compression Service
	cli.print_progress('Initializing compression service')
	compression_service := compression.new_default_compression_service() or {
		cli.print_failed('Failed to initialize compression service: ${err}')
		exit(1)
	}
	cli.print_done()

	// 3. Create Protocol Handler with storage and compression injection
	cli.print_progress('Initializing Kafka protocol handler')
	mut protocol_handler := startup.init_protocol_handler(conf, storage, compression_service)
	cli.print_done()

	// 4. Initialize Broker Registry for multi-broker mode (S3 storage)
	cluster_result := startup.init_cluster_registry(conf, mut storage, s3_adapter_ref, mut
		protocol_handler, mut logger)
	mut broker_registry_opt := cluster_result.registry

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

		// Initialize and register Schema Registry API if enabled
		if conf.schema_registry.enabled {
			cli.print_progress('Initializing schema registry')

			// Create schema registry configuration
			schema_config := schema.RegistryConfig{
				default_compatibility: .backward
				auto_register:         true
			}

			// Create schema registry with storage adapter
			mut schema_registry := schema.new_registry(storage, schema_config)

			// Load existing schemas from storage
			schema_registry.load_from_storage() or {
				logger.warn('Failed to load schemas from storage', observability.field_string('error',
					'${err}'))
			}

			logger.info('Schema registry initialized', observability.field_string('topic',
				conf.schema_registry.topic))

			// Register schema API with REST server
			schema_api := rest.new_schema_api(schema_registry)
			rest_server.set_schema_api(schema_api)
			logger.info('Schema Registry API registered with REST server')
		}

		rest_server.start_background()
		cli.print_done()
		logger.info('REST API server started', observability.field_string('host', conf.rest.host),
			observability.field_int('port', conf.rest.port))
	}

	// 6. Start gRPC Gateway if enabled
	if conf.grpc.enabled {
		cli.print_progress('Starting gRPC gateway')
		startup.init_grpc_server(conf, storage, mut logger) or {
			cli.print_failed('Failed to start gRPC gateway')
		}
		cli.print_done()
	}

	// 7. Start heartbeat worker after all initialization is complete
	// heartbeat_loop uses r.lock which can contend with set_partition_assigner on main thread
	if mut registry := broker_registry_opt {
		registry.start_heartbeat_worker()
		logger.info('Heartbeat worker started')
	}

	// 8. Create and start TCP Server
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
		'alter' {
			cli.run_topic_alter(opts)!
		}
		'help', '-h', '--help' {
			cli.print_topic_help()
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
		return error('Usage: datacore group <list|describe|delete|reset-offset> [options]')
	}

	match args[0] {
		'list' {
			opts := cli.parse_group_options(args[1..])
			cli.run_group_list(opts)!
		}
		'describe' {
			opts := cli.parse_group_options(args[1..])
			cli.run_group_describe(opts)!
		}
		'delete' {
			opts := cli.parse_group_options(args[1..])
			cli.run_group_delete(opts)!
		}
		'reset-offset' {
			opts := cli.parse_group_options(args[1..])
			cli.run_group_reset_offset(opts)!
		}
		'help', '-h', '--help' {
			cli.print_group_help()
		}
		else {
			return error('Unknown group command: ${args[0]}')
		}
	}
}

// Share Group commands
fn run_share_group(args []string) ! {
	if args.len == 0 {
		cli.print_share_group_help()
		return
	}

	match args[0] {
		'list' {
			opts := cli.parse_share_group_options(args[1..])
			cli.run_share_group_list(opts)!
		}
		'describe' {
			opts := cli.parse_share_group_options(args[1..])
			cli.run_share_group_describe(opts)!
		}
		'delete' {
			opts := cli.parse_share_group_options(args[1..])
			cli.run_share_group_delete(opts)!
		}
		'help', '-h', '--help' {
			cli.print_share_group_help()
		}
		else {
			return error('Unknown share-group command: ${args[0]}')
		}
	}
}

// ACL commands
fn run_acl(args []string) ! {
	if args.len == 0 {
		cli.print_acl_help()
		return
	}

	match args[0] {
		'create' {
			opts := cli.parse_acl_options(args[1..])
			cli.run_acl_create(opts)!
		}
		'list' {
			opts := cli.parse_acl_options(args[1..])
			cli.run_acl_list(opts)!
		}
		'delete' {
			opts := cli.parse_acl_options(args[1..])
			cli.run_acl_delete(opts)!
		}
		'help', '-h', '--help' {
			cli.print_acl_help()
		}
		else {
			return error('Unknown acl command: ${args[0]}')
		}
	}
}

// Cluster commands
fn run_cluster(args []string) ! {
	if args.len == 0 {
		cli.print_cluster_help()
		return
	}

	match args[0] {
		'describe' {
			opts := cli.parse_cluster_options(args[1..])
			cli.run_cluster_describe(opts)!
		}
		'brokers' {
			opts := cli.parse_cluster_options(args[1..])
			cli.run_cluster_brokers(opts)!
		}
		'config' {
			opts := cli.parse_cluster_options(args[1..])
			cli.run_cluster_config(opts)!
		}
		'help', '-h', '--help' {
			cli.print_cluster_help()
		}
		else {
			return error('Unknown cluster command: ${args[0]}')
		}
	}
}

// Offset commands
fn run_offset(args []string) ! {
	if args.len == 0 {
		cli.print_offset_help()
		return
	}

	match args[0] {
		'get' {
			opts := cli.parse_offset_options(args[1..])
			cli.run_offset_get(opts)!
		}
		'set' {
			opts := cli.parse_offset_options(args[1..])
			cli.run_offset_set(opts)!
		}
		'help', '-h', '--help' {
			cli.print_offset_help()
		}
		else {
			return error('Unknown offset command: ${args[0]}')
		}
	}
}

// Health check commands
fn run_health_check(args []string) ! {
	if args.len > 0 && (args[0] == 'help' || args[0] == '-h' || args[0] == '--help') {
		cli.print_health_help()
		return
	}
	opts := cli.parse_health_options(args)
	cli.run_health(opts)!
}

fn run_readiness_check(args []string) ! {
	opts := cli.parse_health_options(args)
	cli.run_readiness(opts)!
}

fn run_liveness_check(args []string) ! {
	opts := cli.parse_health_options(args)
	cli.run_liveness(opts)!
}

// Broker Status Implementation

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
fn get_broker_stats(metrics_host string, metrics_port int) BrokerStats {
	host := if metrics_host == '0.0.0.0' { 'localhost' } else { metrics_host }
	url := 'http://${host}:${metrics_port}/metrics'

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
		stats := get_broker_stats(conf.broker.host, conf.observability.metrics.prometheus_port)

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
