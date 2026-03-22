// Broker lifecycle management
//
// Contains broker startup, shutdown, and status functions
// extracted from main.v for maintainability.
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
import infra.auth
import infra.compression
import infra.observability
import service.schema

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

	// 3a. Create and attach audit logger
	audit_logger := auth.new_audit_logger(true)
	protocol_handler.set_audit_logger(audit_logger)
	logger.info('Audit logger initialized', observability.field_int('max_buffer_size',
		audit_logger.max_buffer_size))

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

	// Configure rate limiter if enabled
	if conf.broker.rate_limit.enabled {
		rl_cfg := conf.broker.rate_limit
		rate_limiter := server.new_rate_limiter(server.RateLimiterConfig{
			max_requests_per_second:        rl_cfg.max_requests_per_sec
			max_bytes_per_second:           rl_cfg.max_bytes_per_sec
			per_ip_max_requests_per_second: rl_cfg.per_ip_max_requests_per_sec
			per_ip_max_connections:         rl_cfg.per_ip_max_connections
			burst_multiplier:               rl_cfg.burst_multiplier
			window_size_ms:                 i64(rl_cfg.window_ms)
		})
		tcp_server.set_rate_limiter(rate_limiter)
		logger.info('Rate limiter enabled', observability.field_int('max_rps', rl_cfg.max_requests_per_sec),
			observability.field_int('per_ip_max_rps', rl_cfg.per_ip_max_requests_per_sec))
	}

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
