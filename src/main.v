// DataCore - Kafka-compatible message broker
// Copyright 2026 DataCore Authors
// SPDX-License-Identifier: Apache-2.0
//
// Architecture: Clean Architecture
// Layers: Interface → Infra → Service → Domain

module main

import os
import config as cfg
import interface.server
import interface.cli
import infra.protocol.kafka
import infra.storage.plugins.memory
import infra.storage.plugins.s3
import infra.observability
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
			start_broker(app, opts)!
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

fn start_broker(app &cli.App, opts cli.CliOptions) ! {
	// Print banner
	cli.print_banner(app)

	// Load configuration
	cli.print_progress('Loading configuration')
	conf := cfg.load_config(opts.config_path) or {
		cli.print_failed('${err}')
		return error('Failed to load config')
	}
	cli.print_done()

	// Print startup info
	cli.print_startup_info(conf.broker.host, conf.broker.port, conf.broker.broker_id,
		conf.broker.cluster_id)

	// Initialize logger
	log_level := observability.log_level_from_string(conf.observability.logging.level)
	log_format := if conf.observability.logging.format == 'json' {
		observability.OutputFormat.json
	} else {
		observability.OutputFormat.text
	}

	logger := observability.new_logger(observability.LoggerConfig{
		name:    'datacore'
		level:   log_level
		format:  log_format
		service: 'datacore-broker'
	})

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
		// Map config to S3Config
		s_config := s3.S3Config{
			bucket_name: conf.storage.s3.bucket
			region:      conf.storage.s3.region
			endpoint:    conf.storage.s3.endpoint
			access_key:  conf.storage.s3.access_key
			secret_key:  conf.storage.s3.secret_key
			prefix:      conf.storage.s3.prefix
		}

		masked_key := if s_config.access_key.len > 4 {
			s_config.access_key[0..4] + '****'
		} else {
			'****'
		}
		logger.info('Initializing S3 storage', observability.field_string('bucket', s_config.bucket_name),
			observability.field_string('region', s_config.region), observability.field_string('endpoint',
			s_config.endpoint), observability.field_string('access_key', masked_key))

		if s3_adapter := s3.new_s3_adapter(s_config) {
			storage_opt = port.StoragePort(s3_adapter)
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

	if storage_opt == none {
		cli.print_failed('Failed to initialize any storage engine')
		exit(1)
	}
	mut storage := storage_opt or { panic('unreachable') }
	cli.print_done()

	// 2. Create Protocol Handler with storage injection
	cli.print_progress('Initializing Kafka protocol handler')
	mut protocol_handler := kafka.new_handler(conf.broker.broker_id, conf.broker.host,
		conf.broker.port, conf.broker.cluster_id, storage)
	cli.print_done()

	if conf.schema_registry.enabled {
		logger.info('Schema Registry initialized', observability.field_string('topic',
			conf.schema_registry.topic))
	}

	// 3. Start metrics server if enabled
	if conf.observability.metrics.enabled {
		cli.print_progress('Starting metrics server')
		metrics_server := observability.new_metrics_server(conf.broker.host, conf.observability.metrics.prometheus_port)
		metrics_server.start_background()
		cli.print_done()
	}

	// 4. Write PID file
	cli.write_pid(opts.pid_path) or {
		logger.warn('Failed to write PID file', observability.field_string('path', opts.pid_path))
	}

	// 5. Create and start TCP Server
	server_config := server.ServerConfig{
		host:       conf.broker.host
		port:       conf.broker.port
		broker_id:  conf.broker.broker_id
		cluster_id: conf.broker.cluster_id
	}

	mut tcp_server := server.new_server(server_config, protocol_handler)

	// Print startup complete
	cli.print_startup_complete(conf.broker.host, conf.broker.port)

	// Log startup
	logger.info('Broker started', observability.field_int('broker_id', conf.broker.broker_id),
		observability.field_string('host', conf.broker.host), observability.field_int('port',
		conf.broker.port))

	// Start server (blocking)
	defer {
		cli.print_shutdown()
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

fn show_status(opts cli.CliOptions) ! {
	pid := cli.read_pid(opts.pid_path) or {
		cli.print_status(cli.BrokerStatus{
			running: false
		})
		return
	}

	running := cli.check_pid_running(pid)

	if running {
		// TODO: Query broker for actual stats
		cli.print_status(cli.BrokerStatus{
			running:    true
			pid:        pid
			host:       '0.0.0.0'
			port:       9092
			broker_id:  1
			cluster_id: 'datacore-cluster'
		})
	} else {
		cli.remove_pid(opts.pid_path)
		cli.print_status(cli.BrokerStatus{
			running: false
		})
	}
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

	match args[0] {
		'list' {
			println('\x1b[33m⚠\x1b[0m  Group list command not yet implemented.')
			println('    Use kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list')
		}
		'describe' {
			println('\x1b[33m⚠\x1b[0m  Group describe command not yet implemented.')
			println('    Use kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group>')
		}
		else {
			return error('Unknown group command: ${args[0]}')
		}
	}
}
