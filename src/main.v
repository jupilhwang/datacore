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
import infra.observability

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
    cli.print_startup_info(conf.broker.host, conf.broker.port, conf.broker.broker_id, conf.broker.cluster_id)
    
    // Initialize logger
    log_level := observability.log_level_from_string(conf.observability.logging.level)
    log_format := if conf.observability.logging.format == 'json' {
        observability.OutputFormat.json
    } else {
        observability.OutputFormat.text
    }
    
    logger := observability.new_logger(observability.LoggerConfig{
        name: 'datacore'
        level: log_level
        format: log_format
        service: 'datacore-broker'
    })
    
    // === Clean Architecture: Dependency Injection ===
    
    // 1. Create Infra Layer components
    cli.print_progress('Initializing storage engine')
    mut storage := memory.new_memory_adapter()
    cli.print_done()
    
    // 2. Create Protocol Handler with storage injection
    cli.print_progress('Initializing Kafka protocol handler')
    mut protocol_handler := kafka.new_handler(
        conf.broker.broker_id,
        conf.broker.host,
        conf.broker.port,
        conf.broker.cluster_id,
        storage
    )
    cli.print_done()
    
    // 3. Start metrics server if enabled
    if conf.observability.metrics.enabled {
        cli.print_progress('Starting metrics server')
        metrics_server := observability.new_metrics_server(conf.broker.host, conf.observability.metrics.port)
        metrics_server.start_background()
        cli.print_done()
    }
    
    // 4. Write PID file
    cli.write_pid(opts.pid_path) or {
        logger.warn('Failed to write PID file', observability.field_string('path', opts.pid_path))
    }
    
    // 5. Create and start TCP Server
    server_config := server.ServerConfig{
        host: conf.broker.host
        port: conf.broker.port
        broker_id: conf.broker.broker_id
        cluster_id: conf.broker.cluster_id
    }
    
    mut tcp_server := server.new_server(server_config, protocol_handler)
    
    // Print startup complete
    cli.print_startup_complete(conf.broker.host, conf.broker.port)
    
    // Log startup
    logger.info('Broker started',
        observability.field_int('broker_id', conf.broker.broker_id),
        observability.field_string('host', conf.broker.host),
        observability.field_int('port', conf.broker.port)
    )
    
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
            running: true
            pid: pid
            host: '0.0.0.0'
            port: 9092
            broker_id: 1
            cluster_id: 'datacore-cluster'
        })
    } else {
        cli.remove_pid(opts.pid_path)
        cli.print_status(cli.BrokerStatus{
            running: false
        })
    }
}

// Placeholder implementations for future CLI commands
fn run_topic(args []string) ! {
    if args.len == 0 {
        return error('Usage: datacore topic <create|list|delete|describe> [options]')
    }
    
    match args[0] {
        'create' {
            println('\x1b[33m⚠\x1b[0m  Topic create command not yet implemented.')
            println('    Use kafka-topics.sh --bootstrap-server localhost:9092 --create ...')
        }
        'list' {
            println('\x1b[33m⚠\x1b[0m  Topic list command not yet implemented.')
            println('    Use kafka-topics.sh --bootstrap-server localhost:9092 --list')
        }
        'delete' {
            println('\x1b[33m⚠\x1b[0m  Topic delete command not yet implemented.')
            println('    Use kafka-topics.sh --bootstrap-server localhost:9092 --delete ...')
        }
        'describe' {
            println('\x1b[33m⚠\x1b[0m  Topic describe command not yet implemented.')
            println('    Use kafka-topics.sh --bootstrap-server localhost:9092 --describe ...')
        }
        else {
            return error('Unknown topic command: ${args[0]}')
        }
    }
}

fn run_produce(args []string) ! {
    println('\x1b[33m⚠\x1b[0m  Produce command not yet implemented.')
    println('    Use kafka-console-producer.sh --bootstrap-server localhost:9092 --topic <topic>')
}

fn run_consume(args []string) ! {
    println('\x1b[33m⚠\x1b[0m  Consume command not yet implemented.')
    println('    Use kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic> --from-beginning')
}

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
