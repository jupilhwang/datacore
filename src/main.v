// DataCore - Kafka-compatible message broker
// Copyright 2026 DataCore Authors
// SPDX-License-Identifier: Apache-2.0
//
// Architecture: Clean Architecture
// Layers: API → Adapter → UseCase → Entity

module main

import os
import config as cfg
import interface.server
import interface.cli
import infra.protocol.kafka
import infra.storage.plugins.memory

fn main() {
    args := os.args[1..]
    
    if args.len == 0 {
        app := cli.new_app()
        app.print_help()
        return
    }
    
    match args[0] {
        'help', '-h', '--help' {
            app := cli.new_app()
            app.print_help()
        }
        'version', '-v', '--version' {
            app := cli.new_app()
            app.print_version()
        }
        'broker' {
            run_broker(args[1..]) or {
                eprintln('Error: ${err}')
                exit(1)
            }
        }
        else {
            eprintln('Unknown command: ${args[0]}')
            app := cli.new_app()
            app.print_help()
            exit(1)
        }
    }
}

fn run_broker(args []string) ! {
    if args.len == 0 {
        return error('Usage: datacore broker <start|stop|status>')
    }
    
    match args[0] {
        'start' {
            // Load configuration
            config_path := cli.get_config_path(args[1..])
            conf := cfg.load_config(config_path) or {
                return error('Failed to load config: ${err}')
            }
            
            // === Clean Architecture: Dependency Injection ===
            
            // 1. Create Infra Layer components
            //    Storage Adapter (implements port.StoragePort)
            mut storage := memory.new_memory_adapter()
            
            //    Protocol Handler (Kafka) with storage injection
            mut protocol_handler := kafka.new_handler(
                conf.broker.broker_id,
                conf.broker.host,
                conf.broker.port,
                conf.broker.cluster_id,
                storage
            )
            
            // 2. Create API Layer components
            //    TCP Server (uses Protocol Handler)
            server_config := server.ServerConfig{
                host: conf.broker.host
                port: conf.broker.port
                broker_id: conf.broker.broker_id
                cluster_id: conf.broker.cluster_id
            }
            
            mut tcp_server := server.new_server(server_config, protocol_handler)
            
            // 3. Start server
            tcp_server.start()!
        }
        'stop' {
            println('Stopping broker...')
            // TODO: Implement stop (send signal to running process)
        }
        'status' {
            println('Broker status: not running')
            // TODO: Implement status check
        }
        else {
            return error('Unknown broker command: ${args[0]}')
        }
    }
}
