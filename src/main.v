// DataCore - Kafka-compatible message broker
// Copyright 2026 DataCore Authors
// SPDX-License-Identifier: Apache-2.0
//
// Architecture: Clean Architecture
// Layers: Interface -> Infra -> Service -> Domain

module main

import os
import interface.cli

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
