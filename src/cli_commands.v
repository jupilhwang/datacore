// CLI command handlers
//
// Contains command runner functions for topic, produce, consume,
// group, share-group, acl, cluster, offset, and health commands.
module main

import interface.cli

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
