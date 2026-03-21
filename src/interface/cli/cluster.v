// Interface Layer - CLI Cluster Commands
//
// Provides cluster information commands using the Kafka protocol.
// Supports cluster describe, broker listing, and configuration.
//
// Key features:
// - Describe cluster metadata
// - List brokers
// - Show cluster configuration
module cli

import net.http

/// ClusterOptions holds cluster command options.
pub struct ClusterOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	config_path      string = 'config.toml'
}

/// parse_cluster_options parses cluster command options.
pub fn parse_cluster_options(args []string) ClusterOptions {
	mut opts := ClusterOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = ClusterOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'-c', '--config' {
				if i + 1 < args.len {
					opts = ClusterOptions{
						...opts
						config_path: args[i + 1]
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

/// run_cluster_describe displays cluster metadata.
pub fn run_cluster_describe(opts ClusterOptions) ! {
	println('\x1b[90mFetching cluster information...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Metadata request with empty topics (all topics)
	request := build_metadata_request([])
	send_kafka_request(mut conn, 3, 12, request)!
	response := read_kafka_response(mut conn)!

	info := parse_cluster_metadata_response(response)

	println('')
	println('\x1b[33mCluster Information:\x1b[0m')
	println('  Cluster ID:    ${info.cluster_id}')
	println('  Controller ID: ${info.controller_id}')
	println('')
	println('  Brokers: ${info.brokers.len}')
	for broker in info.brokers {
		println('    [${broker.broker_id}] ${broker.host}:${broker.port}')
	}
}

/// run_cluster_brokers lists all brokers in the cluster.
pub fn run_cluster_brokers(opts ClusterOptions) ! {
	println('\x1b[90mFetching broker list...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_metadata_request([])
	send_kafka_request(mut conn, 3, 12, request)!
	response := read_kafka_response(mut conn)!

	info := parse_cluster_metadata_response(response)

	println('')
	println('\x1b[33mBrokers:\x1b[0m')
	println('  ID    HOST                     PORT   RACK')
	println('  ' + '-'.repeat(55))
	for broker in info.brokers {
		rack := if broker.rack.len > 0 { broker.rack } else { '-' }
		println('  ${broker.broker_id}  ${broker.host}:${broker.port}  rack=${rack}')
	}
	println('')
	println('  Total: ${info.brokers.len} broker(s)')
}

/// run_cluster_config displays cluster configuration.
pub fn run_cluster_config(opts ClusterOptions) ! {
	println('\x1b[90mFetching cluster configuration...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DescribeConfigs request (API Key 32, Version 4) for cluster resource
	request := build_describe_cluster_configs_request()
	send_kafka_request(mut conn, 32, 4, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 10 {
		println('\x1b[33mNo cluster configuration available\x1b[0m')
		return
	}

	println('')
	println('\x1b[33mCluster Configuration:\x1b[0m')
	println('  Note: Use the broker admin API for detailed configuration')
}

/// print_cluster_help prints cluster command help.
pub fn print_cluster_help() {
	println('\x1b[33mCluster Commands:\x1b[0m')
	println('')
	println('Usage: datacore cluster <command> [options]')
	println('')
	println('Commands:')
	println('  describe    Show cluster information')
	println('  brokers     List all brokers')
	println('  config      Show cluster configuration')
	println('')
	println('Options:')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('')
	println('Examples:')
	println('  datacore cluster describe')
	println('  datacore cluster brokers')
	println('  datacore cluster config')
}

// ClusterInfo holds parsed cluster metadata
struct ClusterInfo {
	cluster_id    string
	controller_id int
	brokers       []BrokerSummary
}

// BrokerSummary holds brief broker information
struct BrokerSummary {
	broker_id int
	host      string
	port      int
	rack      string
}

// parse_cluster_metadata_response parses broker and cluster info from a Metadata response.
fn parse_cluster_metadata_response(response []u8) ClusterInfo {
	mut cluster_id := ''
	mut controller_id := -1
	mut brokers := []BrokerSummary{}

	// Minimum response length check
	if response.len < 20 {
		return ClusterInfo{
			cluster_id:    cluster_id
			controller_id: controller_id
			brokers:       brokers
		}
	}

	mut pos := 4 // skip correlation_id

	// Tagged fields (compact format)
	if pos < response.len {
		pos += 1
	}

	// Throttle time ms (4 bytes)
	if pos + 4 > response.len {
		return ClusterInfo{
			cluster_id:    cluster_id
			controller_id: controller_id
			brokers:       brokers
		}
	}
	pos += 4

	// Brokers array (compact array)
	if pos >= response.len {
		return ClusterInfo{
			cluster_id:    cluster_id
			controller_id: controller_id
			brokers:       brokers
		}
	}
	broker_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. broker_count {
		if pos + 4 > response.len {
			break
		}
		broker_id := int(read_i32_be(response, pos))
		pos += 4

		// Host (compact string)
		if pos >= response.len {
			break
		}
		host_len := int(response[pos]) - 1
		pos += 1
		if pos + host_len > response.len {
			break
		}
		host := response[pos..pos + host_len].bytestr()
		pos += host_len

		// Port (4 bytes)
		if pos + 4 > response.len {
			break
		}
		port := int(read_i32_be(response, pos))
		pos += 4

		// Rack (compact nullable string)
		if pos >= response.len {
			break
		}
		rack_len_raw := int(response[pos]) - 1
		pos += 1
		mut rack := ''
		if rack_len_raw > 0 && pos + rack_len_raw <= response.len {
			rack = response[pos..pos + rack_len_raw].bytestr()
			pos += rack_len_raw
		}

		// Tagged fields
		if pos < response.len {
			pos += 1
		}

		brokers << BrokerSummary{
			broker_id: broker_id
			host:      host
			port:      port
			rack:      rack
		}
	}

	// Cluster ID (compact nullable string)
	if pos < response.len {
		cluster_id_len := int(response[pos]) - 1
		pos += 1
		if cluster_id_len > 0 && pos + cluster_id_len <= response.len {
			cluster_id = response[pos..pos + cluster_id_len].bytestr()
			pos += cluster_id_len
		}
	}

	// Controller ID (4 bytes)
	if pos + 4 <= response.len {
		controller_id = int(read_i32_be(response, pos))
	}

	return ClusterInfo{
		cluster_id:    cluster_id
		controller_id: controller_id
		brokers:       brokers
	}
}

fn build_describe_cluster_configs_request() []u8 {
	mut body := []u8{}

	// Resources array (compact array, 1 entry - Cluster resource)
	body << u8(2)

	// Resource type (1 byte) - 4 = CLUSTER
	body << u8(4)

	// Resource name (compact string) - "kafka-cluster"
	cluster_name := 'kafka-cluster'
	body << u8(cluster_name.len + 1)
	body << cluster_name.bytes()

	// Config names (compact array) - empty = all configs
	body << u8(1)

	// Include synonyms (1 byte)
	body << u8(0)

	// Include documentation (1 byte)
	body << u8(0)

	// Tagged fields for resource
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

// ClusterMetricsInfo holds metrics from the REST API
struct ClusterMetricsInfo {
	topics      int
	partitions  int
	connections int
	err         string
}

fn get_cluster_metrics(metrics_host string, metrics_port int) ClusterMetricsInfo {
	host := if metrics_host == '0.0.0.0' { 'localhost' } else { metrics_host }
	url := 'http://${host}:${metrics_port}/metrics'
	resp := http.get(url) or {
		return ClusterMetricsInfo{
			err: 'cannot connect to metrics endpoint on port ${metrics_port}'
		}
	}

	if resp.status_code != 200 {
		return ClusterMetricsInfo{
			err: 'metrics endpoint returned status ${resp.status_code}'
		}
	}

	return ClusterMetricsInfo{
		topics:      get_metric_value_cli(resp.body, 'datacore_topics_total')
		partitions:  get_metric_value_cli(resp.body, 'datacore_partitions_total')
		connections: get_metric_value_cli(resp.body, 'datacore_active_connections')
	}
}

// get_metric_value_cli extracts a gauge metric value from Prometheus text format.
fn get_metric_value_cli(body string, name string) int {
	line_start := body.index('${name}') or { return 0 }
	line_end := body.index_after('\n', line_start) or { return 0 }
	line := body[line_start..line_end]
	parts := line.split(' ')
	if parts.len >= 2 {
		return parts[1].trim_space().int()
	}
	return 0
}
