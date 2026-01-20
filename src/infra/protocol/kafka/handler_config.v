// Infra Layer - Kafka Protocol Handler - Config Operations
// DescribeConfigs handler
module kafka

// DescribeConfigs handler
fn (mut h Handler) handle_describe_configs(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_configs_request(mut reader, version, is_flexible_version(.describe_configs,
		version))!

	resp := h.process_describe_configs(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_describe_configs(req DescribeConfigsRequest, version i16) !DescribeConfigsResponse {
	mut results := []DescribeConfigsResult{}

	for res in req.resources {
		// resource_type: 2 = TOPIC, 4 = BROKER
		if res.resource_type == 2 {
			// TOPIC config
			topic_name := res.resource_name

			// Try to find topic
			topic := h.storage.get_topic(topic_name) or {
				results << DescribeConfigsResult{
					error_code:    i16(ErrorCode.unknown_topic_or_partition)
					error_message: 'Topic ${topic_name} not found'
					resource_type: res.resource_type
					resource_name: topic_name
					configs:       []
				}
				continue
			}

			// Prepare configs
			mut configs := []DescribeConfigsEntry{}

			// Helper to check if a key is requested
			is_requested := fn (key string, names ?[]string) bool {
				if names == none {
					return true
				}
				for n in names {
					if n == key {
						return true
					}
				}
				return false
			}

			if is_requested('retention.ms', res.config_names) {
				configs << DescribeConfigsEntry{
					name:          'retention.ms'
					value:         topic.config['retention.ms'] or { '604800000' }
					read_only:     false
					is_default:    true
					config_source: 4 // DEFAULT_CONFIG
					is_sensitive:  false
					synonyms:      []
					config_type:   0
					documentation: 'The maximum time to retain a log before discarding it'
				}
			}

			if is_requested('cleanup.policy', res.config_names) {
				configs << DescribeConfigsEntry{
					name:          'cleanup.policy'
					value:         topic.config['cleanup.policy'] or { 'delete' }
					read_only:     false
					is_default:    true
					config_source: 4
					is_sensitive:  false
					synonyms:      []
					config_type:   0
					documentation: 'The cleanup policy for the topic'
				}
			}

			results << DescribeConfigsResult{
				error_code:    0
				error_message: none
				resource_type: res.resource_type
				resource_name: topic_name
				configs:       configs
			}
		} else if res.resource_type == 4 {
			// BROKER config
			// Return broker configs
			broker_id_str := res.resource_name

			// Check if it matches our broker ID
			if broker_id_str == '${h.broker_id}' {
				mut configs := []DescribeConfigsEntry{}
				// TODO: Add actual broker configs if needed

				results << DescribeConfigsResult{
					error_code:    0
					error_message: none
					resource_type: res.resource_type
					resource_name: broker_id_str
					configs:       configs
				}
			} else {
				results << DescribeConfigsResult{
					error_code:    i16(ErrorCode.resource_not_found)
					error_message: 'Broker ${broker_id_str} not found'
					resource_type: res.resource_type
					resource_name: broker_id_str
					configs:       []
				}
			}
		} else {
			// Unsupported resource type
			results << DescribeConfigsResult{
				error_code:    i16(ErrorCode.invalid_request)
				error_message: 'Unsupported resource type ${res.resource_type}'
				resource_type: res.resource_type
				resource_name: res.resource_name
				configs:       []
			}
		}
	}

	return DescribeConfigsResponse{
		throttle_time_ms: 0
		results:          results
	}
}
