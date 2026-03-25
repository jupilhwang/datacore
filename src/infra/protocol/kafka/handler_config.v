// Kafka protocol - Config operations
// DescribeConfigs
// Request/response types, parsing, encoding, and handlers
module kafka

import infra.observability
import time

// DescribeConfigs (API Key 32)

/// DescribeConfigsRequest holds the request data for DescribeConfigs (API Key 32).
pub struct DescribeConfigsRequest {
pub:
	resources        []DescribeConfigsResource
	include_synonyms bool
}

/// DescribeConfigsResource holds a resource entry for DescribeConfigs.
pub struct DescribeConfigsResource {
pub:
	resource_type i8
	resource_name string
	config_names  ?[]string
}

/// DescribeConfigsResponse holds the response data for DescribeConfigs.
pub struct DescribeConfigsResponse {
pub:
	throttle_time_ms i32
	results          []DescribeConfigsResult
}

/// DescribeConfigsResult holds the config result for a single resource.
pub struct DescribeConfigsResult {
pub:
	error_code    i16
	error_message ?string
	resource_type i8
	resource_name string
	configs       []DescribeConfigsEntry
}

/// DescribeConfigsEntry holds a single config entry in a DescribeConfigs result.
pub struct DescribeConfigsEntry {
pub:
	name          string
	value         ?string
	read_only     bool
	is_default    bool
	config_source i8
	is_sensitive  bool
	synonyms      []DescribeConfigsSynonym
	config_type   i8
	documentation ?string
}

/// DescribeConfigsSynonym holds a synonym config entry with its source.
pub struct DescribeConfigsSynonym {
pub:
	name          string
	value         ?string
	config_source i8
}

fn parse_describe_configs_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeConfigsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut resources := []DescribeConfigsResource{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := reader.read_flex_string(is_flexible)!

		mut config_names := ?[]string(none)

		n_count := reader.read_flex_array_len(is_flexible)!

		if n_count >= 0 {
			mut names := []string{}
			for _ in 0 .. n_count {
				names << reader.read_flex_string(is_flexible)!
			}
			config_names = names.clone()
		}

		reader.skip_flex_tagged_fields(is_flexible)!

		resources << DescribeConfigsResource{
			resource_type: resource_type
			resource_name: resource_name
			config_names:  config_names
		}
	}

	mut include_synonyms := false
	if version >= 1 {
		include_synonyms = reader.read_i8()! != 0
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return DescribeConfigsRequest{
		resources:        resources
		include_synonyms: include_synonyms
	}
}

/// encode serializes the DescribeConfigsResponse into bytes.
pub fn (r DescribeConfigsResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)
		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
		} else {
			writer.write_nullable_string(res.error_message)
		}
		writer.write_i8(res.resource_type)
		if is_flexible {
			writer.write_compact_string(res.resource_name)
			writer.write_compact_array_len(res.configs.len)
		} else {
			writer.write_string(res.resource_name)
			writer.write_array_len(res.configs.len)
		}

		for c in res.configs {
			if is_flexible {
				writer.write_compact_string(c.name)
				writer.write_compact_nullable_string(c.value)
			} else {
				writer.write_string(c.name)
				writer.write_nullable_string(c.value)
			}
			writer.write_i8(if c.read_only { i8(1) } else { i8(0) })

			if version == 0 {
				writer.write_i8(if c.is_default { i8(1) } else { i8(0) })
			} else {
				// v1+ uses config_source instead of is_default
				writer.write_i8(c.config_source)
			}

			writer.write_i8(if c.is_sensitive { i8(1) } else { i8(0) })

			if version >= 1 {
				// synonyms
				if is_flexible {
					writer.write_compact_array_len(c.synonyms.len)
				} else {
					writer.write_array_len(c.synonyms.len)
				}
				for s in c.synonyms {
					if is_flexible {
						writer.write_compact_string(s.name)
						writer.write_compact_nullable_string(s.value)
					} else {
						writer.write_string(s.name)
						writer.write_nullable_string(s.value)
					}
					writer.write_i8(s.config_source)
					if is_flexible {
						writer.write_tagged_fields()
					}
				}
			}

			if version >= 3 {
				writer.write_i8(c.config_type)
				if is_flexible {
					writer.write_compact_nullable_string(c.documentation)
				} else {
					writer.write_nullable_string(c.documentation)
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// DescribeConfigs handler

// DescribeConfigs handler function
fn (mut h Handler) handle_describe_configs(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_describe_configs_request(mut reader, version, is_flexible_version(.describe_configs,
		version))!

	h.logger.debug('Processing describe configs', observability.field_int('resources',
		req.resources.len), observability.field_bool('include_synonyms', req.include_synonyms))

	resp := h.process_describe_configs(req, version)!

	elapsed := time.since(start_time)
	h.logger.debug('Describe configs completed', observability.field_int('results', resp.results.len),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

fn (mut h Handler) process_describe_configs(req DescribeConfigsRequest, version i16) !DescribeConfigsResponse {
	_ = version
	mut results := []DescribeConfigsResult{}

	for res in req.resources {
		// resource_type: 2 = TOPIC, 4 = BROKER
		if res.resource_type == 2 {
			results << h.describe_topic_configs(res)
		} else if res.resource_type == 4 {
			results << h.describe_broker_configs(res)
		} else {
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
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}
}

// describe_topic_configs builds config entries for a single topic resource.
fn (mut h Handler) describe_topic_configs(res DescribeConfigsResource) DescribeConfigsResult {
	topic_name := res.resource_name

	topic := h.storage.get_topic(topic_name) or {
		return DescribeConfigsResult{
			error_code:    i16(ErrorCode.unknown_topic_or_partition)
			error_message: 'Topic ${topic_name} not found'
			resource_type: res.resource_type
			resource_name: topic_name
			configs:       []
		}
	}

	mut configs := []DescribeConfigsEntry{}

	if is_config_requested('retention.ms', res.config_names) {
		configs << build_config_entry('retention.ms', topic.config['retention.ms'] or {
			'604800000'
		}, 'The maximum time to retain a log before discarding it', 4)
	}

	if is_config_requested('cleanup.policy', res.config_names) {
		configs << build_config_entry('cleanup.policy', topic.config['cleanup.policy'] or {
			'delete'
		}, 'The cleanup policy for the topic', 4)
	}

	return DescribeConfigsResult{
		error_code:    0
		error_message: none
		resource_type: res.resource_type
		resource_name: topic_name
		configs:       configs
	}
}

// describe_broker_configs builds config entries for a single broker resource.
fn (h Handler) describe_broker_configs(res DescribeConfigsResource) DescribeConfigsResult {
	broker_id_str := res.resource_name

	if broker_id_str != '${h.broker_id}' {
		return DescribeConfigsResult{
			error_code:    i16(ErrorCode.resource_not_found)
			error_message: 'Broker ${broker_id_str} not found'
			resource_type: res.resource_type
			resource_name: broker_id_str
			configs:       []
		}
	}

	mut configs := []DescribeConfigsEntry{}

	if is_config_requested('log.retention.ms', res.config_names) {
		configs << build_config_entry('log.retention.ms', '604800000', 'The number of milliseconds to keep a log segment before deletion',
			4)
	}

	if is_config_requested('num.partitions', res.config_names) {
		configs << build_config_entry('num.partitions', '1', 'The default number of partitions per topic',
			4)
	}

	if is_config_requested('default.replication.factor', res.config_names) {
		configs << build_config_entry('default.replication.factor', '1', 'The default replication factor for automatically created topics',
			4)
	}

	if is_config_requested('max.message.bytes', res.config_names) {
		configs << build_config_entry('max.message.bytes', '1048576', 'The largest record batch size allowed by the broker',
			4)
	}

	if is_config_requested('log.segment.bytes', res.config_names) {
		configs << build_config_entry('log.segment.bytes', '1073741824', 'The maximum size of a single log segment file',
			4)
	}

	return DescribeConfigsResult{
		error_code:    0
		error_message: none
		resource_type: res.resource_type
		resource_name: broker_id_str
		configs:       configs
	}
}

// build_config_entry creates a DescribeConfigsEntry with common defaults.
fn build_config_entry(name string, value string, documentation string, source i8) DescribeConfigsEntry {
	return DescribeConfigsEntry{
		name:          name
		value:         value
		read_only:     false
		is_default:    true
		config_source: source
		is_sensitive:  false
		synonyms:      []
		config_type:   0
		documentation: documentation
	}
}

// is_config_requested checks whether a specific config key was requested.
fn is_config_requested(key string, names ?[]string) bool {
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
