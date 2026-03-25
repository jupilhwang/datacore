// Kafka protocol - IncrementalAlterConfigs (API Key 44)
// Request/response types, parsing, encoding, and handlers
module kafka

import service.port
import time

// IncrementalAlterConfigs (API Key 44)
// v0: non-flexible, v1+: flexible

/// IncrementalAlterConfigsRequest holds the request data for IncrementalAlterConfigs (API Key 44).
pub struct IncrementalAlterConfigsRequest {
pub:
	resources     []IncrementalAlterConfigsResource
	validate_only bool
}

/// IncrementalAlterConfigsResource holds a resource entry for IncrementalAlterConfigs.
pub struct IncrementalAlterConfigsResource {
pub:
	resource_type i8
	resource_name string
	configs       []IncrementalAlterableConfig
}

/// IncrementalAlterableConfig holds a single config alteration entry.
/// config_operation: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT
pub struct IncrementalAlterableConfig {
pub:
	name             string
	config_operation i8
	value            ?string
}

/// IncrementalAlterConfigsResponse holds the response data for IncrementalAlterConfigs.
pub struct IncrementalAlterConfigsResponse {
pub:
	throttle_time_ms i32
	responses        []IncrementalAlterConfigsResourceResponse
}

/// IncrementalAlterConfigsResourceResponse holds the result for a single resource.
pub struct IncrementalAlterConfigsResourceResponse {
pub:
	error_code    i16
	error_message ?string
	resource_type i8
	resource_name string
}

fn parse_incremental_alter_configs_request(mut reader BinaryReader, version i16, is_flexible bool) !IncrementalAlterConfigsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut resources := []IncrementalAlterConfigsResource{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := reader.read_flex_string(is_flexible)!

		config_count := reader.read_flex_array_len(is_flexible)!
		mut configs := []IncrementalAlterableConfig{}

		for _ in 0 .. config_count {
			name := reader.read_flex_string(is_flexible)!
			config_operation := reader.read_i8()!
			value := reader.read_flex_nullable_string(is_flexible)!

			reader.skip_flex_tagged_fields(is_flexible)!

			configs << IncrementalAlterableConfig{
				name:             name
				config_operation: config_operation
				value:            if value.len > 0 { value } else { none }
			}
		}

		reader.skip_flex_tagged_fields(is_flexible)!

		resources << IncrementalAlterConfigsResource{
			resource_type: resource_type
			resource_name: resource_name
			configs:       configs
		}
	}

	validate_only := reader.read_i8()! != 0

	reader.skip_flex_tagged_fields(is_flexible)!

	return IncrementalAlterConfigsRequest{
		resources:     resources
		validate_only: validate_only
	}
}

/// encode serializes the IncrementalAlterConfigsResponse into bytes.
pub fn (r IncrementalAlterConfigsResponse) encode(version i16) []u8 {
	is_flexible := version >= 1
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.responses.len)
	} else {
		writer.write_array_len(r.responses.len)
	}

	for resp in r.responses {
		writer.write_i16(resp.error_code)
		if is_flexible {
			writer.write_compact_nullable_string(resp.error_message)
		} else {
			writer.write_nullable_string(resp.error_message)
		}
		writer.write_i8(resp.resource_type)
		if is_flexible {
			writer.write_compact_string(resp.resource_name)
			writer.write_tagged_fields()
		} else {
			writer.write_string(resp.resource_name)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// handle_incremental_alter_configs handles the IncrementalAlterConfigs API (Key 44).
pub fn (mut h Handler) handle_incremental_alter_configs(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 1
	mut reader := new_reader(body)
	req := parse_incremental_alter_configs_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing incremental alter configs', port.field_int('resources',
		req.resources.len), port.field_bool('validate_only', req.validate_only))

	mut responses := []IncrementalAlterConfigsResourceResponse{}

	for res in req.resources {
		// Resource type: 2 = TOPIC, 4 = BROKER
		match res.resource_type {
			2 {
				// TOPIC config
				_ := h.storage.get_topic(res.resource_name) or {
					responses << IncrementalAlterConfigsResourceResponse{
						error_code:    i16(ErrorCode.unknown_topic_or_partition)
						error_message: 'Topic not found: ${res.resource_name}'
						resource_type: res.resource_type
						resource_name: res.resource_name
					}
					continue
				}

				// Validate config operations
				mut has_error := false
				for cfg in res.configs {
					// config_operation: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT
					if cfg.config_operation < 0 || cfg.config_operation > 3 {
						responses << IncrementalAlterConfigsResourceResponse{
							error_code:    i16(ErrorCode.invalid_config)
							error_message: 'Invalid config operation ${cfg.config_operation} for ${cfg.name}'
							resource_type: res.resource_type
							resource_name: res.resource_name
						}
						has_error = true
						break
					}
				}
				if has_error {
					continue
				}

				// Accept configs (not persisted in current implementation)
				responses << IncrementalAlterConfigsResourceResponse{
					error_code:    0
					error_message: none
					resource_type: res.resource_type
					resource_name: res.resource_name
				}
			}
			4 {
				// BROKER config - accept but not persisted
				responses << IncrementalAlterConfigsResourceResponse{
					error_code:    0
					error_message: none
					resource_type: res.resource_type
					resource_name: res.resource_name
				}
			}
			else {
				responses << IncrementalAlterConfigsResourceResponse{
					error_code:    i16(ErrorCode.invalid_request)
					error_message: 'Unsupported resource type: ${res.resource_type}'
					resource_type: res.resource_type
					resource_name: res.resource_name
				}
			}
		}
	}

	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: default_throttle_time_ms
		responses:        responses
	}

	elapsed := time.since(start_time)
	h.logger.debug('Incremental alter configs completed', port.field_int('responses',
		responses.len), port.field_duration('latency', elapsed))

	return resp.encode(version)
}
