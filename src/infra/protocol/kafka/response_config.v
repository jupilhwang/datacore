// Adapter Layer - Kafka Config Response Building
// DescribeConfigs response
module kafka

// ============================================================================
// DescribeConfigs Response (API Key 32)
// ============================================================================

pub struct DescribeConfigsResponse {
pub:
	throttle_time_ms i32
	results          []DescribeConfigsResult
}

pub struct DescribeConfigsResult {
pub:
	error_code    i16
	error_message ?string
	resource_type i8
	resource_name string
	configs       []DescribeConfigsEntry
}

pub struct DescribeConfigsEntry {
pub:
	name          string
	value         ?string
	read_only     bool
	is_default    bool // v0 (deprecated in v1+)
	config_source i8   // v1+ (replaces is_default)
	is_sensitive  bool
	synonyms      []DescribeConfigsSynonym // v1+
	config_type   i8                       // v3+
	documentation ?string                  // v3+
}

pub struct DescribeConfigsSynonym {
pub:
	name          string
	value         ?string
	config_source i8
}

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
