// Infra Layer - Kafka Request Parsing - Metadata Operations
// ApiVersions, Metadata, FindCoordinator, DescribeCluster, DescribeConfigs request parsing
module kafka

// ApiVersions Request
pub struct ApiVersionsRequest {
pub:
	client_software_name    string
	client_software_version string
}

fn parse_api_versions_request(mut reader BinaryReader, version i16, is_flexible bool) !ApiVersionsRequest {
	mut req := ApiVersionsRequest{}

	if version >= 3 {
		if is_flexible {
			req = ApiVersionsRequest{
				client_software_name:    reader.read_compact_string()!
				client_software_version: reader.read_compact_string()!
			}
			reader.skip_tagged_fields()!
		}
	}

	return req
}

// Metadata Request
pub struct MetadataRequest {
pub:
	topics                         []MetadataRequestTopic
	allow_auto_topic_creation      bool
	include_cluster_authorized_ops bool
	include_topic_authorized_ops   bool
}

pub struct MetadataRequestTopic {
pub:
	topic_id []u8
	name     ?string
}

fn parse_metadata_request(mut reader BinaryReader, version i16, is_flexible bool) !MetadataRequest {
	mut topics := []MetadataRequestTopic{}

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			// v10+: Read topic_id (UUID, 16 bytes)
			mut topic_id := []u8{}
			if version >= 10 {
				topic_id = reader.read_uuid()!
			}

			mut topic := MetadataRequestTopic{}
			if is_flexible {
				name := reader.read_compact_string()!
				topic = MetadataRequestTopic{
					topic_id: topic_id
					name:     if name.len > 0 { name } else { none }
				}
			} else {
				topic = MetadataRequestTopic{
					name: reader.read_nullable_string()!
				}
			}

			if is_flexible && reader.remaining() > 0 {
				reader.skip_tagged_fields()!
			}

			topics << topic
		}
	}

	mut allow_auto_topic_creation := true
	if version >= 4 {
		allow_auto_topic_creation = reader.read_i8()! != 0
	}

	mut include_cluster_authorized_ops := false
	mut include_topic_authorized_ops := false
	// v8-v10: both fields present
	// v11+: include_cluster_authorized_operations was removed (moved to DescribeCluster API)
	if version >= 8 && version <= 10 {
		include_cluster_authorized_ops = reader.read_i8()! != 0
		include_topic_authorized_ops = reader.read_i8()! != 0
	} else if version >= 11 {
		// Only include_topic_authorized_operations remains
		include_topic_authorized_ops = reader.read_i8()! != 0
	}

	return MetadataRequest{
		topics:                         topics
		allow_auto_topic_creation:      allow_auto_topic_creation
		include_cluster_authorized_ops: include_cluster_authorized_ops
		include_topic_authorized_ops:   include_topic_authorized_ops
	}
}

// FindCoordinator Request
pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
	mut key := ''
	mut coordinator_keys := []string{}
	mut key_type := i8(0) // 0 = GROUP, 1 = TRANSACTION

	// v0-v3: 'key' is the first field
	if version <= 3 {
		key = if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
	}

	if version >= 1 {
		key_type = reader.read_i8()!
	}

	if version >= 4 {
		// v4+: CoordinatorKeys array (optional)
		keys_len := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if keys_len > 0 {
			for _ in 0 .. keys_len {
				k := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				// Note: Array of strings (primitives) does not have tagged fields per element
				coordinator_keys << k
			}
		}
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return FindCoordinatorRequest{
		key:              key
		key_type:         key_type
		coordinator_keys: coordinator_keys
	}
}

// ============================================================================
// DescribeCluster Request (API Key 48)
// ============================================================================

pub struct DescribeClusterRequest {
pub:
	include_cluster_authorized_operations bool
}

fn parse_describe_cluster_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeClusterRequest {
	// DescribeCluster is always flexible (v0+)
	include_cluster_authorized_operations := reader.read_i8()! != 0

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DescribeClusterRequest{
		include_cluster_authorized_operations: include_cluster_authorized_operations
	}
}

// ============================================================================
// DescribeConfigs Request (API Key 32)
// ============================================================================

pub struct DescribeConfigsRequest {
pub:
	resources        []DescribeConfigsResource
	include_synonyms bool
}

pub struct DescribeConfigsResource {
pub:
	resource_type i8
	resource_name string
	config_names  ?[]string
}

fn parse_describe_configs_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeConfigsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut resources := []DescribeConfigsResource{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}

		mut config_names := ?[]string(none)

		// For config_names
		n_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		if n_count >= 0 {
			mut names := []string{}
			for _ in 0 .. n_count {
				names << if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
			}
			config_names = names.clone()
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}

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

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DescribeConfigsRequest{
		resources:        resources
		include_synonyms: include_synonyms
	}
}
