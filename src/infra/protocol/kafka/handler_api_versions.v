// Kafka Protocol - ApiVersions (API Key 18)
// Request/Response types, parsing, encoding, and handlers
module kafka

// ============================================================================
// ApiVersions (API Key 18)
// ============================================================================

pub struct ApiVersionsRequest {
pub:
	client_software_name    string
	client_software_version string
}

pub struct ApiVersionsResponse {
pub:
	error_code               i16
	api_versions             []ApiVersionsResponseKey
	throttle_time_ms         i32
	supported_features       []ApiVersionsSupportedFeature
	finalized_features_epoch i64
	finalized_features       []ApiVersionsFinalizedFeature
	zk_migration_ready       bool
}

pub struct ApiVersionsResponseKey {
pub:
	api_key     i16
	min_version i16
	max_version i16
}

pub struct ApiVersionsSupportedFeature {
pub:
	name        string
	min_version i16
	max_version i16
}

pub struct ApiVersionsFinalizedFeature {
pub:
	name              string
	max_version_level i16
	min_version_level i16
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

pub fn (r ApiVersionsResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_array_len(r.api_versions.len)
	} else {
		writer.write_array_len(r.api_versions.len)
	}

	for v in r.api_versions {
		writer.write_i16(v.api_key)
		writer.write_i16(v.min_version)
		writer.write_i16(v.max_version)
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

pub fn new_api_versions_response() ApiVersionsResponse {
	supported := get_supported_api_versions()
	mut api_versions := []ApiVersionsResponseKey{}

	for v in supported {
		api_versions << ApiVersionsResponseKey{
			api_key:     i16(v.api_key)
			min_version: v.min_version
			max_version: v.max_version
		}
	}

	return ApiVersionsResponse{
		error_code:               0
		api_versions:             api_versions
		throttle_time_ms:         0
		supported_features:       []
		finalized_features_epoch: -1
		finalized_features:       []
		zk_migration_ready:       false
	}
}

fn (h Handler) handle_api_versions(version i16) []u8 {
	safe_version := if version > 3 { i16(3) } else { version }
	resp := new_api_versions_response()
	return resp.encode(safe_version)
}
