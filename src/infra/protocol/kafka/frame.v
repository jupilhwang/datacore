// Kafka protocol frame
// Frame-based request/response handling
module kafka

/// Frame represents a complete Kafka protocol message.
/// Format: [size: 4 bytes][header][body]
pub struct Frame {
pub:
	size   i32
	header FrameHeader
	body   Body
}

/// FrameHeader can be either a request header or a response header.
pub type FrameHeader = FrameRequestHeader | FrameResponseHeader

/// Frame request header (supports both v0 and v2 formats)
pub struct FrameRequestHeader {
pub:
	api_key        ApiKey
	api_version    i16
	correlation_id i32
	client_id      ?string
}

/// Frame response header
pub struct FrameResponseHeader {
pub:
	correlation_id i32
}

/// Body can be any request or response type.
pub type Body = ApiVersionsRequest
	| ApiVersionsResponse
	| MetadataRequest
	| MetadataResponse
	| ProduceRequest
	| ProduceResponse
	| FetchRequest
	| FetchResponse
	| FindCoordinatorRequest
	| FindCoordinatorResponse
	| JoinGroupRequest
	| JoinGroupResponse
	| SyncGroupRequest
	| SyncGroupResponse
	| HeartbeatRequest
	| HeartbeatResponse
	| LeaveGroupRequest
	| LeaveGroupResponse
	| OffsetCommitRequest
	| OffsetCommitResponse
	| OffsetFetchRequest
	| OffsetFetchResponse
	| ListOffsetsRequest
	| ListOffsetsResponse
	| CreateTopicsRequest
	| CreateTopicsResponse
	| DeleteTopicsRequest
	| DeleteTopicsResponse
	| ListGroupsRequest
	| ListGroupsResponse
	| DescribeGroupsRequest
	| DescribeGroupsResponse
	| InitProducerIdRequest
	| InitProducerIdResponse
	| ConsumerGroupHeartbeatRequest
	| ConsumerGroupHeartbeatResponse
	| DescribeClusterRequest
	| DescribeClusterResponse
	| DescribeConfigsRequest
	| DescribeConfigsResponse

/// ApiKeyProvider interface - each request type knows its own API key.
pub interface ApiKeyProvider {
	api_key() ApiKey
}

/// frame_from_bytes parses a Frame from raw bytes (request).
pub fn frame_from_bytes(data []u8) !Frame {
	if data.len < 8 {
		return error('frame too short: need at least 8 bytes, got ${data.len}')
	}

	mut reader := new_reader(data)

	// Parse header fields
	api_key_raw := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	// Use safe conversion; fall back to metadata for unknown API keys
	api_key := api_key_from_i16(api_key_raw) or { ApiKey.metadata }

	// ApiVersions is always non-flexible in the header (client does not yet know the server version)
	is_flexible := api_key != .api_versions && is_flexible_version(api_key, api_version)

	// In request header v2 (flexible), client_id is still NULLABLE_STRING, not compact
	client_id := reader.read_nullable_string()!

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	// Parse body based on API key
	body_data := data[reader.pos..].clone()
	body := parse_body(api_key, api_version, body_data)!

	// Size covers everything except the size field itself
	size := i32(data.len)

	return Frame{
		size:   size
		header: FrameRequestHeader{
			api_key:        api_key
			api_version:    api_version
			correlation_id: correlation_id
			client_id:      if client_id.len > 0 { client_id } else { none }
		}
		body:   body
	}
}

/// parse_body parses the request body based on the API key and version.
fn parse_body(api_key ApiKey, version i16, data []u8) !Body {
	mut reader := new_reader(data)
	is_flexible := is_flexible_version(api_key, version)

	return match api_key {
		.api_versions {
			Body(parse_api_versions_request(mut reader, version, is_flexible)!)
		}
		.metadata {
			Body(parse_metadata_request(mut reader, version, is_flexible)!)
		}
		.produce {
			Body(parse_produce_request(mut reader, version, is_flexible)!)
		}
		.fetch {
			Body(parse_fetch_request(mut reader, version, is_flexible)!)
		}
		.find_coordinator {
			Body(parse_find_coordinator_request(mut reader, version, is_flexible)!)
		}
		.join_group {
			Body(parse_join_group_request(mut reader, version, is_flexible)!)
		}
		.sync_group {
			Body(parse_sync_group_request(mut reader, version, is_flexible)!)
		}
		.heartbeat {
			Body(parse_heartbeat_request(mut reader, version, is_flexible)!)
		}
		.leave_group {
			Body(parse_leave_group_request(mut reader, version, is_flexible)!)
		}
		.offset_commit {
			Body(parse_offset_commit_request(mut reader, version, is_flexible)!)
		}
		.offset_fetch {
			Body(parse_offset_fetch_request(mut reader, version, is_flexible)!)
		}
		.list_offsets {
			Body(parse_list_offsets_request(mut reader, version, is_flexible)!)
		}
		.create_topics {
			Body(parse_create_topics_request(mut reader, version, is_flexible)!)
		}
		.delete_topics {
			Body(parse_delete_topics_request(mut reader, version, is_flexible)!)
		}
		.list_groups {
			Body(parse_list_groups_request(mut reader, version, is_flexible)!)
		}
		.describe_groups {
			Body(parse_describe_groups_request(mut reader, version, is_flexible)!)
		}
		.init_producer_id {
			Body(parse_init_producer_id_request(mut reader, version, is_flexible)!)
		}
		.consumer_group_heartbeat {
			Body(parse_consumer_group_heartbeat_request(mut reader, version, is_flexible)!)
		}
		.describe_cluster {
			Body(parse_describe_cluster_request(mut reader, version, is_flexible)!)
		}
		.describe_configs {
			Body(parse_describe_configs_request(mut reader, version, is_flexible)!)
		}
		else {
			return error('unsupported API key: ${int(api_key)}')
		}
	}
}

/// frame_response creates a response Frame.
pub fn frame_response(correlation_id i32, api_key ApiKey, version i16, body Body) Frame {
	return Frame{
		size:   0
		header: FrameResponseHeader{
			correlation_id: correlation_id
		}
		body:   body
	}
}

/// response_to_bytes encodes the Frame to bytes (for responses).
pub fn (f Frame) response_to_bytes(api_key ApiKey, version i16) []u8 {
	// Obtain body bytes
	body_bytes := encode_body(f.body, version)

	// ApiVersions always uses a non-flexible response header.
	is_flexible := api_key != .api_versions && is_flexible_version(api_key, version)

	if is_flexible {
		resp_bytes := build_flexible_response(f.header.correlation_id(), body_bytes)
		return resp_bytes
	}
	return build_response(f.header.correlation_id(), body_bytes)
}

/// correlation_id returns the correlation ID from the header.
fn (h FrameHeader) correlation_id() i32 {
	return match h {
		FrameRequestHeader { h.correlation_id }
		FrameResponseHeader { h.correlation_id }
	}
}

/// encode_body encodes the body based on its type.
fn encode_body(body Body, version i16) []u8 {
	return match body {
		ApiVersionsResponse { body.encode(version) }
		MetadataResponse { body.encode(version) }
		ProduceResponse { body.encode(version) }
		FetchResponse { body.encode(version) }
		FindCoordinatorResponse { body.encode(version) }
		JoinGroupResponse { body.encode(version) }
		SyncGroupResponse { body.encode(version) }
		HeartbeatResponse { body.encode(version) }
		LeaveGroupResponse { body.encode(version) }
		OffsetCommitResponse { body.encode(version) }
		OffsetFetchResponse { body.encode(version) }
		ListOffsetsResponse { body.encode(version) }
		CreateTopicsResponse { body.encode(version) }
		DeleteTopicsResponse { body.encode(version) }
		ListGroupsResponse { body.encode(version) }
		DescribeGroupsResponse { body.encode(version) }
		InitProducerIdResponse { body.encode(version) }
		ConsumerGroupHeartbeatResponse { body.encode(version) }
		DescribeClusterResponse { body.encode(version) }
		DescribeConfigsResponse { body.encode(version) }
		else { []u8{} }
	}
}
