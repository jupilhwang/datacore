// Kafka Protocol Frame
// Frame-based request/response processing
module kafka

// Frame represents a complete Kafka protocol message
// Format: [size: 4 bytes][header][body]
pub struct Frame {
pub:
    size   i32           // Total size excluding the size field itself
    header FrameHeader   // Request or Response header
    body   Body          // Request or Response body
}

// FrameHeader can be either a Request header or Response header
pub type FrameHeader = FrameRequestHeader | FrameResponseHeader

// Frame Request Header (supports both v0 and v2 formats)
pub struct FrameRequestHeader {
pub:
    api_key        ApiKey
    api_version    i16
    correlation_id i32
    client_id      ?string  // None means null
}

// Frame Response Header
pub struct FrameResponseHeader {
pub:
    correlation_id i32
}

// Body can be any request or response type
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

// ApiKey trait - each request type knows its API key
pub interface ApiKeyProvider {
    api_key() ApiKey
}

// Parse Frame from raw bytes (request)
pub fn frame_from_bytes(data []u8) !Frame {
    if data.len < 8 {
        return error('frame too short: need at least 8 bytes, got ${data.len}')
    }
    
    mut reader := new_reader(data)
    
    // Parse header fields
    api_key_raw := reader.read_i16()!
    api_version := reader.read_i16()!
    correlation_id := reader.read_i32()!
    
    api_key := unsafe { ApiKey(api_key_raw) }
    
    // ApiVersions is ALWAYS non-flexible in header (client doesn't know server version yet)
    is_flexible := api_key != .api_versions && is_flexible_version(api_key, api_version)
    
    // In Request Header v2 (flexible), client_id is still NULLABLE_STRING (NOT compact!)
    client_id := reader.read_nullable_string()!
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    // Parse body based on API key
    body_data := data[reader.pos..].clone()
    body := parse_body(api_key, api_version, body_data)!
    
    // Calculate size (everything except the size field itself)
    size := i32(data.len)
    
    return Frame{
        size: size
        header: FrameRequestHeader{
            api_key: api_key
            api_version: api_version
            correlation_id: correlation_id
            client_id: if client_id.len > 0 { client_id } else { none }
        }
        body: body
    }
}

// Parse body based on API key and version
fn parse_body(api_key ApiKey, version i16, data []u8) !Body {
    mut reader := new_reader(data)
    is_flexible := is_flexible_version(api_key, version)
    
    return match api_key {
        .api_versions { Body(parse_api_versions_request(mut reader, version, is_flexible)!) }
        .metadata { Body(parse_metadata_request(mut reader, version, is_flexible)!) }
        .produce { Body(parse_produce_request(mut reader, version, is_flexible)!) }
        .fetch { Body(parse_fetch_request(mut reader, version, is_flexible)!) }
        .find_coordinator { Body(parse_find_coordinator_request(mut reader, version, is_flexible)!) }
        .join_group { Body(parse_join_group_request(mut reader, version, is_flexible)!) }
        .sync_group { Body(parse_sync_group_request(mut reader, version, is_flexible)!) }
        .heartbeat { Body(parse_heartbeat_request(mut reader, version, is_flexible)!) }
        .leave_group { Body(parse_leave_group_request(mut reader, version, is_flexible)!) }
        .offset_commit { Body(parse_offset_commit_request(mut reader, version, is_flexible)!) }
        .offset_fetch { Body(parse_offset_fetch_request(mut reader, version, is_flexible)!) }
        .list_offsets { Body(parse_list_offsets_request(mut reader, version, is_flexible)!) }
        .create_topics { Body(parse_create_topics_request(mut reader, version, is_flexible)!) }
        .delete_topics { Body(parse_delete_topics_request(mut reader, version, is_flexible)!) }
        .list_groups { Body(parse_list_groups_request(mut reader, version, is_flexible)!) }
        .describe_groups { Body(parse_describe_groups_request(mut reader, version, is_flexible)!) }
        .init_producer_id { Body(parse_init_producer_id_request(mut reader, version, is_flexible)!) }
        .consumer_group_heartbeat { Body(parse_consumer_group_heartbeat_request(mut reader, version, is_flexible)!) }
        else { return error('unsupported API key: ${int(api_key)}') }
    }
}

// Build response Frame
pub fn frame_response(correlation_id i32, api_key ApiKey, version i16, body Body) Frame {
    return Frame{
        size: 0  // Will be calculated during encoding
        header: FrameResponseHeader{
            correlation_id: correlation_id
        }
        body: body
    }
}

// Encode Frame to bytes (for response)
pub fn (f Frame) response_to_bytes(api_key ApiKey, version i16) []u8 {
    // Get the body bytes
    body_bytes := encode_body(f.body, version)
    
    // ApiVersions always uses non-flexible response header
    is_flexible := api_key != .api_versions && is_flexible_version(api_key, version)
    
    if is_flexible {
        resp_bytes := build_flexible_response(f.header.correlation_id(), body_bytes)
        // Debug: log first bytes of response for troubleshooting header encoding
        eprintln('[DEBUG] Response bytes (first 16): ${resp_bytes[..if resp_bytes.len > 16 { 16 } else { resp_bytes.len }].hex()}')
        return resp_bytes
    }
    return build_response(f.header.correlation_id(), body_bytes)
}

// Get correlation_id from header
fn (h FrameHeader) correlation_id() i32 {
    return match h {
        FrameRequestHeader { h.correlation_id }
        FrameResponseHeader { h.correlation_id }
    }
}

// Encode body based on type
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
        else { []u8{} }  // Request types don't need encoding for response
    }
}

