// Infra Layer - Kafka Protocol Handler
module kafka

import domain
import log
import rand
import service.port

// Protocol Handler - processes Kafka requests and generates responses
pub struct Handler {
    broker_id       i32
    host            string
    broker_port     i32
    cluster_id      string
mut:
    storage         port.StoragePort
    logger          log.Log
}

// new_handler creates a new Kafka protocol handler with storage
pub fn new_handler(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort) Handler {
    return Handler{
        broker_id: broker_id
        host: host
        broker_port: broker_port
        cluster_id: cluster_id
        storage: storage
        logger: log.Log{}
    }
}

// Handle incoming request and return response bytes
pub fn (mut h Handler) handle_request(data []u8) ![]u8 {
    // Parse request
    req := parse_request(data)!
    
    api_key := unsafe { ApiKey(req.header.api_key) }
    version := req.header.api_version
    correlation_id := req.header.correlation_id
    
    // h.logger.debug('Handling request: api_key=${int(api_key)}, version=${version}, correlation_id=${correlation_id}')
    
    // Handle by API key
    response_body := match api_key {
        .api_versions { h.handle_api_versions(version) }
        .metadata { h.handle_metadata(req.body, version)! }
        .find_coordinator { h.handle_find_coordinator(req.body, version)! }
        .produce { h.handle_produce(req.body, version)! }
        .fetch { h.handle_fetch(req.body, version)! }
        .list_offsets { h.handle_list_offsets(req.body, version)! }
        .offset_commit { h.handle_offset_commit(req.body, version)! }
        .offset_fetch { h.handle_offset_fetch(req.body, version)! }
        .join_group { h.handle_join_group(req.body, version)! }
        .sync_group { h.handle_sync_group(req.body, version)! }
        .heartbeat { h.handle_heartbeat(req.body, version)! }
        .leave_group { h.handle_leave_group(req.body, version)! }
        .list_groups { h.handle_list_groups(req.body, version)! }
        .describe_groups { h.handle_describe_groups(req.body, version)! }
        .create_topics { h.handle_create_topics(req.body, version)! }
        .delete_topics { h.handle_delete_topics(req.body, version)! }
        else {
            // h.logger.warn('Unsupported API key: ${int(api_key)}')
            return error('unsupported API key: ${int(api_key)}')
        }
    }
    
    // Build response with header
    // Note: ApiVersions is special - always uses non-flexible header even for v3+
    // because clients don't know server capabilities yet
    if api_key == .api_versions {
        return build_response(correlation_id, response_body)
    }
    
    is_flexible := is_flexible_version(api_key, version)
    if is_flexible {
        return build_flexible_response(correlation_id, response_body)
    }
    return build_response(correlation_id, response_body)
}

// ApiVersions handler
fn (h Handler) handle_api_versions(version i16) []u8 {
    resp := new_api_versions_response()
    return resp.encode(version)
}

// Metadata handler
fn (h Handler) handle_metadata(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_metadata_request(mut reader, version, is_flexible_version(.metadata, version))!
    
    // Build basic metadata response
    resp := MetadataResponse{
        throttle_time_ms: 0
        brokers: [
            MetadataResponseBroker{
                node_id: h.broker_id
                host: h.host
                port: h.broker_port
                rack: none
            },
        ]
        cluster_id: h.cluster_id
        controller_id: h.broker_id
        topics: []  // TODO: Get topics from storage
    }
    
    return resp.encode(version)
}

// FindCoordinator handler
fn (h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator, version))!
    
    resp := FindCoordinatorResponse{
        throttle_time_ms: 0
        error_code: 0
        error_message: none
        node_id: h.broker_id
        host: h.host
        port: h.broker_port
    }
    
    return resp.encode(version)
}

// Produce handler (placeholder)
fn (h Handler) handle_produce(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!
    
    mut topics := []ProduceResponseTopic{}
    for t in req.topic_data {
        mut partitions := []ProduceResponsePartition{}
        for p in t.partition_data {
            // TODO: Store records
            partitions << ProduceResponsePartition{
                index: p.index
                error_code: 0
                base_offset: 0
                log_append_time: -1
                log_start_offset: 0
            }
        }
        topics << ProduceResponseTopic{
            name: t.name
            partitions: partitions
        }
    }
    
    resp := ProduceResponse{
        topics: topics
        throttle_time_ms: 0
    }
    
    return resp.encode(version)
}

// Fetch handler (placeholder)
fn (h Handler) handle_fetch(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_fetch_request(mut reader, version, is_flexible_version(.fetch, version))!
    
    mut topics := []FetchResponseTopic{}
    for t in req.topics {
        mut partitions := []FetchResponsePartition{}
        for p in t.partitions {
            // TODO: Fetch records from storage
            partitions << FetchResponsePartition{
                partition_index: p.partition
                error_code: 0
                high_watermark: 0
                last_stable_offset: 0
                log_start_offset: 0
                records: []u8{}
            }
        }
        topics << FetchResponseTopic{
            name: t.name
            partitions: partitions
        }
    }
    
    resp := FetchResponse{
        throttle_time_ms: 0
        error_code: 0
        session_id: 0
        topics: topics
    }
    
    return resp.encode(version)
}

// ListOffsets handler (placeholder)
fn (h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets, version))!
    
    mut topics := []ListOffsetsResponseTopic{}
    for t in req.topics {
        mut partitions := []ListOffsetsResponsePartition{}
        for p in t.partitions {
            partitions << ListOffsetsResponsePartition{
                partition_index: p.partition_index
                error_code: 0
                timestamp: -1
                offset: 0
            }
        }
        topics << ListOffsetsResponseTopic{
            name: t.name
            partitions: partitions
        }
    }
    
    resp := ListOffsetsResponse{
        throttle_time_ms: 0
        topics: topics
    }
    
    return resp.encode(version)
}

// OffsetCommit handler (placeholder)
fn (h Handler) handle_offset_commit(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_offset_commit_request(mut reader, version, is_flexible_version(.offset_commit, version))!
    
    mut topics := []OffsetCommitResponseTopic{}
    for t in req.topics {
        mut partitions := []OffsetCommitResponsePartition{}
        for p in t.partitions {
            // TODO: Store offset
            partitions << OffsetCommitResponsePartition{
                partition_index: p.partition_index
                error_code: 0
            }
        }
        topics << OffsetCommitResponseTopic{
            name: t.name
            partitions: partitions
        }
    }
    
    resp := OffsetCommitResponse{
        throttle_time_ms: 0
        topics: topics
    }
    
    return resp.encode(version)
}

// OffsetFetch handler (placeholder)
fn (h Handler) handle_offset_fetch(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_offset_fetch_request(mut reader, version, is_flexible_version(.offset_fetch, version))!
    
    mut topics := []OffsetFetchResponseTopic{}
    for t in req.topics {
        mut partitions := []OffsetFetchResponsePartition{}
        for p in t.partitions {
            // TODO: Get offset from storage
            partitions << OffsetFetchResponsePartition{
                partition_index: p
                committed_offset: -1
                committed_metadata: none
                error_code: 0
            }
        }
        topics << OffsetFetchResponseTopic{
            name: t.name
            partitions: partitions
        }
    }
    
    resp := OffsetFetchResponse{
        throttle_time_ms: 0
        topics: topics
        error_code: 0
    }
    
    return resp.encode(version)
}

// JoinGroup handler (placeholder)
fn (h Handler) handle_join_group(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_join_group_request(mut reader, version, is_flexible_version(.join_group, version))!
    
    // TODO: Implement group coordination
    protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }
    member_id := if req.member_id.len > 0 { req.member_id } else { 'member-${h.broker_id}-1' }
    
    resp := JoinGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        generation_id: 1
        protocol_type: req.protocol_type
        protocol_name: protocol_name
        leader: member_id
        member_id: member_id
        members: []
    }
    
    return resp.encode(version)
}

// SyncGroup handler (placeholder)
fn (h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group, version))!
    
    resp := SyncGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        assignment: []u8{}
    }
    
    return resp.encode(version)
}

// Heartbeat handler (placeholder)
fn (h Handler) handle_heartbeat(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_heartbeat_request(mut reader, version, is_flexible_version(.heartbeat, version))!
    
    resp := HeartbeatResponse{
        throttle_time_ms: 0
        error_code: 0
    }
    
    return resp.encode(version)
}

// LeaveGroup handler (placeholder)
fn (h Handler) handle_leave_group(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_leave_group_request(mut reader, version, is_flexible_version(.leave_group, version))!
    
    resp := LeaveGroupResponse{
        throttle_time_ms: 0
        error_code: 0
    }
    
    return resp.encode(version)
}

// ListGroups handler (placeholder)
fn (h Handler) handle_list_groups(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_list_groups_request(mut reader, version, is_flexible_version(.list_groups, version))!
    
    resp := ListGroupsResponse{
        throttle_time_ms: 0
        error_code: 0
        groups: []  // TODO: Get groups from storage
    }
    
    return resp.encode(version)
}

// DescribeGroups handler (placeholder)
fn (h Handler) handle_describe_groups(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_describe_groups_request(mut reader, version, is_flexible_version(.describe_groups, version))!
    
    mut groups := []DescribeGroupsResponseGroup{}
    for group_id in req.groups {
        // TODO: Get group from storage
        groups << DescribeGroupsResponseGroup{
            error_code: i16(ErrorCode.group_id_not_found)
            group_id: group_id
            group_state: ''
            protocol_type: ''
            protocol_data: ''
            members: []
        }
    }
    
    resp := DescribeGroupsResponse{
        throttle_time_ms: 0
        groups: groups
    }
    
    return resp.encode(version)
}

// CreateTopics handler - creates topics in storage
fn (mut h Handler) handle_create_topics(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_create_topics_request(mut reader, version, is_flexible_version(.create_topics, version))!
    
    mut topics := []CreateTopicsResponseTopic{}
    for t in req.topics {
        // Convert config map to domain.TopicConfig
        topic_config := domain.TopicConfig{
            retention_ms: parse_config_i64(t.configs, 'retention.ms', 604800000)
            retention_bytes: parse_config_i64(t.configs, 'retention.bytes', -1)
            segment_bytes: parse_config_i64(t.configs, 'segment.bytes', 1073741824)
            cleanup_policy: t.configs['cleanup.policy'] or { 'delete' }
            min_insync_replicas: parse_config_int(t.configs, 'min.insync.replicas', 1)
            max_message_bytes: parse_config_int(t.configs, 'max.message.bytes', 1048576)
        }
        
        // Try to create topic in storage
        partitions := if t.num_partitions <= 0 { 1 } else { int(t.num_partitions) }
        
        h.storage.create_topic(t.name, partitions, topic_config) or {
            // Determine error code based on error message
            error_code := if err.str().contains('already exists') {
                i16(ErrorCode.topic_already_exists)
            } else if err.str().contains('invalid') {
                i16(ErrorCode.invalid_topic_exception)
            } else {
                i16(ErrorCode.unknown_server_error)
            }
            
            topics << CreateTopicsResponseTopic{
                name: t.name
                topic_id: generate_uuid()
                error_code: error_code
                error_message: err.str()
                num_partitions: t.num_partitions
                replication_factor: t.replication_factor
            }
            continue
        }
        
        // Success
        topics << CreateTopicsResponseTopic{
            name: t.name
            topic_id: generate_uuid()
            error_code: 0
            error_message: none
            num_partitions: t.num_partitions
            replication_factor: t.replication_factor
        }
    }
    
    resp := CreateTopicsResponse{
        throttle_time_ms: 0
        topics: topics
    }
    
    return resp.encode(version)
}

// Helper function to parse config as i64
fn parse_config_i64(configs map[string]string, key string, default_val i64) i64 {
    val := configs[key] or { return default_val }
    return val.i64()
}

// Helper function to parse config as int
fn parse_config_int(configs map[string]string, key string, default_val int) int {
    val := configs[key] or { return default_val }
    return val.int()
}

// DeleteTopics handler - deletes topics from storage
fn (mut h Handler) handle_delete_topics(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_delete_topics_request(mut reader, version, is_flexible_version(.delete_topics, version))!
    
    mut topics := []DeleteTopicsResponseTopic{}
    for name in req.topics {
        // Try to delete topic from storage
        h.storage.delete_topic(name) or {
            // Determine error code based on error message
            error_code := if err.str().contains('not found') {
                i16(ErrorCode.unknown_topic_or_partition)
            } else if err.str().contains('internal') {
                i16(ErrorCode.invalid_topic_exception)  // Internal topics cannot be deleted
            } else {
                i16(ErrorCode.unknown_server_error)
            }
            
            topics << DeleteTopicsResponseTopic{
                name: name
                error_code: error_code
            }
            continue
        }
        
        // Success
        topics << DeleteTopicsResponseTopic{
            name: name
            error_code: 0
        }
    }
    
    resp := DeleteTopicsResponse{
        throttle_time_ms: 0
        topics: topics
    }
    
    return resp.encode(version)
}

// generate_uuid generates a random UUID v4 (16 bytes)
fn generate_uuid() []u8 {
    mut uuid := []u8{len: 16}
    for i in 0 .. 16 {
        uuid[i] = u8(rand.int_in_range(0, 256) or { 0 })
    }
    // Set version (4) and variant (RFC 4122)
    uuid[6] = (uuid[6] & 0x0f) | 0x40  // Version 4
    uuid[8] = (uuid[8] & 0x3f) | 0x80  // Variant RFC 4122
    return uuid
}
