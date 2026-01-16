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

// Handle incoming request and return response bytes (legacy method)
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
        .init_producer_id { h.handle_init_producer_id(req.body, version)! }
        .consumer_group_heartbeat { h.handle_consumer_group_heartbeat(req.body, version)! }
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

// Handle incoming Frame and return response Frame 
// This is the recommended way to process requests
pub fn (mut h Handler) handle_frame(frame Frame) !Frame {
    // Extract request header info
    req_header := match frame.header {
        FrameRequestHeader { frame.header as FrameRequestHeader }
        else { return error('expected request header') }
    }
    
    api_key := req_header.api_key
    version := req_header.api_version
    correlation_id := req_header.correlation_id
    
    // Process body based on type and return response body
    response_body := h.process_body(frame.body, api_key, version)!
    
    // Build response frame
    return frame_response(correlation_id, api_key, version, response_body)
}

// Process body and return response body
fn (mut h Handler) process_body(body Body, api_key ApiKey, version i16) !Body {
    return match body {
        ApiVersionsRequest {
            Body(new_api_versions_response())
        }
        MetadataRequest {
            Body(h.process_metadata(body, version)!)
        }
        ProduceRequest {
            Body(h.process_produce(body, version)!)
        }
        FetchRequest {
            Body(h.process_fetch(body, version)!)
        }
        FindCoordinatorRequest {
            Body(h.process_find_coordinator(body, version)!)
        }
        JoinGroupRequest {
            Body(h.process_join_group(body, version)!)
        }
        SyncGroupRequest {
            Body(h.process_sync_group(body, version)!)
        }
        HeartbeatRequest {
            Body(h.process_heartbeat(body, version)!)
        }
        LeaveGroupRequest {
            Body(h.process_leave_group(body, version)!)
        }
        OffsetCommitRequest {
            Body(h.process_offset_commit(body, version)!)
        }
        OffsetFetchRequest {
            Body(h.process_offset_fetch(body, version)!)
        }
        ListOffsetsRequest {
            Body(h.process_list_offsets(body, version)!)
        }
        CreateTopicsRequest {
            Body(h.process_create_topics(body, version)!)
        }
        DeleteTopicsRequest {
            Body(h.process_delete_topics(body, version)!)
        }
        ListGroupsRequest {
            Body(h.process_list_groups(body, version)!)
        }
        DescribeGroupsRequest {
            Body(h.process_describe_groups(body, version)!)
        }
        InitProducerIdRequest {
            Body(h.process_init_producer_id(body, version)!)
        }
        ConsumerGroupHeartbeatRequest {
            Body(h.process_consumer_group_heartbeat(body, version)!)
        }
        else {
            return error('unsupported request type')
        }
    }
}

// Process Metadata request
fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
    mut resp_topics := []MetadataResponseTopic{}
    topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }
    
    for topic in topic_list {
        mut partitions := []MetadataResponsePartition{}
        for p in 0 .. topic.partition_count {
            partitions << MetadataResponsePartition{
                error_code: 0
                partition_index: i32(p)
                leader_id: h.broker_id
                leader_epoch: 0
                replica_nodes: [h.broker_id]
                isr_nodes: [h.broker_id]
                offline_replicas: []
            }
        }
        resp_topics << MetadataResponseTopic{
            error_code: 0
            name: topic.name
            topic_id: []u8{len: 16}  // Empty UUID for now
            is_internal: false
            partitions: partitions
        }
    }
    
    return MetadataResponse{
        throttle_time_ms: 0
        brokers: [
            MetadataResponseBroker{
                node_id: h.broker_id
                host: h.host
                port: h.broker_port
                rack: ''
            },
        ]
        cluster_id: h.cluster_id
        controller_id: h.broker_id
        topics: resp_topics
        cluster_authorized_ops: -2147483648  // Unknown
    }
}

// Process InitProducerId request
fn (mut h Handler) process_init_producer_id(req InitProducerIdRequest, version i16) !InitProducerIdResponse {
    mut producer_id := req.producer_id
    mut producer_epoch := req.producer_epoch
    
    // For new producers (producer_id == -1), generate a new producer ID
    if producer_id == -1 {
        producer_id = rand.i64()
        if producer_id < 0 {
            producer_id = -producer_id
        }
        producer_epoch = 0
    } else {
        producer_epoch += 1
    }
    
    return InitProducerIdResponse{
        throttle_time_ms: 0
        error_code: 0
        producer_id: producer_id
        producer_epoch: producer_epoch
    }
}

// Stub implementations for other process_* functions
fn (mut h Handler) process_produce(req ProduceRequest, version i16) !ProduceResponse {
    // Delegate to existing implementation via byte handling
    mut reader := new_reader([]u8{})
    is_flexible := is_flexible_version(.produce, version)
    _ = is_flexible
    _ = reader
    // For now, return empty response - full implementation in handle_produce
    return ProduceResponse{
        topics: []
        throttle_time_ms: 0
    }
}

fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
    return FetchResponse{
        throttle_time_ms: 0
        error_code: 0
        session_id: 0
        topics: []
    }
}

fn (mut h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
    return FindCoordinatorResponse{
        throttle_time_ms: 0
        error_code: 0
        node_id: h.broker_id
        host: h.host
        port: h.broker_port
    }
}

fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
    return JoinGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        generation_id: 1
        protocol_type: ''
        protocol_name: ''
        leader: req.member_id
        member_id: req.member_id
        members: []
    }
}

fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
    return SyncGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        assignment: []u8{}
    }
}

fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
    return HeartbeatResponse{
        throttle_time_ms: 0
        error_code: 0
    }
}

fn (mut h Handler) process_leave_group(req LeaveGroupRequest, version i16) !LeaveGroupResponse {
    return LeaveGroupResponse{
        throttle_time_ms: 0
        error_code: 0
    }
}

fn (mut h Handler) process_offset_commit(req OffsetCommitRequest, version i16) !OffsetCommitResponse {
    return OffsetCommitResponse{
        throttle_time_ms: 0
        topics: []
    }
}

fn (mut h Handler) process_offset_fetch(req OffsetFetchRequest, version i16) !OffsetFetchResponse {
    return OffsetFetchResponse{
        throttle_time_ms: 0
        topics: []
        error_code: 0
    }
}

fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
    return ListOffsetsResponse{
        throttle_time_ms: 0
        topics: []
    }
}

fn (mut h Handler) process_create_topics(req CreateTopicsRequest, version i16) !CreateTopicsResponse {
    mut topics := []CreateTopicsResponseTopic{}
    for topic in req.topics {
        topic_id := generate_uuid()
        topics << CreateTopicsResponseTopic{
            name: topic.name
            topic_id: topic_id
            error_code: 0
            error_message: ''
            num_partitions: topic.num_partitions
            replication_factor: topic.replication_factor
        }
    }
    return CreateTopicsResponse{
        throttle_time_ms: 0
        topics: topics
    }
}

fn (mut h Handler) process_delete_topics(req DeleteTopicsRequest, version i16) !DeleteTopicsResponse {
    return DeleteTopicsResponse{
        throttle_time_ms: 0
        topics: []
    }
}

fn (mut h Handler) process_list_groups(req ListGroupsRequest, version i16) !ListGroupsResponse {
    return ListGroupsResponse{
        throttle_time_ms: 0
        error_code: 0
        groups: []
    }
}

fn (mut h Handler) process_describe_groups(req DescribeGroupsRequest, version i16) !DescribeGroupsResponse {
    return DescribeGroupsResponse{
        throttle_time_ms: 0
        groups: []
    }
}

// ApiVersions handler
fn (h Handler) handle_api_versions(version i16) []u8 {
    resp := new_api_versions_response()
    return resp.encode(version)
}

// Metadata handler
fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    _ := parse_metadata_request(mut reader, version, is_flexible_version(.metadata, version))!
    
    // Get topics from storage
    mut resp_topics := []MetadataResponseTopic{}
    topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }
    
    for topic in topic_list {
        mut partitions := []MetadataResponsePartition{}
        for p in 0 .. topic.partition_count {
            partitions << MetadataResponsePartition{
                error_code: 0
                partition_index: p
                leader_id: h.broker_id
                leader_epoch: 0
                replica_nodes: [h.broker_id]
                isr_nodes: [h.broker_id]
                offline_replicas: []
            }
        }
        
        resp_topics << MetadataResponseTopic{
            error_code: 0
            name: topic.name
            topic_id: topic.topic_id
            is_internal: topic.is_internal
            partitions: partitions
            topic_authorized_ops: -2147483648  // Unknown
        }
    }
    
    // Build metadata response
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
        topics: resp_topics
        cluster_authorized_ops: -2147483648  // Unknown
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

// Produce handler - stores records in storage
fn (mut h Handler) handle_produce(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!
    
    mut topics := []ProduceResponseTopic{}
    for t in req.topic_data {
        mut partitions := []ProduceResponsePartition{}
        for p in t.partition_data {
            // Parse RecordBatch to extract records
            parsed := parse_record_batch(p.records) or {
                // If parsing fails, return error
                partitions << ProduceResponsePartition{
                    index: p.index
                    error_code: i16(ErrorCode.corrupt_message)
                    base_offset: -1
                    log_append_time: -1
                    log_start_offset: -1
                }
                continue
            }
            
            if parsed.records.len == 0 {
                // No records to store
                partitions << ProduceResponsePartition{
                    index: p.index
                    error_code: 0
                    base_offset: 0
                    log_append_time: -1
                    log_start_offset: 0
                }
                continue
            }
            
            // Store records in storage (with auto-create topic if not exists)
            mut result := h.storage.append(t.name, int(p.index), parsed.records) or {
                // If topic not found, auto-create it
                if err.str().contains('not found') {
                    // Auto-create topic with default config (1 partition or requested partition + 1)
                    num_partitions := if int(p.index) >= 1 { int(p.index) + 1 } else { 1 }
                    h.storage.create_topic(t.name, num_partitions, domain.TopicConfig{}) or {
                        partitions << ProduceResponsePartition{
                            index: p.index
                            error_code: i16(ErrorCode.unknown_server_error)
                            base_offset: -1
                            log_append_time: -1
                            log_start_offset: -1
                        }
                        continue
                    }
                    // Retry append after topic creation
                    h.storage.append(t.name, int(p.index), parsed.records) or {
                        partitions << ProduceResponsePartition{
                            index: p.index
                            error_code: i16(ErrorCode.unknown_server_error)
                            base_offset: -1
                            log_append_time: -1
                            log_start_offset: -1
                        }
                        continue
                    }
                } else {
                    // Other errors
                    error_code := if err.str().contains('out of range') {
                        i16(ErrorCode.unknown_topic_or_partition)
                    } else {
                        i16(ErrorCode.unknown_server_error)
                    }
                    
                    partitions << ProduceResponsePartition{
                        index: p.index
                        error_code: error_code
                        base_offset: -1
                        log_append_time: -1
                        log_start_offset: -1
                    }
                    continue
                }
            }
            
            // Success
            partitions << ProduceResponsePartition{
                index: p.index
                error_code: 0
                base_offset: result.base_offset
                log_append_time: result.log_append_time
                log_start_offset: result.log_start_offset
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

// Fetch handler - retrieves records from storage
fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_fetch_request(mut reader, version, is_flexible_version(.fetch, version))!
    
    // For v13+, topic_id is used instead of topic name
    // Note: Currently we don't persistently store topic_id mapping
    
    mut topics := []FetchResponseTopic{}
    for t in req.topics {
        // For v13+, we need to find topic name from topic_id
        mut topic_name := t.name
        mut topic_id := t.topic_id.clone()
        if version >= 13 && t.topic_id.len == 16 {
            // Look up topic by ID
            if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
                topic_name = topic_meta.name
                topic_id = topic_meta.topic_id.clone()
            }
        }
        
        mut partitions := []FetchResponsePartition{}
        for p in t.partitions {
            // Fetch records from storage
            result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
                // Handle errors
                error_code := if err.str().contains('not found') {
                    i16(ErrorCode.unknown_topic_or_partition)
                } else if err.str().contains('out of range') {
                    i16(ErrorCode.offset_out_of_range)
                } else {
                    i16(ErrorCode.unknown_server_error)
                }
                
                partitions << FetchResponsePartition{
                    partition_index: p.partition
                    error_code: error_code
                    high_watermark: 0
                    last_stable_offset: 0
                    log_start_offset: 0
                    records: []u8{}
                }
                continue
            }
            
            // Encode records as RecordBatch
            records_data := if result.records.len > 0 {
                encode_record_batch(result.records, p.fetch_offset)
            } else {
                []u8{}
            }
            
            partitions << FetchResponsePartition{
                partition_index: p.partition
                error_code: 0
                high_watermark: result.high_watermark
                last_stable_offset: result.last_stable_offset
                log_start_offset: result.log_start_offset
                records: records_data
            }
        }
        topics << FetchResponseTopic{
            name: topic_name
            topic_id: topic_id
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

// ListOffsets handler - retrieves partition offset info from storage
fn (mut h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets, version))!
    
    mut topics := []ListOffsetsResponseTopic{}
    for t in req.topics {
        mut partitions := []ListOffsetsResponsePartition{}
        for p in t.partitions {
            // Get partition info from storage
            info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
                partitions << ListOffsetsResponsePartition{
                    partition_index: p.partition_index
                    error_code: i16(ErrorCode.unknown_topic_or_partition)
                    timestamp: -1
                    offset: -1
                    leader_epoch: -1
                }
                continue
            }
            
            // Handle special timestamps:
            // -1 = latest offset (high watermark)
            // -2 = earliest offset
            offset := match p.timestamp {
                -1 { info.latest_offset }      // LATEST
                -2 { info.earliest_offset }    // EARLIEST
                else { info.latest_offset }    // For specific timestamps, return latest (simplified)
            }
            
            partitions << ListOffsetsResponsePartition{
                partition_index: p.partition_index
                error_code: 0
                timestamp: p.timestamp
                offset: offset
                leader_epoch: 0  // Leader epoch for v4+
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
        
        created_meta := h.storage.create_topic(t.name, partitions, topic_config) or {
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
        
        // Success - use topic_id from created metadata
        topics << CreateTopicsResponseTopic{
            name: t.name
            topic_id: created_meta.topic_id
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
    for t in req.topics {
        // For v6+, we may need to find topic name from topic_id
        mut topic_name := t.name
        mut topic_id := t.topic_id.clone()
        
        if version >= 6 && t.name.len == 0 && t.topic_id.len == 16 {
            // Look up topic by ID
            if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
                topic_name = topic_meta.name
                topic_id = topic_meta.topic_id.clone()
            } else {
                topics << DeleteTopicsResponseTopic{
                    name: ''
                    topic_id: t.topic_id
                    error_code: i16(ErrorCode.unknown_topic_or_partition)
                }
                continue
            }
        }
        
        // Try to delete topic from storage
        h.storage.delete_topic(topic_name) or {
            // Determine error code based on error message
            error_code := if err.str().contains('not found') {
                i16(ErrorCode.unknown_topic_or_partition)
            } else if err.str().contains('internal') {
                i16(ErrorCode.invalid_topic_exception)  // Internal topics cannot be deleted
            } else {
                i16(ErrorCode.unknown_server_error)
            }
            
            topics << DeleteTopicsResponseTopic{
                name: topic_name
                topic_id: topic_id
                error_code: error_code
            }
            continue
        }
        
        // Success
        topics << DeleteTopicsResponseTopic{
            name: topic_name
            topic_id: topic_id
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

// Handle InitProducerId (API Key 22)
// Returns a producer ID for idempotent/transactional producers
fn (mut h Handler) handle_init_producer_id(body []u8, version i16) ![]u8 {
    is_flexible := is_flexible_version(.init_producer_id, version)
    mut reader := new_reader(body)
    req := parse_init_producer_id_request(mut reader, version, is_flexible)!
    
    mut producer_id := req.producer_id
    mut producer_epoch := req.producer_epoch
    mut error_code := i16(ErrorCode.none)
    
    // For new producers (producer_id == -1), generate a new producer ID
    if producer_id == -1 {
        // Generate a unique producer ID using random number
        // In production, this should be coordinated across brokers
        producer_id = rand.i64()
        if producer_id < 0 {
            producer_id = -producer_id  // Ensure positive
        }
        producer_epoch = 0
    } else {
        // Existing producer - increment epoch
        producer_epoch += 1
    }
    
    // Note: Transactional ID handling would require additional state management
    // For now, we support idempotent producers only
    if transactional_id := req.transactional_id {
        if transactional_id.len > 0 {
            // Transactional producers require coordinator support
            // For now, return success but don't track transactions
            _ = transactional_id  // Acknowledge but don't use yet
        }
    }
    
    resp := InitProducerIdResponse{
        throttle_time_ms: 0
        error_code: error_code
        producer_id: producer_id
        producer_epoch: producer_epoch
    }
    
    return resp.encode(version)
}

// Handle ConsumerGroupHeartbeat (API Key 68) - KIP-848
// This is the new Consumer Rebalance Protocol
fn (mut h Handler) handle_consumer_group_heartbeat(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_consumer_group_heartbeat_request(mut reader, version, true)!  // Always flexible
    
    resp := h.process_consumer_group_heartbeat(req, version)!
    return resp.encode(version)
}

// Process ConsumerGroupHeartbeat request (Frame-based)
fn (mut h Handler) process_consumer_group_heartbeat(req ConsumerGroupHeartbeatRequest, version i16) !ConsumerGroupHeartbeatResponse {
    // KIP-848 ConsumerGroupHeartbeat Protocol:
    // - member_epoch == 0 with empty member_id: New member joining
    // - member_epoch == -1: Member leaving the group
    // - member_epoch > 0: Regular heartbeat
    
    mut error_code := i16(0)
    mut error_message := ?string(none)
    mut member_id := req.member_id
    mut member_epoch := req.member_epoch
    mut heartbeat_interval_ms := i32(3000)  // Default 3 seconds
    mut assignment := ?ConsumerGroupHeartbeatAssignment(none)
    
    // Validate group_id
    if req.group_id.len == 0 {
        return ConsumerGroupHeartbeatResponse{
            throttle_time_ms: 0
            error_code: i16(ErrorCode.invalid_group_id)
            error_message: 'Group ID cannot be empty'
            member_id: none
            member_epoch: -1
            heartbeat_interval_ms: 0
            assignment: none
        }
    }
    
    // Handle different member_epoch scenarios
    if req.member_epoch == 0 && req.member_id.len == 0 {
        // New member joining - generate member_id and assign epoch 1
        member_id = 'member-${h.broker_id}-${rand.i64()}'
        member_epoch = 1
        
        // Create initial assignment based on subscribed topics
        // For simplicity, we'll create an empty assignment for now
        // In a full implementation, this would involve the group coordinator
        // and assignor logic
        mut topic_partitions := []ConsumerGroupHeartbeatResponseTopicPartition{}
        
        for topic_name in req.subscribed_topic_names {
            // Look up topic and assign partitions
            topic_meta := h.storage.get_topic(topic_name) or { continue }
            mut partitions := []i32{}
            // Assign all partitions to this member (simplified single-consumer case)
            for p in 0 .. topic_meta.partition_count {
                partitions << i32(p)
            }
            
            topic_partitions << ConsumerGroupHeartbeatResponseTopicPartition{
                topic_id: topic_meta.topic_id.clone()
                partitions: partitions
            }
        }
        
        if topic_partitions.len > 0 {
            assignment = ConsumerGroupHeartbeatAssignment{
                topic_partitions: topic_partitions
            }
        }
    } else if req.member_epoch == -1 {
        // Member leaving the group
        member_epoch = -1
        // Clear assignment
        assignment = none
    } else if req.member_epoch > 0 {
        // Regular heartbeat - keep the same epoch
        // In a full implementation, we would check if there's a new assignment
        
        // For now, return the same epoch indicating no changes
        // A real implementation would track group state and potentially
        // return a new epoch with new assignments if rebalancing
    } else {
        // Invalid epoch - member_epoch < -1 is invalid
        error_code = i16(ErrorCode.unknown_member_id)
        error_message = 'Invalid member epoch'
        member_epoch = -1
    }
    
    return ConsumerGroupHeartbeatResponse{
        throttle_time_ms: 0
        error_code: error_code
        error_message: error_message
        member_id: if member_id.len > 0 { member_id } else { none }
        member_epoch: member_epoch
        heartbeat_interval_ms: heartbeat_interval_ms
        assignment: assignment
    }
}
