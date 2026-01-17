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
    auth_manager    ?port.AuthManager  // Optional: SASL authentication manager
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
        auth_manager: none
        logger: log.Log{}
    }
}

// new_handler_with_auth creates a new Kafka protocol handler with storage and authentication
pub fn new_handler_with_auth(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager port.AuthManager) Handler {
    return Handler{
        broker_id: broker_id
        host: host
        broker_port: broker_port
        cluster_id: cluster_id
        storage: storage
        auth_manager: auth_manager
        logger: log.Log{}
    }
}

// Build a minimal (empty) consumer group assignment payload
fn empty_consumer_assignment() []u8 {
    mut writer := new_writer()
    // Consumer protocol version
    writer.write_i16(0)
    // assignment array length (0 topics)
    writer.write_i32(0)
    // user data length (0)
    writer.write_i32(0)
    return writer.bytes()
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
        .sasl_handshake { h.handle_sasl_handshake(req.body, version)! }
        .sasl_authenticate { h.handle_sasl_authenticate(req.body, version)! }
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
    
    // If client requested specific topics, return only those
    // If no topics specified (null or empty), return all topics
    if req.topics.len > 0 {
        // Client requested specific topics
        for req_topic in req.topics {
            topic_name := req_topic.name or { '' }
            if topic_name.len == 0 {
                continue
            }
            
            // Try to get the topic from storage
            topic := h.storage.get_topic(topic_name) or {
                // Topic not found - return error response for this topic
                resp_topics << MetadataResponseTopic{
                    error_code: i16(ErrorCode.unknown_topic_or_partition)
                    name: topic_name
                    topic_id: []u8{len: 16}
                    is_internal: false
                    partitions: []
                }
                continue
            }
            
            // Topic found - build partition list
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
                topic_id: topic.topic_id
                is_internal: topic.is_internal
                partitions: partitions
            }
        }
    } else {
        // No specific topics requested - return all topics
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
                topic_id: topic.topic_id
                is_internal: topic.is_internal
                partitions: partitions
            }
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
    if version >= 4 {
        // v4+: return coordinators array
        mut keys := req.coordinator_keys.clone()
        if keys.len == 0 && req.key.len > 0 {
            keys << req.key
        }

        mut coordinators := []FindCoordinatorResponseNode{}
        for key in keys {
            coordinators << FindCoordinatorResponseNode{
                key: key
                node_id: h.broker_id
                host: h.host
                port: h.broker_port
                error_code: 0
                error_message: none
            }
        }

        return FindCoordinatorResponse{
            throttle_time_ms: 0
            coordinators: coordinators
        }
    }

    // v0-v3: single coordinator fields
    return FindCoordinatorResponse{
        throttle_time_ms: 0
        error_code: 0
        error_message: none
        node_id: h.broker_id
        host: h.host
        port: h.broker_port
        coordinators: []FindCoordinatorResponseNode{}
    }
}

fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
    if req.member_id.len == 0 && req.group_instance_id == none {
        return JoinGroupResponse{
            throttle_time_ms: 0
            error_code: i16(ErrorCode.member_id_required)
            generation_id: -1
            protocol_type: req.protocol_type
            protocol_name: ''
            leader: ''
            skip_assignment: false
            member_id: 'member-${h.broker_id}-1'
            members: []
        }
    }

    return JoinGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        generation_id: 1
        protocol_type: req.protocol_type
        protocol_name: if req.protocols.len > 0 { req.protocols[0].name } else { '' }
        leader: req.member_id
        skip_assignment: false
        member_id: req.member_id
        members: if req.member_id.len > 0 && req.protocols.len > 0 {
            [
                JoinGroupResponseMember{
                    member_id: req.member_id
                    group_instance_id: req.group_instance_id
                    metadata: req.protocols[0].metadata
                },
            ]
        } else {
            []
        }
    }
}

fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
    mut assignment := empty_consumer_assignment()
    for a in req.assignments {
        if a.member_id == req.member_id {
            assignment = a.assignment.clone()
            break
        }
    }
    return SyncGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        protocol_type: req.protocol_type
        protocol_name: req.protocol_name
        assignment: assignment
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
        members: []LeaveGroupResponseMember{}
    }
}

fn (mut h Handler) process_offset_commit(req OffsetCommitRequest, version i16) !OffsetCommitResponse {
    return OffsetCommitResponse{
        throttle_time_ms: 0
        topics: []
    }
}

fn (mut h Handler) process_offset_fetch(req OffsetFetchRequest, version i16) !OffsetFetchResponse {
    if version >= 8 {
        return OffsetFetchResponse{
            throttle_time_ms: 0
            topics: []
            error_code: 0
            groups: []
        }
    }
    return OffsetFetchResponse{
        throttle_time_ms: 0
        topics: []
        error_code: 0
        groups: []
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
    req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata, version))!
    
    mut resp_topics := []MetadataResponseTopic{}
    
    // If specific topics requested, return only those
    if req.topics.len > 0 {
        for req_topic in req.topics {
            // Extract topic name - skip if not provided
            if topic_name := req_topic.name {
                // Try to get topic from storage
                mut topic := h.storage.get_topic(topic_name) or {
                    // Topic not found - check if auto-create is enabled
                    if req.allow_auto_topic_creation {
                        // Auto-create topic with default config (1 partition)
                        h.storage.create_topic(topic_name, 1, domain.TopicConfig{}) or {
                            // Failed to create - return error response
                            resp_topics << MetadataResponseTopic{
                                error_code: 3  // UNKNOWN_TOPIC_OR_PARTITION
                                name: topic_name
                                topic_id: []u8{len: 16}
                                is_internal: false
                                partitions: []
                                topic_authorized_ops: -2147483648
                            }
                            continue
                        }
                        // Get the newly created topic
                        h.storage.get_topic(topic_name) or {
                            resp_topics << MetadataResponseTopic{
                                error_code: 3
                                name: topic_name
                                topic_id: []u8{len: 16}
                                is_internal: false
                                partitions: []
                                topic_authorized_ops: -2147483648
                            }
                            continue
                        }
                    } else {
                        // Auto-create disabled - return error response
                        resp_topics << MetadataResponseTopic{
                            error_code: 3  // UNKNOWN_TOPIC_OR_PARTITION
                            name: topic_name
                            topic_id: []u8{len: 16}
                            is_internal: false
                            partitions: []
                            topic_authorized_ops: -2147483648
                        }
                        continue
                    }
                    }
                
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
                    topic_authorized_ops: -2147483648
                }
            }
        }
    } else {
        // No specific topics requested - return all topics
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
                topic_authorized_ops: -2147483648
            }
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
    req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator, version))!

    mut resp := FindCoordinatorResponse{}
    if version >= 4 {
        mut keys := req.coordinator_keys.clone()
        if keys.len == 0 && req.key.len > 0 {
            keys << req.key
        }

        mut coordinators := []FindCoordinatorResponseNode{}
        for key in keys {
            coordinators << FindCoordinatorResponseNode{
                key: key
                node_id: h.broker_id
                host: h.host
                port: h.broker_port
                error_code: 0
                error_message: none
            }
        }

        resp = FindCoordinatorResponse{
            throttle_time_ms: 0
            coordinators: coordinators
        }
    } else {
        resp = FindCoordinatorResponse{
            throttle_time_ms: 0
            error_code: 0
            error_message: none
            node_id: h.broker_id
            host: h.host
            port: h.broker_port
            coordinators: []FindCoordinatorResponseNode{}
        }
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
            result := h.storage.append(t.name, int(p.index), parsed.records) or {
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
                    // Retry append after topic creation and return result
                    retry_result := h.storage.append(t.name, int(p.index), parsed.records) or {
                        partitions << ProduceResponsePartition{
                            index: p.index
                            error_code: i16(ErrorCode.unknown_server_error)
                            base_offset: -1
                            log_append_time: -1
                            log_start_offset: -1
                        }
                        continue
                    }
                    retry_result
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

fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_fetch_request(mut reader, version, is_flexible_version(.fetch, version))!
    
    eprintln('[Fetch] Request: version=${version}, topics=${req.topics.len}')
    
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
            
            // Encode records as RecordBatch (Kafka message format v2)
            records_data := encode_record_batch(result.records, p.fetch_offset)
            eprintln('[Fetch] Topic=${topic_name} partition=${p.partition} has ${result.records.len} records - returning ${records_data.len} bytes')
            
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
    
    encoded := resp.encode(version)
    eprintln('[Fetch] Response version=${version}, size=${encoded.len} bytes')
    if encoded.len > 0 && encoded.len < 200 {
        eprintln('[Fetch] First 100 bytes: ${encoded[..if encoded.len > 100 { 100 } else { encoded.len }].hex()}')
    }
    
    return encoded
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

// OffsetCommit handler - persists consumer group offsets
fn (mut h Handler) handle_offset_commit(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_offset_commit_request(mut reader, version, is_flexible_version(.offset_commit, version))!
    
    // Collect all offsets to commit
    mut all_offsets := []domain.PartitionOffset{}
    for t in req.topics {
        for p in t.partitions {
            all_offsets << domain.PartitionOffset{
                topic: t.name
                partition: int(p.partition_index)
                offset: p.committed_offset
                leader_epoch: -1
                metadata: p.committed_metadata
            }
        }
    }
    
    // Commit offsets to storage
    h.storage.commit_offsets(req.group_id, all_offsets) or {
        // If commit fails, return error for all partitions
        mut topics := []OffsetCommitResponseTopic{}
        for t in req.topics {
            mut partitions := []OffsetCommitResponsePartition{}
            for p in t.partitions {
                partitions << OffsetCommitResponsePartition{
                    partition_index: p.partition_index
                    error_code: i16(ErrorCode.unknown_server_error)
                }
            }
            topics << OffsetCommitResponseTopic{
                name: t.name
                partitions: partitions
            }
        }
        return OffsetCommitResponse{
            throttle_time_ms: 0
            topics: topics
        }.encode(version)
    }
    
    // Success - return 0 error for all partitions
    mut topics := []OffsetCommitResponseTopic{}
    for t in req.topics {
        mut partitions := []OffsetCommitResponsePartition{}
        for p in t.partitions {
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

// OffsetFetch handler - retrieves consumer group offsets
fn (mut h Handler) handle_offset_fetch(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_offset_fetch_request(mut reader, version, is_flexible_version(.offset_fetch, version))!

    if version >= 8 {
        mut groups := []OffsetFetchResponseGroup{}

        mut req_groups := req.groups.clone()
        if req_groups.len == 0 && req.group_id.len > 0 {
            // Bridge v0-7 style into v8 response
            mut gtopics := []OffsetFetchRequestGroupTopic{}
            for t in req.topics {
                gtopics << OffsetFetchRequestGroupTopic{
                    name: t.name
                    partitions: t.partitions
                }
            }
            req_groups << OffsetFetchRequestGroup{
                group_id: req.group_id
                member_id: none
                member_epoch: -1
                topics: gtopics
            }
        }

        for g in req_groups {
            // Collect all topic-partitions to query
            mut partitions_to_fetch := []domain.TopicPartition{}
            for t in g.topics {
                for p in t.partitions {
                    partitions_to_fetch << domain.TopicPartition{
                        topic: t.name
                        partition: int(p)
                    }
                }
            }

            fetched_offsets := h.storage.fetch_offsets(g.group_id, partitions_to_fetch) or {
                groups << OffsetFetchResponseGroup{
                    group_id: g.group_id
                    topics: []
                    error_code: i16(ErrorCode.unknown_server_error)
                }
                continue
            }

            mut topics_map := map[string][]OffsetFetchResponsePartition{}
            for result in fetched_offsets {
                if result.topic !in topics_map {
                    topics_map[result.topic] = []
                }
                topics_map[result.topic] << OffsetFetchResponsePartition{
                    partition_index: i32(result.partition)
                    committed_offset: result.offset
                    committed_leader_epoch: -1
                    committed_metadata: if result.metadata.len > 0 { result.metadata } else { none }
                    error_code: result.error_code
                }
            }

            mut topics := []OffsetFetchResponseGroupTopic{}
            for name, partitions in topics_map {
                topics << OffsetFetchResponseGroupTopic{
                    name: name
                    partitions: partitions
                }
            }

            groups << OffsetFetchResponseGroup{
                group_id: g.group_id
                topics: topics
                error_code: 0
            }
        }

        resp := OffsetFetchResponse{
            throttle_time_ms: 0
            topics: []
            error_code: 0
            groups: groups
        }
        return resp.encode(version)
    }

    // v0-7 behavior
    mut partitions_to_fetch := []domain.TopicPartition{}
    for t in req.topics {
        for p in t.partitions {
            partitions_to_fetch << domain.TopicPartition{
                topic: t.name
                partition: int(p)
            }
        }
    }

    fetched_offsets := h.storage.fetch_offsets(req.group_id, partitions_to_fetch) or {
        return OffsetFetchResponse{
            throttle_time_ms: 0
            topics: []
            error_code: i16(ErrorCode.unknown_server_error)
            groups: []
        }.encode(version)
    }

    mut topics_map := map[string][]OffsetFetchResponsePartition{}
    for result in fetched_offsets {
        if result.topic !in topics_map {
            topics_map[result.topic] = []
        }
        topics_map[result.topic] << OffsetFetchResponsePartition{
            partition_index: i32(result.partition)
            committed_offset: result.offset
            committed_leader_epoch: -1
            committed_metadata: if result.metadata.len > 0 { result.metadata } else { none }
            error_code: result.error_code
        }
    }

    mut topics := []OffsetFetchResponseTopic{}
    for name, partitions in topics_map {
        topics << OffsetFetchResponseTopic{
            name: name
            partitions: partitions
        }
    }

    resp := OffsetFetchResponse{
        throttle_time_ms: 0
        topics: topics
        error_code: 0
        groups: []
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

    if req.member_id.len == 0 && req.group_instance_id == none {
        resp := JoinGroupResponse{
            throttle_time_ms: 0
            error_code: i16(ErrorCode.member_id_required)
            generation_id: -1
            protocol_type: req.protocol_type
            protocol_name: ''
            leader: ''
            skip_assignment: false
            member_id: member_id
            members: []
        }
        return resp.encode(version)
    }
    
    resp := JoinGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        generation_id: 1
        protocol_type: req.protocol_type
        protocol_name: protocol_name
        leader: member_id
        skip_assignment: false
        member_id: member_id
        members: if member_id.len > 0 && req.protocols.len > 0 {
            [
                JoinGroupResponseMember{
                    member_id: member_id
                    group_instance_id: req.group_instance_id
                    metadata: req.protocols[0].metadata
                },
            ]
        } else {
            []
        }
    }
    
    return resp.encode(version)
}

// SyncGroup handler (placeholder)
fn (h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    req := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group, version))!
    
    mut assignment := empty_consumer_assignment()
    for a in req.assignments {
        if a.member_id == req.member_id {
            assignment = a.assignment.clone()
            break
        }
    }

    resp := SyncGroupResponse{
        throttle_time_ms: 0
        error_code: 0
        protocol_type: req.protocol_type
        protocol_name: req.protocol_name
        assignment: assignment
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
        members: []LeaveGroupResponseMember{}
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

// ============================================================================
// SaslHandshake Handler (API Key 17)
// ============================================================================
// Handles SASL mechanism negotiation

fn (mut h Handler) handle_sasl_handshake(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    is_flexible := is_flexible_version(.sasl_handshake, version)
    
    req := parse_sasl_handshake_request(mut reader, version, is_flexible)!
    
    // Get supported mechanisms from auth manager
    mut supported_mechanisms := []string{}
    mut error_code := i16(0)
    
    if auth_mgr := h.auth_manager {
        // Convert SaslMechanism enum to strings
        for m in auth_mgr.supported_mechanisms() {
            supported_mechanisms << m.str()
        }
        
        // Check if the requested mechanism is supported
        if !auth_mgr.is_mechanism_supported(req.mechanism) {
            error_code = i16(ErrorCode.unsupported_sasl_mechanism)
        }
    } else {
        // No auth manager configured - return PLAIN as default supported mechanism
        // This is for backward compatibility when auth is not enabled
        supported_mechanisms = ['PLAIN']
        if req.mechanism.to_upper() != 'PLAIN' {
            error_code = i16(ErrorCode.unsupported_sasl_mechanism)
        }
    }
    
    response := SaslHandshakeResponse{
        error_code: error_code
        mechanisms: supported_mechanisms
    }
    
    return response.encode(version)
}

// ============================================================================
// SaslAuthenticate Handler (API Key 36)
// ============================================================================
// Handles SASL authentication

fn (mut h Handler) handle_sasl_authenticate(body []u8, version i16) ![]u8 {
    mut reader := new_reader(body)
    is_flexible := is_flexible_version(.sasl_authenticate, version)
    
    req := parse_sasl_authenticate_request(mut reader, version, is_flexible)!
    
    // Perform authentication
    if auth_mgr := h.auth_manager {
        result := auth_mgr.authenticate(.plain, req.auth_bytes) or {
            // Authentication error
            response := SaslAuthenticateResponse{
                error_code: i16(ErrorCode.sasl_authentication_failed)
                error_message: 'Authentication failed: ${err.msg()}'
                auth_bytes: []u8{}
                session_lifetime_ms: 0
            }
            return response.encode(version)
        }
        
        if result.error_code == .none {
            // Authentication successful
            response := SaslAuthenticateResponse{
                error_code: 0
                error_message: none
                auth_bytes: result.challenge  // For SCRAM, this would be the server's challenge
                session_lifetime_ms: 0  // No session lifetime limit
            }
            return response.encode(version)
        } else {
            // Authentication failed
            response := SaslAuthenticateResponse{
                error_code: i16(result.error_code)
                error_message: result.error_message
                auth_bytes: []u8{}
                session_lifetime_ms: 0
            }
            return response.encode(version)
        }
    } else {
        // No auth manager - authentication not configured
        // Return error to indicate auth is required but not available
        response := SaslAuthenticateResponse{
            error_code: i16(ErrorCode.illegal_sasl_state)
            error_message: 'SASL authentication not configured'
            auth_bytes: []u8{}
            session_lifetime_ms: 0
        }
        return response.encode(version)
    }
}
