// Adapter Layer - Kafka Response Building
module kafka

// Response Header
pub struct ResponseHeader {
pub:
    correlation_id  i32
}

// Build response with header
pub fn build_response(correlation_id i32, body []u8) []u8 {
    mut writer := new_writer_with_capacity(4 + 4 + body.len)
    
    // Size (total length excluding size field itself)
    writer.write_i32(i32(4 + body.len))
    // Correlation ID
    writer.write_i32(correlation_id)
    // Body
    writer.write_raw(body)
    
    return writer.bytes()
}

// Build flexible response (with tagged fields)
pub fn build_flexible_response(correlation_id i32, body []u8) []u8 {
    mut writer := new_writer_with_capacity(4 + 4 + 1 + body.len)
    
    // Size (total length excluding size field itself)
    writer.write_i32(i32(4 + 1 + body.len))
    // Correlation ID
    writer.write_i32(correlation_id)
    // Tagged fields (empty)
    writer.write_uvarint(0)
    // Body
    writer.write_raw(body)
    
    return writer.bytes()
}

// Response body sum type
pub type ResponseBody = ApiVersionsResponse
    | MetadataResponse
    | ProduceResponse
    | FetchResponse
    | FindCoordinatorResponse
    | JoinGroupResponse
    | SyncGroupResponse
    | HeartbeatResponse
    | LeaveGroupResponse
    | OffsetCommitResponse
    | OffsetFetchResponse
    | ListOffsetsResponse
    | CreateTopicsResponse
    | DeleteTopicsResponse
    | ListGroupsResponse
    | DescribeGroupsResponse
    | InitProducerIdResponse

// ApiVersions Response
pub struct ApiVersionsResponse {
pub:
    error_code      i16
    api_versions    []ApiVersionsResponseKey
    throttle_time_ms i32
}

pub struct ApiVersionsResponseKey {
pub:
    api_key     i16
    min_version i16
    max_version i16
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

// Create default ApiVersions response
pub fn new_api_versions_response() ApiVersionsResponse {
    supported := get_supported_api_versions()
    mut api_versions := []ApiVersionsResponseKey{}
    
    for v in supported {
        api_versions << ApiVersionsResponseKey{
            api_key: i16(v.api_key)
            min_version: v.min_version
            max_version: v.max_version
        }
    }
    
    return ApiVersionsResponse{
        error_code: 0
        api_versions: api_versions
        throttle_time_ms: 0
    }
}

// Metadata Response
pub struct MetadataResponse {
pub:
    throttle_time_ms i32
    brokers         []MetadataResponseBroker
    cluster_id      ?string
    controller_id   i32
    topics          []MetadataResponseTopic
}

pub struct MetadataResponseBroker {
pub:
    node_id i32
    host    string
    port    i32
    rack    ?string
}

pub struct MetadataResponseTopic {
pub:
    error_code          i16
    name                string
    topic_id            []u8  // UUID, 16 bytes (v10+)
    is_internal         bool
    partitions          []MetadataResponsePartition
    topic_authorized_ops i32
}

pub struct MetadataResponsePartition {
pub:
    error_code      i16
    partition_index i32
    leader_id       i32
    leader_epoch    i32
    replica_nodes   []i32
    isr_nodes       []i32
    offline_replicas []i32  // v5+
}

pub fn (r MetadataResponse) encode(version i16) []u8 {
    is_flexible := version >= 9
    mut writer := new_writer()
    
    if version >= 3 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    // Brokers
    if is_flexible {
        writer.write_compact_array_len(r.brokers.len)
    } else {
        writer.write_array_len(r.brokers.len)
    }
    
    for b in r.brokers {
        writer.write_i32(b.node_id)
        if is_flexible {
            writer.write_compact_string(b.host)
        } else {
            writer.write_string(b.host)
        }
        writer.write_i32(b.port)
        if version >= 1 {
            if is_flexible {
                writer.write_compact_string(b.rack or { '' })
            } else {
                writer.write_nullable_string(b.rack)
            }
        }
        if is_flexible {
            writer.write_tagged_fields()
        }
    }
    
    // Cluster ID
    if version >= 2 {
        if is_flexible {
            writer.write_compact_string(r.cluster_id or { '' })
        } else {
            writer.write_nullable_string(r.cluster_id)
        }
    }
    
    // Controller ID
    if version >= 1 {
        writer.write_i32(r.controller_id)
    }
    
    // Topics
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        writer.write_i16(t.error_code)
        if is_flexible {
            writer.write_compact_string(t.name)
        } else {
            writer.write_string(t.name)
        }
        
        // topic_id (UUID, 16 bytes) - v10+
        if version >= 10 {
            writer.write_uuid(t.topic_id)
        }
        
        if version >= 1 {
            writer.write_i8(if t.is_internal { i8(1) } else { i8(0) })
        }
        
        // Partitions
        if is_flexible {
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_array_len(t.partitions.len)
        }
        
        for p in t.partitions {
            writer.write_i16(p.error_code)
            writer.write_i32(p.partition_index)
            writer.write_i32(p.leader_id)
            if version >= 7 {
                writer.write_i32(p.leader_epoch)
            }
            
            // Replica nodes
            if is_flexible {
                writer.write_compact_array_len(p.replica_nodes.len)
            } else {
                writer.write_array_len(p.replica_nodes.len)
            }
            for n in p.replica_nodes {
                writer.write_i32(n)
            }
            
            // ISR nodes
            if is_flexible {
                writer.write_compact_array_len(p.isr_nodes.len)
            } else {
                writer.write_array_len(p.isr_nodes.len)
            }
            for n in p.isr_nodes {
                writer.write_i32(n)
            }
            
            // Offline replicas - v5+
            if version >= 5 {
                if is_flexible {
                    writer.write_compact_array_len(p.offline_replicas.len)
                } else {
                    writer.write_array_len(p.offline_replicas.len)
                }
                for n in p.offline_replicas {
                    writer.write_i32(n)
                }
            }
            
            if is_flexible {
                writer.write_tagged_fields()
            }
        }
        
        if version >= 8 {
            writer.write_i32(t.topic_authorized_ops)
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

// Produce Response
pub struct ProduceResponse {
pub:
    topics          []ProduceResponseTopic
    throttle_time_ms i32
}

pub struct ProduceResponseTopic {
pub:
    name        string
    partitions  []ProduceResponsePartition
}

pub struct ProduceResponsePartition {
pub:
    index           i32
    error_code      i16
    base_offset     i64
    log_append_time i64
    log_start_offset i64
}

pub fn (r ProduceResponse) encode(version i16) []u8 {
    is_flexible := version >= 9
    mut writer := new_writer()
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_string(t.name)
            writer.write_array_len(t.partitions.len)
        }
        
        for p in t.partitions {
            writer.write_i32(p.index)
            writer.write_i16(p.error_code)
            writer.write_i64(p.base_offset)
            if version >= 2 {
                writer.write_i64(p.log_append_time)
            }
            if version >= 5 {
                writer.write_i64(p.log_start_offset)
            }
            // v8+: record_errors (empty array for success)
            if version >= 8 {
                if is_flexible {
                    writer.write_compact_array_len(0)  // No record errors
                } else {
                    writer.write_array_len(0)
                }
            }
            // v8+: error_message (null for success)
            if version >= 8 {
                if is_flexible {
                    writer.write_compact_nullable_string(none)
                } else {
                    writer.write_nullable_string(none)
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
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// Fetch Response
pub struct FetchResponse {
pub:
    throttle_time_ms i32
    error_code      i16
    session_id      i32
    topics          []FetchResponseTopic
}

pub struct FetchResponseTopic {
pub:
    name        string
    partitions  []FetchResponsePartition
}

pub struct FetchResponsePartition {
pub:
    partition_index     i32
    error_code          i16
    high_watermark      i64
    last_stable_offset  i64
    log_start_offset    i64
    records             []u8
}

pub fn (r FetchResponse) encode(version i16) []u8 {
    is_flexible := version >= 12
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    if version >= 7 {
        writer.write_i16(r.error_code)
        writer.write_i32(r.session_id)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_string(t.name)
            writer.write_array_len(t.partitions.len)
        }
        
        for p in t.partitions {
            writer.write_i32(p.partition_index)
            writer.write_i16(p.error_code)
            writer.write_i64(p.high_watermark)
            if version >= 4 {
                writer.write_i64(p.last_stable_offset)
            }
            if version >= 5 {
                writer.write_i64(p.log_start_offset)
            }
            // Aborted transactions (skip for now)
            if version >= 4 {
                if is_flexible {
                    writer.write_compact_array_len(0)
                } else {
                    writer.write_array_len(0)
                }
            }
            // Records
            if is_flexible {
                writer.write_compact_bytes(p.records)
            } else {
                writer.write_bytes(p.records)
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

// FindCoordinator Response
pub struct FindCoordinatorResponse {
pub:
    throttle_time_ms i32
    error_code      i16
    error_message   ?string
    node_id         i32
    host            string
    port            i32
}

pub fn (r FindCoordinatorResponse) encode(version i16) []u8 {
    is_flexible := version >= 3
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    if version >= 1 {
        if is_flexible {
            writer.write_compact_string(r.error_message or { '' })
        } else {
            writer.write_nullable_string(r.error_message)
        }
    }
    writer.write_i32(r.node_id)
    if is_flexible {
        writer.write_compact_string(r.host)
    } else {
        writer.write_string(r.host)
    }
    writer.write_i32(r.port)
    
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// JoinGroup Response
pub struct JoinGroupResponse {
pub:
    throttle_time_ms i32
    error_code      i16
    generation_id   i32
    protocol_type   ?string
    protocol_name   ?string
    leader          string
    member_id       string
    members         []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
    member_id   string
    metadata    []u8
}

pub fn (r JoinGroupResponse) encode(version i16) []u8 {
    is_flexible := version >= 6
    mut writer := new_writer()
    
    if version >= 2 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    writer.write_i32(r.generation_id)
    
    if version >= 7 {
        if is_flexible {
            writer.write_compact_string(r.protocol_type or { '' })
            writer.write_compact_string(r.protocol_name or { '' })
        } else {
            writer.write_nullable_string(r.protocol_type)
            writer.write_nullable_string(r.protocol_name)
        }
    } else {
        if is_flexible {
            writer.write_compact_string(r.protocol_name or { '' })
        } else {
            writer.write_string(r.protocol_name or { '' })
        }
    }
    
    if is_flexible {
        writer.write_compact_string(r.leader)
        writer.write_compact_string(r.member_id)
        writer.write_compact_array_len(r.members.len)
    } else {
        writer.write_string(r.leader)
        writer.write_string(r.member_id)
        writer.write_array_len(r.members.len)
    }
    
    for m in r.members {
        if is_flexible {
            writer.write_compact_string(m.member_id)
            writer.write_compact_bytes(m.metadata)
            writer.write_tagged_fields()
        } else {
            writer.write_string(m.member_id)
            writer.write_bytes(m.metadata)
        }
    }
    
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// SyncGroup Response
pub struct SyncGroupResponse {
pub:
    throttle_time_ms i32
    error_code      i16
    assignment      []u8
}

pub fn (r SyncGroupResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    if is_flexible {
        writer.write_compact_bytes(r.assignment)
        writer.write_tagged_fields()
    } else {
        writer.write_bytes(r.assignment)
    }
    
    return writer.bytes()
}

// Heartbeat Response
pub struct HeartbeatResponse {
pub:
    throttle_time_ms i32
    error_code      i16
}

pub fn (r HeartbeatResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// LeaveGroup Response
pub struct LeaveGroupResponse {
pub:
    throttle_time_ms i32
    error_code      i16
}

pub fn (r LeaveGroupResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// OffsetCommit Response
pub struct OffsetCommitResponse {
pub:
    throttle_time_ms i32
    topics          []OffsetCommitResponseTopic
}

pub struct OffsetCommitResponseTopic {
pub:
    name        string
    partitions  []OffsetCommitResponsePartition
}

pub struct OffsetCommitResponsePartition {
pub:
    partition_index i32
    error_code      i16
}

pub fn (r OffsetCommitResponse) encode(version i16) []u8 {
    is_flexible := version >= 8
    mut writer := new_writer()
    
    if version >= 3 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_string(t.name)
            writer.write_array_len(t.partitions.len)
        }
        for p in t.partitions {
            writer.write_i32(p.partition_index)
            writer.write_i16(p.error_code)
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

// OffsetFetch Response
pub struct OffsetFetchResponse {
pub:
    throttle_time_ms i32
    topics          []OffsetFetchResponseTopic
    error_code      i16
}

pub struct OffsetFetchResponseTopic {
pub:
    name        string
    partitions  []OffsetFetchResponsePartition
}

pub struct OffsetFetchResponsePartition {
pub:
    partition_index     i32
    committed_offset    i64
    committed_metadata  ?string
    error_code          i16
}

pub fn (r OffsetFetchResponse) encode(version i16) []u8 {
    is_flexible := version >= 6
    mut writer := new_writer()
    
    if version >= 3 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_string(t.name)
            writer.write_array_len(t.partitions.len)
        }
        for p in t.partitions {
            writer.write_i32(p.partition_index)
            writer.write_i64(p.committed_offset)
            if is_flexible {
                writer.write_compact_string(p.committed_metadata or { '' })
            } else {
                writer.write_nullable_string(p.committed_metadata)
            }
            writer.write_i16(p.error_code)
            if is_flexible {
                writer.write_tagged_fields()
            }
        }
        if is_flexible {
            writer.write_tagged_fields()
        }
    }
    
    if version >= 2 {
        writer.write_i16(r.error_code)
    }
    
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// ListOffsets Response
pub struct ListOffsetsResponse {
pub:
    throttle_time_ms i32
    topics          []ListOffsetsResponseTopic
}

pub struct ListOffsetsResponseTopic {
pub:
    name        string
    partitions  []ListOffsetsResponsePartition
}

pub struct ListOffsetsResponsePartition {
pub:
    partition_index i32
    error_code      i16
    timestamp       i64
    offset          i64
}

pub fn (r ListOffsetsResponse) encode(version i16) []u8 {
    is_flexible := version >= 6
    mut writer := new_writer()
    
    if version >= 2 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
            writer.write_compact_array_len(t.partitions.len)
        } else {
            writer.write_string(t.name)
            writer.write_array_len(t.partitions.len)
        }
        for p in t.partitions {
            writer.write_i32(p.partition_index)
            writer.write_i16(p.error_code)
            if version >= 1 {
                writer.write_i64(p.timestamp)
                writer.write_i64(p.offset)
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

// CreateTopics Response
pub struct CreateTopicsResponse {
pub:
    throttle_time_ms i32
    topics          []CreateTopicsResponseTopic
}

pub struct CreateTopicsResponseTopic {
pub:
    name            string
    topic_id        []u8  // UUID, 16 bytes (v7+)
    error_code      i16
    error_message   ?string
    num_partitions  i32
    replication_factor i16
}

pub fn (r CreateTopicsResponse) encode(version i16) []u8 {
    is_flexible := version >= 5
    mut writer := new_writer()
    
    if version >= 2 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
        } else {
            writer.write_string(t.name)
        }
        
        // topic_id (UUID, 16 bytes) - v7+
        if version >= 7 {
            writer.write_uuid(t.topic_id)
        }
        
        writer.write_i16(t.error_code)
        if version >= 1 {
            if is_flexible {
                writer.write_compact_string(t.error_message or { '' })
            } else {
                writer.write_nullable_string(t.error_message)
            }
        }
        if version >= 5 {
            writer.write_i32(t.num_partitions)
            writer.write_i16(t.replication_factor)
            // configs array (empty)
            writer.write_compact_array_len(0)
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

// DeleteTopics Response
pub struct DeleteTopicsResponse {
pub:
    throttle_time_ms i32
    topics          []DeleteTopicsResponseTopic
}

pub struct DeleteTopicsResponseTopic {
pub:
    name        string
    error_code  i16
}

pub fn (r DeleteTopicsResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.topics.len)
    } else {
        writer.write_array_len(r.topics.len)
    }
    
    for t in r.topics {
        if is_flexible {
            writer.write_compact_string(t.name)
        } else {
            writer.write_string(t.name)
        }
        writer.write_i16(t.error_code)
        if is_flexible {
            writer.write_tagged_fields()
        }
    }
    
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}

// ListGroups Response
pub struct ListGroupsResponse {
pub:
    throttle_time_ms i32
    error_code      i16
    groups          []ListGroupsResponseGroup
}

pub struct ListGroupsResponseGroup {
pub:
    group_id        string
    protocol_type   string
    group_state     string
}

pub fn (r ListGroupsResponse) encode(version i16) []u8 {
    is_flexible := version >= 3
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)
    
    if is_flexible {
        writer.write_compact_array_len(r.groups.len)
    } else {
        writer.write_array_len(r.groups.len)
    }
    
    for g in r.groups {
        if is_flexible {
            writer.write_compact_string(g.group_id)
            writer.write_compact_string(g.protocol_type)
        } else {
            writer.write_string(g.group_id)
            writer.write_string(g.protocol_type)
        }
        if version >= 4 {
            if is_flexible {
                writer.write_compact_string(g.group_state)
            } else {
                writer.write_string(g.group_state)
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

// DescribeGroups Response
pub struct DescribeGroupsResponse {
pub:
    throttle_time_ms i32
    groups          []DescribeGroupsResponseGroup
}

pub struct DescribeGroupsResponseGroup {
pub:
    error_code      i16
    group_id        string
    group_state     string
    protocol_type   string
    protocol_data   string
    members         []DescribeGroupsResponseMember
}

pub struct DescribeGroupsResponseMember {
pub:
    member_id       string
    client_id       string
    client_host     string
    member_metadata []u8
    member_assignment []u8
}

pub fn (r DescribeGroupsResponse) encode(version i16) []u8 {
    is_flexible := version >= 5
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if is_flexible {
        writer.write_compact_array_len(r.groups.len)
    } else {
        writer.write_array_len(r.groups.len)
    }
    
    for g in r.groups {
        writer.write_i16(g.error_code)
        if is_flexible {
            writer.write_compact_string(g.group_id)
            writer.write_compact_string(g.group_state)
            writer.write_compact_string(g.protocol_type)
            writer.write_compact_string(g.protocol_data)
            writer.write_compact_array_len(g.members.len)
        } else {
            writer.write_string(g.group_id)
            writer.write_string(g.group_state)
            writer.write_string(g.protocol_type)
            writer.write_string(g.protocol_data)
            writer.write_array_len(g.members.len)
        }
        
        for m in g.members {
            if is_flexible {
                writer.write_compact_string(m.member_id)
                writer.write_compact_string(m.client_id)
                writer.write_compact_string(m.client_host)
                writer.write_compact_bytes(m.member_metadata)
                writer.write_compact_bytes(m.member_assignment)
                writer.write_tagged_fields()
            } else {
                writer.write_string(m.member_id)
                writer.write_string(m.client_id)
                writer.write_string(m.client_host)
                writer.write_bytes(m.member_metadata)
                writer.write_bytes(m.member_assignment)
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

// InitProducerId Response (API Key 22)
// Returns a producer ID for idempotent/transactional producers
pub struct InitProducerIdResponse {
pub:
    throttle_time_ms i32  // Throttle time in milliseconds
    error_code       i16  // Error code (0 = success)
    producer_id      i64  // Assigned producer ID
    producer_epoch   i16  // Producer epoch
}

pub fn (r InitProducerIdResponse) encode(version i16) []u8 {
    is_flexible := version >= 2
    mut writer := new_writer()
    
    // throttle_time_ms: INT32 (v0+)
    writer.write_i32(r.throttle_time_ms)
    
    // error_code: INT16 (v0+)
    writer.write_i16(r.error_code)
    
    // producer_id: INT64 (v0+)
    writer.write_i64(r.producer_id)
    
    // producer_epoch: INT16 (v0+)
    writer.write_i16(r.producer_epoch)
    
    // Tagged fields for flexible versions
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}
