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
    throttle_time_ms            i32
    brokers                     []MetadataResponseBroker
    cluster_id                  ?string
    controller_id               i32
    topics                      []MetadataResponseTopic
    cluster_authorized_ops      i32  // v8-v10 only
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
                writer.write_compact_nullable_string(b.rack)
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
            writer.write_compact_nullable_string(r.cluster_id)
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
    
    // v8-v10: cluster_authorized_operations (removed in v11, moved to DescribeCluster API)
    if version >= 8 && version <= 10 {
        writer.write_i32(r.cluster_authorized_ops)
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
    topic_id    []u8  // UUID for v13+
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
        // v13+: topic_id (UUID) instead of name
        if version >= 13 {
            writer.write_uuid(t.topic_id)
        } else if is_flexible {
            writer.write_compact_string(t.name)
        } else {
            writer.write_string(t.name)
        }
        
        if is_flexible {
            writer.write_compact_array_len(t.partitions.len)
        } else {
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
            // v12+: diverging_epoch, current_leader, snapshot_id are optional tagged fields
            // When None, they are omitted entirely. We don't support them yet, so skip.
            // Aborted transactions (v4+) - empty array or null
            if version >= 4 {
                if is_flexible {
                    // COMPACT_NULLABLE_ARRAY: 0 = null, 1 = empty array (len 0)
                    writer.write_uvarint(1)  // empty array (length 0 + 1)
                } else {
                    writer.write_array_len(0)
                }
            }
            // v11+: preferred_read_replica
            if version >= 11 {
                writer.write_i32(-1)  // No preferred replica
            }
            // Records - COMPACT_NULLABLE_BYTES for flexible
            if is_flexible {
                // COMPACT_NULLABLE_BYTES: 0 = null, N+1 = N bytes
                if p.records.len == 0 {
                    writer.write_uvarint(0)  // null (no records)
                } else {
                    writer.write_uvarint(u64(p.records.len) + 1)
                    writer.write_raw(p.records)
                }
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
    // v0-v3: top-level error and single coordinator
    error_code      i16
    error_message   ?string
    node_id         i32
    host            string
    port            i32
    // v4+: coordinators array
    coordinators    []FindCoordinatorResponseNode
}

pub struct FindCoordinatorResponseNode {
pub:
    key           string
    node_id       i32
    host          string
    port          i32
    error_code    i16
    error_message ?string
}

pub fn (r FindCoordinatorResponse) encode(version i16) []u8 {
    is_flexible := version >= 3
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }

    if version <= 3 {
        writer.write_i16(r.error_code)
        if version >= 1 {
            if is_flexible {
                writer.write_compact_nullable_string(r.error_message)
            } else {
                writer.write_nullable_string(r.error_message)
            }
        }

        // v0-v3: single coordinator fields
        writer.write_i32(r.node_id)
        if is_flexible {
            writer.write_compact_string(r.host)
        } else {
            writer.write_string(r.host)
        }
        writer.write_i32(r.port)
    } else {
        // v4+: coordinators array
        if is_flexible {
            writer.write_compact_array_len(r.coordinators.len)
        } else {
            writer.write_array_len(r.coordinators.len)
        }
        for node in r.coordinators {
            if is_flexible {
                writer.write_compact_string(node.key)
            } else {
                writer.write_string(node.key)
            }
            writer.write_i32(node.node_id)
            if is_flexible {
                writer.write_compact_string(node.host)
            } else {
                writer.write_string(node.host)
            }
            writer.write_i32(node.port)
            writer.write_i16(node.error_code)
            if is_flexible {
                writer.write_compact_nullable_string(node.error_message)
            } else {
                writer.write_nullable_string(node.error_message)
            }
            if is_flexible {
                writer.write_tagged_fields()
            }
        }
    }

    if is_flexible {
        writer.write_tagged_fields()
    }
    
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
    skip_assignment bool
    member_id       string
    members         []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
    member_id         string
    group_instance_id ?string  // v5+
    metadata          []u8
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
            writer.write_compact_nullable_string(r.protocol_type)
            writer.write_compact_nullable_string(r.protocol_name)
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
        if version >= 9 {
            writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
        }
        writer.write_compact_string(r.member_id)
        writer.write_compact_array_len(r.members.len)
    } else {
        writer.write_string(r.leader)
        if version >= 9 {
            writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
        }
        writer.write_string(r.member_id)
        writer.write_array_len(r.members.len)
    }
    
    for m in r.members {
        if is_flexible {
            writer.write_compact_string(m.member_id)
            // v5+: group_instance_id
            if version >= 5 {
                writer.write_compact_nullable_string(m.group_instance_id)
            }
            writer.write_compact_bytes(m.metadata)
            writer.write_tagged_fields()
        } else {
            writer.write_string(m.member_id)
            // v5+: group_instance_id
            if version >= 5 {
                writer.write_nullable_string(m.group_instance_id)
            }
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
    protocol_type   ?string
    protocol_name   ?string
    assignment      []u8
}

pub fn (r SyncGroupResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)

    if version >= 5 {
        if is_flexible {
            writer.write_compact_nullable_string(r.protocol_type)
            writer.write_compact_nullable_string(r.protocol_name)
        } else {
            writer.write_nullable_string(r.protocol_type)
            writer.write_nullable_string(r.protocol_name)
        }
    }

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
    members         []LeaveGroupResponseMember
}

pub struct LeaveGroupResponseMember {
pub:
    member_id         string
    group_instance_id ?string
    error_code        i16
}

pub fn (r LeaveGroupResponse) encode(version i16) []u8 {
    is_flexible := version >= 4
    mut writer := new_writer()
    
    if version >= 1 {
        writer.write_i32(r.throttle_time_ms)
    }
    writer.write_i16(r.error_code)

    if version >= 3 {
        if is_flexible {
            writer.write_compact_array_len(r.members.len)
        } else {
            writer.write_array_len(r.members.len)
        }
        for m in r.members {
            if is_flexible {
                writer.write_compact_string(m.member_id)
                writer.write_compact_nullable_string(m.group_instance_id)
            } else {
                writer.write_string(m.member_id)
                writer.write_nullable_string(m.group_instance_id)
            }
            writer.write_i16(m.error_code)
            if is_flexible {
                writer.write_tagged_fields()
            }
        }
    }

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
    groups          []OffsetFetchResponseGroup
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
    committed_leader_epoch i32
    committed_metadata  ?string
    error_code          i16
}

pub struct OffsetFetchResponseGroup {
pub:
    group_id    string
    topics      []OffsetFetchResponseGroupTopic
    error_code  i16
}

pub struct OffsetFetchResponseGroupTopic {
pub:
    name        string
    partitions  []OffsetFetchResponsePartition
}

pub fn (r OffsetFetchResponse) encode(version i16) []u8 {
    is_flexible := version >= 6
    mut writer := new_writer()
    
    if version >= 3 {
        writer.write_i32(r.throttle_time_ms)
    }
    
    if version >= 8 {
        if is_flexible {
            writer.write_compact_array_len(r.groups.len)
        } else {
            writer.write_array_len(r.groups.len)
        }
        for g in r.groups {
            if is_flexible {
                writer.write_compact_string(g.group_id)
                writer.write_compact_array_len(g.topics.len)
            } else {
                writer.write_string(g.group_id)
                writer.write_array_len(g.topics.len)
            }
            for t in g.topics {
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
                    writer.write_i32(p.committed_leader_epoch)
                    if is_flexible {
                        writer.write_compact_nullable_string(p.committed_metadata)
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
            writer.write_i16(g.error_code)
            if is_flexible {
                writer.write_tagged_fields()
            }
        }
    } else {
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
                if version >= 5 {
                    writer.write_i32(p.committed_leader_epoch)
                }
                if is_flexible {
                    writer.write_compact_nullable_string(p.committed_metadata)
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
    leader_epoch    i32  // v4+
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
            // v4+: leader_epoch
            if version >= 4 {
                writer.write_i32(p.leader_epoch)
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
                writer.write_compact_nullable_string(t.error_message)
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
    topic_id    []u8  // v6+: UUID (16 bytes)
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
        // v6+: topic_id (UUID, 16 bytes)
        if version >= 6 {
            writer.write_uuid(t.topic_id)
        }
        writer.write_i16(t.error_code)
        // v5+: error_message (nullable string, we send null for now)
        if version >= 5 {
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

// ConsumerGroupHeartbeat Response (API Key 68) - KIP-848
// Response for the new Consumer Rebalance Protocol
pub struct ConsumerGroupHeartbeatResponse {
pub:
    throttle_time_ms       i32
    error_code             i16
    error_message          ?string
    member_id              ?string
    member_epoch           i32
    heartbeat_interval_ms  i32
    assignment             ?ConsumerGroupHeartbeatAssignment
}

pub struct ConsumerGroupHeartbeatAssignment {
pub:
    topic_partitions  []ConsumerGroupHeartbeatResponseTopicPartition
}

pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
    topic_id     []u8   // UUID (16 bytes)
    partitions   []i32
}

pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
    // ConsumerGroupHeartbeat is always flexible (v0+)
    mut writer := new_writer()
    
    // throttle_time_ms: INT32
    writer.write_i32(r.throttle_time_ms)
    
    // error_code: INT16
    writer.write_i16(r.error_code)
    
    // error_message: COMPACT_NULLABLE_STRING
    writer.write_compact_nullable_string(r.error_message)
    
    // member_id: COMPACT_NULLABLE_STRING
    writer.write_compact_nullable_string(r.member_id)
    
    // member_epoch: INT32
    writer.write_i32(r.member_epoch)
    
    // heartbeat_interval_ms: INT32
    writer.write_i32(r.heartbeat_interval_ms)
    
    // assignment: Assignment (nullable)
    if assignment := r.assignment {
        // Write topic_partitions array
        writer.write_compact_array_len(assignment.topic_partitions.len)
        
        for tp in assignment.topic_partitions {
            // topic_id: UUID (16 bytes)
            writer.write_uuid(tp.topic_id)
            
            // partitions: COMPACT_ARRAY[INT32]
            writer.write_compact_array_len(tp.partitions.len)
            for p in tp.partitions {
                writer.write_i32(p)
            }
            
            // Tagged fields for each topic partition
            writer.write_tagged_fields()
        }
        
        // Tagged fields for assignment
        writer.write_tagged_fields()
    } else {
        // Write -1 to indicate null assignment
        // For compact nullable structs, we use 0 to indicate null (length = 0 - 1 = -1)
        writer.write_uvarint(0)
    }
    
    // Tagged fields at the end
    writer.write_tagged_fields()
    
    return writer.bytes()
}

// ============================================================================
// SaslHandshake Response (API Key 17)
// ============================================================================
// Returns the list of SASL mechanisms supported by the broker

pub struct SaslHandshakeResponse {
pub:
    error_code  i16       // Error code (0 = no error, 33 = unsupported mechanism)
    mechanisms  []string  // List of SASL mechanisms enabled by the broker
}

pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
    // SaslHandshake is never flexible (v0-v1 only)
    mut writer := new_writer()
    
    // error_code: INT16
    writer.write_i16(r.error_code)
    
    // mechanisms: ARRAY[STRING]
    writer.write_array_len(r.mechanisms.len)
    for m in r.mechanisms {
        writer.write_string(m)
    }
    
    return writer.bytes()
}

// ============================================================================
// SaslAuthenticate Response (API Key 36)
// ============================================================================
// Returns the result of SASL authentication

pub struct SaslAuthenticateResponse {
pub:
    error_code        i16      // Error code (0 = success, 58 = SASL_AUTHENTICATION_FAILED)
    error_message     ?string  // Error message if authentication failed
    auth_bytes        []u8     // SASL authentication bytes from server (for multi-step)
    session_lifetime_ms i64    // v1+: Session lifetime in milliseconds (0 = no lifetime)
}

pub fn (r SaslAuthenticateResponse) encode(version i16) []u8 {
    is_flexible := version >= 2
    mut writer := new_writer()
    
    // error_code: INT16
    writer.write_i16(r.error_code)
    
    // error_message: NULLABLE_STRING / COMPACT_NULLABLE_STRING
    if is_flexible {
        writer.write_compact_nullable_string(r.error_message)
    } else {
        writer.write_nullable_string(r.error_message)
    }
    
    // auth_bytes: BYTES / COMPACT_BYTES
    if is_flexible {
        writer.write_compact_bytes(r.auth_bytes)
    } else {
        writer.write_bytes(r.auth_bytes)
    }
    
    // session_lifetime_ms: INT64 (v1+)
    if version >= 1 {
        writer.write_i64(r.session_lifetime_ms)
    }
    
    // Tagged fields for flexible versions
    if is_flexible {
        writer.write_tagged_fields()
    }
    
    return writer.bytes()
}
