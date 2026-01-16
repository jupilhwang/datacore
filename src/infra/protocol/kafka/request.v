// Adapter Layer - Kafka Request Parsing
module kafka

// Request Header v0 (used for most requests)
pub struct RequestHeader {
pub:
    api_key         i16
    api_version     i16
    correlation_id  i32
    client_id       string  // Empty string means null/no client id
}

// Request Header v2 (flexible versions, Kafka 2.4+)
pub struct RequestHeaderV2 {
pub:
    api_key         i16
    api_version     i16
    correlation_id  i32
    client_id       string
    tagged_fields   []TaggedField
}

pub struct TaggedField {
pub:
    tag     u64
    data    []u8
}

// Generic Request wrapper
pub struct Request {
pub:
    header  RequestHeader
    body    []u8
}

// Parse request from raw bytes
// Format: [size: 4 bytes][header][body]
pub fn parse_request(data []u8) !Request {
    if data.len < 8 {
        return error('request too short')
    }
    
    mut reader := new_reader(data)
    
    // Parse header
    api_key := reader.read_i16()!
    api_version := reader.read_i16()!
    correlation_id := reader.read_i32()!
    
    // Check if this is a flexible version (v2 header)
    // Note: ApiVersions is ALWAYS non-flexible in header (client doesn't know server version yet)
    api_key_enum := unsafe { ApiKey(api_key) }
    is_flexible := api_key_enum != .api_versions && is_flexible_version(api_key_enum, api_version)
    
    // In Request Header v2 (flexible), client_id is still NULLABLE_STRING (NOT compact!)
    // Only the tagged_fields at the end are compact-encoded
    client_id := reader.read_nullable_string()!
    
    if is_flexible {
        // Skip tagged fields in header
        reader.skip_tagged_fields()!
    }
    
    // Remaining data is the request body
    body := data[reader.pos..].clone()
    
    return Request{
        header: RequestHeader{
            api_key: api_key
            api_version: api_version
            correlation_id: correlation_id
            client_id: client_id
        }
        body: body
    }
}

// Check if API version uses flexible encoding
pub fn is_flexible_version(api_key ApiKey, version i16) bool {
    // Flexible versions were introduced in Kafka 2.4
    // Each API has its own threshold
    return match api_key {
        .produce { version >= 9 }
        .fetch { version >= 12 }
        .list_offsets { version >= 6 }
        .metadata { version >= 9 }
        .offset_commit { version >= 8 }
        .offset_fetch { version >= 6 }
        .find_coordinator { version >= 3 }
        .join_group { version >= 6 }
        .heartbeat { version >= 4 }
        .leave_group { version >= 4 }
        .sync_group { version >= 4 }
        .describe_groups { version >= 5 }
        .list_groups { version >= 3 }
        .api_versions { version >= 3 }
        .create_topics { version >= 5 }
        .delete_topics { version >= 4 }
        .delete_records { version >= 2 }
        .describe_configs { version >= 4 }
        .alter_configs { version >= 2 }
        .create_partitions { version >= 2 }
        .delete_groups { version >= 2 }
        .describe_cluster { version >= 0 }
        .consumer_group_heartbeat { version >= 0 }
        .consumer_group_describe { version >= 0 }
        .share_group_heartbeat { version >= 0 }
        .share_group_describe { version >= 0 }
        .share_fetch { version >= 0 }
        .share_acknowledge { version >= 0 }
        else { false }
    }
}

// Parse request by API key
pub fn parse_request_body(api_key ApiKey, version i16, body []u8) !RequestBody {
    mut reader := new_reader(body)
    is_flexible := is_flexible_version(api_key, version)
    
    return match api_key {
        .api_versions { RequestBody(parse_api_versions_request(mut reader, version, is_flexible)!) }
        .metadata { RequestBody(parse_metadata_request(mut reader, version, is_flexible)!) }
        .produce { RequestBody(parse_produce_request(mut reader, version, is_flexible)!) }
        .fetch { RequestBody(parse_fetch_request(mut reader, version, is_flexible)!) }
        .find_coordinator { RequestBody(parse_find_coordinator_request(mut reader, version, is_flexible)!) }
        .join_group { RequestBody(parse_join_group_request(mut reader, version, is_flexible)!) }
        .sync_group { RequestBody(parse_sync_group_request(mut reader, version, is_flexible)!) }
        .heartbeat { RequestBody(parse_heartbeat_request(mut reader, version, is_flexible)!) }
        .leave_group { RequestBody(parse_leave_group_request(mut reader, version, is_flexible)!) }
        .offset_commit { RequestBody(parse_offset_commit_request(mut reader, version, is_flexible)!) }
        .offset_fetch { RequestBody(parse_offset_fetch_request(mut reader, version, is_flexible)!) }
        .list_offsets { RequestBody(parse_list_offsets_request(mut reader, version, is_flexible)!) }
        .create_topics { RequestBody(parse_create_topics_request(mut reader, version, is_flexible)!) }
        .delete_topics { RequestBody(parse_delete_topics_request(mut reader, version, is_flexible)!) }
        .list_groups { RequestBody(parse_list_groups_request(mut reader, version, is_flexible)!) }
        .describe_groups { RequestBody(parse_describe_groups_request(mut reader, version, is_flexible)!) }
        else { return error('unsupported API key: ${int(api_key)}') }
    }
}

// Request body sum type
pub type RequestBody = ApiVersionsRequest
    | MetadataRequest
    | ProduceRequest
    | FetchRequest
    | FindCoordinatorRequest
    | JoinGroupRequest
    | SyncGroupRequest
    | HeartbeatRequest
    | LeaveGroupRequest
    | OffsetCommitRequest
    | OffsetFetchRequest
    | ListOffsetsRequest
    | CreateTopicsRequest
    | DeleteTopicsRequest
    | ListGroupsRequest
    | DescribeGroupsRequest

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
                client_software_name: reader.read_compact_string()!
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
    topics                          []MetadataRequestTopic
    allow_auto_topic_creation       bool
    include_cluster_authorized_ops  bool
    include_topic_authorized_ops    bool
}

pub struct MetadataRequestTopic {
pub:
    topic_id    []u8
    name        ?string
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
                    name: if name.len > 0 { name } else { none }
                }
            } else {
                topic = MetadataRequestTopic{
                    name: reader.read_nullable_string()!
                }
            }
            
            if is_flexible {
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
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    return MetadataRequest{
        topics: topics
        allow_auto_topic_creation: allow_auto_topic_creation
        include_cluster_authorized_ops: include_cluster_authorized_ops
        include_topic_authorized_ops: include_topic_authorized_ops
    }
}

// Produce Request (simplified)
pub struct ProduceRequest {
pub:
    transactional_id    ?string
    acks                i16
    timeout_ms          i32
    topic_data          []ProduceRequestTopic
}

pub struct ProduceRequestTopic {
pub:
    name            string
    partition_data  []ProduceRequestPartition
}

pub struct ProduceRequestPartition {
pub:
    index       i32
    records     []u8  // RecordBatch or MessageSet
}

fn parse_produce_request(mut reader BinaryReader, version i16, is_flexible bool) !ProduceRequest {
    mut transactional_id := ?string(none)
    if version >= 3 {
        if is_flexible {
            str := reader.read_compact_string()!
            transactional_id = if str.len > 0 { str } else { none }
        } else {
            str := reader.read_nullable_string()!
            transactional_id = if str.len > 0 { str } else { none }
        }
    }
    
    acks := reader.read_i16()!
    timeout_ms := reader.read_i32()!
    
    topic_count := if is_flexible {
        reader.read_compact_array_len()!
    } else {
        reader.read_array_len()!
    }
    
    mut topic_data := []ProduceRequestTopic{}
    for _ in 0 .. topic_count {
        name := if is_flexible {
            reader.read_compact_string()!
        } else {
            reader.read_string()!
        }
        
        partition_count := if is_flexible {
            reader.read_compact_array_len()!
        } else {
            reader.read_array_len()!
        }
        
        mut partition_data := []ProduceRequestPartition{}
        for _ in 0 .. partition_count {
            index := reader.read_i32()!
            records := if is_flexible {
                reader.read_compact_bytes()!
            } else {
                reader.read_bytes()!
            }
            
            partition_data << ProduceRequestPartition{
                index: index
                records: records
            }
            
            if is_flexible {
                reader.skip_tagged_fields()!
            }
        }
        
        topic_data << ProduceRequestTopic{
            name: name
            partition_data: partition_data
        }
        
        if is_flexible {
            reader.skip_tagged_fields()!
        }
    }
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    return ProduceRequest{
        transactional_id: transactional_id
        acks: acks
        timeout_ms: timeout_ms
        topic_data: topic_data
    }
}

// Fetch Request (simplified)
pub struct FetchRequest {
pub:
    replica_id      i32
    max_wait_ms     i32
    min_bytes       i32
    max_bytes       i32
    isolation_level i8
    topics          []FetchRequestTopic
}

pub struct FetchRequestTopic {
pub:
    name        string
    partitions  []FetchRequestPartition
}

pub struct FetchRequestPartition {
pub:
    partition           i32
    fetch_offset        i64
    partition_max_bytes i32
}

fn parse_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequest {
    replica_id := reader.read_i32()!
    max_wait_ms := reader.read_i32()!
    min_bytes := reader.read_i32()!
    
    mut max_bytes := i32(0x7fffffff)
    if version >= 3 {
        max_bytes = reader.read_i32()!
    }
    
    mut isolation_level := i8(0)
    if version >= 4 {
        isolation_level = reader.read_i8()!
    }
    
    // Skip session_id and session_epoch for version >= 7
    if version >= 7 {
        _ = reader.read_i32()!  // session_id
        _ = reader.read_i32()!  // session_epoch
    }
    
    topic_count := if is_flexible {
        reader.read_compact_array_len()!
    } else {
        reader.read_array_len()!
    }
    
    mut topics := []FetchRequestTopic{}
    for _ in 0 .. topic_count {
        name := if is_flexible {
            reader.read_compact_string()!
        } else {
            reader.read_string()!
        }
        
        partition_count := if is_flexible {
            reader.read_compact_array_len()!
        } else {
            reader.read_array_len()!
        }
        
        mut partitions := []FetchRequestPartition{}
        for _ in 0 .. partition_count {
            partition := reader.read_i32()!
            
            // current_leader_epoch for version >= 9
            if version >= 9 {
                _ = reader.read_i32()!
            }
            
            fetch_offset := reader.read_i64()!
            
            // last_fetched_epoch for version >= 12
            if version >= 12 {
                _ = reader.read_i32()!
            }
            
            // log_start_offset for version >= 5
            if version >= 5 {
                _ = reader.read_i64()!
            }
            
            partition_max_bytes := reader.read_i32()!
            
            partitions << FetchRequestPartition{
                partition: partition
                fetch_offset: fetch_offset
                partition_max_bytes: partition_max_bytes
            }
            
            if is_flexible {
                reader.skip_tagged_fields()!
            }
        }
        
        topics << FetchRequestTopic{
            name: name
            partitions: partitions
        }
        
        if is_flexible {
            reader.skip_tagged_fields()!
        }
    }
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    return FetchRequest{
        replica_id: replica_id
        max_wait_ms: max_wait_ms
        min_bytes: min_bytes
        max_bytes: max_bytes
        isolation_level: isolation_level
        topics: topics
    }
}

// FindCoordinator Request
pub struct FindCoordinatorRequest {
pub:
    key         string
    key_type    i8
}

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
    key := if is_flexible {
        reader.read_compact_string()!
    } else {
        reader.read_string()!
    }
    
    mut key_type := i8(0)  // 0 = GROUP, 1 = TRANSACTION
    if version >= 1 {
        key_type = reader.read_i8()!
    }
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    return FindCoordinatorRequest{
        key: key
        key_type: key_type
    }
}

// JoinGroup Request
pub struct JoinGroupRequest {
pub:
    group_id            string
    session_timeout_ms  i32
    rebalance_timeout_ms i32
    member_id           string
    group_instance_id   ?string
    protocol_type       string
    protocols           []JoinGroupRequestProtocol
}

pub struct JoinGroupRequestProtocol {
pub:
    name        string
    metadata    []u8
}

fn parse_join_group_request(mut reader BinaryReader, version i16, is_flexible bool) !JoinGroupRequest {
    group_id := if is_flexible {
        reader.read_compact_string()!
    } else {
        reader.read_string()!
    }
    
    session_timeout_ms := reader.read_i32()!
    
    mut rebalance_timeout_ms := session_timeout_ms
    if version >= 1 {
        rebalance_timeout_ms = reader.read_i32()!
    }
    
    member_id := if is_flexible {
        reader.read_compact_string()!
    } else {
        reader.read_string()!
    }
    
    mut group_instance_id := ?string(none)
    if version >= 5 {
        if is_flexible {
            str := reader.read_compact_string()!
            group_instance_id = if str.len > 0 { str } else { none }
        } else {
            str := reader.read_nullable_string()!
            group_instance_id = if str.len > 0 { str } else { none }
        }
    }
    
    protocol_type := if is_flexible {
        reader.read_compact_string()!
    } else {
        reader.read_string()!
    }
    
    protocol_count := if is_flexible {
        reader.read_compact_array_len()!
    } else {
        reader.read_array_len()!
    }
    
    mut protocols := []JoinGroupRequestProtocol{}
    for _ in 0 .. protocol_count {
        name := if is_flexible {
            reader.read_compact_string()!
        } else {
            reader.read_string()!
        }
        metadata := if is_flexible {
            reader.read_compact_bytes()!
        } else {
            reader.read_bytes()!
        }
        
        protocols << JoinGroupRequestProtocol{
            name: name
            metadata: metadata
        }
        
        if is_flexible {
            reader.skip_tagged_fields()!
        }
    }
    
    if is_flexible {
        reader.skip_tagged_fields()!
    }
    
    return JoinGroupRequest{
        group_id: group_id
        session_timeout_ms: session_timeout_ms
        rebalance_timeout_ms: rebalance_timeout_ms
        member_id: member_id
        group_instance_id: group_instance_id
        protocol_type: protocol_type
        protocols: protocols
    }
}

// Placeholder structs for other requests
pub struct SyncGroupRequest {
pub:
    group_id        string
    generation_id   i32
    member_id       string
    assignments     []SyncGroupRequestAssignment
}

pub struct SyncGroupRequestAssignment {
pub:
    member_id   string
    assignment  []u8
}

fn parse_sync_group_request(mut reader BinaryReader, version i16, is_flexible bool) !SyncGroupRequest {
    group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    generation_id := reader.read_i32()!
    member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    
    if version >= 3 && is_flexible {
        _ = reader.read_compact_string()!  // group_instance_id
    }
    
    if version >= 5 {
        _ = reader.read_compact_string()!  // protocol_type
        _ = reader.read_compact_string()!  // protocol_name
    }
    
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut assignments := []SyncGroupRequestAssignment{}
    for _ in 0 .. count {
        mid := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        assignment := if is_flexible { reader.read_compact_bytes()! } else { reader.read_bytes()! }
        assignments << SyncGroupRequestAssignment{ member_id: mid, assignment: assignment }
        if is_flexible { reader.skip_tagged_fields()! }
    }
    
    if is_flexible { reader.skip_tagged_fields()! }
    return SyncGroupRequest{ group_id: group_id, generation_id: generation_id, member_id: member_id, assignments: assignments }
}

pub struct HeartbeatRequest {
pub:
    group_id        string
    generation_id   i32
    member_id       string
}

fn parse_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !HeartbeatRequest {
    group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    generation_id := reader.read_i32()!
    member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    if is_flexible { reader.skip_tagged_fields()! }
    return HeartbeatRequest{ group_id: group_id, generation_id: generation_id, member_id: member_id }
}

pub struct LeaveGroupRequest {
pub:
    group_id    string
    member_id   string
}

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
    group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    if is_flexible { reader.skip_tagged_fields()! }
    return LeaveGroupRequest{ group_id: group_id, member_id: member_id }
}

pub struct OffsetCommitRequest {
pub:
    group_id    string
    topics      []OffsetCommitRequestTopic
}

pub struct OffsetCommitRequestTopic {
pub:
    name        string
    partitions  []OffsetCommitRequestPartition
}

pub struct OffsetCommitRequestPartition {
pub:
    partition_index i32
    committed_offset i64
    committed_metadata string
}

fn parse_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetCommitRequest {
    group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    // Skip generation_id, member_id for older versions
    if version >= 1 { _ = reader.read_i32()! }
    if version >= 1 { _ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! } }
    if version >= 7 { _ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! } }
    
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut topics := []OffsetCommitRequestTopic{}
    for _ in 0 .. count {
        name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        pcount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
        mut partitions := []OffsetCommitRequestPartition{}
        for _ in 0 .. pcount {
            pi := reader.read_i32()!
            co := reader.read_i64()!
            if version >= 6 { _ = reader.read_i32()! }  // committed_leader_epoch
            cm := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
            partitions << OffsetCommitRequestPartition{ partition_index: pi, committed_offset: co, committed_metadata: cm }
            if is_flexible { reader.skip_tagged_fields()! }
        }
        topics << OffsetCommitRequestTopic{ name: name, partitions: partitions }
        if is_flexible { reader.skip_tagged_fields()! }
    }
    if is_flexible { reader.skip_tagged_fields()! }
    return OffsetCommitRequest{ group_id: group_id, topics: topics }
}

pub struct OffsetFetchRequest {
pub:
    group_id    string
    topics      []OffsetFetchRequestTopic
}

pub struct OffsetFetchRequestTopic {
pub:
    name        string
    partitions  []i32
}

fn parse_offset_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetFetchRequest {
    group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut topics := []OffsetFetchRequestTopic{}
    if count >= 0 {
        for _ in 0 .. count {
            name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
            pcount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
            mut partitions := []i32{}
            for _ in 0 .. pcount {
                partitions << reader.read_i32()!
            }
            topics << OffsetFetchRequestTopic{ name: name, partitions: partitions }
            if is_flexible { reader.skip_tagged_fields()! }
        }
    }
    if is_flexible { reader.skip_tagged_fields()! }
    return OffsetFetchRequest{ group_id: group_id, topics: topics }
}

pub struct ListOffsetsRequest {
pub:
    replica_id      i32
    isolation_level i8
    topics          []ListOffsetsRequestTopic
}

pub struct ListOffsetsRequestTopic {
pub:
    name        string
    partitions  []ListOffsetsRequestPartition
}

pub struct ListOffsetsRequestPartition {
pub:
    partition_index i32
    timestamp       i64
}

fn parse_list_offsets_request(mut reader BinaryReader, version i16, is_flexible bool) !ListOffsetsRequest {
    replica_id := reader.read_i32()!
    mut isolation_level := i8(0)
    if version >= 2 { isolation_level = reader.read_i8()! }
    
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut topics := []ListOffsetsRequestTopic{}
    for _ in 0 .. count {
        name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        pcount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
        mut partitions := []ListOffsetsRequestPartition{}
        for _ in 0 .. pcount {
            pi := reader.read_i32()!
            if version >= 4 { _ = reader.read_i32()! }  // current_leader_epoch
            ts := reader.read_i64()!
            partitions << ListOffsetsRequestPartition{ partition_index: pi, timestamp: ts }
            if is_flexible { reader.skip_tagged_fields()! }
        }
        topics << ListOffsetsRequestTopic{ name: name, partitions: partitions }
        if is_flexible { reader.skip_tagged_fields()! }
    }
    if is_flexible { reader.skip_tagged_fields()! }
    return ListOffsetsRequest{ replica_id: replica_id, isolation_level: isolation_level, topics: topics }
}

pub struct CreateTopicsRequest {
pub:
    topics      []CreateTopicsRequestTopic
    timeout_ms  i32
}

pub struct CreateTopicsRequestTopic {
pub:
    name            string
    num_partitions  i32
    replication_factor i16
    configs         map[string]string
}

fn parse_create_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !CreateTopicsRequest {
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut topics := []CreateTopicsRequestTopic{}
    for _ in 0 .. count {
        name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        num_partitions := reader.read_i32()!
        replication_factor := reader.read_i16()!
        
        // Skip replica assignments
        acount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
        for _ in 0 .. acount {
            _ = reader.read_i32()!  // partition
            rcount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
            for _ in 0 .. rcount { _ = reader.read_i32()! }
            if is_flexible { reader.skip_tagged_fields()! }
        }
        
        // Parse configs
        ccount := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
        mut configs := map[string]string{}
        for _ in 0 .. ccount {
            cname := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
            cvalue := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
            configs[cname] = cvalue
            if is_flexible { reader.skip_tagged_fields()! }
        }
        
        topics << CreateTopicsRequestTopic{ name: name, num_partitions: num_partitions, replication_factor: replication_factor, configs: configs }
        if is_flexible { reader.skip_tagged_fields()! }
    }
    timeout_ms := reader.read_i32()!
    if is_flexible { reader.skip_tagged_fields()! }
    return CreateTopicsRequest{ topics: topics, timeout_ms: timeout_ms }
}

pub struct DeleteTopicsRequest {
pub:
    topics      []string
    timeout_ms  i32
}

fn parse_delete_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteTopicsRequest {
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut topics := []string{}
    for _ in 0 .. count {
        topics << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        if is_flexible && version >= 6 { reader.skip_tagged_fields()! }
    }
    timeout_ms := reader.read_i32()!
    if is_flexible { reader.skip_tagged_fields()! }
    return DeleteTopicsRequest{ topics: topics, timeout_ms: timeout_ms }
}

pub struct ListGroupsRequest {
pub:
    states_filter   []string
}

fn parse_list_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !ListGroupsRequest {
    mut states_filter := []string{}
    if version >= 4 {
        count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
        for _ in 0 .. count {
            states_filter << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
        }
    }
    if is_flexible { reader.skip_tagged_fields()! }
    return ListGroupsRequest{ states_filter: states_filter }
}

pub struct DescribeGroupsRequest {
pub:
    groups                      []string
    include_authorized_operations bool
}

fn parse_describe_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeGroupsRequest {
    count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
    mut groups := []string{}
    for _ in 0 .. count {
        groups << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
    }
    mut include_authorized_operations := false
    if version >= 3 { include_authorized_operations = reader.read_i8()! != 0 }
    if is_flexible { reader.skip_tagged_fields()! }
    return DescribeGroupsRequest{ groups: groups, include_authorized_operations: include_authorized_operations }
}
