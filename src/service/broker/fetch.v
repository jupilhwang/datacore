// UseCase Layer - Fetch UseCase
module broker

import domain
import service.port

// FetchUseCase handles fetch request business logic
pub struct FetchUseCase {
    storage port.StoragePort
}

pub fn new_fetch_usecase(storage port.StoragePort) &FetchUseCase {
    return &FetchUseCase{
        storage: storage
    }
}

// FetchPartitionRequest represents a fetch request for a single partition
pub struct FetchPartitionRequest {
pub:
    topic           string
    partition       int
    fetch_offset    i64
    max_bytes       int
}

// FetchPartitionResponse represents a fetch response for a single partition
pub struct FetchPartitionResponse {
pub:
    topic               string
    partition           int
    error_code          i16
    high_watermark      i64
    last_stable_offset  i64
    log_start_offset    i64
    records             []domain.Record
}

// FetchRequest represents a fetch request
pub struct FetchRequest {
pub:
    replica_id      i32
    max_wait_ms     i32
    min_bytes       i32
    max_bytes       i32
    isolation_level i8
    partitions      []FetchPartitionRequest
}

// FetchResponse represents a fetch response
pub struct FetchResponse {
pub:
    throttle_time_ms    i32
    error_code          i16
    partitions          []FetchPartitionResponse
}

// Execute processes the fetch request
pub fn (u &FetchUseCase) execute(req FetchRequest) FetchResponse {
    mut partition_responses := []FetchPartitionResponse{}
    
    for part_req in req.partitions {
        // Check topic exists
        _ := u.storage.get_topic(part_req.topic) or {
            partition_responses << FetchPartitionResponse{
                topic: part_req.topic
                partition: part_req.partition
                error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
            }
            continue
        }
        
        // Fetch records
        result := u.storage.fetch(
            part_req.topic,
            part_req.partition,
            part_req.fetch_offset,
            part_req.max_bytes
        ) or {
            partition_responses << FetchPartitionResponse{
                topic: part_req.topic
                partition: part_req.partition
                error_code: i16(domain.ErrorCode.unknown_server_error)
            }
            continue
        }
        
        partition_responses << FetchPartitionResponse{
            topic: part_req.topic
            partition: part_req.partition
            error_code: 0
            high_watermark: result.high_watermark
            last_stable_offset: result.last_stable_offset
            log_start_offset: result.log_start_offset
            records: result.records
        }
    }
    
    return FetchResponse{
        throttle_time_ms: 0
        error_code: 0
        partitions: partition_responses
    }
}
