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
	topic        string
	partition    int
	fetch_offset i64
	max_bytes    int
}

// FetchPartitionResponse represents a fetch response for a single partition
pub struct FetchPartitionResponse {
pub:
	topic              string
	partition          int
	error_code         i16
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
	records            []domain.Record
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
	throttle_time_ms i32
	error_code       i16
	partitions       []FetchPartitionResponse
}

// Parallelization threshold - use parallel fetch when partition count exceeds this
const parallel_threshold = 2

// Execute processes the fetch request
pub fn (u &FetchUseCase) execute(req FetchRequest) FetchResponse {
	// Use parallel fetch for multiple partitions
	if req.partitions.len > parallel_threshold {
		return u.execute_parallel(req)
	}
	return u.execute_sequential(req)
}

// execute_sequential processes fetch request sequentially (for small requests)
fn (u &FetchUseCase) execute_sequential(req FetchRequest) FetchResponse {
	mut partition_responses := []FetchPartitionResponse{cap: req.partitions.len}

	for part_req in req.partitions {
		partition_responses << u.fetch_partition(part_req)
	}

	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		partitions:       partition_responses
	}
}

// execute_parallel processes fetch request in parallel using spawn
fn (u &FetchUseCase) execute_parallel(req FetchRequest) FetchResponse {
	// Create channel for results
	ch := chan FetchPartitionResponse{cap: req.partitions.len}

	// Spawn fetch tasks for each partition
	for part_req in req.partitions {
		spawn u.fetch_partition_async(part_req, ch)
	}

	// Collect results
	mut partition_responses := []FetchPartitionResponse{cap: req.partitions.len}
	for _ in 0 .. req.partitions.len {
		partition_responses << <-ch
	}

	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		partitions:       partition_responses
	}
}

// fetch_partition_async fetches a single partition and sends result to channel
fn (u &FetchUseCase) fetch_partition_async(part_req FetchPartitionRequest, ch chan FetchPartitionResponse) {
	ch <- u.fetch_partition(part_req)
}

// fetch_partition fetches records from a single partition
fn (u &FetchUseCase) fetch_partition(part_req FetchPartitionRequest) FetchPartitionResponse {
	// Check topic exists
	_ := u.storage.get_topic(part_req.topic) or {
		return FetchPartitionResponse{
			topic:      part_req.topic
			partition:  part_req.partition
			error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}

	// Fetch records
	result := u.storage.fetch(part_req.topic, part_req.partition, part_req.fetch_offset,
		part_req.max_bytes) or {
		return FetchPartitionResponse{
			topic:      part_req.topic
			partition:  part_req.partition
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	return FetchPartitionResponse{
		topic:              part_req.topic
		partition:          part_req.partition
		error_code:         0
		high_watermark:     result.high_watermark
		last_stable_offset: result.last_stable_offset
		log_start_offset:   result.log_start_offset
		records:            result.records
	}
}
