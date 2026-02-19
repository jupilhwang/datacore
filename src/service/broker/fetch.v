// Handles business logic for Kafka Fetch requests.
// Used when consumers fetch messages from topic/partition.
module broker

import domain
import service.port
import time

/// FetchUseCase handles fetch request business logic.
/// Supports single and parallel fetch, including timeout handling.
pub struct FetchUseCase {
	storage port.StoragePort
}

/// new_fetch_usecase creates a new FetchUseCase.
pub fn new_fetch_usecase(storage port.StoragePort) &FetchUseCase {
	return &FetchUseCase{
		storage: storage
	}
}

/// FetchPartitionRequest represents a fetch request for a single partition.
pub struct FetchPartitionRequest {
pub:
	topic        string
	partition    int
	fetch_offset i64
	max_bytes    int
}

/// FetchPartitionResponse represents a fetch response for a single partition.
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

/// FetchRequest represents a fetch request.
pub struct FetchRequest {
pub:
	replica_id      i32
	max_wait_ms     i32
	min_bytes       i32
	max_bytes       i32
	isolation_level i8
	partitions      []FetchPartitionRequest
}

/// FetchResponse represents a fetch response.
pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	partitions       []FetchPartitionResponse
}

// Parallel processing threshold - use parallel fetch when partition count exceeds this value
const parallel_threshold = 2

// Default timeout for parallel fetch operations (ms)
const parallel_fetch_timeout_ms = 30000

/// execute processes a fetch request.
/// Selects sequential or parallel processing based on partition count.
pub fn (u &FetchUseCase) execute(req FetchRequest) FetchResponse {
	if req.partitions.len > parallel_threshold {
		return u.execute_parallel(req)
	}
	return u.execute_sequential(req)
}

/// execute_sequential processes fetch requests sequentially (for small requests).
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

/// execute_parallel processes fetch requests in parallel using spawn.
/// Includes timeout handling.
fn (u &FetchUseCase) execute_parallel(req FetchRequest) FetchResponse {
	ch := chan FetchPartitionResponse{cap: req.partitions.len}
	for part_req in req.partitions {
		spawn u.fetch_partition_async(part_req, ch)
	}

	// Calculate timeout from request max_wait_ms or default
	timeout_ms := if req.max_wait_ms > 0 {
		int(req.max_wait_ms)
	} else {
		parallel_fetch_timeout_ms
	}

	// Collect results with timeout
	mut partition_responses := []FetchPartitionResponse{cap: req.partitions.len}
	mut received := 0
	mut timed_out := false

	for received < req.partitions.len && !timed_out {
		select {
			response := <-ch {
				partition_responses << response
				received += 1
			}
			timeout_ms * time.millisecond {
				// Timeout reached - stop waiting for more responses
				timed_out = true
				eprintln('[Fetch] Parallel fetch timeout after ${timeout_ms}ms, received ${received}/${req.partitions.len} responses')
			}
		}
	}

	// If not all responses received, add error responses for missing partitions
	if timed_out && received < req.partitions.len {
		// Build set of received partitions
		mut received_parts := map[string]bool{}
		for resp in partition_responses {
			key := '${resp.topic}:${resp.partition}'
			received_parts[key] = true
		}

		// Add timeout error responses for missing partitions
		for part_req in req.partitions {
			key := '${part_req.topic}:${part_req.partition}'
			if key !in received_parts {
				partition_responses << FetchPartitionResponse{
					topic:      part_req.topic
					partition:  part_req.partition
					error_code: i16(domain.ErrorCode.request_timed_out)
				}
			}
		}
	}

	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		partitions:       partition_responses
	}
}

/// fetch_partition_async fetches a single partition and sends the result to the channel.
fn (u &FetchUseCase) fetch_partition_async(part_req FetchPartitionRequest, ch chan FetchPartitionResponse) {
	ch <- u.fetch_partition(part_req)
}

/// fetch_partition fetches records from a single partition.
fn (u &FetchUseCase) fetch_partition(part_req FetchPartitionRequest) FetchPartitionResponse {
	_ := u.storage.get_topic(part_req.topic) or {
		return FetchPartitionResponse{
			topic:      part_req.topic
			partition:  part_req.partition
			error_code: i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}
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
