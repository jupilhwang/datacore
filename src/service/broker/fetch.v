// Kafka Fetch 요청의 비즈니스 로직을 처리합니다.
// 컨슈머가 토픽/파티션에서 메시지를 가져올 때 사용됩니다.
module broker

import domain
import service.port
import time

/// FetchUseCase는 fetch 요청 비즈니스 로직을 처리합니다.
/// 단일 및 병렬 fetch를 지원하며, 타임아웃 처리를 포함합니다.
pub struct FetchUseCase {
	storage port.StoragePort
}

/// new_fetch_usecase는 새로운 FetchUseCase를 생성합니다.
pub fn new_fetch_usecase(storage port.StoragePort) &FetchUseCase {
	return &FetchUseCase{
		storage: storage
	}
}

/// FetchPartitionRequest는 단일 파티션에 대한 fetch 요청을 나타냅니다.
pub struct FetchPartitionRequest {
pub:
	topic        string
	partition    int
	fetch_offset i64
	max_bytes    int
}

/// FetchPartitionResponse는 단일 파티션에 대한 fetch 응답을 나타냅니다.
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

/// FetchRequest는 fetch 요청을 나타냅니다.
pub struct FetchRequest {
pub:
	replica_id      i32
	max_wait_ms     i32
	min_bytes       i32
	max_bytes       i32
	isolation_level i8
	partitions      []FetchPartitionRequest
}

/// FetchResponse는 fetch 응답을 나타냅니다.
pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	partitions       []FetchPartitionResponse
}

// 병렬 처리 임계값 - 파티션 수가 이 값을 초과하면 병렬 fetch 사용
const parallel_threshold = 2

// 병렬 fetch 작업의 기본 타임아웃 (ms)
const parallel_fetch_timeout_ms = 30000

/// execute는 fetch 요청을 처리합니다.
/// 파티션 수에 따라 순차 또는 병렬 처리를 선택합니다.
pub fn (u &FetchUseCase) execute(req FetchRequest) FetchResponse {
	if req.partitions.len > parallel_threshold {
		return u.execute_parallel(req)
	}
	return u.execute_sequential(req)
}

/// execute_sequential은 fetch 요청을 순차적으로 처리합니다 (소규모 요청용).
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

/// execute_parallel은 spawn을 사용하여 fetch 요청을 병렬로 처리합니다.
/// 타임아웃 처리를 포함합니다.
fn (u &FetchUseCase) execute_parallel(req FetchRequest) FetchResponse {
	ch := chan FetchPartitionResponse{cap: req.partitions.len}
	for part_req in req.partitions {
		spawn u.fetch_partition_async(part_req, ch)
	}

	// 요청의 max_wait_ms 또는 기본값으로 타임아웃 계산
	timeout_ms := if req.max_wait_ms > 0 {
		int(req.max_wait_ms)
	} else {
		parallel_fetch_timeout_ms
	}

	// 타임아웃과 함께 결과 수집
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
				// 타임아웃 도달 - 더 이상 응답 대기 중지
				timed_out = true
				eprintln('[Fetch] Parallel fetch timeout after ${timeout_ms}ms, received ${received}/${req.partitions.len} responses')
			}
		}
	}

	// 모든 응답을 받지 못한 경우, 누락된 파티션에 대해 오류 응답 추가
	if timed_out && received < req.partitions.len {
		// 받은 파티션 집합 구성
		mut received_parts := map[string]bool{}
		for resp in partition_responses {
			key := '${resp.topic}:${resp.partition}'
			received_parts[key] = true
		}

		// 누락된 파티션에 대해 타임아웃 오류 응답 추가
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

/// fetch_partition_async는 단일 파티션을 fetch하고 결과를 채널로 전송합니다.
fn (u &FetchUseCase) fetch_partition_async(part_req FetchPartitionRequest, ch chan FetchPartitionResponse) {
	ch <- u.fetch_partition(part_req)
}

/// fetch_partition은 단일 파티션에서 레코드를 가져옵니다.
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
