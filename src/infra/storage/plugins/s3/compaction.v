// Infra Layer - S3 컴팩션 로직
// S3 스토리지를 위한 세그먼트 컴팩션 및 병합 처리
module s3

import json
import time

/// compaction_worker는 주기적으로 병합할 세그먼트를 확인하고 컴팩션을 수행합니다.
fn (mut a S3StorageAdapter) compaction_worker() {
	for a.compactor_running {
		time.sleep(a.config.compaction_interval_ms)

		a.compact_all_partitions() or {
			// 프로덕션에서는 여기에 구조화된 로깅 사용
			eprintln('[S3] Compaction failed: ${err}')
			continue
		}
	}
}

/// compact_all_partitions는 모든 토픽과 파티션을 순회하며 작은 세그먼트 병합을 시도합니다.
fn (mut a S3StorageAdapter) compact_all_partitions() ! {
	topics := a.list_topics()!

	// 컴팩션할 활성 파티션을 추적하기 위해 map/set 사용
	// 단순화를 위해 알려진 모든 토픽/파티션을 순회

	for t in topics {
		for p in 0 .. t.partition_count {
			a.compact_partition(t.name, p)!
		}
	}
}

/// compact_partition은 특정 파티션의 세그먼트를 컴팩션합니다.
fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	// 1. 현재 인덱스 조회
	mut index := a.get_partition_index(topic, partition)!

	// 2. 컴팩션 대상 세그먼트 식별
	mut segments_to_compact := []LogSegment{}
	mut total_size := i64(0)

	for seg in index.log_segments {
		if total_size >= a.config.target_segment_bytes {
			break
		}

		// 목표 크기보다 작은 세그먼트만 고려
		if seg.size_bytes < a.config.target_segment_bytes {
			segments_to_compact << seg
			total_size += seg.size_bytes
		}
	}

	// 충분한 작은 세그먼트가 있는지 확인
	if segments_to_compact.len < a.min_segment_count_to_compact
		|| total_size < a.config.target_segment_bytes / 2 {
		return
	}

	// 3. 컴팩션 수행
	// 세그먼트 병합 후 새로운 큰 세그먼트를 S3에 업로드
	a.merge_segments(topic, partition, mut index, segments_to_compact)!
}

/// merge_segments는 여러 세그먼트를 하나의 큰 세그먼트로 병합합니다.
fn (mut a S3StorageAdapter) merge_segments(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// 모든 세그먼트 데이터 다운로드 (단순화를 위해 순차 처리, 병렬 가능)
	mut merged_data := []u8{}

	for seg in segments {
		data, _ := a.get_object(seg.key, -1, -1) or {
			// 에러 로깅 후 다음 세그먼트 세트로 계속하거나 에러 반환
			// 안전을 위해 에러 반환
			return error('Failed to download segment ${seg.key}: ${err}')
		}
		merged_data << data
	}

	// 새 세그먼트 메타데이터
	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	new_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	// 병합된 새 세그먼트를 S3에 업로드
	a.put_object(new_key, merged_data)!

	// 4. 인덱스 업데이트 및 이전 세그먼트 삭제 (원자적 인덱스 업데이트)

	// 병합된 세그먼트가 커버하는 오프셋 범위 찾기
	start_index := index.log_segments.index(segments[0])
	if start_index < 0 {
		return error('Compaction internal error: start segment not found in index')
	}
	end_index := index.log_segments.index(segments[segments.len - 1])
	if end_index < 0 {
		return error('Compaction internal error: end segment not found in index')
	}

	// 새 로그 세그먼트 목록 (병합된 것 제외)
	mut new_log_segments := []LogSegment{}

	// 병합 블록 이전의 세그먼트
	if start_index > 0 {
		new_log_segments << index.log_segments[0..start_index]
	}

	// 새로 병합된 세그먼트 추가
	new_log_segments << LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          new_key
		size_bytes:   i64(merged_data.len)
		created_at:   time.now()
	}

	// 병합 블록 이후의 세그먼트
	if end_index < index.log_segments.len - 1 {
		new_log_segments << index.log_segments[end_index + 1..]
	}

	// 인덱스 객체 업데이트
	index.log_segments = new_log_segments
	index_key := a.partition_index_key(topic, partition)

	// 새 인덱스를 원자적으로 쓰기 (이전 것 덮어쓰기)
	a.put_object(index_key, json.encode(index).bytes())!

	// 5. 이전 세그먼트 삭제 (인덱스 업데이트 후 비중요 단계)
	for seg in segments {
		a.delete_object(seg.key) or {
			eprintln('[S3] Failed to delete old segment ${seg.key}: ${err}')
		}
	}
}
