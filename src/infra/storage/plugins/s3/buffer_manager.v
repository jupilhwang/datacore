// Infra Layer - S3 버퍼 매니저
// S3 스토리지를 위한 버퍼 관리 및 플러시 작업 처리
module s3

import json
import strconv
import time

/// TopicPartitionBuffer는 S3에 플러시하기 전 특정 파티션의 레코드를 보관합니다.
/// 배치 쓰기를 통해 S3 API 호출 횟수를 줄이고 성능을 최적화합니다.
struct TopicPartitionBuffer {
mut:
	records            []StoredRecord // 버퍼링된 레코드 목록 (아직 S3에 저장되지 않음)
	current_size_bytes i64            // 버퍼 내 모든 레코드의 현재 총 크기 (바이트)
}

/// async_flush_partition은 단일 파티션 배치에 대해 S3 put과 인덱스 업데이트를 수행합니다.
/// 이 함수는 비동기로 호출되며, 버퍼의 레코드를 S3 세그먼트로 저장합니다.
/// 주의: 현재는 flush_worker에서만 호출되며, 직접 호출은 비활성화되어 있습니다.
fn (mut a S3StorageAdapter) async_flush_partition(partition_key string) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// 1. 락 획득, 버퍼 복사, 메모리 내 버퍼 초기화
	a.buffer_lock.lock()

	mut tp_buffer := a.topic_partition_buffers[partition_key] or {
		a.buffer_lock.unlock()
		return
	}

	if tp_buffer.records.len == 0 {
		a.buffer_lock.unlock()
		return
	}

	buffer_data := tp_buffer.records.clone()
	tp_buffer.records.clear()
	tp_buffer.current_size_bytes = 0
	a.topic_partition_buffers[partition_key] = tp_buffer
	a.buffer_lock.unlock()

	// 2. 배치의 오프셋 계산
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// 3. 세그먼트 인코딩 및 S3에 쓰기
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// S3에 세그먼트 쓰기
	a.put_object(segment_key, segment_data) or {
		// 실패 시 재시도하지 않으면 데이터 손실. 현재는 로깅 후 에러 반환.
		eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during async flush: ${err}')
	}

	// 4. 새 세그먼트로 파티션 인덱스 업데이트 (동시 업데이트로부터 원자적/안전해야 함)

	// 최신 영속화 상태를 보장하기 위해 S3에서 직접 현재 인덱스 조회 (캐시 우회)
	index_key := a.partition_index_key(topic, partition)
	mut index := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}

	if data, _ := a.get_object(index_key, -1, -1) {
		if decoded := json.decode(PartitionIndex, data.bytestr()) {
			index = decoded
		}
	}

	// 새 세그먼트로 인덱스 업데이트
	// 현재 S3 high_watermark를 넘어서 확장되는 경우 세그먼트 추가
	// 이는 인메모리 캐시가 아닌 S3 영속화 상태 기반
	if base_offset >= index.high_watermark {
		// high watermark는 S3에 성공적으로 저장된 데이터 기반으로 계산
		index.high_watermark = end_offset + 1 // 새 high watermark
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// 업데이트된 인덱스를 S3에 쓰기
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during async flush: ${err}')
		}

		// 새 인덱스로 로컬 캐시 업데이트 (인메모리의 더 높은 high_watermark 유지)
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// 캐시와 S3 사이에서 더 높은 high_watermark 유지
			mut final_index := index
			if cached.index.high_watermark > index.high_watermark {
				final_index.high_watermark = cached.index.high_watermark
			}
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     final_index
				cached_at: time.now()
			}
		} else {
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     index
				cached_at: time.now()
			}
		}
		a.topic_lock.unlock()
	} else {
		// 이 세그먼트는 이미 저장된 데이터와 겹침, 인덱스 업데이트 건너뜀
		eprintln('[S3] ASYNC FLUSH WARNING: Segment base_offset ${base_offset} < high_watermark ${index.high_watermark}. Index not updated.')
	}
}

/// flush_worker는 주기적으로 데이터가 축적된 모든 버퍼를 플러시합니다.
/// 인덱스 충돌을 피하기 위해 파티션별 순차 처리를 사용합니다.
/// batch_timeout_ms 간격으로 실행되며, compactor_running이 false가 되면 종료됩니다.
fn (mut a S3StorageAdapter) flush_worker() {
	for a.compactor_running { // compactor_running 플래그를 사용하여 두 워커 모두 중지
		time.sleep(g_s3_config.batch_timeout_ms * time.millisecond)

		// 락을 유지한 채 각 파티션의 버퍼 처리
		// 키 수집과 플러시 사이에 append가 버퍼를 수정하는 경쟁 조건 방지
		a.buffer_lock.lock()

		// 데이터가 있는 모든 파티션 키 수집
		mut keys_to_flush := []string{}
		for key, tp_buffer in a.topic_partition_buffers {
			if tp_buffer.records.len > 0 {
				keys_to_flush << key
			}
		}

		// 각 키에 대해 락을 유지한 채 버퍼 데이터 추출
		mut flush_batches := map[string][]StoredRecord{}
		for key in keys_to_flush {
			if mut tp_buffer := a.topic_partition_buffers[key] {
				if tp_buffer.records.len > 0 {
					// 버퍼를 원자적으로 복제하고 초기화
					flush_batches[key] = tp_buffer.records.clone()
					tp_buffer.records.clear()
					tp_buffer.current_size_bytes = 0
					a.topic_partition_buffers[key] = tp_buffer
				}
			}
		}

		a.buffer_lock.unlock()

		// 이제 각 배치를 S3에 플러시 (락 없이)
		for key, buffer_data in flush_batches {
			if buffer_data.len == 0 {
				continue
			}
			a.flush_buffer_to_s3(key, buffer_data) or {
				eprintln('[S3] Flush failed for ${key}: ${err}')
				// 실패 시 데이터 손실 방지를 위해 버퍼 데이터 복원
				a.buffer_lock.lock()
				if mut tp_buffer := a.topic_partition_buffers[key] {
					// 실패한 데이터를 기존 버퍼 앞에 추가
					mut restored := buffer_data.clone()
					restored << tp_buffer.records
					tp_buffer.records = restored
					// 크기 재계산
					mut size := i64(0)
					for rec in tp_buffer.records {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					tp_buffer.current_size_bytes = size
					a.topic_partition_buffers[key] = tp_buffer
				} else {
					// 버퍼가 삭제됨, 재생성
					mut size := i64(0)
					for rec in buffer_data {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					a.topic_partition_buffers[key] = TopicPartitionBuffer{
						records:            buffer_data.clone()
						current_size_bytes: size
					}
				}
				a.buffer_lock.unlock()
				eprintln('[S3] Buffer restored for ${key} with ${buffer_data.len} records')
			}
		}
	}
}

/// flush_buffer_to_s3는 특정 버퍼 배치를 S3에 플러시합니다.
/// 세그먼트를 S3에 저장하고 파티션 인덱스를 업데이트합니다.
/// 동시 인덱스 업데이트로 인한 손상을 방지하기 위해 index_update_lock을 사용합니다.
fn (mut a S3StorageAdapter) flush_buffer_to_s3(partition_key string, buffer_data []StoredRecord) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// 배치의 오프셋 계산
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// 세그먼트 인코딩 및 S3에 쓰기
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// S3에 세그먼트 쓰기
	a.put_object(segment_key, segment_data) or {
		eprintln('[S3] FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during flush: ${err}')
	}

	// 새 세그먼트로 파티션 인덱스 업데이트
	// 동시 인덱스 업데이트로 인한 인덱스 손상 방지를 위해 락 사용
	a.index_update_lock.lock()
	defer {
		a.index_update_lock.unlock()
	}

	// S3에서 직접 현재 인덱스 조회 (캐시 우회)
	index_key := a.partition_index_key(topic, partition)
	mut index := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}

	if data, _ := a.get_object(index_key, -1, -1) {
		if decoded := json.decode(PartitionIndex, data.bytestr()) {
			index = decoded
		}
	}

	// 세그먼트가 아직 존재하지 않으면 항상 추가
	// 세그먼트 키 비교로 중복 확인
	mut segment_exists := false
	for seg in index.log_segments {
		if seg.key == segment_key {
			segment_exists = true
			break
		}
	}

	if !segment_exists {
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// 순서 유지를 위해 start_offset으로 세그먼트 정렬
		index.log_segments.sort(a.start_offset < b.start_offset)

		// high_watermark를 최대 end_offset + 1로 업데이트
		for seg in index.log_segments {
			if seg.end_offset + 1 > index.high_watermark {
				index.high_watermark = seg.end_offset + 1
			}
		}

		// 업데이트된 인덱스를 S3에 쓰기
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during flush: ${err}')
		}

		// 로컬 캐시 업데이트
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// 캐시와 S3 사이에서 더 높은 high_watermark 유지
			mut final_index := index
			if cached.index.high_watermark > index.high_watermark {
				final_index.high_watermark = cached.index.high_watermark
			}
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     final_index
				cached_at: time.now()
			}
		} else {
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     index
				cached_at: time.now()
			}
		}
		a.topic_lock.unlock()
	}
}
