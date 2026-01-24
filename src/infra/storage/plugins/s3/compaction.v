// Infra Layer - S3 컴팩션 로직
// S3 스토리지를 위한 세그먼트 컴팩션 및 병합 처리
module s3

import json
import time

// Compaction worker 설정 상수
const max_consecutive_failures = 5 // 컴팩션 최대 연속 실패 횟수
const failure_backoff_duration = 5 * time.minute // 연속 실패 시 백오프 시간
const compaction_sequential_threshold = 3 // 순차 처리 임계값 (파티션 수)
const max_compaction_concurrent = 10 // 컴팩션 최대 동시 실행 수
const compaction_size_threshold_divisor = 2 // 컴팩션 크기 임계값 계수 (target_segment_bytes / 2)

/// compaction_worker는 주기적으로 병합할 세그먼트를 확인하고 컴팩션을 수행합니다.
fn (mut a S3StorageAdapter) compaction_worker() {
	mut consecutive_failures := 0

	for a.compactor_running {
		time.sleep(a.config.compaction_interval_ms * time.millisecond)

		log_message(.debug, 'Compaction', 'Starting compaction cycle', {})

		a.compact_all_partitions() or {
			consecutive_failures++
			log_message(.error, 'Compaction', 'Compaction cycle failed', {
				'consecutive_failures': consecutive_failures.str()
				'max_failures':         max_consecutive_failures.str()
				'error':                err.msg()
			})

			// 연속 실패가 너무 많으면 백오프 증가
			if consecutive_failures >= max_consecutive_failures {
				log_message(.warn, 'Compaction', 'Too many consecutive failures, backing off',
					{
					'backoff_duration': failure_backoff_duration.str()
				})
				time.sleep(failure_backoff_duration)
				consecutive_failures = 0
			}
			continue
		}

		// 성공 시 카운터 리셋
		if consecutive_failures > 0 {
			log_message(.info, 'Compaction', 'Compaction succeeded after failures', {
				'previous_failures': consecutive_failures.str()
			})
			consecutive_failures = 0
		}
	}
}

/// compact_all_partitions는 모든 토픽과 파티션을 순회하며 작은 세그먼트 병합을 시도합니다.
/// 병렬 처리를 통해 성능을 최적화합니다.
fn (mut a S3StorageAdapter) compact_all_partitions() ! {
	topics := a.list_topics()!

	// 컴팩션할 파티션 목록 수집
	mut partition_keys := []string{}
	for t in topics {
		for p in 0 .. t.partition_count {
			partition_keys << '${t.name}:${p}'
		}
	}

	// 병렬 컴팩션 (최대 10개 동시 처리)
	if partition_keys.len <= compaction_sequential_threshold {
		// 작은 배치: 순차 처리
		for key in partition_keys {
			parts := key.split(':')
			if parts.len == 2 {
				topic := parts[0]
				partition := parts[1].int()
				a.compact_partition(topic, partition) or {
					log_message(.error, 'Compaction', 'Partition compaction failed', {
						'partition_key': key
						'error':         err.msg()
					})
				}
			}
		}
	} else {
		// 큰 배치: 병렬 처리 (채널 사용)
		// 채널 버퍼 크기를 max_concurrent로 제한하여 메모리 사용량 제어
		max_concurrent := max_compaction_concurrent
		ch := chan bool{cap: max_concurrent}
		mut active := 0

		for key in partition_keys {
			// 동시 실행 제한
			for active >= max_concurrent {
				_ = <-ch
				active--
			}

			active++
			spawn fn [mut a, key, ch] () {
				parts := key.split(':')
				if parts.len == 2 {
					topic := parts[0]
					partition := parts[1].int()
					a.compact_partition(topic, partition) or {
						log_message(.error, 'Compaction', 'Partition compaction failed',
							{
							'partition_key': key
							'error':         err.msg()
						})
					}
				}
				ch <- true
			}()
		}

		// 모든 작업 완료 대기
		for _ in 0 .. active {
			_ = <-ch
		}
	}
}

/// compact_partition은 특정 파티션의 세그먼트를 컴팩션합니다.
fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	start_time := time.now()

	// 메트릭: 컴팩션 시작
	a.metrics_lock.@lock()
	a.metrics.compaction_count++
	a.metrics_lock.unlock()

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
		|| total_size < a.config.target_segment_bytes / compaction_size_threshold_divisor {
		return
	}

	// 3. 컴팩션 수행
	// 세그먼트 병합 후 새로운 큰 세그먼트를 S3에 업로드
	a.merge_segments(topic, partition, mut index, segments_to_compact) or {
		// 메트릭: 컴팩션 실패
		a.metrics_lock.@lock()
		a.metrics.compaction_error_count++
		a.metrics_lock.unlock()
		return err
	}

	// 메트릭: 컴팩션 성공
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.compaction_success_count++
	a.metrics.compaction_total_ms += elapsed_ms
	a.metrics.compaction_bytes_merged += total_size
	a.metrics_lock.unlock()
}

/// merge_segments는 여러 세그먼트를 하나의 큰 세그먼트로 병합합니다.
/// 세그먼트 다운로드를 병렬로 처리하여 성능을 최적화합니다.
fn (mut a S3StorageAdapter) merge_segments(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// 1. 병렬 세그먼트 다운로드
	merged_data := a.download_segments_parallel(segments)!

	// 2. 새 세그먼트 생성 및 업로드
	new_segment := a.create_merged_segment(topic, partition, segments, merged_data)!

	// 3. 인덱스 업데이트
	a.update_index_with_merged_segment(topic, partition, mut index, segments, new_segment)!

	// 4. 이전 세그먼트 병렬 삭제
	a.delete_segments_parallel(segments)
}

/// download_segments_parallel은 여러 세그먼트를 병렬로 다운로드합니다.
fn (mut a S3StorageAdapter) download_segments_parallel(segments []LogSegment) ![]u8 {
	ch := chan []u8{cap: segments.len}
	mut download_errors := []string{}

	for seg in segments {
		spawn fn [mut a, seg, ch] () {
			data, _ := a.get_object(seg.key, -1, -1) or {
				log_message(.error, 'Compaction', 'Failed to download segment', {
					'segment_key': seg.key
					'error':       err.msg()
				})
				ch <- []u8{}
				return
			}
			ch <- data
		}()
	}

	// 다운로드 결과 수집 및 병합
	mut merged_data := []u8{}
	for _ in 0 .. segments.len {
		data := <-ch
		if data.len == 0 {
			download_errors << 'segment download failed'
		} else {
			merged_data << data
		}
	}

	// 다운로드 실패 시 에러 반환
	if download_errors.len > 0 {
		return error('Failed to download ${download_errors.len} segments')
	}

	return merged_data
}

/// create_merged_segment는 병합된 데이터로 새 세그먼트를 생성하고 S3에 업로드합니다.
fn (mut a S3StorageAdapter) create_merged_segment(topic string, partition int, segments []LogSegment, merged_data []u8) !LogSegment {
	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	new_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	// 병합된 새 세그먼트를 S3에 업로드
	a.put_object(new_key, merged_data)!

	return LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          new_key
		size_bytes:   i64(merged_data.len)
		created_at:   time.now()
	}
}

/// update_index_with_merged_segment는 인덱스를 업데이트하여 병합된 세그먼트를 반영합니다.
fn (mut a S3StorageAdapter) update_index_with_merged_segment(topic string, partition int, mut index PartitionIndex, old_segments []LogSegment, new_segment LogSegment) ! {
	// 병합된 세그먼트가 커버하는 오프셋 범위 찾기
	start_index := index.log_segments.index(old_segments[0])
	if start_index < 0 {
		return error('Compaction internal error: start segment not found in index')
	}
	end_index := index.log_segments.index(old_segments[old_segments.len - 1])
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
	new_log_segments << new_segment

	// 병합 블록 이후의 세그먼트
	if end_index < index.log_segments.len - 1 {
		new_log_segments << index.log_segments[end_index + 1..]
	}

	// 인덱스 객체 업데이트
	index.log_segments = new_log_segments
	index_key := a.partition_index_key(topic, partition)

	// 새 인덱스를 원자적으로 쓰기 (이전 것 덮어쓰기)
	a.put_object(index_key, json.encode(index).bytes())!
}

/// delete_segments_parallel은 여러 세그먼트를 병렬로 삭제합니다.
fn (mut a S3StorageAdapter) delete_segments_parallel(segments []LogSegment) {
	ch := chan bool{cap: segments.len}
	for seg in segments {
		spawn fn [mut a, seg, ch] () {
			a.delete_object(seg.key) or {
				log_message(.error, 'Compaction', 'Failed to delete old segment', {
					'segment_key': seg.key
					'error':       err.msg()
				})
			}
			ch <- true
		}()
	}

	// 모든 삭제 완료 대기
	for _ in 0 .. segments.len {
		_ = <-ch
	}
}
