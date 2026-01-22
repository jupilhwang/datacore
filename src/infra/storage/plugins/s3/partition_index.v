// Infra Layer - S3 파티션 인덱스 관리
// 파티션 인덱스 저장 및 캐싱 처리
module s3

import json
import time

/// PartitionIndex는 S3에 저장되는 파티션 인덱스입니다.
struct PartitionIndex {
mut:
	topic           string       // 토픽 이름
	partition       int          // 파티션 번호
	earliest_offset i64          // 가장 이른 오프셋
	high_watermark  i64          // high watermark (다음 쓰기 오프셋)
	log_segments    []LogSegment // 로그 세그먼트 목록
}

/// LogSegment는 S3에 저장된 로그 세그먼트를 나타냅니다.
struct LogSegment {
	start_offset i64       // 시작 오프셋
	end_offset   i64       // 종료 오프셋
	key          string    // S3 객체 키
	size_bytes   i64       // 세그먼트 크기 (바이트)
	created_at   time.Time // 생성 시간
}

/// CachedPartitionIndex는 메타데이터와 함께 캐시된 파티션 인덱스를 담습니다.
struct CachedPartitionIndex {
	index     PartitionIndex // 파티션 인덱스
	etag      string         // S3 ETag
	cached_at time.Time      // 캐시 시간
}

/// get_partition_index는 캐시 또는 S3에서 파티션 인덱스를 조회합니다.
fn (mut a S3StorageAdapter) get_partition_index(topic string, partition int) !PartitionIndex {
	key := '${topic}:${partition}'
	// 1. 캐시 확인
	a.topic_lock.rlock()
	cached_exists := key in a.topic_index_cache
	mut cached_index := PartitionIndex{}
	mut cached_etag := ''
	if cached_exists {
		cached := a.topic_index_cache[key]
		cached_index = cached.index
		cached_etag = cached.etag
		if time.since(cached.cached_at).milliseconds() < g_s3_config.index_cache_ttl_ms {
			a.topic_lock.runlock()
			return cached.index
		}
	}
	a.topic_lock.runlock()

	// 2. S3에서 조회
	index_key := a.partition_index_key(topic, partition)
	data, etag := a.get_object(index_key, -1, -1) or {
		// S3에서 인덱스를 찾을 수 없음
		// 캐시된 버전이 있으면 (오래되었더라도) 새 빈 인덱스 생성보다 선호
		if cached_exists {
			// 캐시 타임스탬프 갱신하되 데이터는 유지
			a.topic_lock.@lock()
			a.topic_index_cache[key] = CachedPartitionIndex{
				index:     cached_index
				etag:      cached_etag
				cached_at: time.now()
			}
			a.topic_lock.unlock()
			return cached_index
		}
		// 캐시가 없으면 새 빈 인덱스 생성
		a.topic_lock.@lock()
		a.topic_index_cache[key] = CachedPartitionIndex{
			index:     PartitionIndex{
				topic:           topic
				partition:       partition
				earliest_offset: 0
				high_watermark:  0
				log_segments:    []
			}
			etag:      ''
			cached_at: time.now()
		}
		a.topic_lock.unlock()
		return a.topic_index_cache[key].index // 새로 캐시된 빈 인덱스 반환
	}

	// 3. S3 인덱스 디코딩
	s3_index := json.decode(PartitionIndex, data.bytestr())!

	// 4. 캐시된 인덱스와 병합 - 더 높은 high_watermark 유지
	// append가 캐시를 업데이트했지만 flush가 아직 S3에 쓰지 않은 경우 처리
	mut final_index := s3_index
	if cached_exists && cached_index.high_watermark > s3_index.high_watermark {
		final_index.high_watermark = cached_index.high_watermark
	}

	a.topic_lock.@lock()
	a.topic_index_cache[key] = CachedPartitionIndex{
		index:     final_index
		etag:      etag
		cached_at: time.now()
	}
	a.topic_lock.unlock()

	return final_index
}
