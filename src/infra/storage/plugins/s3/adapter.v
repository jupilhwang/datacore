// Infra Layer - S3 스토리지 어댑터
// S3 백엔드를 사용한 StoragePort 인터페이스 구현
// 조건부 쓰기(ETag)를 통한 동시성 제어 지원
module s3

import domain
import service.port
import time
import json
import crypto.md5
import sync

// Global config storage to work around V struct copy issues
__global g_s3_config = S3Config{}

// 참고: S3 HTTP 클라이언트 함수들 (sign_request, get_object, put_object 등)은 s3_client.v에 있습니다.
// 참고: PartitionIndex, LogSegment, CachedPartitionIndex는 partition_index.v에 정의되어 있습니다.
// 참고: StoredRecord, TopicPartitionBuffer는 s3_record_codec.v와 buffer_manager.v에 정의되어 있습니다.
// 참고: 버퍼 관리 함수들은 buffer_manager.v에 있습니다.
// 참고: 컴팩션 함수들은 compaction.v에 있습니다.

/// s3_capability는 S3 어댑터의 스토리지 기능을 정의합니다.
pub const s3_capability = domain.StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// S3Config는 S3 스토리지 설정을 담습니다.
pub struct S3Config {
pub mut:
	bucket_name    string = 'datacore-storage' // S3 버킷 이름
	region         string = 'us-east-1'        // AWS 리전
	endpoint       string // 선택사항: MinIO/LocalStack용 엔드포인트
	access_key     string // AWS 액세스 키
	secret_key     string // AWS 시크릿 키
	prefix         string = 'datacore/' // 객체 키 접두사
	max_retries    int    = 3           // 최대 재시도 횟수
	retry_delay_ms int    = 100         // 재시도 지연 시간 (밀리초)
	use_path_style bool   = true        // MinIO 호환성을 위한 경로 스타일 사용
	timezone       string = 'UTC'       // SigV4 호환성을 위한 타임존
	// 배치 설정
	batch_timeout_ms int // 배치 타임아웃 (밀리초)
	batch_max_bytes  i64 // 배치 최대 바이트
	// 컴팩션 설정
	compaction_interval_ms int // 컴팩션 간격 (밀리초)
	target_segment_bytes   i64 // 목표 세그먼트 크기
	index_cache_ttl_ms     int // 인덱스 캐시 TTL (밀리초)
}

/// S3StorageAdapter는 S3 스토리지용 StoragePort를 구현합니다.
pub struct S3StorageAdapter {
pub mut:
	config S3Config
	// TTL이 있는 로컬 캐시
	topic_cache       map[string]CachedTopic
	topic_id_cache    map[string]string // topic_id (hex) -> topic_name, O(1) 조회용
	group_cache       map[string]CachedGroup
	offset_cache      map[string]map[string]i64
	topic_index_cache map[string]CachedPartitionIndex // 인덱스 캐시
	// 스레드 안전성을 위한 락
	topic_lock  sync.RwMutex
	group_lock  sync.RwMutex
	offset_lock sync.RwMutex
	// 배치 S3 쓰기를 위한 플러시 버퍼
	topic_partition_buffers map[string]TopicPartitionBuffer // 키: "topic:partition"
	buffer_lock             sync.Mutex
	index_update_lock       sync.Mutex // 경쟁 조건 방지를 위한 S3 인덱스 업데이트 락
	is_flushing             bool
	// 컴팩션 설정
	min_segment_count_to_compact int = 5 // 컴팩션을 위한 최소 세그먼트 수
	compactor_running            bool
}

/// CachedTopic은 캐시된 토픽 정보를 담습니다.
struct CachedTopic {
	meta      domain.TopicMetadata
	etag      string
	cached_at time.Time
}

/// CachedGroup은 캐시된 그룹 정보를 담습니다.
struct CachedGroup {
	group     domain.ConsumerGroup
	etag      string
	cached_at time.Time
}

// 참고: CachedPartitionIndex는 partition_index.v에 정의되어 있습니다.

// S3 객체 키 구조:
// {prefix}/topics/{topic_name}/metadata.json
// {prefix}/topics/{topic_name}/partitions/{partition}/log-{start_offset}-{end_offset}.bin
// {prefix}/topics/{topic_name}/partitions/{partition}/index.json
// {prefix}/groups/{group_id}/state.json
// {prefix}/offsets/{group_id}/{topic}:{partition}.json

/// new_s3_adapter는 새로운 S3 스토리지 어댑터를 생성합니다.
pub fn new_s3_adapter(config S3Config) !&S3StorageAdapter {
	// Store config in global to work around V struct copy issues
	g_s3_config = config
	
	return &S3StorageAdapter{
		config:                  g_s3_config
		topic_cache:             map[string]CachedTopic{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{}
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
	}
}

// ============================================================
// 토픽 작업 (Topic Operations)
// ============================================================

/// create_topic은 S3에 새로운 토픽을 생성합니다.
pub fn (mut a S3StorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// 토픽 존재 여부 확인
	existing := a.get_topic(name) or { domain.TopicMetadata{} }
	if existing.name.len > 0 {
		return error('Topic already exists: ${name}')
	}

	topic_id := generate_topic_id(name)

	// TopicConfig를 map[string]string으로 변환
	mut config_map := map[string]string{}
	config_map['retention.ms'] = config.retention_ms.str()
	config_map['retention.bytes'] = config.retention_bytes.str()
	config_map['segment.bytes'] = config.segment_bytes.str()
	config_map['cleanup.policy'] = config.cleanup_policy

	meta := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          config_map
		is_internal:     name.starts_with('_')
	}

	// 조건부 쓰기로 S3에 메타데이터 저장 (If-None-Match: *)
	key := a.topic_metadata_key(name)
	data := json.encode(meta)
	a.put_object_if_not_exists(key, data.bytes())!

	// 파티션 인덱스 초기화
	for p in 0 .. partitions {
		index_key := a.partition_index_key(name, p)
		index := PartitionIndex{
			topic:           name
			partition:       p
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}

	// 토픽 캐시에 저장
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      ''
		cached_at: time.now()
	}
	// O(1) 조회를 위해 topic_id -> name 매핑도 캐시
	a.topic_id_cache[meta.topic_id.hex()] = name
	a.topic_lock.unlock()

	return meta
}

/// delete_topic은 S3에서 토픽을 삭제합니다.
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
	// id 캐시에서 제거하기 위해 토픽 메타데이터 조회
	if cached := a.topic_cache[name] {
		a.topic_lock.@lock()
		a.topic_id_cache.delete(cached.meta.topic_id.hex())
		a.topic_lock.unlock()
	}

	// 토픽 접두사로 시작하는 모든 객체 삭제
	prefix := '${g_s3_config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix)!

	// 캐시에서 제거
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

/// list_topics는 S3에서 모든 토픽 목록을 조회합니다.
pub fn (mut a S3StorageAdapter) list_topics() ![]domain.TopicMetadata {
	prefix := '${g_s3_config.prefix}topics/'
	objects := a.list_objects(prefix)!

	mut topics := []domain.TopicMetadata{}
	mut seen := map[string]bool{}

	// 먼저 캐시된 모든 토픽 추가
	a.topic_lock.rlock()
	for name, cached in a.topic_cache {
		if name !in seen {
			seen[name] = true
			topics << cached.meta
		}
	}
	a.topic_lock.runlock()

	// 캐시에 없는 S3의 토픽 추가
	for obj in objects {
		// "datacore/topics/my-topic/metadata.json" 형식의 경로에서 토픽 이름 추출
		if obj.key.ends_with('/metadata.json') {
			parts := obj.key.split('/')
			if parts.len >= 3 {
				topic_name := parts[parts.len - 2]
				if topic_name !in seen {
					seen[topic_name] = true
					if meta := a.get_topic(topic_name) {
						topics << meta
					}
				}
			}
		}
	}

	return topics
}

/// get_topic은 S3에서 토픽 메타데이터를 조회합니다.
pub fn (mut a S3StorageAdapter) get_topic(name string) !domain.TopicMetadata {
	// 캐시 먼저 확인
	a.topic_lock.rlock()
	if cached := a.topic_cache[name] {
		if time.since(cached.cached_at) < time.minute * 5 {
			a.topic_lock.runlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// S3에서 조회
	key := a.topic_metadata_key(name)
	data, etag := a.get_object(key, -1, -1)!

	meta := json.decode(domain.TopicMetadata, data.bytestr())!

	// 캐시 업데이트
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      etag
		cached_at: time.now()
	}
	a.topic_lock.unlock()

	return meta
}

/// get_topic_by_id는 topic_id로 토픽을 조회합니다.
/// O(1) 캐시 조회를 사용합니다.
pub fn (mut a S3StorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	// topic_id를 맵 조회용 hex 문자열로 변환
	topic_id_hex := topic_id.hex()

	// 캐시 먼저 확인 (O(1) 조회)
	a.topic_lock.rlock()
	if topic_name := a.topic_id_cache[topic_id_hex] {
		if cached := a.topic_cache[topic_name] {
			a.topic_lock.runlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// 캐시 미스 - S3에서 조회하고 캐시 채우기
	topics := a.list_topics()!

	// 모든 캐시 업데이트를 위해 한 번만 락
	a.topic_lock.@lock()
	for t in topics {
		// 향후 조회를 위해 topic_id_cache 채우기
		a.topic_id_cache[t.topic_id.hex()] = t.name
	}
	a.topic_lock.unlock()

	// 일치하는 토픽 찾기
	for t in topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('Topic not found')
}

/// add_partitions는 토픽에 파티션을 추가합니다.
pub fn (mut a S3StorageAdapter) add_partitions(name string, new_count int) ! {
	meta := a.get_topic(name)!
	if new_count <= meta.partition_count {
		return error('New partition count must be greater than current')
	}

	// 새 파티션 인덱스 초기화
	for p in meta.partition_count .. new_count {
		index_key := a.partition_index_key(name, p)
		index := PartitionIndex{
			topic:           name
			partition:       p
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}

	// 조건부 쓰기로 토픽 메타데이터 업데이트
	updated_meta := domain.TopicMetadata{
		...meta
		partition_count: new_count
	}

	key := a.topic_metadata_key(name)
	a.put_object(key, json.encode(updated_meta).bytes())!

	// 캐시 무효화
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

// ============================================================
// 레코드 작업 (Record Operations)
// ============================================================

/// append는 파티션에 레코드를 추가합니다 (메모리에 버퍼링, 가득 차면 플러시).
pub fn (mut a S3StorageAdapter) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	if records.len == 0 {
		return domain.AppendResult{
			base_offset:      0
			log_append_time:  time.now().unix_milli()
			log_start_offset: 0
		}
	}

	// 1. 현재 파티션 인덱스 조회
	mut index := a.get_partition_index(topic, partition)!
	base_offset := index.high_watermark
	partition_key := '${topic}:${partition}'

	// 2. 파티션 버퍼에 레코드 추가
	mut bytes_to_add := i64(0)
	mut stored_records := []StoredRecord{}

	for i, rec in records {
		srec := StoredRecord{
			offset:    base_offset + i64(i)
			timestamp: if rec.timestamp.unix_milli() == 0 { time.now() } else { rec.timestamp }
			key:       rec.key
			value:     rec.value
			headers:   rec.headers
		}
		stored_records << srec
		// 간단한 크기 추정 (실제 크기는 메타데이터 인코딩으로 인해 더 큼)
		bytes_to_add += i64(srec.value.len + srec.key.len + 30) // 30은 오프셋, 타임스탬프, 길이 등의 대략적인 오버헤드
	}

	// 3. 인메모리 버퍼 업데이트 및 플러시 확인
	mut should_flush := false // 빠른 경로 플러시용
	a.buffer_lock.lock()
	mut tp_buffer := a.topic_partition_buffers[partition_key] or {
		TopicPartitionBuffer{
			records:            []
			current_size_bytes: 0
		}
	}

	tp_buffer.records << stored_records
	tp_buffer.current_size_bytes += bytes_to_add

	if tp_buffer.current_size_bytes >= g_s3_config.batch_max_bytes {
		should_flush = true
	}

	a.topic_partition_buffers[partition_key] = tp_buffer
	a.buffer_lock.unlock()

	// 4. flush_worker와의 경쟁 조건을 피하기 위해 빠른 경로 플러시는 비활성화됨
	// 모든 플러시는 이제 주기적인 flush_worker에서 처리됨
	// if should_flush {
	// 	go a.async_flush_partition(partition_key)
	// }
	_ = should_flush // 미사용 변수 경고 억제

	// 5. 응답을 위해 인메모리 high_watermark 즉시 업데이트
	// 다음 append가 올바른 오프셋을 받도록 캐시도 업데이트
	new_high_watermark := base_offset + i64(records.len)
	a.topic_lock.@lock()
	if cached := a.topic_index_cache[partition_key] {
		mut updated_index := cached.index
		updated_index.high_watermark = new_high_watermark
		a.topic_index_cache[partition_key] = CachedPartitionIndex{
			index:     updated_index
			etag:      cached.etag
			cached_at: cached.cached_at
		}
	}
	a.topic_lock.unlock()

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  time.now().unix_milli()
		log_start_offset: index.earliest_offset
	}
}

/// fetch는 파티션에서 레코드를 조회합니다.
pub fn (mut a S3StorageAdapter) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	partition_key := '${topic}:${partition}'

	// 세그먼트 정보를 위해 S3 인덱스 조회
	index := a.get_partition_index(topic, partition)!

	if offset < index.earliest_offset {
		return error('Offset out of range (too old): ${offset} < ${index.earliest_offset}')
	}

	// 관련 세그먼트 찾기
	mut all_records := []domain.Record{}
	mut bytes_read := 0
	mut highest_offset_read := offset - 1

	// 1. 먼저 S3 세그먼트에서 읽기 시도
	for seg in index.log_segments {
		if seg.end_offset < offset {
			continue
		}
		if seg.start_offset > offset + i64(max_bytes / 100) { // 대략적인 추정
			break
		}

		// S3에서 세그먼트 조회
		// 최적화: 세그먼트 시작부터 읽는 경우 Range Request 사용
		mut data := []u8{}
		if offset == seg.start_offset && max_bytes > 0 {
			mut fetch_size := i64(max_bytes) * 2
			if fetch_size > seg.size_bytes {
				fetch_size = -1 // 전체 읽기
			}
			range_end := if fetch_size > 0 { fetch_size } else { -1 }
			data, _ = a.get_object(seg.key, 0, range_end) or { continue }
		} else {
			// 인덱스 없는 랜덤 접근: 전체 세그먼트 다운로드 필요
			data, _ = a.get_object(seg.key, -1, -1) or { continue }
		}

		stored_records := decode_stored_records(data)

		for rec in stored_records {
			if rec.offset >= offset && bytes_read < max_bytes {
				// StoredRecord를 domain.Record로 변환
				all_records << domain.Record{
					key:       rec.key
					value:     rec.value
					headers:   rec.headers
					timestamp: rec.timestamp
				}
				bytes_read += rec.value.len + rec.key.len
				if rec.offset > highest_offset_read {
					highest_offset_read = rec.offset
				}
			}
		}

		if bytes_read >= max_bytes {
			break
		}
	}

	// 2. 인메모리 버퍼에서도 읽기 (아직 S3에 플러시되지 않은 데이터)
	// 아직 영속화되지 않은 데이터에 대해 중요함
	if bytes_read < max_bytes {
		a.buffer_lock.lock()
		if tp_buffer := a.topic_partition_buffers[partition_key] {
			for rec in tp_buffer.records {
				// 요청된 오프셋 이상이고 아직 S3 세그먼트에서 읽지 않은 레코드 읽기
				if rec.offset >= offset && rec.offset > highest_offset_read
					&& bytes_read < max_bytes {
					all_records << domain.Record{
						key:       rec.key
						value:     rec.value
						headers:   rec.headers
						timestamp: rec.timestamp
					}
					bytes_read += rec.value.len + rec.key.len
					if rec.offset > highest_offset_read {
						highest_offset_read = rec.offset
					}
				}
			}
		}
		a.buffer_lock.unlock()
	}

	return domain.FetchResult{
		records:            all_records
		high_watermark:     index.high_watermark
		last_stable_offset: index.high_watermark
		log_start_offset:   index.earliest_offset
	}
}

/// delete_records는 지정된 오프셋 이전의 레코드를 삭제합니다.
pub fn (mut a S3StorageAdapter) delete_records(topic string, partition int, before_offset i64) ! {
	mut index := a.get_partition_index(topic, partition)!

	// 삭제할 세그먼트 찾기
	mut segments_to_delete := []string{}
	mut remaining_segments := []LogSegment{}

	for seg in index.log_segments {
		if seg.end_offset < before_offset {
			segments_to_delete << seg.key
		} else {
			remaining_segments << seg
		}
	}

	// S3에서 세그먼트 삭제
	for key in segments_to_delete {
		a.delete_object(key) or {}
	}

	// 인덱스 업데이트
	index.earliest_offset = before_offset
	index.log_segments = remaining_segments

	index_key := a.partition_index_key(topic, partition)
	a.put_object(index_key, json.encode(index).bytes())!
}

// ============================================================
// 파티션 정보 (Partition Info)
// ============================================================

/// get_partition_info는 파티션 정보를 조회합니다.
pub fn (mut a S3StorageAdapter) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	index := a.get_partition_index(topic, partition)!

	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: index.earliest_offset
		latest_offset:   index.high_watermark
		high_watermark:  index.high_watermark
	}
}

// ============================================================
// 컨슈머 그룹 작업 (Consumer Group Operations)
// ============================================================

/// save_group은 컨슈머 그룹을 저장합니다.
pub fn (mut a S3StorageAdapter) save_group(group domain.ConsumerGroup) ! {
	key := a.group_key(group.group_id)
	data := json.encode(group)
	a.put_object(key, data.bytes())!

	// 캐시 업데이트
	a.group_lock.@lock()
	a.group_cache[group.group_id] = CachedGroup{
		group:     group
		etag:      ''
		cached_at: time.now()
	}
	a.group_lock.unlock()
}

/// load_group은 컨슈머 그룹을 로드합니다.
pub fn (mut a S3StorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	// 캐시 확인
	a.group_lock.rlock()
	if cached := a.group_cache[group_id] {
		if time.since(cached.cached_at) < time.second * 30 {
			a.group_lock.runlock()
			return cached.group
		}
	}
	a.group_lock.runlock()

	key := a.group_key(group_id)
	data, etag := a.get_object(key, -1, -1)!

	group := json.decode(domain.ConsumerGroup, data.bytestr())!

	// 캐시 업데이트
	a.group_lock.@lock()
	a.group_cache[group_id] = CachedGroup{
		group:     group
		etag:      etag
		cached_at: time.now()
	}
	a.group_lock.unlock()

	return group
}

/// delete_group은 컨슈머 그룹을 삭제합니다.
pub fn (mut a S3StorageAdapter) delete_group(group_id string) ! {
	key := a.group_key(group_id)
	a.delete_object(key)!

	// 오프셋도 삭제
	offsets_prefix := '${g_s3_config.prefix}offsets/${group_id}/'
	a.delete_objects_with_prefix(offsets_prefix)!

	// 캐시에서 제거
	a.group_lock.@lock()
	a.group_cache.delete(group_id)
	a.group_lock.unlock()
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut a S3StorageAdapter) list_groups() ![]domain.GroupInfo {
	prefix := '${g_s3_config.prefix}groups/'
	objects := a.list_objects(prefix)!

	mut groups := []domain.GroupInfo{}
	mut group_ids := []string{}
	mut seen := map[string]bool{}

	// 첫 번째 패스: 고유한 그룹 ID 수집
	for obj in objects {
		if obj.key.ends_with('/state.json') {
			parts := obj.key.split('/')
			if parts.len >= 2 {
				group_id := parts[parts.len - 2]
				if group_id !in seen {
					seen[group_id] = true
					group_ids << group_id
				}
			}
		}
	}

	// 그룹 배치 로드 (더 나은 성능을 위해 병렬 조회)
	if group_ids.len <= 3 {
		// 작은 배치: 순차 로드
		for group_id in group_ids {
			if group := a.load_group(group_id) {
				groups << domain.GroupInfo{
					group_id:      group_id
					protocol_type: group.protocol_type
					state:         group.state.str()
				}
			}
		}
	} else {
		// 큰 배치: 채널을 사용한 병렬 로드
		// 채널은 옵셔널 타입을 지원하지 않으므로 domain.GroupInfo 직접 사용
		ch := chan domain.GroupInfo{cap: group_ids.len}

		for group_id in group_ids {
			spawn fn [mut a, group_id, ch] () {
				if group := a.load_group(group_id) {
					ch <- domain.GroupInfo{
						group_id:      group.group_id
						protocol_type: group.protocol_type
						state:         group.state.str()
					}
				} else {
					// 실패한 로드에 대해 빈 GroupInfo 전송 (필터링됨)
					ch <- domain.GroupInfo{
						group_id: ''
					}
				}
			}()
		}

		// 결과 수집
		for _ in 0 .. group_ids.len {
			info := <-ch
			if info.group_id.len > 0 {
				groups << info
			}
		}
	}

	return groups
}

// ============================================================
// 오프셋 작업 (Offset Operations)
// ============================================================

/// commit_offsets는 오프셋을 커밋합니다.
pub fn (mut a S3StorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut failed := []string{}
	mut succeeded := []domain.PartitionOffset{}

	// 각 오프셋을 개별적으로 커밋 시도
	for offset in offsets {
		key := a.offset_key(group_id, offset.topic, offset.partition)
		data := json.encode(offset)
		a.put_object(key, data.bytes()) or {
			failed << '${offset.topic}:${offset.partition}'
			eprintln('[WARN] Failed to commit offset for ${offset.topic}:${offset.partition}: ${err}')
			continue
		}
		succeeded << offset
	}

	// 성공적으로 커밋된 오프셋에 대해 로컬 캐시 업데이트
	if succeeded.len > 0 {
		a.offset_lock.@lock()
		if group_id !in a.offset_cache {
			a.offset_cache[group_id] = map[string]i64{}
		}
		for offset in succeeded {
			cache_key := '${offset.topic}:${offset.partition}'
			a.offset_cache[group_id][cache_key] = offset.offset
		}
		a.offset_lock.unlock()
	}

	// 모든 커밋이 실패한 경우에만 에러 반환
	if failed.len == offsets.len {
		return error('all offset commits failed: ${failed.join(', ')}')
	}

	// 부분 성공은 허용됨
	if failed.len > 0 {
		eprintln('[WARN] Partial offset commit success: ${succeeded.len} succeeded, ${failed.len} failed')
	}
}

/// fetch_offsets는 커밋된 오프셋을 조회합니다.
pub fn (mut a S3StorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut results := []domain.OffsetFetchResult{}

	for part in partitions {
		key := a.offset_key(group_id, part.topic, part.partition)

		// 캐시 먼저 시도
		a.offset_lock.rlock()
		cache_key := '${part.topic}:${part.partition}'
		cached_offset := if group_id in a.offset_cache {
			a.offset_cache[group_id][cache_key] or { i64(-1) }
		} else {
			i64(-1)
		}
		a.offset_lock.runlock()

		if cached_offset >= 0 {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     cached_offset
				metadata:   ''
				error_code: 0
			}
			continue
		}

		// S3에서 조회
		data, _ := a.get_object(key, -1, -1) or {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
			continue
		}

		offset_data := json.decode(domain.PartitionOffset, data.bytestr()) or {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
			continue
		}

		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset_data.offset
			metadata:   offset_data.metadata
			error_code: 0
		}
	}

	return results
}

// ============================================================
// 헬스 체크 (Health Check)
// ============================================================

/// health_check는 스토리지 상태를 확인합니다.
pub fn (mut a S3StorageAdapter) health_check() !port.HealthStatus {
	// 소수의 객체 목록 조회 시도
	_ := a.list_objects(g_s3_config.prefix) or { return .unhealthy }
	return .healthy
}

// ============================================================
// 멀티 브로커 지원 (Multi-Broker Support)
// ============================================================

/// get_storage_capability는 스토리지 기능 정보를 반환합니다.
pub fn (a &S3StorageAdapter) get_storage_capability() domain.StorageCapability {
	return s3_capability
}

/// get_cluster_metadata_port는 클러스터 메타데이터 인터페이스를 반환합니다.
/// S3는 멀티 브로커 모드를 지원합니다.
pub fn (mut a S3StorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	// S3 기반 클러스터 메타데이터 구현 반환
	// Note: &a를 전달하여 원본 adapter의 포인터를 사용
	return new_s3_cluster_metadata_adapter(&a)
}

// ============================================================
// S3 키 헬퍼 (S3 Key Helpers)
// ============================================================

/// topic_metadata_key는 토픽 메타데이터의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) topic_metadata_key(name string) string {
	return '${g_s3_config.prefix}topics/${name}/metadata.json'
}

/// partition_index_key는 파티션 인덱스의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) partition_index_key(topic string, partition int) string {
	return '${g_s3_config.prefix}topics/${topic}/partitions/${partition}/index.json'
}

/// log_segment_key는 로그 세그먼트의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) log_segment_key(topic string, partition int, start i64, end i64) string {
	return '${g_s3_config.prefix}topics/${topic}/partitions/${partition}/log-${start:016}-${end:016}.bin'
}

/// group_key는 컨슈머 그룹의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) group_key(group_id string) string {
	return '${g_s3_config.prefix}groups/${group_id}/state.json'
}

/// offset_key는 오프셋의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) offset_key(group_id string, topic string, partition int) string {
	return '${g_s3_config.prefix}offsets/${group_id}/${topic}:${partition}.json'
}

// ============================================================
// S3 작업 (S3 Operations) - 추상화, 실제 SDK로 구현 예정
// ============================================================

// 참고: S3 HTTP 작업 (get_object, put_object, delete_object, list_objects, sign_request 등)은
// s3_client.v로 이동되었습니다.

// 참고: PartitionIndex, LogSegment, get_partition_index는 partition_index.v로 이동되었습니다.

// 참고: StoredRecord와 TopicPartitionBuffer는 s3_record_codec.v와 buffer_manager.v에 정의되어 있습니다.

// 참고: async_flush_partition, flush_worker, flush_buffer_to_s3는 buffer_manager.v로 이동되었습니다.

// ============================================================
// 워커 관리 (Worker Management)
// ============================================================

/// start_workers는 플러시 및 컴팩션 워커를 시작합니다.
pub fn (mut a S3StorageAdapter) start_workers() {
	if a.compactor_running {
		return
	}
	a.compactor_running = true
	go a.flush_worker()
	go a.compaction_worker()
}

// 참고: compaction_worker, compact_all_partitions, compact_partition, merge_segments는
// compaction.v로 이동되었습니다.

// 참고: encode_stored_records와 decode_stored_records는 s3_record_codec.v로 이동되었습니다.

/// generate_topic_id는 토픽 이름으로부터 topic_id를 생성합니다.
fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}
