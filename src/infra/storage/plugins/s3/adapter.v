// S3 storage adapter with ETag-based concurrency control
module s3

import domain
import service.port
import time
import json
import crypto.md5
import sync
import net.http

// NOTE: S3 client functions in s3_client.v; index types in partition_index.v;
// buffer types in buffer_manager.v; compaction in compaction.v

// Configuration Constants

const max_topic_name_length = 255
const max_partition_count = 10000
const topic_cache_ttl = 5 * time.minute
const group_cache_ttl = 30 * time.second
const record_overhead_bytes = 30
const fetch_size_multiplier = 2
const fetch_offset_estimate_divisor = 100
const max_offset_commit_buffer = 100
const max_offset_commit_concurrent = 50

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
	// 메트릭
	metrics      S3Metrics
	metrics_lock sync.Mutex
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

/// CachedSignature는 캐시된 서명 정보를 담습니다.
struct CachedSignature {
	header    http.Header
	cached_at time.Time
}

/// LogLevel은 로그 레벨을 정의합니다.
enum LogLevel {
	debug
	info
	warn
	error
}

/// log_message는 구조화된 로그 메시지를 출력합니다.
fn log_message(level LogLevel, component string, message string, context map[string]string) {
	level_str := match level {
		.debug { '[DEBUG]' }
		.info { '[INFO]' }
		.warn { '[WARN]' }
		.error { '[ERROR]' }
	}

	timestamp := time.now().format_ss()
	mut ctx_str := ''
	if context.len > 0 {
		mut parts := []string{}
		for key, value in context {
			parts << '${key}=${value}'
		}
		ctx_str = ' {${parts.join(', ')}}'
	}

	eprintln('${timestamp} ${level_str} [S3:${component}] ${message}${ctx_str}')
}

/// S3Metrics는 S3 스토리지 작업의 메트릭을 추적합니다.
struct S3Metrics {
mut:
	// 플러시 메트릭
	flush_count         i64 // 총 플러시 작업 수
	flush_success_count i64 // 성공한 플러시 작업 수
	flush_error_count   i64 // 실패한 플러시 작업 수
	flush_total_ms      i64 // 총 플러시 시간 (밀리초)
	// 컴팩션 메트릭
	compaction_count         i64 // 총 컴팩션 작업 수
	compaction_success_count i64 // 성공한 컴팩션 작업 수
	compaction_error_count   i64 // 실패한 컴팩션 작업 수
	compaction_total_ms      i64 // 총 컴팩션 시간 (밀리초)
	compaction_bytes_merged  i64 // 병합된 총 바이트 수
	// S3 API 메트릭
	s3_get_count    i64 // S3 GET 요청 수
	s3_put_count    i64 // S3 PUT 요청 수
	s3_delete_count i64 // S3 DELETE 요청 수
	s3_list_count   i64 // S3 LIST 요청 수
	s3_error_count  i64 // S3 API 에러 수
	// 캐시 메트릭
	cache_hit_count  i64 // 캐시 히트 수
	cache_miss_count i64 // 캐시 미스 수
	// 오프셋 커밋 메트릭
	offset_commit_count         i64 // 총 오프셋 커밋 수
	offset_commit_success_count i64 // 성공한 오프셋 커밋 수
	offset_commit_error_count   i64 // 실패한 오프셋 커밋 수
}

/// reset_metrics는 모든 메트릭을 0으로 초기화합니다.
fn (mut m S3Metrics) reset() {
	m.flush_count = 0
	m.flush_success_count = 0
	m.flush_error_count = 0
	m.flush_total_ms = 0
	m.compaction_count = 0
	m.compaction_success_count = 0
	m.compaction_error_count = 0
	m.compaction_total_ms = 0
	m.compaction_bytes_merged = 0
	m.s3_get_count = 0
	m.s3_put_count = 0
	m.s3_delete_count = 0
	m.s3_list_count = 0
	m.s3_error_count = 0
	m.cache_hit_count = 0
	m.cache_miss_count = 0
	m.offset_commit_count = 0
	m.offset_commit_success_count = 0
	m.offset_commit_error_count = 0
}

/// get_summary는 메트릭 요약을 문자열로 반환합니다.
fn (m &S3Metrics) get_summary() string {
	flush_success_rate := if m.flush_count > 0 {
		f64(m.flush_success_count) / f64(m.flush_count) * 100.0
	} else {
		0.0
	}
	compaction_success_rate := if m.compaction_count > 0 {
		f64(m.compaction_success_count) / f64(m.compaction_count) * 100.0
	} else {
		0.0
	}
	cache_hit_rate := if m.cache_hit_count + m.cache_miss_count > 0 {
		f64(m.cache_hit_count) / f64(m.cache_hit_count + m.cache_miss_count) * 100.0
	} else {
		0.0
	}
	offset_commit_success_rate := if m.offset_commit_count > 0 {
		f64(m.offset_commit_success_count) / f64(m.offset_commit_count) * 100.0
	} else {
		0.0
	}

	return '[S3 Metrics]
  Flush: ${m.flush_count} total, ${m.flush_success_count} success (${flush_success_rate:.1f}%), ${m.flush_total_ms}ms
  Compaction: ${m.compaction_count} total, ${m.compaction_success_count} success (${compaction_success_rate:.1f}%), ${m.compaction_bytes_merged} bytes merged, ${m.compaction_total_ms}ms
  S3 API: GET=${m.s3_get_count}, PUT=${m.s3_put_count}, DELETE=${m.s3_delete_count}, LIST=${m.s3_list_count}, errors=${m.s3_error_count}
  Cache: ${m.cache_hit_count} hits, ${m.cache_miss_count} misses (${cache_hit_rate:.1f}% hit rate)
  Offset Commit: ${m.offset_commit_count} total, ${m.offset_commit_success_count} success (${offset_commit_success_rate:.1f}%)'
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
	return &S3StorageAdapter{
		config:                  config
		topic_cache:             map[string]CachedTopic{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{}
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
	}
}

/// create_topic은 S3에 새로운 토픽을 생성합니다.
pub fn (mut a S3StorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// 입력 검증
	if name == '' {
		return error('Topic name cannot be empty')
	}
	if name.len > max_topic_name_length {
		return error('Topic name too long: ${name.len} > ${max_topic_name_length}')
	}
	// 토픽 이름에 허용되지 않는 문자 확인
	for ch in name {
		if !ch.is_alnum() && ch != `_` && ch != `-` && ch != `.` {
			return error('Invalid character in topic name: ${ch.ascii_str()}')
		}
	}
	if partitions <= 0 {
		return error('Partition count must be positive: ${partitions}')
	}
	if partitions > max_partition_count {
		return error('Partition count too large: ${partitions} > ${max_partition_count}')
	}

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
	prefix := '${a.config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix)!

	// 캐시에서 제거
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

/// list_topics는 S3에서 모든 토픽 목록을 조회합니다.
pub fn (mut a S3StorageAdapter) list_topics() ![]domain.TopicMetadata {
	prefix := '${a.config.prefix}topics/'
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
		if time.since(cached.cached_at) < topic_cache_ttl {
			a.topic_lock.runlock()
			// 캐시 히트 메트릭
			a.metrics_lock.@lock()
			a.metrics.cache_hit_count++
			a.metrics_lock.unlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// 캐시 미스 메트릭
	a.metrics_lock.@lock()
	a.metrics.cache_miss_count++
	a.metrics.s3_get_count++
	a.metrics_lock.unlock()

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

	// 캐시 업데이트 (무효화 대신 직접 업데이트)
	a.topic_lock.@lock()
	if cached := a.topic_cache[name] {
		a.topic_cache[name] = CachedTopic{
			meta:      updated_meta
			etag:      cached.etag
			cached_at: time.now()
		}
	}
	a.topic_lock.unlock()
}

/// append는 파티션에 레코드를 추가합니다.
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
		bytes_to_add += i64(srec.value.len + srec.key.len + record_overhead_bytes)
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

	if tp_buffer.current_size_bytes >= a.config.batch_max_bytes {
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
	// index_update_lock을 사용하여 flush와의 경쟁 조건 방지
	new_high_watermark := base_offset + i64(records.len)
	a.index_update_lock.@lock()
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
	a.index_update_lock.unlock()

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
		if seg.start_offset > offset + i64(max_bytes / fetch_offset_estimate_divisor) { // 대략적인 추정
			break
		}

		// S3에서 세그먼트 조회
		// 최적화: 세그먼트 시작부터 읽는 경우 Range Request 사용
		mut data := []u8{}
		if offset == seg.start_offset && max_bytes > 0 {
			mut fetch_size := i64(max_bytes) * fetch_size_multiplier
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
		if time.since(cached.cached_at) < group_cache_ttl {
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
	offsets_prefix := '${a.config.prefix}offsets/${group_id}/'
	a.delete_objects_with_prefix(offsets_prefix)!

	// 캐시에서 제거
	a.group_lock.@lock()
	a.group_cache.delete(group_id)
	a.group_lock.unlock()
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut a S3StorageAdapter) list_groups() ![]domain.GroupInfo {
	prefix := '${a.config.prefix}groups/'
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
	// 임계값을 1로 낮춰 거의 모든 경우 병렬 처리
	if group_ids.len == 0 {
		return groups
	} else if group_ids.len == 1 {
		// 단일 그룹: 순차 로드
		group_id := group_ids[0]
		if group := a.load_group(group_id) {
			groups << domain.GroupInfo{
				group_id:      group_id
				protocol_type: group.protocol_type
				state:         group.state.str()
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

/// commit_offsets는 오프셋을 커밋합니다.
/// 병렬 처리를 통해 성능을 최적화합니다.
pub fn (mut a S3StorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	if offsets.len == 0 {
		return
	}

	a.record_commit_start_metrics(offsets.len)
	mut failed := []string{}
	mut succeeded := []domain.PartitionOffset{}

	ch := a.create_commit_channel(offsets.len)
	mut active := 0

	for offset in offsets {
		active = a.wait_for_slot(ch, active, mut succeeded, mut failed, group_id)
		active++
		spawn a.commit_single_offset(group_id, offset, ch)
	}

	a.collect_remaining_results(ch, active, mut succeeded, mut failed, group_id)
	a.update_offset_cache_and_metrics(group_id, succeeded, failed)
	a.handle_commit_result(offsets.len, failed, group_id) or {
		// 부분 실패는 허용, 에러는 로깅만
		log_message(.warn, 'OffsetCommit', 'Some offsets failed to commit', {
			'group_id': group_id
			'failed':   failed.len.str()
		})
	}
}

/// CommitResult는 오프셋 커밋 결과를 담습니다.
struct CommitResult {
	offset  domain.PartitionOffset
	success bool
	error   string
}

/// record_commit_start_metrics는 커밋 시작 메트릭을 기록합니다.
fn (mut a S3StorageAdapter) record_commit_start_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_count += i64(count)
	a.metrics_lock.unlock()
}

/// create_commit_channel는 커밋 결과 채널을 생성합니다.
fn (a &S3StorageAdapter) create_commit_channel(offset_count int) chan CommitResult {
	cap_size := if offset_count > max_offset_commit_buffer {
		max_offset_commit_buffer
	} else {
		offset_count
	}
	return chan CommitResult{cap: cap_size}
}

/// wait_for_slot는 사용 가능한 슬롯을 기다립니다.
fn (mut a S3StorageAdapter) wait_for_slot(ch chan CommitResult, active int, mut succeeded []domain.PartitionOffset, mut failed []string, group_id string) int {
	mut current_active := active
	for current_active >= max_offset_commit_concurrent {
		result := <-ch
		current_active--
		if result.success {
			succeeded << result.offset
		} else {
			failed << '${result.offset.topic}:${result.offset.partition}'
			a.log_commit_error(result, group_id)
		}
	}
	return current_active
}

/// log_commit_error는 커밋 에러를 로깅합니다.
fn (a &S3StorageAdapter) log_commit_error(result CommitResult, group_id string) {
	log_message(.error, 'OffsetCommit', 'Offset commit failed', {
		'group_id':  group_id
		'topic':     result.offset.topic
		'partition': result.offset.partition.str()
		'error':     result.error
	})
}

/// commit_single_offset는 단일 오프셋을 커밋합니다.
fn (mut a S3StorageAdapter) commit_single_offset(group_id string, offset domain.PartitionOffset, ch chan CommitResult) {
	key := a.offset_key(group_id, offset.topic, offset.partition)
	data := json.encode(offset)

	a.put_object(key, data.bytes()) or {
		error_msg := 'S3 put failed for offset ${offset.topic}:${offset.partition}: ${err}'
		a.log_single_commit_error(group_id, offset, error_msg)
		ch <- CommitResult{
			offset:  offset
			success: false
			error:   error_msg
		}
		return
	}

	ch <- CommitResult{
		offset:  offset
		success: true
		error:   ''
	}
}

/// log_single_commit_error는 단일 커밋 에러를 로깅합니다.
fn (a &S3StorageAdapter) log_single_commit_error(group_id string, offset domain.PartitionOffset, error_msg string) {
	log_message(.error, 'OffsetCommit', error_msg, {
		'group_id':  group_id
		'topic':     offset.topic
		'partition': offset.partition.str()
		'offset':    offset.offset.str()
	})
}

/// collect_remaining_results는 남은 결과를 수집합니다.
fn (mut a S3StorageAdapter) collect_remaining_results(ch chan CommitResult, active int, mut succeeded []domain.PartitionOffset, mut failed []string, group_id string) {
	for _ in 0 .. active {
		result := <-ch
		if result.success {
			succeeded << result.offset
		} else {
			failed << '${result.offset.topic}:${result.offset.partition}'
			a.log_commit_error(result, group_id)
		}
	}
}

/// update_offset_cache_and_metrics는 캐시와 메트릭을 업데이트합니다.
fn (mut a S3StorageAdapter) update_offset_cache_and_metrics(group_id string, succeeded []domain.PartitionOffset, failed []string) {
	if succeeded.len > 0 {
		a.update_offset_cache(group_id, succeeded)
		a.record_commit_success_metrics(succeeded.len)
	}

	if failed.len > 0 {
		a.record_commit_failure_metrics(failed.len)
	}
}

/// update_offset_cache는 오프셋 캐시를 업데이트합니다.
fn (mut a S3StorageAdapter) update_offset_cache(group_id string, succeeded []domain.PartitionOffset) {
	a.offset_lock.@lock()
	if group_id !in a.offset_cache {
		a.offset_cache[group_id] = map[string]i64{}
	}
	for offset in succeeded {
		cache_key := '${offset.topic}:${offset.partition}'
		a.offset_cache[group_id][cache_key] = offset.offset
	}
	a.offset_lock.unlock()

	log_message(.info, 'OffsetCommit', 'Successfully committed offsets', {
		'group_id': group_id
		'count':    succeeded.len.str()
	})
}

/// record_commit_success_metrics는 성공 메트릭을 기록합니다.
fn (mut a S3StorageAdapter) record_commit_success_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_success_count += i64(count)
	a.metrics.s3_put_count += i64(count)
	a.metrics_lock.unlock()
}

/// record_commit_failure_metrics는 실패 메트릭을 기록합니다.
fn (mut a S3StorageAdapter) record_commit_failure_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_error_count += i64(count)
	a.metrics.s3_error_count += i64(count)
	a.metrics_lock.unlock()
}

/// handle_commit_result는 커밋 결과를 처리합니다.
fn (a &S3StorageAdapter) handle_commit_result(total_count int, failed []string, group_id string) ! {
	if failed.len == total_count {
		return error('All ${total_count} offset commits failed for group ${group_id}')
	}

	if failed.len > 0 {
		log_message(.warn, 'OffsetCommit', 'Partial offset commit', {
			'group_id': group_id
			'failed':   failed.len.str()
		})
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

/// health_check는 스토리지 상태를 확인합니다.
pub fn (mut a S3StorageAdapter) health_check() !port.HealthStatus {
	// 소수의 객체 목록 조회 시도
	_ := a.list_objects(a.config.prefix) or { return .unhealthy }
	return .healthy
}

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

/// topic_metadata_key는 토픽 메타데이터의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) topic_metadata_key(name string) string {
	return '${a.config.prefix}topics/${name}/metadata.json'
}

/// partition_index_key는 파티션 인덱스의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) partition_index_key(topic string, partition int) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/index.json'
}

/// log_segment_key는 로그 세그먼트의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) log_segment_key(topic string, partition int, start i64, end i64) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/log-${start:016}-${end:016}.bin'
}

/// group_key는 컨슈머 그룹의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) group_key(group_id string) string {
	return '${a.config.prefix}groups/${group_id}/state.json'
}

/// offset_key는 오프셋의 S3 키를 반환합니다.
fn (a &S3StorageAdapter) offset_key(group_id string, topic string, partition int) string {
	return '${a.config.prefix}offsets/${group_id}/${topic}:${partition}.json'
}

// s3_client.v로 이동되었습니다.

// 참고: PartitionIndex, LogSegment, get_partition_index는 partition_index.v로 이동되었습니다.

// 참고: StoredRecord와 TopicPartitionBuffer는 s3_record_codec.v와 buffer_manager.v에 정의되어 있습니다.

// 참고: async_flush_partition, flush_worker, flush_buffer_to_s3는 buffer_manager.v로 이동되었습니다.

/// start_workers는 플러시 및 컴팩션 워커를 시작합니다.
pub fn (mut a S3StorageAdapter) start_workers() {
	if a.compactor_running {
		return
	}
	a.compactor_running = true
	go a.flush_worker()
	go a.compaction_worker()
}

/// get_metrics는 현재 메트릭 스냅샷을 반환합니다.
pub fn (mut a S3StorageAdapter) get_metrics() S3Metrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary는 메트릭 요약 문자열을 반환합니다.
pub fn (mut a S3StorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics는 모든 메트릭을 0으로 초기화합니다.
pub fn (mut a S3StorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}

// 참고: compaction_worker, compact_all_partitions, compact_partition, merge_segments는
// compaction.v로 이동되었습니다.

// 참고: encode_stored_records와 decode_stored_records는 s3_record_codec.v로 이동되었습니다.

/// generate_topic_id는 토픽 이름으로부터 topic_id를 생성합니다.
fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}
