// Infra Layer - 메모리 스토리지 어댑터
// 파티션 수준 락킹을 사용한 인메모리 스토리지 구현
// 테스트 및 단일 브로커 환경에 적합
module memory

import domain
import service.port
import sync
import time
import rand

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

	eprintln('${timestamp} ${level_str} [Memory:${component}] ${message}${ctx_str}')
}

/// MemoryMetrics는 메모리 스토리지 작업의 메트릭을 추적합니다.
struct MemoryMetrics {
mut:
	// 토픽 작업 메트릭
	topic_create_count i64
	topic_delete_count i64
	topic_lookup_count i64
	// 레코드 작업 메트릭
	append_count         i64
	append_record_count  i64
	append_bytes         i64
	fetch_count          i64
	fetch_record_count   i64
	delete_records_count i64
	// 오프셋 작업 메트릭
	offset_commit_count i64
	offset_fetch_count  i64
	// 그룹 작업 메트릭
	group_save_count   i64
	group_load_count   i64
	group_delete_count i64
	// 에러 메트릭
	error_count i64
}

/// reset_metrics는 모든 메트릭을 0으로 초기화합니다.
fn (mut m MemoryMetrics) reset() {
	m.topic_create_count = 0
	m.topic_delete_count = 0
	m.topic_lookup_count = 0
	m.append_count = 0
	m.append_record_count = 0
	m.append_bytes = 0
	m.fetch_count = 0
	m.fetch_record_count = 0
	m.delete_records_count = 0
	m.offset_commit_count = 0
	m.offset_fetch_count = 0
	m.group_save_count = 0
	m.group_load_count = 0
	m.group_delete_count = 0
	m.error_count = 0
}

/// get_summary는 메트릭 요약을 문자열로 반환합니다.
fn (m &MemoryMetrics) get_summary() string {
	return '[Memory Metrics]
  Topics: create=${m.topic_create_count}, delete=${m.topic_delete_count}, lookup=${m.topic_lookup_count}
  Records: append=${m.append_count} (${m.append_record_count} records, ${m.append_bytes} bytes), fetch=${m.fetch_count} (${m.fetch_record_count} records), delete=${m.delete_records_count}
  Offsets: commit=${m.offset_commit_count}, fetch=${m.offset_fetch_count}
  Groups: save=${m.group_save_count}, load=${m.group_load_count}, delete=${m.group_delete_count}
  Errors: ${m.error_count}'
}

/// MemoryStorageAdapter는 port.StoragePort를 구현합니다.
/// 메모리 기반 스토리지로 파티션별 락킹을 통해 동시성을 제어합니다.
pub struct MemoryStorageAdapter {
pub mut:
	config MemoryConfig
mut:
	topics         map[string]&TopicStore
	topic_id_index map[string]string // topic_id (hex) -> topic_name, O(1) 조회용
	groups         map[string]domain.ConsumerGroup
	offsets        map[string]map[string]i64
	global_lock    sync.RwMutex
	// 메트릭
	metrics      MemoryMetrics
	metrics_lock sync.Mutex
}

/// MemoryConfig는 메모리 스토리지 설정을 담습니다.
pub struct MemoryConfig {
pub:
	max_messages_per_partition int = 1000000   // 파티션당 최대 메시지 수 (기본 100만)
	max_bytes_per_partition    i64 = -1        // 파티션당 최대 바이트 (-1 = 무제한)
	retention_ms               i64 = 604800000 // 보존 기간 (기본 7일)
	// mmap 설정 (v0.33.0)
	use_mmap       bool // mmap 사용 여부 (파일 기반 영속성, 기본 false)
	mmap_dir       string = '/tmp/datacore' // mmap 파일 디렉토리
	segment_size   i64    = 1073741824      // 세그먼트 크기 (기본 1GB)
	sync_on_append bool // 매 append 시 sync 여부 (기본 false)
}

/// memory_capability는 메모리 어댑터의 스토리지 기능을 정의합니다.
pub const memory_capability = domain.StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

/// TopicStore는 토픽 데이터를 저장합니다.
struct TopicStore {
pub mut:
	metadata        domain.TopicMetadata
	config          domain.TopicConfig
	partitions      []&PartitionStore     // 인메모리 파티션 (use_mmap=false)
	mmap_partitions []&MmapPartitionStore // mmap 파티션 (use_mmap=true, v0.33.0)
	use_mmap        bool                  // mmap 모드 여부
	lock            sync.RwMutex
}

/// PartitionStore는 파티션 데이터를 저장하며 세밀한 락킹을 지원합니다.
struct PartitionStore {
mut:
	records        []domain.Record
	base_offset    i64
	high_watermark i64
	lock           sync.RwMutex
}

/// new_memory_adapter는 새로운 메모리 스토리지 어댑터를 생성합니다.
pub fn new_memory_adapter() &MemoryStorageAdapter {
	return new_memory_adapter_with_config(MemoryConfig{})
}

/// new_memory_adapter_with_config는 사용자 정의 설정으로 어댑터를 생성합니다.
pub fn new_memory_adapter_with_config(config MemoryConfig) &MemoryStorageAdapter {
	return &MemoryStorageAdapter{
		config:         config
		topics:         map[string]&TopicStore{}
		topic_id_index: map[string]string{}
		groups:         map[string]domain.ConsumerGroup{}
		offsets:        map[string]map[string]i64{}
	}
}

/// create_topic은 새로운 토픽을 생성합니다.
/// UUID v4 형식의 topic_id를 자동 생성합니다.
pub fn (mut a MemoryStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// 메트릭: 토픽 생성 시작
	a.metrics_lock.@lock()
	a.metrics.topic_create_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if name in a.topics {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		log_message(.error, 'Topic', 'Topic already exists', {
			'topic': name
		})
		return error('topic already exists')
	}

	// topic_id용 UUID v4 생성 - 배열 한 번에 초기화
	mut topic_id := []u8{len: 16, init: u8(rand.intn(256) or { 0 })}
	// UUID 버전 4 (랜덤) 설정
	topic_id[6] = (topic_id[6] & 0x0f) | 0x40
	topic_id[8] = (topic_id[8] & 0x3f) | 0x80

	// 파티션 스토어 생성 (mmap 모드에 따라 다르게)
	mut partition_stores := []&PartitionStore{}
	mut mmap_partition_stores := []&MmapPartitionStore{}

	if a.config.use_mmap {
		// mmap 모드: MmapPartitionStore 생성 (v0.33.0)
		for i in 0 .. partitions {
			mmap_config := MmapPartitionConfig{
				topic_name:    name
				partition:     i
				base_dir:      a.config.mmap_dir
				segment_size:  a.config.segment_size
				sync_on_write: a.config.sync_on_append
			}
			mmap_part := new_mmap_partition(mmap_config) or {
				// 메트릭: 에러
				a.metrics_lock.@lock()
				a.metrics.error_count++
				a.metrics_lock.unlock()
				log_message(.error, 'Topic', 'Failed to create mmap partition', {
					'topic':     name
					'partition': i.str()
					'error':     err.msg()
				})
				return error('failed to create mmap partition: ${err}')
			}
			mmap_partition_stores << mmap_part
		}
	} else {
		// 인메모리 모드: PartitionStore 생성
		partition_stores = []&PartitionStore{cap: partitions}
		for _ in 0 .. partitions {
			partition_stores << &PartitionStore{
				records:        []domain.Record{}
				base_offset:    0
				high_watermark: 0
			}
		}
	}

	metadata := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     name.starts_with('__')
	}

	a.topics[name] = &TopicStore{
		metadata:        metadata
		config:          config
		partitions:      partition_stores
		mmap_partitions: mmap_partition_stores
		use_mmap:        a.config.use_mmap
	}

	// topic_id -> name 매핑을 캐시에 저장 (O(1) 조회용)
	a.topic_id_index[topic_id.hex()] = name

	log_message(.info, 'Topic', 'Topic created', {
		'topic':      name
		'partitions': partitions.str()
		'use_mmap':   a.config.use_mmap.str()
	})

	return metadata
}

/// delete_topic은 토픽을 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_topic(name string) ! {
	// 메트릭: 토픽 삭제
	a.metrics_lock.@lock()
	a.metrics.topic_delete_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	topic := a.topics[name] or {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}

	// topic_id_index 캐시에서 제거
	a.topic_id_index.delete(topic.metadata.topic_id.hex())

	a.topics.delete(name)

	log_message(.info, 'Topic', 'Topic deleted', {
		'topic': name
	})
}

/// list_topics는 모든 토픽 목록을 반환합니다.
pub fn (mut a MemoryStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, topic in a.topics {
		result << topic.metadata
	}
	return result
}

/// get_topic은 토픽 메타데이터를 조회합니다.
pub fn (mut a MemoryStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	// 메트릭: 토픽 조회
	a.metrics_lock.@lock()
	a.metrics.topic_lookup_count++
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if topic := a.topics[name] {
		return topic.metadata
	}

	// 메트릭: 에러
	a.metrics_lock.@lock()
	a.metrics.error_count++
	a.metrics_lock.unlock()
	return error('topic not found')
}

/// get_topic_by_id는 topic_id로 토픽을 조회합니다.
/// O(1) 캐시 조회를 사용합니다.
pub fn (mut a MemoryStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	// topic_id_index 캐시를 사용한 O(1) 조회
	topic_id_hex := topic_id.hex()
	if topic_name := a.topic_id_index[topic_id_hex] {
		if topic := a.topics[topic_name] {
			return topic.metadata
		}
	}

	return error('topic not found')
}

/// add_partitions는 토픽에 파티션을 추가합니다.
pub fn (mut a MemoryStorageAdapter) add_partitions(name string, new_count int) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	mut topic := a.topics[name] or { return error('topic not found') }

	// mmap 모드에 따라 현재 파티션 수 확인
	current := if topic.use_mmap { topic.mmap_partitions.len } else { topic.partitions.len }
	if new_count <= current {
		return error('new partition count must be greater than current')
	}

	topic.lock.@lock()
	defer { topic.lock.unlock() }

	if topic.use_mmap {
		// mmap 모드: MmapPartitionStore 추가 (v0.33.0)
		for i in current .. new_count {
			mmap_config := MmapPartitionConfig{
				topic_name:    name
				partition:     i
				base_dir:      a.config.mmap_dir
				segment_size:  a.config.segment_size
				sync_on_write: a.config.sync_on_append
			}
			mmap_part := new_mmap_partition(mmap_config) or {
				return error('failed to create mmap partition: ${err}')
			}
			topic.mmap_partitions << mmap_part
		}
	} else {
		// 인메모리 모드
		for _ in current .. new_count {
			topic.partitions << &PartitionStore{
				records:        []domain.Record{}
				base_offset:    0
				high_watermark: 0
			}
		}
	}

	topic.metadata = domain.TopicMetadata{
		...topic.metadata
		partition_count: new_count
	}
}

/// append는 파티션에 레코드를 추가합니다.
/// 파티션 수준 락킹으로 동시성을 제어합니다.
pub fn (mut a MemoryStorageAdapter) append(topic_name string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks // 메모리는 이미 즉시 저장이므로 무시
	// 메트릭: append 시작
	a.metrics_lock.@lock()
	a.metrics.append_count++
	a.metrics.append_record_count += i64(records.len)
	a.metrics_lock.unlock()

	// 읽기 락으로 토픽 조회
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap 모드 분기 (v0.33.0)
	if topic.use_mmap {
		return a.append_mmap(topic, partition, records)
	}

	if partition < 0 || partition >= topic.partitions.len {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	// 특정 파티션만 쓰기 락
	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	base_offset := part.high_watermark
	now := time.now()

	// 바이트 수 계산
	mut bytes_written := i64(0)

	// 타임스탬프와 함께 레코드 추가
	for record in records {
		mut r := record
		if r.timestamp.unix() == 0 {
			r = domain.Record{
				...r
				timestamp: now
			}
		}
		part.records << r
		bytes_written += i64(r.key.len + r.value.len)
	}
	part.high_watermark += i64(records.len)

	// 메트릭: 바이트 수 추가
	a.metrics_lock.@lock()
	a.metrics.append_bytes += bytes_written
	a.metrics_lock.unlock()

	// 보존 정책 적용 (최대 메시지 수)
	if a.config.max_messages_per_partition > 0 {
		excess := part.records.len - a.config.max_messages_per_partition
		if excess > 0 {
			// 슬라이스를 사용하여 clone 없이 제자리에서 요소 이동
			part.records = part.records[excess..]
			part.base_offset += i64(excess)
			log_message(.debug, 'Append', 'Applied retention policy', {
				'topic':         topic_name
				'partition':     partition.str()
				'deleted_count': excess.str()
			})
		}
	}

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: part.base_offset
		record_count:     records.len
	}
}

/// append_mmap은 mmap 모드에서 레코드를 추가합니다. (v0.33.0)
fn (mut a MemoryStorageAdapter) append_mmap(topic &TopicStore, partition int, records []domain.Record) !domain.AppendResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	now := time.now()

	// 레코드를 바이트 배열로 변환
	mut record_bytes := [][]u8{cap: records.len}
	for record in records {
		// 간단한 직렬화: key_len(4) + key + value_len(4) + value
		mut data := []u8{}
		// key length
		key_len := record.key.len
		data << u8(key_len >> 24)
		data << u8(key_len >> 16)
		data << u8(key_len >> 8)
		data << u8(key_len)
		data << record.key
		// value length
		value_len := record.value.len
		data << u8(value_len >> 24)
		data << u8(value_len >> 16)
		data << u8(value_len >> 8)
		data << u8(value_len)
		data << record.value
		record_bytes << data
	}

	base_offset, written := mmap_part.append(record_bytes)!

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: mmap_part.get_base_offset()
		record_count:     written
	}
}

/// fetch는 파티션에서 레코드를 조회합니다.
pub fn (mut a MemoryStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	// 메트릭: fetch 시작
	a.metrics_lock.@lock()
	a.metrics.fetch_count++
	a.metrics_lock.unlock()

	// 읽기 락으로 토픽 조회
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap 모드 분기 (v0.33.0)
	if topic.use_mmap {
		return a.fetch_mmap(topic, partition, offset, max_bytes)
	}

	if partition < 0 || partition >= topic.partitions.len {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	// 파티션 읽기 락
	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	// 오프셋이 범위를 벗어나면 빈 결과 반환
	if offset < part.base_offset {
		return domain.FetchResult{
			records:            []
			first_offset:       part.base_offset
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	start_idx := int(offset - part.base_offset)
	if start_idx >= part.records.len {
		return domain.FetchResult{
			records:            []
			first_offset:       part.high_watermark
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	// max_bytes 기반으로 종료 인덱스 계산
	mut end_idx := start_idx
	mut total_bytes := 0
	max_fetch_bytes := if max_bytes <= 0 { 1048576 } else { max_bytes }

	for end_idx < part.records.len {
		record_size := part.records[end_idx].key.len + part.records[end_idx].value.len + 50
		if total_bytes + record_size > max_fetch_bytes && end_idx > start_idx {
			break
		}
		total_bytes += record_size
		end_idx++

		if end_idx - start_idx >= 1000 {
			break
		}
	}

	fetched_records := part.records[start_idx..end_idx]

	// 메트릭: fetch된 레코드 수
	a.metrics_lock.@lock()
	a.metrics.fetch_record_count += i64(fetched_records.len)
	a.metrics_lock.unlock()

	// 실제 반환되는 첫 번째 레코드의 오프셋 계산
	actual_first_offset := part.base_offset + i64(start_idx)

	return domain.FetchResult{
		records:            fetched_records
		first_offset:       actual_first_offset
		high_watermark:     part.high_watermark
		last_stable_offset: part.high_watermark
		log_start_offset:   part.base_offset
	}
}

/// fetch_mmap은 mmap 모드에서 레코드를 조회합니다. (v0.33.0)
fn (mut a MemoryStorageAdapter) fetch_mmap(topic &TopicStore, partition int, offset i64, max_bytes int) !domain.FetchResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	base_offset := mmap_part.get_base_offset()
	high_watermark := mmap_part.get_high_watermark()

	// 오프셋이 범위를 벗어나면 빈 결과 반환
	if offset < base_offset || offset >= high_watermark {
		return domain.FetchResult{
			records:            []
			first_offset:       if offset < base_offset { base_offset } else { high_watermark }
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// max_bytes 기반으로 최대 레코드 수 계산
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 100 } // 대략적인 레코드 크기 추정
	record_bytes := mmap_part.read(offset, max_records)!

	// 바이트 배열을 domain.Record로 변환
	mut records := []domain.Record{cap: record_bytes.len}
	for data in record_bytes {
		if data.len < 8 {
			continue
		}

		// key_len(4) + key + value_len(4) + value 역직렬화
		key_len := int(u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3]))
		if 4 + key_len + 4 > data.len {
			continue
		}

		key := data[4..4 + key_len].clone()
		value_start := 4 + key_len
		value_len := int(u32(data[value_start]) << 24 | u32(data[value_start + 1]) << 16 | u32(data[
			value_start + 2]) << 8 | u32(data[value_start + 3]))

		if value_start + 4 + value_len > data.len {
			continue
		}

		value := data[value_start + 4..value_start + 4 + value_len].clone()

		records << domain.Record{
			key:       key
			value:     value
			timestamp: time.now() // 실제로는 세그먼트에서 읽어야 함
		}
	}

	return domain.FetchResult{
		records:            records
		first_offset:       offset // 요청된 오프셋이 실제 반환되는 첫 번째 레코드의 오프셋
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

/// delete_records는 지정된 오프셋 이전의 레코드를 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	delete_count := int(before_offset - part.base_offset)
	if delete_count > 0 && delete_count <= part.records.len {
		// clone 대신 슬라이스 할당 사용
		part.records = part.records[delete_count..]
		part.base_offset = before_offset
	}
}

/// get_partition_info는 파티션 정보를 조회합니다.
pub fn (mut a MemoryStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap 모드 분기 (v0.33.0)
	if topic.use_mmap {
		if partition < 0 || partition >= topic.mmap_partitions.len {
			return error('partition out of range')
		}

		mmap_part := topic.mmap_partitions[partition]
		return domain.PartitionInfo{
			topic:           topic_name
			partition:       partition
			earliest_offset: mmap_part.get_base_offset()
			latest_offset:   mmap_part.get_high_watermark()
			high_watermark:  mmap_part.get_high_watermark()
		}
	}

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: part.base_offset
		latest_offset:   part.high_watermark
		high_watermark:  part.high_watermark
	}
}

/// save_group은 컨슈머 그룹을 저장합니다.
pub fn (mut a MemoryStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	// 메트릭: 그룹 저장
	a.metrics_lock.@lock()
	a.metrics.group_save_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.groups[group.group_id] = group
}

/// load_group은 컨슈머 그룹을 로드합니다.
pub fn (mut a MemoryStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	// 메트릭: 그룹 로드
	a.metrics_lock.@lock()
	a.metrics.group_load_count++
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if group := a.groups[group_id] {
		return group
	}

	// 메트릭: 에러
	a.metrics_lock.@lock()
	a.metrics.error_count++
	a.metrics_lock.unlock()
	return error('group not found')
}

/// delete_group은 컨슈머 그룹을 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_group(group_id string) ! {
	// 메트릭: 그룹 삭제
	a.metrics_lock.@lock()
	a.metrics.group_delete_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.groups {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('group not found')
	}
	a.groups.delete(group_id)
	a.offsets.delete(group_id)

	log_message(.info, 'Group', 'Group deleted', {
		'group_id': group_id
	})
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut a MemoryStorageAdapter) list_groups() ![]domain.GroupInfo {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.GroupInfo{}
	for _, group in a.groups {
		result << domain.GroupInfo{
			group_id:      group.group_id
			protocol_type: group.protocol_type
			state:         match group.state {
				.empty { 'Empty' }
				.preparing_rebalance { 'PreparingRebalance' }
				.completing_rebalance { 'CompletingRebalance' }
				.stable { 'Stable' }
				.dead { 'Dead' }
			}
		}
	}
	return result
}

/// commit_offsets는 오프셋을 커밋합니다.
pub fn (mut a MemoryStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	// 메트릭: 오프셋 커밋
	a.metrics_lock.@lock()
	a.metrics.offset_commit_count += i64(offsets.len)
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.offsets {
		a.offsets[group_id] = map[string]i64{}
	}

	for offset in offsets {
		key := '${offset.topic}:${offset.partition}'
		a.offsets[group_id][key] = offset.offset
	}

	log_message(.debug, 'Offset', 'Offsets committed', {
		'group_id': group_id
		'count':    offsets.len.str()
	})
}

/// fetch_offsets는 커밋된 오프셋을 조회합니다.
pub fn (mut a MemoryStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	// 메트릭: 오프셋 조회
	a.metrics_lock.@lock()
	a.metrics.offset_fetch_count += i64(partitions.len)
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut results := []domain.OffsetFetchResult{}

	if group_id !in a.offsets {
		for part in partitions {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return results
	}

	for part in partitions {
		key := '${part.topic}:${part.partition}'
		offset := a.offsets[group_id][key] or { -1 }
		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset
			metadata:   ''
			error_code: 0
		}
	}

	return results
}

/// health_check는 스토리지 상태를 확인합니다.
pub fn (mut a MemoryStorageAdapter) health_check() !port.HealthStatus {
	return .healthy
}

/// get_storage_capability는 스토리지 기능 정보를 반환합니다.
pub fn (a &MemoryStorageAdapter) get_storage_capability() domain.StorageCapability {
	return memory_capability
}

/// get_cluster_metadata_port는 클러스터 메타데이터 포트를 반환합니다.
/// 메모리 스토리지는 멀티 브로커를 지원하지 않으므로 none을 반환합니다.
pub fn (a &MemoryStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

/// StorageStats는 스토리지 통계를 제공합니다.
pub struct StorageStats {
pub:
	topic_count      int
	total_partitions int
	total_records    i64
	total_bytes      i64
	group_count      int
}

/// get_stats는 현재 스토리지 통계를 반환합니다.
pub fn (mut a MemoryStorageAdapter) get_stats() StorageStats {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut total_partitions := 0
	mut total_records := i64(0)
	mut total_bytes := i64(0)

	for _, topic in a.topics {
		total_partitions += topic.partitions.len

		for i in 0 .. topic.partitions.len {
			part := topic.partitions[i]
			total_records += part.high_watermark - part.base_offset
			// 참고: total_bytes는 레코드를 순회하지 않으면 대략적인 값
		}
	}

	return StorageStats{
		topic_count:      a.topics.len
		total_partitions: total_partitions
		total_records:    total_records
		total_bytes:      total_bytes
		group_count:      a.groups.len
	}
}

/// get_metrics는 현재 메트릭 스냅샷을 반환합니다.
pub fn (mut a MemoryStorageAdapter) get_metrics() MemoryMetrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary는 메트릭 요약 문자열을 반환합니다.
pub fn (mut a MemoryStorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics는 모든 메트릭을 0으로 초기화합니다.
pub fn (mut a MemoryStorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}

/// clear는 모든 데이터를 삭제합니다 (테스트용).
pub fn (mut a MemoryStorageAdapter) clear() {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.topics.clear()
	a.topic_id_index.clear()
	a.groups.clear()
	a.offsets.clear()
}
