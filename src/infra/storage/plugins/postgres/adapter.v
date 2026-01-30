// Infra Layer - PostgreSQL 스토리지 어댑터
// 트랜잭션 + 행 락을 사용한 PostgreSQL 기반 스토리지 구현
// 동시성 제어를 위해 PostgreSQL의 FOR UPDATE 락을 활용
module postgres

import db.pg
import domain
import service.port
import time
import rand
import sync
import encoding.hex

// ============================================================
// 로깅 (Logging)
// ============================================================

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

	eprintln('${timestamp} ${level_str} [Postgres:${component}] ${message}${ctx_str}')
}

// ============================================================
// 메트릭 (Metrics)
// ============================================================

/// PostgresMetrics는 PostgreSQL 스토리지 작업의 메트릭을 추적합니다.
struct PostgresMetrics {
mut:
	// 쿼리 메트릭
	query_count       i64
	query_error_count i64
	query_total_ms    i64
	// 토픽 작업 메트릭
	topic_create_count i64
	topic_delete_count i64
	topic_lookup_count i64
	// 레코드 작업 메트릭
	append_count        i64
	append_record_count i64
	fetch_count         i64
	fetch_record_count  i64
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
fn (mut m PostgresMetrics) reset() {
	m.query_count = 0
	m.query_error_count = 0
	m.query_total_ms = 0
	m.topic_create_count = 0
	m.topic_delete_count = 0
	m.topic_lookup_count = 0
	m.append_count = 0
	m.append_record_count = 0
	m.fetch_count = 0
	m.fetch_record_count = 0
	m.offset_commit_count = 0
	m.offset_fetch_count = 0
	m.group_save_count = 0
	m.group_load_count = 0
	m.group_delete_count = 0
	m.error_count = 0
}

/// get_summary는 메트릭 요약을 문자열로 반환합니다.
fn (m &PostgresMetrics) get_summary() string {
	return '[Postgres Metrics]
  Queries: ${m.query_count} total, ${m.query_error_count} errors, ${m.query_total_ms}ms
  Topics: create=${m.topic_create_count}, delete=${m.topic_delete_count}, lookup=${m.topic_lookup_count}
  Records: append=${m.append_count} (${m.append_record_count} records), fetch=${m.fetch_count} (${m.fetch_record_count} records)
  Offsets: commit=${m.offset_commit_count}, fetch=${m.offset_fetch_count}
  Groups: save=${m.group_save_count}, load=${m.group_load_count}, delete=${m.group_delete_count}
  Errors: ${m.error_count}'
}

/// PostgresStorageAdapter는 port.StoragePort를 구현합니다.
/// PostgreSQL을 사용하여 토픽, 레코드, 컨슈머 그룹을 저장합니다.
/// 트랜잭션과 행 락(FOR UPDATE)을 사용하여 동시성을 제어합니다.
pub struct PostgresStorageAdapter {
pub mut:
	config PostgresConfig
mut:
	pool             &pg.ConnectionPool // PostgreSQL 커넥션 풀
	cluster_metadata &PostgresClusterMetadataPort = unsafe { nil } // 클러스터 메타데이터 포트
	topic_cache      map[string]domain.TopicMetadata // topic_name -> metadata 캐시
	topic_id_idx     map[string]string               // topic_id (hex) -> topic_name 인덱스
	cache_lock       sync.RwMutex                    // 캐시 동시성 제어용 락
	initialized      bool // 초기화 완료 여부
	// 메트릭
	metrics      PostgresMetrics
	metrics_lock sync.Mutex
}

/// PostgresConfig는 PostgreSQL 스토리지 설정을 담습니다.
pub struct PostgresConfig {
pub:
	host      string = 'localhost' // 호스트 주소
	port      int    = 5432        // 포트 번호
	user      string = 'datacore'  // 사용자 이름
	password  string // 비밀번호
	database  string = 'datacore' // 데이터베이스 이름
	pool_size int    = 10         // 커넥션 풀 크기
	sslmode   string = 'disable'  // SSL 모드 (disable, allow, prefer, require, verify-ca, verify-full)
}

/// postgres_capability는 PostgreSQL 어댑터의 스토리지 기능을 정의합니다.
pub const postgres_capability = domain.StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// new_postgres_adapter는 새로운 PostgreSQL 스토리지 어댑터를 생성합니다.
/// 커넥션 풀을 초기화하고, 스키마를 생성하며, 토픽 캐시를 로드합니다.
pub fn new_postgres_adapter(config PostgresConfig) !&PostgresStorageAdapter {
	// PostgreSQL 연결 문자열 생성 (sslmode 포함)
	conninfo := 'host=${config.host} port=${config.port} user=${config.user} password=${config.password} dbname=${config.database} sslmode=${config.sslmode}'

	// 연결 문자열을 사용하여 단일 연결 테스트
	test_conn := pg.connect_with_conninfo(conninfo)!
	test_conn.close() or {}

	// pg.Config 생성 (풀 생성용)
	pg_config := pg.Config{
		host:     config.host
		port:     config.port
		user:     config.user
		password: config.password
		dbname:   config.database
	}

	pool := pg.new_connection_pool(pg_config, config.pool_size)!

	mut adapter := &PostgresStorageAdapter{
		config:           config
		pool:             &pool
		cluster_metadata: unsafe { nil }
		topic_cache:      map[string]domain.TopicMetadata{}
		topic_id_idx:     map[string]string{}
		initialized:      false
	}

	adapter.init_schema()!
	adapter.load_topic_cache()!

	// 멀티 브로커 지원을 위한 클러스터 메타데이터 포트 초기화
	cluster_id := 'datacore-cluster' // TODO: 설정 가능하게 변경
	adapter.cluster_metadata = new_cluster_metadata_port(adapter.pool, cluster_id)!

	adapter.initialized = true

	return adapter
}

/// get_row_str는 행 값에서 안전하게 문자열을 가져오는 헬퍼 함수입니다.
/// 인덱스가 범위를 벗어나거나 값이 none이면 기본값을 반환합니다.
fn get_row_str(row &pg.Row, idx int, default_val string) string {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	return val_opt or { default_val }
}

/// get_row_int는 행 값에서 안전하게 정수를 가져오는 헬퍼 함수입니다.
/// 인덱스가 범위를 벗어나거나 값이 none이면 기본값을 반환합니다.
fn get_row_int(row &pg.Row, idx int, default_val int) int {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	val := val_opt or { return default_val }
	return val.int()
}

/// get_row_i64는 행 값에서 안전하게 i64를 가져오는 헬퍼 함수입니다.
/// 인덱스가 범위를 벗어나거나 값이 none이면 기본값을 반환합니다.
fn get_row_i64(row &pg.Row, idx int, default_val i64) i64 {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	val := val_opt or { return default_val }
	return val.i64()
}

/// build_batch_insert_query는 배치 INSERT 쿼리를 생성하는 헬퍼 함수입니다.
/// 파라미터 개수와 행 개수를 받아서 VALUES 절을 생성합니다.
fn build_batch_insert_query(table string, columns []string, params_per_row int, row_count int) string {
	mut values_parts := []string{}
	for i in 0 .. row_count {
		param_idx := i * params_per_row + 1
		mut placeholders := []string{}
		for j in 0 .. params_per_row {
			placeholders << '\$${param_idx + j}'
		}
		values_parts << '(${placeholders.join(', ')})'
	}
	values_clause := values_parts.join(', ')
	columns_clause := columns.join(', ')
	return 'INSERT INTO ${table} (${columns_clause}) VALUES ${values_clause}'
}

/// build_record_insert_params는 레코드 배치 INSERT를 위한 파라미터 배열을 생성합니다.
fn (a &PostgresStorageAdapter) build_record_insert_params(topic_name string, partition int, start_offset i64, records []domain.Record, default_time time.Time) []string {
	mut all_params := []string{}
	for i, record in records {
		ts := if record.timestamp.unix() == 0 { default_time } else { record.timestamp }
		key_hex := if record.key.len > 0 { '\\x${record.key.hex()}' } else { '' }
		value_hex := if record.value.len > 0 { '\\x${record.value.hex()}' } else { '' }
		current_offset := start_offset + i64(i)

		all_params << topic_name
		all_params << partition.str()
		all_params << current_offset.str()
		all_params << key_hex
		all_params << value_hex
		all_params << ts.format_rfc3339()
	}
	return all_params
}

/// build_partition_metadata_params는 파티션 메타데이터 배치 INSERT를 위한 파라미터 배열을 생성합니다.
fn (a &PostgresStorageAdapter) build_partition_metadata_params(topic_name string, partition_count int) []string {
	mut all_params := []string{}
	for p in 0 .. partition_count {
		all_params << topic_name
		all_params << p.str()
		all_params << '0'
		all_params << '0'
	}
	return all_params
}

/// build_partition_metadata_range_params는 특정 범위의 파티션 메타데이터 배치 INSERT를 위한 파라미터 배열을 생성합니다.
fn (a &PostgresStorageAdapter) build_partition_metadata_range_params(topic_name string, start_partition int, end_partition int) []string {
	mut all_params := []string{}
	for p in start_partition .. end_partition {
		all_params << topic_name
		all_params << p.str()
		all_params << '0'
		all_params << '0'
	}
	return all_params
}

/// build_offset_commit_params는 오프셋 커밋 배치 UPSERT를 위한 파라미터 배열을 생성합니다.
fn (a &PostgresStorageAdapter) build_offset_commit_params(group_id string, offsets []domain.PartitionOffset) []string {
	mut all_params := []string{}
	for offset in offsets {
		all_params << group_id
		all_params << offset.topic
		all_params << offset.partition.str()
		all_params << offset.offset.str()
		all_params << offset.metadata
	}
	return all_params
}

/// build_offset_upsert_query는 오프셋 커밋 배치 UPSERT 쿼리를 생성합니다.
fn (a &PostgresStorageAdapter) build_offset_upsert_query(offset_count int) string {
	mut values_parts := []string{}
	for i in 0 .. offset_count {
		param_idx := i * 5 + 1
		values_parts << '(\$${param_idx}, \$${param_idx + 1}, \$${param_idx + 2}, \$${param_idx + 3}, \$${
			param_idx + 4})'
	}
	values_clause := values_parts.join(', ')
	return 'INSERT INTO committed_offsets (group_id, topic_name, partition_id, committed_offset, metadata) VALUES ${values_clause} ON CONFLICT (group_id, topic_name, partition_id) DO UPDATE SET committed_offset = EXCLUDED.committed_offset, metadata = EXCLUDED.metadata, committed_at = NOW()'
}

/// decode_record_rows는 PostgreSQL 쿼리 결과 행을 domain.Record 배열로 디코딩합니다.
fn (a &PostgresStorageAdapter) decode_record_rows(rows []pg.Row) []domain.Record {
	mut records := []domain.Record{}
	for row in rows {
		key_str := get_row_str(&row, 1, '')
		value_str := get_row_str(&row, 2, '')
		ts_str := get_row_str(&row, 3, '')

		// hex에서 키 디코딩
		mut key := []u8{}
		if key_str.len > 0 && key_str.starts_with('\\x') {
			key = hex.decode(key_str[2..]) or { []u8{} }
		}

		// hex에서 값 디코딩
		mut value := []u8{}
		if value_str.len > 0 && value_str.starts_with('\\x') {
			value = hex.decode(value_str[2..]) or { []u8{} }
		}

		ts := time.parse_rfc3339(ts_str) or { time.now() }

		records << domain.Record{
			key:       key
			value:     value
			timestamp: ts
			headers:   map[string][]u8{}
		}
	}
	return records
}

/// init_schema는 데이터베이스 스키마를 초기화합니다.
/// topics, records, partition_metadata, consumer_groups, committed_offsets 테이블을 생성합니다.
/// 이미 존재하는 테이블은 건너뜁니다 (IF NOT EXISTS).
fn (mut a PostgresStorageAdapter) init_schema() ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// 토픽 테이블
	db.exec("
		CREATE TABLE IF NOT EXISTS topics (
			name VARCHAR(255) PRIMARY KEY,
			topic_id BYTEA NOT NULL UNIQUE,
			partition_count INT NOT NULL,
			is_internal BOOLEAN DEFAULT FALSE,
			config JSONB DEFAULT '{}',
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	")!

	// 파티셔닝 지원이 있는 레코드 테이블
	db.exec("
		CREATE TABLE IF NOT EXISTS records (
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			offset_id BIGINT NOT NULL,
			record_key BYTEA,
			record_value BYTEA,
			headers JSONB DEFAULT '[]',
			timestamp TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (topic_name, partition_id, offset_id)
		)
	")!

	// 파티션 메타데이터 테이블
	db.exec('
		CREATE TABLE IF NOT EXISTS partition_metadata (
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			base_offset BIGINT DEFAULT 0,
			high_watermark BIGINT DEFAULT 0,
			PRIMARY KEY (topic_name, partition_id)
		)
	')!

	// 컨슈머 그룹 테이블
	db.exec("
		CREATE TABLE IF NOT EXISTS consumer_groups (
			group_id VARCHAR(255) PRIMARY KEY,
			protocol_type VARCHAR(50),
			state VARCHAR(50) NOT NULL,
			generation_id INT DEFAULT 0,
			leader VARCHAR(255),
			protocol VARCHAR(255),
			members JSONB DEFAULT '[]',
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	")!

	// 커밋된 오프셋 테이블
	db.exec("
		CREATE TABLE IF NOT EXISTS committed_offsets (
			group_id VARCHAR(255) NOT NULL,
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			committed_offset BIGINT NOT NULL,
			metadata VARCHAR(255) DEFAULT '',
			committed_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (group_id, topic_name, partition_id)
		)
	")!

	// 인덱스 생성
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_topic_partition ON records(topic_name, partition_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_offset ON records(topic_name, partition_id, offset_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_committed_offsets_group ON committed_offsets(group_id)')!
}

/// load_topic_cache는 모든 토픽을 메모리 캐시에 로드합니다.
/// 시작 시 한 번 호출되어 토픽 조회 성능을 최적화합니다.
fn (mut a PostgresStorageAdapter) load_topic_cache() ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec('SELECT name, topic_id, partition_count, is_internal FROM topics')!

	a.cache_lock.@lock()
	defer { a.cache_lock.unlock() }

	for row in rows {
		name := get_row_str(&row, 0, '')
		if name == '' {
			continue
		}
		topic_id_str := get_row_str(&row, 1, '')
		partition_count := get_row_int(&row, 2, 0)
		is_internal_str := get_row_str(&row, 3, 'f')
		is_internal := is_internal_str == 't' || is_internal_str == 'true'

		// topic_id를 hex에서 디코딩 (\x 접두사 제거)
		clean_id := topic_id_str.replace('\\x', '')
		topic_id := hex.decode(clean_id) or { []u8{} }

		metadata := domain.TopicMetadata{
			name:            name
			topic_id:        topic_id
			partition_count: partition_count
			is_internal:     is_internal
			config:          map[string]string{}
		}

		a.topic_cache[name] = metadata
		a.topic_id_idx[topic_id.hex()] = name
	}
}

// ============================================================
// 토픽 작업 (Topic Operations)
// ============================================================

/// create_topic은 새로운 토픽을 생성합니다.
/// UUID v4 형식의 topic_id를 자동 생성합니다.
pub fn (mut a PostgresStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	start_time := time.now()

	// 메트릭: 토픽 생성 시작
	a.metrics_lock.@lock()
	a.metrics.topic_create_count++
	a.metrics_lock.unlock()

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// topic_id용 UUID 생성
	mut topic_id := []u8{len: 16}
	for i in 0 .. 16 {
		topic_id[i] = u8(rand.intn(256) or { 0 })
	}
	// UUID 버전 4 (랜덤) 설정
	topic_id[6] = (topic_id[6] & 0x0f) | 0x40
	topic_id[8] = (topic_id[8] & 0x3f) | 0x80

	is_internal := name.starts_with('__')

	// 트랜잭션 시작
	db.begin()!

	// 토픽 삽입
	db.exec_param_many('
		INSERT INTO topics (name, topic_id, partition_count, is_internal)
		VALUES (\$1, \$2, \$3, \$4)
	',
		[name, '\\x${topic_id.hex()}', partitions.str(), is_internal.str()])!

	// 파티션 메타데이터 항목 생성 (배치 INSERT)
	if partitions > 0 {
		all_params := a.build_partition_metadata_params(name, partitions)
		query := build_batch_insert_query('partition_metadata', ['topic_name', 'partition_id',
			'base_offset', 'high_watermark'], 4, partitions)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!

	metadata := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     is_internal
	}

	// 캐시 업데이트
	a.cache_lock.@lock()
	a.topic_cache[name] = metadata
	a.topic_id_idx[topic_id.hex()] = name
	a.cache_lock.unlock()

	// 메트릭: 쿼리 시간 기록
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

	log_message(.info, 'Topic', 'Topic created', {
		'name':       name
		'partitions': partitions.str()
	})

	return metadata
}

/// delete_topic은 토픽을 삭제합니다.
pub fn (mut a PostgresStorageAdapter) delete_topic(name string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// 레코드 삭제
	db.exec_param('DELETE FROM records WHERE topic_name = $1', name)!
	// 파티션 메타데이터 삭제
	db.exec_param('DELETE FROM partition_metadata WHERE topic_name = $1', name)!
	// 커밋된 오프셋 삭제
	db.exec_param('DELETE FROM committed_offsets WHERE topic_name = $1', name)!
	// 토픽 삭제
	db.exec_param('DELETE FROM topics WHERE name = $1', name)!

	db.commit()!

	// 캐시 업데이트
	a.cache_lock.@lock()
	if topic := a.topic_cache[name] {
		a.topic_id_idx.delete(topic.topic_id.hex())
	}
	a.topic_cache.delete(name)
	a.cache_lock.unlock()
}

/// list_topics는 모든 토픽 목록을 반환합니다.
pub fn (mut a PostgresStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, metadata in a.topic_cache {
		result << metadata
	}
	return result
}

/// get_topic은 토픽 메타데이터를 조회합니다.
pub fn (mut a PostgresStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	if metadata := a.topic_cache[name] {
		return metadata
	}
	return error('topic not found')
}

/// get_topic_by_id는 topic_id로 토픽을 조회합니다.
pub fn (mut a PostgresStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	topic_id_hex := topic_id.hex()
	if topic_name := a.topic_id_idx[topic_id_hex] {
		if metadata := a.topic_cache[topic_name] {
			return metadata
		}
	}
	return error('topic not found')
}

/// add_partitions는 토픽에 파티션을 추가합니다.
pub fn (mut a PostgresStorageAdapter) add_partitions(name string, new_count int) ! {
	a.cache_lock.rlock()
	topic := a.topic_cache[name] or {
		a.cache_lock.runlock()
		return error('topic not found')
	}
	current := topic.partition_count
	a.cache_lock.runlock()

	if new_count <= current {
		return error('new partition count must be greater than current')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// 토픽 파티션 수 업데이트
	db.exec_param_many('UPDATE topics SET partition_count = $1, updated_at = NOW() WHERE name = $2',
		[new_count.str(), name])!

	// 새 파티션 메타데이터 항목 생성 (배치 INSERT)
	new_partition_count := new_count - current
	if new_partition_count > 0 {
		all_params := a.build_partition_metadata_range_params(name, current, new_count)
		query := build_batch_insert_query('partition_metadata', ['topic_name', 'partition_id',
			'base_offset', 'high_watermark'], 4, new_partition_count)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!

	// 캐시 업데이트
	a.cache_lock.@lock()
	if mut metadata := a.topic_cache[name] {
		a.topic_cache[name] = domain.TopicMetadata{
			...metadata
			partition_count: new_count
		}
	}
	a.cache_lock.unlock()
}

// ============================================================
// 레코드 작업 (Record Operations)
// ============================================================

/// append는 파티션에 레코드를 추가합니다.
/// 행 락(FOR UPDATE)을 사용하여 동시성을 제어합니다.
pub fn (mut a PostgresStorageAdapter) append(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
	start_time := time.now()

	// 메트릭: append 시작
	a.metrics_lock.@lock()
	a.metrics.append_count++
	a.metrics.append_record_count += i64(records.len)
	a.metrics_lock.unlock()

	// 토픽 존재 확인
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// 업데이트를 위해 파티션 행 락 (행 락)
	rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
		FOR UPDATE
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		db.rollback()!
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&rows[0], 0, 0)
	mut high_watermark := get_row_i64(&rows[0], 1, 0)

	now := time.now()
	start_offset := high_watermark

	// 배치 INSERT를 위한 레코드 삽입
	if records.len > 0 {
		all_params := a.build_record_insert_params(topic_name, partition, high_watermark,
			records, now)
		query := build_batch_insert_query('records', ['topic_name', 'partition_id', 'offset_id',
			'record_key', 'record_value', 'timestamp'], 6, records.len)
		db.exec_param_many(query, all_params)!

		high_watermark += i64(records.len)
	}

	// high watermark 업데이트
	db.exec_param_many('
		UPDATE partition_metadata SET high_watermark = \$1 WHERE topic_name = \$2 AND partition_id = \$3
	',
		[high_watermark.str(), topic_name, partition.str()])!

	db.commit()!

	// 메트릭: 쿼리 시간 기록
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

	return domain.AppendResult{
		base_offset:      start_offset
		log_append_time:  now.unix()
		log_start_offset: base_offset
		record_count:     records.len
	}
}

/// fetch는 파티션에서 레코드를 조회합니다.
pub fn (mut a PostgresStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	start_time := time.now()

	// 메트릭: fetch 시작
	a.metrics_lock.@lock()
	a.metrics.fetch_count++
	a.metrics_lock.unlock()

	// 토픽 존재 확인
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// 파티션 메타데이터 조회
	meta_rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if meta_rows.len == 0 {
		// 메트릭: 에러
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&meta_rows[0], 0, 0)
	high_watermark := get_row_i64(&meta_rows[0], 1, 0)

	// 오프셋이 base 이전이면 빈 결과 반환
	if offset < base_offset {
		return domain.FetchResult{
			records:            []
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// max_bytes 기반으로 limit 계산 (대략적 추정: 레코드당 1KB)
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 1024 }
	limit := if max_records > 1000 { 1000 } else { max_records }

	// 레코드 조회
	rows := db.exec_param_many('
		SELECT offset_id, record_key, record_value, timestamp FROM records
		WHERE topic_name = \$1 AND partition_id = \$2 AND offset_id >= \$3
		ORDER BY offset_id ASC
		LIMIT \$4
	',
		[topic_name, partition.str(), offset.str(), limit.str()])!

	// 레코드 디코딩
	fetched_records := a.decode_record_rows(rows)

	// 메트릭: fetch된 레코드 수 및 쿼리 시간
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.fetch_record_count += i64(fetched_records.len)
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

	return domain.FetchResult{
		records:            fetched_records
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

/// delete_records는 지정된 오프셋 이전의 레코드를 삭제합니다.
pub fn (mut a PostgresStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// 오프셋 이전의 레코드 삭제
	db.exec_param_many('
		DELETE FROM records WHERE topic_name = \$1 AND partition_id = \$2 AND offset_id < \$3
	',
		[topic_name, partition.str(), before_offset.str()])!

	// base offset 업데이트
	db.exec_param_many('
		UPDATE partition_metadata SET base_offset = \$1
		WHERE topic_name = \$2 AND partition_id = \$3 AND base_offset < \$1
	',
		[before_offset.str(), topic_name, partition.str()])!

	db.commit()!
}

// ============================================================
// 오프셋 작업 (Offset Operations)
// ============================================================

/// get_partition_info는 파티션 정보를 조회합니다.
pub fn (mut a PostgresStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&rows[0], 0, 0)
	high_watermark := get_row_i64(&rows[0], 1, 0)

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: base_offset
		latest_offset:   high_watermark
		high_watermark:  high_watermark
	}
}

// ============================================================
// 컨슈머 그룹 작업 (Consumer Group Operations)
// ============================================================

/// save_group은 컨슈머 그룹을 저장합니다.
pub fn (mut a PostgresStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	state_str := match group.state {
		.empty { 'empty' }
		.preparing_rebalance { 'preparing_rebalance' }
		.completing_rebalance { 'completing_rebalance' }
		.stable { 'stable' }
		.dead { 'dead' }
	}

	// 그룹 upsert
	db.exec_param_many('
		INSERT INTO consumer_groups (group_id, protocol_type, state, generation_id, leader, protocol)
		VALUES (\$1, \$2, \$3, \$4, \$5, \$6)
		ON CONFLICT (group_id) DO UPDATE SET
			protocol_type = EXCLUDED.protocol_type,
			state = EXCLUDED.state,
			generation_id = EXCLUDED.generation_id,
			leader = EXCLUDED.leader,
			protocol = EXCLUDED.protocol,
			updated_at = NOW()
	',
		[
		group.group_id,
		group.protocol_type,
		state_str,
		group.generation_id.str(),
		group.leader,
		group.protocol,
	])!
}

/// load_group은 컨슈머 그룹을 로드합니다.
pub fn (mut a PostgresStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec_param('
		SELECT group_id, protocol_type, state, generation_id, leader, protocol
		FROM consumer_groups WHERE group_id = \$1
	',
		group_id)!

	if rows.len == 0 {
		return error('group not found')
	}

	row := rows[0]
	state_str := get_row_str(&row, 2, 'empty')
	state := match state_str {
		'preparing_rebalance' { domain.GroupState.preparing_rebalance }
		'completing_rebalance' { domain.GroupState.completing_rebalance }
		'stable' { domain.GroupState.stable }
		'dead' { domain.GroupState.dead }
		else { domain.GroupState.empty }
	}

	return domain.ConsumerGroup{
		group_id:      get_row_str(&row, 0, '')
		protocol_type: get_row_str(&row, 1, '')
		state:         state
		generation_id: get_row_int(&row, 3, 0)
		leader:        get_row_str(&row, 4, '')
		protocol:      get_row_str(&row, 5, '')
		members:       []domain.GroupMember{}
	}
}

/// delete_group은 컨슈머 그룹을 삭제합니다.
pub fn (mut a PostgresStorageAdapter) delete_group(group_id string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!
	db.exec_param('DELETE FROM committed_offsets WHERE group_id = $1', group_id)!
	db.exec_param('DELETE FROM consumer_groups WHERE group_id = $1', group_id)!
	db.commit()!
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut a PostgresStorageAdapter) list_groups() ![]domain.GroupInfo {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec('SELECT group_id, protocol_type, state FROM consumer_groups')!

	mut result := []domain.GroupInfo{}
	for row in rows {
		state_str := get_row_str(&row, 2, 'Empty')
		state := match state_str {
			'preparing_rebalance' { 'PreparingRebalance' }
			'completing_rebalance' { 'CompletingRebalance' }
			'stable' { 'Stable' }
			'dead' { 'Dead' }
			else { 'Empty' }
		}

		result << domain.GroupInfo{
			group_id:      get_row_str(&row, 0, '')
			protocol_type: get_row_str(&row, 1, '')
			state:         state
		}
	}
	return result
}

// ============================================================
// 오프셋 커밋/조회 (Offset Commit/Fetch)
// ============================================================

/// commit_offsets는 오프셋을 커밋합니다.
pub fn (mut a PostgresStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// 배치 UPSERT 수행
	if offsets.len > 0 {
		all_params := a.build_offset_commit_params(group_id, offsets)
		query := a.build_offset_upsert_query(offsets.len)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!
}

/// fetch_offsets는 커밋된 오프셋을 조회합니다.
pub fn (mut a PostgresStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	mut results := []domain.OffsetFetchResult{}

	if partitions.len == 0 {
		return results
	}

	// 단일 쿼리로 모든 파티션의 오프셋 조회 (IN 절 사용)
	mut topic_partition_pairs := []string{}
	mut all_params := []string{}
	all_params << group_id

	for i, part in partitions {
		param_idx := i * 2 + 2
		topic_partition_pairs << '(topic_name = \$${param_idx} AND partition_id = \$${param_idx + 1})'
		all_params << part.topic
		all_params << part.partition.str()
	}

	where_clause := topic_partition_pairs.join(' OR ')
	query := 'SELECT topic_name, partition_id, committed_offset, metadata FROM committed_offsets WHERE group_id = \$1 AND (${where_clause})'
	rows := db.exec_param_many(query, all_params)!

	// 결과를 맵으로 변환
	mut offset_map := map[string]domain.OffsetFetchResult{}
	for row in rows {
		topic := get_row_str(&row, 0, '')
		partition := get_row_int(&row, 1, 0)
		key := '${topic}:${partition}'
		offset_map[key] = domain.OffsetFetchResult{
			topic:      topic
			partition:  partition
			offset:     get_row_i64(&row, 2, -1)
			metadata:   get_row_str(&row, 3, '')
			error_code: 0
		}
	}

	// 요청된 모든 파티션에 대한 결과 생성
	for part in partitions {
		key := '${part.topic}:${part.partition}'
		if result := offset_map[key] {
			results << result
		} else {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
	}

	return results
}

// ============================================================
// 헬스 체크 (Health Check)
// ============================================================

/// health_check는 스토리지 상태를 확인합니다.
pub fn (mut a PostgresStorageAdapter) health_check() !port.HealthStatus {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.exec('SELECT 1')!
	return .healthy
}

// ============================================================
// 멀티 브로커 지원 (Multi-Broker Support)
// ============================================================

/// get_storage_capability는 스토리지 기능 정보를 반환합니다.
pub fn (a &PostgresStorageAdapter) get_storage_capability() domain.StorageCapability {
	return postgres_capability
}

/// get_cluster_metadata_port는 클러스터 메타데이터 포트를 반환합니다.
/// PostgreSQL은 멀티 브로커 모드를 지원합니다.
pub fn (a &PostgresStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	if a.cluster_metadata != unsafe { nil } {
		return a.cluster_metadata
	}
	return none
}

// ============================================================
// 통계 및 유틸리티 (Stats and Utilities)
// ============================================================

/// StorageStats는 스토리지 통계를 제공합니다.
/// 데이터베이스의 현재 상태를 요약한 정보를 담습니다.
pub struct StorageStats {
pub:
	topic_count      int // 토픽 수
	total_partitions int // 총 파티션 수
	total_records    i64 // 총 레코드 수
	group_count      int // 컨슈머 그룹 수
}

/// get_stats는 현재 스토리지 통계를 반환합니다.
/// 토픽 수, 파티션 수, 레코드 수, 컨슈머 그룹 수를 조회합니다.
pub fn (mut a PostgresStorageAdapter) get_stats() !StorageStats {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	topic_count := db.q_int('SELECT COUNT(*) FROM topics')!
	total_partitions := db.q_int('SELECT COUNT(*) FROM partition_metadata')!
	total_records := db.q_int('SELECT COUNT(*) FROM records')!
	group_count := db.q_int('SELECT COUNT(*) FROM consumer_groups')!

	return StorageStats{
		topic_count:      topic_count
		total_partitions: total_partitions
		total_records:    total_records
		group_count:      group_count
	}
}

/// close는 풀의 모든 연결을 닫습니다.
/// 어댑터 사용이 끝나면 반드시 호출하여 리소스를 해제해야 합니다.
pub fn (mut a PostgresStorageAdapter) close() {
	a.pool.close()
}

// ============================================================
// 메트릭 조회 (Metrics Query)
// ============================================================

/// get_metrics는 현재 메트릭 스냅샷을 반환합니다.
pub fn (mut a PostgresStorageAdapter) get_metrics() PostgresMetrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary는 메트릭 요약 문자열을 반환합니다.
pub fn (mut a PostgresStorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics는 모든 메트릭을 0으로 초기화합니다.
pub fn (mut a PostgresStorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}
