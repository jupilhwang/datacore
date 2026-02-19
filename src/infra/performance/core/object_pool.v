/// 인프라 레이어 - 객체 풀
/// Kafka 레코드와 요청을 위한 고성능 객체 풀링
module core

import sync

// PooledRecord - 풀링된 레코드

/// PooledRecord는 풀에서 관리되는 재사용 가능한 레코드입니다.
@[heap]
pub struct PooledRecord {
pub mut:
	key       []u8
	value     []u8
	headers   []RecordHeader
	timestamp i64
	partition int
	offset    i64
	in_use    bool
}

/// RecordHeader는 레코드 헤더를 나타냅니다.
pub struct RecordHeader {
pub:
	key   string
	value []u8
}

/// reset은 레코드를 재사용을 위해 초기화합니다.
pub fn (mut r PooledRecord) reset() {
	r.key = r.key[..0]
	r.value = r.value[..0]
	r.headers = r.headers[..0]
	r.timestamp = 0
	r.partition = 0
	r.offset = 0
	r.in_use = false
}

/// set_key는 레코드 키를 설정합니다.
pub fn (mut r PooledRecord) set_key(k []u8) {
	r.key = k.clone()
}

/// set_value는 레코드 값을 설정합니다.
pub fn (mut r PooledRecord) set_value(v []u8) {
	r.value = v.clone()
}

/// add_header는 레코드에 헤더를 추가합니다.
pub fn (mut r PooledRecord) add_header(key string, value []u8) {
	r.headers << RecordHeader{
		key:   key
		value: value.clone()
	}
}

// PooledRecordBatch - 풀링된 레코드 배치

/// PooledRecordBatch는 풀에서 관리되는 재사용 가능한 레코드 배치입니다.
@[heap]
pub struct PooledRecordBatch {
pub mut:
	topic       string
	partition   int
	records     []&PooledRecord
	base_offset i64
	first_ts    i64
	max_ts      i64
	crc         u32
	in_use      bool
}

/// reset은 배치를 재사용을 위해 초기화합니다.
pub fn (mut b PooledRecordBatch) reset() {
	b.topic = ''
	b.partition = 0
	// 레코드를 지우되 용량은 유지
	b.records = b.records[..0]
	b.base_offset = 0
	b.first_ts = 0
	b.max_ts = 0
	b.crc = 0
	b.in_use = false
}

/// add_record는 배치에 레코드를 추가합니다.
pub fn (mut b PooledRecordBatch) add_record(r &PooledRecord) {
	b.records << r
	if b.records.len == 1 {
		b.first_ts = r.timestamp
	}
	if r.timestamp > b.max_ts {
		b.max_ts = r.timestamp
	}
}

/// size는 배치의 레코드 수를 반환합니다.
pub fn (b &PooledRecordBatch) size() int {
	return b.records.len
}

/// byte_size는 배치의 예상 바이트 크기를 반환합니다.
pub fn (b &PooledRecordBatch) byte_size() int {
	mut total := 0
	for r in b.records {
		total += r.key.len + r.value.len + 20
	}
	return total
}

// PooledRequest - 풀링된 요청

/// PooledRequest는 풀에서 관리되는 재사용 가능한 요청입니다.
@[heap]
pub struct PooledRequest {
pub mut:
	api_key        i16
	api_version    i16
	correlation_id i32
	client_id      string
	payload        []u8
	in_use         bool
}

/// reset은 요청을 재사용을 위해 초기화합니다.
pub fn (mut r PooledRequest) reset() {
	r.api_key = 0
	r.api_version = 0
	r.correlation_id = 0
	r.client_id = ''
	r.payload = r.payload[..0]
	r.in_use = false
}

// RecordPool - 레코드 객체 풀

/// RecordPool은 PooledRecord 객체를 관리하는 풀입니다.
@[heap]
pub struct RecordPool {
mut:
	pool     []&PooledRecord
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_record_pool은 지정된 최대 크기로 새 레코드 풀을 생성합니다.
pub fn new_record_pool(max_size int) &RecordPool {
	return &RecordPool{
		pool:     []&PooledRecord{cap: max_size}
		max_size: max_size
	}
}

/// get은 풀에서 레코드를 가져옵니다.
pub fn (mut p RecordPool) get() &PooledRecord {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len > 0 {
		mut r := p.pool.pop()
		r.reset()
		r.in_use = true
		p.stats.hits += 1
		return r
	}

	p.stats.misses += 1
	p.stats.allocations += 1

	return &PooledRecord{
		key:     []u8{cap: 256}
		value:   []u8{cap: 4096}
		headers: []RecordHeader{cap: 8}
		in_use:  true
	}
}

/// put은 레코드를 풀에 반환합니다.
pub fn (mut p RecordPool) put(r &PooledRecord) {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len < p.max_size {
		p.pool << r
		p.stats.returns += 1
	} else {
		p.stats.discards += 1
	}
}

/// get_stats는 현재 풀 통계를 반환합니다.
pub fn (mut p RecordPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// RecordBatchPool - 레코드 배치 객체 풀

/// RecordBatchPool은 PooledRecordBatch 객체를 관리하는 풀입니다.
@[heap]
pub struct RecordBatchPool {
mut:
	pool     []&PooledRecordBatch
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_record_batch_pool은 지정된 최대 크기로 새 배치 풀을 생성합니다.
pub fn new_record_batch_pool(max_size int) &RecordBatchPool {
	return &RecordBatchPool{
		pool:     []&PooledRecordBatch{cap: max_size}
		max_size: max_size
	}
}

/// get은 풀에서 배치를 가져옵니다.
pub fn (mut p RecordBatchPool) get() &PooledRecordBatch {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len > 0 {
		mut b := p.pool.pop()
		b.reset()
		b.in_use = true
		p.stats.hits += 1
		return b
	}

	p.stats.misses += 1
	p.stats.allocations += 1

	return &PooledRecordBatch{
		records: []&PooledRecord{cap: 100}
		in_use:  true
	}
}

/// put은 배치를 풀에 반환합니다.
pub fn (mut p RecordBatchPool) put(b &PooledRecordBatch) {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len < p.max_size {
		p.pool << b
		p.stats.returns += 1
	} else {
		p.stats.discards += 1
	}
}

/// get_stats는 현재 풀 통계를 반환합니다.
pub fn (mut p RecordBatchPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// RequestPool - 요청 객체 풀

/// RequestPool은 PooledRequest 객체를 관리하는 풀입니다.
@[heap]
pub struct RequestPool {
mut:
	pool     []&PooledRequest
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_request_pool은 지정된 최대 크기로 새 요청 풀을 생성합니다.
pub fn new_request_pool(max_size int) &RequestPool {
	return &RequestPool{
		pool:     []&PooledRequest{cap: max_size}
		max_size: max_size
	}
}

/// get은 풀에서 요청을 가져옵니다.
pub fn (mut p RequestPool) get() &PooledRequest {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len > 0 {
		mut r := p.pool.pop()
		r.reset()
		r.in_use = true
		p.stats.hits += 1
		return r
	}

	p.stats.misses += 1
	p.stats.allocations += 1

	return &PooledRequest{
		payload: []u8{cap: 4096}
		in_use:  true
	}
}

/// put은 요청을 풀에 반환합니다.
pub fn (mut p RequestPool) put(r &PooledRequest) {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pool.len < p.max_size {
		p.pool << r
		p.stats.returns += 1
	} else {
		p.stats.discards += 1
	}
}

/// get_stats는 현재 풀 통계를 반환합니다.
pub fn (mut p RequestPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// 통계

/// ObjectPoolStats는 객체 풀 통계를 담고 있습니다.
pub struct ObjectPoolStats {
pub mut:
	hits        u64
	misses      u64
	allocations u64
	returns     u64
	discards    u64
	pool_size   int
}

/// hit_rate는 풀 히트율을 반환합니다.
pub fn (s &ObjectPoolStats) hit_rate() f64 {
	total := s.hits + s.misses
	if total == 0 {
		return 0.0
	}
	return f64(s.hits) / f64(total)
}

/// discard_rate는 버려진 비율을 반환합니다.
pub fn (s &ObjectPoolStats) discard_rate() f64 {
	total := s.returns + s.discards
	if total == 0 {
		return 0.0
	}
	return f64(s.discards) / f64(total)
}
