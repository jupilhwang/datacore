/// Infrastructure layer - Object pool
/// High-performance object pooling for Kafka records and requests
module core

import sync

// PooledRecord - pooled record

/// PooledRecord is a reusable record managed by the pool.
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

/// RecordHeader represents a record header.
pub struct RecordHeader {
pub:
	key   string
	value []u8
}

/// reset resets the record for reuse.
fn (mut r PooledRecord) reset() {
	r.key = r.key[..0]
	r.value = r.value[..0]
	r.headers = r.headers[..0]
	r.timestamp = 0
	r.partition = 0
	r.offset = 0
	r.in_use = false
}

/// set_key sets the record key.
pub fn (mut r PooledRecord) set_key(k []u8) {
	r.key = k.clone()
}

/// set_value sets the record value.
pub fn (mut r PooledRecord) set_value(v []u8) {
	r.value = v.clone()
}

/// add_header adds a header to the record.
pub fn (mut r PooledRecord) add_header(key string, value []u8) {
	r.headers << RecordHeader{
		key:   key
		value: value.clone()
	}
}

// PooledRecordBatch - pooled record batch

/// PooledRecordBatch is a reusable record batch managed by the pool.
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

/// reset resets the batch for reuse.
fn (mut b PooledRecordBatch) reset() {
	b.topic = ''
	b.partition = 0
	// Clear records while retaining capacity
	b.records = b.records[..0]
	b.base_offset = 0
	b.first_ts = 0
	b.max_ts = 0
	b.crc = 0
	b.in_use = false
}

/// add_record adds a record to the batch.
pub fn (mut b PooledRecordBatch) add_record(r &PooledRecord) {
	b.records << r
	if b.records.len == 1 {
		b.first_ts = r.timestamp
	}
	if r.timestamp > b.max_ts {
		b.max_ts = r.timestamp
	}
}

/// size returns the number of records in the batch.
pub fn (b &PooledRecordBatch) size() int {
	return b.records.len
}

/// byte_size returns the estimated byte size of the batch.
fn (b &PooledRecordBatch) byte_size() int {
	mut total := 0
	for r in b.records {
		total += r.key.len + r.value.len + 20
	}
	return total
}

// PooledRequest - pooled request

/// PooledRequest is a reusable request managed by the pool.
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

/// reset resets the request for reuse.
fn (mut r PooledRequest) reset() {
	r.api_key = 0
	r.api_version = 0
	r.correlation_id = 0
	r.client_id = ''
	r.payload = r.payload[..0]
	r.in_use = false
}

// RecordPool - record object pool

/// RecordPool is a pool that manages PooledRecord objects.
@[heap]
pub struct RecordPool {
mut:
	pool     []&PooledRecord
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_record_pool creates a new record pool with the specified maximum size.
pub fn new_record_pool(max_size int) &RecordPool {
	return &RecordPool{
		pool:     []&PooledRecord{cap: max_size}
		max_size: max_size
	}
}

/// get retrieves a record from the pool.
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

/// put returns a record to the pool.
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

/// get_stats returns the current pool statistics.
pub fn (mut p RecordPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// RecordBatchPool - record batch object pool

/// RecordBatchPool is a pool that manages PooledRecordBatch objects.
@[heap]
pub struct RecordBatchPool {
mut:
	pool     []&PooledRecordBatch
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_record_batch_pool creates a new batch pool with the specified maximum size.
pub fn new_record_batch_pool(max_size int) &RecordBatchPool {
	return &RecordBatchPool{
		pool:     []&PooledRecordBatch{cap: max_size}
		max_size: max_size
	}
}

/// get retrieves a batch from the pool.
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

/// put returns a batch to the pool.
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

/// get_stats returns the current pool statistics.
pub fn (mut p RecordBatchPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// RequestPool - request object pool

/// RequestPool is a pool that manages PooledRequest objects.
@[heap]
pub struct RequestPool {
mut:
	pool     []&PooledRequest
	max_size int
	stats    ObjectPoolStats
	lock     sync.Mutex
}

/// new_request_pool creates a new request pool with the specified maximum size.
pub fn new_request_pool(max_size int) &RequestPool {
	return &RequestPool{
		pool:     []&PooledRequest{cap: max_size}
		max_size: max_size
	}
}

/// get retrieves a request from the pool.
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

/// put returns a request to the pool.
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

/// get_stats returns the current pool statistics.
pub fn (mut p RequestPool) get_stats() ObjectPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	mut stats := p.stats
	stats.pool_size = p.pool.len
	return stats
}

// Statistics

/// ObjectPoolStats holds object pool statistics.
pub struct ObjectPoolStats {
pub mut:
	hits        u64
	misses      u64
	allocations u64
	returns     u64
	discards    u64
	pool_size   int
}

/// hit_rate returns the pool hit rate.
pub fn (s &ObjectPoolStats) hit_rate() f64 {
	total := s.hits + s.misses
	if total == 0 {
		return 0.0
	}
	return f64(s.hits) / f64(total)
}

/// discard_rate returns the discard rate.
fn (s &ObjectPoolStats) discard_rate() f64 {
	total := s.returns + s.discards
	if total == 0 {
		return 0.0
	}
	return f64(s.discards) / f64(total)
}
