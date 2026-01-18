// Infra Layer - Object Pool
// High-performance object pooling for Kafka records and requests
module core

import sync

// ============================================================================
// Pooled Record
// ============================================================================

@[heap]
pub struct PooledRecord {
pub mut:
    key          []u8
    value        []u8
    headers      []RecordHeader
    timestamp    i64
    partition    int
    offset       i64
    in_use       bool
}

pub struct RecordHeader {
pub:
    key   string
    value []u8
}

pub fn (mut r PooledRecord) reset() {
    r.key = r.key[..0]
    r.value = r.value[..0]
    r.headers = r.headers[..0]
    r.timestamp = 0
    r.partition = 0
    r.offset = 0
    r.in_use = false
}

pub fn (mut r PooledRecord) set_key(k []u8) {
    r.key = k.clone()
}

pub fn (mut r PooledRecord) set_value(v []u8) {
    r.value = v.clone()
}

pub fn (mut r PooledRecord) add_header(key string, value []u8) {
    r.headers << RecordHeader{
        key: key
        value: value.clone()
    }
}

// ============================================================================
// Pooled Record Batch
// ============================================================================

@[heap]
pub struct PooledRecordBatch {
pub mut:
    topic         string
    partition     int
    records       []&PooledRecord
    base_offset   i64
    first_ts      i64
    max_ts        i64
    crc           u32
    in_use        bool
}

pub fn (mut b PooledRecordBatch) reset() {
    b.topic = ''
    b.partition = 0
    // Clear records but keep capacity
    b.records = b.records[..0]
    b.base_offset = 0
    b.first_ts = 0
    b.max_ts = 0
    b.crc = 0
    b.in_use = false
}

pub fn (mut b PooledRecordBatch) add_record(r &PooledRecord) {
    b.records << r
    if b.records.len == 1 {
        b.first_ts = r.timestamp
    }
    if r.timestamp > b.max_ts {
        b.max_ts = r.timestamp
    }
}

pub fn (b &PooledRecordBatch) size() int {
    return b.records.len
}

pub fn (b &PooledRecordBatch) byte_size() int {
    mut total := 0
    for r in b.records {
        total += r.key.len + r.value.len + 20 // overhead estimate
    }
    return total
}

// ============================================================================
// Pooled Request
// ============================================================================

@[heap]
pub struct PooledRequest {
pub mut:
    api_key         i16
    api_version     i16
    correlation_id  i32
    client_id       string
    payload         []u8
    in_use          bool
}

pub fn (mut r PooledRequest) reset() {
    r.api_key = 0
    r.api_version = 0
    r.correlation_id = 0
    r.client_id = ''
    r.payload = r.payload[..0]
    r.in_use = false
}

// ============================================================================
// Object Pool for Records
// ============================================================================

@[heap]
pub struct RecordPool {
mut:
    pool        []&PooledRecord
    max_size    int
    stats       ObjectPoolStats
    lock        sync.Mutex
}

pub fn new_record_pool(max_size int) &RecordPool {
    return &RecordPool{
        pool: []&PooledRecord{cap: max_size}
        max_size: max_size
    }
}

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
        key: []u8{cap: 256}
        value: []u8{cap: 4096}
        headers: []RecordHeader{cap: 8}
        in_use: true
    }
}

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

pub fn (mut p RecordPool) get_stats() ObjectPoolStats {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    mut stats := p.stats
    stats.pool_size = p.pool.len
    return stats
}

// ============================================================================
// Object Pool for Record Batches
// ============================================================================

@[heap]
pub struct RecordBatchPool {
mut:
    pool        []&PooledRecordBatch
    max_size    int
    stats       ObjectPoolStats
    lock        sync.Mutex
}

pub fn new_record_batch_pool(max_size int) &RecordBatchPool {
    return &RecordBatchPool{
        pool: []&PooledRecordBatch{cap: max_size}
        max_size: max_size
    }
}

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
        in_use: true
    }
}

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

pub fn (mut p RecordBatchPool) get_stats() ObjectPoolStats {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    mut stats := p.stats
    stats.pool_size = p.pool.len
    return stats
}

// ============================================================================
// Object Pool for Requests
// ============================================================================

@[heap]
pub struct RequestPool {
mut:
    pool        []&PooledRequest
    max_size    int
    stats       ObjectPoolStats
    lock        sync.Mutex
}

pub fn new_request_pool(max_size int) &RequestPool {
    return &RequestPool{
        pool: []&PooledRequest{cap: max_size}
        max_size: max_size
    }
}

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
        in_use: true
    }
}

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

pub fn (mut p RequestPool) get_stats() ObjectPoolStats {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    mut stats := p.stats
    stats.pool_size = p.pool.len
    return stats
}

// ============================================================================
// Statistics
// ============================================================================

pub struct ObjectPoolStats {
pub mut:
    hits        u64
    misses      u64
    allocations u64
    returns     u64
    discards    u64
    pool_size   int
}

pub fn (s &ObjectPoolStats) hit_rate() f64 {
    total := s.hits + s.misses
    if total == 0 {
        return 0.0
    }
    return f64(s.hits) / f64(total)
}

pub fn (s &ObjectPoolStats) discard_rate() f64 {
    total := s.returns + s.discards
    if total == 0 {
        return 0.0
    }
    return f64(s.discards) / f64(total)
}
