module engines

import os
import infra.performance.core
import infra.performance

pub struct GenericPerformanceEngine {
pub mut:
    buffer_pool       &core.BufferPool = unsafe { nil }
    record_pool       &core.RecordPool = unsafe { nil }
    batch_pool        &core.RecordBatchPool = unsafe { nil }
    request_pool      &core.RequestPool = unsafe { nil }
    config            performance.PerformanceConfig
}

pub fn (e GenericPerformanceEngine) name() string {
    return 'Generic'
}

pub fn (mut e GenericPerformanceEngine) init(config performance.PerformanceConfig) ! {
    e.config = config
    
    pool_config := core.PoolConfig{
        max_tiny: config.buffer_pool_max_tiny
        max_small: config.buffer_pool_max_small
        max_medium: config.buffer_pool_max_medium
        max_large: config.buffer_pool_max_large
        max_huge: config.buffer_pool_max_huge
        prewarm_tiny: if config.buffer_pool_prewarm { 100 } else { 0 }
        prewarm_small: if config.buffer_pool_prewarm { 50 } else { 0 }
        prewarm_medium: if config.buffer_pool_prewarm { 10 } else { 0 }
        prewarm_large: if config.buffer_pool_prewarm { 2 } else { 0 }
    }
    
    e.buffer_pool = core.new_buffer_pool(pool_config)
    e.record_pool = core.new_record_pool(config.record_pool_max_size)
    e.batch_pool = core.new_record_batch_pool(config.batch_pool_max_size)
    e.request_pool = core.new_request_pool(config.request_pool_max_size)
}

pub fn (mut e GenericPerformanceEngine) get_buffer(size int) &core.Buffer {
    return e.buffer_pool.get(size)
}

pub fn (mut e GenericPerformanceEngine) put_buffer(buf &core.Buffer) {
    e.buffer_pool.put(buf)
}

pub fn (mut e GenericPerformanceEngine) get_record() &core.PooledRecord {
    return e.record_pool.get()
}

pub fn (mut e GenericPerformanceEngine) put_record(r &core.PooledRecord) {
    e.record_pool.put(r)
}

pub fn (mut e GenericPerformanceEngine) get_batch() &core.PooledRecordBatch {
    return e.batch_pool.get()
}

pub fn (mut e GenericPerformanceEngine) put_batch(b &core.PooledRecordBatch) {
    e.batch_pool.put(b)
}

pub fn (mut e GenericPerformanceEngine) get_request() &core.PooledRequest {
    return e.request_pool.get()
}

pub fn (mut e GenericPerformanceEngine) put_request(r &core.PooledRequest) {
    e.request_pool.put(r)
}

pub fn (mut e GenericPerformanceEngine) read_file_at(path string, offset i64, size int) ![]u8 {
    // Standard fallback: use mmap if available, otherwise read
    mut f := os.open(path) !
    defer { f.close() }
    f.seek(offset, .start) !
    mut buf := []u8{len: size}
    f.read(mut buf) !
    return buf
}

pub fn (mut e GenericPerformanceEngine) write_file_at(path string, offset i64, data []u8) ! {
    mut f := os.open_file(path, 'r+', 0o644) !
    defer { f.close() }
    f.seek(offset, .start) !
    f.write(data) !
}

pub fn (mut e GenericPerformanceEngine) get_stats() performance.PerformanceStats {
    buf_stats := e.buffer_pool.get_stats()
    return performance.PerformanceStats{
        engine_name: e.name()
        buffer_hits: buf_stats.total_hits()
        buffer_misses: buf_stats.total_misses()
        ops_count: 0 // TODO: Track other ops
    }
}
