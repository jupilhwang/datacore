// Infra Layer - Performance Module Integration
// Provides unified access to all performance components
module performance

// ============================================================================
// Performance Manager
// ============================================================================

@[heap]
pub struct PerformanceManager {
pub mut:
    buffer_pool       &BufferPool
    record_pool       &RecordPool
    batch_pool        &RecordBatchPool
    request_pool      &RequestPool
    enabled           bool
}

// ============================================================================
// Performance Configuration
// ============================================================================

pub struct PerformanceConfig {
pub:
    // Buffer pool settings
    buffer_pool_max_tiny    int = 1000
    buffer_pool_max_small   int = 500
    buffer_pool_max_medium  int = 100
    buffer_pool_max_large   int = 20
    buffer_pool_max_huge    int = 5
    buffer_pool_prewarm     bool = true
    
    // Object pool settings
    record_pool_max_size     int = 10000
    batch_pool_max_size      int = 1000
    request_pool_max_size    int = 5000
    
    // Feature flags
    enable_buffer_pooling    bool = true
    enable_object_pooling    bool = true
    enable_zero_copy         bool = true
}

// new_performance_manager creates a performance manager with given config
pub fn new_performance_manager(config PerformanceConfig) &PerformanceManager {
    pool_config := PoolConfig{
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
    
    return &PerformanceManager{
        buffer_pool: new_buffer_pool(pool_config)
        record_pool: new_record_pool(config.record_pool_max_size)
        batch_pool: new_record_batch_pool(config.batch_pool_max_size)
        request_pool: new_request_pool(config.request_pool_max_size)
        enabled: true
    }
}

// ============================================================================
// Convenience Methods
// ============================================================================

// get_buffer gets a buffer of appropriate size
pub fn (mut m PerformanceManager) get_buffer(min_size int) &Buffer {
    return m.buffer_pool.get(min_size)
}

// put_buffer returns a buffer to the pool
pub fn (mut m PerformanceManager) put_buffer(buf &Buffer) {
    m.buffer_pool.put(buf)
}

// get_record gets a pooled record
pub fn (mut m PerformanceManager) get_record() &PooledRecord {
    return m.record_pool.get()
}

// put_record returns a record to the pool
pub fn (mut m PerformanceManager) put_record(r &PooledRecord) {
    m.record_pool.put(r)
}

// get_batch gets a pooled record batch
pub fn (mut m PerformanceManager) get_batch() &PooledRecordBatch {
    return m.batch_pool.get()
}

// put_batch returns a batch to the pool
pub fn (mut m PerformanceManager) put_batch(b &PooledRecordBatch) {
    m.batch_pool.put(b)
}

// get_request gets a pooled request
pub fn (mut m PerformanceManager) get_request() &PooledRequest {
    return m.request_pool.get()
}

// put_request returns a request to the pool
pub fn (mut m PerformanceManager) put_request(r &PooledRequest) {
    m.request_pool.put(r)
}

// ============================================================================
// Statistics
// ============================================================================

pub struct PerformanceStats {
pub:
    buffer_pool_stats   PoolStats
    record_pool_stats   ObjectPoolStats
    batch_pool_stats    ObjectPoolStats
    request_pool_stats  ObjectPoolStats
}

pub fn (mut m PerformanceManager) get_stats() PerformanceStats {
    return PerformanceStats{
        buffer_pool_stats: m.buffer_pool.get_stats()
        record_pool_stats: m.record_pool.get_stats()
        batch_pool_stats: m.batch_pool.get_stats()
        request_pool_stats: m.request_pool.get_stats()
    }
}

// ============================================================================
// Helper functions for common operations
// ============================================================================

// with_buffer executes a function with a pooled buffer, automatically returning it
pub fn (mut m PerformanceManager) with_buffer(min_size int, f fn (mut Buffer)) {
    mut buf := m.get_buffer(min_size)
    defer { m.put_buffer(buf) }
    f(mut buf)
}

// compute_partition computes Kafka partition for a key
pub fn compute_partition(key []u8, num_partitions int) int {
    return kafka_partition(key, num_partitions)
}

// compute_checksum computes CRC32 checksum
pub fn compute_checksum(data []u8) u32 {
    return crc32_ieee(data)
}
