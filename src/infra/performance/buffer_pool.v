// Infra Layer - Buffer Pool
// High-performance reusable buffer pool to reduce GC pressure
// Based on sync.Pool pattern with size-class bucketing
module performance

import sync

// ============================================================================
// Buffer - Reusable byte buffer wrapper
// ============================================================================

// Buffer represents a reusable byte buffer from the pool
@[heap]
pub struct Buffer {
pub mut:
    data      []u8      // Underlying byte array
    len       int       // Current logical length (bytes used)
    cap       int       // Capacity (allocated size)
    pool      &BufferPool = unsafe { nil }  // Reference to parent pool for return
    size_class SizeClass  // Size class this buffer belongs to
}

// write appends bytes to the buffer, growing if necessary
pub fn (mut b Buffer) write(data []u8) int {
    needed := b.len + data.len
    if needed > b.cap {
        // Buffer is from pool, cannot grow beyond capacity
        // Only write what fits
        available := b.cap - b.len
        if available <= 0 {
            return 0
        }
        for i := 0; i < available; i++ {
            b.data[b.len + i] = data[i]
        }
        b.len += available
        return available
    }
    
    for i, byte in data {
        b.data[b.len + i] = byte
    }
    b.len += data.len
    return data.len
}

// write_byte appends a single byte
pub fn (mut b Buffer) write_byte(byte u8) bool {
    if b.len >= b.cap {
        return false
    }
    b.data[b.len] = byte
    b.len += 1
    return true
}

// write_i32_be writes a 32-bit integer in big-endian
pub fn (mut b Buffer) write_i32_be(val i32) bool {
    if b.len + 4 > b.cap {
        return false
    }
    b.data[b.len] = u8(val >> 24)
    b.data[b.len + 1] = u8(val >> 16)
    b.data[b.len + 2] = u8(val >> 8)
    b.data[b.len + 3] = u8(val)
    b.len += 4
    return true
}

// write_i64_be writes a 64-bit integer in big-endian
pub fn (mut b Buffer) write_i64_be(val i64) bool {
    if b.len + 8 > b.cap {
        return false
    }
    b.data[b.len] = u8(val >> 56)
    b.data[b.len + 1] = u8(val >> 48)
    b.data[b.len + 2] = u8(val >> 40)
    b.data[b.len + 3] = u8(val >> 32)
    b.data[b.len + 4] = u8(val >> 24)
    b.data[b.len + 5] = u8(val >> 16)
    b.data[b.len + 6] = u8(val >> 8)
    b.data[b.len + 7] = u8(val)
    b.len += 8
    return true
}

// bytes returns the used portion of the buffer
pub fn (b &Buffer) bytes() []u8 {
    return b.data[..b.len]
}

// reset clears the buffer for reuse (does not deallocate)
pub fn (mut b Buffer) reset() {
    b.len = 0
}

// remaining returns bytes available
pub fn (b &Buffer) remaining() int {
    return b.cap - b.len
}

// release returns the buffer to its pool
pub fn (mut b Buffer) release() {
    if b.pool != unsafe { nil } {
        b.pool.put(b)
    }
}

// ============================================================================
// Size Classes - Bucketing strategy for buffer sizes
// ============================================================================

// SizeClass represents predefined buffer size categories
pub enum SizeClass {
    tiny     // 256 bytes - for small headers
    small    // 4KB - for typical messages
    medium   // 64KB - for larger batches
    large    // 1MB - for max single message
    huge     // 16MB - for large batch responses
}

// size_class_bytes returns the byte size for a size class
fn size_class_bytes(sc SizeClass) int {
    return match sc {
        .tiny   { 256 }
        .small  { 4096 }
        .medium { 65536 }
        .large  { 1048576 }
        .huge   { 16777216 }
    }
}

// get_size_class determines the appropriate size class for a given size
fn get_size_class(size int) SizeClass {
    if size <= 256 {
        return .tiny
    } else if size <= 4096 {
        return .small
    } else if size <= 65536 {
        return .medium
    } else if size <= 1048576 {
        return .large
    } else {
        return .huge
    }
}

// ============================================================================
// BufferPool - Thread-safe buffer pool with size-class bucketing
// ============================================================================

// PoolConfig configures the buffer pool
pub struct PoolConfig {
pub:
    // Maximum buffers per size class
    max_tiny    int = 1000
    max_small   int = 500
    max_medium  int = 100
    max_large   int = 20
    max_huge    int = 5
    
    // Pre-warm pool with initial buffers
    prewarm_tiny    int = 100
    prewarm_small   int = 50
    prewarm_medium  int = 10
    prewarm_large   int = 2
    prewarm_huge    int = 0
}

// PoolStats holds pool statistics
pub struct PoolStats {
pub mut:
    // Hits (buffer reused from pool)
    hits_tiny       u64
    hits_small      u64
    hits_medium     u64
    hits_large      u64
    hits_huge       u64
    
    // Misses (new buffer allocated)
    misses_tiny     u64
    misses_small    u64
    misses_medium   u64
    misses_large    u64
    misses_huge     u64
    
    // Current pool sizes
    pool_size_tiny      int
    pool_size_small     int
    pool_size_medium    int
    pool_size_large     int
    pool_size_huge      int
    
    // Total bytes allocated
    bytes_allocated     u64
    bytes_reused        u64
}

// BufferPool manages pools of reusable buffers
@[heap]
pub struct BufferPool {
mut:
    // Separate pools for each size class
    tiny_pool       []&Buffer
    small_pool      []&Buffer
    medium_pool     []&Buffer
    large_pool      []&Buffer
    huge_pool       []&Buffer
    
    config          PoolConfig
    stats           PoolStats
    lock            sync.Mutex
}

// new_buffer_pool creates a new buffer pool with the given configuration
pub fn new_buffer_pool(config PoolConfig) &BufferPool {
    mut pool := &BufferPool{
        config: config
        tiny_pool: []&Buffer{cap: config.max_tiny}
        small_pool: []&Buffer{cap: config.max_small}
        medium_pool: []&Buffer{cap: config.max_medium}
        large_pool: []&Buffer{cap: config.max_large}
        huge_pool: []&Buffer{cap: config.max_huge}
    }
    
    // Pre-warm pools
    pool.prewarm()
    
    return pool
}

// new_default_pool creates a buffer pool with default settings
pub fn new_default_pool() &BufferPool {
    return new_buffer_pool(PoolConfig{})
}

// prewarm pre-allocates buffers
fn (mut p BufferPool) prewarm() {
    // Pre-allocate tiny buffers
    for _ in 0 .. p.config.prewarm_tiny {
        buf := p.allocate_buffer(.tiny)
        p.tiny_pool << buf
    }
    
    // Pre-allocate small buffers
    for _ in 0 .. p.config.prewarm_small {
        buf := p.allocate_buffer(.small)
        p.small_pool << buf
    }
    
    // Pre-allocate medium buffers
    for _ in 0 .. p.config.prewarm_medium {
        buf := p.allocate_buffer(.medium)
        p.medium_pool << buf
    }
    
    // Pre-allocate large buffers
    for _ in 0 .. p.config.prewarm_large {
        buf := p.allocate_buffer(.large)
        p.large_pool << buf
    }
    
    // Pre-allocate huge buffers
    for _ in 0 .. p.config.prewarm_huge {
        buf := p.allocate_buffer(.huge)
        p.huge_pool << buf
    }
}

// allocate_buffer creates a new buffer (without pool)
fn (mut p BufferPool) allocate_buffer(sc SizeClass) &Buffer {
    size := size_class_bytes(sc)
    p.stats.bytes_allocated += u64(size)
    
    return &Buffer{
        data: []u8{len: size, cap: size}
        len: 0
        cap: size
        pool: p
        size_class: sc
    }
}

// get retrieves a buffer of at least the specified size
pub fn (mut p BufferPool) get(min_size int) &Buffer {
    sc := get_size_class(min_size)
    return p.get_by_class(sc)
}

// get_by_class retrieves a buffer by size class
pub fn (mut p BufferPool) get_by_class(sc SizeClass) &Buffer {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    match sc {
        .tiny {
            if p.tiny_pool.len > 0 {
                buf := p.tiny_pool.pop()
                buf.reset()
                p.stats.hits_tiny += 1
                p.stats.bytes_reused += u64(buf.cap)
                return buf
            }
            p.stats.misses_tiny += 1
        }
        .small {
            if p.small_pool.len > 0 {
                buf := p.small_pool.pop()
                buf.reset()
                p.stats.hits_small += 1
                p.stats.bytes_reused += u64(buf.cap)
                return buf
            }
            p.stats.misses_small += 1
        }
        .medium {
            if p.medium_pool.len > 0 {
                buf := p.medium_pool.pop()
                buf.reset()
                p.stats.hits_medium += 1
                p.stats.bytes_reused += u64(buf.cap)
                return buf
            }
            p.stats.misses_medium += 1
        }
        .large {
            if p.large_pool.len > 0 {
                buf := p.large_pool.pop()
                buf.reset()
                p.stats.hits_large += 1
                p.stats.bytes_reused += u64(buf.cap)
                return buf
            }
            p.stats.misses_large += 1
        }
        .huge {
            if p.huge_pool.len > 0 {
                buf := p.huge_pool.pop()
                buf.reset()
                p.stats.hits_huge += 1
                p.stats.bytes_reused += u64(buf.cap)
                return buf
            }
            p.stats.misses_huge += 1
        }
    }
    
    // Pool miss - allocate new buffer
    return p.allocate_buffer(sc)
}

// put returns a buffer to the pool
pub fn (mut p BufferPool) put(buf &Buffer) {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    match buf.size_class {
        .tiny {
            if p.tiny_pool.len < p.config.max_tiny {
                p.tiny_pool << buf
            }
            // else: buffer is discarded (GC will collect)
        }
        .small {
            if p.small_pool.len < p.config.max_small {
                p.small_pool << buf
            }
        }
        .medium {
            if p.medium_pool.len < p.config.max_medium {
                p.medium_pool << buf
            }
        }
        .large {
            if p.large_pool.len < p.config.max_large {
                p.large_pool << buf
            }
        }
        .huge {
            if p.huge_pool.len < p.config.max_huge {
                p.huge_pool << buf
            }
        }
    }
}

// get_stats returns pool statistics
pub fn (mut p BufferPool) get_stats() PoolStats {
    p.lock.@lock()
    defer { p.lock.unlock() }
    
    mut stats := p.stats
    stats.pool_size_tiny = p.tiny_pool.len
    stats.pool_size_small = p.small_pool.len
    stats.pool_size_medium = p.medium_pool.len
    stats.pool_size_large = p.large_pool.len
    stats.pool_size_huge = p.huge_pool.len
    
    return stats
}

// total_hits returns total pool hits across all size classes
pub fn (s &PoolStats) total_hits() u64 {
    return s.hits_tiny + s.hits_small + s.hits_medium + s.hits_large + s.hits_huge
}

// total_misses returns total pool misses across all size classes
pub fn (s &PoolStats) total_misses() u64 {
    return s.misses_tiny + s.misses_small + s.misses_medium + s.misses_large + s.misses_huge
}

// hit_rate returns the cache hit rate (0.0 - 1.0)
pub fn (s &PoolStats) hit_rate() f64 {
    total := s.total_hits() + s.total_misses()
    if total == 0 {
        return 0.0
    }
    return f64(s.total_hits()) / f64(total)
}

// ============================================================================
// Global Pool Instance (Singleton)
// ============================================================================

// Global buffer pool instance
__global global_buffer_pool = &BufferPool(unsafe { nil })
__global global_pool_lock = sync.Mutex{}

// get_global_pool returns the global buffer pool singleton
pub fn get_global_pool() &BufferPool {
    global_pool_lock.@lock()
    defer { global_pool_lock.unlock() }
    
    if global_buffer_pool == unsafe { nil } {
        global_buffer_pool = new_default_pool()
    }
    return global_buffer_pool
}

// init_global_pool initializes the global pool with custom config
pub fn init_global_pool(config PoolConfig) {
    global_pool_lock.@lock()
    defer { global_pool_lock.unlock() }
    
    global_buffer_pool = new_buffer_pool(config)
}

// ============================================================================
// Convenience Functions
// ============================================================================

// get_buffer gets a buffer from the global pool
pub fn get_buffer(min_size int) &Buffer {
    return get_global_pool().get(min_size)
}

// get_small_buffer gets a small (4KB) buffer from the global pool
pub fn get_small_buffer() &Buffer {
    return get_global_pool().get_by_class(.small)
}

// get_medium_buffer gets a medium (64KB) buffer from the global pool
pub fn get_medium_buffer() &Buffer {
    return get_global_pool().get_by_class(.medium)
}

// get_large_buffer gets a large (1MB) buffer from the global pool
pub fn get_large_buffer() &Buffer {
    return get_global_pool().get_by_class(.large)
}
