/// Infrastructure layer - Buffer pool
/// High-performance reusable buffer pool to reduce GC pressure
module core

import sync

// Buffer - reusable byte buffer wrapper

/// Buffer represents a reusable byte buffer obtained from the pool.
@[heap]
pub struct Buffer {
pub mut:
	data       []u8
	len        int
	cap        int
	size_class SizeClass
}

/// write appends bytes to the buffer.
pub fn (mut b Buffer) write(data []u8) int {
	needed := b.len + data.len
	if needed > b.cap {
		available := b.cap - b.len
		if available <= 0 {
			return 0
		}
		// Optimized with array slice copy (instead of byte-by-byte loop)
		unsafe {
			C.memcpy(&b.data[b.len], data.data, usize(available))
		}
		b.len += available
		return available
	}

	// Optimized with array slice copy (instead of byte-by-byte loop)
	unsafe {
		C.memcpy(&b.data[b.len], data.data, usize(data.len))
	}
	b.len += data.len
	return data.len
}

/// write_byte appends a single byte.
pub fn (mut b Buffer) write_byte(byte u8) bool {
	if b.len >= b.cap {
		return false
	}
	b.data[b.len] = byte
	b.len += 1
	return true
}

/// write_i32_be writes a 32-bit integer in big-endian order.
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

/// write_i64_be writes a 64-bit integer in big-endian order.
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

/// bytes returns the used portion of the buffer.
pub fn (b &Buffer) bytes() []u8 {
	return b.data[..b.len]
}

/// reset resets the buffer for reuse.
pub fn (mut b Buffer) reset() {
	b.len = 0
}

/// remaining returns the number of available bytes.
pub fn (b &Buffer) remaining() int {
	return b.cap - b.len
}

// Size classes

/// SizeClass represents buffer size classification.
pub enum SizeClass {
	tiny
	small
	medium
	large
	huge
}

/// size_class_bytes returns the byte count for a size class.
fn size_class_bytes(sc SizeClass) int {
	return match sc {
		.tiny { 256 }
		.small { 4096 }
		.medium { 65536 }
		.large { 1048576 }
		.huge { 16777216 }
	}
}

/// get_size_class returns the appropriate size class for the given size.
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

// BufferPool - buffer pool

/// PoolConfig holds buffer pool configuration.
pub struct PoolConfig {
pub:
	max_tiny       int = 1000
	max_small      int = 500
	max_medium     int = 100
	max_large      int = 20
	max_huge       int = 5
	prewarm_tiny   int = 100
	prewarm_small  int = 50
	prewarm_medium int = 10
	prewarm_large  int = 2
}

/// PoolStats holds buffer pool statistics.
pub struct PoolStats {
pub mut:
	hits_tiny        u64
	hits_small       u64
	hits_medium      u64
	hits_large       u64
	hits_huge        u64
	misses_tiny      u64
	misses_small     u64
	misses_medium    u64
	misses_large     u64
	misses_huge      u64
	pool_size_tiny   int
	pool_size_small  int
	pool_size_medium int
	pool_size_large  int
	pool_size_huge   int
	bytes_allocated  u64
	bytes_reused     u64
}

/// total_hits returns the total hit count across all size classes.
pub fn (s &PoolStats) total_hits() u64 {
	return s.hits_tiny + s.hits_small + s.hits_medium + s.hits_large + s.hits_huge
}

/// total_misses returns the total miss count across all size classes.
pub fn (s &PoolStats) total_misses() u64 {
	return s.misses_tiny + s.misses_small + s.misses_medium + s.misses_large + s.misses_huge
}

/// hit_rate returns the cache hit rate.
pub fn (s &PoolStats) hit_rate() f64 {
	total := s.total_hits() + s.total_misses()
	if total == 0 {
		return 0.0
	}
	return f64(s.total_hits()) / f64(total)
}

/// BufferPool manages buffers by size class.
@[heap]
pub struct BufferPool {
mut:
	tiny_pool   []&Buffer
	small_pool  []&Buffer
	medium_pool []&Buffer
	large_pool  []&Buffer
	huge_pool   []&Buffer
	config      PoolConfig
	stats       PoolStats
	lock        sync.Mutex
}

/// new_buffer_pool creates a new buffer pool with the given configuration.
pub fn new_buffer_pool(config PoolConfig) &BufferPool {
	mut pool := &BufferPool{
		config:      config
		tiny_pool:   []&Buffer{cap: config.max_tiny}
		small_pool:  []&Buffer{cap: config.max_small}
		medium_pool: []&Buffer{cap: config.max_medium}
		large_pool:  []&Buffer{cap: config.max_large}
		huge_pool:   []&Buffer{cap: config.max_huge}
	}
	pool.prewarm()
	return pool
}

/// new_default_pool creates a new buffer pool with default configuration.
fn new_default_pool() &BufferPool {
	return new_buffer_pool(PoolConfig{})
}

/// prewarm pre-fills the pool with buffers.
fn (mut p BufferPool) prewarm() {
	for _ in 0 .. p.config.prewarm_tiny {
		p.tiny_pool << p.allocate_buffer(.tiny)
	}
	for _ in 0 .. p.config.prewarm_small {
		p.small_pool << p.allocate_buffer(.small)
	}
	for _ in 0 .. p.config.prewarm_medium {
		p.medium_pool << p.allocate_buffer(.medium)
	}
	for _ in 0 .. p.config.prewarm_large {
		p.large_pool << p.allocate_buffer(.large)
	}
}

/// allocate_buffer allocates a new buffer of the specified size class.
fn (mut p BufferPool) allocate_buffer(sc SizeClass) &Buffer {
	size := size_class_bytes(sc)
	p.stats.bytes_allocated += u64(size)

	return &Buffer{
		data:       []u8{len: size, cap: size}
		len:        0
		cap:        size
		size_class: sc
	}
}

/// get retrieves a buffer from the pool satisfying the minimum size.
pub fn (mut p BufferPool) get(min_size int) &Buffer {
	sc := get_size_class(min_size)
	return p.get_by_class(sc)
}

/// get_by_class retrieves a buffer of the specified size class from the pool.
fn (mut p BufferPool) get_by_class(sc SizeClass) &Buffer {
	p.lock.@lock()
	defer { p.lock.unlock() }

	match sc {
		.tiny {
			if p.tiny_pool.len > 0 {
				mut buf := p.tiny_pool.pop()
				buf.reset()
				p.stats.hits_tiny += 1
				p.stats.bytes_reused += u64(buf.cap)
				return buf
			}
			p.stats.misses_tiny += 1
		}
		.small {
			if p.small_pool.len > 0 {
				mut buf := p.small_pool.pop()
				buf.reset()
				p.stats.hits_small += 1
				p.stats.bytes_reused += u64(buf.cap)
				return buf
			}
			p.stats.misses_small += 1
		}
		.medium {
			if p.medium_pool.len > 0 {
				mut buf := p.medium_pool.pop()
				buf.reset()
				p.stats.hits_medium += 1
				p.stats.bytes_reused += u64(buf.cap)
				return buf
			}
			p.stats.misses_medium += 1
		}
		.large {
			if p.large_pool.len > 0 {
				mut buf := p.large_pool.pop()
				buf.reset()
				p.stats.hits_large += 1
				p.stats.bytes_reused += u64(buf.cap)
				return buf
			}
			p.stats.misses_large += 1
		}
		.huge {
			if p.huge_pool.len > 0 {
				mut buf := p.huge_pool.pop()
				buf.reset()
				p.stats.hits_huge += 1
				p.stats.bytes_reused += u64(buf.cap)
				return buf
			}
			p.stats.misses_huge += 1
		}
	}

	return p.allocate_buffer(sc)
}

/// put returns a buffer to the pool.
pub fn (mut p BufferPool) put(buf &Buffer) {
	p.lock.@lock()
	defer { p.lock.unlock() }

	match buf.size_class {
		.tiny {
			if p.tiny_pool.len < p.config.max_tiny {
				p.tiny_pool << buf
			}
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

/// get_stats returns the current pool statistics.
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
