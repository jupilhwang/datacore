/// Infrastructure layer - BinaryWriter pool
/// Reusable pool of BinaryWriters for Kafka protocol encoding
/// Reduces GC pressure and optimizes memory allocation
module core

import sync

// WriterPool - BinaryWriter reuse pool

/// WriterPool reuses BinaryWriter objects to minimize memory allocations.
@[heap]
pub struct WriterPool {
pub mut:
	config PoolConfig
mut:
	pool       []&PooledWriter
	stats      WriterPoolStats
	lock       sync.Mutex
	is_running bool
}

/// PooledWriter is a BinaryWriter wrapper managed by the pool.
@[heap]
pub struct PooledWriter {
pub mut:
	data       []u8
	len        int
	cap        int
	size_class SizeClass
}

/// WriterPoolStats holds WriterPool statistics.
pub struct WriterPoolStats {
pub mut:
	hits        u64
	misses      u64
	allocations u64
	returns     u64
	discards    u64
	bytes_saved u64
}

/// WriterGuard provides RAII-style writer management.
pub struct WriterGuard {
pub mut:
	pool   &WriterPool
	writer &PooledWriter
	active bool
}

// Global WriterPool instance
__global g_writer_pool = &WriterPool(unsafe { nil })

/// get_global_writer_pool returns the global WriterPool instance.
/// Initializes with default configuration if not yet initialized.
pub fn get_global_writer_pool() &WriterPool {
	if g_writer_pool == unsafe { nil } {
		g_writer_pool = new_writer_pool(PoolConfig{
			max_tiny:   500
			max_small:  200
			max_medium: 50
			max_large:  10
			max_huge:   2
		})
	}
	return g_writer_pool
}

/// new_writer_pool creates a new WriterPool.
pub fn new_writer_pool(config PoolConfig) &WriterPool {
	mut pool := &WriterPool{
		config:     config
		pool:       []&PooledWriter{cap: config.max_small}
		is_running: true
	}
	pool.prewarm()
	return pool
}

/// prewarm pre-fills the pool with writers.
fn (mut p WriterPool) prewarm() {
	// Prewarm primarily with small size class (most commonly used)
	for _ in 0 .. p.config.prewarm_small {
		p.pool << p.allocate_writer(.small)
	}
}

/// allocate_writer allocates a new writer of the specified size class.
fn (mut p WriterPool) allocate_writer(sc SizeClass) &PooledWriter {
	size := size_class_bytes(sc)
	p.stats.allocations += 1

	return &PooledWriter{
		data:       []u8{len: 0, cap: size}
		len:        0
		cap:        size
		size_class: sc
	}
}

/// get retrieves a BinaryWriter from the pool.
/// Must be returned with return_writer() after use.
pub fn (mut p WriterPool) get(min_size int) &PooledWriter {
	if !p.is_running {
		return p.allocate_writer(get_size_class(min_size))
	}

	sc := get_size_class(min_size)

	p.lock.@lock()
	defer { p.lock.unlock() }

	// Find a writer of appropriate size from the pool
	for i := p.pool.len - 1; i >= 0; i-- {
		mut writer := p.pool[i]
		if writer.size_class == sc {
			// Remove from pool and return
			p.pool.delete(i)
			writer.reset()
			p.stats.hits += 1
			p.stats.bytes_saved += u64(writer.cap)
			return writer
		}
	}

	// Not found in pool - allocate new
	p.stats.misses += 1
	return p.allocate_writer(sc)
}

/// get_guard returns a RAII-style WriterGuard.
/// Use with defer to guarantee automatic return.
pub fn (mut p WriterPool) get_guard(min_size int) WriterGuard {
	writer := p.get(min_size)
	return WriterGuard{
		pool:   p
		writer: writer
		active: true
	}
}

/// return_writer returns a writer to the pool.
pub fn (mut p WriterPool) return_writer(mut writer PooledWriter) {
	if !p.is_running || &writer == unsafe { nil } {
		return
	}

	p.lock.@lock()
	defer { p.lock.unlock() }

	// Check maximum size limit
	current_count := p.count_by_class(writer.size_class)
	max_for_class := p.max_for_class(writer.size_class)

	if current_count < max_for_class {
		writer.reset()
		p.pool << &writer
		p.stats.returns += 1
	} else {
		p.stats.discards += 1
	}
}

/// count_by_class returns the number of writers of the specified size class.
fn (p &WriterPool) count_by_class(sc SizeClass) int {
	mut count := 0
	for writer in p.pool {
		if writer.size_class == sc {
			count++
		}
	}
	return count
}

/// max_for_class returns the maximum number of writers for the specified size class.
fn (p &WriterPool) max_for_class(sc SizeClass) int {
	return match sc {
		.tiny { p.config.max_tiny }
		.small { p.config.max_small }
		.medium { p.config.max_medium }
		.large { p.config.max_large }
		.huge { p.config.max_huge }
	}
}

/// get_stats returns the current pool statistics.
pub fn (mut p WriterPool) get_stats() WriterPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }
	return p.stats
}

/// shutdown shuts down the WriterPool.
pub fn (mut p WriterPool) shutdown() {
	p.lock.@lock()
	defer { p.lock.unlock() }
	p.is_running = false
	p.pool.clear()
}

// PooledWriter methods

/// reset resets the writer for reuse.
pub fn (mut w PooledWriter) reset() {
	w.len = 0
	w.data = w.data[..0]
}

/// write writes data to the writer.
pub fn (mut w PooledWriter) write(data []u8) int {
	needed := w.len + data.len
	if needed > w.cap {
		// Capacity exceeded - write only as much as possible
		available := w.cap - w.len
		if available <= 0 {
			return 0
		}
		unsafe {
			C.memcpy(&w.data[w.len], data.data, usize(available))
		}
		w.len += available
		w.data = w.data[..w.len]
		return available
	}

	unsafe {
		C.memcpy(&w.data[w.len], data.data, usize(data.len))
	}
	w.len += data.len
	w.data = w.data[..w.len]
	return data.len
}

/// write_byte writes a single byte.
pub fn (mut w PooledWriter) write_byte(byte u8) bool {
	if w.len >= w.cap {
		return false
	}
	if w.data.len <= w.len {
		w.data << byte
	} else {
		w.data[w.len] = byte
	}
	w.len += 1
	w.data = w.data[..w.len]
	return true
}

/// write_i16_be writes a 16-bit integer in big-endian order.
pub fn (mut w PooledWriter) write_i16_be(val i16) bool {
	if w.len + 2 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 8), u8(val)]
	return w.write(new_data) == 2
}

/// write_i32_be writes a 32-bit integer in big-endian order.
pub fn (mut w PooledWriter) write_i32_be(val i32) bool {
	if w.len + 4 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)]
	return w.write(new_data) == 4
}

/// write_i64_be writes a 64-bit integer in big-endian order.
pub fn (mut w PooledWriter) write_i64_be(val i64) bool {
	if w.len + 8 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 56), u8(val >> 48), u8(val >> 40), u8(val >> 32), u8(val >> 24),
		u8(val >> 16), u8(val >> 8), u8(val)]
	return w.write(new_data) == 8
}

/// bytes returns the writer's data.
pub fn (w &PooledWriter) bytes() []u8 {
	return w.data[..w.len]
}

/// remaining returns the remaining capacity.
pub fn (w &PooledWriter) remaining() int {
	return w.cap - w.len
}

// WriterGuard methods

/// release returns the writer to the pool.
pub fn (mut g WriterGuard) release() {
	if g.active {
		g.pool.return_writer(mut g.writer)
		g.active = false
	}
}

/// get_writer returns a reference to the internal writer.
pub fn (g &WriterGuard) get_writer() &PooledWriter {
	return g.writer
}

// Convenience functions

/// pooled_writer retrieves a BinaryWriter from the pool (uses global pool).
/// Usage: mut writer := pooled_writer(1024)
pub fn pooled_writer(min_size int) &PooledWriter {
	mut pool := get_global_writer_pool()
	return pool.get(min_size)
}

/// release_writer returns a writer to the global pool.
pub fn release_writer(mut writer PooledWriter) {
	mut pool := get_global_writer_pool()
	pool.return_writer(mut writer)
}

/// get_writer_stats returns statistics from the global pool.
pub fn get_writer_stats() WriterPoolStats {
	mut pool := get_global_writer_pool()
	return pool.get_stats()
}

/// reset_writer_pool_stats resets the statistics.
pub fn reset_writer_pool_stats() {
	mut pool := get_global_writer_pool()
	pool.lock.@lock()
	defer { pool.lock.unlock() }
	pool.stats = WriterPoolStats{}
}
