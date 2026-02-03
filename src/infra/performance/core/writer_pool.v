/// 인프라 레이어 - BinaryWriter 풀
/// Kafka 프로토콜 인코딩을 위한 BinaryWriter 재사용 풀
/// GC 압력 감소 및 메모리 할당 최적화
module core

import sync

// ============================================================================
// WriterPool - BinaryWriter 재사용 풀
// ============================================================================

/// WriterPool은 BinaryWriter 객체를 재사용하여 메모리 할당을 최소화합니다.
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

/// PooledWriter는 풀에서 관리되는 BinaryWriter 래퍼입니다.
@[heap]
pub struct PooledWriter {
pub mut:
	data       []u8
	len        int
	cap        int
	size_class SizeClass
}

/// WriterPoolStats는 WriterPool 통계를 담고 있습니다.
pub struct WriterPoolStats {
pub mut:
	hits        u64
	misses      u64
	allocations u64
	returns     u64
	discards    u64
	bytes_saved u64 // 재사용으로 절약된 바이트 수
}

/// WriterGuard는 RAII 스타일의 Writer 관리를 제공합니다.
pub struct WriterGuard {
pub mut:
	pool   &WriterPool
	writer &PooledWriter
	active bool
}

// 전역 WriterPool 인스턴스
__global g_writer_pool = &WriterPool(unsafe { nil })

/// get_global_writer_pool은 전역 WriterPool 인스턴스를 반환합니다.
/// 초기화되지 않은 경우 기본 설정으로 초기화합니다.
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

/// new_writer_pool은 새 WriterPool을 생성합니다.
pub fn new_writer_pool(config PoolConfig) &WriterPool {
	mut pool := &WriterPool{
		config:     config
		pool:       []&PooledWriter{cap: config.max_small}
		is_running: true
	}
	pool.prewarm()
	return pool
}

/// prewarm은 풀을 사전에 Writer로 채웁니다.
fn (mut p WriterPool) prewarm() {
	// small 크기 클래스 위주로 prewarm (가장 흔히 사용됨)
	for _ in 0 .. p.config.prewarm_small {
		p.pool << p.allocate_writer(.small)
	}
}

/// allocate_writer는 지정된 크기 클래스의 새 Writer를 할당합니다.
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

/// get은 풀에서 BinaryWriter를 가져옵니다.
/// 사용 후에는 반드시 return_writer()로 반환해야 합니다.
pub fn (mut p WriterPool) get(min_size int) &PooledWriter {
	if !p.is_running {
		return p.allocate_writer(get_size_class(min_size))
	}

	sc := get_size_class(min_size)

	p.lock.@lock()
	defer { p.lock.unlock() }

	// 풀에서 적절한 크기의 Writer 찾기
	for i := p.pool.len - 1; i >= 0; i-- {
		mut writer := p.pool[i]
		if writer.size_class == sc {
			// 풀에서 제거하고 반환
			p.pool.delete(i)
			writer.reset()
			p.stats.hits += 1
			p.stats.bytes_saved += u64(writer.cap)
			return writer
		}
	}

	// 풀에서 찾지 못함 - 새로 할당
	p.stats.misses += 1
	return p.allocate_writer(sc)
}

/// get_guard는 RAII 스타일의 WriterGuard를 반환합니다.
/// defer로 자동 반환을 보장할 수 있습니다.
pub fn (mut p WriterPool) get_guard(min_size int) WriterGuard {
	writer := p.get(min_size)
	return WriterGuard{
		pool:   p
		writer: writer
		active: true
	}
}

/// return_writer는 Writer를 풀에 반환합니다.
pub fn (mut p WriterPool) return_writer(mut writer PooledWriter) {
	if !p.is_running || &writer == unsafe { nil } {
		return
	}

	p.lock.@lock()
	defer { p.lock.unlock() }

	// 최대 크기 제한 확인
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

/// count_by_class는 지정된 크기 클래스의 Writer 수를 반환합니다.
fn (p &WriterPool) count_by_class(sc SizeClass) int {
	mut count := 0
	for writer in p.pool {
		if writer.size_class == sc {
			count++
		}
	}
	return count
}

/// max_for_class는 지정된 크기 클래스의 최대 Writer 수를 반환합니다.
fn (p &WriterPool) max_for_class(sc SizeClass) int {
	return match sc {
		.tiny { p.config.max_tiny }
		.small { p.config.max_small }
		.medium { p.config.max_medium }
		.large { p.config.max_large }
		.huge { p.config.max_huge }
	}
}

/// get_stats는 현재 풀 통계를 반환합니다.
pub fn (mut p WriterPool) get_stats() WriterPoolStats {
	p.lock.@lock()
	defer { p.lock.unlock() }
	return p.stats
}

/// shutdown은 WriterPool을 종료합니다.
pub fn (mut p WriterPool) shutdown() {
	p.lock.@lock()
	defer { p.lock.unlock() }
	p.is_running = false
	p.pool.clear()
}

// ============================================================================
// PooledWriter 메서드
// ============================================================================

/// reset은 Writer를 재사용을 위해 초기화합니다.
pub fn (mut w PooledWriter) reset() {
	w.len = 0
	w.data = w.data[..0] // 용량은 유지하고 길이만 0으로
}

/// write는 데이터를 Writer에 씁니다.
pub fn (mut w PooledWriter) write(data []u8) int {
	needed := w.len + data.len
	if needed > w.cap {
		// 용량 초과 - 현재 가능한 만큼만 쓰기
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

/// write_byte는 단일 바이트를 씁니다.
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

/// write_i16_be는 16비트 정수를 빅엔디안으로 씁니다.
pub fn (mut w PooledWriter) write_i16_be(val i16) bool {
	if w.len + 2 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 8), u8(val)]
	return w.write(new_data) == 2
}

/// write_i32_be는 32비트 정수를 빅엔디안으로 씁니다.
pub fn (mut w PooledWriter) write_i32_be(val i32) bool {
	if w.len + 4 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)]
	return w.write(new_data) == 4
}

/// write_i64_be는 64비트 정수를 빅엔디안으로 씁니다.
pub fn (mut w PooledWriter) write_i64_be(val i64) bool {
	if w.len + 8 > w.cap {
		return false
	}
	mut new_data := [u8(val >> 56), u8(val >> 48), u8(val >> 40), u8(val >> 32), u8(val >> 24),
		u8(val >> 16), u8(val >> 8), u8(val)]
	return w.write(new_data) == 8
}

/// bytes는 Writer의 데이터를 반환합니다.
pub fn (w &PooledWriter) bytes() []u8 {
	return w.data[..w.len]
}

/// remaining은 남은 용량을 반환합니다.
pub fn (w &PooledWriter) remaining() int {
	return w.cap - w.len
}

// ============================================================================
// WriterGuard 메서드
// ============================================================================

/// release는 Writer를 풀에 반환합니다.
pub fn (mut g WriterGuard) release() {
	if g.active {
		g.pool.return_writer(mut g.writer)
		g.active = false
	}
}

/// get_writer는 내부 Writer에 대한 참조를 반환합니다.
pub fn (g &WriterGuard) get_writer() &PooledWriter {
	return g.writer
}

// ============================================================================
// 편의 함수들
// ============================================================================

/// pooled_writer는 풀에서 BinaryWriter를 가져옵니다 (전역 풀 사용).
/// 사용 예: mut writer := pooled_writer(1024)
pub fn pooled_writer(min_size int) &PooledWriter {
	mut pool := get_global_writer_pool()
	return pool.get(min_size)
}

/// release_writer는 Writer를 전역 풀에 반환합니다.
pub fn release_writer(mut writer PooledWriter) {
	mut pool := get_global_writer_pool()
	pool.return_writer(mut writer)
}

/// get_writer_stats는 전역 풀의 통계를 반환합니다.
pub fn get_writer_stats() WriterPoolStats {
	mut pool := get_global_writer_pool()
	return pool.get_stats()
}

/// reset_writer_pool_stats는 통계를 초기화합니다.
pub fn reset_writer_pool_stats() {
	mut pool := get_global_writer_pool()
	pool.lock.@lock()
	defer { pool.lock.unlock() }
	pool.stats = WriterPoolStats{}
}
