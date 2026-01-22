/// 범용 성능 엔진
/// 모든 플랫폼에서 동작하는 기본 성능 최적화 엔진
module engines

import os
import infra.performance.core

/// GenericPerformanceEngine은 범용 성능 최적화 엔진입니다.
pub struct GenericPerformanceEngine {
pub mut:
	buffer_pool  &core.BufferPool      = unsafe { nil } // 버퍼 풀
	record_pool  &core.RecordPool      = unsafe { nil } // 레코드 풀
	batch_pool   &core.RecordBatchPool = unsafe { nil } // 배치 풀
	request_pool &core.RequestPool     = unsafe { nil } // 요청 풀
	config       core.PerformanceConfig                 // 성능 설정
}

/// name은 엔진 이름을 반환합니다.
pub fn (e GenericPerformanceEngine) name() string {
	return 'Generic'
}

/// init은 주어진 설정으로 엔진을 초기화합니다.
pub fn (mut e GenericPerformanceEngine) init(config core.PerformanceConfig) ! {
	e.config = config

	pool_config := core.PoolConfig{
		max_tiny:       config.buffer_pool_max_tiny
		max_small:      config.buffer_pool_max_small
		max_medium:     config.buffer_pool_max_medium
		max_large:      config.buffer_pool_max_large
		max_huge:       config.buffer_pool_max_huge
		prewarm_tiny:   if config.buffer_pool_prewarm { 100 } else { 0 }
		prewarm_small:  if config.buffer_pool_prewarm { 50 } else { 0 }
		prewarm_medium: if config.buffer_pool_prewarm { 10 } else { 0 }
		prewarm_large:  if config.buffer_pool_prewarm { 2 } else { 0 }
	}

	e.buffer_pool = core.new_buffer_pool(pool_config)
	e.record_pool = core.new_record_pool(config.record_pool_max_size)
	e.batch_pool = core.new_record_batch_pool(config.batch_pool_max_size)
	e.request_pool = core.new_request_pool(config.request_pool_max_size)
}

/// get_buffer는 지정된 크기의 버퍼를 가져옵니다.
pub fn (mut e GenericPerformanceEngine) get_buffer(size int) &core.Buffer {
	return e.buffer_pool.get(size)
}

/// put_buffer는 버퍼를 풀에 반환합니다.
pub fn (mut e GenericPerformanceEngine) put_buffer(buf &core.Buffer) {
	e.buffer_pool.put(buf)
}

/// get_record는 풀에서 레코드를 가져옵니다.
pub fn (mut e GenericPerformanceEngine) get_record() &core.PooledRecord {
	return e.record_pool.get()
}

/// put_record는 레코드를 풀에 반환합니다.
pub fn (mut e GenericPerformanceEngine) put_record(r &core.PooledRecord) {
	e.record_pool.put(r)
}

/// get_batch는 풀에서 배치를 가져옵니다.
pub fn (mut e GenericPerformanceEngine) get_batch() &core.PooledRecordBatch {
	return e.batch_pool.get()
}

/// put_batch는 배치를 풀에 반환합니다.
pub fn (mut e GenericPerformanceEngine) put_batch(b &core.PooledRecordBatch) {
	e.batch_pool.put(b)
}

/// get_request는 풀에서 요청을 가져옵니다.
pub fn (mut e GenericPerformanceEngine) get_request() &core.PooledRequest {
	return e.request_pool.get()
}

/// put_request는 요청을 풀에 반환합니다.
pub fn (mut e GenericPerformanceEngine) put_request(r &core.PooledRequest) {
	e.request_pool.put(r)
}

/// read_file_at은 파일의 지정된 오프셋에서 데이터를 읽습니다.
pub fn (mut e GenericPerformanceEngine) read_file_at(path string, offset i64, size int) ![]u8 {
	// 표준 폴백: 가능하면 mmap 사용, 그렇지 않으면 읽기
	mut f := os.open(path)!
	defer { f.close() }
	f.seek(offset, .start)!
	mut buf := []u8{len: size}
	f.read(mut buf)!
	return buf
}

/// write_file_at은 파일의 지정된 오프셋에 데이터를 씁니다.
pub fn (mut e GenericPerformanceEngine) write_file_at(path string, offset i64, data []u8) ! {
	mut f := os.open_file(path, 'r+', 0o644)!
	defer { f.close() }
	f.seek(offset, .start)!
	f.write(data)!
}

/// get_stats는 현재 성능 통계를 반환합니다.
pub fn (mut e GenericPerformanceEngine) get_stats() core.PerformanceStats {
	buf_stats := e.buffer_pool.get_stats()
	return core.PerformanceStats{
		engine_name:   e.name()
		buffer_hits:   buf_stats.total_hits()
		buffer_misses: buf_stats.total_misses()
		ops_count:     0 // TODO: 다른 작업 추적
	}
}
