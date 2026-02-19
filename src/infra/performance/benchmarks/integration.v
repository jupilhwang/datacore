/// 인프라 레이어 - 성능 통합 모듈
/// 버퍼 풀, 객체 풀, 제로카피를 핵심 컴포넌트에 통합합니다.
module benchmarks

import time
import infra.performance.core
import infra.performance

// 전역 성능 관리자 프록시

/// get_global_performance는 루트 모듈에서 전역 성능 관리자를 반환합니다.
pub fn get_global_performance() &performance.PerformanceManager {
	return performance.get_global_performance()
}

/// init_global_performance는 전역 성능 관리자를 초기화합니다.
pub fn init_global_performance(config performance.PerformanceConfig) {
	performance.init_global_performance(config)
}

// TCP 서버 통합 - 버퍼 할당 헬퍼

/// RequestBuffer는 요청 처리를 위한 풀링된 버퍼를 래핑합니다.
@[heap]
pub struct RequestBuffer {
pub mut:
	buffer     &core.Buffer                    // 내부 버퍼
	manager    &performance.PerformanceManager // 성능 관리자 참조
	created_at time.Time
}

/// new_request_buffer는 요청 처리를 위한 버퍼를 획득합니다.
pub fn new_request_buffer(size int) &RequestBuffer {
	mut mgr := get_global_performance()
	return &RequestBuffer{
		buffer:     mgr.get_buffer(size)
		manager:    mgr
		created_at: time.now()
	}
}

/// data는 내부 바이트 슬라이스를 반환합니다.
pub fn (r &RequestBuffer) data() []u8 {
	return r.buffer.data
}

/// resize는 필요한 경우 버퍼 크기를 조정합니다.
pub fn (mut r RequestBuffer) resize(new_size int) {
	if new_size > r.buffer.cap {
		// 기존 버퍼 반환 후 더 큰 새 버퍼 획득
		r.manager.put_buffer(r.buffer)
		r.buffer = r.manager.get_buffer(new_size)
	}
	r.buffer.len = new_size
}

/// release는 버퍼를 풀에 반환합니다.
pub fn (mut r RequestBuffer) release() {
	r.manager.put_buffer(r.buffer)
}

/// ResponseBuffer는 응답 빌드를 위한 풀링된 버퍼를 래핑합니다.
@[heap]
pub struct ResponseBuffer {
pub mut:
	buffer  &core.Buffer                    // 내부 버퍼
	manager &performance.PerformanceManager // 성능 관리자 참조
	offset  int // 현재 쓰기 위치
}

/// new_response_buffer는 응답 빌드를 위한 버퍼를 획득합니다.
pub fn new_response_buffer(estimated_size int) &ResponseBuffer {
	mut mgr := get_global_performance()
	return &ResponseBuffer{
		buffer:  mgr.get_buffer(estimated_size)
		manager: mgr
		offset:  0
	}
}

/// write는 응답 버퍼에 데이터를 추가합니다.
pub fn (mut r ResponseBuffer) write(data []u8) {
	needed := r.offset + data.len
	if needed > r.buffer.cap {
		// 더 큰 버퍼 필요 - 새 버퍼 획득 후 복사
		mut new_buf := r.manager.get_buffer(needed * 2)
		for i in 0 .. r.offset {
			new_buf.data[i] = r.buffer.data[i]
		}
		r.manager.put_buffer(r.buffer)
		r.buffer = new_buf
	}

	for i, b in data {
		r.buffer.data[r.offset + i] = b
	}
	r.offset += data.len
	r.buffer.len = r.offset
}

/// write_i32_be는 빅엔디안 i32를 씁니다.
pub fn (mut r ResponseBuffer) write_i32_be(val i32) {
	r.write([u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)])
}

/// write_i16_be는 빅엔디안 i16을 씁니다.
pub fn (mut r ResponseBuffer) write_i16_be(val i16) {
	r.write([u8(val >> 8), u8(val)])
}

/// bytes는 쓰여진 바이트들을 반환합니다.
pub fn (r &ResponseBuffer) bytes() []u8 {
	return r.buffer.data[..r.offset]
}

/// len은 현재 길이를 반환합니다.
pub fn (r &ResponseBuffer) len() int {
	return r.offset
}

/// release는 버퍼를 풀에 반환합니다.
pub fn (mut r ResponseBuffer) release() {
	r.manager.put_buffer(r.buffer)
}

// 연결 통합 - 재사용 가능한 읽기/쓰기 버퍼

/// ConnectionBuffers는 연결을 위한 재사용 가능한 버퍼를 보유합니다.
@[heap]
pub struct ConnectionBuffers {
pub mut:
	read_buffer  &core.Buffer                    // 읽기 버퍼
	write_buffer &core.Buffer                    // 쓰기 버퍼
	manager      &performance.PerformanceManager // 성능 관리자 참조
}

/// new_connection_buffers는 연결 버퍼를 생성합니다.
pub fn new_connection_buffers(read_size int, write_size int) &ConnectionBuffers {
	mut mgr := get_global_performance()
	return &ConnectionBuffers{
		read_buffer:  mgr.get_buffer(read_size)
		write_buffer: mgr.get_buffer(write_size)
		manager:      mgr
	}
}

/// get_read_slice는 읽기용 슬라이스를 반환합니다.
pub fn (c &ConnectionBuffers) get_read_slice(size int) []u8 {
	if size <= c.read_buffer.cap {
		return c.read_buffer.data[..size]
	}
	return []u8{len: size}
}

/// get_write_slice는 쓰기용 슬라이스를 반환합니다.
pub fn (c &ConnectionBuffers) get_write_slice(size int) []u8 {
	if size <= c.write_buffer.cap {
		return c.write_buffer.data[..size]
	}
	return []u8{len: size}
}

/// release는 버퍼들을 풀에 반환합니다.
pub fn (mut c ConnectionBuffers) release() {
	c.manager.put_buffer(c.read_buffer)
	c.manager.put_buffer(c.write_buffer)
}

// 스토리지 통합 - 풀링된 레코드

/// StorageRecordPool은 스토리지 작업을 위한 레코드 풀링을 제공합니다.
@[heap]
pub struct StorageRecordPool {
mut:
	manager &performance.PerformanceManager // 성능 관리자 참조
}

/// new_storage_record_pool은 스토리지 레코드 풀을 생성합니다.
pub fn new_storage_record_pool() &StorageRecordPool {
	return &StorageRecordPool{
		manager: get_global_performance()
	}
}

/// get_record는 풀링된 레코드를 획득합니다.
pub fn (mut p StorageRecordPool) get_record() &core.PooledRecord {
	return p.manager.get_record()
}

/// put_record는 레코드를 풀에 반환합니다.
pub fn (mut p StorageRecordPool) put_record(r &core.PooledRecord) {
	p.manager.put_record(r)
}

/// get_batch는 풀링된 배치를 획득합니다.
pub fn (mut p StorageRecordPool) get_batch() &core.PooledRecordBatch {
	return p.manager.get_batch()
}

/// put_batch는 배치를 풀에 반환합니다.
pub fn (mut p StorageRecordPool) put_batch(b &core.PooledRecordBatch) {
	p.manager.put_batch(b)
}

// Fetch 핸들러 통합 - 제로카피 지원

/// FetchBuffer는 제로카피 지원과 함께 fetch 응답 데이터를 보유합니다.
@[heap]
pub struct FetchBuffer {
pub mut:
	buffer        &core.Buffer                    // 내부 버퍼
	manager       &performance.PerformanceManager // 성능 관리자 참조
	zero_copy_fd  int // 제로카피용 파일 디스크립터 (미사용 시 -1)
	zero_copy_off i64 // 제로카피 오프셋
	zero_copy_len int // 제로카피 길이
}

/// new_fetch_buffer는 fetch 버퍼를 생성합니다.
pub fn new_fetch_buffer(size int) &FetchBuffer {
	mut mgr := get_global_performance()
	return &FetchBuffer{
		buffer:        mgr.get_buffer(size)
		manager:       mgr
		zero_copy_fd:  -1
		zero_copy_off: 0
		zero_copy_len: 0
	}
}

/// set_zero_copy는 제로카피 전송을 설정합니다.
pub fn (mut f FetchBuffer) set_zero_copy(fd int, offset i64, length int) {
	f.zero_copy_fd = fd
	f.zero_copy_off = offset
	f.zero_copy_len = length
}

/// has_zero_copy는 제로카피가 사용 가능한지 확인합니다.
pub fn (f &FetchBuffer) has_zero_copy() bool {
	return f.zero_copy_fd >= 0 && f.zero_copy_len > 0
}

/// release는 버퍼를 풀에 반환합니다.
pub fn (mut f FetchBuffer) release() {
	f.manager.put_buffer(f.buffer)
}

// 통합 통계

/// IntegrationStats는 통합 통계를 보유합니다.
pub struct IntegrationStats {
pub:
	request_buffers_allocated  u64 // 할당된 요청 버퍼 수
	response_buffers_allocated u64 // 할당된 응답 버퍼 수
	connection_buffers_active  int // 활성 연결 버퍼 수
	storage_records_pooled     u64 // 풀링된 스토리지 레코드 수
	fetch_zero_copy_count      u64 // 제로카피 fetch 횟수
	perf_stats                 performance.PerformanceStats // 성능 통계
}

/// IntegrationMetrics는 통합 사용량을 추적합니다.
@[heap]
pub struct IntegrationMetrics {
pub mut:
	request_buffers_allocated  u64 // 할당된 요청 버퍼 수
	response_buffers_allocated u64 // 할당된 응답 버퍼 수
	connection_buffers_active  int // 활성 연결 버퍼 수
	storage_records_pooled     u64 // 풀링된 스토리지 레코드 수
	fetch_zero_copy_count      u64 // 제로카피 fetch 횟수
}

/// 메트릭 싱글톤
fn get_metrics() &IntegrationMetrics {
	return &IntegrationMetrics{}
}

/// get_integration_stats는 통합 통계를 반환합니다.
pub fn get_integration_stats() IntegrationStats {
	mut mgr := get_global_performance()
	metrics := get_metrics()
	return IntegrationStats{
		request_buffers_allocated:  metrics.request_buffers_allocated
		response_buffers_allocated: metrics.response_buffers_allocated
		connection_buffers_active:  metrics.connection_buffers_active
		storage_records_pooled:     metrics.storage_records_pooled
		fetch_zero_copy_count:      metrics.fetch_zero_copy_count
		perf_stats:                 mgr.get_stats()
	}
}

// 편의 함수

/// with_request_buffer는 풀링된 요청 버퍼로 함수를 실행합니다.
pub fn with_request_buffer(size int, f fn (mut RequestBuffer)) {
	mut buf := new_request_buffer(size)
	defer { buf.release() }
	f(mut buf)
}

/// with_response_buffer는 풀링된 응답 버퍼로 함수를 실행합니다.
pub fn with_response_buffer(size int, f fn (mut ResponseBuffer)) {
	mut buf := new_response_buffer(size)
	defer { buf.release() }
	f(mut buf)
}

/// allocate_request_buffer는 요청 버퍼를 할당하고 추적합니다.
pub fn allocate_request_buffer(size int) &RequestBuffer {
	return new_request_buffer(size)
}

/// allocate_response_buffer는 응답 버퍼를 할당하고 추적합니다.
pub fn allocate_response_buffer(size int) &ResponseBuffer {
	return new_response_buffer(size)
}
