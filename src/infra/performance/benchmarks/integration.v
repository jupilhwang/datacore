// Infra Layer - Performance Integration Module
// Integrates Buffer Pool, Object Pool, and Zero-Copy into core components
module benchmarks

import time
import infra.performance.core
import infra.performance

// ============================================================================
// Global Performance Manager Proxy
// ============================================================================

// get_global_performance returns the global performance manager from the root module
pub fn get_global_performance() &performance.PerformanceManager {
	return performance.get_global_performance()
}

// init_global_performance initializes the global performance manager
pub fn init_global_performance(config performance.PerformanceConfig) {
	performance.init_global_performance(config)
}

// ============================================================================
// TCP Server Integration - Buffer Allocation Helpers
// ============================================================================

// RequestBuffer wraps a pooled buffer for request processing
@[heap]
pub struct RequestBuffer {
pub mut:
	buffer     &core.Buffer
	manager    &performance.PerformanceManager
	created_at time.Time
}

// new_request_buffer gets a buffer for request processing
pub fn new_request_buffer(size int) &RequestBuffer {
	mut mgr := get_global_performance()
	return &RequestBuffer{
		buffer:     mgr.get_buffer(size)
		manager:    mgr
		created_at: time.now()
	}
}

// data returns the underlying byte slice
pub fn (r &RequestBuffer) data() []u8 {
	return r.buffer.data
}

// resize resizes the buffer if needed
pub fn (mut r RequestBuffer) resize(new_size int) {
	if new_size > r.buffer.cap {
		// Return old buffer and get a new larger one
		r.manager.put_buffer(r.buffer)
		r.buffer = r.manager.get_buffer(new_size)
	}
	r.buffer.len = new_size
}

// release returns the buffer to the pool
pub fn (mut r RequestBuffer) release() {
	r.manager.put_buffer(r.buffer)
}

// ResponseBuffer wraps a pooled buffer for response building
@[heap]
pub struct ResponseBuffer {
pub mut:
	buffer  &core.Buffer
	manager &performance.PerformanceManager
	offset  int // Current write position
}

// new_response_buffer gets a buffer for response building
pub fn new_response_buffer(estimated_size int) &ResponseBuffer {
	mut mgr := get_global_performance()
	return &ResponseBuffer{
		buffer:  mgr.get_buffer(estimated_size)
		manager: mgr
		offset:  0
	}
}

// write appends data to the response buffer
pub fn (mut r ResponseBuffer) write(data []u8) {
	needed := r.offset + data.len
	if needed > r.buffer.cap {
		// Need larger buffer - get new one and copy
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

// write_i32_be writes a big-endian i32
pub fn (mut r ResponseBuffer) write_i32_be(val i32) {
	r.write([u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)])
}

// write_i16_be writes a big-endian i16
pub fn (mut r ResponseBuffer) write_i16_be(val i16) {
	r.write([u8(val >> 8), u8(val)])
}

// bytes returns the written bytes
pub fn (r &ResponseBuffer) bytes() []u8 {
	return r.buffer.data[..r.offset]
}

// len returns the current length
pub fn (r &ResponseBuffer) len() int {
	return r.offset
}

// release returns the buffer to the pool
pub fn (mut r ResponseBuffer) release() {
	r.manager.put_buffer(r.buffer)
}

// ============================================================================
// Connection Integration - Reusable Read/Write Buffers
// ============================================================================

// ConnectionBuffers holds reusable buffers for a connection
@[heap]
pub struct ConnectionBuffers {
pub mut:
	read_buffer  &core.Buffer
	write_buffer &core.Buffer
	manager      &performance.PerformanceManager
}

// new_connection_buffers creates connection buffers
pub fn new_connection_buffers(read_size int, write_size int) &ConnectionBuffers {
	mut mgr := get_global_performance()
	return &ConnectionBuffers{
		read_buffer:  mgr.get_buffer(read_size)
		write_buffer: mgr.get_buffer(write_size)
		manager:      mgr
	}
}

// get_read_slice returns a slice for reading
pub fn (c &ConnectionBuffers) get_read_slice(size int) []u8 {
	if size <= c.read_buffer.cap {
		return c.read_buffer.data[..size]
	}
	return []u8{len: size}
}

// get_write_slice returns a slice for writing
pub fn (c &ConnectionBuffers) get_write_slice(size int) []u8 {
	if size <= c.write_buffer.cap {
		return c.write_buffer.data[..size]
	}
	return []u8{len: size}
}

// release returns buffers to the pool
pub fn (mut c ConnectionBuffers) release() {
	c.manager.put_buffer(c.read_buffer)
	c.manager.put_buffer(c.write_buffer)
}

// ============================================================================
// Storage Integration - Pooled Records
// ============================================================================

// StorageRecordPool provides record pooling for storage operations
@[heap]
pub struct StorageRecordPool {
mut:
	manager &performance.PerformanceManager
}

// new_storage_record_pool creates a storage record pool
pub fn new_storage_record_pool() &StorageRecordPool {
	return &StorageRecordPool{
		manager: get_global_performance()
	}
}

// get_record gets a pooled record
pub fn (mut p StorageRecordPool) get_record() &core.PooledRecord {
	return p.manager.get_record()
}

// put_record returns a record to the pool
pub fn (mut p StorageRecordPool) put_record(r &core.PooledRecord) {
	p.manager.put_record(r)
}

// get_batch gets a pooled batch
pub fn (mut p StorageRecordPool) get_batch() &core.PooledRecordBatch {
	return p.manager.get_batch()
}

// put_batch returns a batch to the pool
pub fn (mut p StorageRecordPool) put_batch(b &core.PooledRecordBatch) {
	p.manager.put_batch(b)
}

// ============================================================================
// Fetch Handler Integration - Zero-Copy Support
// ============================================================================

// FetchBuffer holds fetch response data with zero-copy support
@[heap]
pub struct FetchBuffer {
pub mut:
	buffer        &core.Buffer
	manager       &performance.PerformanceManager
	zero_copy_fd  int // File descriptor for zero-copy (-1 if not used)
	zero_copy_off i64 // Offset for zero-copy
	zero_copy_len int // Length for zero-copy
}

// new_fetch_buffer creates a fetch buffer
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

// set_zero_copy sets up zero-copy transfer
pub fn (mut f FetchBuffer) set_zero_copy(fd int, offset i64, length int) {
	f.zero_copy_fd = fd
	f.zero_copy_off = offset
	f.zero_copy_len = length
}

// has_zero_copy checks if zero-copy is available
pub fn (f &FetchBuffer) has_zero_copy() bool {
	return f.zero_copy_fd >= 0 && f.zero_copy_len > 0
}

// release returns the buffer to the pool
pub fn (mut f FetchBuffer) release() {
	f.manager.put_buffer(f.buffer)
}

// ============================================================================
// Integration Statistics
// ============================================================================

// IntegrationStats holds integration statistics
pub struct IntegrationStats {
pub:
	request_buffers_allocated  u64
	response_buffers_allocated u64
	connection_buffers_active  int
	storage_records_pooled     u64
	fetch_zero_copy_count      u64
	perf_stats                 performance.PerformanceStats
}

// IntegrationMetrics tracks integration usage
@[heap]
pub struct IntegrationMetrics {
pub mut:
	request_buffers_allocated  u64
	response_buffers_allocated u64
	connection_buffers_active  int
	storage_records_pooled     u64
	fetch_zero_copy_count      u64
}

// Metrics singleton
fn get_metrics() &IntegrationMetrics {
	return &IntegrationMetrics{}
}

// get_integration_stats returns integration statistics
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

// ============================================================================
// Convenience Functions
// ============================================================================

// with_request_buffer executes a function with a pooled request buffer
pub fn with_request_buffer(size int, f fn (mut RequestBuffer)) {
	mut buf := new_request_buffer(size)
	defer { buf.release() }
	f(mut buf)
}

// with_response_buffer executes a function with a pooled response buffer
pub fn with_response_buffer(size int, f fn (mut ResponseBuffer)) {
	mut buf := new_response_buffer(size)
	defer { buf.release() }
	f(mut buf)
}

// allocate_request_buffer allocates and tracks a request buffer
pub fn allocate_request_buffer(size int) &RequestBuffer {
	return new_request_buffer(size)
}

// allocate_response_buffer allocates and tracks a response buffer
pub fn allocate_response_buffer(size int) &ResponseBuffer {
	return new_response_buffer(size)
}
