module engines

// io_uring - Linux 5.1+ Async I/O Interface
// Provides high-performance asynchronous I/O using kernel submission/completion queues
//
// Features:
// - Zero-copy I/O operations
// - Batch submission for reduced syscall overhead
// - Polled I/O mode support
// - Link operations for chained requests
//
// Note: Only available on Linux 5.1+, other platforms use fallback
import os

// ============================================================================
// io_uring Constants and Flags
// ============================================================================

$if linux {
	#include <linux/io_uring.h>
	#include <sys/syscall.h>
	#include <sys/mman.h>
	#include <unistd.h>

	// io_uring_setup flags
	const ioring_setup_iopoll = u32(1) // Use polling for I/O completion
	const ioring_setup_sqpoll = u32(2) // Use kernel thread for SQ polling
	const ioring_setup_sq_aff = u32(4) // SQ thread CPU affinity

	// io_uring opcodes
	const ioring_op_nop = u8(0)
	const ioring_op_readv = u8(1)
	const ioring_op_writev = u8(2)
	const ioring_op_fsync = u8(3)
	const ioring_op_read_fixed = u8(4)
	const ioring_op_write_fixed = u8(5)
	const ioring_op_poll_add = u8(6)
	const ioring_op_poll_remove = u8(7)
	const ioring_op_read = u8(22)
	const ioring_op_write = u8(23)

	// Syscall numbers (x86_64)
	const sys_io_uring_setup = 425
	const sys_io_uring_enter = 426
	const sys_io_uring_register = 427

	// io_uring_enter flags
	const ioring_enter_getevents = u32(1)
	const ioring_enter_sq_wakeup = u32(2)

	fn C.syscall(number i64, args ...voidptr) i64
	fn C.mmap(addr voidptr, length usize, prot int, flags int, fd int, offset i64) voidptr
	fn C.munmap(addr voidptr, length usize) int
}

// ============================================================================
// io_uring Structures
// ============================================================================

// IoUringParams holds io_uring setup parameters
pub struct IoUringParams {
pub mut:
	sq_entries     u32
	cq_entries     u32
	flags          u32
	sq_thread_cpu  u32
	sq_thread_idle u32
	features       u32
	resv           [4]u32
	sq_off         SqRingOffsets
	cq_off         CqRingOffsets
}

pub struct SqRingOffsets {
pub:
	head         u32
	tail         u32
	ring_mask    u32
	ring_entries u32
	flags        u32
	dropped      u32
	array        u32
	resv1        u32
	resv2        u64
}

pub struct CqRingOffsets {
pub:
	head         u32
	tail         u32
	ring_mask    u32
	ring_entries u32
	overflow     u32
	cqes         u32
	flags        u32
	resv1        u32
	resv2        u64
}

// IoUringSqe - Submission Queue Entry
pub struct IoUringSqe {
pub mut:
	opcode      u8
	flags       u8
	ioprio      u16
	fd          i32
	off         u64 // offset or addr2
	addr        u64 // buffer address or splice_fd_in
	len         u32 // buffer length or poll events
	rw_flags    u32 // op-specific flags
	user_data   u64 // user data for completion
	buf_index   u16
	personality u16
	splice_fd   i32
	pad2        [2]u64
}

// IoUringCqe - Completion Queue Entry
pub struct IoUringCqe {
pub:
	user_data u64 // correlates to submission
	res       i32 // result (bytes transferred or error)
	flags     u32
}

// ============================================================================
// io_uring Ring Structure
// ============================================================================

// IoUring represents an io_uring instance
pub struct IoUring {
pub mut:
	ring_fd      int
	sq_ring_ptr  voidptr
	cq_ring_ptr  voidptr
	sqes         voidptr
	sq_ring_size usize
	cq_ring_size usize
	sqes_size    usize
	params       IoUringParams
	// Ring state
	sq_head  &u32 = unsafe { nil }
	sq_tail  &u32 = unsafe { nil }
	sq_mask  u32
	sq_array &u32 = unsafe { nil }
	cq_head  &u32 = unsafe { nil }
	cq_tail  &u32 = unsafe { nil }
	cq_mask  u32
	cqes_ptr &IoUringCqe = unsafe { nil }
	// Stats
	submissions u64
	completions u64
	errors      u64
}

// IoUringConfig holds configuration for io_uring setup
pub struct IoUringConfig {
pub:
	queue_depth    u32 = 256
	flags          u32 = 0
	sq_thread_cpu  u32 = 0
	sq_thread_idle u32 = 1000
}

// ============================================================================
// io_uring Result Types
// ============================================================================

pub struct IoUringResult {
pub:
	user_data u64
	result    i32
	success   bool
}

pub struct IoUringStats {
pub:
	submissions u64
	completions u64
	errors      u64
	pending     u64
}

// ============================================================================
// io_uring Implementation
// ============================================================================

// new_io_uring creates a new io_uring instance
pub fn new_io_uring(config IoUringConfig) !IoUring {
	$if linux {
		mut params := IoUringParams{
			sq_entries:     config.queue_depth
			cq_entries:     config.queue_depth * 2
			flags:          config.flags
			sq_thread_cpu:  config.sq_thread_cpu
			sq_thread_idle: config.sq_thread_idle
		}

		// Call io_uring_setup syscall
		fd := C.syscall(sys_io_uring_setup, config.queue_depth, &params)
		if fd < 0 {
			return error('io_uring_setup failed')
		}

		mut ring := IoUring{
			ring_fd: int(fd)
			params:  params
			sq_mask: params.sq_entries - 1
			cq_mask: params.cq_entries - 1
		}

		// Map submission queue ring
		sq_ring_sz := usize(params.sq_off.array) + usize(params.sq_entries) * sizeof(u32)
		ring.sq_ring_size = sq_ring_sz
		ring.sq_ring_ptr = C.mmap(unsafe { nil }, sq_ring_sz, 0x1 | 0x2, 0x1, int(fd),
			0)
		if ring.sq_ring_ptr == voidptr(-1) {
			return error('mmap sq_ring failed')
		}

		// Map completion queue ring
		cq_ring_sz := usize(params.cq_off.cqes) + usize(params.cq_entries) * sizeof(IoUringCqe)
		ring.cq_ring_size = cq_ring_sz
		ring.cq_ring_ptr = C.mmap(unsafe { nil }, cq_ring_sz, 0x1 | 0x2, 0x1, int(fd),
			0x8000000)
		if ring.cq_ring_ptr == voidptr(-1) {
			return error('mmap cq_ring failed')
		}

		// Map SQEs
		sqes_sz := usize(params.sq_entries) * sizeof(IoUringSqe)
		ring.sqes_size = sqes_sz
		ring.sqes = C.mmap(unsafe { nil }, sqes_sz, 0x1 | 0x2, 0x1, int(fd), 0x10000000)
		if ring.sqes == voidptr(-1) {
			return error('mmap sqes failed')
		}

		// Setup ring pointers
		ring.sq_head = unsafe { &u32(voidptr(usize(ring.sq_ring_ptr) + params.sq_off.head)) }
		ring.sq_tail = unsafe { &u32(voidptr(usize(ring.sq_ring_ptr) + params.sq_off.tail)) }
		ring.sq_array = unsafe { &u32(voidptr(usize(ring.sq_ring_ptr) + params.sq_off.array)) }
		ring.cq_head = unsafe { &u32(voidptr(usize(ring.cq_ring_ptr) + params.cq_off.head)) }
		ring.cq_tail = unsafe { &u32(voidptr(usize(ring.cq_ring_ptr) + params.cq_off.tail)) }
		ring.cqes_ptr = unsafe { &IoUringCqe(voidptr(usize(ring.cq_ring_ptr) + params.cq_off.cqes)) }

		return ring
	} $else {
		return error('io_uring is only available on Linux 5.1+')
	}
}

// close releases io_uring resources
pub fn (mut r IoUring) close() {
	$if linux {
		if r.sq_ring_ptr != unsafe { nil } {
			C.munmap(r.sq_ring_ptr, r.sq_ring_size)
		}
		if r.cq_ring_ptr != unsafe { nil } {
			C.munmap(r.cq_ring_ptr, r.cq_ring_size)
		}
		if r.sqes != unsafe { nil } {
			C.munmap(r.sqes, r.sqes_size)
		}
		if r.ring_fd >= 0 {
			os.fd_close(r.ring_fd)
		}
	}
}

// get_sqe gets a free submission queue entry
pub fn (mut r IoUring) get_sqe() ?&IoUringSqe {
	$if linux {
		head := unsafe { *r.sq_head }
		tail := unsafe { *r.sq_tail }
		next := tail + 1

		if (next - head) > r.params.sq_entries {
			return none // Queue full
		}

		idx := tail & r.sq_mask
		sqe := unsafe { &IoUringSqe(voidptr(usize(r.sqes) + usize(idx) * sizeof(IoUringSqe))) }

		// Clear the SQE
		unsafe {
			C.memset(sqe, 0, sizeof(IoUringSqe))
		}

		return sqe
	} $else {
		return none
	}
}

// submit_sqe submits the current SQE
pub fn (mut r IoUring) submit_sqe() {
	$if linux {
		tail := unsafe { *r.sq_tail }
		idx := tail & r.sq_mask
		unsafe {
			r.sq_array[idx] = idx
			*r.sq_tail = tail + 1
		}
		r.submissions++
	}
}

// submit submits pending SQEs and optionally waits for completions
pub fn (mut r IoUring) submit(wait_nr u32) !int {
	$if linux {
		head := unsafe { *r.sq_head }
		tail := unsafe { *r.sq_tail }
		to_submit := tail - head

		if to_submit == 0 && wait_nr == 0 {
			return 0
		}

		flags := if wait_nr > 0 { ioring_enter_getevents } else { u32(0) }
		ret := C.syscall(sys_io_uring_enter, r.ring_fd, to_submit, wait_nr, flags, unsafe { nil })

		if ret < 0 {
			r.errors++
			return error('io_uring_enter failed')
		}

		return int(ret)
	} $else {
		return error('io_uring not available')
	}
}

// peek_cqe peeks at the next completion without consuming
pub fn (r &IoUring) peek_cqe() ?&IoUringCqe {
	$if linux {
		head := unsafe { *r.cq_head }
		tail := unsafe { *r.cq_tail }

		if head == tail {
			return none // No completions
		}

		idx := head & r.cq_mask
		return unsafe { &r.cqes_ptr[idx] }
	} $else {
		return none
	}
}

// consume_cqe marks the current CQE as consumed
pub fn (mut r IoUring) consume_cqe() {
	$if linux {
		unsafe {
			*r.cq_head = *r.cq_head + 1
		}
		r.completions++
	}
}

// wait_cqe waits for at least one completion
pub fn (mut r IoUring) wait_cqe() !IoUringResult {
	$if linux {
		// First check if there's already a completion
		if cqe := r.peek_cqe() {
			result := IoUringResult{
				user_data: cqe.user_data
				result:    cqe.res
				success:   cqe.res >= 0
			}
			r.consume_cqe()
			return result
		}

		// Wait for completion
		r.submit(1)!

		if cqe := r.peek_cqe() {
			result := IoUringResult{
				user_data: cqe.user_data
				result:    cqe.res
				success:   cqe.res >= 0
			}
			r.consume_cqe()
			return result
		}

		return error('no completion available')
	} $else {
		return error('io_uring not available')
	}
}

// get_stats returns io_uring statistics
pub fn (r &IoUring) get_stats() IoUringStats {
	$if linux {
		head := unsafe { *r.sq_head }
		tail := unsafe { *r.sq_tail }
		pending := u64(tail - head)

		return IoUringStats{
			submissions: r.submissions
			completions: r.completions
			errors:      r.errors
			pending:     pending
		}
	} $else {
		return IoUringStats{}
	}
}

// ============================================================================
// High-Level Operations
// ============================================================================

// prep_read prepares a read operation
pub fn (mut r IoUring) prep_read(fd int, buf []u8, offset i64, user_data u64) bool {
	$if linux {
		sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_read
		sqe.fd = i32(fd)
		sqe.off = u64(offset)
		sqe.addr = u64(usize(buf.data))
		sqe.len = u32(buf.len)
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

// prep_write prepares a write operation
pub fn (mut r IoUring) prep_write(fd int, buf []u8, offset i64, user_data u64) bool {
	$if linux {
		sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_write
		sqe.fd = i32(fd)
		sqe.off = u64(offset)
		sqe.addr = u64(usize(buf.data))
		sqe.len = u32(buf.len)
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

// prep_fsync prepares an fsync operation
pub fn (mut r IoUring) prep_fsync(fd int, user_data u64) bool {
	$if linux {
		sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_fsync
		sqe.fd = i32(fd)
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

// prep_nop prepares a no-op (useful for testing)
pub fn (mut r IoUring) prep_nop(user_data u64) bool {
	$if linux {
		sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_nop
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

// ============================================================================
// Batch Operations
// ============================================================================

// BatchOp represents a batch operation
pub struct BatchOp {
pub:
	op_type   BatchOpType
	fd        int
	buf       []u8
	offset    i64
	user_data u64
}

pub enum BatchOpType {
	read
	write
	fsync
	nop
}

// submit_batch submits multiple operations at once
pub fn (mut r IoUring) submit_batch(ops []BatchOp) !int {
	$if linux {
		mut submitted := 0

		for op in ops {
			success := match op.op_type {
				.read { r.prep_read(op.fd, op.buf, op.offset, op.user_data) }
				.write { r.prep_write(op.buf, op.buf, op.offset, op.user_data) }
				.fsync { r.prep_fsync(op.fd, op.user_data) }
				.nop { r.prep_nop(op.user_data) }
			}
			if success {
				submitted++
			}
		}

		return r.submit(0)
	} $else {
		return error('io_uring not available')
	}
}

// wait_batch waits for multiple completions
pub fn (mut r IoUring) wait_batch(count int) ![]IoUringResult {
	$if linux {
		mut results := []IoUringResult{cap: count}

		for _ in 0 .. count {
			result := r.wait_cqe()!
			results << result
		}

		return results
	} $else {
		return error('io_uring not available')
	}
}

// ============================================================================
// Fallback Implementation for Non-Linux
// ============================================================================

// IoUringFallback provides synchronous fallback for non-Linux systems
pub struct IoUringFallback {
pub mut:
	operations u64
}

// new_io_uring_fallback creates a fallback handler
pub fn new_io_uring_fallback() IoUringFallback {
	return IoUringFallback{}
}

// sync_read performs synchronous read (fallback)
pub fn (mut f IoUringFallback) sync_read(mut file os.File, mut buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_read := file.read(mut buf) or { return error('read failed: ${err}') }
	f.operations++
	return bytes_read
}

// sync_write performs synchronous write (fallback)
pub fn (mut f IoUringFallback) sync_write(mut file os.File, buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_written := file.write(buf) or { return error('write failed: ${err}') }
	f.operations++
	return bytes_written
}

// ============================================================================
// Platform-Independent Async I/O Interface
// ============================================================================

// AsyncIoCapabilities indicates platform support
pub struct AsyncIoCapabilities {
pub:
	has_io_uring  bool
	has_aio       bool // POSIX AIO
	has_iocp      bool // Windows IOCP
	platform_name string
}

// get_async_io_capabilities returns available async I/O features
pub fn get_async_io_capabilities() AsyncIoCapabilities {
	$if linux {
		return AsyncIoCapabilities{
			has_io_uring:  true
			has_aio:       true
			has_iocp:      false
			platform_name: 'Linux'
		}
	} $else $if macos {
		return AsyncIoCapabilities{
			has_io_uring:  false
			has_aio:       true
			has_iocp:      false
			platform_name: 'macOS'
		}
	} $else $if windows {
		return AsyncIoCapabilities{
			has_io_uring:  false
			has_aio:       false
			has_iocp:      true
			platform_name: 'Windows'
		}
	} $else {
		return AsyncIoCapabilities{
			has_io_uring:  false
			has_aio:       false
			has_iocp:      false
			platform_name: 'Unknown'
		}
	}
}

// is_io_uring_available checks if io_uring is available at runtime
pub fn is_io_uring_available() bool {
	$if linux {
		// Try to create a minimal io_uring to check availability
		ring := new_io_uring(IoUringConfig{ queue_depth: 1 }) or { return false }
		mut r := ring
		r.close()
		return true
	} $else {
		return false
	}
}
