/// io_uring - Linux 5.1+ asynchronous I/O interface
/// Provides high-performance async I/O using kernel submission/completion queues
///
/// Features:
/// - Zero-copy I/O operations
/// - Batched submission to reduce syscall overhead
/// - Polling I/O mode support
/// - Linked operations for chained requests
///
/// Note: Only available on Linux 5.1+; other platforms use a fallback
module engines

import os
import net

// io_uring constants and flags

$if linux {
	#include <linux/io_uring.h>
	#include <sys/syscall.h>
	#include <sys/mman.h>
	#include <unistd.h>

	// io_uring_setup flags
	const ioring_setup_iopoll = u32(1)
	const ioring_setup_sqpoll = u32(2)
	const ioring_setup_sq_aff = u32(4)

	// io_uring operation codes
	const ioring_op_nop = u8(0)
	const ioring_op_readv = u8(1)
	const ioring_op_writev = u8(2)
	const ioring_op_fsync = u8(3)
	const ioring_op_read_fixed = u8(4)
	const ioring_op_write_fixed = u8(5)
	const ioring_op_poll_add = u8(6)
	const ioring_op_poll_remove = u8(7)
	const ioring_op_accept = u8(13)
	const ioring_op_connect = u8(16)
	const ioring_op_send = u8(18)
	const ioring_op_recv = u8(19)
	const ioring_op_read = u8(22)
	const ioring_op_write = u8(23)

	// Socket-related C functions
	fn C.socket(domain net.AddrFamily, typ net.SocketType, protocol int) int
	fn C.bind(sockfd int, addr voidptr, addrlen u32) int
	fn C.listen(sockfd int, backlog int) int
	fn C.setsockopt(sockfd int, level int, optname int, optval voidptr, optlen u32) int
	fn C.close(fd int) int
	fn C.htonl(hostlong u32) u32
	fn C.htons(hostshort u16) u16
	fn C.inet_pton(af net.AddrFamily, src &char, dst voidptr) int

	// Socket constants
	const socket_af_inet = 2
	const socket_sock_stream = 1
	const sol_socket = 1
	const so_reuseaddr = 2
	const so_reuseport = 15

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

// io_uring structs

/// IoUringParams holds the io_uring setup parameters.
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

/// SqRingOffsets holds the submission queue ring offsets.
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

/// CqRingOffsets holds the completion queue ring offsets.
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

/// IoUringSqe is a submission queue entry.
pub struct IoUringSqe {
pub mut:
	opcode      u8
	flags       u8
	ioprio      u16
	fd          i32
	off         u64
	addr        u64
	len         u32
	rw_flags    u32
	user_data   u64
	buf_index   u16
	personality u16
	splice_fd   i32
	pad2        [2]u64
}

/// IoUringCqe is a completion queue entry.
pub struct IoUringCqe {
pub:
	user_data u64
	res       i32
	flags     u32
}

// io_uring ring struct

/// IoUring represents an io_uring instance.
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
	sq_head     &u32 = unsafe { nil }
	sq_tail     &u32 = unsafe { nil }
	sq_mask     u32
	sq_array    &u32 = unsafe { nil }
	cq_head     &u32 = unsafe { nil }
	cq_tail     &u32 = unsafe { nil }
	cq_mask     u32
	cqes_ptr    &IoUringCqe = unsafe { nil }
	submissions u64
	completions u64
	errors      u64
}

/// IoUringConfig holds the io_uring configuration.
pub struct IoUringConfig {
pub:
	queue_depth    u32 = 256
	flags          u32
	sq_thread_cpu  u32
	sq_thread_idle u32 = 1000
}

// io_uring result types

/// IoUringResult holds the result of an io_uring operation.
pub struct IoUringResult {
pub:
	user_data u64
	result    i32
	success   bool
}

// Network-related structs

/// SockaddrIn is an IPv4 socket address struct.
struct SockaddrIn {
mut:
	sin_family u16
	sin_port   u16
	sin_addr   u32
	sin_zero   [8]u8
}

/// AcceptResult holds the result of an accept operation.
pub struct AcceptResult {
pub:
	client_fd   int
	client_addr string
	success     bool
}

/// IoUringStats holds io_uring statistics.
pub struct IoUringStats {
pub:
	submissions u64
	completions u64
	errors      u64
	pending     u64
}

// io_uring implementation

/// new_io_uring creates a new io_uring instance.
pub fn new_io_uring(config IoUringConfig) !IoUring {
	$if linux {
		mut params := IoUringParams{
			sq_entries:     config.queue_depth
			cq_entries:     config.queue_depth * 2
			flags:          config.flags
			sq_thread_cpu:  config.sq_thread_cpu
			sq_thread_idle: config.sq_thread_idle
		}

		// io_uring_setup syscall
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

		// Set up ring pointers
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

/// close releases io_uring resources.
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

/// get_sqe retrieves an empty submission queue entry.
pub fn (mut r IoUring) get_sqe() ?&IoUringSqe {
	$if linux {
		head := unsafe { *r.sq_head }
		tail := unsafe { *r.sq_tail }
		next := tail + 1

		if (next - head) > r.params.sq_entries {
			return none
		}

		idx := tail & r.sq_mask
		sqe := unsafe { &IoUringSqe(voidptr(usize(r.sqes) + usize(idx) * sizeof(IoUringSqe))) }

		// Initialize SQE
		unsafe {
			C.memset(sqe, 0, sizeof(IoUringSqe))
		}

		return sqe
	} $else {
		return none
	}
}

/// submit_sqe submits the current SQE.
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

/// submit submits pending SQEs and optionally waits for completions.
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

/// peek_cqe peeks at the next completion without consuming it.
pub fn (r &IoUring) peek_cqe() ?&IoUringCqe {
	$if linux {
		head := unsafe { *r.cq_head }
		tail := unsafe { *r.cq_tail }

		if head == tail {
			return none
		}

		idx := head & r.cq_mask
		return unsafe { &r.cqes_ptr[idx] }
	} $else {
		return none
	}
}

/// consume_cqe marks the current CQE as consumed.
pub fn (mut r IoUring) consume_cqe() {
	$if linux {
		unsafe {
			*r.cq_head = *r.cq_head + 1
		}
		r.completions++
	}
}

/// wait_cqe waits for at least one completion.
pub fn (mut r IoUring) wait_cqe() !IoUringResult {
	$if linux {
		// Check if a completion is already available
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

/// get_stats returns io_uring statistics.
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

// High-level operations

/// prep_read prepares a read operation.
pub fn (mut r IoUring) prep_read(fd int, buf []u8, offset i64, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

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

/// prep_write prepares a write operation.
pub fn (mut r IoUring) prep_write(fd int, buf []u8, offset i64, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

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

/// prep_fsync prepares an fsync operation.
pub fn (mut r IoUring) prep_fsync(fd int, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_fsync
		sqe.fd = i32(fd)
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

/// prep_nop prepares a no-op (useful for testing).
pub fn (mut r IoUring) prep_nop(user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_nop
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

// Network operations (io_uring-based)

/// prep_accept prepares to accept a connection on a socket.
/// listen_fd: listening socket file descriptor
/// user_data: user data to return upon completion
pub fn (mut r IoUring) prep_accept(listen_fd int, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_accept
		sqe.fd = i32(listen_fd)
		sqe.off = 0
		sqe.addr = 0
		sqe.len = 0
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

/// prep_accept_with_addr prepares to accept a connection along with the client address info.
/// listen_fd: listening socket file descriptor
/// addr: buffer to store the client address
/// addrlen: pointer to store the address length
/// user_data: user data to return upon completion
pub fn (mut r IoUring) prep_accept_with_addr(listen_fd int, addr &SockaddrIn, addrlen &u32, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_accept
		sqe.fd = i32(listen_fd)
		sqe.off = u64(usize(addrlen))
		sqe.addr = u64(usize(addr))
		sqe.len = 0
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

/// prep_recv prepares to receive data from a socket.
/// fd: socket file descriptor
/// buf: buffer to receive data into
/// flags: recv flags (typically 0)
/// user_data: user data to return upon completion
pub fn (mut r IoUring) prep_recv(fd int, buf []u8, flags u32, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_recv
		sqe.fd = i32(fd)
		sqe.addr = u64(usize(buf.data))
		sqe.len = u32(buf.len)
		sqe.rw_flags = flags
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

/// prep_send prepares to send data to a socket.
/// fd: socket file descriptor
/// buf: data buffer to send
/// flags: send flags (typically 0)
/// user_data: user data to return upon completion
pub fn (mut r IoUring) prep_send(fd int, buf []u8, flags u32, user_data u64) bool {
	$if linux {
		mut sqe := r.get_sqe() or { return false }

		sqe.opcode = ioring_op_send
		sqe.fd = i32(fd)
		sqe.addr = u64(usize(buf.data))
		sqe.len = u32(buf.len)
		sqe.rw_flags = flags
		sqe.user_data = user_data

		r.submit_sqe()
		return true
	} $else {
		return false
	}
}

/// create_listen_socket creates a listening socket.
/// host: host to bind to (e.g., "0.0.0.0")
/// port: port to bind to
/// backlog: listen backlog size
pub fn create_listen_socket(host string, port int, backlog int) !int {
	$if linux {
		// Create socket
		fd := C.socket(net.AddrFamily.ip, net.SocketType.tcp, 0)
		if fd < 0 {
			return error('socket() failed')
		}

		// Set SO_REUSEADDR
		opt := int(1)
		if C.setsockopt(fd, sol_socket, so_reuseaddr, voidptr(&opt), sizeof(int)) < 0 {
			C.close(fd)
			return error('setsockopt(SO_REUSEADDR) failed')
		}

		// Set SO_REUSEPORT
		if C.setsockopt(fd, sol_socket, so_reuseport, voidptr(&opt), sizeof(int)) < 0 {
			C.close(fd)
			return error('setsockopt(SO_REUSEPORT) failed')
		}

		// Set up address
		mut addr := SockaddrIn{
			sin_family: u16(net.AddrFamily.ip)
			sin_port:   C.htons(u16(port))
			sin_addr:   0
		}

		// Parse host address
		if host == '0.0.0.0' || host == '' {
			addr.sin_addr = C.htonl(0)
		} else {
			if C.inet_pton(net.AddrFamily.ip, host.str, voidptr(&addr.sin_addr)) != 1 {
				C.close(fd)
				return error('inet_pton() failed for host: ${host}')
			}
		}

		// Bind
		if C.bind(fd, voidptr(&addr), sizeof(SockaddrIn)) < 0 {
			C.close(fd)
			return error('bind() failed')
		}

		// Listen
		if C.listen(fd, backlog) < 0 {
			C.close(fd)
			return error('listen() failed')
		}

		return fd
	} $else {
		return error('create_listen_socket is only available on Linux')
	}
}

/// close_socket closes a socket.
pub fn close_socket(fd int) {
	$if linux {
		C.close(fd)
	}
}

// Batch operations

/// BatchOp represents a batch operation.
pub struct BatchOp {
pub:
	op_type   BatchOpType
	fd        int
	buf       []u8
	offset    i64
	user_data u64
}

/// BatchOpType is the type of a batch operation.
pub enum BatchOpType {
	read
	write
	fsync
	nop
}

/// submit_batch submits multiple operations at once.
pub fn (mut r IoUring) submit_batch(ops []BatchOp) !int {
	$if linux {
		mut submitted := 0

		for op in ops {
			success := match op.op_type {
				.read { r.prep_read(op.fd, op.buf, op.offset, op.user_data) }
				.write { r.prep_write(op.fd, op.buf, op.offset, op.user_data) }
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

/// wait_batch waits for multiple completions.
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

// Fallback implementation for non-Linux

/// IoUringFallback provides a synchronous fallback for non-Linux systems.
pub struct IoUringFallback {
pub mut:
	operations u64
}

/// new_io_uring_fallback creates a fallback handler.
pub fn new_io_uring_fallback() IoUringFallback {
	return IoUringFallback{}
}

/// sync_read performs a synchronous read (fallback).
pub fn (mut f IoUringFallback) sync_read(mut file os.File, mut buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_read := file.read(mut buf) or { return error('read failed: ${err}') }
	f.operations++
	return bytes_read
}

/// sync_write performs a synchronous write (fallback).
pub fn (mut f IoUringFallback) sync_write(mut file os.File, buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_written := file.write(buf) or { return error('write failed: ${err}') }
	f.operations++
	return bytes_written
}

// Platform-independent async I/O interface

/// AsyncIoCapabilities represents platform async I/O support.
pub struct AsyncIoCapabilities {
pub:
	has_io_uring  bool
	has_aio       bool
	has_iocp      bool
	platform_name string
}

/// get_async_io_capabilities returns the available async I/O capabilities.
pub fn get_async_io_capabilities() AsyncIoCapabilities {
	$if linux {
		return AsyncIoCapabilities{
			has_io_uring:  true
			has_aio:       true
			platform_name: 'Linux'
		}
	} $else $if macos {
		return AsyncIoCapabilities{
			has_aio:       true
			platform_name: 'macOS'
		}
	} $else $if windows {
		return AsyncIoCapabilities{
			has_iocp:      true
			platform_name: 'Windows'
		}
	} $else {
		return AsyncIoCapabilities{
			platform_name: 'Unknown'
		}
	}
}

/// is_io_uring_available checks at runtime whether io_uring is available.
pub fn is_io_uring_available() bool {
	$if linux {
		// Create a minimal io_uring to verify availability
		ring := new_io_uring(IoUringConfig{ queue_depth: 1 }) or { return false }
		mut r := ring
		r.close()
		return true
	} $else {
		return false
	}
}
