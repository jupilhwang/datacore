/// io_uring - Linux 5.1+ 비동기 I/O 인터페이스
/// 커널 제출/완료 큐를 사용한 고성능 비동기 I/O 제공
///
/// 기능:
/// - 제로카피 I/O 연산
/// - 시스템 호출 오버헤드를 줄이기 위한 배치 제출
/// - 폴링 I/O 모드 지원
/// - 연결된 요청을 위한 링크 연산
///
/// 참고: Linux 5.1+ 에서만 사용 가능, 다른 플랫폼은 폴백 사용
module engines

import os
import net

// io_uring 상수 및 플래그

$if linux {
	#include <linux/io_uring.h>
	#include <sys/syscall.h>
	#include <sys/mman.h>
	#include <unistd.h>

	// io_uring_setup 플래그
	const ioring_setup_iopoll = u32(1)
	const ioring_setup_sqpoll = u32(2)
	const ioring_setup_sq_aff = u32(4)

	// io_uring 연산 코드
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

	// 소켓 관련 C 함수
	fn C.socket(domain net.AddrFamily, typ net.SocketType, protocol int) int
	fn C.bind(sockfd int, addr voidptr, addrlen u32) int
	fn C.listen(sockfd int, backlog int) int
	fn C.setsockopt(sockfd int, level int, optname int, optval voidptr, optlen u32) int
	fn C.close(fd int) int
	fn C.htonl(hostlong u32) u32
	fn C.htons(hostshort u16) u16
	fn C.inet_pton(af net.AddrFamily, src &char, dst voidptr) int

	// 소켓 상수
	const socket_af_inet = 2
	const socket_sock_stream = 1
	const sol_socket = 1
	const so_reuseaddr = 2
	const so_reuseport = 15

	// 시스템 호출 번호 (x86_64)
	const sys_io_uring_setup = 425
	const sys_io_uring_enter = 426
	const sys_io_uring_register = 427

	// io_uring_enter 플래그
	const ioring_enter_getevents = u32(1)
	const ioring_enter_sq_wakeup = u32(2)

	fn C.syscall(number i64, args ...voidptr) i64
	fn C.mmap(addr voidptr, length usize, prot int, flags int, fd int, offset i64) voidptr
	fn C.munmap(addr voidptr, length usize) int
}

// io_uring 구조체

/// IoUringParams는 io_uring 설정 파라미터를 담고 있습니다.
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

/// SqRingOffsets는 제출 큐 링 오프셋을 담고 있습니다.
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

/// CqRingOffsets는 완료 큐 링 오프셋을 담고 있습니다.
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

/// IoUringSqe는 제출 큐 항목입니다.
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

/// IoUringCqe는 완료 큐 항목입니다.
pub struct IoUringCqe {
pub:
	user_data u64
	res       i32
	flags     u32
}

// io_uring 링 구조체

/// IoUring은 io_uring 인스턴스를 나타냅니다.
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
	// 링 상태
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

/// IoUringConfig는 io_uring 설정을 담고 있습니다.
pub struct IoUringConfig {
pub:
	queue_depth    u32 = 256
	flags          u32
	sq_thread_cpu  u32
	sq_thread_idle u32 = 1000
}

// io_uring 결과 타입

/// IoUringResult는 io_uring 연산 결과를 담고 있습니다.
pub struct IoUringResult {
pub:
	user_data u64
	result    i32
	success   bool
}

// 네트워크 관련 구조체

/// SockaddrIn은 IPv4 소켓 주소 구조체입니다.
struct SockaddrIn {
mut:
	sin_family u16
	sin_port   u16
	sin_addr   u32
	sin_zero   [8]u8
}

/// AcceptResult는 accept 연산 결과를 담고 있습니다.
pub struct AcceptResult {
pub:
	client_fd   int
	client_addr string
	success     bool
}

/// IoUringStats는 io_uring 통계를 담고 있습니다.
pub struct IoUringStats {
pub:
	submissions u64
	completions u64
	errors      u64
	pending     u64
}

// io_uring 구현

/// new_io_uring은 새 io_uring 인스턴스를 생성합니다.
pub fn new_io_uring(config IoUringConfig) !IoUring {
	$if linux {
		mut params := IoUringParams{
			sq_entries:     config.queue_depth
			cq_entries:     config.queue_depth * 2
			flags:          config.flags
			sq_thread_cpu:  config.sq_thread_cpu
			sq_thread_idle: config.sq_thread_idle
		}

		// io_uring_setup 시스템 호출
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

		// 제출 큐 링 매핑
		sq_ring_sz := usize(params.sq_off.array) + usize(params.sq_entries) * sizeof(u32)
		ring.sq_ring_size = sq_ring_sz
		ring.sq_ring_ptr = C.mmap(unsafe { nil }, sq_ring_sz, 0x1 | 0x2, 0x1, int(fd),
			0)
		if ring.sq_ring_ptr == voidptr(-1) {
			return error('mmap sq_ring failed')
		}

		// 완료 큐 링 매핑
		cq_ring_sz := usize(params.cq_off.cqes) + usize(params.cq_entries) * sizeof(IoUringCqe)
		ring.cq_ring_size = cq_ring_sz
		ring.cq_ring_ptr = C.mmap(unsafe { nil }, cq_ring_sz, 0x1 | 0x2, 0x1, int(fd),
			0x8000000)
		if ring.cq_ring_ptr == voidptr(-1) {
			return error('mmap cq_ring failed')
		}

		// SQE 매핑
		sqes_sz := usize(params.sq_entries) * sizeof(IoUringSqe)
		ring.sqes_size = sqes_sz
		ring.sqes = C.mmap(unsafe { nil }, sqes_sz, 0x1 | 0x2, 0x1, int(fd), 0x10000000)
		if ring.sqes == voidptr(-1) {
			return error('mmap sqes failed')
		}

		// 링 포인터 설정
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

/// close는 io_uring 리소스를 해제합니다.
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

/// get_sqe는 빈 제출 큐 항목을 가져옵니다.
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

		// SQE 초기화
		unsafe {
			C.memset(sqe, 0, sizeof(IoUringSqe))
		}

		return sqe
	} $else {
		return none
	}
}

/// submit_sqe는 현재 SQE를 제출합니다.
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

/// submit은 대기 중인 SQE를 제출하고 선택적으로 완료를 기다립니다.
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

/// peek_cqe는 소비하지 않고 다음 완료를 확인합니다.
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

/// consume_cqe는 현재 CQE를 소비된 것으로 표시합니다.
pub fn (mut r IoUring) consume_cqe() {
	$if linux {
		unsafe {
			*r.cq_head = *r.cq_head + 1
		}
		r.completions++
	}
}

/// wait_cqe는 최소 하나의 완료를 기다립니다.
pub fn (mut r IoUring) wait_cqe() !IoUringResult {
	$if linux {
		// 먼저 이미 완료가 있는지 확인
		if cqe := r.peek_cqe() {
			result := IoUringResult{
				user_data: cqe.user_data
				result:    cqe.res
				success:   cqe.res >= 0
			}
			r.consume_cqe()
			return result
		}

		// 완료 대기
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

/// get_stats는 io_uring 통계를 반환합니다.
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

// 고수준 연산

/// prep_read는 읽기 연산을 준비합니다.
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

/// prep_write는 쓰기 연산을 준비합니다.
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

/// prep_fsync는 fsync 연산을 준비합니다.
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

/// prep_nop은 no-op을 준비합니다 (테스트에 유용).
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

// 네트워크 연산 (io_uring 기반)

/// prep_accept는 소켓에서 연결 수락을 준비합니다.
/// listen_fd: 리스닝 소켓 파일 디스크립터
/// user_data: 완료 시 반환될 사용자 데이터
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

/// prep_accept_with_addr는 클라이언트 주소 정보와 함께 연결 수락을 준비합니다.
/// listen_fd: 리스닝 소켓 파일 디스크립터
/// addr: 클라이언트 주소를 저장할 버퍼
/// addrlen: 주소 길이를 저장할 포인터
/// user_data: 완료 시 반환될 사용자 데이터
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

/// prep_recv는 소켓에서 데이터 수신을 준비합니다.
/// fd: 소켓 파일 디스크립터
/// buf: 데이터를 수신할 버퍼
/// flags: recv 플래그 (일반적으로 0)
/// user_data: 완료 시 반환될 사용자 데이터
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

/// prep_send는 소켓으로 데이터 송신을 준비합니다.
/// fd: 소켓 파일 디스크립터
/// buf: 전송할 데이터 버퍼
/// flags: send 플래그 (일반적으로 0)
/// user_data: 완료 시 반환될 사용자 데이터
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

/// create_listen_socket은 리스닝 소켓을 생성합니다.
/// host: 바인딩할 호스트 (예: "0.0.0.0")
/// port: 바인딩할 포트
/// backlog: listen 백로그 크기
pub fn create_listen_socket(host string, port int, backlog int) !int {
	$if linux {
		// 소켓 생성
		fd := C.socket(net.AddrFamily.ip, net.SocketType.tcp, 0)
		if fd < 0 {
			return error('socket() failed')
		}

		// SO_REUSEADDR 설정
		opt := int(1)
		if C.setsockopt(fd, sol_socket, so_reuseaddr, voidptr(&opt), sizeof(int)) < 0 {
			C.close(fd)
			return error('setsockopt(SO_REUSEADDR) failed')
		}

		// SO_REUSEPORT 설정
		if C.setsockopt(fd, sol_socket, so_reuseport, voidptr(&opt), sizeof(int)) < 0 {
			C.close(fd)
			return error('setsockopt(SO_REUSEPORT) failed')
		}

		// 주소 설정
		mut addr := SockaddrIn{
			sin_family: u16(net.AddrFamily.ip)
			sin_port:   C.htons(u16(port))
			sin_addr:   0
		}

		// 호스트 주소 파싱
		if host == '0.0.0.0' || host == '' {
			addr.sin_addr = C.htonl(0)
		} else {
			if C.inet_pton(net.AddrFamily.ip, host.str, voidptr(&addr.sin_addr)) != 1 {
				C.close(fd)
				return error('inet_pton() failed for host: ${host}')
			}
		}

		// 바인드
		if C.bind(fd, voidptr(&addr), sizeof(SockaddrIn)) < 0 {
			C.close(fd)
			return error('bind() failed')
		}

		// 리슨
		if C.listen(fd, backlog) < 0 {
			C.close(fd)
			return error('listen() failed')
		}

		return fd
	} $else {
		return error('create_listen_socket is only available on Linux')
	}
}

/// close_socket은 소켓을 닫습니다.
pub fn close_socket(fd int) {
	$if linux {
		C.close(fd)
	}
}

// 배치 연산

/// BatchOp은 배치 연산을 나타냅니다.
pub struct BatchOp {
pub:
	op_type   BatchOpType
	fd        int
	buf       []u8
	offset    i64
	user_data u64
}

/// BatchOpType은 배치 연산 타입입니다.
pub enum BatchOpType {
	read
	write
	fsync
	nop
}

/// submit_batch는 여러 연산을 한 번에 제출합니다.
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

/// wait_batch는 여러 완료를 기다립니다.
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

// 비Linux를 위한 폴백 구현

/// IoUringFallback은 비Linux 시스템을 위한 동기 폴백을 제공합니다.
pub struct IoUringFallback {
pub mut:
	operations u64
}

/// new_io_uring_fallback은 폴백 핸들러를 생성합니다.
pub fn new_io_uring_fallback() IoUringFallback {
	return IoUringFallback{}
}

/// sync_read는 동기 읽기를 수행합니다 (폴백).
pub fn (mut f IoUringFallback) sync_read(mut file os.File, mut buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_read := file.read(mut buf) or { return error('read failed: ${err}') }
	f.operations++
	return bytes_read
}

/// sync_write는 동기 쓰기를 수행합니다 (폴백).
pub fn (mut f IoUringFallback) sync_write(mut file os.File, buf []u8, offset i64) !int {
	if offset >= 0 {
		file.seek(offset, .start) or { return error('seek failed: ${err}') }
	}
	bytes_written := file.write(buf) or { return error('write failed: ${err}') }
	f.operations++
	return bytes_written
}

// 플랫폼 독립적 비동기 I/O 인터페이스

/// AsyncIoCapabilities는 플랫폼 지원을 나타냅니다.
pub struct AsyncIoCapabilities {
pub:
	has_io_uring  bool
	has_aio       bool
	has_iocp      bool
	platform_name string
}

/// get_async_io_capabilities는 사용 가능한 비동기 I/O 기능을 반환합니다.
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

/// is_io_uring_available은 런타임에 io_uring 사용 가능 여부를 확인합니다.
pub fn is_io_uring_available() bool {
	$if linux {
		// 최소 io_uring을 생성하여 가용성 확인
		ring := new_io_uring(IoUringConfig{ queue_depth: 1 }) or { return false }
		mut r := ring
		r.close()
		return true
	} $else {
		return false
	}
}
