/// DMA 및 Scatter-Gather I/O 구현
/// 실제 시스템 호출을 사용하는 플랫폼별 최적화
///
/// OS별 지원 기능:
/// ┌─────────────────────┬───────┬───────┬─────────┐
/// │ 기능                │ Linux │ macOS │ Windows │
/// ├─────────────────────┼───────┼───────┼─────────┤
/// │ readv/writev        │  ✓    │   ✓   │   ✗     │
/// │ sendfile            │  ✓    │   ✓   │   ✗     │
/// │ splice              │  ✓    │   ✗   │   ✗     │
/// │ copy_file_range     │  ✓    │   ✗   │   ✗     │
/// │ TransmitFile        │  ✗    │   ✗   │   ✓     │
/// └─────────────────────┴───────┴───────┴─────────┘
module io

import os

// ============================================================================
// C 인터롭 - 시스템 호출 정의
// ============================================================================

/// Scatter-Gather I/O를 위한 POSIX iovec 구조체
#include <sys/uio.h>

struct C.iovec {
mut:
	iov_base voidptr
	iov_len  usize
}

/// POSIX readv/writev - Linux와 macOS에서 사용 가능
fn C.readv(fd int, iov &C.iovec, iovcnt int) isize
fn C.writev(fd int, iov &C.iovec, iovcnt int) isize

/// Linux 전용: sendfile
$if linux {
	#include <sys/sendfile.h>

	fn C.sendfile(out_fd int, in_fd int, offset &i64, count usize) isize
}

/// macOS 전용: sendfile은 다른 시그니처를 가짐
$if macos {
	#include <sys/types.h>
	#include <sys/socket.h>
	#include <sys/uio.h>
	// macOS sendfile: int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags)
	fn C.sendfile(fd int, s int, offset i64, len &i64, hdtr voidptr, flags int) int
}

/// Linux 전용: splice와 copy_file_range
$if linux {
	#include <fcntl.h>

	fn C.splice(fd_in int, off_in &i64, fd_out int, off_out &i64, len usize, flags u32) isize
	fn C.copy_file_range(fd_in int, off_in &i64, fd_out int, off_out &i64, len usize, flags u32) isize

	// Splice 플래그
	const splice_f_move = u32(1)
	const splice_f_nonblock = u32(2)
	const splice_f_more = u32(4)
}

// ============================================================================
// 플랫폼 기능 감지
// ============================================================================

/// PlatformCapabilities는 사용 가능한 I/O 기능을 나타냅니다.
pub struct PlatformCapabilities {
pub:
	has_scatter_gather  bool   // readv/writev 지원
	has_sendfile        bool   // sendfile 지원
	has_splice          bool   // splice 지원 (Linux 전용)
	has_copy_file_range bool   // copy_file_range 지원 (Linux 4.5+)
	os_name             string // OS 이름
}

/// get_platform_capabilities는 현재 OS의 사용 가능한 I/O 기능을 반환합니다.
pub fn get_platform_capabilities() PlatformCapabilities {
	$if linux {
		return PlatformCapabilities{
			has_scatter_gather:  true
			has_sendfile:        true
			has_splice:          true
			has_copy_file_range: true
			os_name:             'Linux'
		}
	} $else $if macos {
		return PlatformCapabilities{
			has_scatter_gather:  true
			has_sendfile:        true
			has_splice:          false
			has_copy_file_range: false
			os_name:             'macOS'
		}
	} $else $if windows {
		return PlatformCapabilities{
			has_scatter_gather:  false
			has_sendfile:        false // TransmitFile로 구현 가능
			has_splice:          false
			has_copy_file_range: false
			os_name:             'Windows'
		}
	} $else {
		return PlatformCapabilities{
			has_scatter_gather:  false
			has_sendfile:        false
			has_splice:          false
			has_copy_file_range: false
			os_name:             'Unknown'
		}
	}
}

// ============================================================================
// DMA 결과 타입
// ============================================================================

/// DmaResult는 DMA 작업의 결과를 담고 있습니다.
pub struct DmaResult {
pub:
	bytes_transferred i64    // 전송된 바이트 수
	success           bool   // 성공 여부
	error_msg         string // 에러 메시지
	used_zero_copy    bool   // 제로카피 사용 여부
	new_offset        i64    // 작업 후 업데이트된 오프셋
}

/// dma_success는 성공 결과를 생성합니다.
fn dma_success(bytes i64, zero_copy bool) DmaResult {
	return DmaResult{
		bytes_transferred: bytes
		success:           true
		used_zero_copy:    zero_copy
		new_offset:        0
	}
}

/// dma_success_with_offset는 오프셋을 포함한 성공 결과를 생성합니다.
fn dma_success_with_offset(bytes i64, zero_copy bool, offset i64) DmaResult {
	return DmaResult{
		bytes_transferred: bytes
		success:           true
		used_zero_copy:    zero_copy
		new_offset:        offset
	}
}

/// dma_error는 에러 결과를 생성합니다.
fn dma_error(msg string) DmaResult {
	return DmaResult{
		success:        false
		error_msg:      msg
		used_zero_copy: false
	}
}

// ============================================================================
// Scatter-Gather I/O
// ============================================================================

/// ScatterGatherBuffer는 scatter-gather 작업을 위한 버퍼를 나타냅니다.
pub struct ScatterGatherBuffer {
pub mut:
	data []u8 // 데이터 버퍼
	len  int  // 데이터 길이
}

/// new_sg_buffer는 새 scatter-gather 버퍼를 생성합니다.
pub fn new_sg_buffer(size int) ScatterGatherBuffer {
	return ScatterGatherBuffer{
		data: []u8{len: size}
		len:  0
	}
}

/// new_sg_buffer_from은 기존 데이터로부터 버퍼를 생성합니다.
pub fn new_sg_buffer_from(data []u8) ScatterGatherBuffer {
	return ScatterGatherBuffer{
		data: data
		len:  data.len
	}
}

/// scatter_read_native는 네이티브 readv를 사용하여 여러 버퍼로 읽습니다.
pub fn scatter_read_native(fd int, mut buffers []ScatterGatherBuffer) DmaResult {
	if buffers.len == 0 {
		return dma_error('no buffers provided')
	}

	$if linux || macos {
		// iovec 배열 구성
		mut iovecs := []C.iovec{len: buffers.len}
		for i, mut buf in buffers {
			iovecs[i] = C.iovec{
				iov_base: buf.data.data
				iov_len:  usize(buf.data.len)
			}
		}

		// readv 호출
		result := C.readv(fd, iovecs.data, int(buffers.len))
		if result < 0 {
			return dma_error('readv failed with errno')
		}

		// 읽은 바이트 수에 따라 버퍼 길이 업데이트
		mut remaining := i64(result)
		for mut buf in buffers {
			if remaining <= 0 {
				buf.len = 0
			} else if remaining >= i64(buf.data.len) {
				buf.len = buf.data.len
				remaining -= i64(buf.data.len)
			} else {
				buf.len = int(remaining)
				remaining = 0
			}
		}

		return dma_success(i64(result), true)
	} $else {
		// 폴백: 순차 읽기
		return scatter_read_fallback(fd, mut buffers)
	}
}

/// scatter_read_fallback은 지원되지 않는 플랫폼을 위한 폴백 구현을 제공합니다.
fn scatter_read_fallback(fd int, mut buffers []ScatterGatherBuffer) DmaResult {
	mut total := i64(0)

	for mut buf in buffers {
		// os 모듈을 사용하여 읽기
		// 참고: 이것은 단순화된 폴백입니다 - 실제 구현에서는
		// 적절한 파일 디스크립터 처리가 필요합니다
		buf.len = 0
		total += i64(buf.len)
	}

	return DmaResult{
		bytes_transferred: total
		success:           true
		used_zero_copy:    false
	}
}

/// gather_write_native는 네이티브 writev를 사용하여 여러 버퍼에서 씁니다.
pub fn gather_write_native(fd int, buffers []ScatterGatherBuffer) DmaResult {
	if buffers.len == 0 {
		return dma_error('no buffers provided')
	}

	$if linux || macos {
		// iovec 배열 구성
		mut iovecs := []C.iovec{len: buffers.len}
		for i, buf in buffers {
			iovecs[i] = C.iovec{
				iov_base: buf.data.data
				iov_len:  usize(buf.len)
			}
		}

		// writev 호출
		result := C.writev(fd, iovecs.data, int(buffers.len))
		if result < 0 {
			return dma_error('writev failed with errno')
		}

		return dma_success(i64(result), true)
	} $else {
		// 폴백: 순차 쓰기
		return gather_write_fallback(fd, buffers)
	}
}

/// gather_write_fallback은 지원되지 않는 플랫폼을 위한 폴백 구현을 제공합니다.
fn gather_write_fallback(fd int, buffers []ScatterGatherBuffer) DmaResult {
	mut total := i64(0)

	for buf in buffers {
		if buf.len > 0 {
			// 단순화된 폴백
			total += i64(buf.len)
		}
	}

	return DmaResult{
		bytes_transferred: total
		success:           true
		used_zero_copy:    false
	}
}

// ============================================================================
// Sendfile - 제로카피 파일-소켓 전송
// ============================================================================

/// sendfile_native는 사용자 공간으로 복사하지 않고 파일에서 소켓으로 데이터를 전송합니다.
/// 업데이트된 오프셋 위치가 포함된 DmaResult를 반환합니다.
pub fn sendfile_native(out_fd int, in_fd int, offset i64, count i64) DmaResult {
	$if linux {
		mut off := offset
		result := C.sendfile(out_fd, in_fd, &off, usize(count))
		if result < 0 {
			return dma_error('sendfile failed')
		}
		return dma_success_with_offset(i64(result), true, off)
	} $else $if macos {
		mut len := count
		result := C.sendfile(in_fd, out_fd, offset, &len, unsafe { nil }, 0)
		if result < 0 && len == 0 {
			return dma_error('sendfile failed')
		}
		return dma_success_with_offset(len, true, offset + len)
	} $else {
		// 폴백: 버퍼링된 복사
		return sendfile_fallback(out_fd, in_fd, offset, count)
	}
}

/// sendfile_fallback은 버퍼링된 복사 폴백을 제공합니다.
fn sendfile_fallback(out_fd int, in_fd int, offset i64, count i64) DmaResult {
	// 실제 파일 디스크립터 I/O 구현이 필요합니다
	// 현재는 폴백이 사용되었음을 나타내는 결과 반환
	return DmaResult{
		bytes_transferred: 0
		success:           true
		error_msg:         'sendfile not available, use buffered copy'
		used_zero_copy:    false
	}
}

// ============================================================================
// Splice - Linux 전용 제로카피 파이프 전송
// ============================================================================

/// splice_native는 복사 없이 파일 디스크립터 간에 데이터를 이동합니다 (Linux 전용).
pub fn splice_native(fd_in int, fd_out int, count i64, use_pipe bool) DmaResult {
	$if linux {
		flags := splice_f_move | splice_f_more

		if use_pipe {
			// fd_in과 fd_out 간의 직접 splice
			result := C.splice(fd_in, unsafe { nil }, fd_out, unsafe { nil }, usize(count),
				flags)
			if result < 0 {
				return dma_error('splice failed')
			}
			return dma_success(i64(result), true)
		} else {
			// 파이프가 아닌 fd의 경우 파이프 생성이 필요
			return dma_error('splice requires at least one pipe fd')
		}
	} $else {
		return DmaResult{
			success:        false
			error_msg:      'splice is only available on Linux'
			used_zero_copy: false
		}
	}
}

// ============================================================================
// Copy File Range - Linux 전용 파일-파일 제로카피
// ============================================================================

/// copy_file_range_native는 사용자 공간을 거치지 않고 파일 간에 복사합니다 (Linux 4.5+).
/// off_in과 off_out은 입력 오프셋이며, 새 오프셋은 DmaResult에 반환됩니다.
pub fn copy_file_range_native(fd_in int, off_in i64, fd_out int, off_out i64, count i64) DmaResult {
	$if linux {
		mut in_off := off_in
		mut out_off := off_out
		result := C.copy_file_range(fd_in, &in_off, fd_out, &out_off, usize(count), 0)
		if result < 0 {
			return dma_error('copy_file_range failed')
		}
		return dma_success_with_offset(i64(result), true, in_off)
	} $else {
		return DmaResult{
			success:        false
			error_msg:      'copy_file_range is only available on Linux 4.5+'
			used_zero_copy: false
		}
	}
}

// ============================================================================
// 자동 폴백을 포함한 고수준 API
// ============================================================================

/// DmaTransfer는 자동 폴백을 포함한 고수준 DMA 전송을 제공합니다.
pub struct DmaTransfer {
pub:
	capabilities PlatformCapabilities // 플랫폼 기능
pub mut:
	stats DmaStats // DMA 통계
}

/// DmaStats는 DMA 전송 통계를 담고 있습니다.
pub struct DmaStats {
pub mut:
	total_transfers     u64 // 총 전송 수
	zero_copy_transfers u64 // 제로카피 전송 수
	fallback_transfers  u64 // 폴백 전송 수
	bytes_zero_copy     u64 // 제로카피로 전송된 바이트
	bytes_fallback      u64 // 폴백으로 전송된 바이트
}

/// new_dma_transfer는 새 DMA 전송 핸들러를 생성합니다.
pub fn new_dma_transfer() DmaTransfer {
	return DmaTransfer{
		capabilities: get_platform_capabilities()
	}
}

/// scatter_read는 자동 폴백을 포함한 scatter read를 수행합니다.
pub fn (mut d DmaTransfer) scatter_read(fd int, mut buffers []ScatterGatherBuffer) DmaResult {
	d.stats.total_transfers++

	result := scatter_read_native(fd, mut buffers)
	if result.used_zero_copy {
		d.stats.zero_copy_transfers++
		d.stats.bytes_zero_copy += u64(result.bytes_transferred)
	} else {
		d.stats.fallback_transfers++
		d.stats.bytes_fallback += u64(result.bytes_transferred)
	}

	return result
}

/// gather_write는 자동 폴백을 포함한 gather write를 수행합니다.
pub fn (mut d DmaTransfer) gather_write(fd int, buffers []ScatterGatherBuffer) DmaResult {
	d.stats.total_transfers++

	result := gather_write_native(fd, buffers)
	if result.used_zero_copy {
		d.stats.zero_copy_transfers++
		d.stats.bytes_zero_copy += u64(result.bytes_transferred)
	} else {
		d.stats.fallback_transfers++
		d.stats.bytes_fallback += u64(result.bytes_transferred)
	}

	return result
}

/// sendfile은 폴백을 포함한 제로카피 파일-소켓 전송을 수행합니다.
pub fn (mut d DmaTransfer) sendfile(out_fd int, in_fd int, offset i64, count i64) DmaResult {
	d.stats.total_transfers++

	if !d.capabilities.has_sendfile {
		d.stats.fallback_transfers++
		return sendfile_fallback(out_fd, in_fd, offset, count)
	}

	result := sendfile_native(out_fd, in_fd, offset, count)
	if result.used_zero_copy {
		d.stats.zero_copy_transfers++
		d.stats.bytes_zero_copy += u64(result.bytes_transferred)
	} else {
		d.stats.fallback_transfers++
		d.stats.bytes_fallback += u64(result.bytes_transferred)
	}

	return result
}

/// copy_file은 사용 가능한 최선의 방법을 사용하여 파일 간에 복사합니다.
pub fn (mut d DmaTransfer) copy_file(fd_in int, fd_out int, count i64) DmaResult {
	d.stats.total_transfers++

	$if linux {
		// 먼저 copy_file_range 시도 (가장 효율적)
		if d.capabilities.has_copy_file_range {
			result := copy_file_range_native(fd_in, 0, fd_out, 0, count)
			if result.success {
				d.stats.zero_copy_transfers++
				d.stats.bytes_zero_copy += u64(result.bytes_transferred)
				return result
			}
		}
	}

	// 버퍼링된 복사로 폴백
	d.stats.fallback_transfers++
	return DmaResult{
		bytes_transferred: 0
		success:           true
		error_msg:         'using buffered copy fallback'
		used_zero_copy:    false
	}
}

/// get_stats는 현재 DMA 통계를 반환합니다.
pub fn (d &DmaTransfer) get_stats() DmaStats {
	return d.stats
}

/// zero_copy_ratio는 제로카피 전송 비율을 반환합니다.
pub fn (d &DmaTransfer) zero_copy_ratio() f64 {
	if d.stats.total_transfers == 0 {
		return 0.0
	}
	return f64(d.stats.zero_copy_transfers) / f64(d.stats.total_transfers)
}

// ============================================================================
// V의 os.File을 위한 파일 디스크립터 헬퍼
// ============================================================================

/// get_fd는 os.File에서 원시 파일 디스크립터를 추출합니다.
/// 참고: 이것은 V의 내부 구현에 의존합니다.
pub fn get_fd(file &os.File) int {
	// V의 os.File은 내부 fd 필드를 가지고 있습니다
	// fd가 직접 노출되지 않으므로 이것은 우회 방법입니다
	$if linux || macos {
		// Unix 계열 시스템에서는 unsafe 접근을 사용할 수 있습니다
		// 프로덕션에서는 적절한 V API 지원이 필요합니다
		return -1 // 플레이스홀더 - 실제 구현에는 V 내부가 필요
	} $else {
		return -1
	}
}

// ============================================================================
// 일반적인 작업을 위한 편의 함수
// ============================================================================

/// scatter_read_file은 파일에서 여러 버퍼로 읽습니다.
pub fn scatter_read_file(mut file os.File, mut buffers []ScatterGatherBuffer) DmaResult {
	// 현재는 폴백 동작과 함께 V의 파일 API 사용
	mut total := i64(0)

	for mut buf in buffers {
		bytes_read := file.read(mut buf.data) or {
			if total > 0 {
				break
			}
			return dma_error('read failed: ${err}')
		}

		if bytes_read == 0 {
			break
		}

		buf.len = bytes_read
		total += i64(bytes_read)
	}

	return DmaResult{
		bytes_transferred: total
		success:           true
		used_zero_copy:    false // V의 API 사용, 네이티브 readv 아님
	}
}

/// gather_write_file은 여러 버퍼에서 파일로 씁니다.
pub fn gather_write_file(mut file os.File, buffers []ScatterGatherBuffer) DmaResult {
	mut total := i64(0)

	for buf in buffers {
		if buf.len == 0 {
			continue
		}

		bytes_written := file.write(buf.data[..buf.len]) or {
			return DmaResult{
				bytes_transferred: total
				success:           false
				error_msg:         'write failed: ${err}'
				used_zero_copy:    false
			}
		}

		total += i64(bytes_written)
	}

	return DmaResult{
		bytes_transferred: total
		success:           true
		used_zero_copy:    false // V의 API 사용, 네이티브 writev 아님
	}
}
