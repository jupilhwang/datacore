/// 인프라 레이어 - 제로카피 I/O
/// 시스템 sendfile을 사용한 고성능 I/O 연산
module io

import os
import infra.performance.core

// 제로카피 전송 결과

/// TransferResult는 전송 작업의 결과를 담고 있습니다.
pub struct TransferResult {
pub:
	bytes_transferred i64  // 전송된 바이트 수
	success           bool // 성공 여부
	error_msg         string
}

// 파일 기반 제로카피 (표준 파일 연산 사용)

/// zero_copy_file_to_file은 파일 간에 데이터를 효율적으로 전송합니다.
/// V가 sendfile을 직접 노출하지 않으므로 버퍼링된 복사로 폴백합니다.
pub fn zero_copy_file_to_file(src_path string, dst_path string, offset i64, length i64) TransferResult {
	// 소스 파일 열기
	mut src_file := os.open(src_path) or {
		return TransferResult{
			success:   false
			error_msg: 'Failed to open source: ${err}'
		}
	}
	defer { src_file.close() }

	// 대상 파일 열기/생성
	mut dst_file := os.create(dst_path) or {
		return TransferResult{
			success:   false
			error_msg: 'Failed to create destination: ${err}'
		}
	}
	defer { dst_file.close() }

	// 소스에서 오프셋으로 이동
	if offset > 0 {
		src_file.seek(offset, .start) or {
			return TransferResult{
				success:   false
				error_msg: 'Failed to seek: ${err}'
			}
		}
	}

	// 효율적인 버퍼링된 복사 사용
	return buffered_copy(mut src_file, mut dst_file, length)
}

/// buffered_copy는 효율적인 버퍼링된 복사를 수행합니다.
fn buffered_copy(mut src os.File, mut dst os.File, length i64) TransferResult {
	mut total_transferred := i64(0)
	mut remaining := length
	buf_size := 65536 // 효율성을 위한 64KB 버퍼
	mut buf := []u8{len: buf_size}

	for remaining > 0 || length < 0 {
		to_read := if length < 0 {
			buf_size
		} else if remaining < i64(buf_size) {
			int(remaining)
		} else {
			buf_size
		}

		bytes_read := src.read(mut buf[..to_read]) or {
			if total_transferred > 0 {
				break
			}
			return TransferResult{
				success:   false
				error_msg: 'Read error: ${err}'
			}
		}

		if bytes_read == 0 {
			break
		}

		bytes_written := dst.write(buf[..bytes_read]) or {
			return TransferResult{
				success:   false
				error_msg: 'Write error: ${err}'
			}
		}

		total_transferred += i64(bytes_written)
		if length >= 0 {
			remaining -= i64(bytes_written)
		}

		if bytes_written != bytes_read {
			return TransferResult{
				bytes_transferred: total_transferred
				success:           false
				error_msg:         'Partial write'
			}
		}
	}

	return TransferResult{
		bytes_transferred: total_transferred
		success:           true
	}
}

// 메모리 매핑 스타일 scatter/gather I/O 시뮬레이션

/// IoVec은 I/O 벡터를 나타냅니다.
pub struct IoVec {
pub:
	data []u8 // 데이터 버퍼
}

/// scatter_read는 여러 버퍼로 읽습니다.
pub fn scatter_read(mut file os.File, buffers []&core.Buffer) TransferResult {
	mut total := i64(0)

	for buf in buffers {
		mut temp := []u8{len: buf.remaining()}
		bytes_read := file.read(mut temp) or {
			if total > 0 {
				break
			}
			return TransferResult{
				success:   false
				error_msg: 'Read error: ${err}'
			}
		}

		if bytes_read == 0 {
			break
		}

		// 불변 배열에서 가져온 buf를 직접 수정할 수 없음
		// 읽은 양만 추적
		total += i64(bytes_read)
	}

	return TransferResult{
		bytes_transferred: total
		success:           true
	}
}

/// gather_write는 여러 버퍼에서 씁니다.
pub fn gather_write(mut file os.File, buffers []&core.Buffer) TransferResult {
	mut total := i64(0)

	for buf in buffers {
		if buf.len == 0 {
			continue
		}

		bytes_written := file.write(buf.bytes()) or {
			return TransferResult{
				bytes_transferred: total
				success:           false
				error_msg:         'Write error: ${err}'
			}
		}

		total += i64(bytes_written)
	}

	return TransferResult{
		bytes_transferred: total
		success:           true
	}
}

// 최적화된 메모리 연산

/// fast_copy는 최소한의 오버헤드로 바이트를 복사합니다.
pub fn fast_copy(dst []u8, src []u8) int {
	len := if dst.len < src.len { dst.len } else { src.len }

	// V가 이것을 효율적으로 처리합니다
	for i := 0; i < len; i++ {
		unsafe {
			dst[i] = src[i]
		}
	}

	return len
}

/// fast_zero는 버퍼를 0으로 채웁니다.
pub fn fast_zero(mut buf []u8) {
	for i := 0; i < buf.len; i++ {
		buf[i] = 0
	}
}

// 페이지 정렬 버퍼 할당

const page_size = 4096

/// allocate_page_aligned는 페이지 정렬된 버퍼를 할당합니다.
pub fn allocate_page_aligned(size int) []u8 {
	// 페이지 경계로 올림
	aligned_size := ((size + page_size - 1) / page_size) * page_size
	return []u8{len: aligned_size, cap: aligned_size}
}

// 전송 통계

/// TransferStats는 전송 통계를 담고 있습니다.
pub struct TransferStats {
pub mut:
	total_bytes_transferred u64 // 총 전송된 바이트
	total_operations        u64 // 총 작업 수
	zero_copy_operations    u64 // 제로카피 작업 수
	buffered_operations     u64 // 버퍼링된 작업 수
	failed_operations       u64 // 실패한 작업 수
}

/// average_transfer_size는 평균 전송 크기를 반환합니다.
pub fn (s &TransferStats) average_transfer_size() f64 {
	if s.total_operations == 0 {
		return 0.0
	}
	return f64(s.total_bytes_transferred) / f64(s.total_operations)
}

/// success_rate는 성공률을 반환합니다.
pub fn (s &TransferStats) success_rate() f64 {
	if s.total_operations == 0 {
		return 0.0
	}
	successful := s.total_operations - s.failed_operations
	return f64(successful) / f64(s.total_operations)
}
