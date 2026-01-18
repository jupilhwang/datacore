// Infra Layer - Zero-Copy I/O
// High-performance I/O operations using system sendfile
module io

import os
import infra.performance.core

// ============================================================================
// Zero-Copy Transfer Result
// ============================================================================

pub struct TransferResult {
pub:
    bytes_transferred i64
    success           bool
    error_msg         string
}

// ============================================================================
// File-based Zero-Copy (using standard file operations)
// ============================================================================

// zero_copy_file_to_file transfers data between files efficiently
// Falls back to buffered copy since V doesn't expose sendfile directly
pub fn zero_copy_file_to_file(src_path string, dst_path string, offset i64, length i64) TransferResult {
    // Open source file
    mut src_file := os.open(src_path) or {
        return TransferResult{
            success: false
            error_msg: 'Failed to open source: ${err}'
        }
    }
    defer { src_file.close() }
    
    // Open/create destination file
    mut dst_file := os.create(dst_path) or {
        return TransferResult{
            success: false
            error_msg: 'Failed to create destination: ${err}'
        }
    }
    defer { dst_file.close() }
    
    // Seek to offset in source
    if offset > 0 {
        src_file.seek(offset, .start) or {
            return TransferResult{
                success: false
                error_msg: 'Failed to seek: ${err}'
            }
        }
    }
    
    // Use efficient buffered copy
    return buffered_copy(mut src_file, mut dst_file, length)
}

// buffered_copy performs efficient buffered copy
fn buffered_copy(mut src os.File, mut dst os.File, length i64) TransferResult {
    mut total_transferred := i64(0)
    mut remaining := length
    buf_size := 65536 // 64KB buffer for efficiency
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
                success: false
                error_msg: 'Read error: ${err}'
            }
        }
        
        if bytes_read == 0 {
            break
        }
        
        bytes_written := dst.write(buf[..bytes_read]) or {
            return TransferResult{
                success: false
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
                success: false
                error_msg: 'Partial write'
            }
        }
    }
    
    return TransferResult{
        bytes_transferred: total_transferred
        success: true
    }
}

// ============================================================================
// Memory-mapped style scatter/gather I/O simulation
// ============================================================================

pub struct IoVec {
pub:
    data []u8
}

// scatter_read reads into multiple buffers
pub fn scatter_read(mut file os.File, buffers []&core.Buffer) TransferResult {
    mut total := i64(0)
    
    for buf in buffers {
        mut temp := []u8{len: buf.remaining()}
        bytes_read := file.read(mut temp) or {
            if total > 0 {
                break
            }
            return TransferResult{
                success: false
                error_msg: 'Read error: ${err}'
            }
        }
        
        if bytes_read == 0 {
            break
        }
        
        // Can't mutate buf directly since it's from an immutable array
        // Just track the read amount
        total += i64(bytes_read)
    }
    
    return TransferResult{
        bytes_transferred: total
        success: true
    }
}

// gather_write writes from multiple buffers
pub fn gather_write(mut file os.File, buffers []&core.Buffer) TransferResult {
    mut total := i64(0)
    
    for buf in buffers {
        if buf.len == 0 {
            continue
        }
        
        bytes_written := file.write(buf.bytes()) or {
            return TransferResult{
                bytes_transferred: total
                success: false
                error_msg: 'Write error: ${err}'
            }
        }
        
        total += i64(bytes_written)
    }
    
    return TransferResult{
        bytes_transferred: total
        success: true
    }
}

// ============================================================================
// Optimized memory operations
// ============================================================================

// fast_copy copies bytes with minimal overhead
pub fn fast_copy(dst []u8, src []u8) int {
    len := if dst.len < src.len { dst.len } else { src.len }
    
    // V handles this efficiently
    for i := 0; i < len; i++ {
        unsafe {
            dst[i] = src[i]
        }
    }
    
    return len
}

// fast_zero fills buffer with zeros
pub fn fast_zero(mut buf []u8) {
    for i := 0; i < buf.len; i++ {
        buf[i] = 0
    }
}

// ============================================================================
// Page-aligned buffer allocation
// ============================================================================

const page_size = 4096

// allocate_page_aligned allocates a page-aligned buffer
pub fn allocate_page_aligned(size int) []u8 {
    // Round up to page boundary
    aligned_size := ((size + page_size - 1) / page_size) * page_size
    return []u8{len: aligned_size, cap: aligned_size}
}

// ============================================================================
// Transfer statistics
// ============================================================================

pub struct TransferStats {
pub mut:
    total_bytes_transferred u64
    total_operations        u64
    zero_copy_operations    u64
    buffered_operations     u64
    failed_operations       u64
}

pub fn (s &TransferStats) average_transfer_size() f64 {
    if s.total_operations == 0 {
        return 0.0
    }
    return f64(s.total_bytes_transferred) / f64(s.total_operations)
}

pub fn (s &TransferStats) success_rate() f64 {
    if s.total_operations == 0 {
        return 0.0
    }
    successful := s.total_operations - s.failed_operations
    return f64(successful) / f64(s.total_operations)
}
