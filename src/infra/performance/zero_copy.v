// Infra Layer - Zero-Copy I/O
// High-performance I/O operations using OS-level optimizations
// sendfile(), splice(), and memory-mapped I/O
module performance

import os

// ============================================================================
// Zero-Copy Transfer Types
// ============================================================================

// TransferMode represents the data transfer mode
pub enum TransferMode {
    copy        // Traditional copy (fallback)
    sendfile    // sendfile() syscall
    splice      // splice() for pipe-based transfer
    mmap        // Memory-mapped I/O
}

// TransferStats tracks zero-copy transfer statistics
pub struct TransferStats {
pub mut:
    bytes_zero_copy     u64  // Bytes transferred via zero-copy
    bytes_copied        u64  // Bytes transferred via traditional copy
    zero_copy_calls     u64  // Number of zero-copy operations
    fallback_calls      u64  // Number of fallback copy operations
    sendfile_errors     u64  // sendfile() errors (fell back to copy)
}

// ============================================================================
// ZeroCopyWriter - Optimized writer for socket I/O
// ============================================================================

// ZeroCopyWriter wraps a socket for zero-copy writes
pub struct ZeroCopyWriter {
pub:
    fd          int          // File descriptor
    mode        TransferMode // Preferred transfer mode
pub mut:
    stats       TransferStats
    buffer      &Buffer = unsafe { nil }  // Write buffer from pool
}

// new_zero_copy_writer creates a new zero-copy writer
pub fn new_zero_copy_writer(fd int) &ZeroCopyWriter {
    return &ZeroCopyWriter{
        fd: fd
        mode: detect_best_mode()
        buffer: get_global_pool().get_by_class(.medium)
    }
}

// detect_best_mode determines the best transfer mode for this platform
fn detect_best_mode() TransferMode {
    $if linux {
        return .sendfile
    }
    $if macos {
        return .sendfile  // macOS also supports sendfile
    }
    return .copy
}

// write_from_file transfers data from file to socket using zero-copy
// This is the key optimization for Fetch responses
pub fn (mut w ZeroCopyWriter) write_from_file(file_path string, offset i64, length int) !int {
    $if linux {
        return w.sendfile_linux(file_path, offset, length)
    } $else $if macos {
        return w.sendfile_macos(file_path, offset, length)
    } $else {
        return w.fallback_copy(file_path, offset, length)
    }
}

// sendfile_linux uses Linux sendfile() syscall
fn (mut w ZeroCopyWriter) sendfile_linux(file_path string, offset i64, length int) !int {
    // Open the source file
    src_fd := C.open(file_path.str, C.O_RDONLY, 0)
    if src_fd < 0 {
        return error('failed to open file: ${file_path}')
    }
    defer { C.close(src_fd) }
    
    mut off := offset
    mut remaining := length
    mut total_sent := 0
    
    for remaining > 0 {
        // Linux sendfile signature:
        // ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
        sent := C.sendfile(w.fd, src_fd, &off, remaining)
        
        if sent < 0 {
            // Error - fall back to copy
            w.stats.sendfile_errors += 1
            copied := w.fallback_copy_fd(src_fd, offset + total_sent, remaining)!
            total_sent += copied
            break
        } else if sent == 0 {
            // EOF
            break
        }
        
        total_sent += int(sent)
        remaining -= int(sent)
    }
    
    w.stats.bytes_zero_copy += u64(total_sent)
    w.stats.zero_copy_calls += 1
    return total_sent
}

// sendfile_macos uses macOS sendfile() syscall
fn (mut w ZeroCopyWriter) sendfile_macos(file_path string, offset i64, length int) !int {
    // Open the source file
    src_fd := C.open(file_path.str, C.O_RDONLY, 0)
    if src_fd < 0 {
        return error('failed to open file: ${file_path}')
    }
    defer { C.close(src_fd) }
    
    mut len := i64(length)
    
    // macOS sendfile signature:
    // int sendfile(int fd, int s, off_t offset, off_t *len, struct sf_hdtr *hdtr, int flags)
    result := C.sendfile(src_fd, w.fd, offset, &len, C.NULL, 0)
    
    if result < 0 && len == 0 {
        // Complete failure - fall back to copy
        w.stats.sendfile_errors += 1
        return w.fallback_copy(file_path, offset, length)
    }
    
    w.stats.bytes_zero_copy += u64(len)
    w.stats.zero_copy_calls += 1
    return int(len)
}

// fallback_copy uses traditional read/write when zero-copy is unavailable
fn (mut w ZeroCopyWriter) fallback_copy(file_path string, offset i64, length int) !int {
    file := os.open(file_path) or {
        return error('failed to open file: ${file_path}')
    }
    defer { file.close() }
    
    // Seek to offset
    file.seek(offset, .start) or {
        return error('failed to seek to offset ${offset}')
    }
    
    // Use pooled buffer for copying
    mut buf := w.buffer
    if buf == unsafe { nil } {
        buf = get_global_pool().get_by_class(.medium)
        w.buffer = buf
    }
    buf.reset()
    
    mut remaining := length
    mut total_sent := 0
    
    for remaining > 0 {
        to_read := if remaining > buf.cap { buf.cap } else { remaining }
        
        // Read into buffer
        bytes_read := file.read(mut buf.data[..to_read]) or {
            break
        }
        
        if bytes_read == 0 {
            break  // EOF
        }
        
        // Write to socket
        mut written := 0
        for written < bytes_read {
            n := C.write(w.fd, &buf.data[written], bytes_read - written)
            if n < 0 {
                return error('write failed')
            }
            written += int(n)
        }
        
        total_sent += written
        remaining -= written
    }
    
    w.stats.bytes_copied += u64(total_sent)
    w.stats.fallback_calls += 1
    return total_sent
}

// fallback_copy_fd copies from an already-open file descriptor
fn (mut w ZeroCopyWriter) fallback_copy_fd(src_fd int, offset i64, length int) !int {
    // Seek to offset
    C.lseek(src_fd, offset, C.SEEK_SET)
    
    // Use pooled buffer for copying
    mut buf := w.buffer
    if buf == unsafe { nil } {
        buf = get_global_pool().get_by_class(.medium)
        w.buffer = buf
    }
    buf.reset()
    
    mut remaining := length
    mut total_sent := 0
    
    for remaining > 0 {
        to_read := if remaining > buf.cap { buf.cap } else { remaining }
        
        // Read from source fd
        bytes_read := C.read(src_fd, &buf.data[0], to_read)
        if bytes_read <= 0 {
            break
        }
        
        // Write to socket
        mut written := 0
        for written < bytes_read {
            n := C.write(w.fd, &buf.data[written], bytes_read - written)
            if n < 0 {
                return error('write failed')
            }
            written += int(n)
        }
        
        total_sent += written
        remaining -= written
    }
    
    w.stats.bytes_copied += u64(total_sent)
    w.stats.fallback_calls += 1
    return total_sent
}

// write writes bytes to the socket (with buffer pooling)
pub fn (mut w ZeroCopyWriter) write(data []u8) !int {
    mut written := 0
    for written < data.len {
        n := C.write(w.fd, &data[written], data.len - written)
        if n < 0 {
            return error('write failed')
        }
        written += int(n)
    }
    return written
}

// write_buffered writes bytes using internal buffer
pub fn (mut w ZeroCopyWriter) write_buffered(data []u8) !int {
    if w.buffer == unsafe { nil } {
        w.buffer = get_global_pool().get_by_class(.medium)
    }
    
    bytes_written := w.buffer.write(data)
    
    // Flush if buffer is full
    if w.buffer.remaining() == 0 {
        w.flush()!
    }
    
    return bytes_written
}

// flush flushes the internal buffer to the socket
pub fn (mut w ZeroCopyWriter) flush() !int {
    if w.buffer == unsafe { nil } || w.buffer.len == 0 {
        return 0
    }
    
    total := w.write(w.buffer.bytes())!
    w.buffer.reset()
    return total
}

// close releases resources
pub fn (mut w ZeroCopyWriter) close() {
    if w.buffer != unsafe { nil } {
        w.buffer.release()
        w.buffer = unsafe { nil }
    }
}

// get_stats returns transfer statistics
pub fn (w &ZeroCopyWriter) get_stats() TransferStats {
    return w.stats
}

// ============================================================================
// Memory-Mapped File Reader
// ============================================================================

// MappedFile represents a memory-mapped file for read operations
pub struct MappedFile {
pub:
    path        string
    size        i64
    fd          int
pub mut:
    data        voidptr  // mmap'd region
    mapped      bool
}

// map_file memory-maps a file for reading
pub fn map_file(path string) !&MappedFile {
    // Get file size
    stat := os.stat(path) or {
        return error('failed to stat file: ${path}')
    }
    
    // Open file
    fd := C.open(path.str, C.O_RDONLY, 0)
    if fd < 0 {
        return error('failed to open file: ${path}')
    }
    
    // mmap the file
    data := C.mmap(C.NULL, stat.size, C.PROT_READ, C.MAP_PRIVATE, fd, 0)
    if data == C.MAP_FAILED {
        C.close(fd)
        return error('mmap failed for file: ${path}')
    }
    
    // Advise kernel we'll read sequentially
    C.madvise(data, stat.size, C.MADV_SEQUENTIAL)
    
    return &MappedFile{
        path: path
        size: stat.size
        fd: fd
        data: data
        mapped: true
    }
}

// read_at reads bytes from a specific offset
pub fn (m &MappedFile) read_at(offset i64, length int) ![]u8 {
    if !m.mapped {
        return error('file not mapped')
    }
    
    if offset < 0 || offset >= m.size {
        return error('offset out of bounds')
    }
    
    actual_length := if offset + length > m.size {
        int(m.size - offset)
    } else {
        length
    }
    
    // Create slice from mapped memory
    mut result := []u8{len: actual_length}
    ptr := unsafe { &u8(m.data) + offset }
    unsafe {
        C.memcpy(&result[0], ptr, actual_length)
    }
    
    return result
}

// get_ptr returns a direct pointer to mapped data (zero-copy access)
pub fn (m &MappedFile) get_ptr(offset i64) !voidptr {
    if !m.mapped {
        return error('file not mapped')
    }
    
    if offset < 0 || offset >= m.size {
        return error('offset out of bounds')
    }
    
    return unsafe { voidptr(&u8(m.data) + offset) }
}

// unmap unmaps and closes the file
pub fn (mut m MappedFile) unmap() {
    if m.mapped {
        C.munmap(m.data, m.size)
        C.close(m.fd)
        m.mapped = false
    }
}

// ============================================================================
// C Declarations for System Calls
// ============================================================================

fn C.open(path &char, flags int, mode int) int
fn C.close(fd int) int
fn C.read(fd int, buf voidptr, count int) int
fn C.write(fd int, buf voidptr, count int) int
fn C.lseek(fd int, offset i64, whence int) i64
fn C.sendfile(out_fd int, in_fd int, offset &i64, count int) i64
fn C.mmap(addr voidptr, length i64, prot int, flags int, fd int, offset i64) voidptr
fn C.munmap(addr voidptr, length i64) int
fn C.madvise(addr voidptr, length i64, advice int) int
fn C.memcpy(dest voidptr, src voidptr, n int) voidptr

// Constants
const (
    c_o_rdonly       = 0
    c_prot_read      = 1
    c_map_private    = 2
    c_map_failed     = voidptr(-1)
    c_madv_sequential = 2
    c_seek_set       = 0
)

fn C.O_RDONLY() int { return c_o_rdonly }
fn C.PROT_READ() int { return c_prot_read }
fn C.MAP_PRIVATE() int { return c_map_private }
fn C.MAP_FAILED() voidptr { return c_map_failed }
fn C.MADV_SEQUENTIAL() int { return c_madv_sequential }
fn C.SEEK_SET() int { return c_seek_set }
fn C.NULL() voidptr { return voidptr(0) }

// ============================================================================
// Vectored I/O (writev) for batched writes
// ============================================================================

// IoVec represents an I/O vector for scatter/gather operations
pub struct IoVec {
pub:
    base    voidptr
    len     int
}

// writev performs vectored write (scatter-gather I/O)
pub fn writev(fd int, vecs []IoVec) !int {
    if vecs.len == 0 {
        return 0
    }
    
    // Convert to C iovec structure
    mut iovecs := []C.iovec{len: vecs.len}
    for i, v in vecs {
        iovecs[i].iov_base = v.base
        iovecs[i].iov_len = v.len
    }
    
    result := C.writev(fd, &iovecs[0], vecs.len)
    if result < 0 {
        return error('writev failed')
    }
    
    return int(result)
}

// C structure for iovec
struct C.iovec {
    iov_base voidptr
    iov_len  int
}

fn C.writev(fd int, iov &C.iovec, iovcnt int) i64

// ============================================================================
// Global Zero-Copy Statistics
// ============================================================================

__global global_zero_copy_stats = TransferStats{}
__global global_stats_lock = sync.Mutex{}

// get_global_zero_copy_stats returns global zero-copy statistics
pub fn get_global_zero_copy_stats() TransferStats {
    global_stats_lock.@lock()
    defer { global_stats_lock.unlock() }
    return global_zero_copy_stats
}

// update_global_stats updates global statistics
pub fn update_global_stats(stats TransferStats) {
    global_stats_lock.@lock()
    defer { global_stats_lock.unlock() }
    
    global_zero_copy_stats.bytes_zero_copy += stats.bytes_zero_copy
    global_zero_copy_stats.bytes_copied += stats.bytes_copied
    global_zero_copy_stats.zero_copy_calls += stats.zero_copy_calls
    global_zero_copy_stats.fallback_calls += stats.fallback_calls
    global_zero_copy_stats.sendfile_errors += stats.sendfile_errors
}
