module performance

// Memory-Mapped File I/O for High-Performance Storage
// Provides efficient file access using OS-level memory mapping

import os
import time

// MmapRegion - Represents a memory-mapped region of a file
pub struct MmapRegion {
pub:
	file_path string // Path to the mapped file
	offset    i64    // Offset in the file
	length    int    // Length of the mapped region
mut:
	data      []u8   // Memory-mapped data (simulated)
	is_mapped bool   // Whether the region is currently mapped
	dirty     bool   // Whether the region has been modified
}

// MmapFile - Memory-mapped file wrapper
pub struct MmapFile {
pub:
	path      string
	read_only bool
mut:
	file      ?os.File
	size      i64
	regions   []MmapRegion
	page_size int
}

// Opens a file for memory mapping
pub fn MmapFile.open(path string, read_only bool) !MmapFile {
	mode := if read_only { 'r' } else { 'r+' }
	file := os.open_file(path, mode) or {
		return error('failed to open file: ${err}')
	}
	
	// Get file size
	file_size := os.file_size(path)
	
	return MmapFile{
		path: path
		read_only: read_only
		file: file
		size: i64(file_size)
		regions: []MmapRegion{}
		page_size: 4096 // Standard page size
	}
}

// Creates a new file for memory mapping
pub fn MmapFile.create(path string, initial_size i64) !MmapFile {
	// Create the file
	mut file := os.create(path) or {
		return error('failed to create file: ${err}')
	}
	
	// Pre-allocate the file with zeros
	if initial_size > 0 {
		zeros := []u8{len: 4096}
		mut remaining := initial_size
		for remaining > 0 {
			write_size := if remaining > 4096 { 4096 } else { int(remaining) }
			file.write(zeros[..write_size]) or {
				return error('failed to pre-allocate file: ${err}')
			}
			remaining -= write_size
		}
	}
	
	file.close()
	
	// Reopen in read-write mode
	return MmapFile.open(path, false)
}

// Maps a region of the file into memory
pub fn (mut m MmapFile) map_region(offset i64, length int) !&MmapRegion {
	if offset < 0 || offset >= m.size {
		return error('offset out of bounds: ${offset}, file size: ${m.size}')
	}
	
	actual_length := if offset + length > m.size {
		int(m.size - offset)
	} else {
		length
	}
	
	// Align offset to page boundary
	page_offset := offset % m.page_size
	aligned_offset := offset - page_offset
	aligned_length := actual_length + int(page_offset)
	
	// Read the data (simulating mmap)
	// In a real implementation, this would use platform-specific mmap syscalls
	mut data := []u8{len: aligned_length}
	
	if file := m.file {
		mut f := file
		f.seek(aligned_offset, .start) or {
			return error('failed to seek: ${err}')
		}
		bytes_read := f.read(mut data) or {
			return error('failed to read: ${err}')
		}
		if bytes_read < aligned_length {
			// Resize if we read less than expected
			data = data[..bytes_read].clone()
		}
	}
	
	region := MmapRegion{
		file_path: m.path
		offset: aligned_offset
		length: data.len
		data: data
		is_mapped: true
		dirty: false
	}
	
	m.regions << region
	return &m.regions[m.regions.len - 1]
}

// Unmaps a region
pub fn (mut m MmapFile) unmap_region(region &MmapRegion) ! {
	for i, r in m.regions {
		if r.offset == region.offset && r.length == region.length {
			if r.dirty && !m.read_only {
				m.sync_region(region)!
			}
			m.regions.delete(i)
			return
		}
	}
	return error('region not found')
}

// Syncs a dirty region back to disk
pub fn (mut m MmapFile) sync_region(region &MmapRegion) ! {
	if m.read_only {
		return error('cannot sync read-only file')
	}
	
	if file := m.file {
		mut f := file
		f.seek(region.offset, .start) or {
			return error('failed to seek: ${err}')
		}
		f.write(region.data) or {
			return error('failed to write: ${err}')
		}
		f.flush()
	}
}

// Syncs all dirty regions
pub fn (mut m MmapFile) sync_all() ! {
	for region in m.regions {
		if region.dirty {
			m.sync_region(&region)!
		}
	}
}

// Returns the file size
pub fn (m MmapFile) file_size() i64 {
	return m.size
}

// Extends the file size
pub fn (mut m MmapFile) extend(new_size i64) ! {
	if m.read_only {
		return error('cannot extend read-only file')
	}
	
	if new_size <= m.size {
		return
	}
	
	if file := m.file {
		mut f := file
		// Seek to end and write zeros
		f.seek(m.size, .start) or {
			return error('failed to seek: ${err}')
		}
		
		zeros := []u8{len: 4096}
		mut remaining := new_size - m.size
		for remaining > 0 {
			write_size := if remaining > 4096 { 4096 } else { int(remaining) }
			f.write(zeros[..write_size]) or {
				return error('failed to extend file: ${err}')
			}
			remaining -= write_size
		}
		f.flush()
	}
	
	m.size = new_size
}

// Closes the memory-mapped file
pub fn (mut m MmapFile) close() ! {
	// Sync all dirty regions
	m.sync_all() or {}
	
	// Clear regions
	m.regions.clear()
	
	// Close file
	if file := m.file {
		mut f := file
		f.close()
	}
	m.file = none
}

// LogSegmentMmap - Memory-mapped log segment for Kafka-style storage
pub struct LogSegmentMmap {
pub:
	base_offset i64
	path        string
mut:
	log_file    ?MmapFile
	index_file  ?MmapFile
	position    i64
	max_size    i64
}

// Creates a new log segment with memory mapping
pub fn LogSegmentMmap.create(dir string, base_offset i64, max_size i64) !LogSegmentMmap {
	log_path := '${dir}/${base_offset:020}.log'
	index_path := '${dir}/${base_offset:020}.index'
	
	// Ensure directory exists
	os.mkdir_all(dir) or {
		return error('failed to create directory: ${err}')
	}
	
	// Create log file
	log_file := MmapFile.create(log_path, max_size) or {
		return error('failed to create log file: ${err}')
	}
	
	// Create index file (10% of log size for index)
	index_size := max_size / 10
	index_file := MmapFile.create(index_path, index_size) or {
		return error('failed to create index file: ${err}')
	}
	
	return LogSegmentMmap{
		base_offset: base_offset
		path: dir
		log_file: log_file
		index_file: index_file
		position: 0
		max_size: max_size
	}
}

// Opens an existing log segment
pub fn LogSegmentMmap.open(dir string, base_offset i64) !LogSegmentMmap {
	log_path := '${dir}/${base_offset:020}.log'
	index_path := '${dir}/${base_offset:020}.index'
	
	// Open log file
	log_file := MmapFile.open(log_path, false) or {
		return error('failed to open log file: ${err}')
	}
	
	// Open index file
	index_file := MmapFile.open(index_path, false) or {
		return error('failed to open index file: ${err}')
	}
	
	return LogSegmentMmap{
		base_offset: base_offset
		path: dir
		log_file: log_file
		index_file: index_file
		position: log_file.size
		max_size: log_file.size
	}
}

// Appends a record to the segment
pub fn (mut s LogSegmentMmap) append(data []u8) !i64 {
	record_size := data.len
	
	if s.position + record_size > s.max_size {
		return error('segment full')
	}
	
	if mut log_file := s.log_file {
		// Map the region where we'll write
		mut region := log_file.map_region(s.position, record_size)!
		
		// Write data to the region
		for i, b in data {
			if i < region.data.len {
				region.data[i] = b
			}
		}
		region.dirty = true
		
		// Sync the region
		log_file.sync_region(region)!
		
		offset := s.position
		s.position += record_size
		
		return offset
	}
	
	return error('log file not initialized')
}

// Reads a record from the segment
pub fn (mut s LogSegmentMmap) read(offset i64, length int) ![]u8 {
	if offset < 0 || offset >= s.position {
		return error('offset out of bounds')
	}
	
	if mut log_file := s.log_file {
		region := log_file.map_region(offset, length)!
		return region.data.clone()
	}
	
	return error('log file not initialized')
}

// Returns current position (next write offset)
pub fn (s LogSegmentMmap) current_position() i64 {
	return s.position
}

// Returns remaining capacity
pub fn (s LogSegmentMmap) remaining_capacity() i64 {
	return s.max_size - s.position
}

// Checks if segment is full
pub fn (s LogSegmentMmap) is_full() bool {
	return s.position >= s.max_size
}

// Flushes all pending writes
pub fn (mut s LogSegmentMmap) flush() ! {
	if mut log_file := s.log_file {
		log_file.sync_all()!
	}
	if mut index_file := s.index_file {
		index_file.sync_all()!
	}
}

// Closes the segment
pub fn (mut s LogSegmentMmap) close() ! {
	if mut log_file := s.log_file {
		log_file.close()!
	}
	if mut index_file := s.index_file {
		index_file.close()!
	}
	s.log_file = none
	s.index_file = none
}

// MmapBenchmark - Benchmarks for mmap vs regular I/O
pub struct MmapBenchmark {
mut:
	iterations u64
	data_size  int
	temp_dir   string
}

pub fn MmapBenchmark.new(iterations u64, data_size int) MmapBenchmark {
	return MmapBenchmark{
		iterations: iterations
		data_size: data_size
		temp_dir: '/tmp/mmap_benchmark_${time.now().unix()}'
	}
}

// Cleanup temporary files
pub fn (mut b MmapBenchmark) cleanup() {
	os.rmdir_all(b.temp_dir) or {}
}

// Benchmarks mmap write vs regular write
pub fn (mut b MmapBenchmark) benchmark_write() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or {
		return error('failed to create temp dir')
	}
	
	// Generate test data
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}
	
	// Benchmark mmap write
	mmap_path := '${b.temp_dir}/mmap_test.dat'
	sw1 := time.new_stopwatch()
	
	mut mmap_file := MmapFile.create(mmap_path, i64(b.data_size * int(b.iterations))) or {
		return error('failed to create mmap file')
	}
	
	for i in 0 .. b.iterations {
		offset := i64(i) * i64(b.data_size)
		mut region := mmap_file.map_region(offset, b.data_size) or { continue }
		for j, byte_val in data {
			if j < region.data.len {
				region.data[j] = byte_val
			}
		}
		region.dirty = true
	}
	mmap_file.sync_all() or {}
	mmap_file.close() or {}
	mmap_time := sw1.elapsed().nanoseconds()
	
	// Benchmark regular write
	regular_path := '${b.temp_dir}/regular_test.dat'
	sw2 := time.new_stopwatch()
	
	mut regular_file := os.create(regular_path) or {
		return error('failed to create regular file')
	}
	
	for _ in 0 .. b.iterations {
		regular_file.write(data) or { continue }
	}
	regular_file.flush()
	regular_file.close()
	regular_time := sw2.elapsed().nanoseconds()
	
	return mmap_time, regular_time
}

// Benchmarks mmap read vs regular read
pub fn (mut b MmapBenchmark) benchmark_read() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or {
		return error('failed to create temp dir')
	}
	
	// Create test file
	test_path := '${b.temp_dir}/read_test.dat'
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}
	
	// Write test data
	mut write_file := os.create(test_path) or {
		return error('failed to create test file')
	}
	for _ in 0 .. b.iterations {
		write_file.write(data) or {}
	}
	write_file.close()
	
	// Benchmark mmap read
	sw1 := time.new_stopwatch()
	mut mmap_file := MmapFile.open(test_path, true) or {
		return error('failed to open mmap file')
	}
	
	for i in 0 .. b.iterations {
		offset := i64(i) * i64(b.data_size)
		_ := mmap_file.map_region(offset, b.data_size) or { continue }
	}
	mmap_file.close() or {}
	mmap_time := sw1.elapsed().nanoseconds()
	
	// Benchmark regular read
	sw2 := time.new_stopwatch()
	mut regular_file := os.open(test_path) or {
		return error('failed to open regular file')
	}
	
	mut read_buf := []u8{len: b.data_size}
	for _ in 0 .. b.iterations {
		regular_file.read(mut read_buf) or { break }
	}
	regular_file.close()
	regular_time := sw2.elapsed().nanoseconds()
	
	return mmap_time, regular_time
}
