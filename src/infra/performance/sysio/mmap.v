/// Memory-mapped file I/O for high-performance storage
/// Provides efficient file access using OS-level memory mapping
module sysio

import os
import time

/// MmapRegion represents a memory-mapped region of a file.
pub struct MmapRegion {
pub:
	file_path string
	offset    i64
	length    int
mut:
	data      []u8
	is_mapped bool
	dirty     bool
}

/// MmapFile is a memory-mapped file wrapper.
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

/// open opens a file for memory mapping.
pub fn MmapFile.open(path string, read_only bool) !MmapFile {
	mode := if read_only { 'r' } else { 'r+' }
	file := os.open_file(path, mode) or { return error('failed to open file: ${err}') }

	// Get file size
	file_size := os.file_size(path)

	return MmapFile{
		path:      path
		read_only: read_only
		file:      file
		size:      i64(file_size)
		regions:   []MmapRegion{}
		page_size: 4096
	}
}

/// create creates a new file for memory mapping.
pub fn MmapFile.create(path string, initial_size i64) !MmapFile {
	// Create file
	mut file := os.create(path) or { return error('failed to create file: ${err}') }

	// Pre-allocate file with zeros
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

/// map_region maps a region of the file into memory.
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

	// Read data (mmap simulation)
	// Real implementation would use the platform-specific mmap syscall
	mut data := []u8{len: aligned_length}

	if file := m.file {
		mut f := file
		f.seek(aligned_offset, .start) or { return error('failed to seek: ${err}') }
		bytes_read := f.read(mut data) or { return error('failed to read: ${err}') }
		if bytes_read < aligned_length {
			// Resize if fewer bytes were read than expected
			data = data[..bytes_read].clone()
		}
	}

	region := MmapRegion{
		file_path: m.path
		offset:    aligned_offset
		length:    data.len
		data:      data
		is_mapped: true
		dirty:     false
	}

	m.regions << region
	return &m.regions[m.regions.len - 1]
}

/// unmap_region unmaps a region.
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

/// sync_region synchronizes a dirty region to disk.
pub fn (mut m MmapFile) sync_region(region &MmapRegion) ! {
	if m.read_only {
		return error('cannot sync read-only file')
	}

	if file := m.file {
		mut f := file
		f.seek(region.offset, .start) or { return error('failed to seek: ${err}') }
		f.write(region.data) or { return error('failed to write: ${err}') }
		f.flush()
	}
}

/// sync_all synchronizes all dirty regions.
pub fn (mut m MmapFile) sync_all() ! {
	for region in m.regions {
		if region.dirty {
			m.sync_region(&region)!
		}
	}
}

/// file_size returns the size of the file.
pub fn (m MmapFile) file_size() i64 {
	return m.size
}

/// extend expands the file size.
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
		f.seek(m.size, .start) or { return error('failed to seek: ${err}') }

		zeros := []u8{len: 4096}
		mut remaining := new_size - m.size
		for remaining > 0 {
			write_size := if remaining > 4096 { 4096 } else { int(remaining) }
			f.write(zeros[..write_size]) or { return error('failed to extend file: ${err}') }
			remaining -= write_size
		}
		f.flush()
	}

	m.size = new_size
}

/// close closes the memory-mapped file.
pub fn (mut m MmapFile) close() ! {
	// Sync all dirty regions
	m.sync_all()!

	// Clean up regions
	m.regions.clear()

	// Close file
	if file := m.file {
		mut f := file
		f.close()
	}
	m.file = none
}

/// LogSegmentMmap is a memory-mapped log segment for Kafka-style storage.
pub struct LogSegmentMmap {
pub:
	base_offset i64
	path        string
mut:
	log_file   ?MmapFile
	index_file ?MmapFile
	position   i64
	max_size   i64
}

/// create creates a new log segment using memory mapping.
pub fn LogSegmentMmap.create(dir string, base_offset i64, max_size i64) !LogSegmentMmap {
	log_path := '${dir}/${base_offset:020}.log'
	index_path := '${dir}/${base_offset:020}.index'

	// Ensure directory exists
	os.mkdir_all(dir) or { return error('failed to create directory: ${err}') }

	// Create log file
	log_file := MmapFile.create(log_path, max_size) or {
		return error('failed to create log file: ${err}')
	}

	// Create index file (10% of log size)
	index_size := max_size / 10
	index_file := MmapFile.create(index_path, index_size) or {
		return error('failed to create index file: ${err}')
	}

	return LogSegmentMmap{
		base_offset: base_offset
		path:        dir
		log_file:    log_file
		index_file:  index_file
		position:    0
		max_size:    max_size
	}
}

/// open opens an existing log segment.
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
		path:        dir
		log_file:    log_file
		index_file:  index_file
		position:    log_file.size
		max_size:    log_file.size
	}
}

/// append appends a record to the segment.
pub fn (mut s LogSegmentMmap) append(data []u8) !i64 {
	record_size := data.len

	if s.position + record_size > s.max_size {
		return error('segment full')
	}

	if mut log_file := s.log_file {
		// Map region to write
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

/// read reads a record from the segment.
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

/// current_position returns the current position (next write offset).
pub fn (s LogSegmentMmap) current_position() i64 {
	return s.position
}

/// remaining_capacity returns the remaining capacity.
pub fn (s LogSegmentMmap) remaining_capacity() i64 {
	return s.max_size - s.position
}

/// is_full checks whether the segment is full.
pub fn (s LogSegmentMmap) is_full() bool {
	return s.position >= s.max_size
}

/// flush flushes all pending writes.
pub fn (mut s LogSegmentMmap) flush() ! {
	if mut log_file := s.log_file {
		log_file.sync_all()!
	}
	if mut index_file := s.index_file {
		index_file.sync_all()!
	}
}

/// close closes the segment.
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

/// MmapBenchmark benchmarks mmap vs regular I/O.
pub struct MmapBenchmark {
mut:
	iterations u64
	data_size  int
	temp_dir   string
}

/// new creates a new MmapBenchmark.
fn MmapBenchmark.new(iterations u64, data_size int) MmapBenchmark {
	return MmapBenchmark{
		iterations: iterations
		data_size:  data_size
		temp_dir:   '/tmp/mmap_benchmark_${time.now().unix()}'
	}
}

/// cleanup removes temporary files.
fn (mut b MmapBenchmark) cleanup() {
	os.rmdir_all(b.temp_dir) or {}
}

/// benchmark_write benchmarks mmap writes vs regular writes.
fn (mut b MmapBenchmark) benchmark_write() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or { return error('failed to create temp dir') }

	// Generate test data
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// mmap write benchmark
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
	mmap_file.sync_all()!
	mmap_file.close()!
	mmap_time := sw1.elapsed().nanoseconds()

	// Regular write benchmark
	regular_path := '${b.temp_dir}/regular_test.dat'
	sw2 := time.new_stopwatch()

	mut regular_file := os.create(regular_path) or { return error('failed to create regular file') }

	for _ in 0 .. b.iterations {
		regular_file.write(data) or { continue }
	}
	regular_file.flush()
	regular_file.close()
	regular_time := sw2.elapsed().nanoseconds()

	return mmap_time, regular_time
}

/// benchmark_read benchmarks mmap reads vs regular reads.
fn (mut b MmapBenchmark) benchmark_read() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or { return error('failed to create temp dir') }

	// Create test file
	test_path := '${b.temp_dir}/read_test.dat'
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// Write test data
	mut write_file := os.create(test_path) or { return error('failed to create test file') }
	for _ in 0 .. b.iterations {
		write_file.write(data)!
	}
	write_file.close()

	// mmap read benchmark
	sw1 := time.new_stopwatch()
	mut mmap_file := MmapFile.open(test_path, true) or { return error('failed to open mmap file') }

	for i in 0 .. b.iterations {
		offset := i64(i) * i64(b.data_size)
		_ := mmap_file.map_region(offset, b.data_size) or { continue }
	}
	mmap_file.close() or {}
	mmap_time := sw1.elapsed().nanoseconds()

	// Regular read benchmark
	sw2 := time.new_stopwatch()
	mut regular_file := os.open(test_path) or { return error('failed to open regular file') }

	mut read_buf := []u8{len: b.data_size}
	for _ in 0 .. b.iterations {
		regular_file.read(mut read_buf) or { break }
	}
	regular_file.close()
	regular_time := sw2.elapsed().nanoseconds()

	return mmap_time, regular_time
}
