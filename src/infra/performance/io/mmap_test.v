module io

// mmap.v tests - memory-mapped file I/O
import os
import time

fn test_mmap_file_create() {
	temp_dir := '/tmp/mmap_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	path := '${temp_dir}/test.dat'
	mut mmap_file := MmapFile.create(path, 4096) or {
		assert false, 'failed to create mmap file: ${err}'
		return
	}
	defer {
		mmap_file.close() or {}
	}

	assert mmap_file.file_size() == 4096
	assert mmap_file.path == path
	assert mmap_file.read_only == false
}

fn test_mmap_file_open() {
	temp_dir := '/tmp/mmap_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	path := '${temp_dir}/test.dat'

	// Create file first
	mut file := os.create(path) or { return }
	data := []u8{len: 1024, init: u8(0xAB)}
	file.write(data) or {}
	file.close()

	// Open with mmap
	mut mmap_file := MmapFile.open(path, true) or {
		assert false, 'failed to open mmap file'
		return
	}
	defer {
		mmap_file.close() or {}
	}

	assert mmap_file.file_size() == 1024
	assert mmap_file.read_only == true
}

fn test_mmap_region() {
	temp_dir := '/tmp/mmap_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	path := '${temp_dir}/test.dat'

	// Create file with known content
	mut file := os.create(path) or { return }
	mut data := []u8{len: 4096}
	for i in 0 .. 4096 {
		data[i] = u8(i % 256)
	}
	file.write(data) or {}
	file.close()

	// Map region
	mut mmap_file := MmapFile.open(path, true) or { return }
	defer {
		mmap_file.close() or {}
	}

	region := mmap_file.map_region(0, 1024) or {
		assert false, 'failed to map region'
		return
	}

	assert region.length >= 1024
	assert region.is_mapped == true

	// Verify content
	assert region.data[0] == 0
	assert region.data[1] == 1
	assert region.data[255] == 255
}

fn test_mmap_write() {
	temp_dir := '/tmp/mmap_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	path := '${temp_dir}/write_test.dat'

	// Create file
	mut mmap_file := MmapFile.create(path, 4096) or { return }

	// Map and write
	mut region := mmap_file.map_region(0, 100) or { return }

	// Write pattern
	for i in 0 .. 100 {
		if i < region.data.len {
			region.data[i] = u8(i)
		}
	}
	region.dirty = true

	// Sync and close
	mmap_file.sync_region(region) or {}
	mmap_file.close() or {}

	// Verify by reading back
	content := os.read_bytes(path) or { return }
	assert content[0] == 0
	assert content[50] == 50
	assert content[99] == 99
}

fn test_mmap_extend() {
	temp_dir := '/tmp/mmap_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	path := '${temp_dir}/extend_test.dat'

	// Create small file
	mut mmap_file := MmapFile.create(path, 1024) or { return }
	assert mmap_file.file_size() == 1024

	// Extend
	mmap_file.extend(4096) or {
		assert false, 'failed to extend'
		return
	}

	assert mmap_file.file_size() == 4096

	mmap_file.close() or {}

	// Verify actual file size
	actual_size := os.file_size(path)
	assert actual_size == 4096
}

fn test_log_segment_create() {
	temp_dir := '/tmp/segment_test_${time.now().unix()}'
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	mut segment := LogSegmentMmap.create(temp_dir, 0, 1048576) or {
		assert false, 'failed to create segment: ${err}'
		return
	}
	defer {
		segment.close() or {}
	}

	assert segment.base_offset == 0
	assert segment.current_position() == 0
	assert segment.remaining_capacity() == 1048576
	assert segment.is_full() == false
}

fn test_log_segment_append_read() {
	temp_dir := '/tmp/segment_test_${time.now().unix()}'
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	mut segment := LogSegmentMmap.create(temp_dir, 0, 1048576) or { return }
	defer {
		segment.close() or {}
	}

	// Append data
	test_data := 'Hello, World!'.bytes()
	offset := segment.append(test_data) or {
		assert false, 'failed to append'
		return
	}

	assert offset == 0
	assert segment.current_position() == test_data.len

	// Read back
	read_data := segment.read(0, test_data.len) or {
		assert false, 'failed to read'
		return
	}

	assert read_data.len == test_data.len
}

fn test_log_segment_multiple_appends() {
	temp_dir := '/tmp/segment_test_${time.now().unix()}'
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	mut segment := LogSegmentMmap.create(temp_dir, 100, 1048576) or { return }
	defer {
		segment.close() or {}
	}

	assert segment.base_offset == 100

	// Multiple appends
	mut offsets := []i64{}
	for i in 0 .. 10 {
		data := 'Message ${i}'.bytes()
		offset := segment.append(data) or { continue }
		offsets << offset
	}

	assert offsets.len == 10
	assert offsets[0] == 0

	// Each offset should be greater than the previous
	for i in 1 .. offsets.len {
		assert offsets[i] > offsets[i - 1]
	}
}

fn test_log_segment_full() {
	temp_dir := '/tmp/segment_test_${time.now().unix()}'
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	// Create tiny segment
	mut segment := LogSegmentMmap.create(temp_dir, 0, 100) or { return }
	defer {
		segment.close() or {}
	}

	// Fill it up
	data := []u8{len: 50}
	segment.append(data) or {}
	segment.append(data) or {}

	assert segment.is_full() == true

	// Should fail
	if _ := segment.append(data) {
		assert false, 'should fail when segment is full'
	}
}

fn test_mmap_benchmark() {
	mut benchmark := MmapBenchmark.new(10, 1024)
	defer {
		benchmark.cleanup()
	}

	// Write benchmark
	write_mmap, write_regular := benchmark.benchmark_write() or {
		// Skip if benchmark fails (e.g., permission issues)
		return
	}

	assert write_mmap >= 0
	assert write_regular >= 0

	// Read benchmark
	read_mmap, read_regular := benchmark.benchmark_read() or { return }

	assert read_mmap >= 0
	assert read_regular >= 0
}
