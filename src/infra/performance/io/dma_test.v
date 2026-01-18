module io

// Tests for dma.v - DMA and Scatter-Gather I/O

import os
import time

fn test_platform_capabilities() {
	caps := get_platform_capabilities()
	
	// OS name should not be empty
	assert caps.os_name.len > 0
	
	$if linux {
		assert caps.has_scatter_gather == true
		assert caps.has_sendfile == true
		assert caps.has_splice == true
		assert caps.has_copy_file_range == true
		assert caps.os_name == 'Linux'
	}
	
	$if macos {
		assert caps.has_scatter_gather == true
		assert caps.has_sendfile == true
		assert caps.has_splice == false
		assert caps.has_copy_file_range == false
		assert caps.os_name == 'macOS'
	}
	
	$if windows {
		assert caps.has_scatter_gather == false
		assert caps.has_sendfile == false
		assert caps.os_name == 'Windows'
	}
}

fn test_sg_buffer_creation() {
	// Test new buffer
	buf := new_sg_buffer(1024)
	assert buf.data.len == 1024
	assert buf.len == 0
	
	// Test buffer from data
	data := [u8(1), 2, 3, 4, 5]
	buf2 := new_sg_buffer_from(data)
	assert buf2.data.len == 5
	assert buf2.len == 5
	assert buf2.data[0] == 1
	assert buf2.data[4] == 5
}

fn test_dma_transfer_creation() {
	dma := new_dma_transfer()
	
	// Check capabilities are detected
	assert dma.capabilities.os_name.len > 0
	
	// Stats should be zero
	assert dma.stats.total_transfers == 0
	assert dma.stats.zero_copy_transfers == 0
	assert dma.stats.fallback_transfers == 0
}

fn test_dma_zero_copy_ratio() {
	mut dma := new_dma_transfer()
	
	// Initially zero
	assert dma.zero_copy_ratio() == 0.0
	
	// Simulate some transfers
	dma.stats.total_transfers = 10
	dma.stats.zero_copy_transfers = 7
	dma.stats.fallback_transfers = 3
	
	ratio := dma.zero_copy_ratio()
	assert ratio == 0.7
}

fn test_scatter_read_file() {
	temp_dir := '/tmp/dma_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}
	
	// Create test file
	test_path := '${temp_dir}/scatter_test.dat'
	mut write_file := os.create(test_path) or { return }
	
	// Write test data: 100 bytes
	mut test_data := []u8{len: 100}
	for i in 0 .. 100 {
		test_data[i] = u8(i)
	}
	write_file.write(test_data) or { return }
	write_file.close()
	
	// Read with scatter
	mut read_file := os.open(test_path) or { return }
	defer { read_file.close() }
	
	mut buffers := [
		new_sg_buffer(30),
		new_sg_buffer(30),
		new_sg_buffer(40),
	]
	
	result := scatter_read_file(mut read_file, mut buffers)
	assert result.success == true
	assert result.bytes_transferred == 100
	
	// Verify buffer contents
	assert buffers[0].len == 30
	assert buffers[0].data[0] == 0
	assert buffers[0].data[29] == 29
	
	assert buffers[1].len == 30
	assert buffers[1].data[0] == 30
	
	assert buffers[2].len == 40
	assert buffers[2].data[0] == 60
}

fn test_gather_write_file() {
	temp_dir := '/tmp/dma_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}
	
	// Create buffers with data
	mut buf1 := new_sg_buffer(10)
	for i in 0 .. 10 {
		buf1.data[i] = u8(i)
	}
	buf1.len = 10
	
	mut buf2 := new_sg_buffer(20)
	for i in 0 .. 20 {
		buf2.data[i] = u8(100 + i)
	}
	buf2.len = 20
	
	mut buf3 := new_sg_buffer(5)
	for i in 0 .. 5 {
		buf3.data[i] = u8(200 + i)
	}
	buf3.len = 5
	
	// Write with gather
	test_path := '${temp_dir}/gather_test.dat'
	mut write_file := os.create(test_path) or { return }
	
	buffers := [buf1, buf2, buf3]
	result := gather_write_file(mut write_file, buffers)
	write_file.close()
	
	assert result.success == true
	assert result.bytes_transferred == 35
	
	// Verify file contents
	content := os.read_bytes(test_path) or { return }
	assert content.len == 35
	assert content[0] == 0
	assert content[9] == 9
	assert content[10] == 100
	assert content[30] == 200
}

fn test_dma_result_helpers() {
	// Test success result
	success := dma_success(1024, true)
	assert success.success == true
	assert success.bytes_transferred == 1024
	assert success.used_zero_copy == true
	assert success.error_msg == ''
	
	// Test success with offset
	success_offset := dma_success_with_offset(2048, true, 100)
	assert success_offset.success == true
	assert success_offset.bytes_transferred == 2048
	assert success_offset.new_offset == 100
	
	// Test error result
	err := dma_error('test error')
	assert err.success == false
	assert err.bytes_transferred == 0
	assert err.used_zero_copy == false
	assert err.error_msg == 'test error'
}

fn test_sendfile_fallback() {
	// Test fallback behavior
	result := sendfile_fallback(-1, -1, 0, 1024)
	assert result.success == true
	assert result.used_zero_copy == false
}

fn test_splice_on_non_linux() {
	$if !linux {
		result := splice_native(-1, -1, 1024, false)
		assert result.success == false
		assert result.error_msg.contains('Linux')
	}
}

fn test_copy_file_range_on_non_linux() {
	$if !linux {
		result := copy_file_range_native(-1, 0, -1, 0, 1024)
		assert result.success == false
		assert result.error_msg.contains('Linux')
	}
}

fn test_dma_stats() {
	mut dma := new_dma_transfer()
	
	// Initial stats
	stats := dma.get_stats()
	assert stats.total_transfers == 0
	assert stats.bytes_zero_copy == 0
	assert stats.bytes_fallback == 0
}

fn test_scatter_gather_empty_buffers() {
	mut empty_buffers := []ScatterGatherBuffer{}
	
	result := gather_write_native(-1, empty_buffers)
	assert result.success == false
	assert result.error_msg.contains('no buffers')
}

fn test_multiple_scatter_read_operations() {
	temp_dir := '/tmp/dma_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}
	
	// Create larger test file
	test_path := '${temp_dir}/multi_scatter.dat'
	mut write_file := os.create(test_path) or { return }
	
	mut test_data := []u8{len: 1000}
	for i in 0 .. 1000 {
		test_data[i] = u8(i % 256)
	}
	write_file.write(test_data) or { return }
	write_file.close()
	
	// Multiple reads
	mut read_file := os.open(test_path) or { return }
	defer { read_file.close() }
	
	mut dma := new_dma_transfer()
	mut total_read := i64(0)
	
	for _ in 0 .. 5 {
		mut buffers := [
			new_sg_buffer(100),
			new_sg_buffer(100),
		]
		
		result := scatter_read_file(mut read_file, mut buffers)
		if result.bytes_transferred == 0 {
			break
		}
		total_read += result.bytes_transferred
	}
	
	assert total_read == 1000
}
