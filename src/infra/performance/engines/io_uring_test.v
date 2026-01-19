module engines

// Tests for io_uring.v - Linux Async I/O
import os
import time

fn test_async_io_capabilities() {
	caps := get_async_io_capabilities()

	// Platform name should not be empty
	assert caps.platform_name.len > 0

	$if linux {
		assert caps.has_io_uring == true
		assert caps.has_aio == true
		assert caps.platform_name == 'Linux'
	}

	$if macos {
		assert caps.has_io_uring == false
		assert caps.has_aio == true
		assert caps.platform_name == 'macOS'
	}

	$if windows {
		assert caps.has_io_uring == false
		assert caps.has_iocp == true
		assert caps.platform_name == 'Windows'
	}
}

fn test_io_uring_config_defaults() {
	config := IoUringConfig{}

	assert config.queue_depth == 256
	assert config.flags == 0
	assert config.sq_thread_cpu == 0
	assert config.sq_thread_idle == 1000
}

fn test_io_uring_creation_non_linux() {
	$if !linux {
		// Should fail on non-Linux
		ring := new_io_uring(IoUringConfig{}) or {
			assert err.msg().contains('Linux')
			return
		}
		assert false, 'should have failed on non-Linux'
	}
}

fn test_io_uring_stats_structure() {
	stats := IoUringStats{
		submissions: 100
		completions: 95
		errors:      5
		pending:     10
	}

	assert stats.submissions == 100
	assert stats.completions == 95
	assert stats.errors == 5
	assert stats.pending == 10
}

fn test_io_uring_result_structure() {
	result := IoUringResult{
		user_data: 12345
		result:    1024
		success:   true
	}

	assert result.user_data == 12345
	assert result.result == 1024
	assert result.success == true
}

fn test_io_uring_fallback_read() {
	temp_dir := '/tmp/io_uring_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	// Create test file
	test_path := '${temp_dir}/test.dat'
	mut test_data := []u8{len: 1024}
	for i in 0 .. 1024 {
		test_data[i] = u8(i % 256)
	}
	os.write_file_array(test_path, test_data) or { return }

	// Test fallback read
	mut fallback := new_io_uring_fallback()
	mut file := os.open(test_path) or { return }
	defer { file.close() }

	mut buf := []u8{len: 100}
	bytes_read := fallback.sync_read(mut file, mut buf, 0) or { return }

	assert bytes_read == 100
	assert buf[0] == 0
	assert buf[99] == 99
	assert fallback.operations == 1
}

fn test_io_uring_fallback_write() {
	temp_dir := '/tmp/io_uring_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	test_path := '${temp_dir}/write_test.dat'

	mut fallback := new_io_uring_fallback()
	mut file := os.create(test_path) or { return }

	buf := [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10]
	bytes_written := fallback.sync_write(mut file, buf, -1) or {
		file.close()
		return
	}
	file.close()

	assert bytes_written == 10
	assert fallback.operations == 1

	// Verify content
	content := os.read_bytes(test_path) or { return }
	assert content.len == 10
	assert content[0] == 1
	assert content[9] == 10
}

fn test_io_uring_fallback_multiple_ops() {
	temp_dir := '/tmp/io_uring_test_${time.now().unix()}'
	os.mkdir_all(temp_dir) or { return }
	defer {
		os.rmdir_all(temp_dir) or {}
	}

	test_path := '${temp_dir}/multi_test.dat'

	mut fallback := new_io_uring_fallback()

	// Multiple writes
	mut file := os.create(test_path) or { return }
	for i in 0 .. 10 {
		buf := [u8(i), u8(i + 1), u8(i + 2)]
		fallback.sync_write(mut file, buf, -1) or { continue }
	}
	file.close()

	assert fallback.operations == 10
}

fn test_batch_op_types() {
	// Test BatchOpType enum
	read_op := BatchOp{
		op_type:   .read
		fd:        1
		buf:       []u8{len: 100}
		offset:    0
		user_data: 1
	}

	write_op := BatchOp{
		op_type:   .write
		fd:        2
		buf:       [u8(1), 2, 3]
		offset:    100
		user_data: 2
	}

	fsync_op := BatchOp{
		op_type:   .fsync
		fd:        3
		user_data: 3
	}

	nop_op := BatchOp{
		op_type:   .nop
		user_data: 4
	}

	assert read_op.op_type == .read
	assert write_op.op_type == .write
	assert fsync_op.op_type == .fsync
	assert nop_op.op_type == .nop
}

fn test_io_uring_params_structure() {
	params := IoUringParams{
		sq_entries: 256
		cq_entries: 512
		flags:      0
	}

	assert params.sq_entries == 256
	assert params.cq_entries == 512
}

fn test_sqe_structure() {
	sqe := IoUringSqe{
		opcode:    1
		fd:        5
		off:       1024
		addr:      0x1000
		len:       4096
		user_data: 99
	}

	assert sqe.opcode == 1
	assert sqe.fd == 5
	assert sqe.off == 1024
	assert sqe.len == 4096
	assert sqe.user_data == 99
}

fn test_cqe_structure() {
	cqe := IoUringCqe{
		user_data: 12345
		res:       1024
		flags:     0
	}

	assert cqe.user_data == 12345
	assert cqe.res == 1024
}

$if linux {
	fn test_io_uring_availability_check() {
		// Check runtime availability
		available := is_io_uring_available()
		// On modern Linux systems, should be available
		// But don't assert - may run in container without io_uring
		_ = available
	}

	fn test_io_uring_basic_creation() {
		ring := new_io_uring(IoUringConfig{ queue_depth: 8 }) or {
			// May fail if kernel doesn't support io_uring
			return
		}

		mut r := ring
		defer { r.close() }

		stats := r.get_stats()
		assert stats.submissions == 0
		assert stats.completions == 0
		assert stats.errors == 0
	}
}
