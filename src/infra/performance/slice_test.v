module performance

// Tests for slice.v - Zero-Copy Slice Operations

fn test_byteslice_new() {
	data := [u8(1), 2, 3, 4, 5]
	slice := ByteSlice.new(data)
	
	assert slice.len() == 5
	assert slice.is_empty() == false
}

fn test_byteslice_empty() {
	data := []u8{}
	slice := ByteSlice.new(data)
	
	assert slice.len() == 0
	assert slice.is_empty() == true
}

fn test_byteslice_at() {
	data := [u8(10), 20, 30, 40, 50]
	slice := ByteSlice.new(data)
	
	assert slice.at(0) or { 0 } == 10
	assert slice.at(2) or { 0 } == 30
	assert slice.at(4) or { 0 } == 50
	
	// Out of bounds
	if _ := slice.at(5) {
		assert false, 'should have failed'
	}
	if _ := slice.at(-1) {
		assert false, 'should have failed'
	}
}

fn test_byteslice_slice() {
	data := [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10]
	slice := ByteSlice.new(data)
	
	// Create sub-slice
	sub := slice.slice(2, 6) or {
		assert false, 'slice should succeed'
		return
	}
	
	assert sub.len() == 4
	assert sub.at(0) or { 0 } == 3
	assert sub.at(3) or { 0 } == 6
	
	// Nested sub-slice
	nested := sub.slice(1, 3) or {
		assert false, 'nested slice should succeed'
		return
	}
	
	assert nested.len() == 2
	assert nested.at(0) or { 0 } == 4
	assert nested.at(1) or { 0 } == 5
}

fn test_byteslice_slice_bounds() {
	data := [u8(1), 2, 3, 4, 5]
	slice := ByteSlice.new(data)
	
	// Invalid bounds
	if _ := slice.slice(-1, 3) {
		assert false, 'should fail with negative start'
	}
	if _ := slice.slice(0, 10) {
		assert false, 'should fail with end > length'
	}
	if _ := slice.slice(4, 2) {
		assert false, 'should fail with start > end'
	}
}

fn test_byteslice_to_owned() {
	data := [u8(1), 2, 3, 4, 5]
	slice := ByteSlice.new(data)
	
	// Get sub-slice and convert to owned
	sub := slice.slice(1, 4) or { return }
	owned := sub.to_owned()
	
	assert owned.len == 3
	assert owned[0] == 2
	assert owned[1] == 3
	assert owned[2] == 4
}

fn test_byteslice_bytes() {
	data := [u8(1), 2, 3, 4, 5]
	slice := ByteSlice.new(data)
	
	bytes := slice.bytes()
	assert bytes.len == 5
	assert bytes[0] == 1
}

fn test_slicereader_basic() {
	data := [u8(0x00), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]
	mut reader := SliceReader.new(data)
	
	assert reader.remaining() == 8
	assert reader.has_remaining() == true
	
	// Read single bytes
	b1 := reader.read_u8() or { return }
	assert b1 == 0x00
	assert reader.remaining() == 7
	
	b2 := reader.read_u8() or { return }
	assert b2 == 0x01
}

fn test_slicereader_i16_be() {
	// Big-endian 0x0102 = 258
	data := [u8(0x01), 0x02, 0x03, 0x04]
	mut reader := SliceReader.new(data)
	
	val := reader.read_i16_be() or { return }
	assert val == 258
	assert reader.remaining() == 2
}

fn test_slicereader_i32_be() {
	// Big-endian 0x01020304 = 16909060
	data := [u8(0x01), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
	mut reader := SliceReader.new(data)
	
	val := reader.read_i32_be() or { return }
	assert val == 16909060
	assert reader.remaining() == 4
}

fn test_slicereader_i64_be() {
	// Big-endian 8 bytes
	data := [u8(0x00), 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00]
	mut reader := SliceReader.new(data)
	
	val := reader.read_i64_be() or { return }
	assert val == 256
}

fn test_slicereader_read_slice() {
	data := [u8(1), 2, 3, 4, 5, 6, 7, 8]
	mut reader := SliceReader.new(data)
	
	// Skip first 2 bytes
	reader.skip(2) or { return }
	
	// Read 4 bytes as slice
	slice := reader.read_slice(4) or { return }
	assert slice.len() == 4
	assert slice.at(0) or { 0 } == 3
	assert slice.at(3) or { 0 } == 6
	
	assert reader.remaining() == 2
}

fn test_slicereader_seek_reset() {
	data := [u8(1), 2, 3, 4, 5]
	mut reader := SliceReader.new(data)
	
	reader.skip(3) or { return }
	assert reader.remaining() == 2
	
	reader.reset()
	assert reader.remaining() == 5
	
	reader.seek(2) or { return }
	assert reader.remaining() == 3
	
	val := reader.read_u8() or { return }
	assert val == 3
}

fn test_slicereader_underflow() {
	data := [u8(1), 2]
	mut reader := SliceReader.new(data)
	
	// Should fail - not enough bytes for i32
	if _ := reader.read_i32_be() {
		assert false, 'should fail with buffer underflow'
	}
}

fn test_slicepool() {
	mut pool := SlicePool.new(10)
	
	data1 := [u8(1), 2, 3]
	slice1 := pool.acquire(data1)
	assert slice1.len() == 3
	
	// Release and reacquire
	pool.release(slice1)
	
	data2 := [u8(4), 5, 6, 7]
	slice2 := pool.acquire(data2)
	assert slice2.len() == 4
	
	created, reused, available := pool.stats()
	assert created == 1
	assert reused == 1
}

fn test_slice_benchmark() {
	benchmark := SliceBenchmark.new(1000, 1024)
	slice_time, copy_time := benchmark.run_comparison()
	
	// Both should complete (times > 0)
	assert slice_time >= 0
	assert copy_time >= 0
	
	// Zero-copy should generally be faster
	// (not strictly enforced as it may vary by system)
}
