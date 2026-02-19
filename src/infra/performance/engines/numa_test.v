module engines

// NUMA module tests
// Tests for NUMA-aware memory allocation and topology detection
import time

// Topology Tests

fn test_get_numa_capabilities() {
	caps := get_numa_capabilities()

	// Should always return valid capabilities
	assert caps.platform_name.len > 0

	$if linux {
		// Linux should report platform correctly
		assert caps.platform_name == 'Linux'
	} $else $if macos {
		assert caps.platform_name == 'macOS'
		assert caps.has_numa == false
	} $else $if windows {
		assert caps.platform_name == 'Windows'
	}
}

fn test_get_numa_topology() {
	topology := get_numa_topology()

	// Should always have at least 1 node
	assert topology.node_count >= 1
	assert topology.cpu_count >= 1
	assert topology.nodes.len >= 1

	// First node should exist
	node0 := topology.nodes[0]
	assert node0.id == 0

	// Local node should be valid
	assert topology.local_node >= 0
	assert topology.local_node < topology.node_count
}

fn test_get_current_node() {
	node := get_current_node()

	// Should return valid node ID
	assert node >= 0

	topology := get_numa_topology()
	assert node < topology.node_count
}

// Memory Allocation Tests

fn test_numa_alloc_local() {
	size := usize(4096)
	mem := numa_alloc_local(size)

	// Should allocate memory
	assert mem.ptr != unsafe { nil }
	assert mem.size == size
	assert mem.policy == .local || mem.policy == .default_policy

	// Memory should be writable
	unsafe {
		test_ptr := &u8(mem.ptr)
		test_ptr[0] = 42
		assert test_ptr[0] == 42
	}
	numa_free(mem)
}

fn test_numa_alloc_on_node() {
	size := usize(8192)
	node := 0

	mem := numa_alloc(size, node)

	assert mem.ptr != unsafe { nil }
	assert mem.size == size

	// Write pattern to verify memory works
	unsafe {
		byte_ptr := &u8(mem.ptr)
		for i in 0 .. 100 {
			byte_ptr[i] = u8(i)
		}
		for i in 0 .. 100 {
			assert byte_ptr[i] == u8(i)
		}
	}
	numa_free(mem)
}

fn test_numa_alloc_interleaved() {
	size := usize(16384)
	mem := numa_alloc_interleaved(size)

	assert mem.ptr != unsafe { nil }
	assert mem.size == size
	assert mem.policy == .interleaved || mem.policy == .default_policy

	// Verify memory is usable
	unsafe {
		C.memset(mem.ptr, 0xAB, size)
		byte_ptr := &u8(mem.ptr)
		assert byte_ptr[0] == 0xAB
		assert byte_ptr[size - 1] == 0xAB
	}
	numa_free(mem)
}

fn test_numa_alloc_fallback() {
	// This tests the fallback path (non-NUMA allocation)
	size := usize(1024)
	mem := numa_alloc_fallback(size)

	assert mem.ptr != unsafe { nil }
	assert mem.size == size
	assert mem.policy == .default_policy
	assert mem.is_numa == false

	numa_free(mem)
}

fn test_numa_free_null() {
	// Should handle null pointer gracefully
	mem := NumaMemory{
		ptr:     unsafe { nil }
		size:    0
		node:    0
		policy:  .default_policy
		is_numa: false
	}

	numa_free(mem)
}

// Buffer Pool Tests

fn test_numa_buffer_pool_creation() {
	config := NumaBufferConfig{
		buffer_size:      4096
		buffers_per_node: 10
		prefer_local:     true
	}

	mut pool := new_numa_buffer_pool(config)
	defer {
		pool.close()
	}

	// Should have pools for each node
	topology := get_numa_topology()
	assert pool.node_pools.len == topology.node_count

	// Each pool should have pre-allocated buffers
	for np in pool.node_pools {
		assert np.buffers.len == config.buffers_per_node
		assert np.available.len == config.buffers_per_node
	}
}

fn test_numa_buffer_pool_get_put() {
	config := NumaBufferConfig{
		buffer_size:      1024
		buffers_per_node: 5
	}

	mut pool := new_numa_buffer_pool(config)
	defer {
		pool.close()
	}

	// Get a buffer
	buf := pool.get_buffer() or {
		assert false, 'Failed to get buffer'
		return
	}

	assert buf.ptr != unsafe { nil }
	assert buf.size == config.buffer_size

	// Use the buffer
	unsafe {
		C.memset(buf.ptr, 0x55, buf.size)
	}

	// Return the buffer
	pool.put_buffer(buf)

	// Stats should be updated
	stats := pool.get_stats()
	assert stats.allocations_total >= 1
}

fn test_numa_buffer_pool_multiple_gets() {
	config := NumaBufferConfig{
		buffer_size:      512
		buffers_per_node: 3
	}

	mut pool := new_numa_buffer_pool(config)
	defer {
		pool.close()
	}

	// Get multiple buffers
	mut buffers := []NumaMemory{}
	for _ in 0 .. 5 {
		if buf := pool.get_buffer() {
			buffers << buf
		}
	}

	assert buffers.len >= 3

	// Return all buffers
	for buf in buffers {
		pool.put_buffer(buf)
	}

	stats := pool.get_stats()
	assert stats.allocations_total >= 5
}

fn test_numa_buffer_pool_stats() {
	config := NumaBufferConfig{
		buffer_size:      2048
		buffers_per_node: 2
	}

	mut pool := new_numa_buffer_pool(config)
	defer {
		pool.close()
	}

	initial_stats := pool.get_stats()
	assert initial_stats.allocations_total == 0

	// Get and put buffer
	if buf := pool.get_buffer() {
		pool.put_buffer(buf)
	}

	final_stats := pool.get_stats()
	assert final_stats.allocations_total == 1
	assert final_stats.cache_hits >= 0
}

// Thread Binding Tests

fn test_bind_to_node() {
	// Test binding to node 0 (always exists)
	result := bind_to_node(0)

	$if linux {
		// On Linux with NUMA, should succeed
		// On Linux without NUMA, returns false
		_ = result
	} $else {
		// On other platforms, should return false
		assert result == false
	}
}

fn test_set_preferred_node() {
	// Should not crash
	set_preferred_node(0)

	// Verify subsequent allocations work
	mem := numa_alloc_local(1024)
	assert mem.ptr != unsafe { nil }
	numa_free(mem)
}

fn test_set_local_alloc() {
	// Should not crash
	set_local_alloc()

	// Verify allocations still work
	mem := numa_alloc_local(2048)
	assert mem.ptr != unsafe { nil }
	numa_free(mem)
}

// NUMA Array Tests

fn test_numa_array_creation() {
	arr := new_numa_array(100, 8)
	defer {
		arr.close()
	}

	assert arr.cap == 100
	assert arr.len == 0
	assert arr.memory.ptr != unsafe { nil }
}

fn test_numa_array_on_node() {
	arr := new_numa_array_on_node(50, 4, 0)
	defer {
		arr.close()
	}

	assert arr.cap == 50
	assert arr.get_ptr() != unsafe { nil }
	assert arr.get_node() >= 0
}

fn test_numa_array_usage() {
	arr := new_numa_array(10, int(sizeof(int)))
	defer {
		arr.close()
	}

	// Use array as int array
	unsafe {
		int_ptr := &int(arr.get_ptr())
		for i in 0 .. 10 {
			int_ptr[i] = i * 10
		}
		for i in 0 .. 10 {
			assert int_ptr[i] == i * 10
		}
	}
}

// Integration Tests

fn test_numa_full_workflow() {
	// 1. Get topology
	topology := get_numa_topology()
	assert topology.node_count >= 1

	// 2. Create buffer pool
	mut pool := new_numa_buffer_pool(NumaBufferConfig{
		buffer_size:      4096
		buffers_per_node: 5
	})
	defer {
		pool.close()
	}

	// 3. Set local allocation preference
	set_local_alloc()

	// 4. Get buffers and perform operations
	mut allocated := []NumaMemory{}
	for _ in 0 .. 10 {
		if buf := pool.get_buffer() {
			// Simulate data processing
			unsafe {
				C.memset(buf.ptr, 0, buf.size)
			}
			allocated << buf
		}
	}

	// 5. Return buffers
	for buf in allocated {
		pool.put_buffer(buf)
	}

	// 6. Verify stats
	stats := pool.get_stats()
	assert stats.allocations_total == 10
}

fn test_numa_memory_isolation() {
	// Test that allocations on different nodes work correctly
	topology := get_numa_topology()

	if topology.node_count < 1 {
		return
	}

	// Allocate on node 0
	mem0 := numa_alloc(4096, 0)
	defer {
		numa_free(mem0)
	}

	assert mem0.ptr != unsafe { nil }

	// Write pattern
	unsafe {
		byte_ptr := &u8(mem0.ptr)
		for i in 0 .. 1000 {
			byte_ptr[i] = u8(i % 256)
		}
	}
	// Verify pattern
	unsafe {
		byte_ptr := &u8(mem0.ptr)
		for i in 0 .. 1000 {
			assert byte_ptr[i] == u8(i % 256)
		}
	}
}

// Performance Tests (Light)

fn test_numa_allocation_performance() {
	iterations := 100
	size := usize(4096)

	// Time NUMA local allocations
	start := time.now()
	for _ in 0 .. iterations {
		mem := numa_alloc_local(size)
		numa_free(mem)
	}
	numa_duration := time.since(start)

	// Time regular allocations
	start2 := time.now()
	for _ in 0 .. iterations {
		ptr := unsafe { C.malloc(size) }
		unsafe { C.free(ptr) }
	}
	regular_duration := time.since(start2)

	// Both should complete in reasonable time
	assert numa_duration.milliseconds() < 5000
	assert regular_duration.milliseconds() < 5000
}

fn test_numa_pool_vs_direct_allocation() {
	iterations := 50
	mut pool := new_numa_buffer_pool(NumaBufferConfig{
		buffer_size:      4096
		buffers_per_node: 100
	})
	defer {
		pool.close()
	}

	// Warm up pool
	for _ in 0 .. 10 {
		if buf := pool.get_buffer() {
			pool.put_buffer(buf)
		}
	}

	// Time pool allocations
	start := time.now()
	for _ in 0 .. iterations {
		if buf := pool.get_buffer() {
			pool.put_buffer(buf)
		}
	}
	pool_duration := time.since(start)

	// Time direct allocations
	start2 := time.now()
	for _ in 0 .. iterations {
		mem := numa_alloc_local(4096)
		numa_free(mem)
	}
	direct_duration := time.since(start2)

	// Pool should generally be faster due to reuse
	_ = pool_duration
	_ = direct_duration
	// Note: Not asserting relative performance as it depends on system
}
