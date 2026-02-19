/// NUMA-aware memory allocation
/// Provides NUMA (Non-Uniform Memory Access) aware memory management for optimal performance on multi-socket systems
///
/// Features:
/// - NUMA topology detection
/// - Local node allocation
/// - Memory binding to specific nodes
/// - Interleaved allocation across nodes
///
/// Note: Full NUMA support is Linux-only; other platforms use a fallback
module engines

import os

// C interop - NUMA library (Linux)

$if linux {
	#flag -lnuma
	#include <numa.h>
	#include <numaif.h>
	#include <sched.h>

	fn C.numa_available() int
	fn C.numa_max_node() int
	fn C.numa_num_configured_nodes() int
	fn C.numa_num_configured_cpus() int
	fn C.numa_node_of_cpu(cpu int) int
	fn C.numa_alloc_onnode(size usize, node int) voidptr
	fn C.numa_alloc_local(size usize) voidptr
	fn C.numa_alloc_interleaved(size usize) voidptr
	fn C.numa_free(mem voidptr, size usize)
	fn C.numa_set_preferred(node int)
	fn C.numa_set_localalloc()
	fn C.numa_run_on_node(node int) int
	fn C.numa_node_size64(node int, freep &i64) i64
	fn C.sched_getcpu() int

	// Memory policy constants
	const mpol_default = 0
	const mpol_preferred = 1
	const mpol_bind = 2
	const mpol_interleave = 3
	const mpol_local = 4
}

// NUMA topology structs

/// NumaNode represents a NUMA node.
pub struct NumaNode {
pub:
	id        int
	total_mem i64
	free_mem  i64
	cpu_count int
	cpus      []int
}

/// NumaTopology represents the NUMA topology of the system.
pub struct NumaTopology {
pub:
	available  bool
	node_count int
	cpu_count  int
	nodes      []NumaNode
	local_node int
}

/// NumaCapabilities represents the level of NUMA support.
pub struct NumaCapabilities {
pub:
	has_numa         bool
	has_node_binding bool
	has_cpu_binding  bool
	has_interleave   bool
	has_local_alloc  bool
	platform_name    string
}

// NUMA topology detection

/// get_numa_capabilities returns the NUMA capabilities of the current platform.
pub fn get_numa_capabilities() NumaCapabilities {
	$if linux {
		available := C.numa_available() >= 0
		return NumaCapabilities{
			has_numa:         available
			has_node_binding: available
			has_cpu_binding:  available
			has_interleave:   available
			has_local_alloc:  available
			platform_name:    'Linux'
		}
	} $else $if macos {
		return NumaCapabilities{
			platform_name: 'macOS'
		}
	} $else $if windows {
		return NumaCapabilities{
			platform_name: 'Windows'
		}
	} $else {
		return NumaCapabilities{
			has_numa:      false
			platform_name: 'Unknown'
		}
	}
}

/// get_numa_topology returns the NUMA topology of the system.
pub fn get_numa_topology() NumaTopology {
	$if linux {
		if C.numa_available() < 0 {
			return NumaTopology{
				available:  false
				node_count: 1
				cpu_count:  1
				local_node: 0
			}
		}

		node_count := C.numa_num_configured_nodes()
		cpu_count := C.numa_num_configured_cpus()
		current_cpu := C.sched_getcpu()
		local_node := if current_cpu >= 0 { C.numa_node_of_cpu(current_cpu) } else { 0 }

		mut nodes := []NumaNode{cap: node_count}

		for node := 0; node < node_count; node++ {
			mut free_mem := i64(0)
			total_mem := C.numa_node_size64(node, &free_mem)

			// Get CPUs for this node
			mut node_cpus := []int{}
			for cpu := 0; cpu < cpu_count; cpu++ {
				if C.numa_node_of_cpu(cpu) == node {
					node_cpus << cpu
				}
			}

			nodes << NumaNode{
				id:        node
				total_mem: total_mem
				free_mem:  free_mem
				cpu_count: node_cpus.len
				cpus:      node_cpus
			}
		}

		return NumaTopology{
			available:  true
			node_count: node_count
			cpu_count:  cpu_count
			nodes:      nodes
			local_node: local_node
		}
	} $else {
		// Fallback: single-node topology
		return NumaTopology{
			available:  false
			node_count: 1
			cpu_count:  1
			nodes:      [
				NumaNode{
					id:        0
					total_mem: get_total_memory()
					free_mem:  get_free_memory()
					cpu_count: 1
					cpus:      [0]
				},
			]
			local_node: 0
		}
	}
}

/// get_current_node returns the NUMA node of the current thread.
pub fn get_current_node() int {
	$if linux {
		if C.numa_available() < 0 {
			return 0
		}
		cpu := C.sched_getcpu()
		if cpu < 0 {
			return 0
		}
		return C.numa_node_of_cpu(cpu)
	} $else {
		return 0
	}
}

// Memory helpers (platform-independent)

/// get_total_memory returns total system memory.
fn get_total_memory() i64 {
	$if linux {
		content := os.read_file('/proc/meminfo') or { return 0 }
		for line in content.split('\n') {
			if line.starts_with('MemTotal:') {
				parts := line.split_any(' \t').filter(it.len > 0)
				if parts.len >= 2 {
					kb := parts[1].i64()
					return kb * 1024
				}
			}
		}
		return 0
	} $else $if macos {
		// macOS: use sysctl
		return 0
	} $else {
		return 0
	}
}

/// get_free_memory returns available free memory.
fn get_free_memory() i64 {
	$if linux {
		content := os.read_file('/proc/meminfo') or { return 0 }
		for line in content.split('\n') {
			if line.starts_with('MemAvailable:') || line.starts_with('MemFree:') {
				parts := line.split_any(' \t').filter(it.len > 0)
				if parts.len >= 2 {
					kb := parts[1].i64()
					return kb * 1024
				}
			}
		}
		return 0
	} $else {
		return 0
	}
}

// NUMA memory allocation

/// NumaMemory represents NUMA-allocated memory.
pub struct NumaMemory {
pub:
	ptr     voidptr
	size    usize
	node    int
	policy  NumaPolicy
	is_numa bool
}

/// NumaPolicy is the NUMA allocation policy.
pub enum NumaPolicy {
	default_policy
	local
	preferred
	bind
	interleaved
}

/// numa_alloc allocates memory on a specific NUMA node.
pub fn numa_alloc(size usize, node int) NumaMemory {
	$if linux {
		if C.numa_available() < 0 {
			return numa_alloc_fallback(size)
		}

		ptr := C.numa_alloc_onnode(size, node)
		if ptr == unsafe { nil } {
			return numa_alloc_fallback(size)
		}

		return NumaMemory{
			ptr:     ptr
			size:    size
			node:    node
			policy:  .bind
			is_numa: true
		}
	} $else {
		return numa_alloc_fallback(size)
	}
}

/// numa_alloc_local allocates memory on the local NUMA node.
pub fn numa_alloc_local(size usize) NumaMemory {
	$if linux {
		if C.numa_available() < 0 {
			return numa_alloc_fallback(size)
		}

		ptr := C.numa_alloc_local(size)
		if ptr == unsafe { nil } {
			return numa_alloc_fallback(size)
		}

		return NumaMemory{
			ptr:     ptr
			size:    size
			node:    get_current_node()
			policy:  .local
			is_numa: true
		}
	} $else {
		return numa_alloc_fallback(size)
	}
}

/// numa_alloc_interleaved allocates memory interleaved across all nodes.
pub fn numa_alloc_interleaved(size usize) NumaMemory {
	$if linux {
		if C.numa_available() < 0 {
			return numa_alloc_fallback(size)
		}

		ptr := C.numa_alloc_interleaved(size)
		if ptr == unsafe { nil } {
			return numa_alloc_fallback(size)
		}

		return NumaMemory{
			ptr:     ptr
			size:    size
			node:    -1
			policy:  .interleaved
			is_numa: true
		}
	} $else {
		return numa_alloc_fallback(size)
	}
}

/// numa_alloc_fallback provides standard allocation when NUMA is unavailable.
fn numa_alloc_fallback(size usize) NumaMemory {
	ptr := unsafe { C.malloc(size) }
	return NumaMemory{
		ptr:     ptr
		size:    size
		node:    0
		policy:  .default_policy
		is_numa: false
	}
}

/// numa_free frees NUMA-allocated memory.
pub fn numa_free(mem NumaMemory) {
	if mem.ptr == unsafe { nil } {
		return
	}

	$if linux {
		if mem.is_numa && C.numa_available() >= 0 {
			C.numa_free(mem.ptr, mem.size)
			return
		}
	}

	unsafe { C.free(mem.ptr) }
}

// NUMA-aware buffer pool

/// NumaBufferPool provides NUMA-aware buffer allocation.
pub struct NumaBufferPool {
pub mut:
	node_pools []NodePool
	topology   NumaTopology
	stats      NumaPoolStats
}

/// NodePool is a per-node buffer pool.
struct NodePool {
mut:
	node      int
	buffers   []NumaMemory
	available []int
	buf_size  usize
}

/// NumaPoolStats holds NUMA pool statistics.
pub struct NumaPoolStats {
pub mut:
	allocations_total  u64
	allocations_local  u64
	allocations_remote u64
	bytes_allocated    u64
	cache_hits         u64
	cache_misses       u64
}

/// NumaBufferConfig is the NUMA buffer pool configuration.
pub struct NumaBufferConfig {
pub:
	buffer_size      usize = 4096
	buffers_per_node int   = 100
	prefer_local     bool  = true
}

/// new_numa_buffer_pool creates a NUMA-aware buffer pool.
pub fn new_numa_buffer_pool(config NumaBufferConfig) NumaBufferPool {
	topology := get_numa_topology()

	mut node_pools := []NodePool{cap: topology.node_count}

	for node in topology.nodes {
		mut pool := NodePool{
			node:      node.id
			buffers:   []NumaMemory{cap: config.buffers_per_node}
			available: []int{cap: config.buffers_per_node}
			buf_size:  config.buffer_size
		}

		// Pre-allocate buffers
		for i in 0 .. config.buffers_per_node {
			mem := numa_alloc(config.buffer_size, node.id)
			pool.buffers << mem
			pool.available << i
		}

		node_pools << pool
	}

	return NumaBufferPool{
		node_pools: node_pools
		topology:   topology
	}
}

/// get_buffer retrieves a buffer, preferring the local node.
pub fn (mut p NumaBufferPool) get_buffer() ?NumaMemory {
	local_node := get_current_node()
	p.stats.allocations_total++

	// Try local node first
	if local_node < p.node_pools.len {
		if buf := p.get_from_node(local_node) {
			p.stats.allocations_local++
			p.stats.cache_hits++
			return buf
		}
	}

	// Try other nodes
	for i, _ in p.node_pools {
		if i == local_node {
			continue
		}
		if buf := p.get_from_node(i) {
			p.stats.allocations_remote++
			p.stats.cache_hits++
			return buf
		}
	}

	// Allocate new on local node
	p.stats.cache_misses++
	p.stats.bytes_allocated += u64(p.node_pools[0].buf_size)
	return numa_alloc_local(p.node_pools[0].buf_size)
}

/// get_from_node retrieves a buffer from the specified node.
fn (mut p NumaBufferPool) get_from_node(node int) ?NumaMemory {
	if node >= p.node_pools.len {
		return none
	}

	mut pool := &p.node_pools[node]
	if pool.available.len == 0 {
		return none
	}

	idx := pool.available.pop()
	return pool.buffers[idx]
}

/// put_buffer returns a buffer to the pool.
pub fn (mut p NumaBufferPool) put_buffer(mem NumaMemory) {
	// Find the node pool
	node := if mem.node >= 0 && mem.node < p.node_pools.len {
		mem.node
	} else {
		0
	}

	// Check if this is our buffer
	mut pool := &p.node_pools[node]
	for i, buf in pool.buffers {
		if buf.ptr == mem.ptr {
			// Clear the buffer
			unsafe {
				C.memset(mem.ptr, 0, mem.size)
			}
			pool.available << i
			return
		}
	}

	// Not our buffer — free it
	numa_free(mem)
}

/// get_stats returns pool statistics.
pub fn (p &NumaBufferPool) get_stats() NumaPoolStats {
	return p.stats
}

/// close releases all pool resources.
pub fn (mut p NumaBufferPool) close() {
	for mut pool in p.node_pools {
		for mem in pool.buffers {
			numa_free(mem)
		}
		pool.buffers.clear()
		pool.available.clear()
	}
}

// NUMA thread binding

/// bind_to_node binds the current thread to a NUMA node.
pub fn bind_to_node(node int) bool {
	$if linux {
		if C.numa_available() < 0 {
			return false
		}
		return C.numa_run_on_node(node) == 0
	} $else {
		return false
	}
}

/// set_preferred_node sets the preferred NUMA node for allocations.
pub fn set_preferred_node(node int) {
	$if linux {
		if C.numa_available() >= 0 {
			C.numa_set_preferred(node)
		}
	}
}

/// set_local_alloc sets the thread to allocate from the local node.
pub fn set_local_alloc() {
	$if linux {
		if C.numa_available() >= 0 {
			C.numa_set_localalloc()
		}
	}
}

// NUMA-aware data structures

/// NumaArray is a NUMA-aware array that keeps data on a specific node.
pub struct NumaArray {
pub:
	memory NumaMemory
	len    int
	cap    int
}

/// new_numa_array creates a NUMA-aware array on the local node.
pub fn new_numa_array(cap int, elem_size int) NumaArray {
	size := usize(cap * elem_size)
	mem := numa_alloc_local(size)

	return NumaArray{
		memory: mem
		len:    0
		cap:    cap
	}
}

/// new_numa_array_on_node creates an array on the specified node.
pub fn new_numa_array_on_node(cap int, elem_size int, node int) NumaArray {
	size := usize(cap * elem_size)
	mem := numa_alloc(size, node)

	return NumaArray{
		memory: mem
		len:    0
		cap:    cap
	}
}

/// close frees the NUMA array.
pub fn (a &NumaArray) close() {
	numa_free(a.memory)
}

/// get_ptr returns a raw pointer for array operations.
pub fn (a &NumaArray) get_ptr() voidptr {
	return a.memory.ptr
}

/// get_node returns the NUMA node on which the array is allocated.
pub fn (a &NumaArray) get_node() int {
	return a.memory.node
}
