/// NUMA 인식 메모리 할당
/// 멀티 소켓 시스템에서 최적의 성능을 위한 NUMA (Non-Uniform Memory Access) 인식 메모리 관리 제공
///
/// 기능:
/// - NUMA 토폴로지 감지
/// - 로컬 노드 할당
/// - 특정 노드에 메모리 바인딩
/// - 노드 간 인터리브 할당
///
/// 참고: 전체 NUMA 지원은 Linux에서만, 다른 플랫폼은 폴백 사용
module engines

import os

// C 인터롭 - NUMA 라이브러리 (Linux)

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

	// 메모리 정책 상수
	const mpol_default = 0
	const mpol_preferred = 1
	const mpol_bind = 2
	const mpol_interleave = 3
	const mpol_local = 4
}

// NUMA 토폴로지 구조체

/// NumaNode는 NUMA 노드를 나타냅니다.
pub struct NumaNode {
pub:
	id        int
	total_mem i64   // 총 메모리 (바이트)
	free_mem  i64   // 여유 메모리 (바이트)
	cpu_count int   // CPU 수
	cpus      []int // CPU 목록
}

/// NumaTopology는 시스템의 NUMA 토폴로지를 나타냅니다.
pub struct NumaTopology {
pub:
	available  bool // NUMA 사용 가능 여부
	node_count int  // 노드 수
	cpu_count  int  // CPU 수
	nodes      []NumaNode
	local_node int // 현재 스레드가 실행 중인 노드
}

/// NumaCapabilities는 NUMA 지원 수준을 나타냅니다.
pub struct NumaCapabilities {
pub:
	has_numa         bool   // NUMA 지원
	has_node_binding bool   // 노드 바인딩 지원
	has_cpu_binding  bool   // CPU 바인딩 지원
	has_interleave   bool   // 인터리브 지원
	has_local_alloc  bool   // 로컬 할당 지원
	platform_name    string // 플랫폼 이름
}

// NUMA 토폴로지 감지

/// get_numa_capabilities는 현재 플랫폼의 NUMA 기능을 반환합니다.
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

/// get_numa_topology는 시스템의 NUMA 토폴로지를 반환합니다.
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

			// 이 노드의 CPU 가져오기
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
		// 폴백: 단일 노드 토폴로지
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

/// get_current_node는 현재 스레드의 NUMA 노드를 반환합니다.
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

// 메모리 헬퍼 (플랫폼 독립적)

/// get_total_memory는 총 메모리를 반환합니다.
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
		// macOS: sysctl 사용
		return 0 // 단순화됨
	} $else {
		return 0
	}
}

/// get_free_memory는 여유 메모리를 반환합니다.
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

// NUMA 메모리 할당

/// NumaMemory는 NUMA 할당된 메모리를 나타냅니다.
pub struct NumaMemory {
pub:
	ptr     voidptr // 메모리 포인터
	size    usize
	node    int        // NUMA 노드
	policy  NumaPolicy // 할당 정책
	is_numa bool       // 실제 NUMA 할당 여부
}

/// NumaPolicy는 NUMA 할당 정책입니다.
pub enum NumaPolicy {
	default_policy // 시스템 기본값
	local          // 로컬 노드에 할당
	preferred      // 특정 노드 선호, 폴백 허용
	bind           // 노드에 엄격히 바인딩
	interleaved    // 노드 간 라운드 로빈
}

/// numa_alloc은 특정 NUMA 노드에 메모리를 할당합니다.
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

/// numa_alloc_local은 로컬 NUMA 노드에 메모리를 할당합니다.
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

/// numa_alloc_interleaved는 모든 노드에 인터리브로 메모리를 할당합니다.
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
			node:    -1 // 모든 노드에 인터리브
			policy:  .interleaved
			is_numa: true
		}
	} $else {
		return numa_alloc_fallback(size)
	}
}

/// numa_alloc_fallback은 NUMA를 사용할 수 없을 때 표준 할당을 제공합니다.
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

/// numa_free는 NUMA 할당된 메모리를 해제합니다.
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

// NUMA 인식 버퍼 풀

/// NumaBufferPool은 NUMA 인식 버퍼 할당을 제공합니다.
pub struct NumaBufferPool {
pub mut:
	node_pools []NodePool    // 노드별 풀
	topology   NumaTopology  // NUMA 토폴로지
	stats      NumaPoolStats // 풀 통계
}

/// NodePool은 노드별 버퍼 풀입니다.
struct NodePool {
mut:
	node      int
	buffers   []NumaMemory // 버퍼 목록
	available []int        // 사용 가능한 버퍼 인덱스
	buf_size  usize        // 버퍼 크기
}

/// NumaPoolStats는 NUMA 풀 통계를 담고 있습니다.
pub struct NumaPoolStats {
pub mut:
	allocations_total  u64 // 총 할당 수
	allocations_local  u64 // 로컬 할당 수
	allocations_remote u64 // 원격 할당 수
	bytes_allocated    u64 // 할당된 바이트
	cache_hits         u64 // 캐시 히트
	cache_misses       u64 // 캐시 미스
}

/// NumaBufferConfig는 NUMA 버퍼 풀 설정입니다.
pub struct NumaBufferConfig {
pub:
	buffer_size      usize = 4096 // 버퍼 크기
	buffers_per_node int   = 100  // 노드당 버퍼 수
	prefer_local     bool  = true // 로컬 선호
}

/// new_numa_buffer_pool은 NUMA 인식 버퍼 풀을 생성합니다.
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

		// 버퍼 사전 할당
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

/// get_buffer는 로컬 노드를 선호하여 버퍼를 가져옵니다.
pub fn (mut p NumaBufferPool) get_buffer() ?NumaMemory {
	local_node := get_current_node()
	p.stats.allocations_total++

	// 먼저 로컬 노드 시도
	if local_node < p.node_pools.len {
		if buf := p.get_from_node(local_node) {
			p.stats.allocations_local++
			p.stats.cache_hits++
			return buf
		}
	}

	// 다른 노드 시도
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

	// 로컬 노드에 새로 할당
	p.stats.cache_misses++
	p.stats.bytes_allocated += u64(p.node_pools[0].buf_size)
	return numa_alloc_local(p.node_pools[0].buf_size)
}

/// get_from_node는 특정 노드에서 버퍼를 가져옵니다.
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

/// put_buffer는 버퍼를 풀에 반환합니다.
pub fn (mut p NumaBufferPool) put_buffer(mem NumaMemory) {
	// 노드 풀 찾기
	node := if mem.node >= 0 && mem.node < p.node_pools.len {
		mem.node
	} else {
		0
	}

	// 우리 버퍼인지 확인
	mut pool := &p.node_pools[node]
	for i, buf in pool.buffers {
		if buf.ptr == mem.ptr {
			// 버퍼 초기화
			unsafe {
				C.memset(mem.ptr, 0, mem.size)
			}
			pool.available << i
			return
		}
	}

	// 우리 버퍼가 아니면 해제
	numa_free(mem)
}

/// get_stats는 풀 통계를 반환합니다.
pub fn (p &NumaBufferPool) get_stats() NumaPoolStats {
	return p.stats
}

/// close는 모든 풀 리소스를 해제합니다.
pub fn (mut p NumaBufferPool) close() {
	for mut pool in p.node_pools {
		for mem in pool.buffers {
			numa_free(mem)
		}
		pool.buffers.clear()
		pool.available.clear()
	}
}

// NUMA 스레드 바인딩

/// bind_to_node는 현재 스레드를 NUMA 노드에 바인딩합니다.
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

/// set_preferred_node는 할당을 위한 선호 NUMA 노드를 설정합니다.
pub fn set_preferred_node(node int) {
	$if linux {
		if C.numa_available() >= 0 {
			C.numa_set_preferred(node)
		}
	}
}

/// set_local_alloc은 스레드가 로컬 노드에서 할당하도록 설정합니다.
pub fn set_local_alloc() {
	$if linux {
		if C.numa_available() >= 0 {
			C.numa_set_localalloc()
		}
	}
}

// NUMA 인식 데이터 구조

/// NumaArray는 특정 노드에 데이터를 유지하는 NUMA 인식 배열입니다.
pub struct NumaArray {
pub:
	memory NumaMemory // NUMA 메모리
	len    int
	cap    int
}

/// new_numa_array는 로컬 노드에 NUMA 인식 배열을 생성합니다.
pub fn new_numa_array(cap int, elem_size int) NumaArray {
	size := usize(cap * elem_size)
	mem := numa_alloc_local(size)

	return NumaArray{
		memory: mem
		len:    0
		cap:    cap
	}
}

/// new_numa_array_on_node는 특정 노드에 배열을 생성합니다.
pub fn new_numa_array_on_node(cap int, elem_size int, node int) NumaArray {
	size := usize(cap * elem_size)
	mem := numa_alloc(size, node)

	return NumaArray{
		memory: mem
		len:    0
		cap:    cap
	}
}

/// close는 NUMA 배열을 해제합니다.
pub fn (a &NumaArray) close() {
	numa_free(a.memory)
}

/// get_ptr은 배열 연산을 위한 원시 포인터를 반환합니다.
pub fn (a &NumaArray) get_ptr() voidptr {
	return a.memory.ptr
}

/// get_node는 배열이 할당된 NUMA 노드를 반환합니다.
pub fn (a &NumaArray) get_node() int {
	return a.memory.node
}
