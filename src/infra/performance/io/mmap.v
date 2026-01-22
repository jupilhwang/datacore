/// 고성능 스토리지를 위한 메모리 매핑 파일 I/O
/// OS 수준 메모리 매핑을 사용한 효율적인 파일 접근 제공
module io

import os
import time

/// MmapRegion은 파일의 메모리 매핑된 영역을 나타냅니다.
pub struct MmapRegion {
pub:
	file_path string // 매핑된 파일 경로
	offset    i64    // 파일 내 오프셋
	length    int    // 매핑된 영역의 길이
mut:
	data      []u8   // 메모리 매핑된 데이터 (시뮬레이션)
	is_mapped bool   // 영역이 현재 매핑되어 있는지 여부
	dirty     bool   // 영역이 수정되었는지 여부
}

/// MmapFile은 메모리 매핑된 파일 래퍼입니다.
pub struct MmapFile {
pub:
	path      string   // 파일 경로
	read_only bool     // 읽기 전용 여부
mut:
	file      ?os.File     // 파일 핸들
	size      i64          // 파일 크기
	regions   []MmapRegion // 매핑된 영역 목록
	page_size int          // 페이지 크기
}

/// open은 메모리 매핑을 위해 파일을 엽니다.
pub fn MmapFile.open(path string, read_only bool) !MmapFile {
	mode := if read_only { 'r' } else { 'r+' }
	file := os.open_file(path, mode) or { return error('failed to open file: ${err}') }

	// 파일 크기 가져오기
	file_size := os.file_size(path)

	return MmapFile{
		path:      path
		read_only: read_only
		file:      file
		size:      i64(file_size)
		regions:   []MmapRegion{}
		page_size: 4096 // 표준 페이지 크기
	}
}

/// create는 메모리 매핑을 위한 새 파일을 생성합니다.
pub fn MmapFile.create(path string, initial_size i64) !MmapFile {
	// 파일 생성
	mut file := os.create(path) or { return error('failed to create file: ${err}') }

	// 0으로 파일 사전 할당
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

	// 읽기-쓰기 모드로 다시 열기
	return MmapFile.open(path, false)
}

/// map_region은 파일의 영역을 메모리에 매핑합니다.
pub fn (mut m MmapFile) map_region(offset i64, length int) !&MmapRegion {
	if offset < 0 || offset >= m.size {
		return error('offset out of bounds: ${offset}, file size: ${m.size}')
	}

	actual_length := if offset + length > m.size {
		int(m.size - offset)
	} else {
		length
	}

	// 오프셋을 페이지 경계에 정렬
	page_offset := offset % m.page_size
	aligned_offset := offset - page_offset
	aligned_length := actual_length + int(page_offset)

	// 데이터 읽기 (mmap 시뮬레이션)
	// 실제 구현에서는 플랫폼별 mmap 시스템 호출을 사용합니다
	mut data := []u8{len: aligned_length}

	if file := m.file {
		mut f := file
		f.seek(aligned_offset, .start) or { return error('failed to seek: ${err}') }
		bytes_read := f.read(mut data) or { return error('failed to read: ${err}') }
		if bytes_read < aligned_length {
			// 예상보다 적게 읽은 경우 크기 조정
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

/// unmap_region은 영역의 매핑을 해제합니다.
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

/// sync_region은 더티 영역을 디스크에 동기화합니다.
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

/// sync_all은 모든 더티 영역을 동기화합니다.
pub fn (mut m MmapFile) sync_all() ! {
	for region in m.regions {
		if region.dirty {
			m.sync_region(&region)!
		}
	}
}

/// file_size는 파일 크기를 반환합니다.
pub fn (m MmapFile) file_size() i64 {
	return m.size
}

/// extend는 파일 크기를 확장합니다.
pub fn (mut m MmapFile) extend(new_size i64) ! {
	if m.read_only {
		return error('cannot extend read-only file')
	}

	if new_size <= m.size {
		return
	}

	if file := m.file {
		mut f := file
		// 끝으로 이동하고 0 쓰기
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

/// close는 메모리 매핑된 파일을 닫습니다.
pub fn (mut m MmapFile) close() ! {
	// 모든 더티 영역 동기화
	m.sync_all() or {}

	// 영역 정리
	m.regions.clear()

	// 파일 닫기
	if file := m.file {
		mut f := file
		f.close()
	}
	m.file = none
}

/// LogSegmentMmap은 Kafka 스타일 스토리지를 위한 메모리 매핑된 로그 세그먼트입니다.
pub struct LogSegmentMmap {
pub:
	base_offset i64    // 기본 오프셋
	path        string // 디렉토리 경로
mut:
	log_file   ?MmapFile // 로그 파일
	index_file ?MmapFile // 인덱스 파일
	position   i64       // 현재 위치
	max_size   i64       // 최대 크기
}

/// create는 메모리 매핑을 사용하여 새 로그 세그먼트를 생성합니다.
pub fn LogSegmentMmap.create(dir string, base_offset i64, max_size i64) !LogSegmentMmap {
	log_path := '${dir}/${base_offset:020}.log'
	index_path := '${dir}/${base_offset:020}.index'

	// 디렉토리 존재 확인
	os.mkdir_all(dir) or { return error('failed to create directory: ${err}') }

	// 로그 파일 생성
	log_file := MmapFile.create(log_path, max_size) or {
		return error('failed to create log file: ${err}')
	}

	// 인덱스 파일 생성 (로그 크기의 10%)
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

/// open은 기존 로그 세그먼트를 엽니다.
pub fn LogSegmentMmap.open(dir string, base_offset i64) !LogSegmentMmap {
	log_path := '${dir}/${base_offset:020}.log'
	index_path := '${dir}/${base_offset:020}.index'

	// 로그 파일 열기
	log_file := MmapFile.open(log_path, false) or {
		return error('failed to open log file: ${err}')
	}

	// 인덱스 파일 열기
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

/// append는 세그먼트에 레코드를 추가합니다.
pub fn (mut s LogSegmentMmap) append(data []u8) !i64 {
	record_size := data.len

	if s.position + record_size > s.max_size {
		return error('segment full')
	}

	if mut log_file := s.log_file {
		// 쓸 영역 매핑
		mut region := log_file.map_region(s.position, record_size)!

		// 영역에 데이터 쓰기
		for i, b in data {
			if i < region.data.len {
				region.data[i] = b
			}
		}
		region.dirty = true

		// 영역 동기화
		log_file.sync_region(region)!

		offset := s.position
		s.position += record_size

		return offset
	}

	return error('log file not initialized')
}

/// read는 세그먼트에서 레코드를 읽습니다.
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

/// current_position은 현재 위치 (다음 쓰기 오프셋)를 반환합니다.
pub fn (s LogSegmentMmap) current_position() i64 {
	return s.position
}

/// remaining_capacity는 남은 용량을 반환합니다.
pub fn (s LogSegmentMmap) remaining_capacity() i64 {
	return s.max_size - s.position
}

/// is_full은 세그먼트가 가득 찼는지 확인합니다.
pub fn (s LogSegmentMmap) is_full() bool {
	return s.position >= s.max_size
}

/// flush는 보류 중인 모든 쓰기를 플러시합니다.
pub fn (mut s LogSegmentMmap) flush() ! {
	if mut log_file := s.log_file {
		log_file.sync_all()!
	}
	if mut index_file := s.index_file {
		index_file.sync_all()!
	}
}

/// close는 세그먼트를 닫습니다.
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

/// MmapBenchmark는 mmap vs 일반 I/O 벤치마크입니다.
pub struct MmapBenchmark {
mut:
	iterations u64    // 반복 횟수
	data_size  int    // 데이터 크기
	temp_dir   string // 임시 디렉토리
}

/// new는 새 MmapBenchmark를 생성합니다.
pub fn MmapBenchmark.new(iterations u64, data_size int) MmapBenchmark {
	return MmapBenchmark{
		iterations: iterations
		data_size:  data_size
		temp_dir:   '/tmp/mmap_benchmark_${time.now().unix()}'
	}
}

/// cleanup은 임시 파일을 정리합니다.
pub fn (mut b MmapBenchmark) cleanup() {
	os.rmdir_all(b.temp_dir) or {}
}

/// benchmark_write는 mmap 쓰기 vs 일반 쓰기를 벤치마크합니다.
pub fn (mut b MmapBenchmark) benchmark_write() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or { return error('failed to create temp dir') }

	// 테스트 데이터 생성
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// mmap 쓰기 벤치마크
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

	// 일반 쓰기 벤치마크
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

/// benchmark_read는 mmap 읽기 vs 일반 읽기를 벤치마크합니다.
pub fn (mut b MmapBenchmark) benchmark_read() !(i64, i64) {
	os.mkdir_all(b.temp_dir) or { return error('failed to create temp dir') }

	// 테스트 파일 생성
	test_path := '${b.temp_dir}/read_test.dat'
	mut data := []u8{len: b.data_size}
	for i in 0 .. b.data_size {
		data[i] = u8(i % 256)
	}

	// 테스트 데이터 쓰기
	mut write_file := os.create(test_path) or { return error('failed to create test file') }
	for _ in 0 .. b.iterations {
		write_file.write(data) or {}
	}
	write_file.close()

	// mmap 읽기 벤치마크
	sw1 := time.new_stopwatch()
	mut mmap_file := MmapFile.open(test_path, true) or { return error('failed to open mmap file') }

	for i in 0 .. b.iterations {
		offset := i64(i) * i64(b.data_size)
		_ := mmap_file.map_region(offset, b.data_size) or { continue }
	}
	mmap_file.close() or {}
	mmap_time := sw1.elapsed().nanoseconds()

	// 일반 읽기 벤치마크
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
