/// Infra Layer - mmap 기반 파티션 스토어
/// Memory Mapped I/O를 사용한 고성능 영속 파티션 스토리지
///
/// 특징:
/// - append-only 로그 세그먼트
/// - sparse 오프셋 인덱스
/// - 자동 세그먼트 롤오버
/// - OS 페이지 캐시 활용
module memory

import os
import time
import infra.performance.io

/// MmapPartitionStore는 mmap 기반 파티션 스토리지입니다.
/// 각 파티션은 여러 세그먼트로 구성되며, 세그먼트는 mmap으로 관리됩니다.
pub struct MmapPartitionStore {
pub:
	topic_name string // 토픽 이름
	partition  int    // 파티션 번호
	base_dir   string // 기본 디렉토리
mut:
	segments       []&io.LogSegmentMmap // 세그먼트 목록
	active_segment ?&io.LogSegmentMmap  // 현재 활성 세그먼트
	base_offset    i64                  // 기본 오프셋 (첫 세그먼트)
	high_watermark i64                  // 최고 수위 (다음 쓰기 오프셋)
	segment_size   i64                  // 세그먼트 최대 크기
	sync_on_write  bool                 // 쓰기마다 sync 여부
	index          MmapOffsetIndex      // 오프셋 인덱스
}

/// MmapOffsetIndex는 오프셋 → 세그먼트/위치 매핑을 관리합니다.
/// sparse 인덱스로 메모리 효율적입니다.
struct MmapOffsetIndex {
mut:
	entries      []OffsetIndexEntry // 인덱스 엔트리
	interval     int = 4096 // 인덱싱 간격 (4096 레코드마다)
	last_indexed i64 // 마지막 인덱싱된 오프셋
}

/// OffsetIndexEntry는 오프셋 인덱스 엔트리입니다.
struct OffsetIndexEntry {
pub:
	offset       i64 // 레코드 오프셋
	segment_idx  int // 세그먼트 인덱스
	position     i64 // 세그먼트 내 위치
	timestamp_ms i64 // 타임스탬프 (밀리초)
}

/// MmapPartitionConfig는 mmap 파티션 설정입니다.
pub struct MmapPartitionConfig {
pub:
	topic_name    string
	partition     int
	base_dir      string
	segment_size  i64  = 1073741824 // 1GB
	sync_on_write bool = false
}

/// new_mmap_partition은 새 mmap 파티션 스토어를 생성합니다.
pub fn new_mmap_partition(config MmapPartitionConfig) !&MmapPartitionStore {
	// 파티션 디렉토리 생성
	partition_dir := '${config.base_dir}/${config.topic_name}/${config.partition}'
	os.mkdir_all(partition_dir) or { return error('failed to create partition directory: ${err}') }

	mut store := &MmapPartitionStore{
		topic_name:     config.topic_name
		partition:      config.partition
		base_dir:       partition_dir
		segments:       []&io.LogSegmentMmap{}
		active_segment: none
		base_offset:    0
		high_watermark: 0
		segment_size:   config.segment_size
		sync_on_write:  config.sync_on_write
		index:          MmapOffsetIndex{
			entries:  []OffsetIndexEntry{}
			interval: 4096
		}
	}

	// 기존 세그먼트 로드 시도
	store.load_existing_segments()!

	// 활성 세그먼트가 없으면 새로 생성
	if store.active_segment == none {
		store.create_new_segment()!
	}

	return store
}

/// load_existing_segments는 디스크에서 기존 세그먼트를 로드합니다.
fn (mut s MmapPartitionStore) load_existing_segments() ! {
	files := os.ls(s.base_dir) or { return }

	// .log 파일만 필터링하고 정렬
	mut log_files := files.filter(it.ends_with('.log'))
	log_files.sort()

	for file in log_files {
		// 파일명에서 base_offset 추출 (예: 00000000000000000000.log)
		base_name := file.replace('.log', '')
		base_offset := base_name.i64()

		segment := io.LogSegmentMmap.open(s.base_dir, base_offset) or {
			eprintln('[MmapPartition] Failed to open segment ${file}: ${err}')
			continue
		}

		s.segments << &segment

		// 첫 세그먼트의 base_offset 설정
		if s.segments.len == 1 {
			s.base_offset = base_offset
		}

		// high_watermark 업데이트
		s.high_watermark = base_offset + segment.current_position()
	}

	// 마지막 세그먼트를 활성 세그먼트로 설정
	if s.segments.len > 0 {
		s.active_segment = s.segments[s.segments.len - 1]
	}
}

/// create_new_segment는 새 세그먼트를 생성합니다.
fn (mut s MmapPartitionStore) create_new_segment() ! {
	// 새 세그먼트의 base_offset은 현재 high_watermark
	base_offset := s.high_watermark

	segment := io.LogSegmentMmap.create(s.base_dir, base_offset, s.segment_size) or {
		return error('failed to create segment: ${err}')
	}

	s.segments << &segment
	s.active_segment = s.segments[s.segments.len - 1]
}

/// append는 레코드를 파티션에 추가합니다.
/// 반환값: (base_offset, records_written)
pub fn (mut s MmapPartitionStore) append(records [][]u8) !(i64, int) {
	if records.len == 0 {
		return s.high_watermark, 0
	}

	mut active := s.active_segment or { return error('no active segment') }

	base_offset := s.high_watermark
	mut written := 0
	now := time.now()

	for record in records {
		// 레코드 프레임: length(4) + timestamp(8) + data
		record_frame := encode_record_frame(record, now.unix_milli())

		// 세그먼트가 가득 찼으면 새 세그먼트 생성
		if active.remaining_capacity() < record_frame.len {
			active.flush() or {}
			s.create_new_segment()!
			active = s.active_segment or { return error('failed to create new segment') }
		}

		// 레코드 쓰기
		_ := active.append(record_frame) or { return error('failed to append record: ${err}') }

		// 인덱스 업데이트
		current_offset := base_offset + written
		s.maybe_add_index_entry(current_offset, s.segments.len - 1, active.current_position())

		written++
	}

	s.high_watermark = base_offset + written

	// sync_on_write가 활성화되면 즉시 플러시
	if s.sync_on_write {
		active.flush() or {}
	}

	return base_offset, written
}

/// read는 지정된 오프셋부터 레코드를 읽습니다.
pub fn (mut s MmapPartitionStore) read(offset i64, max_records int) ![][]u8 {
	if offset < s.base_offset || offset >= s.high_watermark {
		return [][]u8{}
	}

	// 인덱스에서 시작 위치 찾기
	segment_idx, position := s.find_position(offset)

	if segment_idx < 0 || segment_idx >= s.segments.len {
		return error('segment not found for offset ${offset}')
	}

	mut result := [][]u8{cap: max_records}
	mut current_seg_idx := segment_idx
	mut current_pos := position
	mut current_offset := offset

	// 여러 세그먼트에 걸쳐 읽기
	for result.len < max_records && current_offset < s.high_watermark {
		if current_seg_idx >= s.segments.len {
			break
		}

		mut segment := s.segments[current_seg_idx]

		// 세그먼트 끝까지 읽기
		for result.len < max_records && current_pos < segment.current_position() {
			// 레코드 프레임 읽기: length(4) + timestamp(8) + data
			frame := segment.read(current_pos, 12) or { break }
			if frame.len < 12 {
				break
			}

			record_len := int(u32(frame[0]) << 24 | u32(frame[1]) << 16 | u32(frame[2]) << 8 | u32(frame[3]))
			if record_len <= 0 || record_len > 104857600 {
				// 100MB 초과는 비정상
				break
			}

			// 전체 레코드 읽기
			full_frame := segment.read(current_pos, 12 + record_len) or { break }
			if full_frame.len < 12 + record_len {
				break
			}

			// 데이터 추출 (timestamp 건너뛰고)
			record_data := full_frame[12..].clone()
			result << record_data

			current_pos += 12 + record_len
			current_offset++
		}

		// 다음 세그먼트로 이동
		current_seg_idx++
		current_pos = 0
	}

	return result
}

/// find_position은 오프셋에 해당하는 세그먼트와 위치를 찾습니다.
fn (s &MmapPartitionStore) find_position(offset i64) (int, i64) {
	// 인덱스에서 가장 가까운 엔트리 찾기
	mut best_entry := OffsetIndexEntry{
		offset:      s.base_offset
		segment_idx: 0
		position:    0
	}

	for entry in s.index.entries {
		if entry.offset <= offset && entry.offset > best_entry.offset {
			best_entry = entry
		}
	}

	// 인덱스 엔트리에서 순차 스캔
	mut seg_idx := best_entry.segment_idx
	mut pos := best_entry.position
	mut current_offset := best_entry.offset

	for current_offset < offset && seg_idx < s.segments.len {
		mut segment := s.segments[seg_idx]

		for current_offset < offset && pos < segment.current_position() {
			// 레코드 길이 읽기
			frame := segment.read(pos, 4) or { break }
			if frame.len < 4 {
				break
			}

			record_len := int(u32(frame[0]) << 24 | u32(frame[1]) << 16 | u32(frame[2]) << 8 | u32(frame[3]))
			if record_len <= 0 {
				break
			}

			pos += 12 + record_len
			current_offset++
		}

		if current_offset < offset {
			seg_idx++
			pos = 0
		}
	}

	return seg_idx, pos
}

/// maybe_add_index_entry는 인덱싱 간격에 따라 인덱스 엔트리를 추가합니다.
fn (mut s MmapPartitionStore) maybe_add_index_entry(offset i64, segment_idx int, position i64) {
	if offset - s.index.last_indexed >= s.index.interval {
		s.index.entries << OffsetIndexEntry{
			offset:       offset
			segment_idx:  segment_idx
			position:     position
			timestamp_ms: time.now().unix_milli()
		}
		s.index.last_indexed = offset
	}
}

/// get_base_offset는 기본 오프셋을 반환합니다.
pub fn (s &MmapPartitionStore) get_base_offset() i64 {
	return s.base_offset
}

/// get_high_watermark는 최고 수위를 반환합니다.
pub fn (s &MmapPartitionStore) get_high_watermark() i64 {
	return s.high_watermark
}

/// flush는 모든 세그먼트를 디스크에 플러시합니다.
pub fn (mut s MmapPartitionStore) flush() ! {
	for mut segment in s.segments {
		segment.flush()!
	}
}

/// close는 모든 세그먼트를 닫습니다.
pub fn (mut s MmapPartitionStore) close() ! {
	for mut segment in s.segments {
		segment.close()!
	}
	s.segments.clear()
	s.active_segment = none
}

/// encode_record_frame은 레코드를 프레임으로 인코딩합니다.
/// 형식: length(4) + timestamp(8) + data
fn encode_record_frame(data []u8, timestamp_ms i64) []u8 {
	total_len := data.len
	mut frame := []u8{len: 12 + total_len}

	// length (4 bytes, big-endian)
	frame[0] = u8(total_len >> 24)
	frame[1] = u8(total_len >> 16)
	frame[2] = u8(total_len >> 8)
	frame[3] = u8(total_len)

	// timestamp (8 bytes, big-endian)
	frame[4] = u8(timestamp_ms >> 56)
	frame[5] = u8(timestamp_ms >> 48)
	frame[6] = u8(timestamp_ms >> 40)
	frame[7] = u8(timestamp_ms >> 32)
	frame[8] = u8(timestamp_ms >> 24)
	frame[9] = u8(timestamp_ms >> 16)
	frame[10] = u8(timestamp_ms >> 8)
	frame[11] = u8(timestamp_ms)

	// data
	for i, b in data {
		frame[12 + i] = b
	}

	return frame
}

/// MmapPartitionStats는 파티션 통계입니다.
pub struct MmapPartitionStats {
pub:
	segment_count  int // 세그먼트 수
	total_bytes    i64 // 총 바이트
	base_offset    i64 // 기본 오프셋
	high_watermark i64 // 최고 수위
	index_entries  int // 인덱스 엔트리 수
}

/// get_stats는 파티션 통계를 반환합니다.
pub fn (s &MmapPartitionStore) get_stats() MmapPartitionStats {
	mut total_bytes := i64(0)
	for segment in s.segments {
		total_bytes += segment.current_position()
	}

	return MmapPartitionStats{
		segment_count:  s.segments.len
		total_bytes:    total_bytes
		base_offset:    s.base_offset
		high_watermark: s.high_watermark
		index_entries:  s.index.entries.len
	}
}
