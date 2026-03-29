/// Infra Layer - mmap-based partition store
/// High-performance persistent partition storage using Memory Mapped I/O
///
/// Features:
/// - append-only log segments
/// - sparse offset index
/// - automatic segment rollover
/// - OS page cache utilization
module memory

import common
import os
import time
import infra.performance.sysio

/// MmapPartitionStore is an mmap-based partition storage.
/// Each partition consists of multiple segments managed via mmap.
pub struct MmapPartitionStore {
pub:
	topic_name string
	partition  int
	base_dir   string
mut:
	segments       []&sysio.LogSegmentMmap
	active_segment ?&sysio.LogSegmentMmap
	base_offset    i64
	high_watermark i64
	segment_size   i64
	sync_on_write  bool
	index          MmapOffsetIndex
}

/// MmapOffsetIndex manages offset -> segment/position mappings.
/// A sparse index for memory efficiency.
struct MmapOffsetIndex {
mut:
	entries      []OffsetIndexEntry
	interval     int = 4096
	last_indexed i64
}

/// OffsetIndexEntry is an offset index entry.
struct OffsetIndexEntry {
pub:
	offset       i64
	segment_idx  int
	position     i64
	timestamp_ms i64
}

/// MmapPartitionConfig is the mmap partition configuration.
pub struct MmapPartitionConfig {
pub:
	topic_name    string
	partition     int
	base_dir      string
	segment_size  i64 = 1073741824
	sync_on_write bool
}

/// new_mmap_partition creates a new mmap partition store.
pub fn new_mmap_partition(config MmapPartitionConfig) !&MmapPartitionStore {
	// Create partition directory
	partition_dir := '${config.base_dir}/${config.topic_name}/${config.partition}'
	os.mkdir_all(partition_dir) or { return error('failed to create partition directory: ${err}') }

	mut store := &MmapPartitionStore{
		topic_name:     config.topic_name
		partition:      config.partition
		base_dir:       partition_dir
		segments:       []&sysio.LogSegmentMmap{}
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

	// Attempt to load existing segments
	store.load_existing_segments()!

	// Create a new segment if no active segment exists
	if store.active_segment == none {
		store.create_new_segment()!
	}

	return store
}

/// load_existing_segments loads existing segments from disk.
fn (mut s MmapPartitionStore) load_existing_segments() ! {
	files := os.ls(s.base_dir) or { return }

	// Filter and sort .log files only
	mut log_files := files.filter(it.ends_with('.log'))
	log_files.sort()

	for file in log_files {
		// Extract base_offset from filename (e.g. 00000000000000000000.log)
		base_name := file.replace('.log', '')
		base_offset := base_name.i64()

		segment := sysio.LogSegmentMmap.open(s.base_dir, base_offset) or {
			eprintln('[MmapPartition] Failed to open segment ${file}: ${err}')
			continue
		}

		s.segments << &segment

		// Set base_offset from the first segment
		if s.segments.len == 1 {
			s.base_offset = base_offset
		}

		// Update high_watermark
		s.high_watermark = base_offset + segment.current_position()
	}

	// Set the last segment as the active segment
	if s.segments.len > 0 {
		s.active_segment = s.segments[s.segments.len - 1]
	}
}

/// create_new_segment creates a new segment.
fn (mut s MmapPartitionStore) create_new_segment() ! {
	// The base_offset of the new segment is the current high_watermark
	base_offset := s.high_watermark

	segment := sysio.LogSegmentMmap.create(s.base_dir, base_offset, s.segment_size) or {
		return error('failed to create segment: ${err}')
	}

	s.segments << &segment
	s.active_segment = s.segments[s.segments.len - 1]
}

/// append adds records to the partition.
/// Returns: (base_offset, records_written)
pub fn (mut s MmapPartitionStore) append(records [][]u8) !(i64, int) {
	if records.len == 0 {
		return s.high_watermark, 0
	}

	mut active := s.active_segment or { return error('no active segment') }

	base_offset := s.high_watermark
	mut written := 0
	now := time.now()

	for record in records {
		// Record frame: length(4) + timestamp(8) + data
		record_frame := encode_record_frame(record, now.unix_milli())

		// Create a new segment if the current one is full
		if active.remaining_capacity() < record_frame.len {
			active.flush() or {
				eprintln('[MmapPartition] flush before segment rollover failed: ${err}')
			}
			s.create_new_segment()!
			active = s.active_segment or { return error('failed to create new segment') }
		}

		// Write record
		_ := active.append(record_frame) or { return error('failed to append record: ${err}') }

		// Update index
		current_offset := base_offset + written
		s.maybe_add_index_entry(current_offset, s.segments.len - 1, active.current_position())

		written++
	}

	s.high_watermark = base_offset + written

	// Flush immediately if sync_on_write is enabled
	if s.sync_on_write {
		active.flush() or { eprintln('[MmapPartition] sync_on_write flush failed: ${err}') }
	}

	return base_offset, written
}

/// read reads records from the specified offset.
pub fn (mut s MmapPartitionStore) read(offset i64, max_records int) ![][]u8 {
	if offset < s.base_offset || offset >= s.high_watermark {
		return [][]u8{}
	}

	// Find the starting position from the index
	segment_idx, position := s.find_position(offset)

	if segment_idx < 0 || segment_idx >= s.segments.len {
		return error('segment not found for offset ${offset}')
	}

	mut result := [][]u8{cap: max_records}
	mut current_seg_idx := segment_idx
	mut current_pos := position
	mut current_offset := offset

	// Read across multiple segments
	for result.len < max_records && current_offset < s.high_watermark {
		if current_seg_idx >= s.segments.len {
			break
		}

		mut segment := s.segments[current_seg_idx]

		// Read to the end of the segment
		for result.len < max_records && current_pos < segment.current_position() {
			// Read record frame: length(4) + timestamp(8) + data
			frame := segment.read(current_pos, 12) or { break }
			if frame.len < 12 {
				break
			}

			record_len := int(common.read_i32_be(frame) or { break })
			if record_len <= 0 || record_len > 104857600 {
				// More than 100MB is abnormal
				break
			}

			// Read the full record
			full_frame := segment.read(current_pos, 12 + record_len) or { break }
			if full_frame.len < 12 + record_len {
				break
			}

			// Extract data (skip timestamp)
			record_data := full_frame[12..].clone()
			result << record_data

			current_pos += 12 + record_len
			current_offset++
		}

		// Move to the next segment
		current_seg_idx++
		current_pos = 0
	}

	return result
}

/// find_position finds the segment and position corresponding to an offset.
fn (s &MmapPartitionStore) find_position(offset i64) (int, i64) {
	// Find the nearest entry in the index
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

	// Sequential scan from the index entry
	mut seg_idx := best_entry.segment_idx
	mut pos := best_entry.position
	mut current_offset := best_entry.offset

	for current_offset < offset && seg_idx < s.segments.len {
		mut segment := s.segments[seg_idx]

		for current_offset < offset && pos < segment.current_position() {
			// Read record length
			frame := segment.read(pos, 4) or { break }
			if frame.len < 4 {
				break
			}

			record_len := int(common.read_i32_be(frame) or { break })
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

/// maybe_add_index_entry adds an index entry based on the indexing interval.
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

/// get_base_offset returns the base offset.
pub fn (s &MmapPartitionStore) get_base_offset() i64 {
	return s.base_offset
}

/// get_high_watermark returns the high watermark.
pub fn (s &MmapPartitionStore) get_high_watermark() i64 {
	return s.high_watermark
}

/// flush flushes all segments to disk.
pub fn (mut s MmapPartitionStore) flush() ! {
	for mut segment in s.segments {
		segment.flush()!
	}
}

/// close closes all segments.
pub fn (mut s MmapPartitionStore) close() ! {
	for mut segment in s.segments {
		segment.close()!
	}
	s.segments.clear()
	s.active_segment = none
}

/// encode_record_frame encodes a record into a frame.
/// Format: length(4) + timestamp(8) + data
fn encode_record_frame(data []u8, timestamp_ms i64) []u8 {
	mut frame := []u8{cap: 12 + data.len}

	// length (4 bytes, big-endian)
	common.write_i32_be(mut frame, i32(data.len))

	// timestamp (8 bytes, big-endian)
	common.write_i64_be(mut frame, timestamp_ms)

	// data
	frame << data

	return frame
}

/// MmapPartitionStats holds partition statistics.
pub struct MmapPartitionStats {
pub:
	segment_count  int
	total_bytes    i64
	base_offset    i64
	high_watermark i64
	index_entries  int
}

/// get_stats returns partition statistics.
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
