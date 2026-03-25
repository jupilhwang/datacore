// Tests for compaction merge order preservation (Issue #3)
// Verifies that parallel segment downloads are reassembled in original order.
module s3

import time

/// test_parallel_download_results_preserve_order verifies that when 3 segments
/// complete downloads in arbitrary (reversed) order, the merged data still
/// preserves the original segment sequence: seg0 ++ seg1 ++ seg2.
fn test_parallel_download_results_preserve_order() {
	// Arrange: 3 segments with distinct byte markers
	segment_data := [
		[u8(0xAA), 0xAA, 0xAA], // segment 0
		[u8(0xBB), 0xBB, 0xBB], // segment 1
		[u8(0xCC), 0xCC, 0xCC], // segment 2
	]
	segment_count := segment_data.len

	// Act: simulate out-of-order channel delivery (reverse order: 2, 1, 0)
	ch := chan SegmentDownloadResult{cap: segment_count}
	// Send in reverse order to simulate non-deterministic goroutine completion
	ch <- SegmentDownloadResult{
		index: 2
		data:  segment_data[2]
	}
	ch <- SegmentDownloadResult{
		index: 1
		data:  segment_data[1]
	}
	ch <- SegmentDownloadResult{
		index: 0
		data:  segment_data[0]
	}

	// Collect into indexed results array (the pattern under test)
	mut results := [][]u8{len: segment_count}
	for _ in 0 .. segment_count {
		result := <-ch
		results[result.index] = result.data
	}

	// Merge in sequential order
	mut merged := []u8{}
	for r in results {
		merged << r
	}

	// Assert: merged data must be in original segment order
	expected := [u8(0xAA), 0xAA, 0xAA, 0xBB, 0xBB, 0xBB, 0xCC, 0xCC, 0xCC]
	assert merged == expected, 'Merged data must preserve original segment order. Got: ${merged}'
}

/// test_parallel_download_with_spawn_preserves_order uses actual spawn calls
/// with deliberate sleep delays to force out-of-order completion. Verifies
/// that the indexed results array correctly reassembles the original order.
fn test_parallel_download_with_spawn_preserves_order() {
	// Arrange: 3 segments with unique data
	segment_data := [
		[u8(0x01), 0x02, 0x03], // segment 0 (will finish last)
		[u8(0x04), 0x05, 0x06], // segment 1 (will finish second)
		[u8(0x07), 0x08, 0x09], // segment 2 (will finish first)
	]
	segment_count := segment_data.len

	// Act: spawn goroutines with staggered delays to force reverse completion
	ch := chan SegmentDownloadResult{cap: segment_count}
	for i, data in segment_data {
		spawn fn [ch, i, data, segment_count] () {
			// Delay inversely proportional to index to force reverse order
			delay_ms := (segment_count - i) * 10
			time.sleep(delay_ms * time.millisecond)
			ch <- SegmentDownloadResult{
				index: i
				data:  data
			}
		}()
	}

	// Collect into indexed array
	mut results := [][]u8{len: segment_count}
	for _ in 0 .. segment_count {
		result := <-ch
		results[result.index] = result.data
	}

	// Merge sequentially
	mut merged := []u8{}
	for r in results {
		merged << r
	}

	// Assert: order must match original segment sequence
	expected := [u8(0x01), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
	assert merged == expected, 'Spawn-based parallel download must preserve order. Got: ${merged}'
}

/// test_indexed_results_with_single_segment verifies correctness for the
/// edge case of a single-segment download.
fn test_indexed_results_with_single_segment() {
	segment_data := [[u8(0xFF), 0xFE]]

	ch := chan SegmentDownloadResult{cap: 1}
	ch <- SegmentDownloadResult{
		index: 0
		data:  segment_data[0]
	}

	mut results := [][]u8{len: 1}
	result := <-ch
	results[result.index] = result.data

	mut merged := []u8{}
	for r in results {
		merged << r
	}

	assert merged == [u8(0xFF), 0xFE], 'Single segment must pass through unchanged'
}
